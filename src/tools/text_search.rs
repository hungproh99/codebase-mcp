use anyhow::{Context, Result};
use glob::Pattern;
use grep_regex::{RegexMatcher, RegexMatcherBuilder};
use grep_searcher::sinks::UTF8;
use grep_searcher::{BinaryDetection, Searcher, SearcherBuilder};
use ignore::WalkBuilder;
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::task;

use crate::common::insert_object_field;

#[derive(Serialize, Debug)]
pub struct SearchMatch {
    pub file: String,
    pub line: u64,
    pub snippet: String,
    pub line_text: String,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum SearchMode {
    Literal,
    Regex,
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum CaseMode {
    Insensitive,
    Sensitive,
    Smart,
}

#[derive(Debug)]
struct CandidateFile {
    path: PathBuf,
    relative_path: String,
}

#[derive(Default, Debug)]
struct SearchStats {
    paths_received: usize,
    valid_paths: usize,
    invalid_paths: Vec<String>,
    files_considered: usize,
    files_searched: usize,
}

pub fn schema() -> Value {
    json!({
        "name": "text_search",
        "description": "Search files or directories using literal or regex matching.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "paths": { "type": "array", "items": { "type": "string" } },
                "mode": { "type": "string", "enum": ["literal", "regex"] },
                "case_mode": { "type": "string", "enum": ["insensitive", "sensitive", "smart"] },
                "case_sensitive": { "type": "boolean" },
                "max_results": { "type": "integer" },
                "includes": { "type": "array", "items": { "type": "string" } },
                "excludes": { "type": "array", "items": { "type": "string" } },
                "context_lines": { "type": "integer" },
                "explain_no_results": { "type": "boolean" }
            },
            "required": ["query"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let args_owned = args.clone();
    task::spawn_blocking(move || execute_blocking(args_owned))
        .await
        .context("text_search background task failed to join")?
}

fn execute_blocking(args: Value) -> Result<Value> {
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim_end();
    if query.is_empty() {
        return Err(anyhow::anyhow!("Query cannot be empty"));
    }

    let input_paths: Vec<PathBuf> = args
        .get("paths")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| p.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_else(|| vec![std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))]);

    let mode = parse_mode(args.get("mode").and_then(|v| v.as_str()))?;
    let case_mode = parse_case_mode(
        args.get("case_mode").and_then(|v| v.as_str()),
        args.get("case_sensitive").and_then(|v| v.as_bool()),
    )?;
    let case_sensitive_effective = resolve_case_sensitive(case_mode, query);
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(100) as usize;
    let context_lines = args
        .get("context_lines")
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
        .min(10) as usize;
    let explain_no_results = args
        .get("explain_no_results")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let includes = parse_patterns(args.get("includes"))?;
    let excludes = parse_patterns(args.get("excludes"))?;

    let pattern = match mode {
        SearchMode::Literal => regex::escape(query),
        SearchMode::Regex => query.to_string(),
    };

    let matcher = RegexMatcherBuilder::new()
        .case_insensitive(!case_sensitive_effective)
        .build(&pattern)
        .context("Invalid search query")?;

    let searcher = SearcherBuilder::new()
        .binary_detection(BinaryDetection::quit(b'\x00'))
        .line_number(true)
        .before_context(context_lines)
        .after_context(context_lines)
        .build();

    let includes_applied = !includes.is_empty();
    let excludes_applied = !excludes.is_empty();
    let mut stats = SearchStats {
        paths_received: input_paths.len(),
        ..Default::default()
    };
    let mut matches = Vec::new();
    let mut search_errors = 0usize;
    let mut seen = HashSet::new();

    for input_path in input_paths {
        if matches.len() >= max_results {
            break;
        }

        process_input_path(
            &input_path,
            &includes,
            &excludes,
            &matcher,
            &searcher,
            max_results,
            &mut matches,
            &mut stats,
            &mut search_errors,
            &mut seen,
        )?;
    }

    let total_returned = matches.len();
    let limit_reached = total_returned >= max_results;
    let no_results = if explain_no_results && total_returned == 0 {
        Some(json!({
            "reason": no_results_reason(&stats),
            "paths_received": stats.paths_received,
            "valid_paths": stats.valid_paths,
            "invalid_paths": stats.invalid_paths,
            "files_considered": stats.files_considered,
            "files_searched": stats.files_searched,
            "includes_applied": includes_applied,
            "excludes_applied": excludes_applied
        }))
    } else {
        None
    };

    let mut response = json!({
        "matches": matches,
        "total_returned": total_returned,
        "limit_reached": limit_reached,
        "search_errors": search_errors,
        "mode": mode,
        "case_mode": case_mode
    });

    if let Some(no_results) = no_results {
        insert_object_field(&mut response, "no_results", no_results);
    }

    Ok(response)
}

#[allow(clippy::too_many_arguments)]
fn process_input_path(
    input_path: &Path,
    includes: &[Pattern],
    excludes: &[Pattern],
    matcher: &RegexMatcher,
    searcher: &Searcher,
    max_results: usize,
    matches: &mut Vec<SearchMatch>,
    stats: &mut SearchStats,
    search_errors: &mut usize,
    seen: &mut HashSet<String>,
) -> Result<()> {
    if !input_path.exists() {
        stats
            .invalid_paths
            .push(input_path.to_string_lossy().to_string());
        return Ok(());
    }

    let canonical_path = canonicalize_existing_path(input_path);

    if canonical_path.is_file() {
        stats.valid_paths += 1;
        let relative_path = canonical_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(|name| name.to_string())
            .unwrap_or_else(|| canonical_path.to_string_lossy().to_string());

        search_candidate(
            CandidateFile {
                path: canonical_path,
                relative_path,
            },
            includes,
            excludes,
            matcher,
            searcher,
            max_results,
            matches,
            stats,
            search_errors,
            seen,
        );
        return Ok(());
    }

    if !canonical_path.is_dir() {
        stats
            .invalid_paths
            .push(canonical_path.to_string_lossy().to_string());
        return Ok(());
    }

    stats.valid_paths += 1;

    for entry in WalkBuilder::new(&canonical_path)
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .build()
    {
        if matches.len() >= max_results {
            break;
        }

        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        if !entry
            .file_type()
            .is_some_and(|file_type| file_type.is_file())
        {
            continue;
        }

        let path = entry.path().to_path_buf();
        let relative_path = path
            .strip_prefix(&canonical_path)
            .ok()
            .map(normalize_path)
            .filter(|relative| !relative.is_empty())
            .unwrap_or_else(|| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.to_string())
                    .unwrap_or_else(|| path.to_string_lossy().to_string())
            });

        search_candidate(
            CandidateFile {
                path,
                relative_path,
            },
            includes,
            excludes,
            matcher,
            searcher,
            max_results,
            matches,
            stats,
            search_errors,
            seen,
        );
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn search_candidate(
    candidate: CandidateFile,
    includes: &[Pattern],
    excludes: &[Pattern],
    matcher: &RegexMatcher,
    searcher: &Searcher,
    max_results: usize,
    matches: &mut Vec<SearchMatch>,
    stats: &mut SearchStats,
    search_errors: &mut usize,
    seen: &mut HashSet<String>,
) {
    if matches.len() >= max_results {
        return;
    }

    let path_key = normalize_path(&candidate.path);
    if !seen.insert(path_key) {
        return;
    }

    stats.files_considered += 1;

    if !passes_patterns(&candidate, includes, excludes) {
        return;
    }

    let meta = match candidate.path.metadata() {
        Ok(meta) => meta,
        Err(_) => {
            *search_errors += 1;
            return;
        }
    };

    if meta.len() > 5 * 1024 * 1024 {
        return;
    }

    stats.files_searched += 1;
    let mut local_matches = Vec::new();
    let candidate_path = candidate.path.clone();
    let remaining = max_results.saturating_sub(matches.len());

    let search_result = searcher.clone().search_path(
        matcher,
        &candidate_path,
        UTF8(|line_num, line| {
            let raw_line = line.trim_end_matches(['\r', '\n']).to_string();
            local_matches.push(SearchMatch {
                file: candidate_path.to_string_lossy().to_string(),
                line: line_num,
                snippet: raw_line.trim().to_string(),
                line_text: raw_line,
            });

            if local_matches.len() >= remaining {
                Ok(false)
            } else {
                Ok(true)
            }
        }),
    );

    if search_result.is_err() {
        *search_errors += 1;
        return;
    }

    for search_match in local_matches {
        if matches.len() >= max_results {
            break;
        }
        matches.push(search_match);
    }
}

fn parse_mode(raw: Option<&str>) -> Result<SearchMode> {
    match raw.unwrap_or("literal") {
        "literal" => Ok(SearchMode::Literal),
        "regex" => Ok(SearchMode::Regex),
        other => Err(anyhow::anyhow!("Unsupported mode '{}'", other)),
    }
}

fn parse_case_mode(raw: Option<&str>, legacy_case_sensitive: Option<bool>) -> Result<CaseMode> {
    if let Some(mode) = raw {
        return match mode {
            "insensitive" => Ok(CaseMode::Insensitive),
            "sensitive" => Ok(CaseMode::Sensitive),
            "smart" => Ok(CaseMode::Smart),
            other => Err(anyhow::anyhow!("Unsupported case_mode '{}'", other)),
        };
    }

    Ok(match legacy_case_sensitive {
        Some(true) => CaseMode::Sensitive,
        _ => CaseMode::Insensitive,
    })
}

fn resolve_case_sensitive(case_mode: CaseMode, query: &str) -> bool {
    match case_mode {
        CaseMode::Insensitive => false,
        CaseMode::Sensitive => true,
        CaseMode::Smart => query.chars().any(|c| c.is_uppercase()),
    }
}

fn parse_patterns(value: Option<&Value>) -> Result<Vec<Pattern>> {
    value
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(|pattern| {
                    Pattern::new(pattern).context(format!("Invalid glob pattern '{}'", pattern))
                })
                .collect()
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn passes_patterns(candidate: &CandidateFile, includes: &[Pattern], excludes: &[Pattern]) -> bool {
    let candidates = candidate_match_strings(candidate);

    if !includes.is_empty() && !matches_any_pattern(&candidates, includes) {
        return false;
    }

    if !excludes.is_empty() && matches_any_pattern(&candidates, excludes) {
        return false;
    }

    true
}

fn candidate_match_strings(candidate: &CandidateFile) -> Vec<String> {
    let full_path = normalize_path(&candidate.path);
    let file_name = candidate
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();

    let mut values = vec![candidate.relative_path.clone(), full_path];
    if !file_name.is_empty() {
        values.push(file_name);
    }
    values
}

fn matches_any_pattern(values: &[String], patterns: &[Pattern]) -> bool {
    patterns
        .iter()
        .any(|pattern| values.iter().any(|value| pattern.matches(value)))
}

fn no_results_reason(stats: &SearchStats) -> &'static str {
    if stats.valid_paths == 0 {
        "no_valid_paths"
    } else if stats.files_searched == 0 {
        "no_candidate_files"
    } else {
        "no_match_found"
    }
}

fn canonicalize_existing_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}
