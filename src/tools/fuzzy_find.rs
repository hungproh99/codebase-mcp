use anyhow::{Context, Result};
use fuzzy_matcher::FuzzyMatcher;
use fuzzy_matcher::skim::SkimMatcherV2;
use ignore::WalkBuilder;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;
use tokio::task;

pub fn schema() -> Value {
    json!({
        "name": "fuzzy_find",
        "description": "Perform fuzzy path and file name search across one or more roots.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "pattern": { "type": "string" },
                "paths": { "type": "array", "items": { "type": "string" } },
                "target_type": { "type": "string", "enum": ["file", "dir", "any"] },
                "extensions": { "type": "array", "items": { "type": "string" } },
                "max_depth": { "type": "integer" },
                "max_results": { "type": "integer" }
            },
            "required": ["pattern"]
        }
    })
}

#[derive(Clone, Debug)]
struct RankedMatch {
    path: String,
    relative_path: String,
    score: i64,
    entry_type: &'static str,
    size: u64,
    modified_at: u64,
}

pub async fn execute(args: &Value) -> Result<Value> {
    let args_owned = args.clone();
    task::spawn_blocking(move || execute_blocking(args_owned))
        .await
        .context("fuzzy_find background task failed to join")?
}

fn execute_blocking(args: Value) -> Result<Value> {
    let pattern = args
        .get("pattern")
        .and_then(|v| v.as_str())
        .context("Missing/empty pattern")?;
    let paths: Vec<PathBuf> = args
        .get("paths")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| p.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_else(|| vec![std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))]);

    let target_type = args
        .get("target_type")
        .and_then(|v| v.as_str())
        .unwrap_or("any");
    let max_results = args
        .get("max_results")
        .and_then(|v| v.as_u64())
        .unwrap_or(50) as usize;
    let max_depth = args.get("max_depth").and_then(|v| v.as_u64());
    let extensions: Vec<String> = args
        .get("extensions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|p| p.as_str().map(|s| s.to_lowercase()))
                .collect()
        })
        .unwrap_or_default();
    let search_roots: Vec<PathBuf> = paths
        .iter()
        .map(|path| path.canonicalize().unwrap_or_else(|_| path.clone()))
        .collect();

    let matcher = SkimMatcherV2::default();
    let mut ranked = Vec::new();
    let mut seen_paths = HashSet::new();

    for root in &search_roots {
        process_search_root(
            root,
            &search_roots,
            pattern,
            target_type,
            &extensions,
            max_depth,
            max_results,
            &matcher,
            &mut ranked,
            &mut seen_paths,
        )?;
    }

    ranked.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.relative_path.cmp(&right.relative_path))
            .then_with(|| left.path.cmp(&right.path))
    });

    let results: Vec<Value> = ranked
        .into_iter()
        .take(max_results)
        .map(|item| {
            json!({
                "path": item.path,
                "relative_path": item.relative_path,
                "score": item.score,
                "type": item.entry_type,
                "size": item.size,
                "modified_at": item.modified_at
            })
        })
        .collect();

    Ok(json!({
        "results": results,
        "total_returned": results.len()
    }))
}

#[allow(clippy::too_many_arguments)]
fn process_search_root(
    root: &Path,
    all_roots: &[PathBuf],
    pattern: &str,
    target_type: &str,
    extensions: &[String],
    max_depth: Option<u64>,
    max_results: usize,
    matcher: &SkimMatcherV2,
    ranked: &mut Vec<RankedMatch>,
    seen_paths: &mut HashSet<String>,
) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    let canonical_root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());

    if canonical_root.is_file() {
        consider_candidate(
            &canonical_root,
            false,
            all_roots,
            pattern,
            target_type,
            extensions,
            None,
            max_results,
            matcher,
            ranked,
            seen_paths,
            None,
        );
        return Ok(());
    }

    if !canonical_root.is_dir() {
        return Ok(());
    }

    if crate::indexer::is_path_index_ready(&canonical_root)
        && let Some(candidates) = crate::indexer::query_path_candidates(
            &canonical_root,
            pattern,
            indexed_shortlist_limit(max_results),
        )
    {
        if target_type != "file" {
            consider_candidate(
                &canonical_root,
                true,
                all_roots,
                pattern,
                target_type,
                extensions,
                max_depth,
                max_results,
                matcher,
                ranked,
                seen_paths,
                None,
            );
        }

        for candidate in candidates {
            consider_indexed_candidate(
                &candidate,
                all_roots,
                pattern,
                target_type,
                extensions,
                max_depth,
                max_results,
                matcher,
                ranked,
                seen_paths,
            );
        }

        if !ranked.is_empty() {
            return Ok(());
        }
    }

    let mut walk = WalkBuilder::new(&canonical_root);
    walk.hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .threads(num_cpus());
    if let Some(depth) = max_depth {
        walk.max_depth(Some(depth as usize));
    }

    let local_ranked = Arc::new(Mutex::new(Vec::<RankedMatch>::new()));
    let local_seen = Arc::new(Mutex::new(HashSet::<String>::new()));
    let closure_roots = all_roots.to_vec();
    let closure_pattern = pattern.to_string();
    let closure_target_type = target_type.to_string();
    let closure_extensions = extensions.to_vec();
    let closure_max_depth = max_depth;
    let closure_max_results = max_results;

    walk.build_parallel().run(|| {
        let local_ranked = Arc::clone(&local_ranked);
        let local_seen = Arc::clone(&local_seen);
        let roots = closure_roots.clone();
        let pattern = closure_pattern.clone();
        let target_type = closure_target_type.clone();
        let extensions = closure_extensions.clone();
        let matcher = SkimMatcherV2::default();

        Box::new(move |entry| {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => return ignore::WalkState::Continue,
            };

            let Some(file_type) = entry.file_type() else {
                return ignore::WalkState::Continue;
            };

            if target_type == "file" && !file_type.is_file() {
                return ignore::WalkState::Continue;
            }
            if target_type == "dir" && !file_type.is_dir() {
                return ignore::WalkState::Continue;
            }

            let Some(candidate) = score_candidate(
                entry.path(),
                file_type.is_dir(),
                &roots,
                &pattern,
                &target_type,
                &extensions,
                closure_max_depth,
                &matcher,
                None,
            ) else {
                return ignore::WalkState::Continue;
            };

            {
                let mut seen = match local_seen.lock() {
                    Ok(guard) => guard,
                    Err(_) => return ignore::WalkState::Quit,
                };
                if !seen.insert(candidate.path.clone()) {
                    return ignore::WalkState::Continue;
                }
            }

            let mut ranked = match local_ranked.lock() {
                Ok(guard) => guard,
                Err(_) => return ignore::WalkState::Quit,
            };
            push_ranked_match(&mut ranked, candidate, closure_max_results);
            ignore::WalkState::Continue
        })
    });

    let local_ranked = match Arc::try_unwrap(local_ranked) {
        Ok(mutex) => mutex
            .into_inner()
            .map_err(|_| anyhow::anyhow!("fuzzy_find ranked collector is unavailable"))?,
        Err(shared) => shared
            .lock()
            .map_err(|_| anyhow::anyhow!("fuzzy_find ranked collector is unavailable"))?
            .clone(),
    };
    for candidate in local_ranked {
        if !seen_paths.insert(candidate.path.clone()) {
            continue;
        }
        push_ranked_match(ranked, candidate, max_results);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn consider_candidate(
    path: &Path,
    is_dir: bool,
    roots: &[PathBuf],
    pattern: &str,
    target_type: &str,
    extensions: &[String],
    max_depth: Option<u64>,
    max_results: usize,
    matcher: &SkimMatcherV2,
    ranked: &mut Vec<RankedMatch>,
    seen_paths: &mut HashSet<String>,
    cached_meta: Option<(u64, u64)>,
) {
    let Some(candidate) = score_candidate(
        path,
        is_dir,
        roots,
        pattern,
        target_type,
        extensions,
        max_depth,
        matcher,
        cached_meta,
    ) else {
        return;
    };
    if !seen_paths.insert(candidate.path.clone()) {
        return;
    }

    push_ranked_match(ranked, candidate, max_results);
}

#[allow(clippy::too_many_arguments)]
fn consider_indexed_candidate(
    candidate: &crate::indexer::PathQueryCandidate,
    roots: &[PathBuf],
    pattern: &str,
    target_type: &str,
    extensions: &[String],
    max_depth: Option<u64>,
    max_results: usize,
    matcher: &SkimMatcherV2,
    ranked: &mut Vec<RankedMatch>,
    seen_paths: &mut HashSet<String>,
) {
    let Some(candidate) = score_candidate_from_parts(
        &candidate.path,
        candidate.is_dir,
        compute_relative_path(&candidate.path, roots),
        pattern,
        target_type,
        extensions,
        max_depth,
        matcher,
        Some((candidate.size, candidate.modified_at)),
    ) else {
        return;
    };
    if !seen_paths.insert(candidate.path.clone()) {
        return;
    }

    push_ranked_match(ranked, candidate, max_results);
}

fn push_ranked_match(ranked: &mut Vec<RankedMatch>, candidate: RankedMatch, max_results: usize) {
    if max_results == 0 {
        return;
    }

    if ranked.len() >= max_results
        && ranked
            .last()
            .is_some_and(|worst| compare_ranked_match(&candidate, worst).is_lt())
    {
        return;
    }

    ranked.push(candidate);
    ranked.sort_by(compare_ranked_match);
    if ranked.len() > max_results {
        ranked.truncate(max_results);
    }
}

#[allow(clippy::too_many_arguments)]
fn score_candidate(
    path: &Path,
    is_dir: bool,
    roots: &[PathBuf],
    pattern: &str,
    target_type: &str,
    extensions: &[String],
    max_depth: Option<u64>,
    matcher: &SkimMatcherV2,
    cached_meta: Option<(u64, u64)>,
) -> Option<RankedMatch> {
    let relative_path = compute_relative_path(path, roots);
    score_candidate_from_parts(
        path,
        is_dir,
        relative_path,
        pattern,
        target_type,
        extensions,
        max_depth,
        matcher,
        cached_meta,
    )
}

#[allow(clippy::too_many_arguments)]
fn score_candidate_from_parts(
    path: &Path,
    is_dir: bool,
    relative_path: String,
    pattern: &str,
    target_type: &str,
    extensions: &[String],
    max_depth: Option<u64>,
    matcher: &SkimMatcherV2,
    cached_meta: Option<(u64, u64)>,
) -> Option<RankedMatch> {
    if target_type == "file" && is_dir {
        return None;
    }
    if target_type == "dir" && !is_dir {
        return None;
    }

    if !extensions.is_empty() && !is_dir {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase())
            .unwrap_or_default();
        if !extensions.contains(&ext) {
            return None;
        }
    }

    let normalized_path = normalize_path(path);
    if max_depth.is_some() && !within_max_depth(&relative_path, max_depth) {
        return None;
    }

    let score_target = if relative_path.is_empty() {
        normalized_path.as_str()
    } else {
        relative_path.as_str()
    };
    let score = matcher.fuzzy_match(score_target, pattern)?;
    if score <= 0 {
        return None;
    }

    let (size, modified_at) = cached_meta.unwrap_or_else(|| read_entry_metadata(path));

    Some(RankedMatch {
        path: normalized_path,
        relative_path,
        score,
        entry_type: if is_dir { "dir" } else { "file" },
        size,
        modified_at,
    })
}

fn read_entry_metadata(path: &Path) -> (u64, u64) {
    std::fs::metadata(path)
        .map(|meta| {
            let modified_at = meta
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_secs())
                .unwrap_or(0);
            (meta.len(), modified_at)
        })
        .unwrap_or((0, 0))
}

fn within_max_depth(relative_path: &str, max_depth: Option<u64>) -> bool {
    let Some(max_depth) = max_depth else {
        return true;
    };
    if relative_path.is_empty() {
        return true;
    }

    relative_path.split('/').count() as u64 <= max_depth
}

fn compute_relative_path(path: &Path, roots: &[PathBuf]) -> String {
    for root in roots {
        if root.is_dir() {
            if let Ok(stripped) = path.strip_prefix(root) {
                return normalize_path(stripped);
            }
        } else if root.is_file() && root == path {
            return path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
                .to_string();
        }
    }

    normalize_path(path)
}

fn compare_ranked_match(left: &RankedMatch, right: &RankedMatch) -> std::cmp::Ordering {
    right
        .score
        .cmp(&left.score)
        .then_with(|| left.relative_path.cmp(&right.relative_path))
        .then_with(|| left.path.cmp(&right.path))
}

fn indexed_shortlist_limit(max_results: usize) -> usize {
    max_results.saturating_mul(64).clamp(256, 4096)
}

fn normalize_path(path: &Path) -> String {
    let raw = path.to_string_lossy();

    if let Some(stripped) = raw.strip_prefix("\\\\?\\UNC\\") {
        format!("//{}", stripped.replace('\\', "/"))
    } else if let Some(stripped) = raw.strip_prefix("\\\\?\\") {
        stripped.replace('\\', "/")
    } else {
        raw.replace('\\', "/")
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(4)
}
