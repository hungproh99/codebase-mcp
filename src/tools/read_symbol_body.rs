use anyhow::{Context, Result};
use regex::Regex;
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tokio::task;

use crate::tools::ast_support::{
    DEFAULT_AST_FILE_SIZE_LIMIT, find_named_symbol_node, parse_language_filter,
    parse_supported_file, visit_candidate_code_files,
};
use crate::tools::read_file::decode_fuzzy;

const READ_FILE_SIZE_LIMIT: u64 = 10 * 1024 * 1024;
const HEURISTIC_WINDOW_LINES: usize = 80;

pub fn schema() -> Value {
    json!({
        "name": "read_symbol_body",
        "description": "Read a symbol body with AST-first resolution and heuristic fallback.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": { "type": "string" },
                "paths": { "type": "array", "items": { "type": "string" } },
                "file_hint": { "type": "string" },
                "language": { "type": "string" },
                "include_signature": { "type": "boolean" }
            },
            "required": ["symbol"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let args_owned = args.clone();
    task::spawn_blocking(move || execute_blocking(args_owned))
        .await
        .context("read_symbol_body background task failed to join")?
}

fn execute_blocking(args: Value) -> Result<Value> {
    let symbol = args
        .get("symbol")
        .and_then(|v| v.as_str())
        .context("Missing symbol")?;
    let include_signature = args
        .get("include_signature")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let language_filter = parse_language_filter(args.get("language").and_then(|v| v.as_str()))?;

    let search_paths: Vec<PathBuf> = args
        .get("paths")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|item| item.as_str())
                .map(PathBuf::from)
                .collect()
        })
        .unwrap_or_else(|| vec![std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))]);

    let file_hint = args
        .get("file_hint")
        .and_then(|v| v.as_str())
        .map(PathBuf::from);

    if let Some(file_hint) = &file_hint
        && (!file_hint.exists() || !file_hint.is_file())
    {
        return Err(anyhow::anyhow!(
            "file_hint is not a valid file: {}",
            file_hint.to_string_lossy()
        ));
    }

    let mut ast_result = None;
    visit_candidate_code_files(
        &search_paths,
        file_hint.as_deref(),
        language_filter,
        |candidate| {
            if let Some(ast_match) = try_ast_match(candidate, symbol, include_signature)? {
                ast_result = Some(json!({
                    "symbol": symbol,
                    "file": candidate.to_string_lossy().to_string(),
                    "start_line": ast_match.start_line,
                    "end_line": ast_match.end_line,
                    "content": ast_match.content,
                    "match_source": "ast",
                    "confidence": "high"
                }));
                return Ok(false);
            }

            Ok(true)
        },
    )?;

    if let Some(ast_result) = ast_result {
        return Ok(ast_result);
    }

    let definition_pattern = Regex::new(&format!(
        r"(?i)\b(fn|pub\s+fn|def|class|struct|enum|trait|interface|type|function|const|let|var|void|int|bool|auto|static)\s+{}\b",
        regex::escape(symbol)
    ))
    .context("Invalid heuristic definition regex")?;

    let mut heuristic_result = None;
    visit_candidate_code_files(
        &search_paths,
        file_hint.as_deref(),
        language_filter,
        |candidate| {
            if let Some(heuristic_match) =
                try_heuristic_match(candidate, &definition_pattern, include_signature)?
            {
                heuristic_result = Some(json!({
                    "symbol": symbol,
                    "file": candidate.to_string_lossy().to_string(),
                    "start_line": heuristic_match.start_line,
                    "end_line": heuristic_match.end_line,
                    "content": heuristic_match.content,
                    "match_source": "heuristic",
                    "confidence": heuristic_match.confidence
                }));
                return Ok(false);
            }

            Ok(true)
        },
    )?;

    if let Some(heuristic_result) = heuristic_result {
        return Ok(heuristic_result);
    }

    Err(anyhow::anyhow!("Could not resolve symbol '{}'", symbol))
}

struct SymbolBody {
    content: String,
    start_line: usize,
    end_line: usize,
}

struct HeuristicBody {
    content: String,
    start_line: usize,
    end_line: usize,
    confidence: &'static str,
}

fn try_ast_match(path: &Path, symbol: &str, include_signature: bool) -> Result<Option<SymbolBody>> {
    let parsed = match parse_supported_file(path, DEFAULT_AST_FILE_SIZE_LIMIT, None)? {
        Some(parsed) => parsed,
        None => return Ok(None),
    };

    let symbol_node = match find_named_symbol_node(parsed.tree.root_node(), &parsed.source, symbol)
    {
        Some(symbol_node) => symbol_node,
        None => return Ok(None),
    };

    let content_node = if include_signature {
        symbol_node
    } else {
        symbol_node
            .child_by_field_name("body")
            .unwrap_or(symbol_node)
    };

    let content = String::from_utf8_lossy(&parsed.source[content_node.byte_range()]).to_string();
    Ok(Some(SymbolBody {
        content,
        start_line: content_node.start_position().row + 1,
        end_line: content_node.end_position().row + 1,
    }))
}

fn try_heuristic_match(
    path: &Path,
    definition_pattern: &Regex,
    include_signature: bool,
) -> Result<Option<HeuristicBody>> {
    let meta = match std::fs::metadata(path) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };
    if meta.len() > READ_FILE_SIZE_LIMIT {
        return Ok(None);
    }

    let mut file = File::open(path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let (content, _) = decode_fuzzy(&buffer);
    let lines: Vec<&str> = content.split('\n').collect();

    for (line_index, line) in lines.iter().enumerate() {
        if definition_pattern.is_match(line) {
            return Ok(Some(extract_heuristic_body(
                &lines,
                line_index,
                include_signature,
            )));
        }
    }

    Ok(None)
}

fn extract_heuristic_body(
    lines: &[&str],
    definition_index: usize,
    include_signature: bool,
) -> HeuristicBody {
    let definition_line = lines.get(definition_index).copied().unwrap_or("");
    let definition_indent = indentation_width(definition_line);
    let (body_start, body_end, confidence) =
        if let Some(end_line) = find_brace_delimited_end(lines, definition_index) {
            (definition_index, end_line, "medium")
        } else if let Some(end_line) =
            find_indentation_delimited_end(lines, definition_index, definition_indent)
        {
            (definition_index, end_line, "medium")
        } else {
            (
                definition_index,
                std::cmp::min(lines.len(), definition_index + HEURISTIC_WINDOW_LINES),
                "low",
            )
        };

    let mut content_start = if include_signature {
        body_start
    } else {
        std::cmp::min(body_start + 1, body_end)
    };
    if content_start == body_end {
        content_start = body_start;
    }

    let content = lines[content_start..body_end].join("\n");
    HeuristicBody {
        content,
        start_line: content_start + 1,
        end_line: body_end,
        confidence,
    }
}

fn find_brace_delimited_end(lines: &[&str], definition_index: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut seen_open = false;

    for (offset, line) in lines.iter().enumerate().skip(definition_index) {
        for ch in line.chars() {
            match ch {
                '{' => {
                    depth += 1;
                    seen_open = true;
                }
                '}' if seen_open => {
                    depth -= 1;
                    if depth <= 0 {
                        return Some(offset + 1);
                    }
                }
                _ => {}
            }
        }
    }

    None
}

fn find_indentation_delimited_end(
    lines: &[&str],
    definition_index: usize,
    definition_indent: usize,
) -> Option<usize> {
    let mut first_body_line: Option<usize> = None;

    for (index, line) in lines.iter().enumerate().skip(definition_index + 1) {
        if line.trim().is_empty() {
            continue;
        }

        let indent = indentation_width(line);
        if first_body_line.is_none() {
            if indent <= definition_indent {
                return None;
            }
            first_body_line = Some(index);
            continue;
        }

        if indent <= definition_indent {
            return Some(index);
        }
    }

    first_body_line.map(|_| lines.len())
}

fn indentation_width(line: &str) -> usize {
    line.chars().take_while(|ch| ch.is_whitespace()).count()
}
