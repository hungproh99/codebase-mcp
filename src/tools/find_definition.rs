use anyhow::{Context, Result};
use grep_regex::RegexMatcherBuilder;
use grep_searcher::{BinaryDetection, SearcherBuilder, sinks::UTF8};
use serde_json::{Value, json};
use std::path::PathBuf;
use tokio::task;

use crate::tools::ast_support::visit_candidate_code_files;

/// Max files to scan before returning a partial result.
const MAX_FILES_SCANNED: usize = 50_000;
const MAX_FILE_SIZE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_RESULTS: usize = 20;

pub fn schema() -> Value {
    json!({
        "name": "find_definition",
        "description": "Find likely symbol definitions across the project.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": { "type": "string" },
                "paths": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["symbol"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let args_owned = args.clone();
    task::spawn_blocking(move || execute_blocking(args_owned))
        .await
        .context("find_definition background task failed to join")?
}

fn execute_blocking(args: Value) -> Result<Value> {
    let symbol = args
        .get("symbol")
        .and_then(|v| v.as_str())
        .context("Missing symbol")?;

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

    if !search_paths.iter().any(|path| path.exists()) {
        return Err(anyhow::anyhow!(
            "No valid search path found for find_definition"
        ));
    }

    let pattern_str = format!(
        r"(?i)\b(fn|pub\s+fn|def|class|struct|enum|trait|interface|type|function|const|let|var|void|int|bool|auto|static)\s+{}\b",
        regex::escape(symbol)
    );

    let matcher = RegexMatcherBuilder::new()
        .case_insensitive(false)
        .build(&pattern_str)
        .context("Invalid regex pattern")?;
    let mut searcher = SearcherBuilder::new()
        .binary_detection(BinaryDetection::quit(0))
        .build();

    let mut definitions = Vec::new();
    let mut files_scanned = 0usize;
    let mut files_skipped = 0usize;
    let mut limit_reached = false;

    visit_candidate_code_files(&search_paths, None, None, |candidate| {
        if limit_reached {
            return Ok(false);
        }

        if files_scanned >= MAX_FILES_SCANNED {
            limit_reached = true;
            return Ok(false);
        }

        let meta = match std::fs::metadata(candidate) {
            Ok(meta) => meta,
            Err(_) => {
                files_skipped += 1;
                return Ok(true);
            }
        };

        if meta.len() > MAX_FILE_SIZE_BYTES {
            files_skipped += 1;
            return Ok(true);
        }

        files_scanned += 1;

        let path_str = candidate.to_string_lossy().to_string();
        if definitions.len() >= MAX_RESULTS {
            limit_reached = true;
            return Ok(false);
        }

        let mut search_failed = false;
        let search_result = searcher.search_path(
            &matcher,
            candidate,
            UTF8(|line_num, line| {
                definitions.push(json!({
                    "file": path_str.clone(),
                    "line": line_num,
                    "snippet": line.trim()
                }));

                if definitions.len() >= MAX_RESULTS {
                    limit_reached = true;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }),
        );

        if search_result.is_err() {
            search_failed = true;
        }

        if search_failed {
            files_skipped += 1;
        }

        Ok(!limit_reached)
    })?;

    Ok(json!({
        "symbol": symbol,
        "definitions": definitions,
        "total_returned": definitions.len(),
        "files_scanned": files_scanned,
        "files_skipped_non_code": files_skipped,
        "limit_reached": limit_reached
    }))
}
