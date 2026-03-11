use anyhow::{Context, Result};
use git2::BlameOptions;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

use crate::common::repo_relative_path;

use super::git_helper;

#[derive(Clone)]
struct BlameLine {
    line: usize,
    author: String,
    commit: String,
    timestamp: i64,
}

pub fn schema() -> Value {
    json!({
        "name": "git_blame",
        "description": "Return line-level author and commit attribution for a file.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": { "type": "string" },
                "start_line": { "type": "integer" },
                "end_line": { "type": "integer" }
            },
            "required": ["file_path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("file_path")
        .and_then(|v| v.as_str())
        .context("Missing file_path")?;
    let path = PathBuf::from(path_str);

    let start_line = args
        .get("start_line")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(1);
    let end_line = args
        .get("end_line")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(usize::MAX);

    let repo = match git_helper::open_repo(&path) {
        Ok(repo) => repo,
        Err(e) => {
            tracing::warn!("libgit2 failed ({}), falling back to git CLI", e);
            return execute_cli(&path, start_line, end_line).await;
        }
    };

    let relative_path = repo_relative_path(&repo, &path);
    let mut opts = BlameOptions::new();
    let blame = repo.blame_file(&relative_path, Some(&mut opts))?;

    let mut blame_lines = Vec::new();
    for hunk in blame.iter() {
        let hunk_start = hunk.final_start_line();
        let hunk_end = hunk_start + hunk.lines_in_hunk() - 1;
        if hunk_end < start_line || hunk_start > end_line {
            continue;
        }

        let line_start = hunk_start.max(start_line);
        let line_end = hunk_end.min(end_line);
        let commit_hash = hunk.final_commit_id().to_string();
        let sig = hunk.final_signature();
        let author = format!(
            "{} <{}>",
            sig.name().unwrap_or("Unknown"),
            sig.email().unwrap_or("Unknown")
        );
        let timestamp = sig.when().seconds();

        for line in line_start..=line_end {
            blame_lines.push(BlameLine {
                line,
                author: author.clone(),
                commit: commit_hash.clone(),
                timestamp,
            });
        }
    }

    build_response(path_str, blame_lines)
}

async fn execute_cli(path: &Path, start_line: usize, end_line: usize) -> Result<Value> {
    let mut args = vec!["blame", "--line-porcelain"];

    let line_str;
    if start_line > 1 || end_line < usize::MAX {
        let end = if end_line == usize::MAX {
            String::new()
        } else {
            end_line.to_string()
        };
        line_str = format!("{},{}", start_line, end);
        args.push("-L");
        args.push(&line_str);
    }

    let path_str = path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string();
    args.push(&path_str);

    let parent_dir = path.parent().unwrap_or(path);
    let output = tokio::process::Command::new("git")
        .current_dir(parent_dir)
        .args(&args)
        .output()
        .await
        .context("Failed to execute git blame")?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut blame_lines = Vec::new();
    let mut current_commit = String::new();
    let mut current_author = String::from("Unknown");
    let mut current_email = String::from("Unknown");
    let mut current_time = 0i64;
    let mut current_line = 0usize;

    for line in stdout.lines() {
        if line.starts_with('\t') {
            blame_lines.push(BlameLine {
                line: current_line,
                author: format!(
                    "{} <{}>",
                    current_author,
                    current_email.trim_matches('<').trim_matches('>')
                ),
                commit: current_commit.clone(),
                timestamp: current_time,
            });
            continue;
        }

        if current_commit.is_empty() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                current_commit = parts[0].to_string();
                current_line = parts[2].parse::<usize>().unwrap_or(0);
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("author ") {
            current_author = value.to_string();
        } else if let Some(value) = line.strip_prefix("author-mail ") {
            current_email = value.to_string();
        } else if let Some(value) = line.strip_prefix("author-time ") {
            current_time = value.parse::<i64>().unwrap_or_default();
        } else if line.len() >= 40 && line.as_bytes()[40] == b' ' {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 {
                current_commit = parts[0].to_string();
                current_line = parts[2].parse::<usize>().unwrap_or(0);
                current_author = String::from("Unknown");
                current_email = String::from("Unknown");
                current_time = 0;
            }
        }
    }

    build_response(&path.to_string_lossy(), blame_lines)
}

fn build_response(path: &str, blame_lines: Vec<BlameLine>) -> Result<Value> {
    let total_lines = blame_lines.len();
    let blame_hunks = build_hunks(&blame_lines);
    let blame_lines = blame_lines
        .into_iter()
        .map(|line| {
            json!({
                "line": line.line,
                "author": line.author,
                "commit": line.commit,
                "timestamp": line.timestamp
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "file_path": path,
        "total_lines": total_lines,
        "blame_lines": blame_lines,
        "blame_hunks": blame_hunks
    }))
}

fn build_hunks(blame_lines: &[BlameLine]) -> Vec<Value> {
    let mut hunks = Vec::new();
    let mut start = 0usize;

    while start < blame_lines.len() {
        let first = &blame_lines[start];
        let mut end = start;

        while end + 1 < blame_lines.len() {
            let next = &blame_lines[end + 1];
            let is_same_hunk = next.author == first.author
                && next.commit == first.commit
                && next.timestamp == first.timestamp
                && next.line == blame_lines[end].line + 1;

            if !is_same_hunk {
                break;
            }
            end += 1;
        }

        hunks.push(json!({
            "start_line": first.line,
            "end_line": blame_lines[end].line,
            "line_count": end - start + 1,
            "author": first.author,
            "commit": first.commit,
            "timestamp": first.timestamp
        }));

        start = end + 1;
    }

    hunks
}
