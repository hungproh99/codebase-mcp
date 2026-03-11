use anyhow::{Context, Result};
use git2::{DiffOptions, Sort};
use serde_json::{Value, json};
use std::path::PathBuf;

use crate::common::repo_root_string;

use super::git_helper;

pub fn schema() -> Value {
    json!({
        "name": "git_log",
        "description": "List repository commit history.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": { "type": "string" },
                "max_count": { "type": "integer" },
                "since": { "type": "integer" }
            },
            "required": ["repo_path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("repo_path")
        .and_then(|v| v.as_str())
        .context("Missing/empty repo_path")?;
    let path = PathBuf::from(path_str);
    let max_count = args.get("max_count").and_then(|v| v.as_u64()).unwrap_or(10) as usize;
    let since = args.get("since").and_then(|v| v.as_i64()).unwrap_or(0);

    let repo = match git_helper::open_repo(&path) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("libgit2 failed ({}), falling back to git CLI", e);
            return execute_cli(&path, max_count, since).await;
        }
    };

    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    revwalk.set_sorting(Sort::TIME)?;

    let mut logs = Vec::new();

    for id in revwalk {
        if logs.len() >= max_count {
            break;
        }

        if let Ok(oid) = id
            && let Ok(commit) = repo.find_commit(oid)
        {
            let time = commit.time().seconds();
            if since > 0 && time < since {
                break;
            }

            let author = commit.author().name().unwrap_or("Unknown").to_string();
            let email = commit.author().email().unwrap_or("Unknown").to_string();
            let msg = commit.message().unwrap_or("").to_string();
            let files_changed = count_files_changed(&repo, &commit);

            logs.push(json!({
                "hash": oid.to_string(),
                "author": format!("{} <{}>", author, email),
                "timestamp": time,
                "message": msg.trim_end(),
                "files_changed": files_changed
            }));
        }
    }

    Ok(json!({
        "repository": repo_root_string(&repo),
        "total_returned": logs.len(),
        "commits": logs
    }))
}

fn count_files_changed(repo: &git2::Repository, commit: &git2::Commit) -> usize {
    let tree = match commit.tree() {
        Ok(t) => t,
        Err(_) => return 0,
    };

    let parent_commit = commit.parent(0).ok();
    let parent_tree = parent_commit
        .as_ref()
        .and_then(|p: &git2::Commit| p.tree().ok());

    let mut opts = DiffOptions::new();
    let diff = repo.diff_tree_to_tree(parent_tree.as_ref(), Some(&tree), Some(&mut opts));

    match diff {
        Ok(d) => d.deltas().count(),
        Err(_) => 0,
    }
}

async fn execute_cli(path: &std::path::Path, max_count: usize, since: i64) -> Result<Value> {
    let toplevel = git_helper::execute_git_cli(path, &["rev-parse", "--show-toplevel"]).await?;
    let repository = toplevel.trim().to_string();

    let mut args = vec![
        "log",
        "--name-status",
        "--format=COMMIT|||%H|||%an <%ae>|||%ct|||%s",
    ];
    let max_str = format!("-n{}", max_count);
    args.push(&max_str);

    let since_str;
    if since > 0 {
        since_str = format!("--since={}", since);
        args.push(&since_str);
    }

    let log_out = git_helper::execute_git_cli(path, &args).await?;

    let mut logs = Vec::new();
    let mut current_commit = None;
    let mut files_changed = 0;

    let push_current = |commit: &mut Option<Value>, files: &mut usize, logs: &mut Vec<Value>| {
        if let Some(mut c) = commit.take() {
            if let Some(obj) = c.as_object_mut() {
                obj.insert("files_changed".to_string(), json!(*files));
            }
            logs.push(c);
        }
        *files = 0;
    };

    for line in log_out.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(payload) = line.strip_prefix("COMMIT|||") {
            push_current(&mut current_commit, &mut files_changed, &mut logs);
            let parts: Vec<&str> = payload.splitn(4, "|||").collect();
            if parts.len() == 4 {
                current_commit = Some(json!({
                    "hash": parts[0],
                    "author": parts[1],
                    "timestamp": parts[2].parse::<i64>().unwrap_or_default(),
                    "message": parts[3]
                }));
            }
            continue;
        }

        files_changed += 1;
    }

    push_current(&mut current_commit, &mut files_changed, &mut logs);

    Ok(json!({
        "repository": repository,
        "total_returned": logs.len(),
        "commits": logs
    }))
}
