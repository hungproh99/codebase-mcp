use anyhow::{Context, Result};
use git2::StatusOptions;
use serde_json::{Value, json};
use std::path::PathBuf;

use crate::common::repo_root_string;

use super::git_helper;

pub fn schema() -> Value {
    json!({
        "name": "git_status",
        "description": "Return repository status, including staged, modified, and untracked files.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": { "type": "string" }
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

    let repo = match git_helper::open_repo(&path) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("libgit2 failed ({}), falling back to git CLI", e);
            return execute_cli(&path).await;
        }
    };

    let mut opts = StatusOptions::new();
    opts.include_untracked(true);

    let statuses = repo.statuses(Some(&mut opts))?;

    let mut staged = Vec::new();
    let mut modified = Vec::new();
    let mut untracked = Vec::new();

    for entry in statuses.iter() {
        let status = entry.status();
        if let Some(path) = entry.path() {
            if status.is_index_new()
                || status.is_index_modified()
                || status.is_index_deleted()
                || status.is_index_renamed()
            {
                staged.push(path.to_string());
            }
            if status.is_wt_modified() || status.is_wt_deleted() {
                modified.push(path.to_string());
            }
            if status.is_wt_new() {
                untracked.push(path.to_string());
            }
        }
    }

    let branch = repo
        .head()
        .ok()
        .and_then(|h| h.shorthand().map(|s| s.to_string()))
        .unwrap_or_else(|| "HEAD".to_string());

    Ok(json!({
        "repository": repo_root_string(&repo),
        "branch": branch,
        "staged": staged,
        "modified": modified,
        "untracked": untracked,
        "total_changes": staged.len() + modified.len() + untracked.len()
    }))
}

async fn execute_cli(path: &std::path::Path) -> Result<Value> {
    let toplevel = git_helper::execute_git_cli(path, &["rev-parse", "--show-toplevel"]).await?;
    let repository = toplevel.trim().to_string();

    let branch_out = git_helper::execute_git_cli(path, &["rev-parse", "--abbrev-ref", "HEAD"])
        .await
        .unwrap_or_else(|_| "HEAD".to_string());
    let branch = branch_out.trim().to_string();

    let status_out = git_helper::execute_git_cli(path, &["status", "--porcelain"]).await?;

    let mut staged = Vec::new();
    let mut modified = Vec::new();
    let mut untracked = Vec::new();

    for line in status_out.lines() {
        if line.len() >= 3 {
            let status = &line[0..2];
            let file_path = line[3..].trim().to_string();

            if status == "??" {
                untracked.push(file_path);
            } else {
                let mut is_staged = false;
                let c0 = status.chars().next().unwrap_or(' ');
                let c1 = status.chars().nth(1).unwrap_or(' ');

                if matches!(c0, 'M' | 'A' | 'D' | 'R' | 'C') {
                    staged.push(file_path.clone());
                    is_staged = true;
                }

                if matches!(c1, 'M' | 'D') && !is_staged {
                    modified.push(file_path);
                }
            }
        }
    }

    Ok(json!({
        "repository": repository,
        "branch": branch,
        "staged": staged,
        "modified": modified,
        "untracked": untracked,
        "total_changes": staged.len() + modified.len() + untracked.len()
    }))
}
