use anyhow::{Context, Result};
use git2::DiffOptions;
use serde_json::{Value, json};
use std::path::PathBuf;

use crate::common::repo_root_string;

use super::git_helper;

pub fn schema() -> Value {
    json!({
        "name": "git_diff",
        "description": "Return repository diff output for staged or unstaged changes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "repo_path": { "type": "string" },
                "staged": { "type": "boolean" },
                "context_lines": { "type": "integer" }
            },
            "required": ["repo_path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("repo_path")
        .and_then(|v| v.as_str())
        .context("Missing repo_path")?;
    let path = PathBuf::from(path_str);
    let staged = args
        .get("staged")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let context_lines = args
        .get("context_lines")
        .and_then(|v| v.as_u64())
        .unwrap_or(3) as u32;

    let repo = match git_helper::open_repo(&path) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("libgit2 failed ({}), falling back to git CLI", e);
            return execute_cli(&path, staged, context_lines).await;
        }
    };

    let mut opts = DiffOptions::new();
    opts.context_lines(context_lines);
    opts.include_untracked(true);

    let diff = if staged {
        let tree = repo.head()?.peel_to_tree()?;
        repo.diff_tree_to_index(Some(&tree), None, Some(&mut opts))?
    } else {
        repo.diff_index_to_workdir(None, Some(&mut opts))?
    };

    let mut hunks_output = Vec::new();

    let _ = diff.print(git2::DiffFormat::Patch, |_delta, _hunk, line| {
        let content = String::from_utf8_lossy(line.content()).to_string();
        let prefix = match line.origin() {
            '+' | '-' | ' ' => line.origin().to_string(),
            _ => String::new(),
        };

        hunks_output.push(format!("{}{}", prefix, content));
        true
    });

    let mut raw_diff = hunks_output.join("");
    let mut truncated = false;
    const MAX_DIFF_BYTES: usize = 100 * 1024;
    if raw_diff.len() > MAX_DIFF_BYTES {
        raw_diff.truncate(MAX_DIFF_BYTES);
        raw_diff.push_str("\n... [TRUNCATED - diff exceeds 100KB] ...");
        truncated = true;
    }

    Ok(json!({
        "repository": repo_root_string(&repo),
        "is_staged": staged,
        "diff_patch": raw_diff,
        "truncated": truncated
    }))
}

async fn execute_cli(path: &std::path::Path, staged: bool, context_lines: u32) -> Result<Value> {
    let toplevel = git_helper::execute_git_cli(path, &["rev-parse", "--show-toplevel"]).await?;
    let repository = toplevel.trim().to_string();

    let mut args = vec!["diff"];
    if staged {
        args.push("--staged");
    }

    let ctx_str = format!("-U{}", context_lines);
    args.push(&ctx_str);

    let output = tokio::process::Command::new("git")
        .current_dir(path)
        .args(&args)
        .output()
        .await
        .context("Failed to execute git diff")?;

    let mut raw_diff = String::from_utf8_lossy(&output.stdout).into_owned();
    let mut truncated = false;
    const MAX_DIFF_BYTES: usize = 100 * 1024;

    if raw_diff.len() > MAX_DIFF_BYTES {
        raw_diff.truncate(MAX_DIFF_BYTES);
        raw_diff.push_str("\n... [TRUNCATED - diff exceeds 100KB] ...");
        truncated = true;
    }

    Ok(json!({
        "repository": repository,
        "is_staged": staged,
        "diff_patch": raw_diff,
        "truncated": truncated
    }))
}
