use anyhow::{Context, Result};
use git2::Repository;
use std::path::Path;

/// Open git repo with fallback strategies:
/// 1. discover() - standard
/// 2. open_ext() with ceiling dirs disabled - bypass safe.directory
/// 3. open() - direct
pub fn open_repo(path: &Path) -> Result<Repository> {
    let path_str = path.to_string_lossy();

    // Strategy 1: standard discover
    if let Ok(repo) = Repository::discover(path) {
        return Ok(repo);
    }

    // Strategy 2: open_ext with no flags - skip safe.directory check
    if let Ok(repo) = Repository::open_ext(path, git2::RepositoryOpenFlags::empty(), &[] as &[&str])
    {
        return Ok(repo);
    }

    // Strategy 3: Try .git subdir directly
    let git_dir = path.join(".git");
    if git_dir.exists()
        && let Ok(repo) = Repository::open(&git_dir)
    {
        return Ok(repo);
    }

    // Walk up to find .git
    let mut current = path.to_path_buf();
    for _ in 0..10 {
        let git_dir = current.join(".git");
        if git_dir.exists()
            && let Ok(repo) = Repository::open(&git_dir)
        {
            return Ok(repo);
        }
        if !current.pop() {
            break;
        }
    }

    Err(anyhow::anyhow!(
        "Cannot open git repo at '{}'. Tried discover(), open_ext(), and manual .git lookup. Run `git -C \"{}\" status` to verify.",
        path_str,
        path_str
    ))
}

pub async fn execute_git_cli(repo_path: &Path, args: &[&str]) -> Result<String> {
    let output = tokio::process::Command::new("git")
        .current_dir(repo_path)
        .args(args)
        .output()
        .await
        .context("Failed to spawn git process")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("git CLI failed: {}", stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}
