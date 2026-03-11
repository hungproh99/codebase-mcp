use git2::Repository;
use serde_json::{Map, Value};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn ensure_object(value: &mut Value) -> &mut Map<String, Value> {
    if !value.is_object() {
        *value = Value::Object(Map::new());
    }

    match value {
        Value::Object(object) => object,
        _ => unreachable!("value was normalized into an object"),
    }
}

pub fn insert_object_field(target: &mut Value, key: impl Into<String>, value: Value) {
    ensure_object(target).insert(key.into(), value);
}

pub fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn env_var(names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| std::env::var(name).ok())
}

pub fn env_var_os(names: &[&str]) -> Option<OsString> {
    names.iter().find_map(std::env::var_os)
}

pub fn repo_root(repo: &Repository) -> PathBuf {
    let root = repo
        .workdir()
        .map(Path::to_path_buf)
        .or_else(|| repo.path().parent().map(Path::to_path_buf))
        .unwrap_or_else(|| repo.path().to_path_buf());

    root.canonicalize().unwrap_or(root)
}

pub fn repo_root_string(repo: &Repository) -> String {
    repo_root(repo).to_string_lossy().into_owned()
}

pub fn repo_relative_path(repo: &Repository, path: &Path) -> PathBuf {
    let repo_root = repo_root(repo);
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    canonical_path
        .strip_prefix(&repo_root)
        .unwrap_or(path)
        .to_path_buf()
}
