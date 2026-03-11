use anyhow::{Context, Result};
use lazy_static::lazy_static;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::common::ensure_object;
use crate::security::path_guard::{GUARD, Tier};
use crate::tools::read_file::decode_fuzzy;

const MAX_HISTORY_ENTRIES: usize = 200;
pub const MAX_TRACKED_SNAPSHOT_BYTES: usize = 10 * 1024 * 1024;

lazy_static! {
    static ref HISTORY_STATE: Mutex<HistoryState> = Mutex::new(HistoryState::default());
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotState {
    Missing,
    File,
    Directory,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathSnapshot {
    pub state: SnapshotState,
    pub bytes: Option<Vec<u8>>,
    pub encoding_label: Option<String>,
    pub line_ending: Option<String>,
}

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub entry_id: String,
    pub tool_name: String,
    pub path: String,
    pub kind: String,
    pub timestamp_unix: u64,
    pub before: PathSnapshot,
    pub after: PathSnapshot,
    pub summary: String,
}

#[derive(Default)]
struct HistoryState {
    undo_stack: Vec<HistoryEntry>,
    redo_stack: Vec<HistoryEntry>,
    next_entry_id: u64,
}

#[derive(Clone, Debug)]
pub struct RecordOutcome {
    pub recorded: bool,
    pub entry_id: Option<String>,
    pub reason: Option<String>,
}

pub struct ApplyOutcome {
    pub entry: HistoryEntry,
    pub undo_depth: usize,
    pub redo_depth: usize,
}

pub fn missing_snapshot() -> PathSnapshot {
    PathSnapshot {
        state: SnapshotState::Missing,
        bytes: None,
        encoding_label: None,
        line_ending: None,
    }
}

pub fn directory_snapshot() -> PathSnapshot {
    PathSnapshot {
        state: SnapshotState::Directory,
        bytes: None,
        encoding_label: None,
        line_ending: None,
    }
}

pub fn file_snapshot(
    bytes: Vec<u8>,
    encoding_label: Option<String>,
    line_ending: Option<String>,
) -> PathSnapshot {
    PathSnapshot {
        state: SnapshotState::File,
        bytes: Some(bytes),
        encoding_label,
        line_ending,
    }
}

pub fn capture_snapshot(path: &Path) -> Result<PathSnapshot, String> {
    if !path.exists() {
        return Ok(missing_snapshot());
    }

    if path.is_dir() {
        return Ok(directory_snapshot());
    }

    let metadata = fs::metadata(path).map_err(|err| format!("read metadata failed: {}", err))?;
    if metadata.len() as usize > MAX_TRACKED_SNAPSHOT_BYTES {
        return Err(format!(
            "file exceeds history snapshot limit ({} bytes > {} bytes)",
            metadata.len(),
            MAX_TRACKED_SNAPSHOT_BYTES
        ));
    }

    let bytes = fs::read(path).map_err(|err| format!("read file failed: {}", err))?;
    let (encoding_label, line_ending) = infer_text_metadata(&bytes);
    Ok(file_snapshot(bytes, encoding_label, line_ending))
}

fn infer_text_metadata(bytes: &[u8]) -> (Option<String>, Option<String>) {
    let (_, encoding) = decode_fuzzy(bytes);
    let text = String::from_utf8_lossy(bytes);
    let line_ending = if text.contains("\r\n") {
        Some("crlf".to_string())
    } else if text.contains('\n') {
        Some("lf".to_string())
    } else {
        None
    };

    (
        Some(normalize_encoding_label(encoding).to_string()),
        line_ending,
    )
}

fn normalize_encoding_label(raw: &str) -> &'static str {
    if raw.eq_ignore_ascii_case("utf-16le") {
        "UTF-16LE"
    } else if raw.eq_ignore_ascii_case("utf-16be") {
        "UTF-16BE"
    } else if raw.eq_ignore_ascii_case("windows-1252") {
        "WINDOWS-1252"
    } else {
        "UTF-8"
    }
}

pub fn record_change(
    tool_name: &str,
    path: &Path,
    before: PathSnapshot,
    after: PathSnapshot,
    summary: impl Into<String>,
) -> RecordOutcome {
    let kind = entry_kind(&before, &after);

    if let Some(reason) = untrackable_reason(&before).or_else(|| untrackable_reason(&after)) {
        return RecordOutcome {
            recorded: false,
            entry_id: None,
            reason: Some(reason),
        };
    }

    let mut guard = match HISTORY_STATE.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return RecordOutcome {
                recorded: false,
                entry_id: None,
                reason: Some("history state is unavailable".to_string()),
            };
        }
    };

    guard.redo_stack.clear();
    let entry_id = format!("h{}", guard.next_entry_id);
    guard.next_entry_id += 1;

    guard.undo_stack.push(HistoryEntry {
        entry_id: entry_id.clone(),
        tool_name: tool_name.to_string(),
        path: path.to_string_lossy().to_string(),
        kind: kind.to_string(),
        timestamp_unix: current_unix_timestamp(),
        before,
        after,
        summary: summary.into(),
    });

    if guard.undo_stack.len() > MAX_HISTORY_ENTRIES {
        let overflow = guard.undo_stack.len() - MAX_HISTORY_ENTRIES;
        guard.undo_stack.drain(0..overflow);
    }

    RecordOutcome {
        recorded: true,
        entry_id: Some(entry_id),
        reason: None,
    }
}

fn untrackable_reason(snapshot: &PathSnapshot) -> Option<String> {
    match (&snapshot.state, snapshot.bytes.as_ref()) {
        (SnapshotState::File, Some(bytes)) if bytes.len() > MAX_TRACKED_SNAPSHOT_BYTES => {
            Some(format!(
                "file exceeds history snapshot limit ({} bytes > {} bytes)",
                bytes.len(),
                MAX_TRACKED_SNAPSHOT_BYTES
            ))
        }
        (SnapshotState::File, None) => Some("file snapshot missing bytes".to_string()),
        _ => None,
    }
}

fn entry_kind(before: &PathSnapshot, after: &PathSnapshot) -> &'static str {
    if before.state == SnapshotState::Directory || after.state == SnapshotState::Directory {
        "directory"
    } else {
        "file"
    }
}

pub fn attach_history_metadata(response: &mut Value, outcome: &RecordOutcome) {
    let object = ensure_object(response);
    object.insert("history_recorded".to_string(), json!(outcome.recorded));
    object.insert("history_entry_id".to_string(), json!(outcome.entry_id));
    object.insert("history_reason".to_string(), json!(outcome.reason));
}

pub fn no_history(reason: impl Into<String>) -> RecordOutcome {
    RecordOutcome {
        recorded: false,
        entry_id: None,
        reason: Some(reason.into()),
    }
}

pub fn status_json() -> Value {
    let guard = match HISTORY_STATE.lock() {
        Ok(guard) => guard,
        Err(_) => {
            return json!({
                "undo_depth": 0,
                "redo_depth": 0,
                "next_undo": Value::Null,
                "next_redo": Value::Null,
                "error": "history state is unavailable"
            });
        }
    };
    json!({
        "undo_depth": guard.undo_stack.len(),
        "redo_depth": guard.redo_stack.len(),
        "next_undo": guard.undo_stack.last().map(entry_summary),
        "next_redo": guard.redo_stack.last().map(entry_summary)
    })
}

pub fn undo_last(force: bool) -> Result<ApplyOutcome, String> {
    let entry = {
        let mut guard = HISTORY_STATE
            .lock()
            .map_err(|_| "history state is unavailable".to_string())?;
        guard
            .undo_stack
            .pop()
            .ok_or_else(|| "undo stack is empty".to_string())?
    };

    match apply_history_entry(&entry, &entry.after, &entry.before, force) {
        Ok(()) => {
            let mut guard = HISTORY_STATE
                .lock()
                .map_err(|_| "history state is unavailable".to_string())?;
            guard.redo_stack.push(entry.clone());
            let outcome = ApplyOutcome {
                entry,
                undo_depth: guard.undo_stack.len(),
                redo_depth: guard.redo_stack.len(),
            };
            Ok(outcome)
        }
        Err(err) => {
            let mut guard = HISTORY_STATE
                .lock()
                .map_err(|_| "history state is unavailable".to_string())?;
            guard.undo_stack.push(entry);
            Err(err)
        }
    }
}

pub fn redo_last(force: bool) -> Result<ApplyOutcome, String> {
    let entry = {
        let mut guard = HISTORY_STATE
            .lock()
            .map_err(|_| "history state is unavailable".to_string())?;
        guard
            .redo_stack
            .pop()
            .ok_or_else(|| "redo stack is empty".to_string())?
    };

    match apply_history_entry(&entry, &entry.before, &entry.after, force) {
        Ok(()) => {
            let mut guard = HISTORY_STATE
                .lock()
                .map_err(|_| "history state is unavailable".to_string())?;
            guard.undo_stack.push(entry.clone());
            let outcome = ApplyOutcome {
                entry,
                undo_depth: guard.undo_stack.len(),
                redo_depth: guard.redo_stack.len(),
            };
            Ok(outcome)
        }
        Err(err) => {
            let mut guard = HISTORY_STATE
                .lock()
                .map_err(|_| "history state is unavailable".to_string())?;
            guard.redo_stack.push(entry);
            Err(err)
        }
    }
}

fn apply_history_entry(
    entry: &HistoryEntry,
    expected_current: &PathSnapshot,
    target: &PathSnapshot,
    force: bool,
) -> Result<(), String> {
    let path = PathBuf::from(&entry.path);
    let (canonical_from_guard, tier, reason) = GUARD.check_path(&path);
    if tier == Tier::Blocked {
        return Err(reason.unwrap_or_else(|| {
            format!(
                "path is blocked by server policy: {}",
                canonical_from_guard.to_string_lossy()
            )
        }));
    }

    if !force || entry.kind != "file" {
        let current = capture_snapshot(&path)?;
        if current != *expected_current {
            return Err(
                "current filesystem state does not match expected history snapshot".to_string(),
            );
        }
    }

    apply_snapshot(&path, target)
}

fn apply_snapshot(path: &Path, target: &PathSnapshot) -> Result<(), String> {
    match target.state {
        SnapshotState::Missing => apply_missing(path),
        SnapshotState::Directory => apply_directory(path),
        SnapshotState::File => apply_file(path, target),
    }
}

fn apply_missing(path: &Path) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_dir() {
        fs::remove_dir(path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::DirectoryNotEmpty {
                "directory_not_empty".to_string()
            } else {
                format!("failed to remove directory: {}", err)
            }
        })?;
        return Ok(());
    }

    fs::remove_file(path).map_err(|err| format!("failed to remove file: {}", err))
}

fn apply_directory(path: &Path) -> Result<(), String> {
    if path.exists() {
        if path.is_dir() {
            return Ok(());
        }
        return Err("path_conflict: target path is a file".to_string());
    }

    fs::create_dir_all(path).map_err(|err| format!("failed to create directory: {}", err))
}

fn apply_file(path: &Path, target: &PathSnapshot) -> Result<(), String> {
    if path.exists() && path.is_dir() {
        return Err("path_conflict: target path is a directory".to_string());
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|err| format!("failed to create parent directories: {}", err))?;
    }

    let bytes = target
        .bytes
        .as_ref()
        .context("target file snapshot is missing bytes")
        .map_err(|err| err.to_string())?;

    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .map_err(|err| format!("failed to open file for history apply: {}", err))?;
    file.write_all(bytes)
        .map_err(|err| format!("failed to write file for history apply: {}", err))
}

fn entry_summary(entry: &HistoryEntry) -> Value {
    json!({
        "entry_id": entry.entry_id,
        "tool_name": entry.tool_name,
        "path": entry.path,
        "kind": entry.kind,
        "timestamp_unix": entry.timestamp_unix,
        "summary": entry.summary
    })
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[allow(dead_code)]
pub fn clear_history() {
    if let Ok(mut guard) = HISTORY_STATE.lock() {
        guard.undo_stack.clear();
        guard.redo_stack.clear();
        guard.next_entry_id = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn record_change_clears_redo_and_limits_stack() {
        clear_history();
        let dir = tempdir().unwrap();
        let path = dir.path().join("sample.txt");
        let before = missing_snapshot();
        let after = file_snapshot(
            b"hello".to_vec(),
            Some("UTF-8".to_string()),
            Some("lf".to_string()),
        );

        let first = record_change(
            "create_file",
            &path,
            before.clone(),
            after.clone(),
            "create",
        );
        assert!(first.recorded);

        let _ = undo_last(true).unwrap();
        assert_eq!(
            status_json().get("redo_depth").and_then(|v| v.as_u64()),
            Some(1)
        );

        let second = record_change("create_file", &path, before, after, "create again");
        assert!(second.recorded);
        assert_eq!(
            status_json().get("redo_depth").and_then(|v| v.as_u64()),
            Some(0)
        );
    }

    #[test]
    fn oversized_snapshot_is_skipped() {
        clear_history();
        let path = PathBuf::from("large.txt");
        let oversized = vec![b'x'; MAX_TRACKED_SNAPSHOT_BYTES + 1];
        let outcome = record_change(
            "create_file",
            &path,
            missing_snapshot(),
            file_snapshot(oversized, Some("UTF-8".to_string()), Some("lf".to_string())),
            "large write",
        );
        assert!(!outcome.recorded);
        assert!(outcome.reason.unwrap().contains("history snapshot limit"));
    }

    #[test]
    fn undo_redo_restores_file_state() {
        clear_history();
        let dir = tempdir().unwrap();
        let path = dir.path().join("undo.txt");
        fs::write(&path, "before").unwrap();

        let before = capture_snapshot(&path).unwrap();
        fs::write(&path, "after").unwrap();
        let after = capture_snapshot(&path).unwrap();

        let outcome = record_change("edit_file", &path, before.clone(), after, "edit");
        assert!(outcome.recorded);

        undo_last(false).unwrap();
        assert_eq!(fs::read(&path).unwrap(), before.bytes.unwrap());

        redo_last(false).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"after");
    }
}
