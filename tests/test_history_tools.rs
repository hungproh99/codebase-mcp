#![allow(clippy::await_holding_lock)]

use codebase_mcp::history::clear_history;
use codebase_mcp::tools::{
    batch_tool_call, convert_file_format, create_directory, create_file, delete_file, edit_file,
    history_status, redo_last_change, undo_last_change,
};
use serde_json::json;
use std::fs;
use std::sync::{LazyLock, Mutex, MutexGuard};
use tempfile::tempdir;

static TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn lock_history() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap()
}

async fn current_status() -> serde_json::Value {
    history_status::execute(&json!({})).await.unwrap()
}

#[tokio::test]
async fn test_create_file_undo_redo_flow() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("note.txt");
    let create = create_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "content": "hello"
    }))
    .await
    .unwrap();

    assert_eq!(
        create.get("history_recorded").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert!(
        create
            .get("history_entry_id")
            .and_then(|v| v.as_str())
            .is_some()
    );

    let status = current_status().await;
    assert_eq!(status.get("undo_depth").and_then(|v| v.as_u64()), Some(1));
    assert_eq!(status.get("redo_depth").and_then(|v| v.as_u64()), Some(0));

    let undo = undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(undo.get("success").and_then(|v| v.as_bool()), Some(true));
    assert!(!path.exists());

    let redo = redo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(redo.get("success").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(fs::read_to_string(&path).unwrap(), "hello");
}

#[tokio::test]
async fn test_edit_file_undo_redo_flow() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("edit.txt");
    fs::write(&path, "before").unwrap();

    let edit = edit_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "mode": "append",
        "content": " after"
    }))
    .await
    .unwrap();

    assert_eq!(
        edit.get("history_recorded").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(fs::read_to_string(&path).unwrap(), "before after");

    undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "before");

    redo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "before after");
}

#[tokio::test]
async fn test_delete_file_undo_redo_flow() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("gone.txt");
    fs::write(&path, "delete me").unwrap();

    let delete = delete_file::execute(&json!({
        "path": path.to_str().unwrap()
    }))
    .await
    .unwrap();

    assert_eq!(
        delete.get("history_recorded").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert!(!path.exists());

    undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), "delete me");

    redo_last_change::execute(&json!({})).await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn test_convert_file_format_undo_redo_flow() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("format.txt");
    let original_bytes = b"alpha\r\nbeta\r\n".to_vec();
    fs::write(&path, &original_bytes).unwrap();

    let convert = convert_file_format::execute(&json!({
        "path": path.to_str().unwrap(),
        "target_encoding": "UTF-16LE",
        "target_line_ending": "lf"
    }))
    .await
    .unwrap();

    assert_eq!(
        convert.get("history_recorded").and_then(|v| v.as_bool()),
        Some(true)
    );
    let converted_bytes = fs::read(&path).unwrap();
    assert!(converted_bytes.starts_with(&[0xFF, 0xFE]));
    assert_ne!(converted_bytes, original_bytes);

    undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), original_bytes);

    redo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(fs::read(&path).unwrap(), converted_bytes);
}

#[tokio::test]
async fn test_create_directory_undo_respects_non_empty_safety() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("folder");

    let create = create_directory::execute(&json!({
        "path": path.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(
        create.get("history_recorded").and_then(|v| v.as_bool()),
        Some(true)
    );

    fs::write(path.join("child.txt"), "nested").unwrap();

    let undo = undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(undo.get("success").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(
        undo.get("error_code").and_then(|v| v.as_str()),
        Some("undo_failed")
    );
    assert!(
        undo.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("directory_not_empty")
    );

    let undo_force = undo_last_change::execute(&json!({ "force": true }))
        .await
        .unwrap();
    assert_eq!(
        undo_force.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert!(path.exists());

    fs::remove_file(path.join("child.txt")).unwrap();
    let undo_ok = undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(undo_ok.get("success").and_then(|v| v.as_bool()), Some(true));
    assert!(!path.exists());

    let redo = redo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(redo.get("success").and_then(|v| v.as_bool()), Some(true));
    assert!(path.exists());
}

#[tokio::test]
async fn test_drift_requires_force_for_file_undo() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let path = dir.path().join("drift.txt");

    create_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "content": "stable"
    }))
    .await
    .unwrap();

    fs::write(&path, "drifted").unwrap();

    let undo = undo_last_change::execute(&json!({})).await.unwrap();
    assert_eq!(undo.get("success").and_then(|v| v.as_bool()), Some(false));
    assert!(
        undo.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("expected history snapshot")
    );

    let undo_force = undo_last_change::execute(&json!({ "force": true }))
        .await
        .unwrap();
    assert_eq!(
        undo_force.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert!(!path.exists());
}

#[tokio::test]
async fn test_batch_history_and_history_skip_for_oversized_snapshot() {
    let _guard = lock_history();
    clear_history();

    let dir = tempdir().unwrap();
    let first = dir.path().join("a.txt");
    let second = dir.path().join("b.txt");

    let batch = batch_tool_call::execute(&json!({
        "calls": [
            {
                "tool": "create_file",
                "args": { "path": first.to_str().unwrap(), "content": "one" }
            },
            {
                "tool": "create_file",
                "args": { "path": second.to_str().unwrap(), "content": "two" }
            }
        ]
    }))
    .await
    .unwrap();

    assert_eq!(batch.get("total").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(
        batch["results"][0]["result"]["history_recorded"].as_bool(),
        Some(true)
    );
    assert_eq!(
        batch["results"][1]["result"]["history_recorded"].as_bool(),
        Some(true)
    );

    let status = current_status().await;
    assert_eq!(status.get("undo_depth").and_then(|v| v.as_u64()), Some(2));

    let huge_path = dir.path().join("huge.txt");
    let huge = "x".repeat(10 * 1024 * 1024 + 1);
    let create = create_file::execute(&json!({
        "path": huge_path.to_str().unwrap(),
        "content": huge
    }))
    .await
    .unwrap();

    assert_eq!(create.get("success").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        create.get("history_recorded").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert!(
        create
            .get("history_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("history snapshot limit")
    );
}
