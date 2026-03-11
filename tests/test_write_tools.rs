use codebase_mcp::tools::{create_file, delete_file, edit_file};
use serde_json::json;
use tempfile::tempdir;

#[tokio::test]
async fn test_create_file_create_and_overwrite_flow() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested").join("note.txt");

    let create_res = create_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "content": "hello"
    }))
    .await
    .unwrap();

    assert_eq!(
        create_res.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        create_res.get("created").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");

    let exists_res = create_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "content": "second"
    }))
    .await
    .unwrap();

    assert_eq!(
        exists_res.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        exists_res.get("error_code").and_then(|v| v.as_str()),
        Some("already_exists")
    );
    assert!(exists_res.get("reason").and_then(|v| v.as_str()).is_some());

    let overwrite_res = create_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "content": "second",
        "overwrite": true
    }))
    .await
    .unwrap();

    assert_eq!(
        overwrite_res.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        overwrite_res.get("overwritten").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "second");
}

#[tokio::test]
async fn test_edit_file_find_replace_and_missing_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("edit.txt");
    std::fs::write(&path, "alpha beta beta").unwrap();

    let replace_res = edit_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "mode": "find_replace",
        "find": "beta",
        "replace": "B",
        "replace_all": true,
        "expected_replacements": 2
    }))
    .await
    .unwrap();

    assert_eq!(
        replace_res.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        replace_res.get("replacements").and_then(|v| v.as_u64()),
        Some(2)
    );
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "alpha B B");

    let no_match_res = edit_file::execute(&json!({
        "path": path.to_str().unwrap(),
        "mode": "find_replace",
        "find": "gamma",
        "replace": "X"
    }))
    .await
    .unwrap();

    assert_eq!(
        no_match_res.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        no_match_res.get("error_code").and_then(|v| v.as_str()),
        Some("no_match")
    );
    assert!(
        no_match_res
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some()
    );

    let missing_path = dir.path().join("missing.txt");
    let missing_res = edit_file::execute(&json!({
        "path": missing_path.to_str().unwrap(),
        "mode": "replace",
        "content": "new"
    }))
    .await
    .unwrap();

    assert_eq!(
        missing_res.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        missing_res.get("error_code").and_then(|v| v.as_str()),
        Some("file_not_found")
    );
    assert!(missing_res.get("reason").and_then(|v| v.as_str()).is_some());

    let create_missing_res = edit_file::execute(&json!({
        "path": missing_path.to_str().unwrap(),
        "mode": "replace",
        "content": "new",
        "create_if_missing": true
    }))
    .await
    .unwrap();

    assert_eq!(
        create_missing_res.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        create_missing_res
            .get("file_created")
            .and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(std::fs::read_to_string(&missing_path).unwrap(), "new");
}

#[tokio::test]
async fn test_delete_file_success_missing_and_directory_case() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("to-delete.txt");
    std::fs::write(&file_path, "delete me").unwrap();

    let delete_ok = delete_file::execute(&json!({
        "path": file_path.to_str().unwrap()
    }))
    .await
    .unwrap();

    assert_eq!(
        delete_ok.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        delete_ok.get("deleted").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert!(!file_path.exists());

    let delete_missing = delete_file::execute(&json!({
        "path": file_path.to_str().unwrap()
    }))
    .await
    .unwrap();

    assert_eq!(
        delete_missing.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        delete_missing.get("error_code").and_then(|v| v.as_str()),
        Some("file_not_found")
    );
    assert!(
        delete_missing
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some()
    );

    let delete_missing_ok = delete_file::execute(&json!({
        "path": file_path.to_str().unwrap(),
        "missing_ok": true
    }))
    .await
    .unwrap();

    assert_eq!(
        delete_missing_ok.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        delete_missing_ok.get("deleted").and_then(|v| v.as_bool()),
        Some(false)
    );

    let dir_path = dir.path().join("folder");
    std::fs::create_dir_all(&dir_path).unwrap();
    let delete_dir = delete_file::execute(&json!({
        "path": dir_path.to_str().unwrap()
    }))
    .await
    .unwrap();

    assert_eq!(
        delete_dir.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        delete_dir.get("error_code").and_then(|v| v.as_str()),
        Some("path_is_directory")
    );
    assert!(delete_dir.get("reason").and_then(|v| v.as_str()).is_some());
}
