use codebase_mcp::tools::{
    compare_symbols, create_directory, diff_two_snippets, extract_json_schema, find_json_paths,
    list_exports, list_imports, sqlite_inspect,
};
use rusqlite::Connection;
use serde_json::json;
use std::fs;
use tempfile::tempdir;

#[tokio::test]
async fn test_create_directory_supports_nested_creation_and_existing_behavior() {
    let dir = tempdir().unwrap();
    let target = dir.path().join("nested").join("leaf");

    let create_res = create_directory::execute(&json!({
        "path": target.to_str().unwrap()
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
    assert!(target.exists());

    let existing_res = create_directory::execute(&json!({
        "path": target.to_str().unwrap()
    }))
    .await
    .unwrap();
    assert_eq!(
        existing_res.get("success").and_then(|v| v.as_bool()),
        Some(true)
    );
    assert_eq!(
        existing_res.get("created").and_then(|v| v.as_bool()),
        Some(false)
    );

    let fail_existing_res = create_directory::execute(&json!({
        "path": target.to_str().unwrap(),
        "allow_existing": false
    }))
    .await
    .unwrap();
    assert_eq!(
        fail_existing_res.get("success").and_then(|v| v.as_bool()),
        Some(false)
    );
    assert_eq!(
        fail_existing_res.get("error_code").and_then(|v| v.as_str()),
        Some("already_exists")
    );
}

#[tokio::test]
async fn test_list_imports_and_exports_cover_typescript_and_rust() {
    let dir = tempdir().unwrap();
    let tsx_path = dir.path().join("widget.tsx");
    fs::write(
        &tsx_path,
        "import React from \"react\";\nimport type { Foo } from \"./types\";\nexport { Foo } from \"./types\";\nexport const answer = 42;\nexport default function App() { return <div />; }\n",
    )
    .unwrap();

    let rust_path = dir.path().join("mod.rs");
    fs::write(
        &rust_path,
        "use crate::inner::Thing;\npub use crate::inner::PublicThing;\npub struct Model;\npub fn run() {}\n",
    )
    .unwrap();

    let ts_imports = list_imports::execute(&json!({ "path": tsx_path.to_str().unwrap() }))
        .await
        .unwrap();
    assert_eq!(
        ts_imports.get("total_imports").and_then(|v| v.as_u64()),
        Some(2)
    );

    let first_ts_import = ts_imports
        .get("imports")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .cloned()
        .unwrap();
    assert_eq!(
        first_ts_import.get("source").and_then(|v| v.as_str()),
        Some("react")
    );

    let ts_exports = list_exports::execute(&json!({ "path": tsx_path.to_str().unwrap() }))
        .await
        .unwrap();
    assert_eq!(
        ts_exports.get("total_exports").and_then(|v| v.as_u64()),
        Some(3)
    );
    assert!(
        ts_exports
            .get("exports")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .any(|item| item.get("kind").and_then(|v| v.as_str()) == Some("reexport"))
    );

    let rust_imports = list_imports::execute(&json!({ "path": rust_path.to_str().unwrap() }))
        .await
        .unwrap();
    assert_eq!(
        rust_imports.get("total_imports").and_then(|v| v.as_u64()),
        Some(2)
    );

    let rust_exports = list_exports::execute(&json!({ "path": rust_path.to_str().unwrap() }))
        .await
        .unwrap();
    assert!(
        rust_exports
            .get("exports")
            .and_then(|v| v.as_array())
            .unwrap()
            .iter()
            .any(|item| item.get("name").and_then(|v| v.as_str()) == Some("run"))
    );
}

#[tokio::test]
async fn test_json_tools_extract_paths_and_schema() {
    let json_text = r#"{"user":{"name":"Ada","roles":["admin"],"active":true}}"#;

    let paths_res = find_json_paths::execute(&json!({
        "json_text": json_text
    }))
    .await
    .unwrap();
    let returned_paths: Vec<String> = paths_res
        .get("paths")
        .and_then(|v| v.as_array())
        .unwrap()
        .iter()
        .filter_map(|item| {
            item.get("path")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
        .collect();
    assert!(returned_paths.contains(&"$.user.roles[]".to_string()));

    let schema_res = extract_json_schema::execute(&json!({
        "json_text": json_text
    }))
    .await
    .unwrap();
    assert_eq!(
        schema_res
            .get("schema")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str()),
        Some("object")
    );
    assert_eq!(
        schema_res
            .get("schema")
            .and_then(|v| v.get("properties"))
            .and_then(|v| v.get("user"))
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str()),
        Some("object")
    );
}

#[tokio::test]
async fn test_sqlite_inspect_lists_tables_and_query_results() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("state.vscdb");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute(
        "CREATE TABLE kv (id INTEGER PRIMARY KEY, key TEXT NOT NULL, value TEXT NOT NULL)",
        [],
    )
    .unwrap();
    conn.execute("INSERT INTO kv (key, value) VALUES ('theme', 'dark')", [])
        .unwrap();
    conn.execute("INSERT INTO kv (key, value) VALUES ('locale', 'vi')", [])
        .unwrap();
    drop(conn);

    let inspect_res = sqlite_inspect::execute(&json!({
        "path": db_path.to_str().unwrap(),
        "table": "kv",
        "sample_limit": 1,
        "sql": "SELECT key, value FROM kv ORDER BY id"
    }))
    .await
    .unwrap();

    assert_eq!(
        inspect_res
            .get("tables")
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
        Some(1)
    );
    assert_eq!(
        inspect_res
            .get("table_info")
            .and_then(|v| v.get("sample_rows"))
            .and_then(|v| v.as_array())
            .map(|items| items.len()),
        Some(1)
    );
    assert_eq!(
        inspect_res
            .get("query_result")
            .and_then(|v| v.get("row_count_returned"))
            .and_then(|v| v.as_u64()),
        Some(2)
    );
}

#[tokio::test]
async fn test_diff_two_snippets_and_compare_symbols_return_unified_diff() {
    let diff_res = diff_two_snippets::execute(&json!({
        "left": "alpha\nbeta\n",
        "right": "alpha\ngamma\n",
        "left_label": "old",
        "right_label": "new"
    }))
    .await
    .unwrap();
    assert_eq!(
        diff_res.get("same_content").and_then(|v| v.as_bool()),
        Some(false)
    );
    let unified_diff = diff_res
        .get("unified_diff")
        .and_then(|v| v.as_str())
        .unwrap();
    assert!(unified_diff.contains("-beta"));
    assert!(unified_diff.contains("+gamma"));

    let dir = tempdir().unwrap();
    let left_path = dir.path().join("left.rs");
    let right_path = dir.path().join("right.rs");
    fs::write(
        &left_path,
        "fn provider() {\n    step_one();\n    step_two();\n}\n",
    )
    .unwrap();
    fs::write(
        &right_path,
        "fn provider() {\n    step_one();\n    step_three();\n}\n",
    )
    .unwrap();

    let compare_res = compare_symbols::execute(&json!({
        "left": {
            "symbol": "provider",
            "paths": [left_path.to_str().unwrap()]
        },
        "right": {
            "symbol": "provider",
            "paths": [right_path.to_str().unwrap()]
        }
    }))
    .await
    .unwrap();

    assert_eq!(
        compare_res.get("same_content").and_then(|v| v.as_bool()),
        Some(false)
    );
    let compare_diff = compare_res
        .get("unified_diff")
        .and_then(|v| v.as_str())
        .unwrap();
    assert!(compare_diff.contains("step_two"));
    assert!(compare_diff.contains("step_three"));
}
