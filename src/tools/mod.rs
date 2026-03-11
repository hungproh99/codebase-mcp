use anyhow::Result;
use serde_json::{Value, json};

mod ast_support;
mod diff_support;

pub mod batch_tool_call;
pub mod compare_symbols;
pub mod convert_file_format;
pub mod count_file_lines;
pub mod create_directory;
pub mod create_file;
pub mod delete_file;
pub mod diff_two_snippets;
pub mod edit_file;
pub mod extract_json_schema;
pub mod file_summary;
pub mod find_definition;
pub mod find_json_paths;
pub mod find_references;
pub mod fuzzy_find;
pub mod get_call_graph;
pub mod get_semantic_diff;
pub mod get_symbols;
pub mod git_blame;
pub mod git_diff;
pub mod git_helper;
pub mod git_log;
pub mod git_status;
pub mod history_status;
pub mod list_exports;
pub mod list_imports;
pub mod peek_archive;
pub mod project_map;
pub mod read_file;
pub mod read_snippets;
pub mod read_symbol_body;
pub mod redo_last_change;
pub mod resolve_path;
pub mod server_health;
pub mod sqlite_inspect;
pub mod text_search;
pub mod undo_last_change;
pub mod workspace_stats;

pub fn list_tools() -> Vec<Value> {
    vec![
        resolve_path::schema(),
        text_search::schema(),
        read_file::schema(),
        count_file_lines::schema(),
        convert_file_format::schema(),
        create_file::schema(),
        create_directory::schema(),
        delete_file::schema(),
        edit_file::schema(),
        history_status::schema(),
        file_summary::schema(),
        read_snippets::schema(),
        read_symbol_body::schema(),
        list_imports::schema(),
        list_exports::schema(),
        find_json_paths::schema(),
        extract_json_schema::schema(),
        sqlite_inspect::schema(),
        diff_two_snippets::schema(),
        compare_symbols::schema(),
        fuzzy_find::schema(),
        project_map::schema(),
        get_symbols::schema(),
        git_status::schema(),
        git_diff::schema(),
        git_log::schema(),
        workspace_stats::schema(),
        server_health::schema(),
        peek_archive::schema(),
        git_blame::schema(),
        find_definition::schema(),
        find_references::schema(),
        get_call_graph::schema(),
        get_semantic_diff::schema(),
        redo_last_change::schema(),
        undo_last_change::schema(),
        batch_tool_call::schema(),
    ]
}

pub async fn call_tool(params: Value) -> Result<Value> {
    let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    // MCP tool responses are wrapped in the standard content array shape.
    let result = match name {
        "resolve_path" => resolve_path::execute(&arguments).await,
        "text_search" => text_search::execute(&arguments).await,
        "read_file_range" => read_file::execute(&arguments).await,
        "count_file_lines" => count_file_lines::execute(&arguments).await,
        "convert_file_format" => convert_file_format::execute(&arguments).await,
        "create_file" => create_file::execute(&arguments).await,
        "create_directory" => create_directory::execute(&arguments).await,
        "delete_file" => delete_file::execute(&arguments).await,
        "edit_file" => edit_file::execute(&arguments).await,
        "history_status" => history_status::execute(&arguments).await,
        "file_summary" => file_summary::execute(&arguments).await,
        "read_snippets" => read_snippets::execute(&arguments).await,
        "read_symbol_body" => read_symbol_body::execute(&arguments).await,
        "list_imports" => list_imports::execute(&arguments).await,
        "list_exports" => list_exports::execute(&arguments).await,
        "find_json_paths" => find_json_paths::execute(&arguments).await,
        "extract_json_schema" => extract_json_schema::execute(&arguments).await,
        "sqlite_inspect" => sqlite_inspect::execute(&arguments).await,
        "diff_two_snippets" => diff_two_snippets::execute(&arguments).await,
        "compare_symbols" => compare_symbols::execute(&arguments).await,
        "fuzzy_find" => fuzzy_find::execute(&arguments).await,
        "project_map" => project_map::execute(&arguments).await,
        "get_symbols" => get_symbols::execute(&arguments).await,
        "git_status" => git_status::execute(&arguments).await,
        "git_diff" => git_diff::execute(&arguments).await,
        "git_log" => git_log::execute(&arguments).await,
        "workspace_stats" => workspace_stats::execute(&arguments).await,
        "server_health" => server_health::execute(&arguments).await,
        "peek_archive" => peek_archive::execute(&arguments).await,
        "git_blame" => git_blame::execute(&arguments).await,
        "find_definition" => find_definition::execute(&arguments).await,
        "find_references" => find_references::execute(&arguments).await,
        "get_call_graph" => get_call_graph::execute(&arguments).await,
        "get_semantic_diff" => get_semantic_diff::execute(&arguments).await,
        "redo_last_change" => redo_last_change::execute(&arguments).await,
        "undo_last_change" => undo_last_change::execute(&arguments).await,
        "batch_tool_call" => batch_tool_call::execute(&arguments).await,
        _ => return Err(anyhow::anyhow!("Tool not found: {}", name)),
    };

    match result {
        Ok(data) => Ok(
            json!({ "content": [{ "type": "text", "text": serde_json::to_string(&data).unwrap_or_default() }] }),
        ),
        Err(e) => {
            Ok(json!({ "isError": true, "content": [{ "type": "text", "text": e.to_string() }] }))
        }
    }
}
