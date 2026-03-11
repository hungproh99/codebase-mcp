use anyhow::{Context, Result};
use serde_json::{Value, json};
use tree_sitter::Node;

use crate::tools::ast_support::{
    DEFAULT_AST_FILE_SIZE_LIMIT, LanguageKind, child_field_text, node_text,
    normalized_string_literal, parse_supported_file,
};

pub fn schema() -> Value {
    json!({
        "name": "list_imports",
        "description": "List Rust and TypeScript imports for a file.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" }
            },
            "required": ["path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .context("Missing path")?;
    let path = std::path::PathBuf::from(path_str);

    let parsed = parse_supported_file(&path, DEFAULT_AST_FILE_SIZE_LIMIT, None)?
        .context("Unsupported extension for list_imports")?;

    if !matches!(
        parsed.language_kind,
        LanguageKind::Rust | LanguageKind::JavaScript
    ) {
        return Err(anyhow::anyhow!(
            "list_imports currently supports Rust and JavaScript/TypeScript files"
        ));
    }

    let mut imports = Vec::new();
    collect_imports_recursive(
        parsed.tree.root_node(),
        parsed.language_kind,
        &parsed.source,
        &mut imports,
    );

    Ok(json!({
        "file": path_str,
        "language": parsed.language_name,
        "imports": imports,
        "total_imports": imports.len()
    }))
}

fn collect_imports_recursive(
    node: Node<'_>,
    language_kind: LanguageKind,
    source: &[u8],
    imports: &mut Vec<Value>,
) {
    match language_kind {
        LanguageKind::Rust if node.kind() == "use_declaration" => {
            if let Some(statement) = node_text(node, source) {
                let trimmed = statement.trim();
                let is_public = trimmed.starts_with("pub use ");
                let clause = trimmed
                    .trim_start_matches("pub ")
                    .trim_start_matches("use ")
                    .trim_end_matches(';')
                    .trim()
                    .to_string();

                imports.push(json!({
                    "line": node.start_position().row + 1,
                    "kind": if is_public { "pub_use" } else { "use" },
                    "source": clause,
                    "clause": clause,
                    "statement": trimmed
                }));
            }
        }
        LanguageKind::JavaScript if node.kind() == "import_statement" => {
            if let Some(statement) = node_text(node, source) {
                let trimmed = statement.trim();
                let source_value = child_field_text(&node, "source", source)
                    .map(normalized_string_literal)
                    .unwrap_or_default();
                let clause = parse_js_import_clause(trimmed);
                let kind = if trimmed.starts_with("import type ") {
                    "type_import"
                } else if clause.is_empty() {
                    "side_effect"
                } else {
                    "import"
                };

                imports.push(json!({
                    "line": node.start_position().row + 1,
                    "kind": kind,
                    "source": source_value,
                    "clause": clause,
                    "statement": trimmed
                }));
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_imports_recursive(child, language_kind, source, imports);
    }
}

fn parse_js_import_clause(statement: &str) -> String {
    let trimmed = statement.trim_end_matches(';').trim();
    if !trimmed.starts_with("import ") {
        return String::new();
    }

    if let Some(index) = trimmed.rfind(" from ") {
        trimmed["import ".len()..index].trim().to_string()
    } else {
        String::new()
    }
}
