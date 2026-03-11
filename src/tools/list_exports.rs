use anyhow::{Context, Result};
use regex::Regex;
use serde_json::{Value, json};
use tree_sitter::Node;

use crate::tools::ast_support::{
    DEFAULT_AST_FILE_SIZE_LIMIT, LanguageKind, declaration_name, node_text, parse_supported_file,
};

pub fn schema() -> Value {
    json!({
        "name": "list_exports",
        "description": "List Rust and TypeScript exports for a file.",
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
        .context("Unsupported extension for list_exports")?;

    if !matches!(
        parsed.language_kind,
        LanguageKind::Rust | LanguageKind::JavaScript
    ) {
        return Err(anyhow::anyhow!(
            "list_exports currently supports Rust and JavaScript/TypeScript files"
        ));
    }

    let mut exports = Vec::new();
    collect_exports_recursive(
        parsed.tree.root_node(),
        parsed.language_kind,
        &parsed.source,
        &mut exports,
    );

    Ok(json!({
        "file": path_str,
        "language": parsed.language_name,
        "exports": exports,
        "total_exports": exports.len()
    }))
}

fn collect_exports_recursive(
    node: Node<'_>,
    language_kind: LanguageKind,
    source: &[u8],
    exports: &mut Vec<Value>,
) {
    match language_kind {
        LanguageKind::Rust => collect_rust_export(node, source, exports),
        LanguageKind::JavaScript => collect_js_export(node, source, exports),
        LanguageKind::Python => {}
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_exports_recursive(child, language_kind, source, exports);
    }
}

fn collect_rust_export(node: Node<'_>, source: &[u8], exports: &mut Vec<Value>) {
    if let Some(statement) = node_text(node, source) {
        let trimmed = statement.trim();

        if node.kind() == "use_declaration" && trimmed.starts_with("pub use ") {
            exports.push(json!({
                "line": node.start_position().row + 1,
                "kind": "reexport",
                "name": Value::Null,
                "source": trimmed
                    .trim_start_matches("pub use ")
                    .trim_end_matches(';')
                    .trim(),
                "statement": trimmed
            }));
            return;
        }

        if !trimmed.starts_with("pub ") && !trimmed.starts_with("pub(") {
            return;
        }

        let interesting_kind = matches!(
            node.kind(),
            "function_item"
                | "struct_item"
                | "enum_item"
                | "trait_item"
                | "impl_item"
                | "mod_item"
                | "const_item"
                | "static_item"
                | "type_item"
                | "type_alias"
        );
        if !interesting_kind {
            return;
        }

        exports.push(json!({
            "line": node.start_position().row + 1,
            "kind": "pub_item",
            "name": declaration_name(&node, source),
            "source": Value::Null,
            "statement": trimmed.lines().next().unwrap_or(trimmed)
        }));
    }
}

fn collect_js_export(node: Node<'_>, source: &[u8], exports: &mut Vec<Value>) {
    if node.kind() != "export_statement" {
        return;
    }

    let statement = match node_text(node, source) {
        Some(statement) => statement.trim(),
        None => return,
    };

    let source_value = extract_from_source(statement);
    let kind = if statement.starts_with("export default") {
        "default"
    } else if source_value.is_some() {
        "reexport"
    } else if statement.starts_with("export {") {
        "named"
    } else {
        "declaration"
    };

    exports.push(json!({
        "line": node.start_position().row + 1,
        "kind": kind,
        "name": extract_export_name(statement),
        "source": source_value,
        "statement": statement
    }));
}

fn extract_from_source(statement: &str) -> Option<String> {
    let regex = Regex::new(r#"from\s+["'`](.+?)["'`]"#).ok()?;
    regex
        .captures(statement)
        .and_then(|caps| caps.get(1).map(|value| value.as_str().to_string()))
}

fn extract_export_name(statement: &str) -> Value {
    let patterns = [
        r"export\s+default\s+function\s+([A-Za-z0-9_$]+)",
        r"export\s+default\s+class\s+([A-Za-z0-9_$]+)",
        r"export\s+(?:async\s+)?function\s+([A-Za-z0-9_$]+)",
        r"export\s+class\s+([A-Za-z0-9_$]+)",
        r"export\s+const\s+([A-Za-z0-9_$]+)",
        r"export\s+let\s+([A-Za-z0-9_$]+)",
        r"export\s+var\s+([A-Za-z0-9_$]+)",
        r"export\s+interface\s+([A-Za-z0-9_$]+)",
        r"export\s+type\s+([A-Za-z0-9_$]+)",
        r"export\s+enum\s+([A-Za-z0-9_$]+)",
    ];

    for pattern in patterns {
        if let Ok(regex) = Regex::new(pattern)
            && let Some(caps) = regex.captures(statement)
            && let Some(value) = caps.get(1)
        {
            return json!(value.as_str());
        }
    }

    Value::Null
}
