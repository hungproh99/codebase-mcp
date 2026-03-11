use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use tree_sitter::{Language, Node, Parser};

use super::git_helper;
use crate::common::repo_relative_path;

#[derive(Clone, Debug)]
struct SymbolData {
    name: String,
    kind: String,
    signature: String,
    body_hash: u64,
    start_line: usize,
    end_line: usize,
}

#[derive(Clone, Debug)]
struct DiffRecord {
    diff_type: String,
    symbol: String,
    kind: String,
    before: Option<String>,
    after: Option<String>,
    line: usize,
    start_line: usize,
    end_line: usize,
}

pub fn schema() -> Value {
    json!({
        "name": "get_semantic_diff",
        "description": "Compute AST-based semantic changes for a file against a base commit.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "file_path": { "type": "string" },
                "base_commit": { "type": "string" }
            },
            "required": ["file_path"]
        }
    })
}

fn get_language(ext: &str) -> Option<Language> {
    match ext {
        "rs" => Some(tree_sitter_rust::LANGUAGE.into()),
        "js" | "jsx" | "mjs" | "cjs" => Some(tree_sitter_javascript::LANGUAGE.into()),
        "ts" => Some(tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()),
        "tsx" => Some(tree_sitter_typescript::LANGUAGE_TSX.into()),
        "py" => Some(tree_sitter_python::LANGUAGE.into()),
        _ => None,
    }
}

fn is_symbol_node(kind: &str) -> bool {
    matches!(
        kind,
        "function_item"
            | "struct_item"
            | "enum_item"
            | "trait_item"
            | "impl_item"
            | "function_definition"
            | "class_definition"
            | "function_declaration"
            | "class_declaration"
            | "method_definition"
            | "arrow_function"
    )
}

fn is_container_symbol(kind: &str) -> bool {
    matches!(kind, "class_definition" | "class_declaration" | "impl_item")
}

fn node_name<'a>(node: &Node<'a>, source: &'a [u8]) -> Option<&'a str> {
    let name_node = node.child_by_field_name("name")?;
    std::str::from_utf8(&source[name_node.byte_range()]).ok()
}

fn calculate_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn first_line_preview(node: &Node<'_>, source: &[u8]) -> String {
    let end_byte = std::cmp::min(node.start_byte() + 160, source.len());
    std::str::from_utf8(&source[node.start_byte()..end_byte])
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

fn extract_symbols(source_code: &[u8], language: Language) -> Result<HashMap<String, SymbolData>> {
    let mut parser = Parser::new();
    parser.set_language(&language)?;

    let tree = parser
        .parse(source_code, None)
        .context("Tree-sitter parse failed")?;
    let root = tree.root_node();

    let mut map = HashMap::new();
    collect_symbols(root, source_code, None, &mut map);
    Ok(map)
}

fn collect_symbols(
    node: Node<'_>,
    source: &[u8],
    parent_path: Option<String>,
    map: &mut HashMap<String, SymbolData>,
) {
    let mut next_parent = parent_path.clone();

    if is_symbol_node(node.kind())
        && let Some(name) = node_name(&node, source)
    {
        let signature = first_line_preview(&node, source);
        let body_text = std::str::from_utf8(&source[node.byte_range()]).unwrap_or("");
        let id_prefix = parent_path.as_deref().unwrap_or("");
        let id = if id_prefix.is_empty() {
            format!("{}:{}", node.kind(), name)
        } else {
            format!("{}::{}:{}", id_prefix, node.kind(), name)
        };

        map.insert(
            id,
            SymbolData {
                name: name.to_string(),
                kind: node.kind().to_string(),
                signature,
                body_hash: calculate_hash(&body_text),
                start_line: node.start_position().row + 1,
                end_line: node.end_position().row + 1,
            },
        );

        next_parent = Some(match parent_path {
            Some(parent) => format!("{}::{}", parent, name),
            None => name.to_string(),
        });
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_symbols(child, source, next_parent.clone(), map);
    }
}

fn describe_change(record: &DiffRecord) -> String {
    match record.diff_type.as_str() {
        "signature_change" => format!("Symbol '{}' changed its signature", record.symbol),
        "logic_change" => format!("Symbol '{}' changed its internal logic", record.symbol),
        "added" => format!("Symbol '{}' was added", record.symbol),
        "deleted" => format!("Symbol '{}' was deleted", record.symbol),
        _ => format!("Symbol '{}' changed", record.symbol),
    }
}

fn should_keep_diff(candidate: &DiffRecord, diffs: &[DiffRecord]) -> bool {
    if candidate.diff_type != "logic_change" || !is_container_symbol(&candidate.kind) {
        return true;
    }

    !diffs.iter().any(|other| {
        other.symbol != candidate.symbol
            && other.line >= candidate.start_line
            && other.line <= candidate.end_line
            && matches!(
                other.diff_type.as_str(),
                "logic_change" | "signature_change" | "added" | "deleted"
            )
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("file_path")
        .and_then(|v| v.as_str())
        .context("Missing file_path")?;
    let base_commit = args
        .get("base_commit")
        .and_then(|v| v.as_str())
        .unwrap_or("HEAD");

    let path = PathBuf::from(path_str);
    if !path.exists() {
        return Err(anyhow::anyhow!("File does not exist: {}", path_str));
    }

    let repo = git_helper::open_repo(&path)?;
    let relative_path = repo_relative_path(&repo, &path);
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let language = get_language(ext).context(format!(
        "Unsupported extension '{}' for get_semantic_diff",
        ext
    ))?;

    let object = repo.revparse_single(&format!(
        "{}:{}",
        base_commit,
        relative_path.to_string_lossy().replace('\\', "/")
    ))?;
    let old_blob = object.as_blob().context("Object is not a blob")?;
    let old_source = old_blob.content();

    let meta = std::fs::metadata(&path)?;
    if meta.len() > 2 * 1024 * 1024 {
        return Err(anyhow::anyhow!(
            "File too large for AST semantic diff (> 2MB limit)"
        ));
    }
    let new_source = std::fs::read(&path)?;

    let old_symbols = extract_symbols(old_source, language.clone())?;
    let new_symbols = extract_symbols(&new_source, language)?;
    let mut diffs = Vec::new();

    for (id, new_symbol) in &new_symbols {
        if let Some(old_symbol) = old_symbols.get(id) {
            if old_symbol.body_hash != new_symbol.body_hash {
                let diff_type = if old_symbol.signature != new_symbol.signature {
                    "signature_change"
                } else {
                    "logic_change"
                };
                diffs.push(DiffRecord {
                    diff_type: diff_type.to_string(),
                    symbol: new_symbol.name.clone(),
                    kind: new_symbol.kind.clone(),
                    before: Some(old_symbol.signature.clone()),
                    after: Some(new_symbol.signature.clone()),
                    line: new_symbol.start_line,
                    start_line: new_symbol.start_line,
                    end_line: new_symbol.end_line,
                });
            }
        } else {
            diffs.push(DiffRecord {
                diff_type: "added".to_string(),
                symbol: new_symbol.name.clone(),
                kind: new_symbol.kind.clone(),
                before: None,
                after: Some(new_symbol.signature.clone()),
                line: new_symbol.start_line,
                start_line: new_symbol.start_line,
                end_line: new_symbol.end_line,
            });
        }
    }

    for (id, old_symbol) in &old_symbols {
        if !new_symbols.contains_key(id) {
            diffs.push(DiffRecord {
                diff_type: "deleted".to_string(),
                symbol: old_symbol.name.clone(),
                kind: old_symbol.kind.clone(),
                before: Some(old_symbol.signature.clone()),
                after: None,
                line: old_symbol.start_line,
                start_line: old_symbol.start_line,
                end_line: old_symbol.end_line,
            });
        }
    }

    let mut filtered: Vec<DiffRecord> = diffs
        .iter()
        .filter(|candidate| should_keep_diff(candidate, &diffs))
        .cloned()
        .collect();
    filtered.sort_by_key(|record| record.line);

    let diffs_json: Vec<Value> = filtered
        .iter()
        .map(|record| {
            json!({
                "type": record.diff_type,
                "symbol": record.symbol,
                "kind": record.kind,
                "before": record.before,
                "after": record.after,
                "line": record.line,
                "description": describe_change(record)
            })
        })
        .collect();

    Ok(json!({
        "file": path_str,
        "base": base_commit,
        "total_semantic_changes": diffs_json.len(),
        "diffs": diffs_json
    }))
}
