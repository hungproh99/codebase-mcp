use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use tree_sitter::{Language, Node, Parser, Tree};

pub const DEFAULT_AST_FILE_SIZE_LIMIT: u64 = 2 * 1024 * 1024;

const CODE_EXTENSIONS: &[&str] = &[
    "c", "cc", "cpp", "cxx", "h", "hh", "hpp", "hxx", "inc", "inl", "asm", "s", "S", "rs", "js",
    "jsx", "ts", "tsx", "mjs", "cjs", "vue", "svelte", "py", "pyi", "rb", "php", "java", "kt",
    "kts", "scala", "go", "swift", "dart", "cs", "fs", "fsx", "sh", "bash", "zsh", "ps1", "bat",
    "cmd", "json", "yaml", "yml", "toml", "xml", "html", "htm", "css", "scss", "less", "sql",
    "proto", "graphql", "gql", "gn", "gni", "gyp", "gypi", "cmake", "mk", "mak", "md", "txt",
    "rst", "cfg", "ini", "conf", "lua", "r", "m", "mm", "d", "zig", "nim", "v", "ex", "exs", "elm",
    "clj",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LanguageKind {
    Rust,
    JavaScript,
    Python,
}

pub struct ParsedAstFile {
    pub language_kind: LanguageKind,
    pub language_name: &'static str,
    pub source: Vec<u8>,
    pub tree: Tree,
}

pub fn parse_language_filter(raw: Option<&str>) -> Result<Option<LanguageKind>> {
    match raw {
        None => Ok(None),
        Some("rust") | Some("rs") => Ok(Some(LanguageKind::Rust)),
        Some("python") | Some("py") => Ok(Some(LanguageKind::Python)),
        Some("javascript") | Some("js") | Some("jsx") | Some("ts") | Some("tsx")
        | Some("typescript") => Ok(Some(LanguageKind::JavaScript)),
        Some(other) => Err(anyhow::anyhow!("Unsupported language '{}'", other)),
    }
}

pub fn detect_language(path: &Path) -> Option<(LanguageKind, &'static str, Language)> {
    let ext = path.extension().and_then(|value| value.to_str())?;
    match ext {
        "rs" => Some((
            LanguageKind::Rust,
            "Rust",
            tree_sitter_rust::LANGUAGE.into(),
        )),
        "js" | "jsx" | "mjs" | "cjs" => Some((
            LanguageKind::JavaScript,
            "JavaScript",
            tree_sitter_javascript::LANGUAGE.into(),
        )),
        "ts" => Some((
            LanguageKind::JavaScript,
            "TypeScript",
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
        )),
        "tsx" => Some((
            LanguageKind::JavaScript,
            "TypeScript",
            tree_sitter_typescript::LANGUAGE_TSX.into(),
        )),
        "py" => Some((
            LanguageKind::Python,
            "Python",
            tree_sitter_python::LANGUAGE.into(),
        )),
        _ => None,
    }
}

pub fn parse_supported_file(
    path: &Path,
    max_bytes: u64,
    language_filter: Option<LanguageKind>,
) -> Result<Option<ParsedAstFile>> {
    if !path.exists() || !path.is_file() {
        return Ok(None);
    }

    let meta = std::fs::metadata(path)?;
    if meta.len() > max_bytes {
        return Ok(None);
    }

    let (language_kind, language_name, language) = match detect_language(path) {
        Some(language) => language,
        None => return Ok(None),
    };

    if let Some(filter) = language_filter
        && filter != language_kind
    {
        return Ok(None);
    }

    let mut parser = Parser::new();
    parser.set_language(&language)?;

    let mut file = File::open(path)?;
    let mut source = Vec::new();
    file.read_to_end(&mut source)?;

    let tree = parser
        .parse(source.as_slice(), None)
        .context("Tree-sitter parse failed")?;

    Ok(Some(ParsedAstFile {
        language_kind,
        language_name,
        source,
        tree,
    }))
}

pub fn visit_candidate_code_files<F>(
    search_paths: &[PathBuf],
    file_hint: Option<&Path>,
    language_filter: Option<LanguageKind>,
    mut visitor: F,
) -> Result<()>
where
    F: FnMut(&Path) -> Result<bool>,
{
    let mut seen = HashSet::new();

    if let Some(file_hint) = file_hint
        && !visit_candidate(file_hint, language_filter, &mut seen, &mut visitor)?
    {
        return Ok(());
    }

    for search_path in search_paths {
        if !search_path.exists() {
            continue;
        }

        let canonical_search_path = search_path
            .canonicalize()
            .unwrap_or_else(|_| search_path.to_path_buf());

        if canonical_search_path.is_file() {
            if !visit_candidate(
                &canonical_search_path,
                language_filter,
                &mut seen,
                &mut visitor,
            )? {
                return Ok(());
            }
            continue;
        }

        if !canonical_search_path.is_dir() {
            continue;
        }

        for entry in WalkBuilder::new(&canonical_search_path)
            .hidden(true)
            .ignore(true)
            .git_ignore(true)
            .git_exclude(true)
            .require_git(false)
            .build()
        {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };

            if !entry
                .file_type()
                .is_some_and(|file_type| file_type.is_file())
            {
                continue;
            }

            if !visit_candidate(entry.path(), language_filter, &mut seen, &mut visitor)? {
                return Ok(());
            }
        }
    }

    Ok(())
}

fn visit_candidate<F>(
    path: &Path,
    language_filter: Option<LanguageKind>,
    seen: &mut HashSet<String>,
    visitor: &mut F,
) -> Result<bool>
where
    F: FnMut(&Path) -> Result<bool>,
{
    if !is_code_file(path) {
        return Ok(true);
    }

    if let Some(filter) = language_filter {
        match detect_language(path) {
            Some((kind, _, _)) if kind == filter => {}
            _ => return Ok(true),
        }
    }

    let normalized = normalize_path(path);
    if seen.insert(normalized) {
        return visitor(path);
    }

    Ok(true)
}

pub fn is_code_file(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| CODE_EXTENSIONS.contains(&ext.to_lowercase().as_str()))
        .unwrap_or(false)
}

pub fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

pub fn node_text<'a>(node: Node<'a>, source: &'a [u8]) -> Option<&'a str> {
    std::str::from_utf8(&source[node.byte_range()]).ok()
}

pub fn child_field_text<'a>(node: &Node<'a>, field: &str, source: &'a [u8]) -> Option<&'a str> {
    let field_node = node.child_by_field_name(field)?;
    node_text(field_node, source)
}

pub fn declaration_name<'a>(node: &Node<'a>, source: &'a [u8]) -> Option<&'a str> {
    child_field_text(node, "name", source).or_else(|| child_field_text(node, "function", source))
}

pub fn first_line_preview(node: Node<'_>, source: &[u8], max_bytes: usize) -> String {
    let end_byte = std::cmp::min(node.start_byte() + max_bytes, source.len());
    std::str::from_utf8(&source[node.start_byte()..end_byte])
        .unwrap_or("")
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

pub fn is_symbol_node(kind: &str) -> bool {
    matches!(
        kind,
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
            | "function_definition"
            | "class_definition"
            | "function_declaration"
            | "class_declaration"
            | "method_definition"
            | "arrow_function"
    )
}

pub fn is_function_like_node(kind: &str) -> bool {
    matches!(
        kind,
        "function_item" | "function_definition" | "function_declaration" | "method_definition"
    )
}

pub fn find_named_symbol_node<'a>(node: Node<'a>, source: &[u8], symbol: &str) -> Option<Node<'a>> {
    if is_symbol_node(node.kind()) && declaration_name(&node, source) == Some(symbol) {
        return Some(node);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(found) = find_named_symbol_node(child, source, symbol) {
            return Some(found);
        }
    }

    None
}

pub fn find_named_function_like<'a>(
    node: Node<'a>,
    source: &[u8],
    symbol: &str,
) -> Option<Node<'a>> {
    if is_function_like_node(node.kind()) && declaration_name(&node, source) == Some(symbol) {
        return Some(node);
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        if let Some(found) = find_named_function_like(child, source, symbol) {
            return Some(found);
        }
    }

    None
}

pub fn collect_symbols(root: Node<'_>, source: &[u8]) -> Vec<Value> {
    let mut symbols = Vec::new();
    collect_symbols_recursive(root, source, &mut symbols, None);
    symbols
}

fn collect_symbols_recursive(
    node: Node<'_>,
    source: &[u8],
    symbols: &mut Vec<Value>,
    parent_name: Option<String>,
) {
    let mut current_parent = parent_name.clone();

    if is_symbol_node(node.kind()) {
        let start_pos = node.start_position();
        let end_pos = node.end_position();
        let name = declaration_name(&node, source).unwrap_or("<anonymous>");

        symbols.push(json!({
            "name": name,
            "kind": node.kind(),
            "start_line": start_pos.row + 1,
            "end_line": end_pos.row + 1,
            "signature": first_line_preview(node, source, 160),
            "parent": parent_name.clone()
        }));

        current_parent = Some(name.to_string());
    }

    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        collect_symbols_recursive(child, source, symbols, current_parent.clone());
    }
}

pub fn normalized_string_literal(raw: &str) -> String {
    raw.trim()
        .trim_start_matches(['"', '\'', '`'])
        .trim_end_matches(['"', '\'', '`'])
        .to_string()
}
