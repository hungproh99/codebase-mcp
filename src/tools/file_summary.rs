use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

fn detect_language(ext: &str) -> &'static str {
    match ext {
        "rs" => "Rust",
        "py" => "Python",
        "js" => "JavaScript",
        "ts" | "tsx" => "TypeScript",
        "jsx" => "JSX",
        "java" => "Java",
        "c" | "h" => "C",
        "cpp" | "cc" | "cxx" | "hpp" => "C++",
        "cs" => "C#",
        "go" => "Go",
        "rb" => "Ruby",
        "php" => "PHP",
        "swift" => "Swift",
        "kt" | "kts" => "Kotlin",
        "scala" => "Scala",
        "sh" | "bash" => "Shell",
        "ps1" => "PowerShell",
        "sql" => "SQL",
        "html" | "htm" => "HTML",
        "css" | "scss" | "sass" | "less" => "CSS",
        "json" => "JSON",
        "yaml" | "yml" => "YAML",
        "toml" => "TOML",
        "xml" => "XML",
        "md" | "markdown" => "Markdown",
        "txt" => "Text",
        "proto" => "Protobuf",
        "lua" => "Lua",
        "r" => "R",
        "dart" => "Dart",
        "ex" | "exs" => "Elixir",
        "zig" => "Zig",
        _ => "Unknown",
    }
}

pub fn schema() -> Value {
    json!({
        "name": "file_summary",
        "description": "Return quick file metadata, binary detection, and a short preview.",
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
    let path = PathBuf::from(path_str);

    if !path.exists() || !path.is_file() {
        return Err(anyhow::anyhow!(
            "File does not exist or is not a file: {}",
            path_str
        ));
    }

    let meta = std::fs::metadata(&path)?;
    let total_size = meta.len();

    // Basic language detection from the file extension.
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    let language = detect_language(ext);

    // Last modified timestamp
    let last_modified = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
        .unwrap_or(0);

    // Read up to 8KB
    let mut file = File::open(&path)?;
    let mut buffer = vec![0; std::cmp::min(8192, total_size as usize)];
    let bytes_read = file.read(&mut buffer)?;
    buffer.truncate(bytes_read);

    // Heuristic: if b'\x00' exists in first 8KB -> Binary file
    let is_binary = crate::tools::read_file::is_probably_binary(&buffer);

    if is_binary {
        return Ok(json!({
            "path": path_str,
            "size": total_size,
            "language": language,
            "last_modified": last_modified,
            "is_binary": true,
            "lines": 0,
            "outline_preview": null,
            "warning": "This is a binary file."
        }));
    }

    let (preview_content, encoding) = crate::tools::read_file::decode_fuzzy(&buffer);
    let preview_lines: Vec<&str> = preview_content.lines().take(10).collect();
    let outline_preview = preview_lines.join("\n");

    let line_count = if total_size > 50 * 1024 * 1024 {
        -1
    } else {
        match std::fs::read(&path) {
            Ok(full_buf) => {
                let (full_content, _) = crate::tools::read_file::decode_fuzzy(&full_buf);
                crate::tools::read_file::count_text_lines(&full_content) as i64
            }
            Err(_) => -1,
        }
    };

    Ok(json!({
        "path": path_str,
        "size": total_size,
        "language": language,
        "encoding": encoding,
        "last_modified": last_modified,
        "is_binary": false,
        "lines": line_count,
        "outline_preview": outline_preview,
    }))
}
