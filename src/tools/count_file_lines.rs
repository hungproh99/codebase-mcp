use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::path::PathBuf;

pub fn schema() -> Value {
    json!({
        "name": "count_file_lines",
        "description": "Count lines in a text file with basic encoding and binary detection.",
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
    let buffer = std::fs::read(&path)?;
    let preview = &buffer[..std::cmp::min(buffer.len(), 8192)];

    if crate::tools::read_file::is_probably_binary(preview) {
        return Ok(json!({
            "path": path_str,
            "file_size_bytes": meta.len(),
            "encoding": Value::Null,
            "is_binary": true,
            "line_count": 0,
            "warning": "This appears to be a binary file."
        }));
    }

    let (content, encoding) = crate::tools::read_file::decode_fuzzy(&buffer);
    let line_count = crate::tools::read_file::count_text_lines(&content);

    Ok(json!({
        "path": path_str,
        "file_size_bytes": meta.len(),
        "encoding": encoding,
        "is_binary": false,
        "line_count": line_count,
        "ends_with_newline": content.ends_with('\n')
    }))
}
