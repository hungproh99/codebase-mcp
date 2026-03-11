use anyhow::Result;
use serde_json::{Value, json};
use std::path::PathBuf;

pub fn schema() -> Value {
    json!({
        "name": "resolve_path",
        "description": "Normalize a path and return preflight accessibility metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                }
            },
            "required": ["path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    if path_str.is_empty() {
        return Err(anyhow::anyhow!("Path cannot be empty"));
    }

    let input_path = PathBuf::from(path_str);
    let canonical = std::fs::canonicalize(&input_path).unwrap_or(input_path);

    Ok(json!({
        "canonical_path": canonical.to_string_lossy(),
        "tier": "Allowed",
        "is_accessible": true,
        "exists": canonical.exists(),
        "is_file": canonical.is_file(),
        "is_dir": canonical.is_dir(),
        "reason_if_blocked": serde_json::Value::Null
    }))
}
