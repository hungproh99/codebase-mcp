use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use crate::tools::read_file::decode_fuzzy;

pub fn schema() -> Value {
    json!({
        "name": "find_json_paths",
        "description": "Enumerate JSON paths from a file or inline JSON.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "json_text": { "type": "string" },
                "max_paths": { "type": "integer" },
                "include_array_indexes": { "type": "boolean" }
            }
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let (value, source_kind, source_value) = load_json_input(args)?;
    let max_paths = args
        .get("max_paths")
        .and_then(|v| v.as_u64())
        .unwrap_or(200) as usize;
    let include_array_indexes = args
        .get("include_array_indexes")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let mut paths = Vec::new();
    let mut truncated = false;
    collect_paths(
        &value,
        "$".to_string(),
        include_array_indexes,
        max_paths,
        &mut paths,
        &mut truncated,
    );

    Ok(json!({
        "source_kind": source_kind,
        "source": source_value,
        "paths": paths,
        "total_returned": paths.len(),
        "truncated": truncated
    }))
}

fn load_json_input(args: &Value) -> Result<(Value, &'static str, String)> {
    if let Some(json_text) = args.get("json_text").and_then(|v| v.as_str()) {
        let value = serde_json::from_str::<Value>(json_text).context("Invalid json_text")?;
        return Ok((value, "inline", "inline".to_string()));
    }

    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .context("Either path or json_text is required")?;
    let path = PathBuf::from(path_str);
    if !path.exists() || !path.is_file() {
        return Err(anyhow::anyhow!(
            "File does not exist or is not a file: {}",
            path_str
        ));
    }

    let mut file = File::open(&path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;
    let (text, _) = decode_fuzzy(&buffer);
    let value = serde_json::from_str::<Value>(&text).context("Invalid JSON file content")?;
    Ok((value, "path", path_str.to_string()))
}

fn collect_paths(
    value: &Value,
    current_path: String,
    include_array_indexes: bool,
    max_paths: usize,
    paths: &mut Vec<Value>,
    truncated: &mut bool,
) {
    if paths.len() >= max_paths {
        *truncated = true;
        return;
    }

    paths.push(json!({
        "path": current_path,
        "value_type": value_type(value),
        "sample_value": sample_value(value)
    }));

    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = format!("{}.{}", current_path, key);
                collect_paths(
                    child,
                    child_path,
                    include_array_indexes,
                    max_paths,
                    paths,
                    truncated,
                );
                if *truncated {
                    return;
                }
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let child_path = if include_array_indexes {
                    format!("{}[{}]", current_path, index)
                } else {
                    format!("{}[]", current_path)
                };
                collect_paths(
                    child,
                    child_path,
                    include_array_indexes,
                    max_paths,
                    paths,
                    truncated,
                );
                if *truncated {
                    return;
                }
            }
        }
        _ => {}
    }
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn sample_value(value: &Value) -> Value {
    match value {
        Value::Null => Value::Null,
        Value::Bool(boolean) => json!(boolean),
        Value::Number(number) => json!(number),
        Value::String(string) => {
            let truncated: String = string.chars().take(80).collect();
            json!(truncated)
        }
        Value::Array(items) => json!(format!("array(len={})", items.len())),
        Value::Object(map) => json!(format!("object(keys={})", map.len())),
    }
}
