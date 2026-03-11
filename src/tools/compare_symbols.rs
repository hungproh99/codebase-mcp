use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::common::insert_object_field;
use crate::tools::diff_support::build_diff_payload;
use crate::tools::read_symbol_body;

pub fn schema() -> Value {
    json!({
        "name": "compare_symbols",
        "description": "Compare two resolved symbols and return metadata plus a unified diff.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "left": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string" },
                        "paths": { "type": "array", "items": { "type": "string" } },
                        "file_hint": { "type": "string" },
                        "language": { "type": "string" }
                    },
                    "required": ["symbol"]
                },
                "right": {
                    "type": "object",
                    "properties": {
                        "symbol": { "type": "string" },
                        "paths": { "type": "array", "items": { "type": "string" } },
                        "file_hint": { "type": "string" },
                        "language": { "type": "string" }
                    },
                    "required": ["symbol"]
                },
                "include_signature": { "type": "boolean" }
            },
            "required": ["left", "right"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let left = args.get("left").context("Missing left object")?;
    let right = args.get("right").context("Missing right object")?;
    let include_signature = args
        .get("include_signature")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let left_result = read_symbol_for_compare(left, include_signature).await?;
    let right_result = read_symbol_for_compare(right, include_signature).await?;

    let left_content = left_result
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let right_content = right_result
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or_default();

    let left_label = format!(
        "{}:{}",
        left_result
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("left"),
        left_result
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("symbol")
    );
    let right_label = format!(
        "{}:{}",
        right_result
            .get("file")
            .and_then(|v| v.as_str())
            .unwrap_or("right"),
        right_result
            .get("symbol")
            .and_then(|v| v.as_str())
            .unwrap_or("symbol")
    );

    let mut response = build_diff_payload(left_content, right_content, &left_label, &right_label);
    insert_object_field(&mut response, "left", left_result);
    insert_object_field(&mut response, "right", right_result);

    Ok(response)
}

async fn read_symbol_for_compare(config: &Value, include_signature: bool) -> Result<Value> {
    let symbol = config
        .get("symbol")
        .and_then(|v| v.as_str())
        .context("Missing symbol in compare_symbols side config")?;

    let mut args = json!({
        "symbol": symbol,
        "include_signature": include_signature
    });

    if let Some(paths) = config.get("paths") {
        insert_object_field(&mut args, "paths", paths.clone());
    }
    if let Some(file_hint) = config.get("file_hint") {
        insert_object_field(&mut args, "file_hint", file_hint.clone());
    }
    if let Some(language) = config.get("language") {
        insert_object_field(&mut args, "language", language.clone());
    }

    read_symbol_body::execute(&args).await
}
