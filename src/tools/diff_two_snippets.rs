use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::common::insert_object_field;
use crate::tools::diff_support::build_diff_payload;

pub fn schema() -> Value {
    json!({
        "name": "diff_two_snippets",
        "description": "Compare two snippets and return a unified diff summary.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "left": { "type": "string" },
                "right": { "type": "string" },
                "left_label": { "type": "string" },
                "right_label": { "type": "string" }
            },
            "required": ["left", "right"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let left = args
        .get("left")
        .and_then(|v| v.as_str())
        .context("Missing left snippet")?;
    let right = args
        .get("right")
        .and_then(|v| v.as_str())
        .context("Missing right snippet")?;
    let left_label = args
        .get("left_label")
        .and_then(|v| v.as_str())
        .unwrap_or("left");
    let right_label = args
        .get("right_label")
        .and_then(|v| v.as_str())
        .unwrap_or("right");

    let mut response = build_diff_payload(left, right, left_label, right_label);
    insert_object_field(&mut response, "left_label", json!(left_label));
    insert_object_field(&mut response, "right_label", json!(right_label));

    Ok(response)
}
