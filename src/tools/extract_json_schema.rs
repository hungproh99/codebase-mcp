use anyhow::Result;
use serde_json::{Map, Value, json};

use crate::tools::find_json_paths;

pub fn schema() -> Value {
    json!({
        "name": "extract_json_schema",
        "description": "Infer a compact JSON schema from a file or inline JSON.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "json_text": { "type": "string" }
            }
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let loader_response = find_json_paths::execute(args).await?;
    let (source_kind, source, value) = load_value(args, &loader_response)?;
    let schema = infer_schema(&value);

    Ok(json!({
        "source_kind": source_kind,
        "source": source,
        "schema": schema
    }))
}

fn load_value(args: &Value, loader_response: &Value) -> Result<(&'static str, String, Value)> {
    let source_kind = loader_response
        .get("source_kind")
        .and_then(|v| v.as_str())
        .unwrap_or("inline");
    let source = loader_response
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or("inline")
        .to_string();

    let value = if let Some(json_text) = args.get("json_text").and_then(|v| v.as_str()) {
        serde_json::from_str(json_text)?
    } else if let Some(path) = args.get("path").and_then(|v| v.as_str()) {
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)?
    } else {
        return Err(anyhow::anyhow!("Either path or json_text is required"));
    };

    Ok((
        if source_kind == "path" {
            "path"
        } else {
            "inline"
        },
        source,
        value,
    ))
}

fn infer_schema(value: &Value) -> Value {
    match value {
        Value::Null => json!({ "type": "null" }),
        Value::Bool(_) => json!({ "type": "boolean" }),
        Value::Number(number) if number.is_i64() || number.is_u64() => json!({ "type": "integer" }),
        Value::Number(_) => json!({ "type": "number" }),
        Value::String(_) => json!({ "type": "string" }),
        Value::Array(items) => infer_array_schema(items),
        Value::Object(map) => infer_object_schema(map),
    }
}

fn infer_array_schema(items: &[Value]) -> Value {
    if items.is_empty() {
        return json!({
            "type": "array",
            "items": {}
        });
    }

    let mut item_schema = infer_schema(&items[0]);
    for item in items.iter().skip(1) {
        item_schema = merge_schemas(item_schema, infer_schema(item));
    }

    json!({
        "type": "array",
        "items": item_schema
    })
}

fn infer_object_schema(map: &Map<String, Value>) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();

    for (key, value) in map {
        properties.insert(key.clone(), infer_schema(value));
        required.push(key.clone());
    }

    json!({
        "type": "object",
        "properties": properties,
        "required": required
    })
}

fn merge_schemas(left: Value, right: Value) -> Value {
    if left == right {
        return left;
    }

    let left_type = left.get("type").cloned();
    let right_type = right.get("type").cloned();

    match (left_type, right_type) {
        (Some(Value::String(left_type)), Some(Value::String(right_type)))
            if left_type == "object" && right_type == "object" =>
        {
            merge_object_schemas(left, right)
        }
        (Some(Value::String(left_type)), Some(Value::String(right_type)))
            if left_type == "array" && right_type == "array" =>
        {
            let left_items = left.get("items").cloned().unwrap_or_else(|| json!({}));
            let right_items = right.get("items").cloned().unwrap_or_else(|| json!({}));
            json!({
                "type": "array",
                "items": merge_schemas(left_items, right_items)
            })
        }
        (Some(left_type), Some(right_type)) => json!({
            "type": [left_type, right_type]
        }),
        _ => json!({}),
    }
}

fn merge_object_schemas(left: Value, right: Value) -> Value {
    let mut merged_properties = Map::new();
    let left_properties = left
        .get("properties")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let right_properties = right
        .get("properties")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();

    for key in left_properties.keys().chain(right_properties.keys()) {
        if merged_properties.contains_key(key) {
            continue;
        }

        let merged = match (left_properties.get(key), right_properties.get(key)) {
            (Some(left_schema), Some(right_schema)) => {
                merge_schemas(left_schema.clone(), right_schema.clone())
            }
            (Some(left_schema), None) => left_schema.clone(),
            (None, Some(right_schema)) => right_schema.clone(),
            (None, None) => json!({}),
        };
        merged_properties.insert(key.clone(), merged);
    }

    let left_required: Vec<String> = left
        .get("required")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    let right_required: Vec<String> = right
        .get("required")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();

    let required: Vec<String> = left_required
        .into_iter()
        .filter(|key| right_required.contains(key))
        .collect();

    json!({
        "type": "object",
        "properties": merged_properties,
        "required": required
    })
}
