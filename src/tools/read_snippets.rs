use anyhow::{Context, Result};
use futures::future::join_all;
use serde_json::{Value, json};
use tokio::task;

use crate::tools::read_file;

pub fn schema() -> Value {
    json!({
        "name": "read_snippets",
        "description": "Read multiple file ranges in one request.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "requests": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "path": { "type": "string" },
                            "start_line": { "type": "integer" },
                            "end_line": { "type": "integer" },
                            "max_lines": { "type": "integer" },
                            "max_bytes": { "type": "integer" },
                            "include_line_numbers": { "type": "boolean" }
                        },
                        "required": ["path"]
                    },
                }
            },
            "required": ["requests"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let requests = args
        .get("requests")
        .and_then(|v| v.as_array())
        .context("Missing/Invalid 'requests' field")?;

    let jobs = requests.iter().map(|request| {
        let request = request.clone();
        task::spawn_blocking(move || execute_single_request(request))
    });

    let results = join_all(jobs)
        .await
        .into_iter()
        .map(|result| match result {
            Ok(payload) => payload,
            Err(err) => json!({
                "path": "<unknown>",
                "status": "error",
                "error": format!("read_snippets worker failed: {}", err)
            }),
        })
        .collect::<Vec<_>>();

    Ok(json!({ "results": results }))
}

fn execute_single_request(request: Value) -> Value {
    let path = request
        .get("path")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>")
        .to_string();

    match read_file::execute_sync(&request) {
        Ok(Value::Object(mut object)) => {
            object.insert("path".to_string(), json!(path));
            object.insert("status".to_string(), json!("success"));
            Value::Object(object)
        }
        Ok(other) => json!({
            "path": path,
            "status": "success",
            "result": other
        }),
        Err(err) => json!({
            "path": path,
            "status": "error",
            "error": err.to_string()
        }),
    }
}
