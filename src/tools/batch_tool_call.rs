use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::future::Future;
use std::pin::Pin;

fn extract_text_payload(result: &Value) -> Option<&str> {
    result
        .get("content")
        .and_then(|v| v.as_array())
        .and_then(|items| items.first())
        .and_then(|item| item.get("text"))
        .and_then(|v| v.as_str())
}

fn flatten_tool_result(result: Value) -> Result<Value> {
    if result
        .get("isError")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        let message = extract_text_payload(&result)
            .unwrap_or("Unknown tool error")
            .to_string();
        return Err(anyhow::anyhow!(message));
    }

    if let Some(text) = extract_text_payload(&result) {
        return match serde_json::from_str::<Value>(text) {
            Ok(parsed) => Ok(parsed),
            Err(_) => Ok(json!({ "raw_text": text })),
        };
    }

    Ok(result)
}

pub fn schema() -> Value {
    json!({
        "name": "batch_tool_call",
        "description": "Run multiple tools sequentially in one request and flatten the combined results.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "calls": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "tool": { "type": "string" },
                            "args": { "type": "object" }
                        },
                        "required": ["tool", "args"]
                    }
                }
            },
            "required": ["calls"]
        }
    })
}

pub fn execute(args: &Value) -> Pin<Box<dyn Future<Output = Result<Value>> + '_>> {
    Box::pin(async move {
        let calls = args
            .get("calls")
            .and_then(|v| v.as_array())
            .context("Missing 'calls' array")?;

        if calls.is_empty() {
            return Ok(json!({ "results": [], "total": 0 }));
        }
        if calls.len() > 20 {
            return Err(anyhow::anyhow!(
                "batch_tool_call accepts at most 20 calls per request"
            ));
        }

        let mut results = Vec::with_capacity(calls.len());

        for call in calls {
            let tool_name = call
                .get("tool")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            // Prevent recursive batch calls.
            if tool_name == "batch_tool_call" {
                results.push(json!({
                    "tool": tool_name,
                    "status": "error",
                    "error": "Recursive batch_tool_call is not allowed"
                }));
                continue;
            }

            let tool_args = call.get("args").cloned().unwrap_or(json!({}));

            let params = json!({
                "name": tool_name,
                "arguments": tool_args
            });

            match super::call_tool(params).await {
                Ok(result) => match flatten_tool_result(result) {
                    Ok(flattened) => results.push(json!({
                        "tool": tool_name,
                        "status": "ok",
                        "result": flattened
                    })),
                    Err(e) => results.push(json!({
                        "tool": tool_name,
                        "status": "error",
                        "error": e.to_string()
                    })),
                },
                Err(e) => results.push(json!({
                    "tool": tool_name,
                    "status": "error",
                    "error": format!("{}", e)
                })),
            }
        }

        Ok(json!({
            "results": results,
            "total": results.len()
        }))
    })
}
