use anyhow::Result;
use serde_json::{Value, json};

use crate::history;

pub fn schema() -> Value {
    json!({
        "name": "undo_last_change",
        "description": "Undo the most recent tracked change in this session.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "force": {
                    "type": "boolean",
                }
            }
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let force = args.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
    match history::undo_last(force) {
        Ok(outcome) => Ok(json!({
            "success": true,
            "operation": "undo",
            "force": force,
            "entry": {
                "entry_id": outcome.entry.entry_id,
                "tool_name": outcome.entry.tool_name,
                "path": outcome.entry.path,
                "kind": outcome.entry.kind,
                "timestamp_unix": outcome.entry.timestamp_unix,
                "summary": outcome.entry.summary
            },
            "undo_depth": outcome.undo_depth,
            "redo_depth": outcome.redo_depth,
            "message": "undo applied"
        })),
        Err(err) => Ok(json!({
            "success": false,
            "operation": "undo",
            "force": force,
            "error_code": if err == "undo stack is empty" { "empty_undo_stack" } else { "undo_failed" },
            "message": err
        })),
    }
}
