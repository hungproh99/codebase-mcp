use anyhow::Result;
use serde_json::{Value, json};

use crate::history;

pub fn schema() -> Value {
    json!({
        "name": "redo_last_change",
        "description": "Redo the most recent undone tracked change in this session.",
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
    match history::redo_last(force) {
        Ok(outcome) => Ok(json!({
            "success": true,
            "operation": "redo",
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
            "message": "redo applied"
        })),
        Err(err) => Ok(json!({
            "success": false,
            "operation": "redo",
            "force": force,
            "error_code": if err == "redo stack is empty" { "empty_redo_stack" } else { "redo_failed" },
            "message": err
        })),
    }
}
