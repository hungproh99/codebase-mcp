use anyhow::Result;
use serde_json::{Value, json};

use crate::history;

pub fn schema() -> Value {
    json!({
        "name": "history_status",
        "description": "Return undo and redo stack status for the current session.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    })
}

pub async fn execute(_args: &Value) -> Result<Value> {
    Ok(history::status_json())
}
