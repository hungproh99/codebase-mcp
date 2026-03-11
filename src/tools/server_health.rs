use crate::common::unix_timestamp_secs;
use crate::indexer::{get_active_runtime_snapshot, get_runtime_snapshots, stale_index_after_secs};
use crate::version::SERVER_VERSION;
use anyhow::Result;
use serde_json::{Value, json};

lazy_static::lazy_static! {
    pub static ref START_TIME: u64 = unix_timestamp_secs();
}

pub fn schema() -> Value {
    json!({
        "name": "server_health",
        "description": "Check server uptime and indexing health.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    })
}

pub async fn execute(_args: &Value) -> Result<Value> {
    let now = unix_timestamp_secs();
    let uptime_secs = now - *START_TIME;

    let runtimes = get_runtime_snapshots();
    let active_runtime = get_active_runtime_snapshot();
    let primary_runtime = active_runtime.as_ref().or_else(|| runtimes.first());
    let total_indexed_entries: usize = runtimes
        .iter()
        .map(|runtime| runtime.indexed_entries_count)
        .sum();
    let primary_workspace_root = primary_runtime.map(|runtime| runtime.workspace_root.clone());
    let primary_workspace_source = primary_runtime.map(|runtime| runtime.workspace_source.clone());
    let primary_index_file = primary_runtime.map(|runtime| runtime.index_file.clone());
    let primary_loaded_from_disk = primary_runtime.map(|runtime| runtime.loaded_from_disk);
    let primary_scan_complete = primary_runtime.map(|runtime| runtime.scan_complete);
    let primary_loaded_entries = primary_runtime.map(|runtime| runtime.last_loaded_entries);
    let primary_last_persisted_entries =
        primary_runtime.map(|runtime| runtime.last_persisted_entries);
    let primary_last_persisted_at = primary_runtime.and_then(|runtime| runtime.last_persisted_at);
    let primary_last_scan_completed_at =
        primary_runtime.and_then(|runtime| runtime.last_scan_completed_at);
    let primary_last_refresh_requested_at =
        primary_runtime.and_then(|runtime| runtime.last_refresh_requested_at);
    let primary_last_request_source =
        primary_runtime.and_then(|runtime| runtime.last_request_source.clone());
    let primary_last_error = primary_runtime.and_then(|runtime| runtime.last_error.clone());
    let primary_index_kind = primary_runtime.map(|runtime| runtime.index_kind);
    let primary_indexed_entries = primary_runtime.map(|runtime| runtime.indexed_entries_count);
    let index_status = if runtimes.is_empty() {
        "disabled"
    } else if total_indexed_entries > 0 {
        "active"
    } else {
        "idle"
    };

    Ok(json!({
        "status": "healthy",
        "uptime_seconds": uptime_secs,
        "index_status": index_status,
        "cached_files_count": total_indexed_entries,
        "indexed_entries_count": total_indexed_entries,
        "index_workspace_count": runtimes.len(),
        "index_workspaces": runtimes,
        "active_index_workspace_root": active_runtime.as_ref().map(|runtime| runtime.workspace_root.clone()),
        "active_index_workspace_source": active_runtime.as_ref().map(|runtime| runtime.workspace_source.clone()),
        "index_workspace_root": primary_workspace_root,
        "index_workspace_source": primary_workspace_source,
        "index_kind": primary_index_kind,
        "index_file": primary_index_file,
        "index_loaded_from_disk": primary_loaded_from_disk,
        "index_scan_complete": primary_scan_complete,
        "index_loaded_entries": primary_loaded_entries,
        "index_indexed_entries": primary_indexed_entries,
        "index_last_persisted_entries": primary_last_persisted_entries,
        "index_last_persisted_at": primary_last_persisted_at,
        "index_last_scan_completed_at": primary_last_scan_completed_at,
        "index_last_refresh_requested_at": primary_last_refresh_requested_at,
        "index_last_request_source": primary_last_request_source,
        "index_last_error": primary_last_error,
        "index_stale_after_seconds": stale_index_after_secs(),
        "version": SERVER_VERSION,
        "transport": "stdio (JSON-RPC)"
    }))
}
