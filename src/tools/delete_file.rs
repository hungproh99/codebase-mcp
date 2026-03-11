use anyhow::Result;
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};

use crate::history::{
    attach_history_metadata, capture_snapshot, missing_snapshot, no_history, record_change,
};
use crate::security::path_guard::{GUARD, Tier};

fn error_reason(error_code: &str) -> &'static str {
    match error_code {
        "invalid_path" => "Path argument is missing or invalid.",
        "path_blocked" => "Path is blocked by server policy.",
        "file_not_found" => "Target file does not exist.",
        "path_is_directory" => "Target path points to a directory, not a file.",
        "permission_denied" => "Operation was denied by filesystem permissions.",
        "invalid_input" => "Input is invalid for the requested operation.",
        "file_locked" => "File is locked by another process.",
        "io_error" => "Filesystem I/O error occurred.",
        _ => "Unknown error.",
    }
}

pub fn schema() -> Value {
    json!({
        "name": "delete_file",
        "description": "Delete a file with structured success and error metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "missing_ok": { "type": "boolean" }
            },
            "required": ["path"]
        }
    })
}

fn error_response(
    path: &Path,
    canonical: &Path,
    error_code: &str,
    message: impl Into<String>,
) -> Value {
    json!({
        "success": false,
        "path": path.to_string_lossy(),
        "canonical_path": canonical.to_string_lossy(),
        "error_code": error_code,
        "reason": error_reason(error_code),
        "message": message.into()
    })
}

fn io_error_response(
    path: &Path,
    canonical: &Path,
    operation: &str,
    err: &std::io::Error,
) -> Value {
    let error_code = if err.raw_os_error() == Some(32) {
        "file_locked"
    } else {
        match err.kind() {
            std::io::ErrorKind::PermissionDenied => "permission_denied",
            std::io::ErrorKind::NotFound => "file_not_found",
            std::io::ErrorKind::InvalidInput => "invalid_input",
            _ => "io_error",
        }
    };

    json!({
        "success": false,
        "path": path.to_string_lossy(),
        "canonical_path": canonical.to_string_lossy(),
        "error_code": error_code,
        "reason": error_reason(error_code),
        "operation": operation,
        "message": format!("Failed to {}: {}", operation, err),
        "io_kind": format!("{:?}", err.kind()),
        "os_error": err.raw_os_error()
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");

    if path_str.is_empty() {
        let empty = PathBuf::from("");
        return Ok(error_response(
            Path::new(path_str),
            empty.as_path(),
            "invalid_path",
            "path is required",
        ));
    }

    let path = PathBuf::from(path_str);
    let (canonical_from_guard, tier, reason) = GUARD.check_path(&path);
    if tier == Tier::Blocked {
        return Ok(error_response(
            &path,
            &canonical_from_guard,
            "path_blocked",
            reason.unwrap_or_else(|| "path is blocked by server policy".to_string()),
        ));
    }

    let missing_ok = args
        .get("missing_ok")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if !path.exists() {
        if missing_ok {
            let mut response = json!({
                "success": true,
                "path": path.to_string_lossy(),
                "canonical_path": canonical_from_guard.to_string_lossy(),
                "deleted": false,
                "existed_before": false,
                "message": "path does not exist; nothing deleted"
            });
            attach_history_metadata(&mut response, &no_history("no filesystem change"));
            return Ok(response);
        }

        return Ok(error_response(
            &path,
            &canonical_from_guard,
            "file_not_found",
            "file does not exist",
        ));
    }

    if path.is_dir() {
        return Ok(error_response(
            &path,
            &canonical_from_guard,
            "path_is_directory",
            "target path is a directory; delete_file only removes files",
        ));
    }

    let size_before = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let before_snapshot = capture_snapshot(&path);

    if let Err(err) = fs::remove_file(&path) {
        return Ok(io_error_response(
            &path,
            &canonical_from_guard,
            "delete file",
            &err,
        ));
    }

    let history_outcome = match before_snapshot {
        Ok(before) => record_change(
            "delete_file",
            &path,
            before,
            missing_snapshot(),
            "delete file",
        ),
        Err(reason) => no_history(reason),
    };

    let mut response = json!({
        "success": true,
        "path": path.to_string_lossy(),
        "canonical_path": canonical_from_guard.to_string_lossy(),
        "deleted": true,
        "existed_before": true,
        "bytes_removed": size_before,
        "message": "file deleted"
    });
    attach_history_metadata(&mut response, &history_outcome);
    Ok(response)
}
