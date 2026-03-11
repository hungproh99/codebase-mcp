use anyhow::Result;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::history::{
    attach_history_metadata, capture_snapshot, file_snapshot, missing_snapshot, no_history,
    record_change,
};
use crate::security::path_guard::{GUARD, Tier};

fn error_reason(error_code: &str) -> &'static str {
    match error_code {
        "invalid_path" => "Path argument is missing or invalid.",
        "path_blocked" => "Path is blocked by server policy.",
        "invalid_encoding" => "Requested encoding is not supported.",
        "invalid_line_ending" => "Requested line ending mode is invalid.",
        "encoding_error" => "Content cannot be encoded with requested encoding.",
        "path_is_directory" => "Target path points to a directory, not a file.",
        "already_exists" => "Target file already exists.",
        "parent_missing" => "Parent directory does not exist.",
        "permission_denied" => "Operation was denied by filesystem permissions.",
        "not_found" => "Target path was not found.",
        "invalid_input" => "Input is invalid for the requested operation.",
        "file_locked" => "File is locked by another process.",
        "io_error" => "Filesystem I/O error occurred.",
        _ => "Unknown error.",
    }
}

pub fn schema() -> Value {
    json!({
        "name": "create_file",
        "description": "Create or overwrite a file with optional parent creation, encoding, and line endings.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "content": { "type": "string" },
                "overwrite": { "type": "boolean" },
                "create_parents": { "type": "boolean" },
                "target_encoding": { "type": "string", "enum": ["UTF-8", "Windows-1252"] },
                "target_line_ending": { "type": "string", "enum": ["lf", "crlf"] }
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
            std::io::ErrorKind::NotFound => "not_found",
            std::io::ErrorKind::AlreadyExists => "already_exists",
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

fn normalize_line_endings(
    content: &str,
    target_line_ending: Option<&str>,
) -> std::result::Result<(String, &'static str), &'static str> {
    match target_line_ending.map(|v| v.to_ascii_lowercase()) {
        None => Ok((content.to_string(), "preserve")),
        Some(v) if v == "lf" => Ok((content.replace("\r\n", "\n"), "lf")),
        Some(v) if v == "crlf" => {
            let normalized = content.replace("\r\n", "\n").replace('\n', "\r\n");
            Ok((normalized, "crlf"))
        }
        _ => Err("target_line_ending must be 'lf' or 'crlf'"),
    }
}

fn encode_content(
    content: &str,
    target_encoding: &str,
) -> std::result::Result<Vec<u8>, &'static str> {
    match target_encoding {
        "UTF-8" => Ok(content.as_bytes().to_vec()),
        "WINDOWS-1252" => {
            let (cow, _, has_unmappable) = encoding_rs::WINDOWS_1252.encode(content);
            if has_unmappable {
                return Err("content cannot be losslessly converted to Windows-1252");
            }
            Ok(cow.into_owned())
        }
        _ => Err("target_encoding must be UTF-8 or Windows-1252"),
    }
}

fn line_ending_metadata(content: &str) -> Option<String> {
    if content.contains("\r\n") {
        Some("crlf".to_string())
    } else if content.contains('\n') {
        Some("lf".to_string())
    } else {
        None
    }
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

    let overwrite = args
        .get("overwrite")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let create_parents = args
        .get("create_parents")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let content = args.get("content").and_then(|v| v.as_str()).unwrap_or("");

    let target_encoding = args
        .get("target_encoding")
        .and_then(|v| v.as_str())
        .unwrap_or("UTF-8")
        .to_ascii_uppercase();

    let normalized_encoding = match target_encoding.as_str() {
        "UTF-8" | "WINDOWS-1252" => target_encoding,
        _ => {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "invalid_encoding",
                "target_encoding must be UTF-8 or Windows-1252",
            ));
        }
    };

    let (normalized_content, line_ending_applied) = match normalize_line_endings(
        content,
        args.get("target_line_ending").and_then(|v| v.as_str()),
    ) {
        Ok(v) => v,
        Err(msg) => {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "invalid_line_ending",
                msg,
            ));
        }
    };

    let final_bytes = match encode_content(&normalized_content, &normalized_encoding) {
        Ok(bytes) => bytes,
        Err(msg) => {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "encoding_error",
                msg,
            ));
        }
    };

    let existed_before = path.exists();
    let before_snapshot = if existed_before {
        capture_snapshot(&path)
    } else {
        Ok(missing_snapshot())
    };
    if existed_before {
        if path.is_dir() {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "path_is_directory",
                "target path points to a directory",
            ));
        }

        if !overwrite {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "already_exists",
                "file already exists (set overwrite=true to replace it)",
            ));
        }
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        if !create_parents {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "parent_missing",
                "parent directory does not exist (set create_parents=true)",
            ));
        }

        if let Err(err) = fs::create_dir_all(parent) {
            return Ok(io_error_response(
                &path,
                &canonical_from_guard,
                "create parent directories",
                &err,
            ));
        }
    }

    let mut open_options = OpenOptions::new();
    open_options.write(true);
    if overwrite {
        open_options.create(true).truncate(true);
    } else {
        open_options.create_new(true);
    }

    let mut file = match open_options.open(&path) {
        Ok(f) => f,
        Err(err) => {
            return Ok(io_error_response(
                &path,
                &canonical_from_guard,
                "open file for writing",
                &err,
            ));
        }
    };

    if let Err(err) = file.write_all(&final_bytes) {
        return Ok(io_error_response(
            &path,
            &canonical_from_guard,
            "write file",
            &err,
        ));
    }

    let canonical_written = std::fs::canonicalize(&path).unwrap_or(canonical_from_guard);
    let history_outcome = match before_snapshot {
        Ok(before) => record_change(
            "create_file",
            &path,
            before,
            file_snapshot(
                final_bytes.clone(),
                Some(normalized_encoding.clone()),
                line_ending_metadata(&normalized_content),
            ),
            if existed_before {
                "overwrite file"
            } else {
                "create file"
            },
        ),
        Err(reason) => no_history(reason),
    };

    let mut response = json!({
        "success": true,
        "path": path.to_string_lossy(),
        "canonical_path": canonical_written.to_string_lossy(),
        "created": !existed_before,
        "overwritten": existed_before,
        "bytes_written": final_bytes.len(),
        "target_encoding": normalized_encoding,
        "line_ending": line_ending_applied,
        "message": if existed_before { "file overwritten" } else { "file created" }
    });
    attach_history_metadata(&mut response, &history_outcome);
    Ok(response)
}
