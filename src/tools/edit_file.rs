use anyhow::Result;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::history::{
    attach_history_metadata, file_snapshot, missing_snapshot, no_history, record_change,
};
use crate::security::path_guard::{GUARD, Tier};
use crate::tools::read_file::decode_fuzzy;

const MAX_EDIT_FILE_BYTES: u64 = 10 * 1024 * 1024;

fn error_reason(error_code: &str) -> &'static str {
    match error_code {
        "invalid_path" => "Path argument is missing or invalid.",
        "path_blocked" => "Path is blocked by server policy.",
        "invalid_mode" => "Mode is not one of supported values.",
        "path_is_directory" => "Target path points to a directory, not a file.",
        "file_too_large" => "File exceeds allowed size for this operation.",
        "file_not_found" => "Target file does not exist.",
        "missing_content" => "Required content argument is missing for selected mode.",
        "missing_find" => "Required find argument is missing or empty.",
        "no_match" => "Find text was not found in current file content.",
        "replacement_mismatch" => "Actual replacements do not match expected count.",
        "invalid_line_ending" => "Requested line ending mode is invalid.",
        "invalid_encoding" => "Requested encoding is not supported.",
        "encoding_error" => "Content cannot be encoded with requested encoding.",
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
        "name": "edit_file",
        "description": "Edit a file using replace, append, prepend, or find-replace modes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "mode": {
                    "type": "string",
                    "enum": ["replace", "append", "prepend", "find_replace"],
                },
                "content": { "type": "string" },
                "find": { "type": "string" },
                "replace": { "type": "string" },
                "replace_all": { "type": "boolean" },
                "expected_replacements": { "type": "integer" },
                "create_if_missing": { "type": "boolean" },
                "create_parents": { "type": "boolean" },
                "target_encoding": { "type": "string", "enum": ["UTF-8", "Windows-1252"] },
                "target_line_ending": {
                    "type": "string",
                    "enum": ["preserve", "lf", "crlf"],
                }
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
    target_line_ending: &str,
) -> std::result::Result<String, &'static str> {
    match target_line_ending {
        "preserve" => Ok(content.to_string()),
        "lf" => Ok(content.replace("\r\n", "\n")),
        "crlf" => {
            let normalized = content.replace("\r\n", "\n").replace('\n', "\r\n");
            Ok(normalized)
        }
        _ => Err("target_line_ending must be preserve, lf, or crlf"),
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

fn normalize_encoding_label(raw: &str) -> &'static str {
    if raw.eq_ignore_ascii_case("windows-1252") {
        "WINDOWS-1252"
    } else {
        "UTF-8"
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

    let mode = args
        .get("mode")
        .and_then(|v| v.as_str())
        .unwrap_or("replace")
        .to_ascii_lowercase();

    if !["replace", "append", "prepend", "find_replace"].contains(&mode.as_str()) {
        return Ok(error_response(
            &path,
            &canonical_from_guard,
            "invalid_mode",
            "mode must be one of: replace, append, prepend, find_replace",
        ));
    }

    let create_if_missing = args
        .get("create_if_missing")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let create_parents = args
        .get("create_parents")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let existed_before = path.exists();

    let (old_content, old_bytes, previous_encoding): (String, Vec<u8>, Option<String>) =
        if existed_before {
            if path.is_dir() {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "path_is_directory",
                    "target path points to a directory",
                ));
            }

            let meta = match fs::metadata(&path) {
                Ok(m) => m,
                Err(err) => {
                    return Ok(io_error_response(
                        &path,
                        &canonical_from_guard,
                        "read metadata",
                        &err,
                    ));
                }
            };

            if meta.len() > MAX_EDIT_FILE_BYTES {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "file_too_large",
                    format!(
                        "file is too large for edit_file ({} bytes > {} bytes)",
                        meta.len(),
                        MAX_EDIT_FILE_BYTES
                    ),
                ));
            }

            let bytes = match fs::read(&path) {
                Ok(b) => b,
                Err(err) => {
                    return Ok(io_error_response(
                        &path,
                        &canonical_from_guard,
                        "read file",
                        &err,
                    ));
                }
            };

            let (decoded, detected_encoding) = decode_fuzzy(&bytes);
            (decoded, bytes, Some(detected_encoding.to_string()))
        } else {
            if !create_if_missing {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "file_not_found",
                    "file does not exist (set create_if_missing=true to create it)",
                ));
            }

            (String::new(), Vec::new(), None)
        };

    let has_content = args.get("content").is_some();
    let content = args.get("content").and_then(|v| v.as_str()).unwrap_or("");

    let mut replacements_applied: usize = 0;

    let mut new_content = match mode.as_str() {
        "replace" => {
            if !has_content {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "missing_content",
                    "content is required when mode=replace",
                ));
            }
            content.to_string()
        }
        "append" => {
            if !has_content {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "missing_content",
                    "content is required when mode=append",
                ));
            }
            format!("{}{}", old_content, content)
        }
        "prepend" => {
            if !has_content {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "missing_content",
                    "content is required when mode=prepend",
                ));
            }
            format!("{}{}", content, old_content)
        }
        "find_replace" => {
            let find = args.get("find").and_then(|v| v.as_str()).unwrap_or("");
            if find.is_empty() {
                return Ok(error_response(
                    &path,
                    &canonical_from_guard,
                    "missing_find",
                    "find is required and cannot be empty when mode=find_replace",
                ));
            }
            let replace = args.get("replace").and_then(|v| v.as_str()).unwrap_or("");
            let replace_all = args
                .get("replace_all")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

            if replace_all {
                replacements_applied = old_content.matches(find).count();
                if replacements_applied == 0 {
                    return Ok(error_response(
                        &path,
                        &canonical_from_guard,
                        "no_match",
                        "find text was not found in the file",
                    ));
                }
                old_content.replace(find, replace)
            } else {
                if !old_content.contains(find) {
                    return Ok(error_response(
                        &path,
                        &canonical_from_guard,
                        "no_match",
                        "find text was not found in the file",
                    ));
                }
                replacements_applied = 1;
                old_content.replacen(find, replace, 1)
            }
        }
        _ => old_content.clone(),
    };

    if let Some(expected) = args
        .get("expected_replacements")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        && expected != replacements_applied
    {
        return Ok(json!({
            "success": false,
            "path": path.to_string_lossy(),
            "canonical_path": canonical_from_guard.to_string_lossy(),
            "error_code": "replacement_mismatch",
            "reason": error_reason("replacement_mismatch"),
            "message": format!(
                "expected {} replacements but got {}",
                expected,
                replacements_applied
            ),
            "expected_replacements": expected,
            "actual_replacements": replacements_applied
        }));
    }

    let target_line_ending = args
        .get("target_line_ending")
        .and_then(|v| v.as_str())
        .unwrap_or("preserve")
        .to_ascii_lowercase();

    new_content = match normalize_line_endings(&new_content, &target_line_ending) {
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

    let requested_encoding = args
        .get("target_encoding")
        .and_then(|v| v.as_str())
        .map(|v| v.to_ascii_uppercase());

    let target_encoding = match requested_encoding {
        Some(enc) if enc == "UTF-8" || enc == "WINDOWS-1252" => enc,
        Some(_) => {
            return Ok(error_response(
                &path,
                &canonical_from_guard,
                "invalid_encoding",
                "target_encoding must be UTF-8 or Windows-1252",
            ));
        }
        None => previous_encoding
            .as_deref()
            .map(normalize_encoding_label)
            .unwrap_or("UTF-8")
            .to_string(),
    };

    let final_bytes = match encode_content(&new_content, &target_encoding) {
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

    let changed = !existed_before || old_bytes != final_bytes;
    if !changed {
        let canonical_after = std::fs::canonicalize(&path).unwrap_or(canonical_from_guard);
        let mut response = json!({
            "success": true,
            "path": path.to_string_lossy(),
            "canonical_path": canonical_after.to_string_lossy(),
            "mode": mode,
            "file_existed_before": true,
            "file_created": false,
            "changed": false,
            "replacements": replacements_applied,
            "bytes_before": old_bytes.len(),
            "bytes_written": old_bytes.len(),
            "previous_encoding": previous_encoding,
            "target_encoding": target_encoding,
            "line_ending": target_line_ending,
            "message": "no content changes detected"
        });
        attach_history_metadata(&mut response, &no_history("no filesystem change"));
        return Ok(response);
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
    open_options.write(true).truncate(true);
    if !existed_before {
        open_options.create(true);
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

    let canonical_after = std::fs::canonicalize(&path).unwrap_or(canonical_from_guard);
    let before_snapshot = if existed_before {
        file_snapshot(
            old_bytes.clone(),
            previous_encoding
                .clone()
                .map(|v| normalize_encoding_label(&v).to_string()),
            line_ending_metadata(&old_content),
        )
    } else {
        missing_snapshot()
    };
    let after_snapshot = file_snapshot(
        final_bytes.clone(),
        Some(target_encoding.clone()),
        line_ending_metadata(&new_content),
    );
    let history_outcome = record_change(
        "edit_file",
        &path,
        before_snapshot,
        after_snapshot,
        if existed_before {
            "edit file"
        } else {
            "create file via edit_file"
        },
    );

    let mut response = json!({
        "success": true,
        "path": path.to_string_lossy(),
        "canonical_path": canonical_after.to_string_lossy(),
        "mode": mode,
        "file_existed_before": existed_before,
        "file_created": !existed_before,
        "changed": true,
        "replacements": replacements_applied,
        "bytes_before": old_bytes.len(),
        "bytes_written": final_bytes.len(),
        "previous_encoding": previous_encoding,
        "target_encoding": target_encoding,
        "line_ending": target_line_ending,
        "message": if existed_before { "file updated" } else { "file created and updated" }
    });
    attach_history_metadata(&mut response, &history_outcome);
    Ok(response)
}
