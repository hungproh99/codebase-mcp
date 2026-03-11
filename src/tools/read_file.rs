use anyhow::{Context, Result};
use encoding_rs::{UTF_16BE, UTF_16LE, WINDOWS_1252};
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use crate::common::insert_object_field;

pub fn schema() -> Value {
    json!({
        "name": "read_file_range",
        "description": "Read a file or line range with encoding detection and truncation metadata.",
        "inputSchema": {
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
        }
    })
}

struct LimitedContent {
    content: String,
    returned_lines: usize,
    truncated: bool,
    omitted_lines: usize,
    next_start_line: Option<usize>,
    end_line: usize,
}

pub fn decode_fuzzy(buffer: &[u8]) -> (String, &'static str) {
    if buffer.starts_with(&[0xFF, 0xFE]) {
        let (cow, _, _) = UTF_16LE.decode(&buffer[2..]);
        return (cow.into_owned(), "UTF-16LE");
    }

    if buffer.starts_with(&[0xFE, 0xFF]) {
        let (cow, _, _) = UTF_16BE.decode(&buffer[2..]);
        return (cow.into_owned(), "UTF-16BE");
    }

    match std::str::from_utf8(buffer) {
        Ok(s) => (s.to_string(), "UTF-8"),
        Err(_) => {
            let (cow, encoding, _) = WINDOWS_1252.decode(buffer);
            (cow.into_owned(), encoding.name())
        }
    }
}

pub(crate) fn has_utf16_bom(buffer: &[u8]) -> bool {
    buffer.starts_with(&[0xFF, 0xFE]) || buffer.starts_with(&[0xFE, 0xFF])
}

pub(crate) fn is_probably_binary(buffer: &[u8]) -> bool {
    !has_utf16_bom(buffer) && buffer.contains(&b'\x00')
}

pub(crate) fn count_text_lines(content: &str) -> usize {
    if content.is_empty() {
        return 0;
    }

    content.split_terminator('\n').count()
}

fn split_text_lines(content: &str) -> Vec<&str> {
    if content.is_empty() {
        Vec::new()
    } else {
        content.split_terminator('\n').collect()
    }
}

pub async fn execute(args: &Value) -> Result<Value> {
    execute_sync(args)
}

pub(crate) fn execute_sync(args: &Value) -> Result<Value> {
    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .context("Missing path")?;
    let path = PathBuf::from(path_str);

    if !path.exists() || !path.is_file() {
        return Err(anyhow::anyhow!(
            "File does not exist or is not a file: {}",
            path_str
        ));
    }

    let meta = std::fs::metadata(&path)?;
    let size_mb = meta.len() / 1024 / 1024;
    if size_mb > 10 {
        return Err(anyhow::anyhow!(
            "File is too large ({}MB > 10MB limit)",
            size_mb
        ));
    }

    let start_line = args.get("start_line").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
    let end_line = args
        .get("end_line")
        .and_then(|v| v.as_u64())
        .unwrap_or(u64::MAX) as usize;
    let max_lines = args
        .get("max_lines")
        .and_then(|v| v.as_u64())
        .map(|value| value as usize);
    let max_bytes = args
        .get("max_bytes")
        .and_then(|v| v.as_u64())
        .map(|value| value as usize);
    let include_line_numbers = args
        .get("include_line_numbers")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    if start_line == 0 {
        return Err(anyhow::anyhow!("start_line must be >= 1"));
    }

    let mut file = File::open(&path)?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let (content, encoding) = decode_fuzzy(&buffer);
    let lines = split_text_lines(&content);
    let total_lines = lines.len();
    let ends_with_newline = content.ends_with('\n');

    let actual_start = std::cmp::min(start_line - 1, total_lines);
    let actual_end = std::cmp::min(end_line, total_lines);
    let selected_lines = &lines[actual_start..actual_end];
    let limited = apply_limits(
        selected_lines,
        actual_start + 1,
        include_line_numbers,
        max_lines,
        max_bytes,
        ends_with_newline && actual_end == total_lines,
    );

    let mut response = json!({
        "content": limited.content,
        "start_line": actual_start + 1,
        "end_line": limited.end_line,
        "total_lines": total_lines,
        "encoding": encoding,
        "file_size_bytes": meta.len(),
        "truncated": limited.truncated,
        "omitted_lines": limited.omitted_lines,
        "returned_lines": limited.returned_lines
    });

    if let Some(next_start_line) = limited.next_start_line {
        insert_object_field(&mut response, "next_start_line", json!(next_start_line));
    }

    Ok(response)
}

fn apply_limits(
    lines: &[&str],
    first_line_number: usize,
    include_line_numbers: bool,
    max_lines: Option<usize>,
    max_bytes: Option<usize>,
    append_terminal_newline: bool,
) -> LimitedContent {
    let line_limit = max_lines.unwrap_or(usize::MAX);
    let byte_limit = max_bytes.unwrap_or(usize::MAX);
    let mut rendered_lines = Vec::new();
    let mut used_bytes = 0usize;

    for (index, line) in lines.iter().enumerate() {
        if rendered_lines.len() >= line_limit {
            break;
        }

        let line_number = first_line_number + index;
        let rendered = render_line(line_number, line, include_line_numbers);
        let rendered_bytes = rendered.len();
        let separator_bytes = if rendered_lines.is_empty() { 0 } else { 1 };

        if used_bytes
            .saturating_add(separator_bytes)
            .saturating_add(rendered_bytes)
            > byte_limit
        {
            break;
        }

        used_bytes += separator_bytes + rendered_bytes;
        rendered_lines.push(rendered);
    }

    let returned_lines = rendered_lines.len();
    let omitted_lines = lines.len().saturating_sub(returned_lines);
    let truncated = omitted_lines > 0;
    let next_start_line = if truncated {
        Some(first_line_number + returned_lines)
    } else {
        None
    };
    let end_line = returned_lines
        .checked_sub(1)
        .map(|offset| first_line_number + offset)
        .unwrap_or_else(|| first_line_number.saturating_sub(1));

    let mut content = rendered_lines.join("\n");
    if append_terminal_newline && !truncated && returned_lines > 0 {
        content.push('\n');
    }

    LimitedContent {
        content,
        returned_lines,
        truncated,
        omitted_lines,
        next_start_line,
        end_line,
    }
}

fn render_line(line_number: usize, line: &str, include_line_numbers: bool) -> String {
    if include_line_numbers {
        format!("{}: {}", line_number, line)
    } else {
        line.to_string()
    }
}
