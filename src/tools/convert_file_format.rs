use anyhow::Result;
use encoding_rs::WINDOWS_1252;
use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::history::{attach_history_metadata, file_snapshot, no_history, record_change};
use crate::tools::read_file::decode_fuzzy;

enum TargetEncoding {
    Utf8,
    Utf16Le,
    Windows1252,
}

impl TargetEncoding {
    fn canonical_name(&self) -> &'static str {
        match self {
            Self::Utf8 => "UTF-8",
            Self::Utf16Le => "UTF-16LE",
            Self::Windows1252 => "WINDOWS-1252",
        }
    }
}

fn parse_target_encoding(raw: &str) -> Result<TargetEncoding> {
    match raw.trim().to_ascii_uppercase().as_str() {
        "UTF-8" | "UTF8" => Ok(TargetEncoding::Utf8),
        "UTF-16LE" | "UTF16LE" => Ok(TargetEncoding::Utf16Le),
        "WINDOWS-1252" | "CP1252" => Ok(TargetEncoding::Windows1252),
        other => Err(anyhow::anyhow!(
            "Unsupported target_encoding '{}'. Supported values: UTF-8, UTF-16LE, Windows-1252",
            other
        )),
    }
}

fn encode_utf16le_with_bom(content: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + content.len() * 2);
    bytes.extend_from_slice(&[0xFF, 0xFE]);
    for unit in content.encode_utf16() {
        bytes.extend_from_slice(&unit.to_le_bytes());
    }
    bytes
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

pub fn schema() -> Value {
    json!({
        "name": "convert_file_format",
        "description": "Rewrite a file with normalized encoding and line endings.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "target_encoding": {
                    "type": "string",
                    "enum": ["UTF-8", "UTF-16LE", "Windows-1252"],
                },
                "target_line_ending": {
                    "type": "string",
                    "enum": ["lf", "crlf"],
                }
            },
            "required": ["path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    if path_str.is_empty() {
        return Err(anyhow::anyhow!("Missing path argument"));
    }
    let path = PathBuf::from(path_str);

    if !path.exists() || !path.is_file() {
        return Err(anyhow::anyhow!(
            "File does not exist or is not a file: {}",
            path_str
        ));
    }

    let target_encoding = parse_target_encoding(
        args.get("target_encoding")
            .and_then(|v| v.as_str())
            .unwrap_or("UTF-8"),
    )?;
    let target_line_ending = args
        .get("target_line_ending")
        .and_then(|v| v.as_str())
        .map(|s| s.to_lowercase());

    let read_result = File::open(&path).and_then(|mut f| {
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(buf)
    });

    let buffer = match read_result {
        Ok(b) => b,
        Err(e) => {
            if let Some(os_err) = e.raw_os_error()
                && os_err == 32
            {
                return Ok(json!({
                    "isError": true,
                    "content": [{
                        "type": "text",
                        "text": "File is locked by another process (OS error 32). Please check which process is holding the file handle before retrying."
                    }]
                }));
            }
            return Err(e.into());
        }
    };

    let (mut content, previous_encoding) = decode_fuzzy(&buffer);

    if let Some(le) = target_line_ending {
        if le == "lf" {
            content = content.replace("\r\n", "\n");
        } else if le == "crlf" {
            content = content.replace("\r\n", "\n").replace('\n', "\r\n");
        }
    }

    let final_line_ending = line_ending_metadata(&content);
    let final_bytes = match target_encoding {
        TargetEncoding::Utf8 => content.into_bytes(),
        TargetEncoding::Utf16Le => encode_utf16le_with_bom(&content),
        TargetEncoding::Windows1252 => {
            let (cow, _, has_unmappable) = WINDOWS_1252.encode(&content);
            if has_unmappable {
                return Err(anyhow::anyhow!(
                    "Content cannot be losslessly converted to Windows-1252"
                ));
            }
            cow.into_owned()
        }
    };
    let history_outcome = if final_bytes == buffer {
        no_history("no filesystem change")
    } else {
        record_change(
            "convert_file_format",
            &path,
            file_snapshot(
                buffer.clone(),
                Some(previous_encoding.to_string()),
                line_ending_metadata(&decode_fuzzy(&buffer).0),
            ),
            file_snapshot(
                final_bytes.clone(),
                Some(target_encoding.canonical_name().to_string()),
                final_line_ending,
            ),
            "convert file format",
        )
    };

    let write_result = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&path)
        .and_then(|mut f| f.write_all(&final_bytes));

    match write_result {
        Ok(_) => {
            let mut response = json!({
                "success": true,
                "previous_encoding": previous_encoding,
                "target_encoding": target_encoding.canonical_name(),
                "file_size": final_bytes.len(),
                "message": format!(
                    "Successfully converted file to {}",
                    target_encoding.canonical_name()
                )
            });
            attach_history_metadata(&mut response, &history_outcome);
            Ok(response)
        }
        Err(e) => {
            if let Some(os_err) = e.raw_os_error()
                && os_err == 32
            {
                return Ok(json!({
                    "isError": true,
                    "content": [{
                        "type": "text",
                        "text": "File is locked by another process (OS error 32) when trying to write. Please check handle."
                    }]
                }));
            }
            Err(e.into())
        }
    }
}
