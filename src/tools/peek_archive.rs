use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

fn normalize_archive_entry_path(raw: &str) -> String {
    raw.replace('\\', "/").trim_start_matches("./").to_string()
}

pub fn schema() -> Value {
    json!({
        "name": "peek_archive",
        "description": "List archive entries or read a file inside an archive.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "archive_path": { "type": "string" },
                "inner_path": { "type": "string" }
            },
            "required": ["archive_path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let archive_path_str = args
        .get("archive_path")
        .and_then(|v| v.as_str())
        .context("Missing archive_path")?;
    let inner_path_opt = args
        .get("inner_path")
        .and_then(|v| v.as_str())
        .map(normalize_archive_entry_path);

    let archive_path = PathBuf::from(archive_path_str);
    if !archive_path.exists() || !archive_path.is_file() {
        return Err(anyhow::anyhow!(
            "Archive file does not exist: {}",
            archive_path_str
        ));
    }

    let ext = archive_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    let is_zip = ext == "zip" || ext == "jar" || ext == "apk";
    let is_tar_gz = archive_path_str.ends_with(".tar.gz") || archive_path_str.ends_with(".tgz");
    let is_tar = ext == "tar";

    if is_zip {
        let file = File::open(&archive_path)?;
        let mut archive = zip::ZipArchive::new(file)?;

        if let Some(inner_path) = inner_path_opt {
            let mut file_in_zip = if let Ok(file) = archive.by_name(&inner_path) {
                file
            } else {
                let mut matched_index = None;
                for i in 0..archive.len() {
                    if let Ok(file) = archive.by_index(i)
                        && normalize_archive_entry_path(file.name()) == inner_path
                    {
                        matched_index = Some(i);
                        break;
                    }
                }

                let index = matched_index
                    .context(format!("File {} not found inside archive", inner_path))?;
                archive.by_index(index)?
            };
            let mut buffer = Vec::new();

            // Limit 10MB
            file_in_zip.read_to_end(&mut buffer)?;
            if buffer.len() > 10 * 1024 * 1024 {
                return Err(anyhow::anyhow!("Inner file is too large (> 10MB)"));
            }

            let (content, enc) = crate::tools::read_file::decode_fuzzy(&buffer);

            Ok(json!({
                "archive": archive_path_str,
                "inner_file": inner_path,
                "encoding": enc,
                "content": content
            }))
        } else {
            let mut entries = Vec::new();
            for i in 0..archive.len() {
                if let Ok(file) = archive.by_index(i) {
                    entries.push(json!({
                        "name": normalize_archive_entry_path(file.name()),
                        "size": file.size(),
                        "is_dir": file.is_dir()
                    }));
                }
            }
            Ok(json!({
                "archive": archive_path_str,
                "entries": entries,
                "total_entries": entries.len()
            }))
        }
    } else if is_tar || is_tar_gz {
        let file = File::open(&archive_path)?;

        // Box the streams so the match arms return a single type.
        let reader: Box<dyn Read> = if is_tar_gz {
            Box::new(GzDecoder::new(file))
        } else {
            Box::new(file)
        };

        let mut archive = tar::Archive::new(reader);

        if let Some(inner_path) = inner_path_opt {
            let mut buf = Vec::new();
            let mut found = false;

            for entry in archive.entries()? {
                let mut entry = entry?;
                let entry_name = normalize_archive_entry_path(&entry.path()?.to_string_lossy());
                if entry_name == inner_path {
                    entry.read_to_end(&mut buf)?;
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(anyhow::anyhow!(
                    "File {} not found inside archive",
                    inner_path
                ));
            }

            if buf.len() > 10 * 1024 * 1024 {
                return Err(anyhow::anyhow!("Inner file is too large (> 10MB)"));
            }

            let (content, enc) = crate::tools::read_file::decode_fuzzy(&buf);

            Ok(json!({
                "archive": archive_path_str,
                "inner_file": inner_path,
                "encoding": enc,
                "content": content
            }))
        } else {
            let mut entries = Vec::new();
            for entry in archive.entries()? {
                let entry = entry?;
                entries.push(json!({
                    "name": normalize_archive_entry_path(&entry.path()?.to_string_lossy()),
                    "size": entry.header().size()?,
                    "is_dir": entry.header().entry_type().is_dir()
                }));
            }
            Ok(json!({
                "archive": archive_path_str,
                "entries": entries,
                "total_entries": entries.len()
            }))
        }
    } else {
        Err(anyhow::anyhow!(
            "Unsupported archive format. Supported formats: .zip, .tar, .tar.gz"
        ))
    }
}
