use anyhow::{Context, Result};
use ignore::WalkBuilder;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use tokio::task;

/// Max files to stat before returning a partial result.
const MAX_FILES: usize = 200_000;

pub fn schema() -> Value {
    json!({
        "name": "workspace_stats",
        "description": "Summarize file, line, and language counts for a workspace path.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" }
            },
            "required": ["path"]
        }
    })
}

fn get_lang_from_ext(ext: &str) -> &'static str {
    match ext {
        "rs" => "Rust",
        "js" | "jsx" | "mjs" | "cjs" => "JavaScript",
        "ts" | "tsx" => "TypeScript",
        "py" | "pyi" => "Python",
        "go" => "Go",
        "java" => "Java",
        "kt" | "kts" => "Kotlin",
        "c" | "h" => "C",
        "cpp" | "cc" | "cxx" | "hpp" | "hh" | "hxx" | "inl" | "inc" => "C++",
        "cs" => "C#",
        "swift" => "Swift",
        "dart" => "Dart",
        "rb" => "Ruby",
        "php" => "PHP",
        "html" | "htm" => "HTML",
        "css" | "scss" | "sass" | "less" => "CSS",
        "md" | "rst" => "Markdown",
        "json" => "JSON",
        "yaml" | "yml" => "YAML",
        "toml" => "TOML",
        "xml" => "XML",
        "sh" | "bash" | "zsh" => "Shell",
        "ps1" | "bat" | "cmd" => "PowerShell/Batch",
        "sql" => "SQL",
        "proto" => "Protobuf",
        "gn" | "gni" => "GN",
        "gyp" | "gypi" => "GYP",
        "cmake" | "mk" => "CMake/Make",
        "lua" => "Lua",
        "vue" | "svelte" => "Vue/Svelte",
        "m" | "mm" => "Objective-C",
        _ => "Other",
    }
}

fn count_lines_in_file(path: &std::path::Path) -> u64 {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(_) => return 0,
    };

    let mut buffer = [0u8; 64 * 1024];
    let mut newline_count = 0u64;
    let mut has_bytes = false;
    let mut ends_with_newline = false;

    loop {
        let read = match file.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => read,
            Err(_) => return 0,
        };

        has_bytes = true;
        newline_count += bytecount::count(&buffer[..read], b'\n') as u64;
        ends_with_newline = buffer[read - 1] == b'\n';
    }

    if !has_bytes {
        0
    } else if ends_with_newline {
        newline_count
    } else {
        newline_count + 1
    }
}

pub async fn execute(args: &Value) -> Result<Value> {
    let args_owned = args.clone();
    task::spawn_blocking(move || execute_blocking(args_owned))
        .await
        .context("workspace_stats background task failed to join")?
}

fn execute_blocking(args: Value) -> Result<Value> {
    let path_str = args
        .get("path")
        .and_then(|v| v.as_str())
        .context("Missing path")?;
    let path = PathBuf::from(path_str);

    if !path.exists() || !path.is_dir() {
        return Err(anyhow::anyhow!(
            "Path is not a valid directory: {}",
            path_str
        ));
    }

    let canonical_path = std::fs::canonicalize(&path).unwrap_or_else(|_| path.clone());

    let mut total_files: usize = 0;
    let mut total_size: u64 = 0;
    let mut total_lines: u64 = 0;

    let mut lang_files: HashMap<&str, usize> = HashMap::new();
    let mut lang_size: HashMap<&str, u64> = HashMap::new();
    let mut lang_lines: HashMap<&str, u64> = HashMap::new();

    // Track largest files as (path, size), top 10.
    let mut largest_files: Vec<(String, u64)> = Vec::new();
    let mut limit_reached = false;

    let walker = WalkBuilder::new(&path)
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .build();

    for result in walker {
        if total_files >= MAX_FILES {
            limit_reached = true;
            break;
        }

        let entry = match result {
            Ok(e) => e,
            Err(_) => continue,
        };

        if !entry.file_type().is_some_and(|ft| ft.is_file()) {
            continue;
        }

        total_files += 1;

        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        total_size += size;
        let line_count = count_lines_in_file(entry.path());
        total_lines += line_count;

        let file_path = entry.path().to_string_lossy().to_string();

        if largest_files.len() < 10 || size > largest_files.last().map(|f| f.1).unwrap_or(0) {
            largest_files.push((file_path, size));
            if largest_files.len() > 20 {
                largest_files.sort_by(|a, b| b.1.cmp(&a.1));
                largest_files.truncate(10);
            }
        }

        let ext = entry
            .path()
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        let lang = get_lang_from_ext(ext);
        *lang_files.entry(lang).or_insert(0) += 1;
        *lang_size.entry(lang).or_insert(0) += size;
        *lang_lines.entry(lang).or_insert(0) += line_count;
    }

    let mut languages_out = Vec::new();
    for (lang, file_count) in &lang_files {
        languages_out.push(json!({
            "language": lang,
            "files": file_count,
            "lines": lang_lines.get(lang).unwrap_or(&0),
            "size_bytes": lang_size.get(lang).unwrap_or(&0)
        }));
    }

    languages_out.sort_by(|a, b| {
        let count_a = a.get("files").and_then(|v| v.as_u64()).unwrap_or(0);
        let count_b = b.get("files").and_then(|v| v.as_u64()).unwrap_or(0);
        count_b.cmp(&count_a)
    });

    largest_files.sort_by(|a, b| b.1.cmp(&a.1));
    largest_files.truncate(10);
    let largest_out: Vec<Value> = largest_files
        .iter()
        .map(|(p, s)| json!({ "path": p, "size": s }))
        .collect();

    Ok(json!({
        "path": path_str,
        "canonical_path": canonical_path.to_string_lossy(),
        "total_files": total_files,
        "total_lines": total_lines,
        "total_size_bytes": total_size,
        "total_size_mb": format!("{:.1}", total_size as f64 / 1_048_576.0),
        "languages_breakdown": languages_out,
        "largest_files": largest_out,
        "limit_reached": limit_reached,
        "note": if limit_reached {
            format!("Scanning stopped at {} files. Use a subdirectory for finer stats.", MAX_FILES)
        } else {
            String::new()
        }
    }))
}
