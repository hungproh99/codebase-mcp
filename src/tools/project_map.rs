use anyhow::Result;
use ignore::WalkBuilder;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::PathBuf;

pub fn schema() -> Value {
    json!({
        "name": "project_map",
        "description": "Build a tree view of a project with optional size metadata.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": { "type": "string" },
                "max_depth": { "type": "integer" },
                "show_sizes": { "type": "boolean" }
            },
            "required": ["path"]
        }
    })
}

pub async fn execute(args: &Value) -> Result<Value> {
    let path_str = args.get("path").and_then(|v| v.as_str()).unwrap_or("");
    let path = PathBuf::from(path_str);

    if !path.exists() || !path.is_dir() {
        return Err(anyhow::anyhow!(
            "Path is not a valid directory: {}",
            path_str
        ));
    }

    let max_depth = args.get("max_depth").and_then(|v| v.as_u64()).unwrap_or(3) as usize;
    let show_sizes = args
        .get("show_sizes")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let walker = WalkBuilder::new(&path)
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .max_depth(Some(max_depth))
        .build();

    // Map: Dir -> vec[Files]
    let mut dir_map: HashMap<String, Vec<Value>> = HashMap::new();
    let mut root_dirs = Vec::new();

    for entry in walker.flatten() {
        if entry.path() == path {
            continue;
        } // skip root self

        let parent_path = entry
            .path()
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let is_dir = entry.file_type().is_some_and(|ft| ft.is_dir());

        // Build size if requested
        let size = if show_sizes && !is_dir {
            json!(entry.metadata().map(|m| m.len()).unwrap_or(0))
        } else {
            Value::Null
        };

        let item = json!({
            "name": entry.file_name().to_string_lossy().to_string(),
            "type": if is_dir { "dir" } else { "file" },
            "size_b": size
        });

        dir_map.entry(parent_path.clone()).or_default().push(item);

        if (parent_path == path_str || parent_path == path.to_string_lossy()) && is_dir {
            root_dirs.push(entry.path().to_string_lossy().to_string());
        }
    }

    // A simple representation of the first-level depth map for the agent.
    // Return flat directory grouped to not blow up Context Token Limit
    Ok(json!({
        "root": path_str,
        "max_depth": max_depth,
        "tree_representation": dir_map,
        "note": "Skipped hidden / git / node_modules inherently to preserve token context."
    }))
}
