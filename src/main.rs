mod common;
mod history;
mod indexer;
mod mcp;
mod security;
mod tools;
mod version;

use mcp::{JsonRpcRequest, JsonRpcResponse};
use serde_json::{Value, json};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::time::{Duration, Instant, timeout};
use tracing::{Level, debug, error, info, warn};
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use version::SERVER_VERSION;

const SERVER_NAME: &str = "codebase-mcp";
const LOG_LEVEL_ENV_NAMES: &[&str] = &["CODEBASE_MCP_LOG", "TURBO_LOG"];
const LOG_FILE_ENV_NAMES: &[&str] = &["CODEBASE_MCP_LOG_FILE", "TURBO_LOG_FILE"];
const WORKSPACE_ROOT_ENV_NAMES: &[&str] =
    &["CODEBASE_MCP_WORKSPACE_ROOT", "TURBO_FS_WORKSPACE_ROOT"];

/// Maximum timeout for one tool call (seconds).
const TOOL_TIMEOUT_SECS: u64 = 60;
static INDEX_ROOTS: OnceLock<Vec<PathBuf>> = OnceLock::new();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransportMode {
    /// Legacy mode: one JSON message per line.
    Line,
    /// MCP stdio framing mode: Content-Length + JSON body.
    Framed,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_level = common::env_var(LOG_LEVEL_ENV_NAMES)
        .and_then(|s| s.parse().ok())
        .unwrap_or(Level::DEBUG);

    if let Some(log_file_path) = common::env_var(LOG_FILE_ENV_NAMES) {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_file_path)?;
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(BoxMakeWriter::new(file))
            .with_max_level(log_level)
            .with_target(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_writer(io::stderr)
            .with_max_level(log_level)
            .with_target(false)
            .init();
    }

    info!(
        "{} v{} starting (log_level={:?})",
        SERVER_NAME, SERVER_VERSION, log_level
    );

    // Force-init START_TIME.
    lazy_static::initialize(&tools::server_health::START_TIME);

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut stdout = io::stdout();
    let mut line_buf = String::new();
    let mut body_buf = Vec::new();
    let mut request_count: u64 = 0;

    info!("Ready - waiting for JSON-RPC on stdin");

    loop {
        let (raw_request, transport_mode, bytes_read) =
            match read_next_message(&mut reader, &mut line_buf, &mut body_buf).await? {
                Some(v) => v,
                None => {
                    info!("EOF on stdin, shutting down");
                    break;
                }
            };

        request_count += 1;
        debug!(req = request_count, bytes = bytes_read, "<- recv");

        let req: Result<JsonRpcRequest, _> = serde_json::from_str(&raw_request);
        match req {
            Ok(request) => {
                let id = request.id.clone().unwrap_or(serde_json::Value::Null);
                let method = request.method.clone();
                debug!(req = request_count, method = %method, "dispatching");

                let response = match method.as_str() {
                    "initialize" => {
                        if let Some(params) = &request.params {
                            maybe_set_index_roots(params);
                        }
                        info!("Client initialized");
                        JsonRpcResponse::success(
                            id,
                            json!({
                                "protocolVersion": "2024-11-05",
                                "serverInfo": {
                                    "name": SERVER_NAME,
                                    "version": SERVER_VERSION
                                },
                                "capabilities": {
                                    "tools": {}
                                }
                            }),
                        )
                    }
                    "notifications/initialized" => {
                        start_indexer_if_needed();
                        debug!("notifications/initialized - indexer triggered");
                        continue;
                    }
                    "tools/list" => {
                        let tools = tools::list_tools();
                        debug!(count = tools.len(), "tools/list");
                        JsonRpcResponse::success(id, json!({ "tools": tools }))
                    }
                    "tools/call" => {
                        if !security::rate_limiter::GLOBAL_LIMITER.allow() {
                            warn!("Rate limit exceeded");
                            JsonRpcResponse::error(id, -32000, "Rate limit exceeded (max 50 req/s)")
                        } else {
                            let params = request.params.unwrap_or(json!({}));
                            let tool_name = params
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown")
                                .to_string();
                            maybe_index_tool_workspaces(&tool_name, &params);

                            debug!(tool = %tool_name, "-> tool call start");
                            let start = Instant::now();

                            match timeout(
                                Duration::from_secs(TOOL_TIMEOUT_SECS),
                                tools::call_tool(params),
                            )
                            .await
                            {
                                Ok(Ok(result)) => {
                                    let elapsed = start.elapsed();
                                    debug!(
                                        tool = %tool_name,
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "OK tool call"
                                    );
                                    if elapsed.as_millis() > 5000 {
                                        warn!(
                                            tool = %tool_name,
                                            elapsed_ms = elapsed.as_millis() as u64,
                                            "SLOW tool call (>5s)"
                                        );
                                    }
                                    JsonRpcResponse::success(id, result)
                                }
                                Ok(Err(e)) => {
                                    error!(
                                        tool = %tool_name,
                                        error = %e,
                                        elapsed_ms = start.elapsed().as_millis() as u64,
                                        "tool call error"
                                    );
                                    JsonRpcResponse::success(
                                        id,
                                        json!({
                                            "isError": true,
                                            "content": [{
                                                "type": "text",
                                                "text": format!("Error: {}", e)
                                            }]
                                        }),
                                    )
                                }
                                Err(_) => {
                                    error!(
                                        tool = %tool_name,
                                        timeout_s = TOOL_TIMEOUT_SECS,
                                        "tool call timeout"
                                    );
                                    JsonRpcResponse::success(
                                        id,
                                        json!({
                                            "isError": true,
                                            "content": [{
                                                "type": "text",
                                                "text": format!(
                                                    "Tool '{}' timed out after {}s. Try narrowing the search scope or using includes/excludes filters.",
                                                    tool_name, TOOL_TIMEOUT_SECS
                                                )
                                            }]
                                        }),
                                    )
                                }
                            }
                        }
                    }
                    "resources/list" => JsonRpcResponse::success(id, json!({ "resources": [] })),
                    "resources/templates/list" => {
                        JsonRpcResponse::success(id, json!({ "resourceTemplates": [] }))
                    }
                    "prompts/list" => JsonRpcResponse::success(id, json!({ "prompts": [] })),
                    _ => {
                        warn!(method = %method, "Unknown method");
                        JsonRpcResponse::error(id, -32601, "Method not found")
                    }
                };

                let out_bytes = write_response(&mut stdout, &response, transport_mode)?;
                debug!(req = request_count, bytes = out_bytes, "-> send");
            }
            Err(e) => {
                error!(error = %e, raw_len = raw_request.len(), "Parse error");
                let err = JsonRpcResponse::error(serde_json::Value::Null, -32700, "Parse error");
                let out_bytes = write_response(&mut stdout, &err, transport_mode)?;
                debug!(
                    req = request_count,
                    bytes = out_bytes,
                    "-> send parse-error"
                );
            }
        }
    }

    info!(total_requests = request_count, "Server shutdown");
    Ok(())
}

fn parse_content_length_header(line: &str) -> Option<usize> {
    let (name, value) = line.split_once(':')?;
    if !name.trim().eq_ignore_ascii_case("content-length") {
        return None;
    }
    value.trim().parse::<usize>().ok()
}

async fn read_next_message(
    reader: &mut BufReader<tokio::io::Stdin>,
    line_buf: &mut String,
    body_buf: &mut Vec<u8>,
) -> anyhow::Result<Option<(String, TransportMode, usize)>> {
    loop {
        line_buf.clear();
        let n = reader.read_line(line_buf).await?;
        if n == 0 {
            return Ok(None);
        }

        if line_buf.trim().is_empty() {
            continue;
        }

        let trimmed = line_buf.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            return Ok(Some((line_buf.clone(), TransportMode::Line, n)));
        }

        // Not JSON start; try MCP framed headers.
        if !line_buf.contains(':') {
            // Unknown line format, let parser handle it as line mode.
            return Ok(Some((line_buf.clone(), TransportMode::Line, n)));
        }

        let mut content_length = parse_content_length_header(line_buf);
        let mut header_bytes = n;

        loop {
            line_buf.clear();
            let h = reader.read_line(line_buf).await?;
            if h == 0 {
                return Err(anyhow::anyhow!(
                    "Unexpected EOF while reading MCP framed headers"
                ));
            }
            header_bytes += h;

            if line_buf == "\n" || line_buf == "\r\n" {
                break;
            }

            if content_length.is_none() {
                content_length = parse_content_length_header(line_buf);
            }
        }

        let len = content_length
            .ok_or_else(|| anyhow::anyhow!("Missing Content-Length header in MCP frame"))?;

        body_buf.resize(len, 0);
        reader.read_exact(body_buf).await?;

        let body = std::str::from_utf8(body_buf)
            .map_err(|e| anyhow::anyhow!("Invalid UTF-8 in framed payload: {}", e))?
            .to_string();

        return Ok(Some((body, TransportMode::Framed, header_bytes + len)));
    }
}

fn write_response(
    stdout: &mut io::Stdout,
    response: &JsonRpcResponse,
    mode: TransportMode,
) -> anyhow::Result<usize> {
    let out = serde_json::to_string(response)?;
    match mode {
        TransportMode::Framed => {
            let header = format!("Content-Length: {}\r\n\r\n", out.len());
            write!(stdout, "{}{}", header, out)?;
            stdout.flush()?;
            Ok(header.len() + out.len())
        }
        TransportMode::Line => {
            writeln!(stdout, "{}", out)?;
            stdout.flush()?;
            Ok(out.len() + 1)
        }
    }
}

fn maybe_set_index_roots(params: &serde_json::Value) {
    if INDEX_ROOTS.get().is_some() {
        return;
    }

    let roots = extract_index_roots(params);
    if roots.is_empty() {
        return;
    }

    if INDEX_ROOTS.set(roots.clone()).is_ok() {
        info!(
            workspace_count = roots.len(),
            "Captured client workspace roots for background indexer"
        );
    }
}

fn extract_index_roots(params: &serde_json::Value) -> Vec<PathBuf> {
    let mut roots = Vec::new();

    if let Some(ws_root) = params
        .get("clientInfo")
        .and_then(|v| v.get("workspaceRoot"))
        .and_then(|v| v.as_str())
    {
        push_unique_root(&mut roots, PathBuf::from(ws_root));
    }

    if let Some(root_entries) = params.get("roots").and_then(|v| v.as_array()) {
        for root in root_entries {
            if let Some(uri) = root.get("uri").and_then(|v| v.as_str())
                && let Some(path) = uri_to_path(uri)
            {
                push_unique_root(&mut roots, path);
            }
        }
    }

    if let Some(root_uri) = params.get("rootUri").and_then(|v| v.as_str())
        && let Some(path) = uri_to_path(root_uri)
    {
        push_unique_root(&mut roots, path);
    }

    if let Some(workspace_folders) = params.get("workspaceFolders").and_then(|v| v.as_array()) {
        for folder in workspace_folders {
            if let Some(uri) = folder.get("uri").and_then(|v| v.as_str())
                && let Some(path) = uri_to_path(uri)
            {
                push_unique_root(&mut roots, path);
            }
        }
    }

    roots
        .into_iter()
        .filter(|path| path.exists() && path.is_dir())
        .collect()
}

fn uri_to_path(uri: &str) -> Option<PathBuf> {
    if let Some(path_str) = uri.strip_prefix("file:///") {
        return Some(PathBuf::from(path_str));
    }

    if let Some(path_str) = uri.strip_prefix("file://") {
        return Some(PathBuf::from(path_str));
    }

    if !uri.contains("://") {
        return Some(PathBuf::from(uri));
    }

    None
}

fn start_indexer_if_needed() {
    if let Some(roots) = INDEX_ROOTS.get()
        && !roots.is_empty()
    {
        for root in roots {
            info!(
                root = %root.display(),
                "Starting background indexer on client workspace root"
            );
            indexer::ensure_workspace_index(root.clone(), "client_initialize".to_string());
        }
        return;
    }

    if let Some((root, source_label)) = explicit_workspace_root_from_env() {
        if root.exists() && root.is_dir() {
            info!(
                root = %root.display(),
                source = source_label,
                "Starting background indexer from environment workspace root"
            );
            indexer::ensure_workspace_index(root, source_label.to_string());
            return;
        }

        warn!(
            root = %root.display(),
            source = source_label,
            "Ignoring invalid environment workspace root"
        );
    }

    if let Ok(current_dir) = std::env::current_dir() {
        if looks_like_workspace_root(&current_dir) {
            warn!(
                root = %current_dir.display(),
                "No workspace root provided by client; falling back to current_dir() for background indexer"
            );
            indexer::ensure_workspace_index(current_dir, "process_current_dir".to_string());
            return;
        }

        warn!(
            root = %current_dir.display(),
            "No workspace root provided by client; skipping current_dir() fallback because it does not look like a workspace root"
        );
    }

    warn!("No workspace root provided by client; background indexer remains disabled");
}

fn explicit_workspace_root_from_env() -> Option<(PathBuf, &'static str)> {
    WORKSPACE_ROOT_ENV_NAMES.iter().find_map(|name| {
        std::env::var(name).ok().map(|value| {
            let source = match *name {
                "CODEBASE_MCP_WORKSPACE_ROOT" => "env:CODEBASE_MCP_WORKSPACE_ROOT",
                _ => "env:TURBO_FS_WORKSPACE_ROOT",
            };
            (PathBuf::from(value), source)
        })
    })
}

fn maybe_index_tool_workspaces(tool_name: &str, params: &Value) {
    let arguments = match params.get("arguments") {
        Some(arguments) => arguments,
        None => return,
    };

    let roots = infer_workspace_roots_from_tool_arguments(arguments);
    for root in roots {
        indexer::ensure_workspace_index(root, format!("tool_call:{}", tool_name));
    }
}

fn infer_workspace_roots_from_tool_arguments(arguments: &Value) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    collect_workspace_roots(arguments, None, &mut roots);

    let mut inferred = Vec::new();
    for candidate in roots {
        if let Some(workspace_root) = discover_workspace_root_for_path(&candidate) {
            push_unique_root(&mut inferred, workspace_root);
        }
    }

    inferred
}

fn collect_workspace_roots(value: &Value, parent_key: Option<&str>, roots: &mut Vec<PathBuf>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                collect_workspace_roots(child, Some(key.as_str()), roots);
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_workspace_roots(item, parent_key, roots);
            }
        }
        Value::String(raw) => {
            if parent_key.is_some_and(is_workspace_path_key)
                && let Some(candidate) = resolve_tool_path_candidate(raw)
            {
                roots.push(candidate);
            }
        }
        _ => {}
    }
}

fn is_workspace_path_key(key: &str) -> bool {
    matches!(
        key,
        "path"
            | "paths"
            | "file_path"
            | "repo_path"
            | "archive_path"
            | "input_file"
            | "output_file"
            | "file_hint"
    )
}

fn resolve_tool_path_candidate(raw: &str) -> Option<PathBuf> {
    if raw.is_empty() {
        return None;
    }

    let mut path = if raw.starts_with("file://") {
        uri_to_path(raw)?
    } else {
        PathBuf::from(raw)
    };

    if !path.is_absolute() {
        let cwd = std::env::current_dir().ok()?;
        path = cwd.join(path);
    }

    existing_anchor_for_path(path)
}

fn existing_anchor_for_path(path: PathBuf) -> Option<PathBuf> {
    let mut current = path.as_path();
    while !current.exists() {
        current = current.parent()?;
    }

    Some(canonicalize_existing_path(current))
}

fn discover_workspace_root_for_path(path: &Path) -> Option<PathBuf> {
    if let Some(indexed_root) = indexer::indexed_workspace_root_for_path(path) {
        return Some(indexed_root);
    }

    let mut current = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()?.to_path_buf()
    };

    loop {
        if looks_like_workspace_root(&current) {
            return Some(current);
        }

        let Some(parent) = current.parent() else {
            break;
        };
        current = parent.to_path_buf();
    }

    if path.is_dir() {
        Some(path.to_path_buf())
    } else {
        path.parent().map(PathBuf::from)
    }
}

fn canonicalize_existing_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn push_unique_root(roots: &mut Vec<PathBuf>, candidate: PathBuf) {
    let candidate_key = normalize_root_key(&candidate);
    if roots
        .iter()
        .any(|existing| normalize_root_key(existing) == candidate_key)
    {
        return;
    }

    roots.push(candidate);
}

fn normalize_root_key(path: &Path) -> String {
    let normalized = path.to_string_lossy().replace('\\', "/");
    #[cfg(windows)]
    {
        normalized.to_ascii_lowercase()
    }
    #[cfg(not(windows))]
    {
        normalized
    }
}

fn looks_like_workspace_root(path: &Path) -> bool {
    if !path.exists() || !path.is_dir() {
        return false;
    }

    const WORKSPACE_MARKERS: &[&str] = &[
        ".git",
        "Cargo.toml",
        "package.json",
        "pnpm-workspace.yaml",
        "yarn.lock",
        "package-lock.json",
        "pyproject.toml",
        "requirements.txt",
        "go.mod",
        "pom.xml",
        "build.gradle",
        "settings.gradle",
        "WORKSPACE",
        "WORKSPACE.bazel",
        ".hg",
        ".svn",
    ];

    WORKSPACE_MARKERS
        .iter()
        .any(|marker| path.join(marker).exists())
}
