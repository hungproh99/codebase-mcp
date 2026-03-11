use dashmap::DashMap;
use notify::{Config, Event, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::DefaultHasher};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

use crate::common::env_var_os;

lazy_static::lazy_static! {
    static ref INDEX_RUNTIMES: RwLock<BTreeMap<String, IndexRuntime>> = RwLock::new(BTreeMap::new());
    static ref PATH_INDEXES: DashMap<String, Arc<RwLock<PathIndex>>> = DashMap::new();
    static ref ACTIVE_INDEXERS: DashMap<String, ()> = DashMap::new();
    static ref ACTIVE_FULL_SCANS: DashMap<String, ()> = DashMap::new();
    static ref PENDING_FULL_SCAN_RESETS: DashMap<String, ()> = DashMap::new();
    static ref ACTIVE_WORKSPACE_KEY: RwLock<Option<String>> = RwLock::new(None);
}

static INDEX_STORAGE_CLEARED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone)]
struct IndexRuntime {
    workspace_root: PathBuf,
    workspace_source: String,
    index_file: PathBuf,
    loaded_from_disk: bool,
    scan_complete: bool,
    last_loaded_entries: usize,
    last_persisted_entries: usize,
    last_persisted_at: Option<u64>,
    last_scan_completed_at: Option<u64>,
    last_refresh_requested_at: Option<u64>,
    last_request_source: Option<String>,
    last_error: Option<String>,
    indexed_entries_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexRuntimeSnapshot {
    pub workspace_root: String,
    pub workspace_source: String,
    pub index_file: String,
    pub loaded_from_disk: bool,
    pub scan_complete: bool,
    pub last_loaded_entries: usize,
    pub last_persisted_entries: usize,
    pub last_persisted_at: Option<u64>,
    pub last_scan_completed_at: Option<u64>,
    pub last_refresh_requested_at: Option<u64>,
    pub last_request_source: Option<String>,
    pub last_error: Option<String>,
    pub indexed_entries_count: usize,
    pub cached_files_count: usize,
    pub index_kind: &'static str,
    pub index_status: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedIndex {
    schema_version: u32,
    workspace_root: String,
    saved_at: u64,
    #[serde(default)]
    scan_complete: bool,
    entries: Vec<PersistedEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedEntry {
    relative_path: String,
    is_dir: bool,
    size: u64,
    last_modified: u64,
}

#[derive(Debug, Clone)]
struct PathIndexEntry {
    absolute_path: PathBuf,
    normalized_path: String,
    relative_path: String,
    relative_path_lower: String,
    file_name_lower: String,
    extension_lower: String,
    is_dir: bool,
    size: u64,
    last_modified: u64,
}

#[derive(Debug, Default)]
struct PathIndex {
    entries: Vec<Option<PathIndexEntry>>,
    path_lookup: HashMap<String, usize>,
    term_postings: HashMap<String, Vec<usize>>,
    live_entries: usize,
}

#[derive(Debug, Clone)]
pub struct PathQueryCandidate {
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
    pub modified_at: u64,
}

const DEBOUNCE_MS: u64 = 300;
const PERSIST_INTERVAL_SECS: u64 = 2;
const MAX_PREWARM_ENTRIES: usize = 100_000;
const FULL_SCAN_PERSIST_CHUNK: usize = 5_000;
const INDEX_SCHEMA_VERSION: u32 = 4;
const DEFAULT_STALE_INDEX_SECS: u64 = 60 * 60;
const MIN_REFRESH_INTERVAL_SECS: u64 = 30;
const MIN_INDEX_TERM_LEN: usize = 3;
const MAX_EXACT_TERM_POSTINGS: usize = 32_768;
const MAX_SHORTLIST_CANDIDATES: usize = 8_192;
const MAX_SHORTLIST_WORKSET: usize = 65_536;

#[derive(Debug, Clone, Copy)]
struct ScanProgress {
    scanned: usize,
    complete: bool,
}

pub fn get_runtime_snapshots() -> Vec<IndexRuntimeSnapshot> {
    let runtimes = match INDEX_RUNTIMES.read() {
        Ok(guard) => guard.values().cloned().collect::<Vec<_>>(),
        Err(_) => return Vec::new(),
    };

    runtimes
        .into_iter()
        .map(runtime_snapshot_from_state)
        .collect()
}

pub fn get_active_runtime_snapshot() -> Option<IndexRuntimeSnapshot> {
    let active_key = match ACTIVE_WORKSPACE_KEY.read() {
        Ok(guard) => guard.clone(),
        Err(_) => None,
    }?;

    match INDEX_RUNTIMES.read() {
        Ok(guard) => guard
            .get(&active_key)
            .cloned()
            .map(runtime_snapshot_from_state),
        Err(_) => None,
    }
}

pub fn indexed_workspace_root_for_path(path: &Path) -> Option<PathBuf> {
    let canonical_path = canonicalize_or_original(path.to_path_buf());
    match INDEX_RUNTIMES.read() {
        Ok(guard) => guard
            .values()
            .filter(|state| path_belongs_to_workspace(&state.workspace_root, &canonical_path))
            .max_by_key(|state| state.workspace_root.components().count())
            .map(|state| state.workspace_root.clone()),
        Err(_) => None,
    }
}

pub fn is_path_index_ready(path: &Path) -> bool {
    let canonical_path = canonicalize_or_original(path.to_path_buf());
    match INDEX_RUNTIMES.read() {
        Ok(guard) => guard.values().any(|state| {
            state.scan_complete && path_belongs_to_workspace(&state.workspace_root, &canonical_path)
        }),
        Err(_) => false,
    }
}

pub fn query_path_candidates(
    search_root: &Path,
    pattern: &str,
    shortlist_limit: usize,
) -> Option<Vec<PathQueryCandidate>> {
    let canonical_root = canonicalize_or_original(search_root.to_path_buf());
    let (workspace_key, workspace_root) = ready_workspace_for_path(&canonical_root)?;
    let relative_root = relative_root_prefix(&workspace_root, &canonical_root)?;
    let anchor_terms = query_anchor_terms(pattern);
    if anchor_terms.is_empty() {
        return None;
    }

    let index = PATH_INDEXES.get(&workspace_key)?.value().clone();
    let guard = index.read().ok()?;
    guard.shortlist_candidates(&anchor_terms, relative_root.as_deref(), shortlist_limit)
}

fn runtime_snapshot_from_state(state: IndexRuntime) -> IndexRuntimeSnapshot {
    let indexed_entries_count = state.indexed_entries_count;
    IndexRuntimeSnapshot {
        workspace_root: state.workspace_root.to_string_lossy().to_string(),
        workspace_source: state.workspace_source,
        index_file: state.index_file.to_string_lossy().to_string(),
        loaded_from_disk: state.loaded_from_disk,
        scan_complete: state.scan_complete,
        last_loaded_entries: state.last_loaded_entries,
        last_persisted_entries: state.last_persisted_entries,
        last_persisted_at: state.last_persisted_at,
        last_scan_completed_at: state.last_scan_completed_at,
        last_refresh_requested_at: state.last_refresh_requested_at,
        last_request_source: state.last_request_source,
        last_error: state.last_error,
        indexed_entries_count,
        cached_files_count: indexed_entries_count,
        index_kind: "path",
        index_status: if indexed_entries_count > 0 {
            "active".to_string()
        } else {
            "idle".to_string()
        },
    }
}

pub fn spawn_background_indexer(workspace_root: PathBuf, workspace_source: String) {
    clear_persisted_indexes_once();

    let workspace_root = canonicalize_or_original(workspace_root);
    let workspace_key = normalize_path_for_identity(&workspace_root);
    let index_file = index_file_path_for_workspace(&workspace_root);

    set_active_workspace(&workspace_key, &workspace_source);

    if ACTIVE_INDEXERS.insert(workspace_key.clone(), ()).is_some() {
        return;
    }

    reset_runtime_state(
        &workspace_key,
        &workspace_root,
        &workspace_source,
        &index_file,
    );

    thread::spawn(move || {
        let _active_guard = ActiveIndexerGuard::new(workspace_key.clone());

        let loaded_from_disk = match load_persisted_index(&workspace_root, &index_file) {
            Ok(Some(snapshot)) => {
                let loaded_entries =
                    apply_snapshot_to_index(&workspace_key, &workspace_root, &snapshot);
                record_load_state(
                    &workspace_key,
                    true,
                    snapshot.scan_complete,
                    loaded_entries,
                    snapshot.saved_at,
                    None,
                );
                info!(
                    workspace = %workspace_root.display(),
                    index_file = %index_file.display(),
                    loaded_entries,
                    "Loaded persisted path index"
                );
                true
            }
            Ok(None) => {
                record_load_state(&workspace_key, false, false, 0, 0, None);
                false
            }
            Err(err) => {
                record_load_state(&workspace_key, false, false, 0, 0, Some(err.clone()));
                error!(
                    workspace = %workspace_root.display(),
                    error = %err,
                    "Failed to load persisted path index"
                );
                false
            }
        };

        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher = match RecommendedWatcher::new(tx, Config::default()) {
            Ok(w) => w,
            Err(e) => {
                record_runtime_error(&workspace_key, format!("watcher_create_failed: {}", e));
                error!(workspace = %workspace_root.display(), error = %e, "Failed to create watcher");
                return;
            }
        };

        if let Err(e) = watcher.watch(&workspace_root, RecursiveMode::Recursive) {
            record_runtime_error(&workspace_key, format!("watcher_attach_failed: {}", e));
            error!(
                workspace = %workspace_root.display(),
                error = %e,
                "Failed to attach filesystem watcher"
            );
            return;
        }

        if !loaded_from_disk {
            let start = Instant::now();
            let prewarm = prewarm_index(&workspace_key, &workspace_root);
            info!(
                workspace = %workspace_root.display(),
                scanned = prewarm.scanned,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "Path pre-warm completed"
            );

            if prewarm.complete {
                record_scan_complete(&workspace_key, true);
            }
            match persist_index_snapshot(&workspace_key, &workspace_root, &index_file) {
                Ok(saved) => record_persist_state(&workspace_key, saved, None),
                Err(err) => {
                    record_persist_state(&workspace_key, 0, Some(err.clone()));
                    error!(
                        workspace = %workspace_root.display(),
                        error = %err,
                        "Failed to persist initial path index"
                    );
                }
            }
            if !prewarm.complete {
                spawn_full_scan_refresh(
                    workspace_root.clone(),
                    workspace_key.clone(),
                    index_file.clone(),
                    false,
                );
            }
        } else {
            info!(
                workspace = %workspace_root.display(),
                "Using persisted path index; skipping full pre-warm"
            );
            if !is_runtime_scan_complete(&workspace_key) {
                spawn_full_scan_refresh(
                    workspace_root.clone(),
                    workspace_key.clone(),
                    index_file.clone(),
                    false,
                );
            }
        }

        let mut pending: HashMap<PathBuf, Instant> = HashMap::new();
        let mut index_dirty = false;
        let mut last_persist = Instant::now();

        loop {
            match rx.recv_timeout(Duration::from_millis(DEBOUNCE_MS)) {
                Ok(Ok(Event { paths, .. })) => {
                    let now = Instant::now();
                    for path in paths {
                        pending.insert(path, now);
                    }
                }
                Ok(Err(_)) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    if index_dirty {
                        match persist_index_snapshot(&workspace_key, &workspace_root, &index_file) {
                            Ok(saved) => record_persist_state(&workspace_key, saved, None),
                            Err(err) => record_persist_state(&workspace_key, 0, Some(err)),
                        }
                    }
                    break;
                }
            }

            let now = Instant::now();
            let debounce = Duration::from_millis(DEBOUNCE_MS);
            let ready: Vec<PathBuf> = pending
                .iter()
                .filter(|(_, ts)| now.duration_since(**ts) >= debounce)
                .map(|(p, _)| p.clone())
                .collect();

            let mut changed = false;
            for path in ready {
                pending.remove(&path);
                if is_ignore_config_path(&workspace_root, &path) {
                    record_scan_complete(&workspace_key, false);
                    spawn_full_scan_refresh(
                        workspace_root.clone(),
                        workspace_key.clone(),
                        index_file.clone(),
                        true,
                    );
                    changed = true;
                    continue;
                }

                if apply_fs_change(&workspace_key, &workspace_root, &path) {
                    changed = true;
                }
            }

            if changed {
                index_dirty = true;
            }

            if index_dirty
                && now.duration_since(last_persist) >= Duration::from_secs(PERSIST_INTERVAL_SECS)
            {
                match persist_index_snapshot(&workspace_key, &workspace_root, &index_file) {
                    Ok(saved) => record_persist_state(&workspace_key, saved, None),
                    Err(err) => record_persist_state(&workspace_key, 0, Some(err)),
                }
                index_dirty = false;
                last_persist = now;
            }
        }
    });
}

fn canonicalize_or_original(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn ready_workspace_for_path(path: &Path) -> Option<(String, PathBuf)> {
    let canonical_path = canonicalize_or_original(path.to_path_buf());
    let runtime = match INDEX_RUNTIMES.read() {
        Ok(guard) => guard
            .iter()
            .filter(|(_, state)| {
                state.scan_complete
                    && path_belongs_to_workspace(&state.workspace_root, &canonical_path)
            })
            .max_by_key(|(_, state)| state.workspace_root.components().count())
            .map(|(key, state)| (key.clone(), state.workspace_root.clone())),
        Err(_) => None,
    }?;

    Some(runtime)
}

fn prewarm_index(workspace_key: &str, workspace_root: &Path) -> ScanProgress {
    let max_prewarm_duration = Duration::from_secs(15);
    let start = Instant::now();
    let mut count = 0usize;

    for entry in ignore::WalkBuilder::new(workspace_root)
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .build()
        .flatten()
    {
        let Some(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() && !file_type.is_dir() {
            continue;
        }

        if count >= MAX_PREWARM_ENTRIES {
            warn!(
                workspace = %workspace_root.display(),
                max_entries = MAX_PREWARM_ENTRIES,
                "Path pre-warm hit entry limit"
            );
            return ScanProgress {
                scanned: count,
                complete: false,
            };
        }

        if start.elapsed() >= max_prewarm_duration {
            warn!(
                workspace = %workspace_root.display(),
                elapsed_ms = max_prewarm_duration.as_millis() as u64,
                scanned = count,
                "Path pre-warm hit time limit"
            );
            return ScanProgress {
                scanned: count,
                complete: false,
            };
        }

        let Ok(metadata) = entry.metadata() else {
            continue;
        };

        if apply_path_metadata(
            workspace_key,
            workspace_root,
            entry.path(),
            file_type.is_dir(),
            &metadata,
        ) {
            count += 1;
        }
    }

    ScanProgress {
        scanned: count,
        complete: true,
    }
}

fn apply_snapshot_to_index(
    workspace_key: &str,
    workspace_root: &Path,
    snapshot: &PersistedIndex,
) -> usize {
    with_path_index_write(workspace_key, |index| {
        index.clear();
        for entry in &snapshot.entries {
            index.insert_persisted_entry(workspace_root, entry);
        }
        index.live_entries
    })
    .unwrap_or(0)
}

fn apply_fs_change(workspace_key: &str, workspace_root: &Path, path: &Path) -> bool {
    if path.exists() {
        let canonical_path = canonicalize_or_original(path.to_path_buf());
        let Ok(metadata) = fs::metadata(path) else {
            return false;
        };
        if metadata.is_dir() {
            if !path_is_indexable(workspace_root, &canonical_path) {
                return remove_indexed_entries_under(workspace_key, &canonical_path) > 0;
            }

            let changed_dir = apply_path_metadata(
                workspace_key,
                workspace_root,
                &canonical_path,
                true,
                &metadata,
            );
            let changed_parents =
                ensure_parent_directories(workspace_key, workspace_root, &canonical_path);
            return changed_dir || changed_parents;
        }
        if !metadata.is_file() {
            return remove_indexed_entries_under(workspace_key, &canonical_path) > 0;
        }

        if !path_is_indexable(workspace_root, &canonical_path) {
            return remove_single_index_entry(workspace_key, &canonical_path);
        }

        let changed_file = apply_path_metadata(
            workspace_key,
            workspace_root,
            &canonical_path,
            false,
            &metadata,
        );
        let changed_parents =
            ensure_parent_directories(workspace_key, workspace_root, &canonical_path);
        changed_file || changed_parents
    } else {
        let removed_direct = remove_single_index_entry(workspace_key, path);
        let removed_children = remove_indexed_entries_under(workspace_key, path) > 0;
        removed_direct || removed_children
    }
}

fn apply_path_metadata(
    workspace_key: &str,
    workspace_root: &Path,
    path: &Path,
    is_dir: bool,
    metadata: &fs::Metadata,
) -> bool {
    let last_modified = metadata_modified_at(metadata);
    let size = if is_dir { 0 } else { metadata.len() };
    let Some((changed, live_entries)) = with_path_index_write(workspace_key, |index| {
        let changed = index.upsert(workspace_root, path, is_dir, size, last_modified);
        (changed, index.live_entries)
    }) else {
        return false;
    };

    if changed {
        record_indexed_entries_count(workspace_key, live_entries);
    }

    changed
}

fn ensure_parent_directories(workspace_key: &str, workspace_root: &Path, path: &Path) -> bool {
    let mut changed = false;
    let mut current = path.parent();

    while let Some(dir) = current {
        if !path_belongs_to_workspace(workspace_root, dir) {
            break;
        }

        let Ok(metadata) = fs::metadata(dir) else {
            current = dir.parent();
            continue;
        };
        if metadata.is_dir()
            && apply_path_metadata(workspace_key, workspace_root, dir, true, &metadata)
        {
            changed = true;
        }

        if dir == workspace_root {
            break;
        }
        current = dir.parent();
    }

    changed
}

fn remove_single_index_entry(workspace_key: &str, path: &Path) -> bool {
    let Some((removed, live_entries)) = with_path_index_write(workspace_key, |index| {
        let removed = index.remove_path(path);
        (removed, index.live_entries)
    }) else {
        return false;
    };

    if removed {
        record_indexed_entries_count(workspace_key, live_entries);
    }

    removed
}

fn remove_indexed_entries_under(workspace_key: &str, path: &Path) -> usize {
    let Some((removed, live_entries)) = with_path_index_write(workspace_key, |index| {
        let removed = index.remove_subtree(path);
        (removed, index.live_entries)
    }) else {
        return 0;
    };

    if removed > 0 {
        record_indexed_entries_count(workspace_key, live_entries);
    }

    removed
}

fn metadata_modified_at(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .unwrap_or(SystemTime::UNIX_EPOCH)
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn path_is_indexable(workspace_root: &Path, path: &Path) -> bool {
    let canonical_path = canonicalize_or_original(path.to_path_buf());
    if !path_belongs_to_workspace(workspace_root, &canonical_path) || !canonical_path.exists() {
        return false;
    }
    if canonical_path == workspace_root {
        return true;
    }

    let target_key = normalize_path_for_identity(&canonical_path);
    let filter_target_key = target_key.clone();
    let filter_canonical_path = canonical_path.clone();
    let workspace_root = workspace_root.to_path_buf();
    let filter_workspace_root = workspace_root.clone();
    let target_parent = canonical_path
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root.clone());

    for entry in ignore::WalkBuilder::new(&workspace_root)
        .hidden(true)
        .ignore(true)
        .git_ignore(true)
        .git_exclude(true)
        .require_git(false)
        .filter_entry(move |entry| {
            if entry.path() == filter_workspace_root {
                return true;
            }

            if entry.file_type().is_some_and(|ft| ft.is_dir()) {
                return target_parent.starts_with(entry.path())
                    || entry.path() == filter_canonical_path;
            }

            normalize_path_for_identity(entry.path()) == filter_target_key
        })
        .build()
        .flatten()
    {
        let entry_key = normalize_path_for_identity(entry.path());
        if entry_key == target_key {
            return true;
        }
    }

    false
}

fn is_ignore_config_path(workspace_root: &Path, path: &Path) -> bool {
    if !path_belongs_to_workspace(workspace_root, path) {
        return false;
    }

    match path.file_name().and_then(|name| name.to_str()) {
        Some(".gitignore") | Some(".ignore") => true,
        _ => normalize_path_for_identity(path).ends_with("/.git/info/exclude"),
    }
}

fn path_belongs_to_workspace(workspace_root: &Path, path: &Path) -> bool {
    let workspace_key = normalize_path_for_identity(workspace_root);
    let path_key = normalize_path_for_identity(path);

    path_key == workspace_key || path_key.starts_with(&(workspace_key + "/"))
}

fn legacy_index_storage_root() -> PathBuf {
    // Keep the old turbo-fs cache location so upgrades can clean persisted snapshots
    // created before the public rename to codebase-mcp.
    if let Some(custom_dir) = env_var_os(&["CODEBASE_MCP_INDEX_DIR", "TURBO_FS_INDEX_DIR"]) {
        return PathBuf::from(custom_dir);
    }

    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(local_app_data)
            .join("turbo-fs")
            .join("index-cache");
    }

    if let Some(xdg_cache_home) = std::env::var_os("XDG_CACHE_HOME") {
        return PathBuf::from(xdg_cache_home)
            .join("turbo-fs")
            .join("index-cache");
    }

    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".cache")
            .join("turbo-fs")
            .join("index-cache");
    }

    std::env::temp_dir().join("turbo-fs").join("index-cache")
}

fn index_storage_root() -> PathBuf {
    if let Some(custom_dir) = env_var_os(&["CODEBASE_MCP_INDEX_DIR", "TURBO_FS_INDEX_DIR"]) {
        return PathBuf::from(custom_dir).join("path-index");
    }

    if let Some(local_app_data) = std::env::var_os("LOCALAPPDATA") {
        return PathBuf::from(local_app_data)
            .join("codebase-mcp")
            .join("path-index-cache");
    }

    if let Some(xdg_cache_home) = std::env::var_os("XDG_CACHE_HOME") {
        return PathBuf::from(xdg_cache_home)
            .join("codebase-mcp")
            .join("path-index-cache");
    }

    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".cache")
            .join("codebase-mcp")
            .join("path-index-cache");
    }

    std::env::temp_dir()
        .join("codebase-mcp")
        .join("path-index-cache")
}

fn clear_persisted_indexes_once() {
    if INDEX_STORAGE_CLEARED.swap(true, Ordering::AcqRel) {
        return;
    }

    for root in [legacy_index_storage_root(), index_storage_root()] {
        if root.exists() {
            let _ = fs::remove_dir_all(&root);
        }
    }
}

fn index_file_path_for_workspace(workspace_root: &Path) -> PathBuf {
    index_file_path_for_workspace_in(&index_storage_root(), workspace_root)
}

fn index_file_path_for_workspace_in(storage_root: &Path, workspace_root: &Path) -> PathBuf {
    let workspace_hash = hash_workspace_root(workspace_root);
    storage_root.join(format!("{}.path.json", workspace_hash))
}

fn hash_workspace_root(workspace_root: &Path) -> String {
    let normalized = normalize_path_for_identity(workspace_root);
    let mut hasher = DefaultHasher::new();
    normalized.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn normalize_path_for_identity(path: &Path) -> String {
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

fn load_persisted_index(
    workspace_root: &Path,
    index_file: &Path,
) -> Result<Option<PersistedIndex>, String> {
    if !index_file.exists() {
        return Ok(None);
    }

    let raw = fs::read_to_string(index_file).map_err(|e| format!("read_failed: {}", e))?;
    let snapshot: PersistedIndex =
        serde_json::from_str(&raw).map_err(|e| format!("parse_failed: {}", e))?;

    if snapshot.schema_version != INDEX_SCHEMA_VERSION {
        return Err(format!(
            "unsupported_schema_version: got={}, expected={}",
            snapshot.schema_version, INDEX_SCHEMA_VERSION
        ));
    }

    if snapshot.workspace_root != normalize_path_for_identity(workspace_root) {
        return Err("workspace_mismatch".to_string());
    }

    Ok(Some(snapshot))
}

fn persist_index_snapshot(
    workspace_key: &str,
    workspace_root: &Path,
    index_file: &Path,
) -> Result<usize, String> {
    let Some(entries) = with_path_index_read(workspace_key, |index| index.persisted_entries())
    else {
        return Ok(0);
    };

    let payload = PersistedIndex {
        schema_version: INDEX_SCHEMA_VERSION,
        workspace_root: normalize_path_for_identity(workspace_root),
        saved_at: current_unix_timestamp(),
        scan_complete: is_path_index_ready(workspace_root),
        entries,
    };

    if let Some(parent) = index_file.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create_parent_failed: {}", e))?;
    }

    let serialized =
        serde_json::to_vec(&payload).map_err(|e| format!("serialize_failed: {}", e))?;
    let tmp_file = index_file.with_extension("json.tmp");
    fs::write(&tmp_file, serialized).map_err(|e| format!("write_failed: {}", e))?;

    if index_file.exists() {
        let _ = fs::remove_file(index_file);
    }

    fs::rename(&tmp_file, index_file).map_err(|e| {
        let _ = fs::remove_file(&tmp_file);
        format!("rename_failed: {}", e)
    })?;

    Ok(payload.entries.len())
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn stale_index_after_secs() -> u64 {
    crate::common::env_var(&["CODEBASE_MCP_INDEX_STALE_SECS", "TURBO_FS_INDEX_STALE_SECS"])
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_STALE_INDEX_SECS)
}

pub fn ensure_workspace_index(workspace_root: PathBuf, workspace_source: String) {
    let workspace_root = canonicalize_or_original(workspace_root);
    if !workspace_root.exists() || !workspace_root.is_dir() {
        return;
    }

    let workspace_key = normalize_path_for_identity(&workspace_root);
    let index_file = index_file_path_for_workspace(&workspace_root);
    let now = current_unix_timestamp();
    let stale_after = stale_index_after_secs();

    set_active_workspace(&workspace_key, &workspace_source);

    let Some(action) = classify_index_action(&workspace_key, now, stale_after) else {
        spawn_background_indexer(workspace_root, workspace_source);
        return;
    };

    if let Some(requested_at) = action.refresh_requested_at
        && now.saturating_sub(requested_at) < MIN_REFRESH_INTERVAL_SECS
    {
        return;
    }

    if action.needs_refresh {
        record_refresh_request(&workspace_key, &workspace_source);
        spawn_full_scan_refresh(workspace_root, workspace_key, index_file, false);
    }
}

struct EnsureIndexAction {
    needs_refresh: bool,
    refresh_requested_at: Option<u64>,
}

fn classify_index_action(
    workspace_key: &str,
    now: u64,
    stale_after: u64,
) -> Option<EnsureIndexAction> {
    let runtime = match INDEX_RUNTIMES.read() {
        Ok(guard) => guard.get(workspace_key).cloned(),
        Err(_) => None,
    }?;

    if !ACTIVE_INDEXERS.contains_key(workspace_key) {
        return None;
    }

    let last_scan_completed_at = runtime.last_scan_completed_at;
    let index_is_stale = last_scan_completed_at
        .map(|timestamp| now.saturating_sub(timestamp) >= stale_after)
        .unwrap_or(true);

    Some(EnsureIndexAction {
        needs_refresh: !runtime.scan_complete || index_is_stale,
        refresh_requested_at: runtime.last_refresh_requested_at,
    })
}

fn reset_runtime_state(
    workspace_key: &str,
    workspace_root: &Path,
    workspace_source: &str,
    index_file: &Path,
) {
    PATH_INDEXES.insert(
        workspace_key.to_string(),
        Arc::new(RwLock::new(PathIndex::default())),
    );

    with_runtime_map_write(|state| {
        state.insert(
            workspace_key.to_string(),
            IndexRuntime {
                workspace_root: workspace_root.to_path_buf(),
                workspace_source: workspace_source.to_string(),
                index_file: index_file.to_path_buf(),
                loaded_from_disk: false,
                scan_complete: false,
                last_loaded_entries: 0,
                last_persisted_entries: 0,
                last_persisted_at: None,
                last_scan_completed_at: None,
                last_refresh_requested_at: None,
                last_request_source: Some(workspace_source.to_string()),
                last_error: None,
                indexed_entries_count: 0,
            },
        );
    });
}

fn record_load_state(
    workspace_key: &str,
    loaded_from_disk: bool,
    scan_complete: bool,
    loaded_entries: usize,
    saved_at: u64,
    err: Option<String>,
) {
    with_runtime_write(workspace_key, |state| {
        state.loaded_from_disk = loaded_from_disk;
        state.scan_complete = scan_complete;
        state.last_loaded_entries = loaded_entries;
        state.indexed_entries_count = loaded_entries;
        if scan_complete && saved_at > 0 {
            state.last_scan_completed_at = Some(saved_at);
        }
        state.last_error = err;
    });
}

fn record_persist_state(workspace_key: &str, persisted_entries: usize, err: Option<String>) {
    with_runtime_write(workspace_key, |state| {
        state.last_persisted_entries = persisted_entries;
        if err.is_none() {
            state.last_persisted_at = Some(current_unix_timestamp());
        }
        state.last_error = err;
    });
}

fn record_runtime_error(workspace_key: &str, err: String) {
    with_runtime_write(workspace_key, |state| {
        state.last_error = Some(err);
    });
}

fn record_refresh_request(workspace_key: &str, request_source: &str) {
    let now = current_unix_timestamp();
    with_runtime_write(workspace_key, |state| {
        state.last_refresh_requested_at = Some(now);
        state.last_request_source = Some(request_source.to_string());
    });
}

fn record_scan_complete(workspace_key: &str, scan_complete: bool) {
    with_runtime_write(workspace_key, |state| {
        state.scan_complete = scan_complete;
        if scan_complete {
            state.last_scan_completed_at = Some(current_unix_timestamp());
        }
    });
}

fn record_indexed_entries_count(workspace_key: &str, indexed_entries_count: usize) {
    with_runtime_write(workspace_key, |state| {
        state.indexed_entries_count = indexed_entries_count;
    });
}

fn set_active_workspace(workspace_key: &str, request_source: &str) {
    if let Ok(mut guard) = ACTIVE_WORKSPACE_KEY.write() {
        *guard = Some(workspace_key.to_string());
    }

    with_runtime_write(workspace_key, |state| {
        state.last_request_source = Some(request_source.to_string());
    });
}

fn is_runtime_scan_complete(workspace_key: &str) -> bool {
    match INDEX_RUNTIMES.read() {
        Ok(guard) => guard
            .get(workspace_key)
            .map(|state| state.scan_complete)
            .unwrap_or(false),
        Err(_) => false,
    }
}

fn spawn_full_scan_refresh(
    workspace_root: PathBuf,
    workspace_key: String,
    index_file: PathBuf,
    reset_index: bool,
) {
    if ACTIVE_FULL_SCANS
        .insert(workspace_key.clone(), ())
        .is_some()
    {
        if reset_index {
            PENDING_FULL_SCAN_RESETS.insert(workspace_key, ());
        }
        return;
    }

    thread::spawn(move || {
        let _guard = ActiveFullScanGuard::new(workspace_key.clone());
        let mut reset_next = reset_index;

        loop {
            if reset_next {
                record_scan_complete(&workspace_key, false);
                clear_path_index(&workspace_key);
            }

            let mut changed_since_persist = 0usize;
            let mut last_persist = Instant::now();

            for entry in ignore::WalkBuilder::new(&workspace_root)
                .hidden(true)
                .ignore(true)
                .git_ignore(true)
                .git_exclude(true)
                .require_git(false)
                .build()
                .flatten()
            {
                let Some(file_type) = entry.file_type() else {
                    continue;
                };
                if !file_type.is_file() && !file_type.is_dir() {
                    continue;
                }

                let Ok(metadata) = entry.metadata() else {
                    continue;
                };

                if apply_path_metadata(
                    &workspace_key,
                    &workspace_root,
                    entry.path(),
                    file_type.is_dir(),
                    &metadata,
                ) {
                    changed_since_persist += 1;
                }

                if changed_since_persist >= FULL_SCAN_PERSIST_CHUNK
                    || last_persist.elapsed() >= Duration::from_secs(PERSIST_INTERVAL_SECS)
                {
                    match persist_index_snapshot(&workspace_key, &workspace_root, &index_file) {
                        Ok(saved) => record_persist_state(&workspace_key, saved, None),
                        Err(err) => record_persist_state(&workspace_key, 0, Some(err)),
                    }
                    changed_since_persist = 0;
                    last_persist = Instant::now();
                }
            }

            record_scan_complete(&workspace_key, true);
            match persist_index_snapshot(&workspace_key, &workspace_root, &index_file) {
                Ok(saved) => record_persist_state(&workspace_key, saved, None),
                Err(err) => record_persist_state(&workspace_key, 0, Some(err)),
            }

            reset_next = PENDING_FULL_SCAN_RESETS.remove(&workspace_key).is_some();
            if !reset_next {
                break;
            }
        }
    });
}

fn with_runtime_map_write<F>(mutate: F)
where
    F: FnOnce(&mut BTreeMap<String, IndexRuntime>),
{
    if let Ok(mut guard) = INDEX_RUNTIMES.write() {
        mutate(&mut guard);
    }
}

fn with_runtime_write<F>(workspace_key: &str, mutate: F)
where
    F: FnOnce(&mut IndexRuntime),
{
    if let Ok(mut guard) = INDEX_RUNTIMES.write()
        && let Some(state) = guard.get_mut(workspace_key)
    {
        mutate(state);
    }
}

fn path_index_handle(workspace_key: &str) -> Option<Arc<RwLock<PathIndex>>> {
    PATH_INDEXES
        .get(workspace_key)
        .map(|entry| entry.value().clone())
}

fn with_path_index_read<T, F>(workspace_key: &str, read: F) -> Option<T>
where
    F: FnOnce(&PathIndex) -> T,
{
    let handle = path_index_handle(workspace_key)?;
    let guard = handle.read().ok()?;
    Some(read(&guard))
}

fn with_path_index_write<T, F>(workspace_key: &str, mutate: F) -> Option<T>
where
    F: FnOnce(&mut PathIndex) -> T,
{
    let handle = path_index_handle(workspace_key)?;
    let mut guard = handle.write().ok()?;
    Some(mutate(&mut guard))
}

fn clear_path_index(workspace_key: &str) {
    let _ = with_path_index_write(workspace_key, |index| index.clear());
    record_indexed_entries_count(workspace_key, 0);
}

struct ActiveIndexerGuard {
    workspace_key: String,
}

impl ActiveIndexerGuard {
    fn new(workspace_key: String) -> Self {
        Self { workspace_key }
    }
}

impl Drop for ActiveIndexerGuard {
    fn drop(&mut self) {
        ACTIVE_INDEXERS.remove(&self.workspace_key);
    }
}

struct ActiveFullScanGuard {
    workspace_key: String,
}

impl ActiveFullScanGuard {
    fn new(workspace_key: String) -> Self {
        Self { workspace_key }
    }
}

impl Drop for ActiveFullScanGuard {
    fn drop(&mut self) {
        ACTIVE_FULL_SCANS.remove(&self.workspace_key);
    }
}

impl PathIndex {
    fn clear(&mut self) {
        self.entries.clear();
        self.path_lookup.clear();
        self.term_postings.clear();
        self.live_entries = 0;
    }

    fn upsert(
        &mut self,
        workspace_root: &Path,
        path: &Path,
        is_dir: bool,
        size: u64,
        last_modified: u64,
    ) -> bool {
        let absolute_path = canonicalize_or_original(path.to_path_buf());
        let identity = normalize_path_for_identity(&absolute_path);
        let relative_path = relative_path_from_workspace(workspace_root, &absolute_path);
        let relative_path_lower = normalize_query_value(&relative_path);
        let normalized_path = normalize_output_path(&absolute_path);
        let file_name_lower = absolute_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let extension_lower = absolute_path
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase();

        if let Some(&entry_id) = self.path_lookup.get(&identity)
            && let Some(existing) = self.entries.get_mut(entry_id).and_then(Option::as_mut)
        {
            if existing.is_dir == is_dir
                && existing.size == size
                && existing.last_modified == last_modified
            {
                return false;
            }

            existing.absolute_path = absolute_path;
            existing.normalized_path = normalized_path;
            existing.relative_path = relative_path;
            existing.relative_path_lower = relative_path_lower;
            existing.file_name_lower = file_name_lower;
            existing.extension_lower = extension_lower;
            existing.is_dir = is_dir;
            existing.size = size;
            existing.last_modified = last_modified;
            return true;
        }

        let entry = PathIndexEntry {
            absolute_path,
            normalized_path,
            relative_path,
            relative_path_lower,
            file_name_lower,
            extension_lower,
            is_dir,
            size,
            last_modified,
        };
        let entry_id = self.entries.len();
        self.entries.push(Some(entry.clone()));
        self.path_lookup.insert(identity, entry_id);
        self.live_entries += 1;
        self.index_entry_terms(entry_id, &entry);
        true
    }

    fn insert_persisted_entry(&mut self, workspace_root: &Path, entry: &PersistedEntry) {
        let absolute_path = path_from_relative(workspace_root, &entry.relative_path);
        let _ = self.upsert(
            workspace_root,
            &absolute_path,
            entry.is_dir,
            entry.size,
            entry.last_modified,
        );
    }

    fn remove_path(&mut self, path: &Path) -> bool {
        let identity = normalize_path_for_identity(path);
        let Some(entry_id) = self.path_lookup.remove(&identity) else {
            return false;
        };
        if self
            .entries
            .get_mut(entry_id)
            .and_then(Option::take)
            .is_some()
        {
            self.live_entries = self.live_entries.saturating_sub(1);
            return true;
        }
        false
    }

    fn remove_subtree(&mut self, path: &Path) -> usize {
        let prefix = normalize_path_for_identity(path);
        let child_prefix = prefix.clone() + "/";
        let keys: Vec<String> = self
            .path_lookup
            .keys()
            .filter(|key| **key == prefix || key.starts_with(&child_prefix))
            .cloned()
            .collect();

        let mut removed = 0usize;
        for key in keys {
            if let Some(entry_id) = self.path_lookup.remove(&key)
                && self
                    .entries
                    .get_mut(entry_id)
                    .and_then(Option::take)
                    .is_some()
            {
                removed += 1;
            }
        }
        self.live_entries = self.live_entries.saturating_sub(removed);
        removed
    }

    fn persisted_entries(&self) -> Vec<PersistedEntry> {
        let mut entries = self
            .entries
            .iter()
            .filter_map(|entry| entry.as_ref())
            .map(|entry| PersistedEntry {
                relative_path: entry.relative_path.clone(),
                is_dir: entry.is_dir,
                size: entry.size,
                last_modified: entry.last_modified,
            })
            .collect::<Vec<_>>();

        entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        entries
    }

    fn index_entry_terms(&mut self, entry_id: usize, entry: &PathIndexEntry) {
        let mut terms = entry_terms(entry);
        terms.sort();
        terms.dedup();
        for term in terms {
            self.term_postings.entry(term).or_default().push(entry_id);
        }
    }

    fn shortlist_candidates(
        &self,
        anchor_terms: &[String],
        root_prefix: Option<&str>,
        shortlist_limit: usize,
    ) -> Option<Vec<PathQueryCandidate>> {
        let shortlist_limit = shortlist_limit.clamp(1, MAX_SHORTLIST_CANDIDATES);
        let mut counts: HashMap<usize, u16> = HashMap::new();
        let mut used_terms = 0usize;

        for term in anchor_terms {
            let Some(postings) = self.term_postings.get(term) else {
                continue;
            };
            if postings.len() > MAX_EXACT_TERM_POSTINGS {
                continue;
            }

            used_terms += 1;
            for entry_id in postings {
                *counts.entry(*entry_id).or_insert(0) += 1;
            }

            if counts.len() > MAX_SHORTLIST_WORKSET {
                return None;
            }
            if used_terms >= 1 && counts.len() <= shortlist_limit {
                break;
            }
        }

        if used_terms == 0 || counts.is_empty() {
            return None;
        }

        let mut ranked = counts.into_iter().collect::<Vec<_>>();
        ranked.sort_by(|(left_id, left_hits), (right_id, right_hits)| {
            right_hits.cmp(left_hits).then_with(|| {
                let left_key = self
                    .entries
                    .get(*left_id)
                    .and_then(|entry| entry.as_ref())
                    .map(|entry| entry.relative_path.as_str())
                    .unwrap_or("");
                let right_key = self
                    .entries
                    .get(*right_id)
                    .and_then(|entry| entry.as_ref())
                    .map(|entry| entry.relative_path.as_str())
                    .unwrap_or("");
                left_key.cmp(right_key)
            })
        });

        let mut results = Vec::new();
        for (entry_id, _) in ranked {
            let Some(entry) = self.entries.get(entry_id).and_then(|entry| entry.as_ref()) else {
                continue;
            };
            if !entry_matches_prefix(entry, root_prefix) {
                continue;
            }

            results.push(PathQueryCandidate {
                path: entry.absolute_path.clone(),
                is_dir: entry.is_dir,
                size: entry.size,
                modified_at: entry.last_modified,
            });
            if results.len() >= shortlist_limit {
                break;
            }
        }

        if results.is_empty() {
            return None;
        }

        Some(results)
    }
}

fn entry_matches_prefix(entry: &PathIndexEntry, root_prefix: Option<&str>) -> bool {
    let Some(prefix) = root_prefix else {
        return true;
    };
    if prefix.is_empty() {
        return true;
    }

    entry.relative_path_lower == prefix
        || entry
            .relative_path_lower
            .starts_with(&(prefix.to_string() + "/"))
}

fn entry_terms(entry: &PathIndexEntry) -> Vec<String> {
    let mut terms = HashSet::new();

    if !entry.relative_path_lower.is_empty() {
        for segment in entry.relative_path_lower.split('/') {
            push_segment_terms(&mut terms, segment);
        }
    }

    if !entry.file_name_lower.is_empty() {
        push_segment_terms(&mut terms, &entry.file_name_lower);
    }

    terms.into_iter().collect()
}

fn push_segment_terms(terms: &mut HashSet<String>, segment: &str) {
    let segment = segment.trim_matches('/');
    if segment.is_empty() {
        return;
    }

    if segment.len() >= MIN_INDEX_TERM_LEN {
        terms.insert(segment.to_string());
    }

    for token in segment
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|token| token.len() >= MIN_INDEX_TERM_LEN)
    {
        terms.insert(token.to_string());
    }
}

fn query_anchor_terms(pattern: &str) -> Vec<String> {
    let normalized = normalize_query_value(pattern);
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut terms = HashSet::new();
    if normalized.len() >= MIN_INDEX_TERM_LEN
        && normalized
            .chars()
            .any(|ch| matches!(ch, '/' | '.' | '_' | '-'))
    {
        terms.insert(normalized.clone());
    }

    for segment in normalized.split('/') {
        push_segment_terms(&mut terms, segment);
    }

    let mut ordered = terms.into_iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
    ordered
}

fn relative_root_prefix(workspace_root: &Path, search_root: &Path) -> Option<Option<String>> {
    if !path_belongs_to_workspace(workspace_root, search_root) {
        return None;
    }
    let stripped = search_root.strip_prefix(workspace_root).ok()?;
    let relative = normalize_query_value(&normalize_output_path(stripped));
    if relative.is_empty() {
        Some(None)
    } else {
        Some(Some(relative))
    }
}

fn relative_path_from_workspace(workspace_root: &Path, path: &Path) -> String {
    path.strip_prefix(workspace_root)
        .ok()
        .map(normalize_output_path)
        .unwrap_or_else(|| normalize_output_path(path))
}

fn path_from_relative(workspace_root: &Path, relative_path: &str) -> PathBuf {
    if relative_path.is_empty() {
        return workspace_root.to_path_buf();
    }

    let mut path = workspace_root.to_path_buf();
    for component in relative_path.split('/') {
        if component.is_empty() {
            continue;
        }
        path.push(component);
    }
    path
}

fn normalize_query_value(value: &str) -> String {
    value.replace('\\', "/").to_ascii_lowercase()
}

fn normalize_output_path(path: &Path) -> String {
    let raw = path.to_string_lossy();
    if let Some(stripped) = raw.strip_prefix("\\\\?\\UNC\\") {
        format!("//{}", stripped.replace('\\', "/"))
    } else if let Some(stripped) = raw.strip_prefix("\\\\?\\") {
        stripped.replace('\\', "/")
    } else {
        raw.replace('\\', "/")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tempfile::tempdir;

    lazy_static::lazy_static! {
        static ref TEST_CACHE_LOCK: Mutex<()> = Mutex::new(());
    }

    fn reset_index_state_for_tests() {
        PATH_INDEXES.clear();
        ACTIVE_INDEXERS.clear();
        ACTIVE_FULL_SCANS.clear();
        PENDING_FULL_SCAN_RESETS.clear();
        INDEX_STORAGE_CLEARED.store(false, Ordering::Release);
        if let Ok(mut guard) = ACTIVE_WORKSPACE_KEY.write() {
            *guard = None;
        }
        with_runtime_map_write(|state| state.clear());
    }

    #[test]
    fn workspace_index_path_is_stable_and_workspace_scoped() {
        let _guard = TEST_CACHE_LOCK.lock().unwrap();
        reset_index_state_for_tests();
        let storage_root = tempdir().unwrap();
        let ws1 = tempdir().unwrap();
        let ws2 = tempdir().unwrap();

        let idx1_a = index_file_path_for_workspace_in(storage_root.path(), ws1.path());
        let idx1_b = index_file_path_for_workspace_in(storage_root.path(), ws1.path());
        let idx2 = index_file_path_for_workspace_in(storage_root.path(), ws2.path());

        assert_eq!(idx1_a, idx1_b);
        assert_ne!(idx1_a, idx2);
    }

    #[test]
    fn persist_and_load_roundtrip_restores_path_candidates() {
        let _guard = TEST_CACHE_LOCK.lock().unwrap();
        reset_index_state_for_tests();

        let workspace = tempdir().unwrap();
        let storage_root = tempdir().unwrap();
        let workspace_root = workspace.path().canonicalize().unwrap();
        let index_file = index_file_path_for_workspace_in(storage_root.path(), &workspace_root);
        let key = normalize_path_for_identity(&workspace_root);

        reset_runtime_state(&key, &workspace_root, "test", &index_file);
        fs::create_dir_all(workspace_root.join("chrome/app")).unwrap();
        fs::write(
            workspace_root.join("chrome/app/chromium_strings.grd"),
            "<messages />\n",
        )
        .unwrap();

        let dir_meta = fs::metadata(workspace_root.join("chrome/app")).unwrap();
        let file_meta =
            fs::metadata(workspace_root.join("chrome/app/chromium_strings.grd")).unwrap();
        apply_path_metadata(
            &key,
            &workspace_root,
            &workspace_root.join("chrome/app"),
            true,
            &dir_meta,
        );
        apply_path_metadata(
            &key,
            &workspace_root,
            &workspace_root.join("chrome/app/chromium_strings.grd"),
            false,
            &file_meta,
        );
        record_scan_complete(&key, true);

        let saved = persist_index_snapshot(&key, &workspace_root, &index_file).unwrap();
        assert_eq!(saved, 2);

        clear_path_index(&key);
        let loaded = load_persisted_index(&workspace_root, &index_file)
            .unwrap()
            .unwrap();
        let loaded_count = apply_snapshot_to_index(&key, &workspace_root, &loaded);
        record_scan_complete(&key, true);
        assert_eq!(loaded_count, 2);

        let candidates = query_path_candidates(&workspace_root, "chromium_strings.grd", 8)
            .expect("path shortlist");
        assert_eq!(candidates.len(), 1);
        assert!(
            candidates[0]
                .path
                .ends_with(Path::new("chrome/app/chromium_strings.grd"))
        );
    }

    #[test]
    fn path_is_indexable_respects_gitignore_for_single_file_checks() {
        let _guard = TEST_CACHE_LOCK.lock().unwrap();
        reset_index_state_for_tests();

        let workspace = tempdir().unwrap();
        let root = workspace.path().canonicalize().unwrap();
        let tracked = root.join("tracked.rs");
        let ignored = root.join("ignored.rs");

        fs::write(root.join(".gitignore"), "ignored.rs\n").unwrap();
        fs::write(&tracked, "fn tracked() {}\n").unwrap();
        fs::write(&ignored, "fn ignored() {}\n").unwrap();

        assert!(path_is_indexable(&root, &tracked));
        assert!(!path_is_indexable(&root, &ignored));
    }

    #[test]
    fn classify_index_action_marks_old_scan_as_stale() {
        let _guard = TEST_CACHE_LOCK.lock().unwrap();
        reset_index_state_for_tests();

        let workspace = tempdir().unwrap();
        let root = workspace.path().canonicalize().unwrap();
        let key = normalize_path_for_identity(&root);
        let index_file = index_file_path_for_workspace(&root);
        let now = current_unix_timestamp();

        reset_runtime_state(&key, &root, "test", &index_file);
        with_runtime_write(&key, |state| {
            state.scan_complete = true;
            state.last_scan_completed_at = Some(now.saturating_sub(120));
        });
        ACTIVE_INDEXERS.insert(key.clone(), ());

        let action = classify_index_action(&key, now, 60).unwrap();
        assert!(action.needs_refresh);
    }

    #[test]
    fn active_runtime_snapshot_prefers_latest_workspace_request() {
        let _guard = TEST_CACHE_LOCK.lock().unwrap();
        reset_index_state_for_tests();

        let workspace_a = tempdir().unwrap();
        let workspace_b = tempdir().unwrap();
        let root_a = workspace_a.path().canonicalize().unwrap();
        let root_b = workspace_b.path().canonicalize().unwrap();
        let key_a = normalize_path_for_identity(&root_a);
        let key_b = normalize_path_for_identity(&root_b);
        let index_a = index_file_path_for_workspace(&root_a);
        let index_b = index_file_path_for_workspace(&root_b);

        reset_runtime_state(&key_a, &root_a, "client_initialize", &index_a);
        reset_runtime_state(&key_b, &root_b, "client_initialize", &index_b);
        set_active_workspace(&key_b, "tool_call:fuzzy_find");

        let snapshot = get_active_runtime_snapshot().unwrap();
        assert_eq!(snapshot.workspace_root, root_b.to_string_lossy());
        assert_eq!(
            snapshot.last_request_source.as_deref(),
            Some("tool_call:fuzzy_find")
        );
    }
}
