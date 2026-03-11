use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Tier {
    Allowed,
    Blocked,
}

pub struct PathGuard {
    pub blocked_patterns: Vec<glob::Pattern>,
}

impl PathGuard {
    pub fn new(blocked: Vec<String>) -> Self {
        let patterns = blocked
            .iter()
            .filter_map(|p| glob::Pattern::new(p).ok())
            .collect();
        Self {
            blocked_patterns: patterns,
        }
    }

    pub fn check_path(&self, raw_path: impl AsRef<Path>) -> (PathBuf, Tier, Option<String>) {
        let path = raw_path.as_ref();
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let canonical_str = canonical.to_string_lossy().replace('\\', "/");

        for pattern in &self.blocked_patterns {
            if pattern.matches(&canonical_str) {
                return (
                    canonical,
                    Tier::Blocked,
                    Some(format!("Path matches blocked pattern: {}", pattern)),
                );
            }
        }

        (canonical, Tier::Allowed, None)
    }
}

lazy_static::lazy_static! {
    pub static ref GUARD: PathGuard = PathGuard::new(vec![
        "**/node_modules/**".to_string(),
        "**/.git/objects/**".to_string(),
    ]);
}
