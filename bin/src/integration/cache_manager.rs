//! Cache manager for market data.
//!
//! Provides a singleton-like cache manager that handles the SQLite cache
//! with a platform-specific default location.

use perth_data::cache::SqliteCache;
use perth_data::error::DataError;
use std::path::PathBuf;

/// Get the default cache directory path.
///
/// Uses platform-specific cache directories:
/// - Linux: `~/.cache/perth/`
/// - macOS: `~/Library/Caches/perth/`
/// - Windows: `%LOCALAPPDATA%\perth\cache\`
pub(crate) fn default_cache_dir() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("perth")
}

/// Get the default cache database path.
pub(crate) fn default_cache_path() -> PathBuf {
    default_cache_dir().join("perth.db")
}

/// Get the configured cache path.
pub(crate) fn get_cache_path() -> PathBuf {
    default_cache_path()
}

/// Open the cache, creating the directory if needed.
pub(crate) fn open_cache() -> Result<SqliteCache, DataError> {
    let cache_path = get_cache_path();

    // Ensure parent directory exists
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    SqliteCache::new(&cache_path)
}
