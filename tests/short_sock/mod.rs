//! Test socket paths that stay short whatever the id they serve.
//!
//! Portability: macOS caps a unix socket path at 104 bytes (`SUN_LEN`) and its `$TMPDIR` is long,
//! so a test socket's file name never embeds a session/instance id or node name.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

/// `dir/<prefix>-<n>.sock`, with `n` unique within the test binary.
pub fn sock_path(dir: &Path, prefix: &str) -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    dir.join(format!(
        "{prefix}-{}.sock",
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
