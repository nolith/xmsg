//! A synthetic `/proc/<pid>/stat` whose content changes between reads, as a reused PID's does.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// A stat line with `ppid` (field 4) and start time `start` (field 22).
pub fn stat_line(pid: u32, ppid: u32, start: u64) -> String {
    format!("{pid} (p{pid}) S {ppid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {start} 0 0\n")
}

/// Makes `<proc_root>/<pid>/stat` a FIFO whose successive reads return `lines` in turn,
/// cycling: each open sees one line and then EOF, as a fresh read of `/proc` does.
///
/// The pause after each line lets the reader close before the next line is written. A reader
/// slower than that drops the next line, which shifts the sequence but never serves content
/// that is not in `lines`.
pub fn reused_stat(proc_root: &Path, pid: u32, lines: Vec<String>) {
    let dir = proc_root.join(pid.to_string());
    fs::create_dir_all(&dir).unwrap();
    let fifo = dir.join("stat");
    let _ = fs::remove_file(&fifo);
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap();
    assert!(made.success(), "mkfifo {fifo:?}");
    std::thread::spawn(move || {
        for line in lines.iter().cycle().take(64) {
            let Ok(mut f) = fs::OpenOptions::new().write(true).open(&fifo) else {
                return;
            };
            let _ = f.write_all(line.as_bytes());
            drop(f);
            std::thread::sleep(Duration::from_millis(50));
        }
    });
}
