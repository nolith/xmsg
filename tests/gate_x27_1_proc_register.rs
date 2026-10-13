use serde_json::Value;
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::broadcast;

use xmsg::agy::{
    dev_major, dev_minor, new_agy_store, run_register_server, scan_and_register_agy_proc, AgyConfig,
};
use xmsg::storage;

fn setup_mock_process(
    proc_root: &Path,
    pid: u32,
    ppid: u32,
    exe: Option<&Path>,
    open_file: Option<&Path>,
    starttime: &str,
) {
    let pid_dir = proc_root.join(pid.to_string());
    let fd_dir = pid_dir.join("fd");
    fs::create_dir_all(&fd_dir).unwrap();

    let stat_content = format!(
        "{pid} (proc) S {ppid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {starttime} 0 0 0 0 0 0 0 0 0 0\n"
    );
    fs::write(pid_dir.join("stat"), stat_content).unwrap();

    if let Some(e) = exe {
        let exe_link = pid_dir.join("exe");
        let _ = fs::remove_file(&exe_link);
        std::os::unix::fs::symlink(e, &exe_link).unwrap();
    }

    if let Some(target) = open_file {
        let fd_link = fd_dir.join("3");
        let _ = fs::remove_file(&fd_link);
        std::os::unix::fs::symlink(target, &fd_link).unwrap();
    }
}

fn run_hook(bin: &str, sock_path: &Path, conv_id: &str) -> (i32, Value) {
    use std::io::Write;
    let mut child = Command::new(bin)
        .args([
            "register",
            "agy",
            "--hook",
            "--sock",
            sock_path.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let stdin_payload = serde_json::json!({
        "conversationId": conv_id
    });
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(stdin_payload.to_string().as_bytes())
        .unwrap();

    let output = child.wait_with_output().unwrap();
    let status = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8(output.stdout).unwrap();
    let val: Value = serde_json::from_str(stdout.trim()).expect("valid json output");
    (status, val)
}

// ---------------------------------------------------------------------------
// Oracle 1:
// Fake agy tree: an attested agy with the presence lock, a child whose environ
// carries both variables => conversation is registered without any hook.
// Mutant: watcher disabled => RED.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn oracle_1_fake_agy_tree_child_environ_registers_without_hook() {
    let proc_dir = tempdir().unwrap();
    let proc_root = proc_dir.path().to_path_buf();
    let presence_dir = tempdir().unwrap();
    let proc_locks_dir = tempdir().unwrap();
    let proc_locks = proc_locks_dir.path().join("locks");

    let conv_id = "conv-oracle-1";
    let lock_file = presence_dir.path().join(format!("{conv_id}.lock"));
    fs::write(&lock_file, "").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    let maj = dev_major(meta.dev());
    let min = dev_minor(meta.dev());
    let ino = meta.ino();

    let holder_pid = 1000u32;
    let child_pid = 1001u32;

    fs::write(
        &proc_locks,
        format!(
            "1: FLOCK ADVISORY WRITE {holder_pid} {:02x}:{:02x}:{} 0 EOF\n",
            maj, min, ino
        ),
    )
    .unwrap();

    let trusted_exe = std::env::current_exe().unwrap();
    setup_mock_process(
        &proc_root,
        holder_pid,
        1,
        Some(&trusted_exe),
        Some(&lock_file),
        "100",
    );

    setup_mock_process(&proc_root, child_pid, holder_pid, None, None, "200");
    let environ_path = proc_root.join(child_pid.to_string()).join("environ");
    let env_bytes = b"ANTIGRAVITY_LS_ADDRESS=127.0.0.1:44555\0ANTIGRAVITY_CSRF_TOKEN=token-oracle-1\0OTHER_VAR=secret\0";
    fs::write(&environ_path, env_bytes).unwrap();

    let config = AgyConfig {
        presence_dir: presence_dir.path().to_path_buf(),
        proc_locks_path: proc_locks,
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_exe],
    };

    let store = new_agy_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    storage::init_db(&conn).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let my_uid = xmsg::agent::current_uid();

    let reg_count = scan_and_register_agy_proc(&config, &store, &db, my_uid)
        .await
        .expect("scan should succeed");

    assert_eq!(reg_count, 1, "must register exactly 1 conversation");

    let store_lock = store.read().unwrap();
    let reg_info = store_lock
        .values()
        .find(|i| i.conversation_id == conv_id)
        .expect("conversation must be registered in store");

    let creds = reg_info
        .credentials
        .as_ref()
        .expect("credentials must be present");
    assert_eq!(creds.ls_address, "127.0.0.1:44555");
    assert_eq!(creds.csrf_token, "token-oracle-1");
    assert!(!creds.is_stale);
}

// ---------------------------------------------------------------------------
// Oracle 2:
// Variables in a process OUTSIDE the attested agy's subtree (sibling process or
// child of a non-attested agy) => not registered.
// Mutant: skip the ancestry check => RED.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn oracle_2_process_outside_attested_subtree_not_registered() {
    let proc_dir = tempdir().unwrap();
    let proc_root = proc_dir.path().to_path_buf();
    let presence_dir = tempdir().unwrap();
    let proc_locks_dir = tempdir().unwrap();
    let proc_locks = proc_locks_dir.path().join("locks");

    let conv_id = "conv-oracle-2";
    let lock_file = presence_dir.path().join(format!("{conv_id}.lock"));
    fs::write(&lock_file, "").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    let maj = dev_major(meta.dev());
    let min = dev_minor(meta.dev());
    let ino = meta.ino();

    let holder_pid = 2000u32;
    let sibling_pid = 2001u32; // Sibling: PPID is 1, NOT holder_pid!

    fs::write(
        &proc_locks,
        format!(
            "1: FLOCK ADVISORY WRITE {holder_pid} {:02x}:{:02x}:{} 0 EOF\n",
            maj, min, ino
        ),
    )
    .unwrap();

    let trusted_exe = std::env::current_exe().unwrap();
    setup_mock_process(
        &proc_root,
        holder_pid,
        1,
        Some(&trusted_exe),
        Some(&lock_file),
        "100",
    );

    // Sibling process carries variables but is NOT a descendant of holder_pid
    setup_mock_process(&proc_root, sibling_pid, 1, None, None, "200");
    let environ_path = proc_root.join(sibling_pid.to_string()).join("environ");
    let env_bytes =
        b"ANTIGRAVITY_LS_ADDRESS=127.0.0.1:44555\0ANTIGRAVITY_CSRF_TOKEN=token-oracle-2\0";
    fs::write(&environ_path, env_bytes).unwrap();

    let config = AgyConfig {
        presence_dir: presence_dir.path().to_path_buf(),
        proc_locks_path: proc_locks,
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_exe],
    };

    let store = new_agy_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    storage::init_db(&conn).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let my_uid = xmsg::agent::current_uid();

    let reg_count = scan_and_register_agy_proc(&config, &store, &db, my_uid)
        .await
        .expect("scan should succeed");

    assert_eq!(
        reg_count, 0,
        "sibling outside subtree must not be registered"
    );
    assert!(store.read().unwrap().is_empty(), "store must remain empty");
}

// ---------------------------------------------------------------------------
// Oracle 3:
// Child whose PID was recycled (start time mismatch) => ignored.
// Mutant: no start-time check => RED.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn oracle_3_recycled_child_pid_starttime_mismatch_ignored() {
    let proc_dir = tempdir().unwrap();
    let proc_root = proc_dir.path().to_path_buf();
    let presence_dir = tempdir().unwrap();
    let proc_locks_dir = tempdir().unwrap();
    let proc_locks = proc_locks_dir.path().join("locks");

    let conv_id = "conv-oracle-3";
    let lock_file = presence_dir.path().join(format!("{conv_id}.lock"));
    fs::write(&lock_file, "").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    let maj = dev_major(meta.dev());
    let min = dev_minor(meta.dev());
    let ino = meta.ino();

    let holder_pid = 3000u32;
    let child_pid = 3001u32;

    fs::write(
        &proc_locks,
        format!(
            "1: FLOCK ADVISORY WRITE {holder_pid} {:02x}:{:02x}:{} 0 EOF\n",
            maj, min, ino
        ),
    )
    .unwrap();

    let trusted_exe = std::env::current_exe().unwrap();
    setup_mock_process(
        &proc_root,
        holder_pid,
        1,
        Some(&trusted_exe),
        Some(&lock_file),
        "100",
    );

    setup_mock_process(&proc_root, child_pid, holder_pid, None, None, "200");
    let environ_path = proc_root.join(child_pid.to_string()).join("environ");

    // Use a FIFO so we can mutate the child's stat starttime while environ is being read
    let fifo_path = environ_path.clone();
    let _ = fs::remove_file(&fifo_path);
    let status = Command::new("mkfifo")
        .arg(&fifo_path)
        .status()
        .expect("mkfifo command failed");
    assert!(status.success());

    let proc_root_clone = proc_root.clone();
    let fifo_writer = std::thread::spawn(move || {
        use std::io::Write;
        let mut f = fs::OpenOptions::new().write(true).open(&fifo_path).unwrap();

        // While reader is opening/reading the FIFO, rewrite the child's stat file with a new starttime (recycled PID)
        let recycled_stat = format!(
            "{child_pid} (proc) S {holder_pid} 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 9999 0 0 0 0 0 0 0 0 0 0\n"
        );
        fs::write(
            proc_root_clone.join(child_pid.to_string()).join("stat"),
            recycled_stat,
        )
        .unwrap();

        let env_bytes =
            b"ANTIGRAVITY_LS_ADDRESS=127.0.0.1:44555\0ANTIGRAVITY_CSRF_TOKEN=token-oracle-3\0";
        f.write_all(env_bytes).unwrap();
    });

    let config = AgyConfig {
        presence_dir: presence_dir.path().to_path_buf(),
        proc_locks_path: proc_locks,
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_exe],
    };

    let store = new_agy_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    storage::init_db(&conn).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let my_uid = xmsg::agent::current_uid();

    let reg_count = scan_and_register_agy_proc(&config, &store, &db, my_uid)
        .await
        .expect("scan should complete without error");

    fifo_writer.join().unwrap();

    assert_eq!(reg_count, 0, "recycled child pid must be ignored");
    assert!(store.read().unwrap().is_empty(), "store must remain empty");
}

// ---------------------------------------------------------------------------
// Oracle 4:
// Unreadable environ => no registration, no error surfaced, hook output unchanged.
// Mutant: treat unreadable as fatal => RED.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn oracle_4_unreadable_environ_no_registration_no_fatal_error() {
    let proc_dir = tempdir().unwrap();
    let proc_root = proc_dir.path().to_path_buf();
    let presence_dir = tempdir().unwrap();
    let proc_locks_dir = tempdir().unwrap();
    let proc_locks = proc_locks_dir.path().join("locks");

    let conv_id = "conv-oracle-4";
    let lock_file = presence_dir.path().join(format!("{conv_id}.lock"));
    fs::write(&lock_file, "").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    let maj = dev_major(meta.dev());
    let min = dev_minor(meta.dev());
    let ino = meta.ino();

    let holder_pid = 4000u32;
    let child_pid = 4001u32;

    fs::write(
        &proc_locks,
        format!(
            "1: FLOCK ADVISORY WRITE {holder_pid} {:02x}:{:02x}:{} 0 EOF\n",
            maj, min, ino
        ),
    )
    .unwrap();

    let trusted_exe = std::env::current_exe().unwrap();
    setup_mock_process(
        &proc_root,
        holder_pid,
        1,
        Some(&trusted_exe),
        Some(&lock_file),
        "100",
    );

    setup_mock_process(&proc_root, child_pid, holder_pid, None, None, "200");
    let environ_path = proc_root.join(child_pid.to_string()).join("environ");
    // Make environ unreadable by setting permissions to 0000
    fs::write(&environ_path, b"secret").unwrap();
    fs::set_permissions(&environ_path, fs::Permissions::from_mode(0o000)).unwrap();

    let config = AgyConfig {
        presence_dir: presence_dir.path().to_path_buf(),
        proc_locks_path: proc_locks,
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_exe],
    };

    let store = new_agy_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    storage::init_db(&conn).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let my_uid = xmsg::agent::current_uid();

    let res = scan_and_register_agy_proc(&config, &store, &db, my_uid).await;
    assert!(
        res.is_ok(),
        "unreadable environ must not produce a fatal error"
    );
    assert_eq!(res.unwrap(), 0, "no registration should occur");
    assert!(store.read().unwrap().is_empty(), "store must remain empty");
}

// ---------------------------------------------------------------------------
// Oracle 5:
// Hook stand-down:
//   A watching + first unregistered turn => hook prints {}
//   still unregistered at a later turn   => hook injects userMessage
//   A not watching                       => hook injects on first turn
// Mutants:
//   never stand down => RED (first-turn injection while A watches)
//   stand down forever => RED (no injection on the later turn)
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn oracle_5_hook_stand_down_mechanics() {
    let bin = env!("CARGO_BIN_EXE_xmsg");
    let sock_dir = tempdir().unwrap();
    fs::set_permissions(sock_dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let reg_sock = sock_dir.path().join("register.sock");

    let proc_dir = tempdir().unwrap();
    let proc_root = proc_dir.path().to_path_buf();
    let presence_dir = tempdir().unwrap();
    let proc_locks_dir = tempdir().unwrap();
    let proc_locks = proc_locks_dir.path().join("locks");

    let conv_watched = "conv-watched-standdown";
    let conv_unwatched = "conv-unwatched-immediate";

    // Setup conv_watched with presence lock and attested agy
    let lock_file = presence_dir.path().join(format!("{conv_watched}.lock"));
    fs::write(&lock_file, "").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    let maj = dev_major(meta.dev());
    let min = dev_minor(meta.dev());
    let ino = meta.ino();

    let holder_pid = 5000u32;
    fs::write(
        &proc_locks,
        format!(
            "1: FLOCK ADVISORY WRITE {holder_pid} {:02x}:{:02x}:{} 0 EOF\n",
            maj, min, ino
        ),
    )
    .unwrap();

    let trusted_exe = std::env::current_exe().unwrap();
    setup_mock_process(
        &proc_root,
        holder_pid,
        1,
        Some(&trusted_exe),
        Some(&lock_file),
        "100",
    );

    let config = AgyConfig {
        presence_dir: presence_dir.path().to_path_buf(),
        proc_locks_path: proc_locks,
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_exe],
    };

    let store = new_agy_store();
    let pi_store = xmsg::pi::new_pi_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    storage::init_db(&conn).unwrap();
    let db = Arc::new(Mutex::new(conn));
    let (pi_notify_tx, _) = broadcast::channel(16);
    let my_uid = xmsg::agent::current_uid();
    let reply_ttl = Duration::from_secs(604800);

    let s_path = reg_sock.clone();
    let s_cfg = config;
    let s_store = store.clone();
    let s_pi = pi_store;
    let s_db = db;
    let s_tx = pi_notify_tx;
    tokio::spawn(async move {
        let _ = run_register_server(
            s_path,
            s_cfg,
            s_store,
            s_pi,
            s_db,
            s_tx,
            reply_ttl,
            my_uid,
            xmsg::svc::new_svc_store(),
            std::collections::HashMap::new(),
            broadcast::channel(16).0,
            None,
        )
        .await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    // --- Part 1: conv_watched ---
    // Turn 1: A is watching and this is the first turn => hook prints {}
    let (code1, out1) = run_hook(bin, &reg_sock, conv_watched);
    assert_eq!(code1, 0);
    assert_eq!(
        out1,
        serde_json::json!({}),
        "turn 1 while watched must stand down and output {{}}"
    );

    // Turn 2: Still unregistered at later turn => hook injects userMessage
    let (code2, out2) = run_hook(bin, &reg_sock, conv_watched);
    assert_eq!(code2, 0);
    let steps = out2["injectSteps"]
        .as_array()
        .expect("injectSteps array must exist on turn 2");
    assert_eq!(steps.len(), 1, "must inject userMessage on turn 2");
    let msg = steps[0]["userMessage"].as_str().unwrap();
    assert!(
        msg.contains("register agy"),
        "message must ask model to run register agy: {msg}"
    );

    // --- Part 2: conv_unwatched ---
    // Turn 1: A is NOT watching (no presence lock held) => hook injects userMessage immediately
    let (code_unw, out_unw) = run_hook(bin, &reg_sock, conv_unwatched);
    assert_eq!(code_unw, 0);
    let steps_unw = out_unw["injectSteps"]
        .as_array()
        .expect("injectSteps array must exist on turn 1 for unwatched");
    assert_eq!(
        steps_unw.len(),
        1,
        "must inject userMessage immediately when not watched"
    );
    let msg_unw = steps_unw[0]["userMessage"].as_str().unwrap();
    assert!(
        msg_unw.contains("register agy"),
        "message must ask model to run register agy: {msg_unw}"
    );
}
