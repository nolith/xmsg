use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncReadExt};
use tokio::net::{TcpListener, UnixListener};
use tokio::sync::Mutex;

use xmsg::agy::{new_agy_store, AgyConfig, AgySessionInfo};
use xmsg::claim::{
    get_all_claims_for_id, is_uuid_shaped, sync_claims_db, validate_service_name,
};
use xmsg::http::{build_router, AppState};
use xmsg::pi::new_pi_store;
use xmsg::storage::init_db;

fn setup_mock_proc(proc_root: &Path, pid: u32, starttime: &str) {
    let pid_dir = proc_root.join(pid.to_string());
    fs::create_dir_all(&pid_dir).unwrap();
    let stat = format!(
        "{pid} (test) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 {starttime} 0 0 0 0 0 0 0 0 0 0\n"
    );
    fs::write(pid_dir.join("stat"), stat).unwrap();
}

fn remove_mock_proc(proc_root: &Path, pid: u32) {
    let _ = fs::remove_dir_all(proc_root.join(pid.to_string()));
}

fn write_claude_session_file(
    dir: &Path,
    pid: u32,
    session_id: &str,
    name: &str,
    proc_start: &str,
    sock_path: &Path,
) {
    let session_json = serde_json::json!({
        "pid": pid,
        "sessionId": session_id,
        "name": name,
        "cwd": "/workspace",
        "status": "idle",
        "kind": "interactive",
        "startedAt": 1000,
        "updatedAt": 2000,
        "procStart": proc_start,
        "messagingSocketPath": sock_path.display().to_string(),
    });
    fs::write(
        dir.join(format!("{pid}_{session_id}.json")),
        session_json.to_string(),
    )
    .unwrap();
}

fn remove_claude_session_file(dir: &Path, pid: u32, session_id: &str) {
    let _ = fs::remove_file(dir.join(format!("{pid}_{session_id}.json")));
}

struct SocketListener {
    _sock_dir: TempDir,
    sock_path: PathBuf,
    received_lines: Arc<Mutex<Vec<String>>>,
    stop_signal: Arc<AtomicBool>,
}

impl Drop for SocketListener {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::Relaxed);
    }
}

async fn start_socket_listener(name: &str) -> SocketListener {
    let sock_dir = tempdir().expect("tempdir for socket");
    let sock_path = sock_dir.path().join(format!("{name}.sock"));
    let listener = UnixListener::bind(&sock_path).expect("bind unix socket");
    let received_lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let received_clone = received_lines.clone();
    let stop_signal = Arc::new(AtomicBool::new(false));
    let stop_clone = stop_signal.clone();

    tokio::spawn(async move {
        while !stop_clone.load(Ordering::Relaxed) {
            tokio::select! {
                res = listener.accept() => {
                    if let Ok((mut stream, _)) = res {
                        let mut buf = Vec::new();
                        let mut temp = [0u8; 1024];
                        while let Ok(n) = stream.read(&mut temp).await {
                            if n == 0 { break; }
                            buf.extend_from_slice(&temp[..n]);
                            if buf.ends_with(b"\n") {
                                break;
                            }
                        }
                        if let Ok(s) = String::from_utf8(buf) {
                            received_clone.lock().await.push(s);
                        }
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
    });

    SocketListener {
        _sock_dir: sock_dir,
        sock_path,
        received_lines,
        stop_signal,
    }
}

struct RunningServer {
    base_url: String,
    agent_sock: PathBuf,
    agent_task: tokio::task::JoinHandle<()>,
    http_task: tokio::task::JoinHandle<()>,
}

impl RunningServer {
    async fn stop(self) {
        self.agent_task.abort();
        self.http_task.abort();
        let _ = self.agent_task.await;
        let _ = self.http_task.await;
        let _ = fs::remove_file(&self.agent_sock);
    }
}

async fn start_test_server(
    agent_sock: PathBuf,
    sessions_dir: PathBuf,
    proc_root: PathBuf,
    db_path: PathBuf,
) -> RunningServer {
    let my_uid = xmsg::agent::current_uid();
    let agy_config = AgyConfig {
        presence_dir: proc_root.join("presence"),
        proc_locks_path: proc_root.join("locks"),
        proc_root,
        agy_bin: "agy".to_string(),
        trusted_agy_exes: Vec::new(),
    };
    let agy_store = new_agy_store();
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();
    let db = Arc::new(std::sync::Mutex::new(conn));
    let (notify_tx, _) = tokio::sync::broadcast::channel(1024);
    let (pi_notify_tx, _) = tokio::sync::broadcast::channel(1024);
    let (svc_notify_tx, _) = tokio::sync::broadcast::channel(1024);

    let state = Arc::new(AppState {
        sessions_dirs: vec![sessions_dir],
        agy_config,
        agy_store,
        pi_store: new_pi_store(),
        pi_notify_tx,
        svc_store: xmsg::svc::new_svc_store(),
        svc_notify_tx,
        host_label: "test-node".to_string(),
        max_body: 65536,
        request_counter: AtomicU64::new(1),
        db: db.clone(),
        notify_tx,
        reply_ttl: Duration::from_secs(60),
        idempotency_ttl: Duration::from_secs(60),
        long_poll_semaphore: Arc::new(tokio::sync::Semaphore::new(4)),
        fed_state: None,
    });

    let router = build_router(state.clone());
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = http_listener.local_addr().unwrap().port();
    let base_url = format!("http://127.0.0.1:{port}");

    let http_task = tokio::spawn(async move {
        axum::serve(http_listener, router).await.unwrap();
    });

    let st = state.clone();
    let asock = agent_sock.clone();
    let agent_task = tokio::spawn(async move {
        let _ = xmsg::agent::run_agent_server(asock, st, my_uid).await;
    });

    for _ in 0..100 {
        if agent_sock.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    RunningServer {
        base_url,
        agent_sock,
        agent_task,
        http_task,
    }
}

// =============================================================================
// Oracle 1: Order and persistence across server restart (X and X~2)
// Mutant M1a: re-order by procStart on every scan => RED
// Mutant M1b: memory-only claim table (CREATE TEMP TABLE) => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_1_order_and_persistence_across_restart() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    let sock_dir = tmp.path().join("sockets");
    fs::create_dir_all(&sessions_dir).unwrap();
    fs::create_dir_all(&sock_dir).unwrap();

    let conv_id = "0199cecf-0001-7000-8000-000000000001";
    let sock1 = tmp.path().join("p1.sock");
    let sock2 = tmp.path().join("p2.sock");

    // P1 starts with procStart "200"
    setup_mock_proc(&proc_root, 1001, "200");
    write_claude_session_file(&sessions_dir, 1001, conv_id, "first", "200", &sock1);

    let agent_sock = sock_dir.join("agent.sock");
    let server1 = start_test_server(
        agent_sock.clone(),
        sessions_dir.clone(),
        proc_root.clone(),
        db_path.clone(),
    )
    .await;

    let client = reqwest::Client::new();

    // Scan 1: P1 is observed first, receives plain conv_id (suffix 1)
    let resp1 = client
        .get(format!("{}/v1/sessions", server1.base_url))
        .send()
        .await
        .unwrap();
    let s1: Vec<serde_json::Value> = resp1.json().await.unwrap();
    assert_eq!(s1.len(), 1);
    assert_eq!(s1[0]["sessionId"], conv_id);

    // Later: P2 arrives with an EARLIER procStart "100" presenting the same ID
    setup_mock_proc(&proc_root, 1002, "100");
    write_claude_session_file(&sessions_dir, 1002, conv_id, "second", "100", &sock2);

    // Scan 2: P1 must KEEP plain conv_id; P2 must receive X~2 despite starting earlier
    let resp2 = client
        .get(format!("{}/v1/sessions", server1.base_url))
        .send()
        .await
        .unwrap();
    let s2: Vec<serde_json::Value> = resp2.json().await.unwrap();
    assert_eq!(s2.len(), 2);
    let p1_entry = s2.iter().find(|s| s["pid"] == 1001).unwrap();
    let p2_entry = s2.iter().find(|s| s["pid"] == 1002).unwrap();
    assert_eq!(
        p1_entry["sessionId"], conv_id,
        "First observed process P1 MUST retain plain ID"
    );
    assert_eq!(
        p2_entry["sessionId"],
        format!("{conv_id}~2"),
        "Later observed process P2 MUST receive ~2 despite earlier procStart"
    );

    // Stop Server 1 and restart as Server 2 against the same DB file
    server1.stop().await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let server2 = start_test_server(
        agent_sock.clone(),
        sessions_dir.clone(),
        proc_root.clone(),
        db_path.clone(),
    )
    .await;

    // Scan on restarted server: persistence preserves claims
    let resp3 = client
        .get(format!("{}/v1/sessions", server2.base_url))
        .send()
        .await
        .unwrap();
    let s3: Vec<serde_json::Value> = resp3.json().await.unwrap();
    assert_eq!(s3.len(), 2);
    let p1_after = s3.iter().find(|s| s["pid"] == 1001).unwrap();
    let p2_after = s3.iter().find(|s| s["pid"] == 1002).unwrap();
    assert_eq!(
        p1_after["sessionId"], conv_id,
        "Persistence across restart MUST preserve plain ID for P1"
    );
    assert_eq!(
        p2_after["sessionId"],
        format!("{conv_id}~2"),
        "Persistence across restart MUST preserve ~2 for P2"
    );

    server2.stop().await;
}

// =============================================================================
// Oracle 2: Release on session switch (<pid>.json stops naming ID)
// Mutant M2: session switch keeps claim active => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_2_release_on_session_switch() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let id_x = "0199cecf-0002-7000-8000-000000000001";
    let id_y = "0199cecf-0002-7000-8000-000000000002";
    let sock1 = tmp.path().join("p1.sock");
    let sock2 = tmp.path().join("p2.sock");

    setup_mock_proc(&proc_root, 1001, "100");
    write_claude_session_file(&sessions_dir, 1001, id_x, "p1", "100", &sock1);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();

    // 1. Initial sync: pid 1001 gets plain id_x
    let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs.len(), 1);
    assert_eq!(pairs[0].0.address(), id_x);
    assert_eq!(pairs[0].0.suffix, 1);

    // 2. Session switch: remove id_x file and write id_y file for pid 1001
    remove_claude_session_file(&sessions_dir, 1001, id_x);
    write_claude_session_file(&sessions_dir, 1001, id_y, "p1-switched", "100", &sock1);

    let (pairs2, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs2.len(), 1);
    assert_eq!(pairs2[0].0.id, id_y);
    assert_eq!(pairs2[0].0.suffix, 1);

    // Verify in DB that claim on id_x is marked released
    let all_x_claims = get_all_claims_for_id(&conn, id_x).unwrap();
    assert_eq!(all_x_claims.len(), 1);
    assert!(
        all_x_claims[0].released,
        "id_x claim MUST be released on session switch"
    );

    // 3. Newly observed process pid 1002 starts presenting id_x
    setup_mock_proc(&proc_root, 1002, "200");
    write_claude_session_file(&sessions_dir, 1002, id_x, "p2", "200", &sock2);

    let (pairs3, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    let p2_claim = pairs3
        .iter()
        .find(|(c, _)| c.pid == 1002)
        .unwrap()
        .0
        .clone();
    assert_eq!(
        p2_claim.suffix, 1,
        "Newly observed process MUST receive released plain ID (suffix 1)"
    );
    assert_eq!(p2_claim.address(), id_x);
}

// =============================================================================
// Oracle 3: No promotion while live, no reuse of suffixes while records exist
// Mutant M3a: promote ~2 when plain is free => RED
// Mutant M3b: reuse the lowest free suffix => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_3_no_promotion_while_live_no_reuse_of_suffixes() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let id = "0199cecf-0003-7000-8000-000000000001";
    let sock1 = tmp.path().join("p1.sock");
    let sock2 = tmp.path().join("p2.sock");
    let sock3 = tmp.path().join("p3.sock");
    let sock4 = tmp.path().join("p4.sock");
    let sock5 = tmp.path().join("p5.sock");

    setup_mock_proc(&proc_root, 1001, "100");
    setup_mock_proc(&proc_root, 1002, "200");
    setup_mock_proc(&proc_root, 1003, "300");
    write_claude_session_file(&sessions_dir, 1001, id, "p1", "100", &sock1);
    write_claude_session_file(&sessions_dir, 1002, id, "p2", "200", &sock2);
    write_claude_session_file(&sessions_dir, 1003, id, "p3", "300", &sock3);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();

    // 1. Initial sync: P1 has suffix 1, P2 has suffix 2, P3 has suffix 3
    let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs.len(), 3);
    assert_eq!(
        pairs.iter().find(|(c, _)| c.pid == 1001).unwrap().0.suffix,
        1
    );
    assert_eq!(
        pairs.iter().find(|(c, _)| c.pid == 1002).unwrap().0.suffix,
        2
    );
    assert_eq!(
        pairs.iter().find(|(c, _)| c.pid == 1003).unwrap().0.suffix,
        3
    );

    // 2. P2 exits first, freeing suffix ~2 below the maximum suffix 3
    remove_mock_proc(&proc_root, 1002);
    remove_claude_session_file(&sessions_dir, 1002, id);
    let (pairs2, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs2.len(), 2);

    // 3. P1 exits, freeing plain ID
    remove_mock_proc(&proc_root, 1001);
    remove_claude_session_file(&sessions_dir, 1001, id);
    let (pairs3, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs3.len(), 1);
    let p3_claim = &pairs3[0].0;
    assert_eq!(p3_claim.pid, 1003);
    assert_eq!(
        p3_claim.suffix, 3,
        "P3 MUST NEVER be promoted to suffix 1 while live"
    );
    assert_eq!(p3_claim.address(), format!("{id}~3"));

    // 4. P4 arrives presenting ID: plain suffix 1 is free, so P4 gets suffix 1
    setup_mock_proc(&proc_root, 1004, "400");
    write_claude_session_file(&sessions_dir, 1004, id, "p4", "400", &sock4);

    let (pairs4, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    let p4_claim = pairs4
        .iter()
        .find(|(c, _)| c.pid == 1004)
        .unwrap()
        .0
        .clone();
    assert_eq!(p4_claim.suffix, 1, "P4 takes free plain ID (suffix 1)");

    // 5. P5 arrives presenting ID: plain is held by P4, suffix 2 was released, suffix 3 is held by P3.
    // Production MUST assign suffix 4 (max_suffix + 1), and MUST NOT reuse freed suffix 2.
    setup_mock_proc(&proc_root, 1005, "500");
    write_claude_session_file(&sessions_dir, 1005, id, "p5", "500", &sock5);

    let (pairs5, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    let p5_claim = pairs5
        .iter()
        .find(|(c, _)| c.pid == 1005)
        .unwrap()
        .0
        .clone();
    assert_eq!(
        p5_claim.suffix, 4,
        "P5 MUST get monotonic suffix 4 (max_suffix + 1), NOT reused suffix 2"
    );
    assert_ne!(
        p5_claim.suffix, 2,
        "Released suffix 2 MUST NOT be reused while record exists"
    );
    assert_eq!(p5_claim.address(), format!("{id}~4"));
}

// =============================================================================
// Oracle 4: Ties broken by (procStart, pid)
// Mutant M4: take read_dir order (no sort) => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_4_ties_broken_by_proc_start_and_pid() {
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path().join("proc");
    let id = "0199cecf-0004-7000-8000-000000000001";
    let sock = tmp.path().join("sock");

    setup_mock_proc(&proc_root, 2000, "200");
    setup_mock_proc(&proc_root, 1000, "100");

    let mut inverted_count = 0;

    // Build in both creation orders, ensuring at least one fixture inverts (procStart, pid) in read_dir
    let mut attempt = 0;
    let (sd1, read_pids1) = loop {
        attempt += 1;
        let sd = tmp.path().join(format!("sessions_1_{attempt}"));
        fs::create_dir_all(&sd).unwrap();
        write_claude_session_file(&sd, 2000, id, "p2000", "200", &sock);
        write_claude_session_file(&sd, 1000, id, "p1000", "100", &sock);

        let read_pids: Vec<u32> = fs::read_dir(&sd)
            .unwrap()
            .map(|e| {
                let fname = e.unwrap().file_name().to_string_lossy().to_string();
                fname.split('_').next().unwrap().parse::<u32>().unwrap()
            })
            .collect();

        if read_pids == vec![2000, 1000] || attempt >= 50 {
            break (sd, read_pids);
        }
    };

    let (sd2, _read_pids2) = {
        let sd = tmp.path().join("sessions_2");
        fs::create_dir_all(&sd).unwrap();
        write_claude_session_file(&sd, 1000, id, "p1000", "100", &sock);
        write_claude_session_file(&sd, 2000, id, "p2000", "200", &sock);
        let r: Vec<u32> = fs::read_dir(&sd)
            .unwrap()
            .map(|e| {
                let fname = e.unwrap().file_name().to_string_lossy().to_string();
                fname.split('_').next().unwrap().parse::<u32>().unwrap()
            })
            .collect();
        (sd, r)
    };

    if read_pids1 == vec![2000, 1000] {
        inverted_count += 1;
    }

    for (order, sd) in [(1, sd1), (2, sd2)] {
        let db_path = tmp.path().join(format!("xmsg_{order}.db"));
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        init_db(&conn).unwrap();

        let (pairs, _) = sync_claims_db(&conn, &proc_root, &sd, "test-node").unwrap();
        assert_eq!(pairs.len(), 2);
        let p1000_claim = pairs.iter().find(|(c, _)| c.pid == 1000).unwrap().0.clone();
        let p2000_claim = pairs.iter().find(|(c, _)| c.pid == 2000).unwrap().0.clone();

        assert_eq!(
            p1000_claim.suffix, 1,
            "1000 has smaller (procStart, pid) -> MUST win suffix 1 regardless of creation or read_dir order"
        );
        assert_eq!(p2000_claim.suffix, 2);
    }

    assert!(
        inverted_count >= 1,
        "read_dir order MUST invert (procStart, pid) in at least one fixture to validate tie rule"
    );
}

// =============================================================================
// Oracle 5: Fork identity in send/reply attribution and authorization (D-5)
// Mutant M5: caller identity from session file's sessionId => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_5_fork_identity_attribution_and_routing() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let conv_id = "0199cecf-0005-7000-8000-000000000001";
    let listener_p1 = start_socket_listener("p1").await;
    let listener_p2 = start_socket_listener("p2").await;

    setup_mock_proc(&proc_root, 1001, "100");
    setup_mock_proc(&proc_root, 1002, "200");
    write_claude_session_file(
        &sessions_dir,
        1001,
        conv_id,
        "first",
        "100",
        &listener_p1.sock_path,
    );
    write_claude_session_file(
        &sessions_dir,
        1002,
        conv_id,
        "second",
        "200",
        &listener_p2.sock_path,
    );

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();

    // 1. Initial observation: P1 gets plain X, P2 gets X~2
    let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs.len(), 2);

    let agy_config = AgyConfig {
        presence_dir: proc_root.join("presence"),
        proc_locks_path: proc_root.join("locks"),
        proc_root: proc_root.clone(),
        agy_bin: "agy".to_string(),
        trusted_agy_exes: Vec::new(),
    };
    let agy_store = new_agy_store();
    let pi_store = new_pi_store();
    let svc_store = xmsg::svc::new_svc_store();

    // 2. Caller resolution for P2 (pid 1002)
    let caller_p2 = xmsg::agent::resolve_caller_session(
        &proc_root,
        &[sessions_dir.clone()][..],
        &agy_config,
        &agy_store,
        &pi_store,
        &svc_store,
        Some(&conn),
        1002,
    )
    .expect("resolve caller P2");

    assert_eq!(
        caller_p2.session_id,
        format!("{conv_id}~2"),
        "Caller identity for fork process MUST resolve to X~2 via claim table"
    );

    // 3. From-attribution: message sent by P2 is attributed to X~2, NOT X
    let badge = xmsg::inbox::sanitize_attested_from(
        "test-node",
        &caller_p2.harness,
        &caller_p2.session_id,
    );
    assert_eq!(
        badge,
        format!("xmsg@test-node · claude:{conv_id}~2"),
        "Sender badge MUST attribute to X~2"
    );
    assert_ne!(
        badge,
        format!("xmsg@test-node · claude:{conv_id}"),
        "Sender badge MUST NOT attribute to plain X"
    );

    // 4. Reply authorization: P2 may reply to a message addressed to X~2, and is denied for one addressed to X
    let msg_for_plain = xmsg::storage::MessageRecord {
        id: "msg-plain-001".to_string(),
        created_at: xmsg::storage::now_epoch_secs(),
        session_id: conv_id.to_string(),
        from_name: "test@peer".to_string(),
        bytes: 10,
        outcome: "delivered".to_string(),
        recipient_harness: "claude".to_string(),
        return_harness: None,
        return_session_id: None,
        push_replies: false,
        thread_id: "t1".to_string(),
        return_host: None,
    };
    xmsg::storage::insert_message(&conn, &msg_for_plain).unwrap();

    let msg_for_fork = xmsg::storage::MessageRecord {
        id: "msg-fork-002".to_string(),
        created_at: xmsg::storage::now_epoch_secs(),
        session_id: format!("{conv_id}~2"),
        from_name: "test@peer".to_string(),
        bytes: 10,
        outcome: "delivered".to_string(),
        recipient_harness: "claude".to_string(),
        return_harness: None,
        return_session_id: None,
        push_replies: false,
        thread_id: "t2".to_string(),
        return_host: None,
    };
    xmsg::storage::insert_message(&conn, &msg_for_fork).unwrap();

    // Check authorization against msg_for_plain: caller is X~2, recipient is X -> denied
    assert_ne!(
        caller_p2.session_id, msg_for_plain.session_id,
        "P2 must NOT match recipient of message addressed to plain X"
    );

    // Check authorization against msg_for_fork: caller is X~2, recipient is X~2 -> allowed
    assert_eq!(
        caller_p2.session_id, msg_for_fork.session_id,
        "P2 MUST match recipient of message addressed to X~2"
    );
}

// =============================================================================
// Oracle 6: Fork notice exact text and single delivery
// Mutant M6: notify only the newcomer => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_6_fork_notice_exact_text_and_single_delivery() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let conv_id = "0199cecf-0006-7000-8000-000000000001";
    let listener_p1 = start_socket_listener("p1").await;
    let listener_p2 = start_socket_listener("p2").await;

    setup_mock_proc(&proc_root, 1001, "100");
    write_claude_session_file(
        &sessions_dir,
        1001,
        conv_id,
        "first",
        "100",
        &listener_p1.sock_path,
    );

    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open(&db_path).unwrap(),
    ));
    {
        let c = conn.lock().unwrap();
        init_db(&c).unwrap();
    }

    // First process starts: no fork notice
    let (_, notices) = sync_claims_db(&conn.lock().unwrap(), &proc_root, &sessions_dir, "test-node").unwrap();
    xmsg::claim::deliver_fork_notices(&conn, notices).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(listener_p1.received_lines.lock().await.len(), 0);

    // Second process starts for same conversation ID
    setup_mock_proc(&proc_root, 1002, "200");
    write_claude_session_file(
        &sessions_dir,
        1002,
        conv_id,
        "second",
        "200",
        &listener_p2.sock_path,
    );

    let (_, notices) = sync_claims_db(&conn.lock().unwrap(), &proc_root, &sessions_dir, "test-node").unwrap();
    xmsg::claim::deliver_fork_notices(&conn, notices).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify exact notice text received by P1
    let lines_p1 = listener_p1.received_lines.lock().await.clone();
    assert_eq!(lines_p1.len(), 1, "P1 must receive exactly one notice");
    let expected_p1_text = format!(
        "xmsg: conversation {} is open in two processes on test-node. This process is {}; the other is {}~2. A send to {} reaches {}.",
        conv_id, conv_id, conv_id, conv_id, conv_id
    );
    assert!(
        lines_p1[0].contains(&expected_p1_text),
        "P1 notice mismatch: expected text '{}', got: '{}'",
        expected_p1_text,
        lines_p1[0]
    );

    // Verify exact notice text received by P2
    let lines_p2 = listener_p2.received_lines.lock().await.clone();
    assert_eq!(lines_p2.len(), 1, "P2 must receive exactly one notice");
    let expected_p2_text = format!(
        "xmsg: conversation {} is open in two processes on test-node. This process is {}~2; the other is {}. A send to {} reaches {}.",
        conv_id, conv_id, conv_id, conv_id, conv_id
    );
    assert!(
        lines_p2[0].contains(&expected_p2_text),
        "P2 notice mismatch: expected text '{}', got: '{}'",
        expected_p2_text,
        lines_p2[0]
    );

    // Subsequent sync: NO duplicate notice delivered
    let (_, notices) = sync_claims_db(&conn.lock().unwrap(), &proc_root, &sessions_dir, "test-node").unwrap();
    xmsg::claim::deliver_fork_notices(&conn, notices).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        listener_p1.received_lines.lock().await.len(),
        1,
        "No duplicate notice to P1"
    );
    assert_eq!(
        listener_p2.received_lines.lock().await.len(),
        1,
        "No duplicate notice to P2"
    );
}

// =============================================================================
// Oracle 7: agy conversation UUID across restart (PM1 case and caller resolution)
// Mutant M7a: key on agy:{pid}:{starttime} in AgySessionInfo::new => RED
// Mutant M7b: key on agy:{pid}:{starttime} at registration sites => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_7_agy_conversation_uuid_across_restart() {
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path().join("proc");
    let presence_dir = tmp.path().join("presence");
    let proc_locks = tmp.path().join("proc_locks");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&presence_dir).unwrap();
    fs::create_dir_all(&sessions_dir).unwrap();

    let conv_uuid = "4a111111-2222-4333-8444-555555555555";
    let lock_file = presence_dir.join(format!("{conv_uuid}.lock"));
    fs::write(&lock_file, "lock").unwrap();
    let meta = fs::metadata(&lock_file).unwrap();
    use std::os::unix::fs::MetadataExt;
    let dev = meta.dev();
    let ino = meta.ino();
    let maj = xmsg::agy::dev_major(dev);
    let min = xmsg::agy::dev_minor(dev);

    let trusted_agy = tmp.path().join("trusted_agy");
    fs::write(&trusted_agy, "binary").unwrap();

    let agy_config = AgyConfig {
        presence_dir: presence_dir.clone(),
        proc_locks_path: proc_locks.clone(),
        proc_root: proc_root.clone(),
        agy_bin: "agy".to_string(),
        trusted_agy_exes: vec![trusted_agy.clone()],
    };
    let agy_store = new_agy_store();
    let pi_store = new_pi_store();
    let svc_store = xmsg::svc::new_svc_store();
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    init_db(&conn).unwrap();

    // 1. Initial agy process (pid 7777, start "100")
    let locks_content_1 = format!(
        "1: FLOCK ADVISORY WRITE 7777 {:02x}:{:02x}:{} 0 EOF\n",
        maj, min, ino
    );
    fs::write(&proc_locks, &locks_content_1).unwrap();
    setup_mock_proc(&proc_root, 7777, "100");
    let pid7777_dir = proc_root.join("7777");
    let _ = fs::create_dir_all(pid7777_dir.join("fd"));
    let _ = std::os::unix::fs::symlink(&trusted_agy, pid7777_dir.join("exe"));
    let _ = std::os::unix::fs::symlink(&lock_file, pid7777_dir.join("fd").join("3"));

    let my_uid = xmsg::agent::current_uid();
    let reg1 = xmsg::agy::attest_mcp_caller(
        &proc_root,
        &proc_locks,
        &presence_dir,
        &[trusted_agy.clone()],
        my_uid,
        my_uid,
        7777,
    )
    .expect("attest 7777");

    assert_eq!(reg1.1, conv_uuid);
    let session_key = reg1.1.clone();
    assert_eq!(session_key, conv_uuid, "Session key MUST be conversation UUID");

    let session_info = AgySessionInfo::new(conv_uuid.to_string(), 7777, "100".to_string(), None);
    assert_eq!(session_info.session_key, conv_uuid);
    assert_eq!(session_info.conversation_id, conv_uuid);
    assert!(is_uuid_shaped(&session_info.session_key));

    agy_store
        .write()
        .unwrap()
        .insert(session_key.clone(), session_info.clone());

    // Initial caller resolution for 7777
    let caller1 = xmsg::agent::resolve_caller_session(
        &proc_root,
        &[sessions_dir.clone()][..],
        &agy_config,
        &agy_store,
        &pi_store,
        &svc_store,
        Some(&conn),
        7777,
    )
    .expect("resolve caller 7777");
    assert_eq!(caller1.session_id, conv_uuid);

    // A message is received addressed to conversation UUID
    let msg = xmsg::storage::MessageRecord {
        id: "msg-before-restart-1".to_string(),
        created_at: xmsg::storage::now_epoch_secs(),
        session_id: conv_uuid.to_string(),
        from_name: "orchestrator".to_string(),
        bytes: 12,
        outcome: "queued".to_string(),
        recipient_harness: "agy".to_string(),
        return_harness: None,
        return_session_id: None,
        push_replies: false,
        thread_id: "t1".to_string(),
        return_host: None,
    };
    xmsg::storage::insert_message(&conn, &msg).unwrap();

    // 2. Restart agy with NEW PID 8888, start "300", resuming SAME conversation UUID
    remove_mock_proc(&proc_root, 7777);

    let locks_content_2 = format!(
        "1: FLOCK ADVISORY WRITE 8888 {:02x}:{:02x}:{} 0 EOF\n",
        maj, min, ino
    );
    fs::write(&proc_locks, &locks_content_2).unwrap();
    setup_mock_proc(&proc_root, 8888, "300");
    let pid8888_dir = proc_root.join("8888");
    let _ = fs::create_dir_all(pid8888_dir.join("fd"));
    let _ = std::os::unix::fs::symlink(&trusted_agy, pid8888_dir.join("exe"));
    let _ = std::os::unix::fs::symlink(&lock_file, pid8888_dir.join("fd").join("3"));

    let reg2 = xmsg::agy::attest_mcp_caller(
        &proc_root,
        &proc_locks,
        &presence_dir,
        &[trusted_agy.clone()],
        my_uid,
        my_uid,
        8888,
    )
    .expect("attest 8888");

    assert_eq!(reg2.1, conv_uuid);

    // Emulate mcp_start store update:
    {
        let mut store_lock = agy_store.write().unwrap();
        store_lock
            .entry(reg2.1.clone())
            .and_modify(|current| {
                if current.pid != 8888 || current.starttime != "300" {
                    *current = AgySessionInfo::new(reg2.1.clone(), 8888, "300".to_string(), None);
                }
            })
            .or_insert_with(|| AgySessionInfo::new(reg2.1.clone(), 8888, "300".to_string(), None));
    }

    // 3. Restarted agy resolves as caller under conversation UUID
    let caller2 = xmsg::agent::resolve_caller_session(
        &proc_root,
        &[sessions_dir.clone()][..],
        &agy_config,
        &agy_store,
        &pi_store,
        &svc_store,
        Some(&conn),
        8888,
    )
    .expect("resolve restarted caller 8888");

    assert_eq!(
        caller2.session_id, conv_uuid,
        "Restarted agy must resolve caller session_id to conversation UUID"
    );

    // 4. Restarted agy replies to the message received before restart:
    assert_eq!(
        caller2.session_id, msg.session_id,
        "Restarted agy must match recipient of message received before restart"
    );
    assert_eq!(caller2.harness, msg.recipient_harness);
}

// =============================================================================
// Oracle 8: pi server-minted UUID v4
// Mutant M8: honour the asserted pi ID => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_8_pi_server_minted_uuid_v4() {
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path().join("proc");
    fs::create_dir_all(&proc_root).unwrap();

    let pi_store = new_pi_store();
    let db = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    init_db(&db.lock().unwrap()).unwrap();
    let (pi_notify_tx, _) = tokio::sync::broadcast::channel(16);

    let my_uid = xmsg::agent::current_uid();
    setup_mock_proc(&proc_root, 5555, "100");

    let req = xmsg::pi::PiRegisterRequest {
        session_id: Some("asserted-custom-pi-id-z".to_string()),
        session_name: Some("pi-worker".to_string()),
        cwd: Some("/workspace".to_string()),
    };

    let (client_read, server_write) = tokio::io::duplex(1024);
    let (server_read, _client_write) = tokio::io::duplex(1024);
    let reader = tokio::io::BufReader::new(server_read);

    let p_root = proc_root.clone();
    let p_store = pi_store.clone();
    let p_db = db.clone();
    let p_tx = pi_notify_tx.clone();
    tokio::spawn(async move {
        let _ = xmsg::pi::handle_pi_connection(
            reader,
            server_write,
            p_root,
            p_store,
            my_uid,
            my_uid,
            5555,
            req,
            p_db,
            p_tx,
            Duration::from_secs(60),
        )
        .await;
    });

    let mut resp_line = String::new();
    let mut buf_client = tokio::io::BufReader::new(client_read);
    buf_client.read_line(&mut resp_line).await.unwrap();

    let v: serde_json::Value = serde_json::from_str(&resp_line).expect("valid JSON response");
    assert_eq!(v["status"], "ok");
    let minted_id = v["sessionId"].as_str().expect("sessionId in response");

    // Must NOT honour the asserted custom ID
    assert_ne!(
        minted_id, "asserted-custom-pi-id-z",
        "Server MUST NOT honour asserted pi session ID"
    );
    assert!(
        is_uuid_shaped(minted_id),
        "Minted session ID must be UUID-shaped: {minted_id}"
    );

    // Must be UUID v4
    assert_eq!(minted_id.chars().nth(14).unwrap(), '4');
    let var = minted_id.chars().nth(19).unwrap();
    assert!(matches!(var, '8' | '9' | 'a' | 'b'));

    // Second registration on a new process must mint a DIFFERENT UUID
    setup_mock_proc(&proc_root, 6666, "200");
    let req2 = xmsg::pi::PiRegisterRequest {
        session_id: Some("asserted-custom-pi-id-z".to_string()),
        session_name: Some("pi-worker".to_string()),
        cwd: Some("/workspace".to_string()),
    };
    let (client_read2, server_write2) = tokio::io::duplex(1024);
    let (server_read2, _client_write2) = tokio::io::duplex(1024);
    let reader2 = tokio::io::BufReader::new(server_read2);

    let p_root2 = proc_root.clone();
    let p_store2 = pi_store.clone();
    let p_db2 = db.clone();
    let p_tx2 = pi_notify_tx.clone();
    tokio::spawn(async move {
        let _ = xmsg::pi::handle_pi_connection(
            reader2,
            server_write2,
            p_root2,
            p_store2,
            my_uid,
            my_uid,
            6666,
            req2,
            p_db2,
            p_tx2,
            Duration::from_secs(60),
        )
        .await;
    });

    let mut resp_line2 = String::new();
    let mut buf_client2 = tokio::io::BufReader::new(client_read2);
    buf_client2.read_line(&mut resp_line2).await.unwrap();

    let v2: serde_json::Value = serde_json::from_str(&resp_line2).expect("valid JSON response");
    assert_eq!(v2["status"], "ok");
    let minted_id2 = v2["sessionId"].as_str().expect("sessionId in response");
    assert_ne!(minted_id2, "asserted-custom-pi-id-z");
    assert_ne!(
        minted_id2, minted_id,
        "Subsequent registration must mint a fresh UUID v4"
    );
}

// =============================================================================
// Oracle 9: Service name validation refusals
// Mutant M9a-site: drop the check at registration => RED
// Mutant M9a-fn: drop the char and UUID checks => RED
// Mutant M9b: drop the xmsg reservation => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_9_service_name_validation_refusals() {
    // 1. Direct validation tests for validate_service_name (catches M9a-fn and M9b)
    assert!(
        validate_service_name("xmsg").is_err(),
        "Name 'xmsg' must be refused"
    );
    assert!(
        validate_service_name("worker@host").is_err(),
        "Name with '@' must be refused"
    );
    assert!(
        validate_service_name("worker·daemon").is_err(),
        "Name with '·' must be refused"
    );
    assert!(
        validate_service_name("worker~2").is_err(),
        "Name with '~' must be refused"
    );
    assert!(
        validate_service_name("worker.daemon").is_err(),
        "Name with '.' must be refused"
    );
    assert!(
        validate_service_name("550e8400-e29b-41d4-a716-446655440000").is_err(),
        "UUID-shaped service name must be refused"
    );
    assert!(validate_service_name("guard").is_ok());
    assert!(validate_service_name("dispatcher").is_ok());

    // 2. Registration site validation via handle_svc_connection (catches M9a-site)
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path().join("proc");
    fs::create_dir_all(&proc_root).unwrap();
    let svc_store = xmsg::svc::new_svc_store();
    let db = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    init_db(&db.lock().unwrap()).unwrap();
    let (svc_notify_tx, _) = tokio::sync::broadcast::channel(16);
    let my_uid = xmsg::agent::current_uid();
    setup_mock_proc(&proc_root, 9991, "100");

    let bad_names = [
        "xmsg",
        "worker@host",
        "550e8400-e29b-41d4-a716-446655440000",
        "worker.dot",
        "worker~tilde",
    ];

    for bad_name in bad_names {
        let (client_read, server_write) = tokio::io::duplex(1024);
        let (server_read, _client_write) = tokio::io::duplex(1024);
        let reader = tokio::io::BufReader::new(server_read);

        let req = xmsg::svc::SvcRegisterRequest {
            name: bad_name.to_string(),
            cwd: None,
        };

        let trusted = std::collections::HashMap::new();
        let res = xmsg::svc::handle_svc_connection(
            reader,
            server_write,
            proc_root.clone(),
            svc_store.clone(),
            &trusted,
            my_uid,
            my_uid,
            9991,
            req,
            db.clone(),
            svc_notify_tx.clone(),
            Duration::from_secs(60),
            false,
            None,
            None,
        )
        .await;

        assert!(
            res.is_err(),
            "Registration for invalid service name '{bad_name}' MUST fail"
        );

        let mut resp_line = String::new();
        let mut buf_client = tokio::io::BufReader::new(client_read);
        buf_client.read_line(&mut resp_line).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(&resp_line).expect("valid JSON response");
        assert_eq!(
            v["status"], "error",
            "Registration response for '{bad_name}' must be error"
        );
    }
}
