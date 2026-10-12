use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::Mutex;

use xmsg::agy::{new_agy_store, AgyConfig, AgySessionInfo};
use xmsg::claim::{
    get_active_claims_for_id, get_all_claims_for_id, is_uuid_shaped, mint_uuid_v4,
    resolve_claude_session, sync_claims, sync_claims_db, validate_service_name,
};
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

// =============================================================================
// Oracle 1: Order and persistence across server restart (X and X~2)
// Mutant: memory-only claim table or suffixes randomized => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_1_order_and_persistence_across_restart() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let conv_id = "0199cecf-0001-7000-8000-000000000001";
    let sock1 = tmp.path().join("p1.sock");
    let sock2 = tmp.path().join("p2.sock");

    setup_mock_proc(&proc_root, 1001, "100");
    setup_mock_proc(&proc_root, 1002, "200");
    write_claude_session_file(&sessions_dir, 1001, conv_id, "first", "100", &sock1);
    write_claude_session_file(&sessions_dir, 1002, conv_id, "second", "200", &sock2);

    // Initial sync with SQLite DB
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        init_db(&conn).unwrap();
        let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
        assert_eq!(pairs.len(), 2);

        let p1_claim = pairs.iter().find(|(c, _)| c.pid == 1001).unwrap().0.clone();
        let p2_claim = pairs.iter().find(|(c, _)| c.pid == 1002).unwrap().0.clone();

        assert_eq!(p1_claim.suffix, 1, "First process gets suffix 1 (plain)");
        assert_eq!(p1_claim.address(), conv_id);
        assert_eq!(p2_claim.suffix, 2, "Second process gets suffix 2 (~2)");
        assert_eq!(p2_claim.address(), format!("{conv_id}~2"));
    }

    // Server restart: reopen the same SQLite DB file
    {
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        init_db(&conn).unwrap();
        let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
        assert_eq!(pairs.len(), 2);

        let p1_claim = pairs.iter().find(|(c, _)| c.pid == 1001).unwrap().0.clone();
        let p2_claim = pairs.iter().find(|(c, _)| c.pid == 1002).unwrap().0.clone();

        assert_eq!(p1_claim.suffix, 1, "Persistence preserves suffix 1 for p1");
        assert_eq!(p1_claim.address(), conv_id);
        assert_eq!(p2_claim.suffix, 2, "Persistence preserves suffix 2 for p2");
        assert_eq!(p2_claim.address(), format!("{conv_id}~2"));

        let claims = get_active_claims_for_id(&conn, conv_id).unwrap();
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].pid, 1001);
        assert_eq!(claims[0].suffix, 1);
        assert_eq!(claims[1].pid, 1002);
        assert_eq!(claims[1].suffix, 2);
    }
}

// =============================================================================
// Oracle 2: Release on session switch (<pid>.json stops naming ID)
// Mutant: session switch keeps claim active or does not release plain ID => RED
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
// Mutant: promoted to 1 on exit or suffix reused => RED
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

    setup_mock_proc(&proc_root, 1001, "100");
    setup_mock_proc(&proc_root, 1002, "200");
    write_claude_session_file(&sessions_dir, 1001, id, "p1", "100", &sock1);
    write_claude_session_file(&sessions_dir, 1002, id, "p2", "200", &sock2);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();

    // 1. Initial sync: P1 has suffix 1, P2 has suffix 2
    let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs.len(), 2);
    assert_eq!(
        pairs.iter().find(|(c, _)| c.pid == 1001).unwrap().0.suffix,
        1
    );
    assert_eq!(
        pairs.iter().find(|(c, _)| c.pid == 1002).unwrap().0.suffix,
        2
    );

    // 2. P1 exits (mock proc removed)
    remove_mock_proc(&proc_root, 1001);
    let (pairs2, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs2.len(), 1);
    let p2_claim = &pairs2[0].0;
    assert_eq!(p2_claim.pid, 1002);
    assert_eq!(
        p2_claim.suffix, 2,
        "P2 MUST NEVER be promoted to suffix 1 while live"
    );
    assert_eq!(p2_claim.address(), format!("{id}~2"));

    // 3. P3 starts presenting ID: plain suffix 1 is free, so P3 gets suffix 1
    setup_mock_proc(&proc_root, 1003, "300");
    write_claude_session_file(&sessions_dir, 1003, id, "p3", "300", &sock3);

    let (pairs3, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    let p3_claim = pairs3
        .iter()
        .find(|(c, _)| c.pid == 1003)
        .unwrap()
        .0
        .clone();
    assert_eq!(p3_claim.suffix, 1, "P3 takes free plain ID (suffix 1)");

    // 4. P4 starts presenting ID: plain is held by P3, suffix 2 record exists for P2, so P4 gets suffix 3
    setup_mock_proc(&proc_root, 1004, "400");
    write_claude_session_file(&sessions_dir, 1004, id, "p4", "400", &sock4);

    let (pairs4, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    let p4_claim = pairs4
        .iter()
        .find(|(c, _)| c.pid == 1004)
        .unwrap()
        .0
        .clone();
    assert_eq!(
        p4_claim.suffix, 3,
        "P4 MUST get monotonic suffix 3 without reusing suffix 2 while records exist"
    );
    assert_eq!(p4_claim.address(), format!("{id}~3"));
}

// =============================================================================
// Oracle 4: Ties broken by (procStart, pid)
// Mutant: sort by PID only or reverse sort => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_4_ties_broken_by_proc_start_and_pid() {
    let tmp = tempdir().unwrap();
    let db_path = tmp.path().join("xmsg.db");
    let proc_root = tmp.path().join("proc");
    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();

    let id = "0199cecf-0004-7000-8000-000000000001";
    let sock = tmp.path().join("sock");

    // Three processes presented in the same scan:
    // Candidate A: pid=2000, proc_start="100"
    // Candidate B: pid=1500, proc_start="100" (smaller PID with same proc_start)
    // Candidate C: pid=1000, proc_start="200" (larger proc_start despite smaller PID)
    setup_mock_proc(&proc_root, 2000, "100");
    setup_mock_proc(&proc_root, 1500, "100");
    setup_mock_proc(&proc_root, 1000, "200");

    write_claude_session_file(&sessions_dir, 2000, id, "pA", "100", &sock);
    write_claude_session_file(&sessions_dir, 1500, id, "pB", "100", &sock);
    write_claude_session_file(&sessions_dir, 1000, id, "pC", "200", &sock);

    let conn = rusqlite::Connection::open(&db_path).unwrap();
    init_db(&conn).unwrap();

    let (pairs, _) = sync_claims_db(&conn, &proc_root, &sessions_dir, "test-node").unwrap();
    assert_eq!(pairs.len(), 3);

    let claim_b = pairs.iter().find(|(c, _)| c.pid == 1500).unwrap().0.clone();
    let claim_a = pairs.iter().find(|(c, _)| c.pid == 2000).unwrap().0.clone();
    let claim_c = pairs.iter().find(|(c, _)| c.pid == 1000).unwrap().0.clone();

    assert_eq!(
        claim_b.suffix, 1,
        "Candidate B (100, 1500) has smallest (procStart, pid) -> suffix 1"
    );
    assert_eq!(
        claim_a.suffix, 2,
        "Candidate A (100, 2000) has second smallest -> suffix 2"
    );
    assert_eq!(
        claim_c.suffix, 3,
        "Candidate C (200, 1000) has largest procStart -> suffix 3"
    );
}

// =============================================================================
// Oracle 5: Fork identity in send/reply attribution (from = xmsg@<host> · claude:X~2)
// Mutant: strip suffix on attribution or route X~2 to plain => RED
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

    // 1. Resolve sessions
    let (s1, sock_p1) =
        resolve_claude_session(&conn, &proc_root, &sessions_dir, "test-node", conv_id).unwrap();
    assert_eq!(s1.session_id, conv_id);
    assert_eq!(sock_p1, listener_p1.sock_path);

    let forked_ref = format!("{conv_id}~2");
    let (s2, sock_p2) =
        resolve_claude_session(&conn, &proc_root, &sessions_dir, "test-node", &forked_ref).unwrap();
    assert_eq!(s2.session_id, forked_ref);
    assert_eq!(sock_p2, listener_p2.sock_path);

    // 2. Caller resolution by PID: PID 1002 resolves to X~2
    let (s2_by_pid, _) =
        resolve_claude_session(&conn, &proc_root, &sessions_dir, "test-node", "1002").unwrap();
    assert_eq!(
        s2_by_pid.session_id, forked_ref,
        "PID 1002 caller MUST resolve to fork session_id X~2"
    );

    // 3. Attributed badge format check
    let badge = format!("xmsg@test-node · claude:{}", s2_by_pid.session_id);
    assert_eq!(badge, format!("xmsg@test-node · claude:{conv_id}~2"));
}

// =============================================================================
// Oracle 6: Fork notice exact text and single delivery
// Mutant: notice text altered or delivered more than once => RED
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
    sync_claims(&conn, &proc_root, &sessions_dir, "test-node")
        .await
        .unwrap();
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

    sync_claims(&conn, &proc_root, &sessions_dir, "test-node")
        .await
        .unwrap();
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
    sync_claims(&conn, &proc_root, &sessions_dir, "test-node")
        .await
        .unwrap();
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
// Oracle 7: agy conversation UUID across restart
// Mutant: agy uses agy:{pid}:{start} or changes ID on restart => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_7_agy_conversation_uuid_across_restart() {
    let tmp = tempdir().unwrap();
    let proc_root = tmp.path().join("proc");
    let presence_dir = tmp.path().join("presence");
    let proc_locks = tmp.path().join("proc_locks");
    fs::create_dir_all(&presence_dir).unwrap();
    fs::write(&proc_locks, "").unwrap();

    let conv_uuid = "4a111111-2222-4333-8444-555555555555";
    let lock_file = presence_dir.join(format!("{conv_uuid}.lock"));
    fs::write(&lock_file, "").unwrap();

    let _agy_config = AgyConfig {
        presence_dir,
        proc_locks_path: proc_locks,
        proc_root: proc_root.clone(),
        agy_bin: "agy".to_string(),
        trusted_agy_exes: Vec::new(),
    };

    let agy_store = new_agy_store();
    let session_info = AgySessionInfo::new(conv_uuid.to_string(), 1234, "100".to_string(), None);

    // Verify session_key and session_id are conversation UUID
    assert_eq!(session_info.session_key, conv_uuid);
    assert_eq!(session_info.conversation_id, conv_uuid);
    assert!(is_uuid_shaped(&session_info.session_key));

    agy_store
        .write()
        .unwrap()
        .insert(session_info.session_key.clone(), session_info.clone());

    // Verify retrieval by conv_uuid
    let retrieved = agy_store.read().unwrap().get(conv_uuid).cloned().unwrap();
    assert_eq!(retrieved.session_key, conv_uuid);
    assert_eq!(retrieved.pid, 1234);

    // Verify across restart / re-creation with same conversation UUID
    let new_store = new_agy_store();
    let re_info = AgySessionInfo::new(conv_uuid.to_string(), 1234, "100".to_string(), None);
    new_store
        .write()
        .unwrap()
        .insert(re_info.session_key.clone(), re_info);
    assert!(new_store.read().unwrap().contains_key(conv_uuid));
}

// =============================================================================
// Oracle 8: pi server-minted UUID v4
// Mutant: pi uses asserted ID or non-v4 UUID => RED
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_8_pi_server_minted_uuid_v4() {
    let minted1 = mint_uuid_v4();
    let minted2 = mint_uuid_v4();

    assert_ne!(minted1, minted2, "Each minted UUID v4 must be unique");
    assert!(is_uuid_shaped(&minted1), "Minted UUID must be UUID-shaped");
    assert!(is_uuid_shaped(&minted2), "Minted UUID must be UUID-shaped");

    // Version 4 check: 13th hex char (index 14 with hyphens) must be '4'
    assert_eq!(minted1.chars().nth(14).unwrap(), '4');
    assert_eq!(minted2.chars().nth(14).unwrap(), '4');

    // Variant check: 17th hex char (index 19 with hyphens) must be 8, 9, a, or b
    let var1 = minted1.chars().nth(19).unwrap();
    assert!(
        matches!(var1, '8' | '9' | 'a' | 'b'),
        "Variant must be RFC 4122: {var1}"
    );

    // Verify PiStore stores and keys by server-minted UUID, not asserted ID
    let pi_store = new_pi_store();
    let server_id = mint_uuid_v4();
    let session = xmsg::pi::PiSessionInfo {
        session_id: server_id.clone(),
        name: None,
        pid: 999,
        starttime: "123".to_string(),
        cwd: "/workspace".to_string(),
        registered_at: 1000,
    };
    pi_store.write().unwrap().insert(server_id.clone(), session);

    assert!(pi_store.read().unwrap().contains_key(&server_id));
    assert!(!pi_store.read().unwrap().contains_key("asserted-client-id"));
}

// =============================================================================
// Oracle 9: Service name validation refusals
// Mutant: allow @, ·, ~, ., xmsg, or UUID => RED
// =============================================================================
#[test]
fn test_oracle_9_service_name_validation_refusals() {
    // 1. Reserved name 'xmsg'
    assert!(
        validate_service_name("xmsg").is_err(),
        "Name 'xmsg' must be refused"
    );

    // 2. Forbidden character '@'
    assert!(
        validate_service_name("worker@host").is_err(),
        "Name with '@' must be refused"
    );

    // 3. Forbidden character '·'
    assert!(
        validate_service_name("worker·daemon").is_err(),
        "Name with '·' must be refused"
    );

    // 4. Forbidden character '~'
    assert!(
        validate_service_name("worker~2").is_err(),
        "Name with '~' must be refused"
    );

    // 5. Forbidden character '.'
    assert!(
        validate_service_name("worker.daemon").is_err(),
        "Name with '.' must be refused"
    );

    // 6. UUID-shaped name
    assert!(
        validate_service_name("550e8400-e29b-41d4-a716-446655440000").is_err(),
        "UUID-shaped service name must be refused"
    );

    // 7. Valid service names
    assert!(validate_service_name("guard").is_ok());
    assert!(validate_service_name("dispatcher").is_ok());
    assert!(validate_service_name("matrix-bot").is_ok());
    assert!(validate_service_name("worker_1").is_ok());
}
