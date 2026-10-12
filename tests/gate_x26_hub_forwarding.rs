use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};

use serde_json::Value;
use xmsg::fed::{generate_self_signed_ed25519_pem, normalize_pin};

struct ChildGuard(Option<tokio::process::Child>);

impl ChildGuard {
    fn new(child: tokio::process::Child) -> Self {
        Self(Some(child))
    }
    fn child_mut(&mut self) -> &mut tokio::process::Child {
        self.0.as_mut().unwrap()
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.start_kill();
        }
    }
}

fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr").port()
}

async fn send_via_agent_sock(
    agent_sock: &Path,
    target_ref: &str,
    text: &str,
    push_replies: bool,
) -> Value {
    let stream = UnixStream::connect(agent_sock)
        .await
        .expect("connect to agent.sock");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let send_req = serde_json::json!({
        "action": "send",
        "ref": target_ref,
        "text": text,
        "push_replies": push_replies,
    });
    writer
        .write_all(format!("{send_req}\n").as_bytes())
        .await
        .unwrap();

    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();
    serde_json::from_str(&resp_line).expect("valid json response from agent.sock")
}

async fn reply_via_agent_sock(agent_sock: &Path, message_id: &str, text: &str) -> Value {
    let stream = UnixStream::connect(agent_sock)
        .await
        .expect("connect to agent.sock");
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let reply_req = serde_json::json!({
        "action": "reply",
        "messageId": message_id,
        "text": text,
    });
    writer
        .write_all(format!("{reply_req}\n").as_bytes())
        .await
        .unwrap();

    let mut resp_line = String::new();
    reader.read_line(&mut resp_line).await.unwrap();
    serde_json::from_str(&resp_line).expect("valid json response from agent.sock")
}

fn create_claude_session_fixture(
    sessions_dir: &Path,
    session_id: &str,
    name: &str,
) -> (PathBuf, Arc<Mutex<Vec<String>>>) {
    let sock_path = sessions_dir.join(format!("inbox-{session_id}.sock"));
    let listener = UnixListener::bind(&sock_path).unwrap();
    let received = Arc::new(Mutex::new(Vec::new()));
    let rx_clone = received.clone();

    tokio::spawn(async move {
        loop {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = Vec::new();
                let _ = stream.read_to_end(&mut buf).await;
                if let Ok(s) = String::from_utf8(buf) {
                    rx_clone.lock().unwrap().push(s);
                }
            }
        }
    });

    let my_pid = std::process::id();
    let live_proc = PathBuf::from(xmsg::process::LIVE_PROC_ROOT);
    let my_proc_start =
        xmsg::process::starttime(&live_proc, my_pid).unwrap_or_else(|_| "1000".to_string());
    let session_json = serde_json::json!({
        "pid": my_pid,
        "sessionId": session_id,
        "name": name,
        "cwd": "/workspace",
        "status": "idle",
        "kind": "interactive",
        "startedAt": 1000,
        "updatedAt": 2000,
        "procStart": my_proc_start,
        "messagingSocketPath": sock_path.display().to_string()
    });
    fs::write(
        sessions_dir.join(format!("{my_pid}-{session_id}.json")),
        session_json.to_string(),
    )
    .unwrap();

    (sock_path, received)
}

async fn wait_for_inbox_message(
    received: &Arc<Mutex<Vec<String>>>,
    timeout_dur: Duration,
) -> Option<String> {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout_dur {
        {
            let guard = received.lock().unwrap();
            if let Some(msg) = guard.first() {
                return Some(msg.clone());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[allow(dead_code)]
struct TestNode {
    _tmp: TempDir,
    name: String,
    fed_port: u16,
    http_port: u16,
    agent_sock: PathBuf,
    sessions_dir: PathBuf,
    db_path: PathBuf,
    child: ChildGuard,
}

async fn spawn_test_node(
    name: &str,
    fed_port: u16,
    http_port: u16,
    cert_pem: &str,
    key_pem: &str,
    peers_json: Value,
) -> TestNode {
    let tmp = tempdir().unwrap();
    fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o700)).unwrap();

    let cert_file = tmp.path().join("cert.pem");
    let key_file = tmp.path().join("key.pem");
    let peers_file = tmp.path().join("peers.json");
    let db_path = tmp.path().join("node.db");

    fs::write(&cert_file, cert_pem.as_bytes()).unwrap();
    fs::write(&key_file, key_pem.as_bytes()).unwrap();
    fs::write(&peers_file, peers_json.to_string()).unwrap();

    let run_dir = tmp.path().join("run");
    fs::create_dir_all(&run_dir).unwrap();
    fs::set_permissions(&run_dir, fs::Permissions::from_mode(0o700)).unwrap();

    let sessions_dir = tmp.path().join("sessions");
    fs::create_dir_all(&sessions_dir).unwrap();
    fs::set_permissions(&sessions_dir, fs::Permissions::from_mode(0o700)).unwrap();

    let agent_sock = run_dir.join("agent.sock");
    let register_sock = run_dir.join("register.sock");
    let http_sock = run_dir.join("http.sock");

    let bin = env!("CARGO_BIN_EXE_xmsg");
    let child = tokio::process::Command::new(bin)
        .env("XDG_RUNTIME_DIR", &run_dir)
        .args([
            "serve",
            "--host-label",
            name,
            "--listen",
            &format!("127.0.0.1:{http_port}"),
            "--fed-listen",
            &format!("127.0.0.1:{fed_port}"),
            "--peers-file",
            peers_file.to_str().unwrap(),
            "--fed-cert",
            cert_file.to_str().unwrap(),
            "--fed-key",
            key_file.to_str().unwrap(),
            "--db-path",
            db_path.to_str().unwrap(),
            "--sessions-dir",
            sessions_dir.to_str().unwrap(),
            "--register-sock",
            register_sock.to_str().unwrap(),
            "--agent-sock",
            agent_sock.to_str().unwrap(),
            "--http-sock",
            http_sock.to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn test node");

    let mut guard = ChildGuard::new(child);

    let mut ready = false;
    let mut exited = None;
    for _ in 0..100 {
        if let Ok(Some(status)) = guard.child_mut().try_wait() {
            exited = Some(status);
            break;
        }
        if agent_sock.exists()
            && tokio::net::TcpStream::connect(("127.0.0.1", fed_port))
                .await
                .is_ok()
            && tokio::net::TcpStream::connect(("127.0.0.1", http_port))
                .await
                .is_ok()
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    if !ready {
        let mut err = String::new();
        if let Some(mut stderr) = guard.child_mut().stderr.take() {
            use tokio::io::AsyncReadExt;
            let _ = stderr.read_to_string(&mut err).await;
        }
        if let Some(status) = exited {
            panic!("node '{name}' exited prematurely with {status}: {err}");
        } else {
            panic!("node '{name}' failed to become ready: {err}");
        }
    }

    TestNode {
        _tmp: tmp,
        name: name.to_string(),
        fed_port,
        http_port,
        agent_sock,
        sessions_dir,
        db_path,
        child: guard,
    }
}

// Extract message_id from delivery footer: "[xmsg] message_id=<id> ..."
fn extract_message_id(text: &str) -> Option<String> {
    for line in text.lines() {
        if let Some(pos) = line.find("message_id=") {
            let rest = &line[pos + 11..];
            let id = rest.split_whitespace().next().unwrap_or("");
            if !id.is_empty() {
                return Some(id.to_string());
            }
        }
    }
    None
}

async fn run_test_with_port_retry<F, Fut>(name: &str, f: F)
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    for attempt in 1..=5 {
        let task = tokio::task::spawn(f());
        match task.await {
            Ok(()) => return,
            Err(join_err) if join_err.is_panic() => {
                let panic_payload = join_err.into_panic();
                let is_port_conflict = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                    s.contains("AddrInUse")
                        || s.contains("Address already in use")
                        || s.contains("address already in use")
                        || s.contains("failed to bind")
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.contains("AddrInUse")
                        || s.contains("Address already in use")
                        || s.contains("address already in use")
                        || s.contains("failed to bind")
                } else {
                    false
                };

                if is_port_conflict && attempt < 5 {
                    eprintln!("{name}: port collision on attempt {attempt}, retrying...");
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
                std::panic::resume_unwind(panic_payload);
            }
            Err(e) => panic!("{name} join error: {e}"),
        }
    }
}

// =============================================================================
// Oracle 1: Relay and reply
// A session on A sends to <session>@B@H. It is delivered on B with origin
// showing A's principal and A as origin host. B's session replies, and A receives it.
// Mutant: keep refusing '@' (today's no_forward) => RED
// Line: test_oracle_1_relay_and_reply
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_1_relay_and_reply() {
    run_test_with_port_retry("test_oracle_1_relay_and_reply", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        // Node A knows Hub
        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        // Hub knows A (with "forward") and B (with "send", "reply")
        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply", "forward"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        // Node B knows Hub (with "relayed" capability)
        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply", "relayed"]
            }
        });

        let node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;

        // Create session on A (Alice) and session on B (Bob)
        let (_sock_a, rx_a) = create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");
        let (_sock_b, rx_b) = create_claude_session_fixture(&node_b.sessions_dir, "bob", "Bob");

        // Alice on A sends to bob@node-b@hub
        let send_resp = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Hello Bob from Alice via Hub!",
            true,
        )
        .await;

        assert_eq!(
            send_resp["status"], "ok",
            "send must succeed, got: {:?}",
            send_resp
        );

        // Wait for Bob on B to receive the relayed message
        let b_msg = wait_for_inbox_message(&rx_b, Duration::from_secs(5))
            .await
            .expect("Bob should receive relayed message on B");

        assert!(
            b_msg.contains("Hello Bob from Alice via Hub!"),
            "Bob should see original message body: {b_msg}"
        );
        assert!(
            b_msg.contains("xmsg@node-a · claude:Alice") || b_msg.contains("xmsg@node-a"),
            "Bob should see origin showing node-a: {b_msg}"
        );

        let msg_id = extract_message_id(&b_msg).expect("message_id in Bob's delivery footer");

        // Bob on B replies to Alice's message
        let reply_resp = reply_via_agent_sock(
            &node_b.agent_sock,
            &msg_id,
            "Hello Alice! Reply routed through Hub.",
        )
        .await;

        assert_eq!(
            reply_resp["status"], "ok",
            "reply must succeed, got: {:?}",
            reply_resp
        );

        // Alice on A should receive the reply pushed to her inbox socket
        let a_reply = wait_for_inbox_message(&rx_a, Duration::from_secs(5))
            .await
            .expect("Alice should receive reply on A");

        assert!(
            a_reply.contains("Hello Alice! Reply routed through Hub."),
            "Alice should see reply text: {a_reply}"
        );
        assert!(
            a_reply.contains("claude:Bob"),
            "Alice should see origin of reply from Bob: {a_reply}"
        );
    })
    .await;
}

// =============================================================================
// Oracle 2: Polling sender
// Same as Oracle 1, but with push_replies: false: A's sender reads the reply by long-poll.
// Mutant: drop the forward record on H so the reply isn't relayed => RED
// Line: test_oracle_2_polling_sender
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_2_polling_sender() {
    run_test_with_port_retry("test_oracle_2_polling_sender", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply", "forward"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply", "relayed"]
            }
        });

        let node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;

        let (_sock_a, rx_a) = create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");
        let (_sock_b, rx_b) = create_claude_session_fixture(&node_b.sessions_dir, "bob", "Bob");

        // Alice sends with push_replies: false
        let send_resp = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Polling sender test message",
            false,
        )
        .await;

        assert_eq!(send_resp["status"], "ok");
        let sent_msg_id = send_resp["delivery"]["messageId"].as_str().unwrap();

        let b_msg = wait_for_inbox_message(&rx_b, Duration::from_secs(5))
            .await
            .expect("Bob should receive relayed message on B");
        let msg_id = extract_message_id(&b_msg).expect("message_id in footer");
        assert_eq!(sent_msg_id, msg_id);

        // Bob replies
        let reply_resp = reply_via_agent_sock(
            &node_b.agent_sock,
            &msg_id,
            "Reply meant for long-polling sender",
        )
        .await;
        assert_eq!(reply_resp["status"], "ok");

        // Alice should NOT receive pushed message on inbox socket (push_replies = false)
        tokio::time::sleep(Duration::from_millis(200)).await;
        {
            let guard = rx_a.lock().unwrap();
            assert!(
                guard.is_empty(),
                "Alice inbox should not receive push reply"
            );
        }

        // Alice reads reply by long-poll on node A's HTTP endpoint
        let client = reqwest::Client::new();
        let poll_url = format!(
            "http://127.0.0.1:{}/v1/messages/{sent_msg_id}/replies?wait=5",
            node_a.http_port
        );
        let poll_resp = client
            .get(&poll_url)
            .send()
            .await
            .expect("poll replies request");
        assert_eq!(poll_resp.status(), axum::http::StatusCode::OK);

        let replies: Vec<Value> = poll_resp.json().await.expect("json array of replies");
        assert!(!replies.is_empty(), "long-poll should return replies");
        assert_eq!(
            replies[0]["text"], "Reply meant for long-polling sender",
            "reply text should match"
        );
    })
    .await;
}

// =============================================================================
// Oracle 3: A client that can't be dialled
// A is outbound-linked to H (dial: "outbound"); H marks A leaf.
// The A->B send and its reply still complete.
// Mutant: do not store reply for undialable origin in fed_reply_handler => RED
// Line: test_oracle_3_undialable_client
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_3_undialable_client() {
    run_test_with_port_retry("test_oracle_3_undialable_client", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        // Node A is outbound-linked to Hub
        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "dial": "outbound",
                "allow": ["send", "reply", "inbox"]
            }
        });

        // Hub marks A as leaf and allows forward; B is dialable
        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "leaf": true,
                "allow": ["send", "reply", "inbox", "forward"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        // Node B allows relayed from hub
        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply", "relayed"]
            }
        });

        let node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;

        let (_sock_a, rx_a) = create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");
        let (_sock_b, rx_b) = create_claude_session_fixture(&node_b.sessions_dir, "bob", "Bob");

        // Alice on outbound-linked node A sends to Bob on B via Hub
        let send_resp = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Outbound client send to Bob",
            true,
        )
        .await;

        assert_eq!(send_resp["status"], "ok");

        let b_msg = wait_for_inbox_message(&rx_b, Duration::from_secs(5))
            .await
            .expect("Bob should receive message from outbound client");
        assert!(b_msg.contains("Outbound client send to Bob"));
        let msg_id = extract_message_id(&b_msg).expect("message_id");

        // Bob replies
        let reply_resp = reply_via_agent_sock(
            &node_b.agent_sock,
            &msg_id,
            "Reply to outbound linked client Alice",
        )
        .await;
        assert_eq!(reply_resp["status"], "ok");

        // Alice's outbound reply poller pulls reply from Hub and delivers locally
        let a_reply = wait_for_inbox_message(&rx_a, Duration::from_secs(8))
            .await
            .expect("Alice should receive reply via outbound link poller");
        assert!(a_reply.contains("Reply to outbound linked client Alice"));
    })
    .await;
}

// =============================================================================
// Oracle 4: Gates
// 4a: H refuses when A's entry lacks forward. Mutant: skip that check => RED
// 4b: B refuses when H doesn't allow relaying. Mutant: skip that check => RED
// 4c: dest's targets applies to forwarded ref.
// Line: test_oracle_4_gates
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_4_gates() {
    run_test_with_port_retry("test_oracle_4_gates", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        // 4a test setup: H does NOT give A "forward" capability
        let peers_h_no_fwd = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply"] // NO "forward"
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        let peers_b_no_relayed = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"] // NO "relayed"
            }
        });

        let _node_b = spawn_test_node(
            "node-b",
            port_b,
            http_b,
            &cert_b,
            &key_b,
            peers_b_no_relayed,
        )
        .await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h_no_fwd).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;
        let (_sock_a, _rx_a) =
            create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");

        // Gate 4a: Send through Hub without "forward" capability must fail with no_forward
        let res_4a = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Test lacking forward",
            true,
        )
        .await;

        assert_eq!(res_4a["status"], "error");
        assert_eq!(
            res_4a["error"], "no_forward",
            "4a: H must refuse send lacking 'forward' with no_forward: {:?}",
            res_4a
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_4b_recipient_relayed_gate() {
    run_test_with_port_retry("test_oracle_4b_recipient_relayed_gate", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        // Hub allows forward for A
        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply", "forward"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        // B does NOT allow relayed
        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"] // NO "relayed"
            }
        });

        let _node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;
        let (_sock_a, _rx_a) =
            create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");

        let res_4b = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Test lacking relayed on B",
            true,
        )
        .await;

        assert_eq!(res_4b["status"], "error");
        assert_eq!(
            res_4b["error"], "op_denied",
            "4b: B must refuse forwarded message lacking 'relayed' with op_denied: {:?}",
            res_4b
        );
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_4c_dest_targets_gate() {
    run_test_with_port_retry("test_oracle_4c_dest_targets_gate", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        // Hub restricts B's targets to only ["allowed_bob"]
        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply", "forward"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"],
                "targets": ["claude:allowed_bob"]
            }
        });

        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply", "relayed"]
            }
        });

        let _node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;
        let (_sock_a, _rx_a) =
            create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");

        // Send to forbidden target on B
        let res_4c = send_via_agent_sock(
            &node_a.agent_sock,
            "forbidden_bob@node-b@hub",
            "Test targets check",
            true,
        )
        .await;

        assert_eq!(res_4c["status"], "error");
        assert_eq!(
            res_4c["error"], "op_denied",
            "4c: H must check dest targets and reject forbidden ref with op_denied: {:?}",
            res_4c
        );
    })
    .await;
}

// =============================================================================
// Oracle 5: One hop
// A message forwarded by H to a second forwarding node H2 is refused there.
// Mutant: no hop mark => RED
// Line: test_oracle_5_one_hop
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_5_one_hop() {
    run_test_with_port_retry("test_oracle_5_one_hop", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h1, key_h1, pin_h1) = generate_self_signed_ed25519_pem("hub1").unwrap();
        let (cert_h2, key_h2, pin_h2) = generate_self_signed_ed25519_pem("hub2").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h1 = get_free_port();
        let http_h1 = get_free_port();
        let port_h2 = get_free_port();
        let http_h2 = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub1": {
                "address": format!("127.0.0.1:{port_h1}"),
                "pin": normalize_pin(&pin_h1),
                "allow": ["send", "reply"]
            }
        });

        let peers_h1 = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply", "forward"]
            },
            "hub2": {
                "address": format!("127.0.0.1:{port_h2}"),
                "pin": normalize_pin(&pin_h2),
                "allow": ["send", "reply"]
            }
        });

        let peers_h2 = serde_json::json!({
            "hub1": {
                "address": format!("127.0.0.1:{port_h1}"),
                "pin": normalize_pin(&pin_h1),
                "allow": ["send", "reply", "forward", "relayed"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        let peers_b = serde_json::json!({
            "hub2": {
                "address": format!("127.0.0.1:{port_h2}"),
                "pin": normalize_pin(&pin_h2),
                "allow": ["send", "reply", "relayed"]
            }
        });

        let _node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub2 = spawn_test_node("hub2", port_h2, http_h2, &cert_h2, &key_h2, peers_h2).await;
        let _hub1 = spawn_test_node("hub1", port_h1, http_h1, &cert_h1, &key_h1, peers_h1).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;
        let (_sock_a, _rx_a) =
            create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");

        // A sends to bob@node-b@hub2@hub1: H1 forwards to H2, H2 must refuse with no_forward
        let res = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub2@hub1",
            "Two-hop attempt",
            true,
        )
        .await;

        assert_eq!(res["status"], "error");
        assert_eq!(
            res["error"], "no_forward",
            "H2 must refuse already-forwarded envelope with no_forward: {:?}",
            res
        );
    })
    .await;
}

// =============================================================================
// Oracle 6: Off by default
// With no forward anywhere, ref@dest@H is refused with no_forward, exactly as today.
// Mutant: allow forwarding unconditionally without checking peer config => RED
// Line: test_oracle_6_off_by_default
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_oracle_6_off_by_default() {
    run_test_with_port_retry("test_oracle_6_off_by_default", || async move {
        let (cert_a, key_a, pin_a) = generate_self_signed_ed25519_pem("node-a").unwrap();
        let (cert_h, key_h, pin_h) = generate_self_signed_ed25519_pem("hub").unwrap();
        let (cert_b, key_b, pin_b) = generate_self_signed_ed25519_pem("node-b").unwrap();

        let port_a = get_free_port();
        let http_a = get_free_port();
        let port_h = get_free_port();
        let http_h = get_free_port();
        let port_b = get_free_port();
        let http_b = get_free_port();

        let peers_a = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        // Default configuration: no "forward" or "relayed" anywhere
        let peers_h = serde_json::json!({
            "node-a": {
                "address": format!("127.0.0.1:{port_a}"),
                "pin": normalize_pin(&pin_a),
                "allow": ["send", "reply"]
            },
            "node-b": {
                "address": format!("127.0.0.1:{port_b}"),
                "pin": normalize_pin(&pin_b),
                "allow": ["send", "reply"]
            }
        });

        let peers_b = serde_json::json!({
            "hub": {
                "address": format!("127.0.0.1:{port_h}"),
                "pin": normalize_pin(&pin_h),
                "allow": ["send", "reply"]
            }
        });

        let _node_b = spawn_test_node("node-b", port_b, http_b, &cert_b, &key_b, peers_b).await;
        let _hub = spawn_test_node("hub", port_h, http_h, &cert_h, &key_h, peers_h).await;
        let node_a = spawn_test_node("node-a", port_a, http_a, &cert_a, &key_a, peers_a).await;
        let (_sock_a, _rx_a) =
            create_claude_session_fixture(&node_a.sessions_dir, "alice", "Alice");

        let res = send_via_agent_sock(
            &node_a.agent_sock,
            "bob@node-b@hub",
            "Send to unconfigured hub",
            true,
        )
        .await;

        assert_eq!(res["status"], "error");
        assert_eq!(
            res["error"], "no_forward",
            "Hub without forward capability must refuse with no_forward: {:?}",
            res
        );
    })
    .await;
}
