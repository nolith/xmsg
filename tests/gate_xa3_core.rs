//! XA3 gating oracles for local resolution, labels and schema N, each against a real
//! `xmsg serve` (plan §3 XA3, oracles 1, 2, 3, 8 with the XA1 carry-over, and 9).
//!
//! Live recipients are real processes: a Claude fake is a `sleep` child (or this test
//! process) named by a session file whose `procStart` is its real start time, with an
//! inbox socket the test listens on; a service is this test process, trusted through
//! `--svc-exe <name>=<this test binary>` and registered on `register.sock`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
const SIGTERM: i32 = 15;

const ID_A: &str = "a3a3a3a3-0000-4000-8000-00000000000a";
const ID_B: &str = "b3b3b3b3-0000-4000-8000-00000000000b";
const ID_X: &str = "c3c3c3c3-0000-4000-8000-00000000000c";
const ID_T: &str = "d3d3d3d3-0000-4000-8000-00000000000d";
const ID_L: &str = "e3e3e3e3-0000-4000-8000-00000000000e";

fn live_starttime(pid: u32) -> String {
    xmsg::process::starttime(Path::new(xmsg::process::LIVE_PROC_ROOT), pid)
        .expect("live start time")
}

fn node_name() -> String {
    xmsg::node::local_node_name()
}

/// A real `xmsg serve`, stopped with SIGTERM on drop.
struct Node {
    dir: PathBuf,
    sessions: PathBuf,
    http_sock: PathBuf,
    agent_sock: PathBuf,
    register_sock: PathBuf,
    db: PathBuf,
    child: Option<Child>,
}

impl Node {
    /// Starts `xmsg serve` in `dir`, trusting this test binary as each of `services`.
    fn start(dir: &Path, services: &[&str]) -> Node {
        let run = dir.join("run");
        let sessions = dir.join("sessions");
        for d in [&run, &sessions] {
            fs::create_dir_all(d).unwrap();
            fs::set_permissions(d, fs::Permissions::from_mode(0o700)).unwrap();
        }
        let mut node = Node {
            dir: dir.to_path_buf(),
            http_sock: run.join("http.sock"),
            agent_sock: run.join("agent.sock"),
            register_sock: run.join("register.sock"),
            db: dir.join("xmsg.db"),
            sessions,
            child: None,
        };
        node.spawn(services);
        node
    }

    fn spawn(&mut self, services: &[&str]) {
        let exe = std::env::current_exe().unwrap();
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_xmsg"));
        cmd.env("XDG_RUNTIME_DIR", self.dir.join("run"))
            .arg("serve")
            .arg("--db-path")
            .arg(&self.db)
            .arg("--sessions-dir")
            .arg(&self.sessions)
            .arg("--http-sock")
            .arg(&self.http_sock)
            .arg("--agent-sock")
            .arg(&self.agent_sock)
            .arg("--register-sock")
            .arg(&self.register_sock)
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        for name in services {
            cmd.arg("--svc-exe")
                .arg(format!("{name}={}", exe.display()));
        }
        for s in [&self.http_sock, &self.agent_sock, &self.register_sock] {
            let _ = fs::remove_file(s);
        }
        let mut child = cmd.spawn().expect("spawn xmsg serve");
        for _ in 0..200 {
            if self.http_sock.exists() && self.agent_sock.exists() && self.register_sock.exists() {
                self.child = Some(child);
                return;
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("xmsg serve exited early: {status}");
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = child.kill();
        panic!("xmsg serve never bound its sockets");
    }

    /// SIGTERM, then wait for a clean exit.
    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            unsafe {
                kill(child.id() as i32, SIGTERM);
            }
            for _ in 0..200 {
                if let Ok(Some(status)) = child.try_wait() {
                    assert!(status.success(), "xmsg serve exit: {status}");
                    return;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            let _ = child.kill();
            let _ = child.wait();
            panic!("xmsg serve did not exit within 5s of SIGTERM");
        }
    }

    async fn http(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> (u16, String) {
        let sock = self.http_sock.clone();
        let (method, path) = (method.to_string(), path.to_string());
        let body = body.map(|b| b.to_string());
        let (status, text) = tokio::task::spawn_blocking(move || {
            xmsg::http::http_request_unix(&sock, &method, &path, body.as_deref())
        })
        .await
        .unwrap()
        .expect("http.sock request");
        (status.as_u16(), text)
    }

    /// An anonymous send over `http.sock`.
    async fn send(&self, to: &str, text: &str) -> (u16, serde_json::Value) {
        let (status, body) = self
            .http(
                "POST",
                &format!("/v1/sessions/{to}/messages"),
                Some(serde_json::json!({ "from": "ci", "text": text })),
            )
            .await;
        (
            status,
            serde_json::from_str(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    async fn list(&self) -> Vec<serde_json::Value> {
        let (status, body) = self.http("GET", "/v1/sessions", None).await;
        assert_eq!(status, 200, "{body}");
        serde_json::from_str(&body).unwrap()
    }

    /// One frame over `agent.sock`, from this test process.
    async fn agent(&self, frame: serde_json::Value) -> serde_json::Value {
        let stream = UnixStream::connect(&self.agent_sock).await.unwrap();
        let (r, mut w) = stream.into_split();
        w.write_all(format!("{frame}\n").as_bytes()).await.unwrap();
        let mut line = String::new();
        BufReader::new(r).read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A live process to stand behind a Claude fake.
struct Sleeper(Child);

impl Sleeper {
    fn new() -> Sleeper {
        Sleeper(
            Command::new("sleep")
                .arg("600")
                .stdout(Stdio::null())
                .spawn()
                .expect("spawn sleep"),
        )
    }
    fn pid(&self) -> u32 {
        self.0.id()
    }
    fn end(mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for Sleeper {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A Claude fake's inbox: every line delivered to its messaging socket.
struct Inbox {
    _dir: TempDir,
    sock: PathBuf,
    lines: Arc<Mutex<Vec<String>>>,
}

fn inbox() -> Inbox {
    let dir = tempdir().unwrap();
    let sock = dir.path().join("inbox.sock");
    let listener = UnixListener::bind(&sock).unwrap();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let rx = lines.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = String::new();
            let _ = stream.read_to_string(&mut buf).await;
            rx.lock().unwrap().push(buf);
        }
    });
    Inbox {
        _dir: dir,
        sock,
        lines,
    }
}

impl Inbox {
    async fn wait_line(&self) -> Option<String> {
        for _ in 0..100 {
            if let Some(line) = self.lines.lock().unwrap().first() {
                return Some(line.clone());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        None
    }
    async fn wait_for(&self, needle: &str) -> bool {
        for _ in 0..100 {
            if self
                .lines
                .lock()
                .unwrap()
                .iter()
                .any(|l| l.contains(needle))
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }
    fn is_empty(&self) -> bool {
        self.lines.lock().unwrap().is_empty()
    }
}

/// The `from-name` attribute of a delivered inbox line.
fn from_name(line: &str) -> String {
    let parsed: xmsg::inbox::InboxLine = serde_json::from_str(line).expect("inbox line");
    let content = parsed.message.content;
    let start = content.find("from-name=\"").expect("from-name") + "from-name=\"".len();
    let end = start + content[start..].find('"').expect("closing quote");
    content[start..end].to_string()
}

/// Writes a Claude session file presenting `id` for process `pid`.
fn claude_file(node: &Node, tag: &str, pid: u32, id: &str, name: &str, inbox: &Path) {
    let session = serde_json::json!({
        "pid": pid,
        "sessionId": id,
        "name": name,
        "cwd": "/w",
        "status": "idle",
        "kind": "interactive",
        "startedAt": 1,
        "updatedAt": 2,
        "procStart": live_starttime(pid),
        "messagingSocketPath": inbox.display().to_string(),
    });
    fs::write(
        node.sessions.join(format!("{pid}_{tag}.json")),
        session.to_string(),
    )
    .unwrap();
}

/// A service registration on `register.sock`, held open.
struct Service {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Service {
    async fn register(node: &Node, name: &str) -> Service {
        let stream = UnixStream::connect(&node.register_sock).await.unwrap();
        let (r, w) = stream.into_split();
        let mut svc = Service {
            reader: BufReader::new(r),
            writer: w,
        };
        let resp = svc
            .call(serde_json::json!({ "harness": "svc", "name": name }))
            .await;
        assert_eq!(resp["status"], "ok", "register {name}: {resp}");
        svc
    }

    async fn call(&mut self, frame: serde_json::Value) -> serde_json::Value {
        self.writer
            .write_all(format!("{frame}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(10), self.reader.read_line(&mut line))
            .await
            .expect("register.sock answer")
            .unwrap();
        serde_json::from_str(&line).unwrap()
    }

    /// One poll: the delivered message, or `None` on timeout.
    async fn poll(&mut self, wait_secs: u64) -> Option<serde_json::Value> {
        let resp = self
            .call(serde_json::json!({ "action": "poll", "waitSecs": wait_secs }))
            .await;
        (resp["action"] == "deliver").then_some(resp)
    }
}

// =============================================================================
// Oracle 1 (F1 is gone, A11). With a Claude fake labelled `genie-guard` and a service
// `genie-guard`, a bare `genie-guard` reaches the service.
// Mutant: try instance labels before services (the old HTTP resolver order) => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_1_services_resolve_before_instance_labels() {
    let tmp = tempdir().unwrap();
    let node = Node::start(tmp.path(), &["genie-guard"]);
    let mut guard = Service::register(&node, "genie-guard").await;

    let impostor = Sleeper::new();
    let impostor_inbox = inbox();
    claude_file(
        &node,
        "imp",
        impostor.pid(),
        ID_A,
        "genie-guard",
        &impostor_inbox.sock,
    );
    let listed = node.list().await;
    assert!(
        listed
            .iter()
            .any(|s| s["sessionId"] == ID_A && s["name"] == "genie-guard"),
        "the impostor is live and labelled genie-guard: {listed:?}"
    );

    let (status, body) = node.send("genie-guard", "for the guard").await;
    assert_eq!(status, 202, "{body}");
    assert_eq!(body["sessionId"], "genie-guard", "{body}");
    let delivered = guard.poll(3).await.expect("the service receives it");
    assert_eq!(delivered["text"], "for the guard");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        impostor_inbox.is_empty(),
        "an instance label never shadows a service"
    );
}

// =============================================================================
// Oracle 2 (A7). Two live instances labelled `foo` give `ambiguous`, listing both
// addresses as `id@node`.
// Mutant: take the first match => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_2_duplicate_labels_are_ambiguous() {
    let tmp = tempdir().unwrap();
    let node = Node::start(tmp.path(), &[]);
    let (p1, p2) = (Sleeper::new(), Sleeper::new());
    let (in1, in2) = (inbox(), inbox());
    claude_file(&node, "a", p1.pid(), ID_A, "foo", &in1.sock);
    claude_file(&node, "b", p2.pid(), ID_B, "foo", &in2.sock);

    let (status, body) = node.send("foo", "which foo").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "ambiguous", "{body}");
    let detail = body["detail"].as_str().unwrap();
    let node = node_name();
    for id in [ID_A, ID_B] {
        assert!(
            detail.contains(&format!("{id}@{node}")),
            "candidates list {id}@{node}: {detail}"
        );
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(in1.is_empty() && in2.is_empty(), "nothing is guessed");
}

// =============================================================================
// Oracle 3 (D-4). While only `X~2` is live, a bare `X` gives `ambiguous` with
// `X~2@node`.
// Mutant: treat a bare X with only forks live as not running => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_3_bare_id_with_only_forks_live_is_ambiguous() {
    let tmp = tempdir().unwrap();
    let node = Node::start(tmp.path(), &[]);
    let (p1, p2) = (Sleeper::new(), Sleeper::new());
    let (in1, in2) = (inbox(), inbox());

    claude_file(&node, "p1", p1.pid(), ID_X, "x", &in1.sock);
    node.list().await; // P1 observed first: plain X
    claude_file(&node, "p2", p2.pid(), ID_X, "x", &in2.sock);
    let listed = node.list().await;
    let fork = format!("{ID_X}~2");
    assert!(
        listed.iter().any(|s| s["sessionId"] == fork),
        "P2 holds X~2: {listed:?}"
    );

    fs::remove_file(node.sessions.join(format!("{}_p1.json", p1.pid()))).unwrap();
    p1.end();

    let (status, body) = node.send(ID_X, "bare-send-body").await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(body["error"], "ambiguous", "{body}");
    let detail = body["detail"].as_str().unwrap();
    assert!(
        detail.contains(&format!("{fork}@{}", node_name())),
        "the candidates name the live fork: {detail}"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    // In2 holds P2's fork notice (XA1); the bare send must not have reached it.
    assert!(
        !in2.lines
            .lock()
            .unwrap()
            .iter()
            .any(|l| l.contains("bare-send-body")),
        "a fork is never taken for the plain ID"
    );

    // Control: the fork's own address still reaches it.
    let (status, body) = node.send(&fork, "to the fork").await;
    assert_eq!(status, 202, "{body}");
    assert!(
        in2.wait_for("to the fork").await,
        "the fork's address reaches it"
    );
}

// =============================================================================
// Oracle 8 (A12, AM-8). An attested sender's label is `label · id@node`. A Claude
// session named `x · y@z` is listed with no label.
// Mutant: render the raw name (drop the AM-8 check) => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_8_label_render() {
    let tmp = tempdir().unwrap();
    let node = Node::start(tmp.path(), &[]);
    let target = Sleeper::new();
    let target_inbox = inbox();
    claude_file(&node, "t", target.pid(), ID_T, "target", &target_inbox.sock);
    let me_inbox = inbox();
    claude_file(&node, "me", std::process::id(), ID_L, "lab", &me_inbox.sock);

    let resp = node
        .agent(serde_json::json!({ "action": "send", "ref": ID_T, "text": "hi" }))
        .await;
    assert_eq!(resp["status"], "ok", "{resp}");
    let line = target_inbox.wait_line().await.expect("delivered");
    assert_eq!(from_name(&line), format!("lab · {ID_L}@{}", node_name()));

    let spoof = Sleeper::new();
    let spoof_inbox = inbox();
    claude_file(&node, "s", spoof.pid(), ID_A, "x · y@z", &spoof_inbox.sock);
    let listed = node.list().await;
    let entry = listed
        .iter()
        .find(|s| s["sessionId"] == ID_A)
        .unwrap_or_else(|| panic!("the session stays listed: {listed:?}"));
    assert!(
        entry["name"].is_null(),
        "a label carrying `@` or `·` is dropped: {entry}"
    );
}

// =============================================================================
// Oracle 8, XA1 carry-over (A12, A6, D-5). The label's attested part is the instance
// ADDRESS: a send from the `X~2` fork shows `X~2` in the receiver's label.
// Mutant: take the label's id from the session name => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_8_fork_sends_as_its_address() {
    let tmp = tempdir().unwrap();
    let node = Node::start(tmp.path(), &[]);
    let target = Sleeper::new();
    let target_inbox = inbox();
    claude_file(&node, "t", target.pid(), ID_T, "target", &target_inbox.sock);

    let holder = Sleeper::new();
    let holder_inbox = inbox();
    claude_file(&node, "h", holder.pid(), ID_X, "forked", &holder_inbox.sock);
    node.list().await; // the holder takes plain X
    let me_inbox = inbox();
    claude_file(
        &node,
        "me",
        std::process::id(),
        ID_X,
        "forked",
        &me_inbox.sock,
    );
    node.list().await; // this process takes X~2

    let resp = node
        .agent(serde_json::json!({ "action": "send", "ref": ID_T, "text": "from the fork" }))
        .await;
    assert_eq!(resp["status"], "ok", "{resp}");
    let line = target_inbox.wait_line().await.expect("delivered");
    assert_eq!(
        from_name(&line),
        format!("forked · {ID_X}~2@{}", node_name()),
        "the fork's label carries its own address"
    );
}

// =============================================================================
// Oracle 9 (schema N, plan gate v0 C5). A database created by the deployed ee1b6ca
// binary, with a row in every message table, starts at schema N with those tables
// empty and `user_version` N; a `forwarded_messages` table (199cecf's) is dropped too.
// Mutant: skip the drop => RED.
// =============================================================================
const EE1B6CA_DB: &str = include_str!("fixtures/ee1b6ca-db.sql");

const EE1B6CA_MESSAGE_TABLES: &[&str] = &[
    "messages",
    "outbound",
    "replies",
    "pi_pending_messages",
    "agy_pending_messages",
    "svc_pending_messages",
    "idempotency_keys",
    "peer_mailbox",
];

fn count(conn: &rusqlite::Connection, table: &str) -> i64 {
    conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

fn user_version(conn: &rusqlite::Connection) -> i64 {
    conn.query_row("PRAGMA user_version", [], |r| r.get(0))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_9_migration_from_ee1b6ca_drops_message_state() {
    for with_forwarded in [false, true] {
        let tmp = tempdir().unwrap();
        let db = tmp.path().join("xmsg.db");
        {
            let conn = rusqlite::Connection::open(&db).unwrap();
            conn.execute_batch(EE1B6CA_DB).unwrap();
            if with_forwarded {
                conn.execute_batch(
                    "CREATE TABLE forwarded_messages (id TEXT PRIMARY KEY, origin_peer TEXT NOT NULL, created_at INTEGER NOT NULL);
                     INSERT INTO forwarded_messages VALUES ('01OLDFWD', 'hub', 1760000000);",
                )
                .unwrap();
            }
            assert_eq!(user_version(&conn), 0, "the fixture records no version");
            for table in EE1B6CA_MESSAGE_TABLES {
                assert_eq!(count(&conn, table), 1, "fixture row in {table}");
            }
        }

        let mut node = Node::start(tmp.path(), &[]);
        node.stop();

        let conn = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(
            user_version(&conn),
            xmsg::storage::SCHEMA_VERSION,
            "the database is at schema N"
        );
        for table in EE1B6CA_MESSAGE_TABLES {
            assert_eq!(
                count(&conn, table),
                0,
                "{table} is emptied by the migration"
            );
        }
        if with_forwarded {
            assert_eq!(count(&conn, "forwarded_messages"), 0);
        }
    }
}

// =============================================================================
// Oracle 9, second arm (C5's restart). A pending message written at schema N survives
// an xmsg restart.
// Mutant: drop message state on every start => RED.
// =============================================================================
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_oracle_9_schema_n_survives_restart() {
    let tmp = tempdir().unwrap();
    let mut node = Node::start(tmp.path(), &["keeper"]);
    let keeper = Service::register(&node, "keeper").await;
    let (status, body) = node.send("keeper", "kept across a restart").await;
    assert_eq!(status, 202, "{body}");
    let message_id = body["messageId"].as_str().unwrap().to_string();
    drop(keeper);
    node.stop();
    {
        let conn = rusqlite::Connection::open(&node.db).unwrap();
        assert_eq!(user_version(&conn), xmsg::storage::SCHEMA_VERSION);
        assert_eq!(
            count(&conn, "svc_pending_messages"),
            1,
            "queued before the restart"
        );
    }

    node.spawn(&["keeper"]);
    let mut keeper = Service::register(&node, "keeper").await;
    let delivered = keeper
        .poll(3)
        .await
        .expect("the message queued at schema N survives the restart");
    assert_eq!(delivered["messageId"], message_id.as_str());
    assert_eq!(delivered["text"], "kept across a restart");
}
