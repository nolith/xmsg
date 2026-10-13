//! An HTTP caller in a process of its own, for oracles that need a caller with a chosen
//! ancestry (A15): one whose parent is a Claude fake, or one outside every tree.
//!
//! The child is this test binary, re-run on the one test named by the caller (whose body is
//! `http_child::serve_if_child()`). It reads `req.json` from the directory named by
//! `XMSG_TEST_HTTP_CHILD`, makes the request, and writes `resp.json` there.
#![allow(dead_code)]

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

const ENV: &str = "XMSG_TEST_HTTP_CHILD";

/// Where a request goes: `http.sock` or the TCP `--listen` address.
#[derive(Clone)]
pub enum Via {
    Unix(PathBuf),
    Tcp(String),
}

/// The child's side: a no-op unless this process was started as an HTTP child.
pub fn serve_if_child() {
    let Ok(dir) = std::env::var(ENV) else {
        return;
    };
    let dir = PathBuf::from(dir);
    let req: Value =
        serde_json::from_str(&fs::read_to_string(dir.join("req.json")).unwrap()).expect("req.json");
    let method = req["method"].as_str().unwrap();
    let path = req["path"].as_str().unwrap();
    let body = req["body"].as_str();
    let raw = match body {
        Some(b) => format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{b}",
            b.len()
        ),
        None => format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
    };
    let mut out = Vec::new();
    let mut exchange = |s: &mut dyn ReadWrite| -> std::io::Result<()> {
        s.write_all(raw.as_bytes())?;
        s.read_to_end(&mut out).map(|_| ())
    };
    let res = match (req["via"].as_str().unwrap(), req["at"].as_str().unwrap()) {
        ("unix", at) => {
            std::os::unix::net::UnixStream::connect(at).and_then(|mut s| exchange(&mut s))
        }
        (_, at) => std::net::TcpStream::connect(at).and_then(|mut s| exchange(&mut s)),
    };
    let resp = match res {
        Ok(()) => {
            let text = String::from_utf8_lossy(&out).to_string();
            let status: u16 = text
                .split_whitespace()
                .nth(1)
                .and_then(|c| c.parse().ok())
                .unwrap_or(0);
            let body = text
                .split_once("\r\n\r\n")
                .map(|(_, b)| b.to_string())
                .unwrap_or_default();
            json!({ "status": status, "body": body, "pid": std::process::id() })
        }
        Err(e) => json!({ "status": 0, "body": e.to_string(), "pid": std::process::id() }),
    };
    let tmp = dir.join("resp.json.tmp");
    fs::write(&tmp, resp.to_string()).unwrap();
    fs::rename(&tmp, dir.join("resp.json")).unwrap();
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

fn request_value(via: &Via, method: &str, path: &str, body: Option<&Value>) -> Value {
    let (kind, at) = match via {
        Via::Unix(p) => ("unix", p.display().to_string()),
        Via::Tcp(a) => ("tcp", a.clone()),
    };
    json!({
        "via": kind,
        "at": at,
        "method": method,
        "path": path,
        "body": body.map(|b| b.to_string()),
    })
}

/// Waits for the child's `resp.json` and returns (status, body).
async fn take_response(dir: &Path) -> (u16, String) {
    let resp = dir.join("resp.json");
    for _ in 0..600 {
        if let Ok(text) = fs::read_to_string(&resp) {
            let _ = fs::remove_file(&resp);
            let v: Value = serde_json::from_str(&text).unwrap();
            return (
                v["status"].as_u64().unwrap() as u16,
                v["body"].as_str().unwrap().to_string(),
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "the HTTP child wrote no response in 30s ({})",
        dir.display()
    );
}

fn child_args(test_name: &str) -> String {
    format!("\"$XMSG_TEST_EXE\" {test_name} --exact --nocapture --test-threads=1 >/dev/null 2>&1")
}

/// A Claude fake whose child makes each request: a `sh` loop that starts one HTTP child
/// per request. Its pid is the fake's pid, and every request comes from its child.
pub struct FakeParent {
    child: Child,
    dir: tempfile::TempDir,
}

impl FakeParent {
    /// `test_name` is the test in this binary whose body is `serve_if_child()`.
    pub fn spawn(test_name: &str) -> FakeParent {
        let dir = tempfile::tempdir().unwrap();
        let script = format!(
            "while :; do if [ -e \"$XMSG_TEST_HTTP_CHILD/go\" ]; then rm -f \"$XMSG_TEST_HTTP_CHILD/go\"; {}; fi; sleep 0.05; done",
            child_args(test_name)
        );
        let child = Command::new("sh")
            .arg("-c")
            .arg(script)
            .env(ENV, dir.path())
            .env("XMSG_TEST_EXE", std::env::current_exe().unwrap())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sh");
        FakeParent { child, dir }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// One request, made by a child of this fake.
    pub async fn request(
        &self,
        via: &Via,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> (u16, String) {
        let req = request_value(via, method, path, body);
        fs::write(self.dir.path().join("req.json"), req.to_string()).unwrap();
        fs::write(self.dir.path().join("go"), "").unwrap();
        take_response(self.dir.path()).await
    }

    /// The fake exits.
    pub fn end(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for FakeParent {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One request from a process outside every tree: the child is started in the background
/// by a `sh` that exits at once, so its parent is no process of this test.
pub async fn request_detached(
    test_name: &str,
    via: &Via,
    method: &str,
    path: &str,
    body: Option<&Value>,
) -> (u16, String) {
    let dir = tempfile::tempdir().unwrap();
    let req = request_value(via, method, path, body);
    fs::write(dir.path().join("req.json"), req.to_string()).unwrap();
    let status = Command::new("sh")
        .arg("-c")
        .arg(format!("{} &", child_args(test_name)))
        .env(ENV, dir.path())
        .env("XMSG_TEST_EXE", std::env::current_exe().unwrap())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn sh");
    assert!(status.success());
    take_response(dir.path()).await
}
