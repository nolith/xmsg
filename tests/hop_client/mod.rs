//! An in-process federation client that sends an envelope straight to a node.
//!
//! Until H6's routing, nothing in xmsg originates a hop (`to.ref = <ref>@<dest>`, A3, B4);
//! the hub's handling of one is driven here by presenting the origin node's credentials
//! over mTLS and posting the envelope the hub accepts on `/fed/v2/messages`.
#![allow(dead_code)] // each test crate that includes this module uses part of it

use std::time::Duration;

use rustls::pki_types::ServerName;
use serde_json::{json, Value};
use xmsg::fed::make_tls_connector;

/// The origin node's credentials, DER (from `generate_self_signed_ed25519`).
pub struct Origin<'a> {
    pub cert_der: &'a [u8],
    pub key_der: &'a [u8],
}

/// Posts a hop envelope from a session principal on the origin to the hub at
/// `127.0.0.1:<hub_port>` (pinned by `hub_pin`): (HTTP status, response JSON).
pub async fn hop_send(
    origin: &Origin<'_>,
    hub_pin: &str,
    hub_port: u16,
    to_ref: &str,
    body: &str,
) -> (u16, Value) {
    send_envelope(
        origin,
        hub_pin,
        hub_port,
        envelope("alice", "Alice", to_ref, body),
    )
    .await
}

/// A v2 envelope from the instance `id` labelled `label`. Its origin claims a node the
/// receiver must not read (N-2): the receiver attests the node.
pub fn envelope(id: &str, label: &str, to_ref: &str, body: &str) -> Value {
    json!({
        "v": 2,
        "id": ulid::Ulid::new().to_string(),
        "origin": { "id": id, "node": "unattested.claim", "label": label },
        "to": { "ref": to_ref },
        "body": body,
        "thread_id": ulid::Ulid::new().to_string(),
        "created_at": 1000
    })
}

/// Posts `envelope` to `/fed/v2/messages` on the node at `127.0.0.1:<port>`.
pub async fn send_envelope(
    origin: &Origin<'_>,
    pin: &str,
    port: u16,
    envelope: Value,
) -> (u16, Value) {
    request(
        origin,
        pin,
        port,
        "POST",
        "/fed/v2/messages",
        Some(envelope),
    )
    .await
}

/// One request to the hub's federation listener as the origin node: (HTTP status, JSON).
pub async fn request(
    origin: &Origin<'_>,
    hub_pin: &str,
    hub_port: u16,
    method: &str,
    uri: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let fut = async {
        let connector = make_tls_connector(origin.cert_der, origin.key_der, hub_pin)
            .expect("origin TLS connector");
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", hub_port))
            .await
            .expect("connect to the hub's federation listener");
        let tls = connector
            .connect(ServerName::try_from("hub".to_string()).unwrap(), tcp)
            .await
            .expect("mTLS handshake with the hub");
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls))
                .await
                .expect("HTTP handshake with the hub");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = hyper::Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.map(|b| b.to_string()).unwrap_or_default(),
            )))
            .unwrap();
        let resp = sender.send_request(req).await.expect("hub answers");
        let status = resp.status().as_u16();
        use http_body_util::BodyExt;
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        if bytes.is_empty() {
            return (status, Value::Null);
        }
        let v: Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
            panic!(
                "hub answered {status} with non-JSON: {}",
                String::from_utf8_lossy(&bytes)
            )
        });
        (status, v)
    };
    tokio::time::timeout(Duration::from_secs(15), fut)
        .await
        .expect("hub request timed out after 15s")
}
