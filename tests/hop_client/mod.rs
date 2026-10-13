//! An in-process federation client that sends a hop envelope straight to a hub.
//!
//! Until XA5's routing, nothing in xmsg originates a hop (`to.ref = <ref>@<dest>`); the
//! hub's handling of one is driven here by presenting the origin node's credentials over
//! mTLS and posting the envelope the hub accepts on `/fed/v1/messages`.

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
    let envelope = json!({
        "v": 1,
        "id": ulid::Ulid::new().to_string(),
        "principal": {
            "kind": "session",
            "harness": "claude",
            "session_id": "alice",
            "name": "Alice"
        },
        "to": { "ref": to_ref },
        "body": body,
        "thread_id": ulid::Ulid::new().to_string(),
        "created_at": 1000
    });

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
            .method("POST")
            .uri("/fed/v1/messages")
            .header("content-type", "application/json")
            .body(http_body_util::Full::new(bytes::Bytes::from(
                envelope.to_string(),
            )))
            .unwrap();
        let resp = sender.send_request(req).await.expect("hub answers");
        let status = resp.status().as_u16();
        use http_body_util::BodyExt;
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
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
        .expect("hop send timed out after 15s")
}
