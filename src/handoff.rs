//! A14's queue across one user's nodes (XA7; A16, D-1).
//!
//! A parked node cannot see X go live elsewhere, so the node that gains X asks: when a claim
//! scan newly observes X holding its plain ID, this node asks each of its user's other nodes
//! for X's queue, all of them at once, so a conversation parked on several of them gets
//! every queue in one registration (the user's parked listings are one destination).
//!
//! A holder hands its queue over only when the asker is the only live listing of X in a
//! complete view of the user's nodes and the asker: a node of the user that could not be
//! asked keeps the queue where it is, and so do two live listings. It transfers first and
//! drops its queue and its parked listing only when the asker acknowledges; the asker
//! stores what it was handed under the message IDs, so a transfer repeated after a lost
//! acknowledgement or a holder's crash delivers nothing twice (X1). A node of another user
//! is refused.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::{extract::State, http::StatusCode, response::IntoResponse, Extension, Json};
use rustls::pki_types::ServerName;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::error::AppError;
use crate::fed::{AuthenticatedPeer, FedState};
use crate::http::AppState;
use crate::storage::{self, QueuedMessage};

/// How often, and how many times, an asker retries a holder that could not answer for
/// sure (unreachable, crashed, or without a complete view).
const RETRY_INTERVAL: Duration = Duration::from_secs(1);
const ATTEMPTS: usize = 60;

/// A hand-off request's bound: the holder asks the user's nodes for `list` first.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HandoffRequest {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub messages: Vec<QueuedMessage>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ack {
    pub id: String,
    pub messages: Vec<String>,
}

// ponytail: one process-wide channel, set once by `serve`; a library caller that never
// spawns the puller observes without pulling.
static OBSERVED: OnceLock<mpsc::UnboundedSender<String>> = OnceLock::new();

/// A claim scan newly observed `id` holding its plain ID (D-1's event).
pub fn observed(id: &str) {
    if let Some(tx) = OBSERVED.get() {
        let _ = tx.send(id.to_string());
    }
}

/// Starts pulling queues for newly observed conversations, on a node with peers.
pub fn spawn(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let _ = OBSERVED.set(tx);
    tokio::spawn(async move {
        while let Some(id) = rx.recv().await {
            tokio::spawn(pull(state.clone(), id));
        }
    })
}

/// The user's other nodes this node can dial: its own user's peers, less dial-out clients.
fn holders(fs: &FedState) -> Vec<String> {
    let me = crate::fed::own_user(fs);
    fs.current_peers()
        .iter()
        .filter(|(_, c)| !c.leaf && crate::mesh::peer_user(c).as_deref() == Some(me.as_str()))
        .map(|(k, _)| k.clone())
        .collect()
}

/// Pulls X's queue from every holder, retrying the ones that could not answer for sure.
async fn pull(state: Arc<AppState>, id: String) {
    let Some(fs) = state.fed_state.clone() else {
        return;
    };
    let mut pending = holders(&fs);
    for _ in 0..ATTEMPTS {
        if pending.is_empty() {
            return;
        }
        let mut asked = tokio::task::JoinSet::new();
        for key in pending.drain(..) {
            let (state, fs, id) = (state.clone(), fs.clone(), id.clone());
            asked.spawn(async move {
                let settled = pull_from(&state, &fs, &key, &id).await;
                (key, settled)
            });
        }
        while let Some(res) = asked.join_next().await {
            match res {
                Ok((key, false)) => pending.push(key),
                Ok((_, true)) => {}
                Err(e) => warn!("hand-off task failed for {id}: {e}"),
            }
        }
        if !pending.is_empty() {
            tokio::time::sleep(RETRY_INTERVAL).await;
        }
    }
    warn!("hand-off of {id} unsettled with {}", pending.join(", "));
}

/// One holder's hand-off: true once it has answered for sure (handed over and
/// acknowledged, or refused), false to ask again.
async fn pull_from(state: &AppState, fs: &FedState, key: &str, id: &str) -> bool {
    let req = serde_json::json!({ "id": id });
    let transfer = match post(fs, key, "/fed/v2/handoff", &req).await {
        Ok((StatusCode::OK, body)) => match serde_json::from_slice::<Transfer>(&body) {
            Ok(t) => t,
            Err(e) => {
                warn!("unreadable hand-off of {id} from {key}: {e}");
                return false;
            }
        },
        // Not parked there, two live listings, or not this node's to take.
        Ok((s, _)) if matches!(s.as_u16(), 403 | 404 | 409) => return true,
        Ok((s, _)) => {
            debug!("hand-off of {id} from {key}: {s}");
            return false;
        }
        Err(e) => {
            debug!("hand-off of {id} from {key}: {e}");
            return false;
        }
    };
    let ids: Vec<String> = transfer.messages.iter().map(|m| m.id.clone()).collect();
    {
        let Ok(db) = state.db.lock() else {
            return false;
        };
        for m in &transfer.messages {
            let m = QueuedMessage {
                instance_id: id.to_string(),
                ..m.clone()
            };
            if let Err(e) = storage::insert_handed_over(&db, &m) {
                warn!("storing handed-over message {}: {e}", m.id);
                return false;
            }
        }
    }
    // Delivered here, by the scan that takes a live holder's queue (A14).
    if !ids.is_empty() {
        let _ = crate::claim::sync_claims(
            &state.db,
            &state.agy_config.proc_root,
            &state.sessions_dirs,
            &state.node_name,
        )
        .await;
    }
    let ack = Ack {
        id: id.to_string(),
        messages: ids,
    };
    let ack = serde_json::to_value(&ack).unwrap_or_default();
    matches!(
        post(fs, key, "/fed/v2/handoff/ack", &ack).await,
        Ok((StatusCode::OK, _))
    )
}

/// Refuses a node of another user (A16): only the user's own nodes take its queues.
fn same_user(fs: &FedState, peer: &AuthenticatedPeer) -> Result<String, AppError> {
    let me = crate::fed::own_user(fs);
    if peer.user != me {
        return Err(AppError::Denied(format!(
            "'{}' is not a node of user '{me}'",
            peer.name
        )));
    }
    Ok(me)
}

/// `POST /fed/v2/handoff`: X's queue, for the asker that is X's only live listing.
pub async fn handoff_handler(
    State(fs): State<Arc<FedState>>,
    Extension(peer): Extension<Arc<AuthenticatedPeer>>,
    Json(req): Json<HandoffRequest>,
) -> Result<axum::response::Response, AppError> {
    let me = same_user(&fs, &peer)?;
    let id = req.id;
    let here = crate::surface::listing(&fs.local(), fs.reply_ttl, &me).await?;
    match here
        .iter()
        .find(|e| e.kind == "instance" && e.id == id)
        .map(|e| e.status.as_str())
    {
        Some("parked") => {}
        Some(_) => {
            return Err(AppError::Ambiguous(format!(
                "{id}@{} is live",
                fs.node_name
            )))
        }
        None => return Err(AppError::NotFound(id)),
    }

    // A complete view of the user's nodes, and the asker.
    let peers = fs.current_peers();
    let mut keys: Vec<String> = peers
        .iter()
        .filter(|(_, c)| crate::mesh::peer_user(c).as_deref() == Some(me.as_str()))
        .map(|(k, _)| k.clone())
        .collect();
    if !keys.contains(&peer.name) {
        keys.push(peer.name.clone());
    }
    let mut asked = tokio::task::JoinSet::new();
    for key in keys {
        let fs = fs.clone();
        asked.spawn(async move {
            let (entries, st) = crate::fed::list_peer(&fs, &key).await;
            (key, entries, st)
        });
    }
    let (mut unasked, mut live) = (Vec::new(), Vec::new());
    while let Some(res) = asked.join_next().await {
        let (key, entries, st) = res.map_err(|e| AppError::Internal(e.to_string()))?;
        if st.status != "ok" {
            unasked.push(key);
        } else if entries
            .iter()
            .any(|e| e.kind == "instance" && e.id == id && e.status == "live")
        {
            live.push(key);
        }
    }
    if !unasked.is_empty() {
        unasked.sort();
        return Err(AppError::Unavailable(format!(
            "could not ask {} for {id}; its queue stays on {}",
            unasked.join(", "),
            fs.node_name
        )));
    }
    live.sort();
    match live.as_slice() {
        [one] if *one == peer.name => {}
        [_, _, ..] => {
            return Err(AppError::Ambiguous(
                live.iter()
                    .map(|k| format!("{id}@{k}"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ))
        }
        _ => {
            return Err(AppError::NotLive(format!(
                "'{id}' is not live on {} alone",
                peer.name
            )))
        }
    }

    let messages = {
        let db = fs
            .db
            .lock()
            .map_err(|e| AppError::Internal(e.to_string()))?;
        storage::queued_for(&db, &id, storage::now_epoch_secs())
            .map_err(|e| AppError::Internal(e.to_string()))?
    };
    Ok((StatusCode::OK, Json(Transfer { messages })).into_response())
}

/// `POST /fed/v2/handoff/ack`: the asker holds what it was handed; drop it here.
pub async fn ack_handler(
    State(fs): State<Arc<FedState>>,
    Extension(peer): Extension<Arc<AuthenticatedPeer>>,
    Json(ack): Json<Ack>,
) -> Result<axum::response::Response, AppError> {
    same_user(&fs, &peer)?;
    let db = fs
        .db
        .lock()
        .map_err(|e| AppError::Internal(e.to_string()))?;
    storage::drop_handed_over(&db, &ack.id, &ack.messages)
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok((StatusCode::OK, Json(serde_json::json!({}))).into_response())
}

/// POSTs `body` to the direct peer keyed `key`: its status and body.
async fn post(
    fs: &FedState,
    key: &str,
    uri: &str,
    body: &serde_json::Value,
) -> Result<(StatusCode, bytes::Bytes), AppError> {
    let peer = fs
        .current_peers()
        .get(key)
        .cloned()
        .ok_or_else(|| AppError::UnknownPeer(key.to_string()))?;
    let unreachable = |what: &str, e: &dyn std::fmt::Display| {
        AppError::PeerUnreachable(format!("{what} '{}': {e}", peer.name))
    };
    let fut = async {
        let connector = crate::fed::make_tls_connector_for_peer(fs, &peer)?;
        let tcp = tokio::net::TcpStream::connect(&peer.address)
            .await
            .map_err(|e| unreachable("connecting to", &e))?;
        let name = ServerName::try_from(peer.name.clone())
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let tls = connector
            .connect(name, tcp)
            .await
            .map_err(|e| unreachable("TLS with", &e))?;
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(tls))
                .await
                .map_err(|e| unreachable("HTTP with", &e))?;
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let req = hyper::Request::builder()
            .method("POST")
            .uri(uri)
            .header("content-type", "application/json")
            .header("host", peer.name.as_str())
            .body(http_body_util::Full::new(bytes::Bytes::from(
                body.to_string(),
            )))
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let resp = sender
            .send_request(req)
            .await
            .map_err(|e| unreachable("request to", &e))?;
        let status = resp.status();
        use http_body_util::BodyExt;
        let limit = fs.max_body * 64 + 4096;
        let bytes = http_body_util::Limited::new(resp.into_body(), limit)
            .collect()
            .await
            .map_err(|e| unreachable("reading from", &e))?
            .to_bytes();
        Ok((status, bytes))
    };
    tokio::time::timeout(REQUEST_TIMEOUT, fut)
        .await
        .map_err(|_| unreachable("timed out asking", &"no answer"))?
}
