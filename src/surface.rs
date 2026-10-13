//! The local messaging surface (A13): `list`, `send`, `reply` and `replies`, one
//! implementation behind HTTP, `agent.sock`, `register.sock` and MCP.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;

use crate::error::AppError;
use crate::http::{AppState, ResolvedTarget};
use crate::inbox;
use crate::registry::Session;
use crate::storage::{self, MessageRecord, Origin, ReplyRecord};

/// The longest `replies` wait, in seconds (A-12).
pub const MAX_WAIT_SECS: u64 = 60;

/// What local delivery reads, borrowed from an `AppState` or a `FedState`.
pub struct Local<'a> {
    pub db: &'a Arc<Mutex<rusqlite::Connection>>,
    pub sessions_dirs: &'a [PathBuf],
    pub node_name: &'a str,
    pub agy_config: &'a crate::agy::AgyConfig,
    pub agy_store: &'a crate::agy::AgyStore,
    pub pi_store: &'a crate::pi::PiStore,
    pub pi_notify_tx: &'a broadcast::Sender<String>,
    pub svc_store: &'a crate::svc::SvcStore,
    pub svc_notify_tx: &'a broadcast::Sender<String>,
}

impl AppState {
    pub fn local(&self) -> Local<'_> {
        Local {
            db: &self.db,
            sessions_dirs: &self.sessions_dirs,
            node_name: &self.node_name,
            agy_config: &self.agy_config,
            agy_store: &self.agy_store,
            pi_store: &self.pi_store,
            pi_notify_tx: &self.pi_notify_tx,
            svc_store: &self.svc_store,
            svc_notify_tx: &self.svc_notify_tx,
        }
    }
}

impl crate::fed::FedState {
    pub fn local(&self) -> Local<'_> {
        Local {
            db: &self.db,
            sessions_dirs: std::slice::from_ref(&self.sessions_dir),
            node_name: &self.node_name,
            agy_config: &self.agy_config,
            agy_store: &self.agy_store,
            pi_store: &self.pi_store,
            pi_notify_tx: &self.pi_notify_tx,
            svc_store: &self.svc_store,
            svc_notify_tx: &self.svc_notify_tx,
        }
    }
}

fn db_err(e: impl std::fmt::Display) -> AppError {
    AppError::Internal(e.to_string())
}

// -----------------------------------------------------------------------------
// Senders
// -----------------------------------------------------------------------------

/// Who is sending: an attested session or service (its inbox receives pushed replies,
/// A10), or an anonymous HTTP caller, who reads replies with `replies` (A15).
#[derive(Debug, Clone)]
pub enum Sender {
    Session(Session),
    Anonymous(String),
}

impl Sender {
    /// A registered service named `name`, as the leaf principal or a `register.sock` caller.
    pub fn service(name: &str) -> Sender {
        Sender::Session(service_session(name))
    }

    /// A12's label for this sender on `node`.
    pub fn label(&self, node: &str) -> Result<String, AppError> {
        match self {
            Sender::Session(s) => Ok(inbox::sender_label(
                node,
                &s.harness,
                &s.session_id,
                s.name.as_deref(),
            )),
            Sender::Anonymous(from) => inbox::sanitize_from(node, from),
        }
    }

    pub fn origin(&self, node: &str) -> Origin {
        match self {
            Sender::Session(s) if s.harness == "svc" => Origin::service(&s.session_id, node),
            Sender::Session(s) => Origin::instance(
                &s.session_id,
                node,
                s.name
                    .as_deref()
                    .and_then(inbox::clean_label)
                    .filter(|l| l != &s.session_id)
                    .as_deref(),
            ),
            Sender::Anonymous(from) => Origin::anonymous(from, node),
        }
    }

    /// The idempotency principal: the instance ID or service name, `anon:<from>` for an
    /// anonymous caller (A15).
    pub fn idempotency_principal(&self) -> String {
        match self {
            Sender::Session(s) => s.session_id.clone(),
            Sender::Anonymous(from) => format!("anon:{from}"),
        }
    }

    pub fn fed_principal(&self) -> crate::fed::FedPrincipal {
        match self {
            Sender::Session(s) if s.harness == "svc" => crate::fed::FedPrincipal::Service {
                name: s.session_id.clone(),
            },
            Sender::Session(s) => crate::fed::FedPrincipal::Session {
                harness: s.harness.clone(),
                session_id: s.session_id.clone(),
                name: s.name.clone().unwrap_or_else(|| s.session_id.clone()),
            },
            Sender::Anonymous(from) => crate::fed::FedPrincipal::Anonymous { from: from.clone() },
        }
    }

    /// Where a reply is pushed: the sender's kind and ID, `None` for a sender with no
    /// inbox (A10 as amended by A-2).
    fn inbox(&self) -> (Option<String>, Option<String>) {
        match self {
            Sender::Session(s) => (Some(s.harness.clone()), Some(s.session_id.clone())),
            Sender::Anonymous(_) => (None, None),
        }
    }
}

/// The session a registered service acts as. Its ID is its name.
pub fn service_session(name: &str) -> Session {
    Session {
        session_id: name.to_string(),
        name: Some(name.to_string()),
        pid: 0,
        cwd: "/".to_string(),
        status: "idle".to_string(),
        kind: "daemon".to_string(),
        entrypoint: None,
        version: None,
        started_at: 0,
        updated_at: 0,
        harness: "svc".to_string(),
        registered: Some(true),
    }
}

/// The leaf principal's service name: the configured principal without a `svc:` prefix.
pub fn leaf_service_name(principal: &str) -> &str {
    principal.strip_prefix("svc:").unwrap_or(principal)
}

/// Parses one socket frame. A frame with fields `T` does not know is refused
/// `bad_request` naming every one of them (plan §6 D3), whatever their order.
pub fn parse_frame<T: serde::de::DeserializeOwned>(line: &str) -> Result<T, AppError> {
    let bad = |e: serde_json::Error| AppError::BadRequest(e.to_string());
    let mut value: serde_json::Value = serde_json::from_str(line).map_err(bad)?;
    let mut unknown = Vec::new();
    loop {
        let err = match serde_json::from_value::<T>(value.clone()) {
            Ok(frame) if unknown.is_empty() => return Ok(frame),
            Ok(_) => break,
            Err(e) => e,
        };
        let msg = err.to_string();
        let field = msg
            .strip_prefix("unknown field `")
            .and_then(|r| r.split_once('`'))
            .map(|(f, _)| f.to_string());
        match field {
            Some(f) if value.as_object_mut().and_then(|o| o.remove(&f)).is_some() => {
                unknown.push(format!("`{f}`"))
            }
            _ if unknown.is_empty() => return Err(AppError::BadRequest(msg)),
            _ => break,
        }
    }
    Err(AppError::BadRequest(format!(
        "unknown field{} {}",
        if unknown.len() > 1 { "s" } else { "" },
        unknown.join(", ")
    )))
}

// -----------------------------------------------------------------------------
// The address grammar
// -----------------------------------------------------------------------------

/// Splits a `to` into its local part and its `@node`, if any. An address has at most one
/// `@`; the hop `x@dest@hub` is not in the grammar (A3, A9, B4).
pub fn parse_to(to: &str) -> Result<(&str, Option<&str>), AppError> {
    let to = to.trim();
    if to.is_empty() {
        return Err(AppError::BadRequest("'to' must not be empty".to_string()));
    }
    match to.matches('@').count() {
        0 => Ok((to, None)),
        1 => {
            let (r, node) = to.split_once('@').expect("one '@'");
            if r.is_empty() || node.is_empty() {
                return Err(AppError::BadRequest(format!(
                    "'{to}' is not an address: expected <id or name>[@<node>]"
                )));
            }
            Ok((r, Some(node)))
        }
        _ => Err(AppError::BadRequest(format!(
            "'{to}' has more than one '@': an address is <id or name>[@<node>], and the route is the server's"
        ))),
    }
}

// -----------------------------------------------------------------------------
// list
// -----------------------------------------------------------------------------

/// One entry of `list` (A13): its address, kind, label and status, and the node holding it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Recipient {
    pub address: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub status: String,
    pub node: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListQuery {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
}

fn recipient(id: &str, service: bool, label: Option<String>, node: &str) -> Recipient {
    Recipient {
        address: format!("{id}@{node}"),
        kind: if service { "service" } else { "instance" }.to_string(),
        label: if service {
            None
        } else {
            label
                .as_deref()
                .and_then(inbox::clean_label)
                .filter(|l| l != id)
        },
        status: "live".to_string(),
        node: node.to_string(),
    }
}

/// The recipients the caller may see on this node, or on the peer `node` names.
pub async fn list(state: &AppState, query: &ListQuery) -> Result<Vec<Recipient>, AppError> {
    if let Some(kind) = query.kind.as_deref() {
        if kind != "service" && kind != "instance" {
            return Err(AppError::BadRequest(format!(
                "kind '{kind}' is not 'service' or 'instance'"
            )));
        }
    }
    let mut out = match query.node.as_deref() {
        Some(node) if node != state.node_name => {
            let fed_state = state
                .fed_state
                .as_ref()
                .filter(|fs| fs.peers.contains_name(node))
                .ok_or_else(|| AppError::UnknownPeer(node.to_string()))?;
            let key = fed_state.peers.key_for(node).unwrap_or(node).to_string();
            crate::fed::list_federated_sessions(fed_state, &key)
                .await?
                .into_iter()
                .map(|e| recipient(&e.session_id, e.harness == "svc", e.name, &key))
                .collect()
        }
        _ => local_recipients(state).await?,
    };
    out.retain(|r| {
        query.kind.as_deref().is_none_or(|k| r.kind == k)
            && query.name.as_deref().is_none_or(|n| {
                let local = r.address.rsplit_once('@').map_or("", |(l, _)| l);
                r.label
                    .as_deref()
                    .is_some_and(|l| l.eq_ignore_ascii_case(n))
                    || (r.kind == "service" && local.eq_ignore_ascii_case(n))
            })
    });
    Ok(out)
}

async fn local_recipients(state: &AppState) -> Result<Vec<Recipient>, AppError> {
    let node = state.node_name.as_str();
    let query = crate::registry::SessionsQuery::default();
    let pairs = crate::claim::sync_claims(
        &state.db,
        &state.agy_config.proc_root,
        &state.sessions_dirs,
        node,
    )
    .await?;
    let mut out: Vec<Recipient> = pairs
        .into_iter()
        .map(|(claim, entry)| recipient(&claim.address(), false, entry.name, node))
        .collect();
    for s in crate::agy::list_agy_sessions(&state.agy_config, &state.agy_store, &query) {
        out.push(recipient(&s.session_id, false, None, node));
    }
    for s in crate::pi::list_pi_sessions(&state.agy_config.proc_root, &state.pi_store, &query) {
        out.push(recipient(&s.session_id, false, s.name, node));
    }
    for s in crate::svc::list_svc_sessions(&state.agy_config.proc_root, &state.svc_store, &query) {
        out.push(recipient(&s.session_id, true, None, node));
    }
    Ok(out)
}

// -----------------------------------------------------------------------------
// Local delivery
// -----------------------------------------------------------------------------

fn agy_has_creds(store: &crate::agy::AgyStore, session: &Session) -> bool {
    let store_lock = store.read().unwrap();
    store_lock
        .get(&session.session_id)
        .or_else(|| {
            store_lock.values().find(|info| {
                info.conversation_id == session.session_id
                    || session.name.as_deref() == Some(&info.conversation_id)
            })
        })
        .map(|info| info.has_credentials())
        .unwrap_or(false)
}

/// Delivers a message to a resolved local target. Returns the target's ID, its harness
/// and the outcome.
pub(crate) async fn deliver(
    local: &Local<'_>,
    target: &ResolvedTarget,
    from_name: &str,
    message_id: &str,
    body: &str,
    origin: Origin,
) -> Result<(String, &'static str, &'static str), AppError> {
    let envelope_text = || {
        format!(
            "[xmsg] from={from_name} message_id={message_id} — reply with the xmsg reply tool\n\n{body}"
        )
    };
    match target {
        ResolvedTarget::Claude(session, socket_path) => {
            let body_with_footer = format!(
                "{body}\n\n[xmsg] message_id={message_id} — reply with the xmsg reply tool"
            );
            let line = inbox::encode_transport_line(from_name, &body_with_footer)?;
            inbox::deliver_to_socket(socket_path, &line).await?;
            Ok((session.session_id.clone(), "claude", "delivered"))
        }
        ResolvedTarget::Agy(session) => {
            if agy_has_creds(local.agy_store, session) {
                crate::agy::deliver_agy(
                    local.agy_config,
                    local.agy_store,
                    session,
                    from_name,
                    message_id,
                    body,
                )
                .await?;
                return Ok((session.session_id.clone(), "agy", "delivered"));
            }
            let agy_msg = storage::AgyPendingMessage {
                id: message_id.to_string(),
                session_id: session.session_id.clone(),
                created_at: storage::now_epoch_secs(),
                from_name: from_name.to_string(),
                bytes: body.len(),
                text: body.to_string(),
                envelope: envelope_text(),
                delivered_at: None,
            };
            let db = local.db.lock().map_err(db_err)?;
            storage::insert_agy_message(&db, &agy_msg).map_err(db_err)?;
            Ok((session.session_id.clone(), "agy", "queued"))
        }
        ResolvedTarget::Pi(session) => {
            let pi_msg = storage::PiPendingMessage {
                id: message_id.to_string(),
                session_id: session.session_id.clone(),
                created_at: storage::now_epoch_secs(),
                from_name: from_name.to_string(),
                bytes: body.len(),
                text: body.to_string(),
                envelope: envelope_text(),
                delivered_at: None,
            };
            {
                let db = local.db.lock().map_err(db_err)?;
                if !storage::insert_pi_message(&db, &pi_msg).map_err(db_err)? {
                    return Err(AppError::ServiceUnavailable(
                        "pi session message queue is full".to_string(),
                    ));
                }
            }
            let _ = local.pi_notify_tx.send(session.session_id.clone());
            Ok((session.session_id.clone(), "pi", "delivered"))
        }
        ResolvedTarget::Svc(session) => {
            let svc_msg = storage::SvcPendingMessage {
                id: message_id.to_string(),
                session_id: session.session_id.clone(),
                created_at: storage::now_epoch_secs(),
                from_name: from_name.to_string(),
                bytes: body.len(),
                text: body.to_string(),
                envelope: envelope_text(),
                delivered_at: None,
                origin,
                in_reply_to: None,
            };
            {
                let db = local.db.lock().map_err(db_err)?;
                if !storage::insert_svc_message(&db, &svc_msg).map_err(db_err)? {
                    return Err(AppError::ServiceUnavailable(
                        "svc session message queue is full".to_string(),
                    ));
                }
            }
            let _ = local.svc_notify_tx.send(session.session_id.clone());
            Ok((session.session_id.clone(), "svc", "delivered"))
        }
    }
}

/// Pushes a reply into the inbox of a local sender (A10): a Claude, agy or pi session, or
/// a registered service, whose push lands in the inbox it long-polls with `in_reply_to`.
/// Returns `pushed`, `sender_gone` or `push_failed`.
#[allow(clippy::too_many_arguments)]
pub async fn push_reply(
    local: &Local<'_>,
    ret_harness: &str,
    ret_session_id: &str,
    in_reply_to: &str,
    reply_id: &str,
    replier_label: &str,
    replier_origin: Origin,
    text: &str,
) -> &'static str {
    let envelope = format!(
        "[xmsg] reply to message_id={in_reply_to} — message_id={reply_id}; reply with the xmsg reply tool\n\n{text}"
    );
    match ret_harness {
        "claude" => {
            match crate::claim::resolve_claude_session_and_deliver(
                local.db,
                &local.agy_config.proc_root,
                local.sessions_dirs,
                local.node_name,
                ret_session_id,
            )
            .await
            {
                Ok((_session, socket_path)) if socket_path.exists() => {
                    let Ok(line) = inbox::encode_transport_line(replier_label, &envelope) else {
                        return "push_failed";
                    };
                    match inbox::deliver_to_socket(&socket_path, &line).await {
                        Ok(()) => "pushed",
                        Err(e) => {
                            let err = e.to_string();
                            if err.contains("Connection refused")
                                || err.contains("No such file or directory")
                            {
                                "sender_gone"
                            } else {
                                "push_failed"
                            }
                        }
                    }
                }
                Ok(_) | Err(AppError::Gone { .. }) | Err(AppError::NotFound(_)) => "sender_gone",
                Err(_) => "push_failed",
            }
        }
        "agy" => {
            match crate::agy::resolve_agy_session(local.agy_config, local.agy_store, ret_session_id)
            {
                Ok(Some(session)) if agy_has_creds(local.agy_store, &session) => {
                    match crate::agy::deliver_agy_envelope(
                        local.agy_config,
                        local.agy_store,
                        &session,
                        replier_label,
                        &envelope,
                    )
                    .await
                    {
                        Ok(()) => "pushed",
                        Err(AppError::Gone { .. }) | Err(AppError::CredentialsStale(_)) => {
                            "sender_gone"
                        }
                        Err(_) => "push_failed",
                    }
                }
                Ok(Some(session)) => {
                    let agy_msg = storage::AgyPendingMessage {
                        id: reply_id.to_string(),
                        session_id: session.session_id.clone(),
                        created_at: storage::now_epoch_secs(),
                        from_name: replier_label.to_string(),
                        bytes: text.len(),
                        text: text.to_string(),
                        envelope,
                        delivered_at: None,
                    };
                    match local.db.lock() {
                        Ok(db) if storage::insert_agy_message(&db, &agy_msg).is_ok() => "pushed",
                        _ => "push_failed",
                    }
                }
                Ok(None) => "sender_gone",
                Err(_) => "push_failed",
            }
        }
        "pi" => {
            match crate::pi::resolve_pi_session(
                &local.agy_config.proc_root,
                local.pi_store,
                ret_session_id,
            ) {
                Ok(Some(session)) => {
                    let pi_msg = storage::PiPendingMessage {
                        id: reply_id.to_string(),
                        session_id: session.session_id.clone(),
                        created_at: storage::now_epoch_secs(),
                        from_name: replier_label.to_string(),
                        bytes: text.len(),
                        text: text.to_string(),
                        envelope,
                        delivered_at: None,
                    };
                    let inserted = match local.db.lock() {
                        Ok(db) => storage::insert_pi_message(&db, &pi_msg).unwrap_or(false),
                        Err(_) => false,
                    };
                    if inserted {
                        let _ = local.pi_notify_tx.send(session.session_id);
                        "pushed"
                    } else {
                        "push_failed"
                    }
                }
                Ok(None) => "sender_gone",
                Err(_) => "push_failed",
            }
        }
        "svc" => {
            match crate::svc::resolve_svc_session(
                &local.agy_config.proc_root,
                local.svc_store,
                ret_session_id,
            ) {
                Ok(Some(session)) => {
                    let svc_msg = storage::SvcPendingMessage {
                        id: reply_id.to_string(),
                        session_id: session.session_id.clone(),
                        created_at: storage::now_epoch_secs(),
                        from_name: replier_label.to_string(),
                        bytes: text.len(),
                        text: text.to_string(),
                        envelope,
                        delivered_at: None,
                        origin: replier_origin,
                        in_reply_to: Some(in_reply_to.to_string()),
                    };
                    let inserted = match local.db.lock() {
                        Ok(db) => storage::insert_svc_message(&db, &svc_msg).unwrap_or(false),
                        Err(_) => false,
                    };
                    if inserted {
                        let _ = local.svc_notify_tx.send(session.session_id);
                        "pushed"
                    } else {
                        "push_failed"
                    }
                }
                Ok(None) => "sender_gone",
                Err(_) => "push_failed",
            }
        }
        _ => "sender_gone",
    }
}

// -----------------------------------------------------------------------------
// send
// -----------------------------------------------------------------------------

/// What `send` returns (A13): the message ID, the resolved address (`id@node` for an
/// instance, `name@node` for a service) and the outcome.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SendResult {
    pub message_id: String,
    pub address: String,
    pub outcome: String,
}

fn check_idempotency_key(key: &str) -> Result<(), AppError> {
    if key.is_empty() || key.len() > 128 || key.chars().any(|c| c.is_control()) {
        return Err(AppError::BadRequest(
            "invalid idempotency_key: must be 1..128 non-control chars".to_string(),
        ));
    }
    Ok(())
}

/// Sends `text` to `to` from `sender`. A replay of an idempotency key returns the first
/// send's result, its message ID included; the key is the sender's (A15).
pub async fn send(
    state: &AppState,
    sender: &Sender,
    to: &str,
    text: &str,
    idempotency_key: Option<&str>,
) -> Result<SendResult, AppError> {
    if text.is_empty() {
        return Err(AppError::BadRequest("text must not be empty".to_string()));
    }
    if text.len() > state.max_body {
        return Err(AppError::PayloadTooLarge {
            size: text.len(),
            limit: state.max_body,
        });
    }
    let (target_ref, node) = parse_to(to)?;
    let from_name = sender.label(&state.node_name)?;
    let principal = sender.idempotency_principal();

    if let Some(key) = idempotency_key {
        check_idempotency_key(key)?;
        let existing = {
            let db = state.db.lock().map_err(db_err)?;
            storage::get_idempotency_record(&db, &principal, key, state.idempotency_ttl.as_secs())
                .map_err(db_err)?
        };
        if let Some(record) = existing {
            if record.body != text {
                return Err(AppError::Conflict(format!(
                    "idempotency key '{key}' was already used with a different message body"
                )));
            }
            return Ok(SendResult {
                message_id: record.message_id,
                address: record.session_id,
                outcome: record.outcome,
            });
        }
    }

    let (ret_harness, ret_session_id) = sender.inbox();
    let message_id = ulid::Ulid::new().to_string();
    let now = storage::now_epoch_secs();

    let (address, recipient_harness, outcome, return_host) = match node {
        Some(node) if node != state.node_name => {
            let fed_state = state
                .fed_state
                .as_ref()
                .filter(|fs| fs.peers.contains_name(node))
                .ok_or_else(|| AppError::UnknownPeer(node.to_string()))?
                .clone();
            // A typed nickname or canonical name, as this node's entry key.
            let peer_key = fed_state.peers.key_for(node).unwrap_or(node).to_string();
            let envelope = crate::fed::FedEnvelope {
                v: 1,
                id: message_id.clone(),
                principal: sender.fed_principal(),
                to: crate::fed::FedTarget {
                    r#ref: target_ref.to_string(),
                },
                body: text.to_string(),
                thread_id: message_id.clone(),
                created_at: now,
                forwarded: None,
            };
            let resp = crate::fed::send_federated_message(&fed_state, &peer_key, &envelope).await?;
            let outcome = resp.outcome.unwrap_or_else(|| "delivered".to_string());
            let address = format!("{}@{peer_key}", resp.session_id);
            // A-2: the outbound reply poller is armed by the sender's kind.
            if ret_harness.is_some()
                && fed_state
                    .peers
                    .get(&peer_key)
                    .is_some_and(|p| p.is_outbound())
            {
                crate::fed::spawn_outbound_reply_poller(
                    fed_state.clone(),
                    peer_key.clone(),
                    message_id.clone(),
                );
            }
            let db = state.db.lock().map_err(db_err)?;
            let _ = storage::insert_outbound(&db, &message_id, &peer_key, &address, &outcome, now);
            (
                address,
                "fed".to_string(),
                outcome,
                Some(state.node_name.clone()),
            )
        }
        _ => {
            let target = crate::http::resolve_target_session(state, target_ref).await?;
            let (id, harness, outcome) = deliver(
                &state.local(),
                &target,
                &from_name,
                &message_id,
                text,
                sender.origin(&state.node_name),
            )
            .await?;
            (
                format!("{id}@{}", state.node_name),
                harness.to_string(),
                outcome.to_string(),
                None,
            )
        }
    };

    let session_id = address
        .rsplit_once('@')
        .map_or(address.as_str(), |(id, _)| id)
        .to_string();
    {
        let db = state.db.lock().map_err(db_err)?;
        storage::insert_message(
            &db,
            &MessageRecord {
                id: message_id.clone(),
                created_at: now,
                session_id,
                from_name: from_name.clone(),
                bytes: text.len(),
                outcome: outcome.clone(),
                recipient_harness,
                return_harness: ret_harness,
                return_session_id: ret_session_id,
                thread_id: message_id.clone(),
                return_host,
            },
        )
        .map_err(db_err)?;
        if let Some(key) = idempotency_key {
            let record = storage::IdempotencyRecord {
                principal,
                key: key.to_string(),
                body: text.to_string(),
                message_id: message_id.clone(),
                session_id: address.clone(),
                from_name: from_name.clone(),
                bytes: text.len(),
                outcome: outcome.clone(),
                created_at: now,
            };
            let _ = storage::insert_idempotency_record(&db, &record);
            let _ = storage::purge_idempotency_keys(&db, state.idempotency_ttl.as_secs());
        }
        let ttl = state.reply_ttl.as_secs();
        let _ = storage::purge_messages(&db, ttl);
        let _ = storage::purge_replies(&db, ttl);
        let _ = storage::purge_pi_messages(&db, ttl);
        let _ = storage::purge_agy_messages(&db, ttl);
        let _ = storage::purge_svc_messages(&db, ttl);
        let _ = storage::purge_peer_mailbox(&db, ttl);
        let _ = storage::purge_forwarded_messages(&db, ttl);
    }

    // Invariant: message bodies are never logged.
    tracing::info!(
        target: "xmsg",
        message_id = %message_id,
        address = %address,
        from = %from_name,
        bytes = text.len(),
        outcome = %outcome
    );
    Ok(SendResult {
        message_id,
        address,
        outcome,
    })
}

// -----------------------------------------------------------------------------
// reply
// -----------------------------------------------------------------------------

async fn push_federated_reply(
    state: &AppState,
    caller: &Session,
    ret_host: &str,
    in_reply_to: &str,
    reply_id: &str,
    text: &str,
) -> &'static str {
    let Some(fed_state) = &state.fed_state else {
        return "push_failed";
    };
    let replier = crate::fed::FedReplier {
        harness: caller.harness.clone(),
        session_id: caller.session_id.clone(),
        name: caller
            .name
            .clone()
            .unwrap_or_else(|| caller.session_id.clone()),
    };
    let envelope = crate::fed::FedReplyEnvelope {
        v: 1,
        id: reply_id.to_string(),
        in_reply_to: in_reply_to.to_string(),
        replier,
        text: text.to_string(),
        created_at: storage::now_epoch_secs(),
    };
    match crate::fed::send_federated_reply(fed_state, ret_host, &envelope).await {
        Ok(_) => "pushed",
        Err(AppError::Gone { .. }) => "sender_gone",
        Err(_) => "push_failed",
    }
}

/// `caller` replies to `message_id`, a message it received. Every reply is stored; it is
/// also pushed to the original sender when that sender has an inbox (A10, A-2).
pub async fn reply(
    state: &AppState,
    caller: &Session,
    message_id: &str,
    text: &str,
) -> Result<ReplyRecord, AppError> {
    if message_id.is_empty() || text.is_empty() {
        return Err(AppError::BadRequest(
            "message_id and text are required".to_string(),
        ));
    }
    if text.len() > state.max_body {
        return Err(AppError::PayloadTooLarge {
            size: text.len(),
            limit: state.max_body,
        });
    }
    let (msg, is_forwarded) = {
        let db = state.db.lock().map_err(db_err)?;
        let msg = storage::get_message(&db, message_id)
            .map_err(db_err)?
            .ok_or_else(|| AppError::NotFound(format!("message '{message_id}'")))?;
        let fwd = storage::get_forwarded_message(&db, message_id)
            .map_err(db_err)?
            .is_some();
        (msg, fwd)
    };
    // Reply rights are keyed on the receiving instance ID (A5).
    if caller.harness != msg.recipient_harness || caller.session_id != msg.session_id {
        return Err(AppError::NotRecipient(format!(
            "'{}' is not the recipient of message '{message_id}'",
            caller.session_id
        )));
    }

    let reply_id = ulid::Ulid::new().to_string();
    let remote_host = msg
        .return_host
        .as_deref()
        .filter(|h| !h.is_empty() && *h != state.node_name);
    // A leaf (or other undialable) origin pulls its replies; nothing is pushed to it.
    let to_leaf = remote_host.is_some_and(|h| {
        state
            .fed_state
            .as_ref()
            .and_then(|fs| fs.peers.get(h))
            .is_some_and(|p| p.leaf)
    });
    let has_inbox = msg.return_harness.is_some() && msg.return_session_id.is_some();

    // A reply always travels back to a remote origin node, which decides how it reaches
    // the sender (A-2); a local one is pushed when the sender has an inbox (A10).
    let push_outcome = match remote_host {
        Some(_) if to_leaf => Some("stored"),
        Some(host) => {
            Some(push_federated_reply(state, caller, host, message_id, &reply_id, text).await)
        }
        None if !has_inbox || is_forwarded => None,
        None => {
            let ret_harness = msg.return_harness.as_deref().unwrap_or_default();
            let ret_session_id = msg.return_session_id.as_deref().unwrap_or_default();
            let replier_label = inbox::sender_label(
                &state.node_name,
                &caller.harness,
                &caller.session_id,
                caller.name.as_deref(),
            );
            let replier_origin = Sender::Session(caller.clone()).origin(&state.node_name);
            let outcome = push_reply(
                &state.local(),
                ret_harness,
                ret_session_id,
                message_id,
                &reply_id,
                &replier_label,
                replier_origin,
                text,
            )
            .await;
            if outcome == "pushed" {
                // The pushed reply is itself a message its receiver may answer.
                let pushed = MessageRecord {
                    id: reply_id.clone(),
                    created_at: storage::now_epoch_secs(),
                    session_id: ret_session_id.to_string(),
                    from_name: replier_label,
                    bytes: text.len(),
                    outcome: "delivered".to_string(),
                    recipient_harness: ret_harness.to_string(),
                    return_harness: Some(caller.harness.clone()),
                    return_session_id: Some(caller.session_id.clone()),
                    thread_id: if msg.thread_id.is_empty() {
                        msg.id.clone()
                    } else {
                        msg.thread_id.clone()
                    },
                    return_host: None,
                };
                if let Ok(db) = state.db.lock() {
                    let _ = storage::insert_message(&db, &pushed);
                }
            }
            Some(outcome)
        }
    };
    // The pushed copy's ID: a local push that landed, or the envelope a remote origin
    // received (which deduplicates by it).
    let pushed_id = match (remote_host, push_outcome) {
        (Some(_), Some("stored")) | (_, None) => None,
        (Some(_), Some(_)) | (None, Some("pushed")) => Some(reply_id.as_str()),
        (None, Some(_)) => None,
    };

    let record = {
        let db = state.db.lock().map_err(db_err)?;
        let record = storage::insert_reply(
            &db,
            message_id,
            &caller.session_id,
            text,
            push_outcome,
            pushed_id,
        )
        .map_err(db_err)?;
        let _ = storage::purge_replies(&db, state.reply_ttl.as_secs());
        let _ = storage::purge_messages(&db, state.reply_ttl.as_secs());
        record
    };
    let _ = state.notify_tx.send(message_id.to_string());
    Ok(record)
}

// -----------------------------------------------------------------------------
// replies
// -----------------------------------------------------------------------------

/// The stored replies to `message_id` after `after`, waiting up to `wait` seconds (capped
/// at `MAX_WAIT_SECS`) for the first (A10, A-12). Anyone holding the message ID may read
/// them (A15).
pub async fn replies(
    state: &AppState,
    message_id: &str,
    after: i64,
    wait: u64,
) -> Result<Vec<ReplyRecord>, AppError> {
    let wait_secs = wait.min(MAX_WAIT_SECS);
    let outbound = {
        let db = state.db.lock().map_err(db_err)?;
        let msg = storage::get_message(&db, message_id).map_err(db_err)?;
        let out = storage::get_outbound(&db, message_id).map_err(db_err)?;
        if msg.is_none() && out.is_none() {
            return Err(AppError::NotFound(format!("message '{message_id}'")));
        }
        out
    };

    let _permit = if wait_secs > 0 {
        match state.long_poll_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(permit),
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(AppError::ServiceUnavailable(
                    "too many concurrent long-poll requests".to_string(),
                ));
            }
            Err(e) => return Err(AppError::Internal(e.to_string())),
        }
    } else {
        None
    };

    let stored = |state: &AppState| -> Result<Vec<ReplyRecord>, AppError> {
        let db = state.db.lock().map_err(db_err)?;
        storage::get_replies_after(&db, message_id, after).map_err(db_err)
    };

    // A message sent to a dialable peer: its replies may still be there.
    if let (Some(outbound), Some(fed_state)) = (outbound, state.fed_state.as_ref()) {
        let peer_is_leaf = fed_state
            .peers
            .get(&outbound.peer)
            .map(|p| p.leaf)
            .unwrap_or(false);
        if !peer_is_leaf {
            let existing = stored(state)?;
            if !existing.is_empty() {
                return Ok(existing);
            }
            let remote = crate::fed::poll_federated_replies(
                fed_state,
                &outbound.peer,
                message_id,
                after,
                wait_secs,
            )
            .await?;
            let db = state.db.lock().map_err(db_err)?;
            let known = storage::get_replies_after(&db, message_id, 0).map_err(db_err)?;
            for r in &remote {
                if !known
                    .iter()
                    .any(|ex| ex.text == r.text && ex.replier_session_id == r.replier_session_id)
                {
                    let _ = storage::insert_reply(
                        &db,
                        message_id,
                        &r.replier_session_id,
                        &r.text,
                        r.push_outcome.as_deref(),
                        r.pushed_message_id.as_deref(),
                    );
                }
            }
            return Ok(remote);
        }
    }

    let rx = (wait_secs > 0).then(|| state.notify_tx.subscribe());
    let existing = stored(state)?;
    let Some(mut receiver) = rx.filter(|_| existing.is_empty()) else {
        return Ok(existing);
    };
    let _ = tokio::time::timeout(Duration::from_secs(wait_secs), async {
        loop {
            match receiver.recv().await {
                Ok(id) if id == message_id => break,
                Err(_) => break,
                _ => {}
            }
        }
    })
    .await;
    stored(state)
}
