use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MessageRecord {
    pub id: String,
    pub created_at: i64,
    pub session_id: String,
    pub from_name: String,
    pub bytes: usize,
    pub outcome: String,
    #[serde(default = "default_recipient_harness")]
    pub recipient_harness: String,
    #[serde(default)]
    pub return_harness: Option<String>,
    #[serde(default)]
    pub return_session_id: Option<String>,
    #[serde(default)]
    pub thread_id: String,
    #[serde(default)]
    pub return_host: Option<String>,
}

fn default_recipient_harness() -> String {
    "claude".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReplyRecord {
    pub seq: i64,
    pub message_id: String,
    pub created_at: i64,
    pub replier_session_id: String,
    pub text: String,
    #[serde(default)]
    pub push_outcome: Option<String>,
    #[serde(default)]
    pub pushed_message_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PiPendingMessage {
    pub id: String,
    pub session_id: String,
    pub created_at: i64,
    pub from_name: String,
    pub bytes: usize,
    pub text: String,
    pub envelope: String,
    pub delivered_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AgyPendingMessage {
    pub id: String,
    pub session_id: String,
    pub created_at: i64,
    pub from_name: String,
    pub bytes: usize,
    pub text: String,
    pub envelope: String,
    pub delivered_at: Option<i64>,
}

/// Where a message came from, one shape for every sender (A12): an instance's `id` or a
/// service's name, the `node` that attests it, and the sender's own `label`. An anonymous
/// sender has neither `id` nor `service`; its `label` is its unattested `from`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    pub node: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl Origin {
    pub fn instance(id: &str, node: &str, label: Option<&str>) -> Origin {
        Origin {
            id: Some(id.to_string()),
            service: None,
            node: node.to_string(),
            label: label.map(str::to_string),
        }
    }

    pub fn service(name: &str, node: &str) -> Origin {
        Origin {
            id: None,
            service: Some(name.to_string()),
            node: node.to_string(),
            label: None,
        }
    }

    pub fn anonymous(from: &str, node: &str) -> Origin {
        Origin {
            id: None,
            service: None,
            node: node.to_string(),
            label: Some(from.to_string()),
        }
    }

    /// A12's one render, with `node` shown as `shown_node` (a reader's nickname for it):
    /// `name@node` for a service, `label · id@node` for an instance, and
    /// `<from> · anon@node` for an anonymous sender.
    pub fn render(&self, shown_node: &str) -> String {
        match (&self.service, &self.id) {
            (Some(name), _) => crate::inbox::sender_label(shown_node, "svc", name, None),
            (None, Some(id)) => {
                crate::inbox::sender_label(shown_node, "", id, self.label.as_deref())
            }
            (None, None) => {
                let from = self
                    .label
                    .as_deref()
                    .and_then(crate::inbox::clean_label)
                    .unwrap_or_default();
                format!("{from} · anon@{shown_node}")
            }
        }
    }

    /// The sender's attested key: its instance ID or service name, `None` when anonymous.
    pub fn key(&self) -> Option<&str> {
        self.service.as_deref().or(self.id.as_deref())
    }
}

fn origin_from_sql(s: String) -> Result<Origin> {
    serde_json::from_str(&s).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SvcPendingMessage {
    pub id: String,
    pub session_id: String,
    pub created_at: i64,
    pub from_name: String,
    pub bytes: usize,
    pub text: String,
    pub envelope: String,
    pub delivered_at: Option<i64>,
    pub origin: Origin,
    /// The message this one answers, when it is a pushed reply (A10).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct IdempotencyRecord {
    pub principal: String,
    pub key: String,
    pub body: String,
    pub message_id: String,
    pub session_id: String,
    pub from_name: String,
    pub bytes: usize,
    pub outcome: String,
    pub created_at: i64,
}

pub fn now_epoch_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Schema N: the first schema that records its version (`PRAGMA user_version`).
/// Neither the deployed `ee1b6ca` nor any build before it set one, so they read 0.
pub const SCHEMA_VERSION: i64 = 2;

/// The tables that hold message state. A database below schema N has them in an
/// older shape, and the migration drops them with their rows (limited release).
/// `replies` references `messages`, so it is dropped first: with foreign keys on, dropping
/// a parent that still has child rows fails.
const MESSAGE_STATE_TABLES: &[&str] = &[
    "replies",
    "messages",
    "outbound",
    "pi_pending_messages",
    "agy_pending_messages",
    "svc_pending_messages",
    "idempotency_keys",
    "peer_mailbox",
    "forwarded_messages",
    "instance_queue",
];

const SCHEMA_N: &str = r#"
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            recipient_harness TEXT NOT NULL DEFAULT 'claude',
            return_harness TEXT,
            return_session_id TEXT,
            thread_id TEXT NOT NULL DEFAULT '',
            return_host TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS outbound (
            id TEXT PRIMARY KEY,
            peer TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            outcome TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_outbound_created_at ON outbound(created_at);

        CREATE TABLE IF NOT EXISTS replies (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL REFERENCES messages(id),
            created_at INTEGER NOT NULL,
            replier_session_id TEXT NOT NULL,
            text TEXT NOT NULL,
            push_outcome TEXT,
            pushed_message_id TEXT
        );

        CREATE TABLE IF NOT EXISTS pi_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS agy_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER
        );

        CREATE TABLE IF NOT EXISTS svc_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER,
            origin TEXT NOT NULL,
            in_reply_to TEXT
        );

        CREATE TABLE IF NOT EXISTS idempotency_keys (
            principal TEXT NOT NULL,
            key TEXT NOT NULL,
            body TEXT NOT NULL,
            message_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (principal, key)
        );

        CREATE INDEX IF NOT EXISTS idx_replies_message_seq ON replies(message_id, seq);
        CREATE INDEX IF NOT EXISTS idx_replies_created_at ON replies(created_at);
        CREATE INDEX IF NOT EXISTS idx_pi_pending_session ON pi_pending_messages(session_id, delivered_at);
        CREATE INDEX IF NOT EXISTS idx_pi_pending_created_at ON pi_pending_messages(created_at);
        CREATE INDEX IF NOT EXISTS idx_agy_pending_session ON agy_pending_messages(session_id, delivered_at);
        CREATE INDEX IF NOT EXISTS idx_agy_pending_created_at ON agy_pending_messages(created_at);
        CREATE INDEX IF NOT EXISTS idx_svc_pending_session ON svc_pending_messages(session_id, delivered_at);
        CREATE INDEX IF NOT EXISTS idx_svc_pending_created_at ON svc_pending_messages(created_at);
        CREATE INDEX IF NOT EXISTS idx_idempotency_created_at ON idempotency_keys(created_at);

        CREATE TABLE IF NOT EXISTS peer_mailbox (
            id TEXT PRIMARY KEY,
            peer TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            body TEXT NOT NULL,
            envelope TEXT NOT NULL,
            origin TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_peer_mailbox_peer_created ON peer_mailbox(peer, created_at);

        CREATE TABLE IF NOT EXISTS forwarded_messages (
            id TEXT PRIMARY KEY,
            origin_peer TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_forwarded_messages_created_at ON forwarded_messages(created_at);

        CREATE TABLE IF NOT EXISTS instance_claims (
            id TEXT NOT NULL,
            pid INTEGER NOT NULL,
            proc_start TEXT NOT NULL,
            suffix INTEGER NOT NULL,
            released INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (id, pid, proc_start)
        );
        CREATE INDEX IF NOT EXISTS idx_instance_claims_id ON instance_claims(id);
        CREATE INDEX IF NOT EXISTS idx_instance_claims_proc ON instance_claims(pid, proc_start);

        CREATE TABLE IF NOT EXISTS instance_queue (
            id TEXT PRIMARY KEY,
            instance_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            body TEXT NOT NULL,
            delivered_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_instance_queue_instance ON instance_queue(instance_id, delivered_at);

        CREATE TABLE IF NOT EXISTS instance_seen (
            id TEXT PRIMARY KEY,
            harness TEXT NOT NULL,
            last_live INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fork_notices (
            id TEXT NOT NULL,
            pid1 INTEGER NOT NULL,
            proc_start1 TEXT NOT NULL,
            pid2 INTEGER NOT NULL,
            proc_start2 TEXT NOT NULL,
            recipient_pid INTEGER NOT NULL,
            recipient_proc_start TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY (id, pid1, proc_start1, pid2, proc_start2, recipient_pid, recipient_proc_start)
        );
        "#;

/// Opens a database at schema N. A database already at N keeps its rows, and gains any
/// table schema N has since been extended with in place (plan §6 D12); one below N (the
/// deployed `ee1b6ca` schema, or none at all) has its message state dropped and is created
/// at N in one transaction; one above N is refused.
pub fn init_db(conn: &Connection) -> Result<()> {
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    if version == SCHEMA_VERSION {
        return conn.execute_batch(SCHEMA_N);
    }
    if version > SCHEMA_VERSION {
        return Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISMATCH),
            Some(format!(
                "database schema version {version} is newer than this xmsg (schema {SCHEMA_VERSION})"
            )),
        ));
    }
    let tx = conn.unchecked_transaction()?;
    for table in MESSAGE_STATE_TABLES {
        tx.execute_batch(&format!("DROP TABLE IF EXISTS {table};"))?;
    }
    tx.execute_batch(SCHEMA_N)?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
    tx.commit()
}

pub fn insert_message(conn: &Connection, msg: &MessageRecord) -> Result<()> {
    conn.execute(
        "INSERT INTO messages (id, created_at, session_id, from_name, bytes, outcome, recipient_harness, return_harness, return_session_id, thread_id, return_host) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        params![
            msg.id,
            msg.created_at,
            msg.session_id,
            msg.from_name,
            msg.bytes as i64,
            msg.outcome,
            msg.recipient_harness,
            msg.return_harness,
            msg.return_session_id,
            msg.thread_id,
            msg.return_host.as_deref().unwrap_or(""),
        ],
    )?;
    Ok(())
}

pub fn get_message(conn: &Connection, id: &str) -> Result<Option<MessageRecord>> {
    let mut stmt = conn.prepare(
        "SELECT id, created_at, session_id, from_name, bytes, outcome, recipient_harness, return_harness, return_session_id, thread_id, return_host FROM messages WHERE id = ?1",
    )?;
    let mut rows = stmt.query(params![id])?;

    if let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(4)?;
        let ret_host: String = row.get(10)?;
        let return_host = if ret_host.is_empty() {
            None
        } else {
            Some(ret_host)
        };
        Ok(Some(MessageRecord {
            id: row.get(0)?,
            created_at: row.get(1)?,
            session_id: row.get(2)?,
            from_name: row.get(3)?,
            bytes: bytes_i64 as usize,
            outcome: row.get(5)?,
            recipient_harness: row.get(6)?,
            return_harness: row.get(7)?,
            return_session_id: row.get(8)?,
            thread_id: row.get(9)?,
            return_host,
        }))
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OutboundRecord {
    pub id: String,
    pub peer: String,
    pub target_ref: String,
    pub outcome: String,
    pub created_at: i64,
}

pub fn insert_outbound(
    conn: &Connection,
    id: &str,
    peer: &str,
    target_ref: &str,
    outcome: &str,
    created_at: i64,
) -> Result<()> {
    conn.execute(
        "INSERT INTO outbound (id, peer, target_ref, outcome, created_at) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![id, peer, target_ref, outcome, created_at],
    )?;
    Ok(())
}

pub fn get_outbound(conn: &Connection, id: &str) -> Result<Option<OutboundRecord>> {
    let mut stmt = conn
        .prepare("SELECT id, peer, target_ref, outcome, created_at FROM outbound WHERE id = ?1")?;
    let mut rows = stmt.query(params![id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(OutboundRecord {
            id: row.get(0)?,
            peer: row.get(1)?,
            target_ref: row.get(2)?,
            outcome: row.get(3)?,
            created_at: row.get(4)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn update_outbound_outcome(conn: &Connection, id: &str, outcome: &str) -> Result<()> {
    conn.execute(
        "UPDATE outbound SET outcome = ?1 WHERE id = ?2",
        params![outcome, id],
    )?;
    Ok(())
}

pub fn update_outbound_target(conn: &Connection, id: &str, target_ref: &str) -> Result<()> {
    conn.execute(
        "UPDATE outbound SET target_ref = ?1 WHERE id = ?2",
        params![target_ref, id],
    )?;
    Ok(())
}

pub fn delete_outbound(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM outbound WHERE id = ?1", params![id])?;
    Ok(())
}

pub fn insert_reply(
    conn: &Connection,
    message_id: &str,
    replier_session_id: &str,
    text: &str,
    push_outcome: Option<&str>,
    pushed_message_id: Option<&str>,
) -> Result<ReplyRecord> {
    let created_at = now_epoch_secs();
    conn.execute(
        "INSERT INTO replies (message_id, created_at, replier_session_id, text, push_outcome, pushed_message_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            message_id,
            created_at,
            replier_session_id,
            text,
            push_outcome,
            pushed_message_id
        ],
    )?;
    let seq = conn.last_insert_rowid();

    Ok(ReplyRecord {
        seq,
        message_id: message_id.to_string(),
        created_at,
        replier_session_id: replier_session_id.to_string(),
        text: text.to_string(),
        push_outcome: push_outcome.map(|s| s.to_string()),
        pushed_message_id: pushed_message_id.map(|s| s.to_string()),
    })
}

pub fn get_replies_after(
    conn: &Connection,
    message_id: &str,
    after_seq: i64,
) -> Result<Vec<ReplyRecord>> {
    let mut stmt = conn.prepare(
        "SELECT seq, message_id, created_at, replier_session_id, text, push_outcome, pushed_message_id FROM replies WHERE message_id = ?1 AND seq > ?2 ORDER BY seq ASC",
    )?;
    let rows = stmt.query_map(params![message_id, after_seq], |row| {
        Ok(ReplyRecord {
            seq: row.get(0)?,
            message_id: row.get(1)?,
            created_at: row.get(2)?,
            replier_session_id: row.get(3)?,
            text: row.get(4)?,
            push_outcome: row.get(5)?,
            pushed_message_id: row.get(6)?,
        })
    })?;

    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

pub fn get_all_replies(conn: &Connection, message_id: &str) -> Result<Vec<ReplyRecord>> {
    get_replies_after(conn, message_id, 0)
}

pub fn get_reply_by_pushed_id(
    conn: &Connection,
    pushed_message_id: &str,
) -> Result<Option<ReplyRecord>> {
    let mut stmt = conn.prepare(
        "SELECT seq, message_id, created_at, replier_session_id, text, push_outcome, pushed_message_id FROM replies WHERE pushed_message_id = ?1 LIMIT 1",
    )?;
    let mut rows = stmt.query(params![pushed_message_id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(ReplyRecord {
            seq: row.get(0)?,
            message_id: row.get(1)?,
            created_at: row.get(2)?,
            replier_session_id: row.get(3)?,
            text: row.get(4)?,
            push_outcome: row.get(5)?,
            pushed_message_id: row.get(6)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn update_reply_push_outcome(
    conn: &Connection,
    pushed_message_id: &str,
    push_outcome: &str,
) -> Result<bool> {
    let count = conn.execute(
        "UPDATE replies SET push_outcome = ?1 WHERE pushed_message_id = ?2",
        params![push_outcome, pushed_message_id],
    )?;
    Ok(count > 0)
}

pub fn purge_replies(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let cutoff = now_epoch_secs() - (ttl_secs as i64);
    conn.execute("DELETE FROM replies WHERE created_at < ?1", params![cutoff])
}

pub fn purge_messages(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let cutoff = now_epoch_secs() - (ttl_secs as i64);
    conn.execute(
        "DELETE FROM messages WHERE created_at < ?1",
        params![cutoff],
    )
}

pub const MAX_PI_QUEUE_PER_SESSION: usize = 100;

pub fn count_pending_pi_messages(conn: &Connection, session_id: &str) -> Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM pi_pending_messages WHERE session_id = ?1 AND delivered_at IS NULL",
    )?;
    let count: i64 = stmt.query_row(params![session_id], |row| row.get(0))?;
    Ok(count as usize)
}

pub fn insert_pi_message(conn: &Connection, msg: &PiPendingMessage) -> Result<bool> {
    let pending = count_pending_pi_messages(conn, &msg.session_id)?;
    if pending >= MAX_PI_QUEUE_PER_SESSION {
        return Ok(false);
    }
    conn.execute(
        "INSERT INTO pi_pending_messages (id, session_id, created_at, from_name, bytes, text, envelope, delivered_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            msg.id,
            msg.session_id,
            msg.created_at,
            msg.from_name,
            msg.bytes as i64,
            msg.text,
            msg.envelope,
            msg.delivered_at,
        ],
    )?;
    Ok(true)
}

pub fn get_next_pending_pi_message(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<PiPendingMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, session_id, created_at, from_name, bytes, text, envelope, delivered_at FROM pi_pending_messages WHERE session_id = ?1 AND delivered_at IS NULL ORDER BY created_at ASC, rowid ASC LIMIT 1",
    )?;
    let mut rows = stmt.query(params![session_id])?;

    if let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(4)?;
        Ok(Some(PiPendingMessage {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at: row.get(2)?,
            from_name: row.get(3)?,
            bytes: bytes_i64 as usize,
            text: row.get(5)?,
            envelope: row.get(6)?,
            delivered_at: row.get(7)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn ack_pi_message(conn: &Connection, session_id: &str, message_id: &str) -> Result<bool> {
    let now = now_epoch_secs();
    let count = conn.execute(
        "UPDATE pi_pending_messages SET delivered_at = ?1 WHERE id = ?2 AND session_id = ?3 AND delivered_at IS NULL",
        params![now, message_id, session_id],
    )?;
    Ok(count > 0)
}

pub fn purge_pi_messages(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let cutoff = now_epoch_secs() - (ttl_secs as i64);
    conn.execute(
        "DELETE FROM pi_pending_messages WHERE created_at < ?1",
        params![cutoff],
    )
}

pub fn insert_agy_message(conn: &Connection, msg: &AgyPendingMessage) -> Result<bool> {
    conn.execute(
        "INSERT INTO agy_pending_messages (id, session_id, created_at, from_name, bytes, text, envelope, delivered_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            msg.id,
            msg.session_id,
            msg.created_at,
            msg.from_name,
            msg.bytes as i64,
            msg.text,
            msg.envelope,
            msg.delivered_at,
        ],
    )?;
    Ok(true)
}

pub fn fetch_undelivered_agy_messages(
    conn: &Connection,
    session_keys: &[&str],
) -> Result<Vec<AgyPendingMessage>> {
    if session_keys.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders: Vec<String> = (1..=session_keys.len()).map(|i| format!("?{i}")).collect();
    let query_str = format!(
        "SELECT id, session_id, created_at, from_name, bytes, text, envelope, delivered_at FROM agy_pending_messages WHERE session_id IN ({}) AND delivered_at IS NULL ORDER BY created_at ASC, rowid ASC",
        placeholders.join(", ")
    );
    let mut stmt = conn.prepare(&query_str)?;
    let rusqlite_params: Vec<&dyn rusqlite::ToSql> = session_keys
        .iter()
        .map(|s| s as &dyn rusqlite::ToSql)
        .collect();
    let mut rows = stmt.query(rusqlite_params.as_slice())?;

    let mut result = Vec::new();
    while let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(4)?;
        result.push(AgyPendingMessage {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at: row.get(2)?,
            from_name: row.get(3)?,
            bytes: bytes_i64 as usize,
            text: row.get(5)?,
            envelope: row.get(6)?,
            delivered_at: row.get(7)?,
        });
    }
    Ok(result)
}

pub fn mark_agy_message_delivered(conn: &Connection, id: &str, delivered_at: i64) -> Result<bool> {
    let count = conn.execute(
        "UPDATE agy_pending_messages SET delivered_at = ?1 WHERE id = ?2 AND delivered_at IS NULL",
        params![delivered_at, id],
    )?;
    Ok(count > 0)
}

pub fn update_message_outcome(conn: &Connection, id: &str, outcome: &str) -> Result<bool> {
    let count = conn.execute(
        "UPDATE messages SET outcome = ?1 WHERE id = ?2",
        params![outcome, id],
    )?;
    Ok(count > 0)
}

pub fn purge_agy_messages(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let cutoff = now_epoch_secs() - (ttl_secs as i64);
    conn.execute(
        "DELETE FROM agy_pending_messages WHERE created_at < ?1",
        params![cutoff],
    )
}

pub const MAX_SVC_QUEUE_PER_SESSION: usize = 100;

pub fn count_pending_svc_messages(conn: &Connection, session_id: &str) -> Result<usize> {
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM svc_pending_messages WHERE session_id = ?1 AND delivered_at IS NULL",
    )?;
    let count: i64 = stmt.query_row(params![session_id], |row| row.get(0))?;
    Ok(count as usize)
}

pub fn insert_svc_message(conn: &Connection, msg: &SvcPendingMessage) -> Result<bool> {
    let pending = count_pending_svc_messages(conn, &msg.session_id)?;
    if pending >= MAX_SVC_QUEUE_PER_SESSION {
        return Ok(false);
    }
    let origin_json = serde_json::to_string(&msg.origin)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    conn.execute(
        "INSERT INTO svc_pending_messages (id, session_id, created_at, from_name, bytes, text, envelope, delivered_at, origin, in_reply_to) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            msg.id,
            msg.session_id,
            msg.created_at,
            msg.from_name,
            msg.bytes as i64,
            msg.text,
            msg.envelope,
            msg.delivered_at,
            origin_json,
            msg.in_reply_to,
        ],
    )?;
    Ok(true)
}

pub fn get_next_pending_svc_message(
    conn: &Connection,
    session_id: &str,
) -> Result<Option<SvcPendingMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, session_id, created_at, from_name, bytes, text, envelope, delivered_at, origin, in_reply_to FROM svc_pending_messages WHERE session_id = ?1 AND delivered_at IS NULL ORDER BY created_at ASC, rowid ASC LIMIT 1",
    )?;
    let mut rows = stmt.query(params![session_id])?;

    if let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(4)?;
        let origin = origin_from_sql(row.get(8)?)?;
        Ok(Some(SvcPendingMessage {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at: row.get(2)?,
            from_name: row.get(3)?,
            bytes: bytes_i64 as usize,
            text: row.get(5)?,
            envelope: row.get(6)?,
            delivered_at: row.get(7)?,
            origin,
            in_reply_to: row.get(9)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn ack_svc_message(conn: &Connection, session_id: &str, message_id: &str) -> Result<bool> {
    let now = now_epoch_secs();
    let count = conn.execute(
        "UPDATE svc_pending_messages SET delivered_at = ?1 WHERE id = ?2 AND session_id = ?3 AND delivered_at IS NULL",
        params![now, message_id, session_id],
    )?;
    Ok(count > 0)
}

pub fn purge_svc_messages(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let cutoff = now_epoch_secs() - (ttl_secs as i64);
    conn.execute(
        "DELETE FROM svc_pending_messages WHERE created_at < ?1",
        params![cutoff],
    )
}

pub fn get_idempotency_record(
    conn: &Connection,
    principal: &str,
    key: &str,
    ttl_secs: u64,
) -> Result<Option<IdempotencyRecord>> {
    let now = now_epoch_secs();
    let cutoff = now.saturating_sub(ttl_secs as i64);
    let mut stmt = conn.prepare(
        "SELECT principal, key, body, message_id, session_id, from_name, bytes, outcome, created_at
         FROM idempotency_keys
         WHERE principal = ?1 AND key = ?2 AND created_at >= ?3",
    )?;
    let mut rows = stmt.query(params![principal, key, cutoff])?;
    if let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(6)?;
        Ok(Some(IdempotencyRecord {
            principal: row.get(0)?,
            key: row.get(1)?,
            body: row.get(2)?,
            message_id: row.get(3)?,
            session_id: row.get(4)?,
            from_name: row.get(5)?,
            bytes: bytes_i64 as usize,
            outcome: row.get(7)?,
            created_at: row.get(8)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn insert_idempotency_record(conn: &Connection, record: &IdempotencyRecord) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO idempotency_keys
         (principal, key, body, message_id, session_id, from_name, bytes, outcome, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            record.principal,
            record.key,
            record.body,
            record.message_id,
            record.session_id,
            record.from_name,
            record.bytes as i64,
            record.outcome,
            record.created_at,
        ],
    )?;
    Ok(())
}

pub fn purge_idempotency_keys(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let now = now_epoch_secs();
    let cutoff = now.saturating_sub(ttl_secs as i64);
    conn.execute(
        "DELETE FROM idempotency_keys WHERE created_at < ?1",
        params![cutoff],
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PeerMailboxMessage {
    pub id: String,
    pub peer: String,
    pub target_ref: String,
    pub created_at: i64,
    pub from_name: String,
    pub bytes: usize,
    pub body: String,
    pub envelope: String,
    pub origin: Origin,
}

pub fn insert_peer_mailbox(conn: &Connection, msg: &PeerMailboxMessage) -> Result<bool> {
    let origin_json = serde_json::to_string(&msg.origin)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    conn.execute(
        "INSERT INTO peer_mailbox (id, peer, target_ref, created_at, from_name, bytes, body, envelope, origin)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            msg.id,
            msg.peer,
            msg.target_ref,
            msg.created_at,
            msg.from_name,
            msg.bytes as i64,
            msg.body,
            msg.envelope,
            origin_json,
        ],
    )?;
    Ok(true)
}

pub fn get_next_peer_mailbox_message(
    conn: &Connection,
    peer: &str,
) -> Result<Option<PeerMailboxMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, peer, target_ref, created_at, from_name, bytes, body, envelope, origin
         FROM peer_mailbox
         WHERE peer = ?1
         ORDER BY created_at ASC, rowid ASC
         LIMIT 1",
    )?;
    let mut rows = stmt.query(params![peer])?;
    if let Some(row) = rows.next()? {
        let bytes_i64: i64 = row.get(5)?;
        let origin = origin_from_sql(row.get(8)?)?;
        Ok(Some(PeerMailboxMessage {
            id: row.get(0)?,
            peer: row.get(1)?,
            target_ref: row.get(2)?,
            created_at: row.get(3)?,
            from_name: row.get(4)?,
            bytes: bytes_i64 as usize,
            body: row.get(6)?,
            envelope: row.get(7)?,
            origin,
        }))
    } else {
        Ok(None)
    }
}

pub fn delete_peer_mailbox_message(conn: &Connection, peer: &str, id: &str) -> Result<bool> {
    let count = conn.execute(
        "DELETE FROM peer_mailbox WHERE peer = ?1 AND id = ?2",
        params![peer, id],
    )?;
    Ok(count > 0)
}

pub fn count_peer_mailbox_messages(conn: &Connection, peer: &str) -> Result<usize> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM peer_mailbox WHERE peer = ?1")?;
    let count: i64 = stmt.query_row(params![peer], |row| row.get(0))?;
    Ok(count as usize)
}

pub fn purge_peer_mailbox(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let now = now_epoch_secs();
    let cutoff = now.saturating_sub(ttl_secs as i64);
    conn.execute(
        "DELETE FROM peer_mailbox WHERE created_at < ?1",
        params![cutoff],
    )
}

pub fn is_svc_message_acked(conn: &Connection, id: &str) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT delivered_at FROM svc_pending_messages WHERE id = ?1 LIMIT 1")?;
    let mut rows = stmt.query(params![id])?;
    if let Some(row) = rows.next()? {
        let delivered_at: Option<i64> = row.get(0)?;
        Ok(delivered_at.is_some())
    } else {
        Ok(false)
    }
}

#[derive(Debug, Clone)]
pub struct ForwardedMessageRecord {
    pub id: String,
    pub origin_peer: String,
    pub created_at: i64,
}

pub fn insert_forwarded_message(
    conn: &Connection,
    id: &str,
    origin_peer: &str,
    created_at: i64,
) -> Result<()> {
    conn.execute(
        "INSERT OR REPLACE INTO forwarded_messages (id, origin_peer, created_at) VALUES (?1, ?2, ?3)",
        params![id, origin_peer, created_at],
    )?;
    Ok(())
}

pub fn get_forwarded_message(
    conn: &Connection,
    id: &str,
) -> Result<Option<ForwardedMessageRecord>> {
    let mut stmt =
        conn.prepare("SELECT id, origin_peer, created_at FROM forwarded_messages WHERE id = ?1")?;
    let mut rows = stmt.query(params![id])?;
    if let Some(row) = rows.next()? {
        Ok(Some(ForwardedMessageRecord {
            id: row.get(0)?,
            origin_peer: row.get(1)?,
            created_at: row.get(2)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn purge_forwarded_messages(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let now = now_epoch_secs();
    let cutoff = now.saturating_sub(ttl_secs as i64);
    conn.execute(
        "DELETE FROM forwarded_messages WHERE created_at < ?1",
        params![cutoff],
    )
}

pub fn delete_forwarded_message(conn: &Connection, id: &str) -> Result<()> {
    conn.execute("DELETE FROM forwarded_messages WHERE id = ?1", params![id])?;
    Ok(())
}

// -----------------------------------------------------------------------------
// A14's local queue: instances observed before and not running now
// -----------------------------------------------------------------------------

/// A message held for a Claude instance that is not running (A14). `body` is what its inbox
/// receives, the send time included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedMessage {
    pub id: String,
    pub instance_id: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub from_name: String,
    pub body: String,
}

pub fn insert_queued(conn: &Connection, msg: &QueuedMessage) -> Result<()> {
    conn.execute(
        "INSERT INTO instance_queue (id, instance_id, created_at, expires_at, from_name, body) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![msg.id, msg.instance_id, msg.created_at, msg.expires_at, msg.from_name, msg.body],
    )?;
    Ok(())
}

/// Takes the unexpired, undelivered messages queued for `instance_id`, oldest first, and
/// marks them delivered, so a later observation does not deliver them again.
pub fn take_queued(conn: &Connection, instance_id: &str, now: i64) -> Result<Vec<QueuedMessage>> {
    let mut stmt = conn.prepare(
        "SELECT id, instance_id, created_at, expires_at, from_name, body FROM instance_queue WHERE instance_id = ?1 AND delivered_at IS NULL AND expires_at > ?2 ORDER BY created_at ASC, id ASC",
    )?;
    let msgs = stmt
        .query_map(params![instance_id, now], |row| {
            Ok(QueuedMessage {
                id: row.get(0)?,
                instance_id: row.get(1)?,
                created_at: row.get(2)?,
                expires_at: row.get(3)?,
                from_name: row.get(4)?,
                body: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>>>()?;
    for m in &msgs {
        conn.execute(
            "UPDATE instance_queue SET delivered_at = ?1 WHERE id = ?2",
            params![now, m.id],
        )?;
    }
    Ok(msgs)
}

/// Puts back a message whose delivery failed, for the next observation.
pub fn untake_queued(conn: &Connection, id: &str) -> Result<()> {
    conn.execute(
        "UPDATE instance_queue SET delivered_at = NULL WHERE id = ?1",
        params![id],
    )?;
    Ok(())
}

pub fn purge_instance_queue(conn: &Connection, ttl_secs: u64) -> Result<usize> {
    let now = now_epoch_secs();
    conn.execute(
        "DELETE FROM instance_queue WHERE expires_at <= ?1 OR (delivered_at IS NOT NULL AND delivered_at < ?2)",
        params![now, now - ttl_secs as i64],
    )
}

/// Records that instance `id` of `harness` was live at `now`: the "ever observed" record
/// A14's queue and D-1's lifetime read.
pub fn mark_live(conn: &Connection, id: &str, harness: &str, now: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO instance_seen (id, harness, last_live) VALUES (?1, ?2, ?3) ON CONFLICT(id) DO UPDATE SET harness = excluded.harness, last_live = MAX(last_live, excluded.last_live)",
        params![id, harness, now],
    )?;
    Ok(())
}

/// An instance observed before: its harness, when it was last live, and whether it has an
/// unexpired queued message at `now`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenInstance {
    pub harness: String,
    pub last_live: i64,
    pub queued: bool,
}

pub fn seen_instance(conn: &Connection, id: &str, now: i64) -> Result<Option<SeenInstance>> {
    let row = conn.query_row(
        "SELECT harness, last_live FROM instance_seen WHERE id = ?1",
        params![id],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
    );
    let (harness, last_live) = match row {
        Ok(r) => r,
        Err(rusqlite::Error::QueryReturnedNoRows) => return Ok(None),
        Err(e) => return Err(e),
    };
    let queued: i64 = conn.query_row(
        "SELECT COUNT(*) FROM instance_queue WHERE instance_id = ?1 AND delivered_at IS NULL AND expires_at > ?2",
        params![id, now],
        |r| r.get(0),
    )?;
    Ok(Some(SeenInstance {
        harness,
        last_live,
        queued: queued > 0,
    }))
}

/// `secs` since the epoch as an RFC 3339 UTC time, `2026-10-12T08:30:00Z`.
pub fn utc_rfc3339(secs: i64) -> String {
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod queue_tests {
    use super::*;

    #[test]
    fn utc_rfc3339_known_instants() {
        assert_eq!(utc_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(utc_rfc3339(1_791_801_000), "2026-10-12T10:30:00Z");
    }
}
