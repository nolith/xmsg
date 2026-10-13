use rusqlite::{params, Connection, Result};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing::warn;

use crate::error::AppError;
use crate::inbox;
use crate::process;
use crate::registry::{self, Session, SessionDirs, SessionFileEntry};
use crate::storage::{self, now_epoch_secs, MessageRecord};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceClaim {
    pub id: String,
    pub pid: u32,
    pub proc_start: String,
    pub suffix: u32,
    pub released: bool,
    pub created_at: i64,
}

impl InstanceClaim {
    pub fn address(&self) -> String {
        if self.suffix == 1 {
            self.id.clone()
        } else {
            format!("{}~{}", self.id, self.suffix)
        }
    }
}

pub fn cmp_proc_start(a: &str, b: &str) -> Ordering {
    match (a.parse::<u64>(), b.parse::<u64>()) {
        (Ok(n1), Ok(n2)) => n1.cmp(&n2),
        _ => a.cmp(b),
    }
}

pub fn is_uuid_shaped(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (i, &b) in bytes.iter().enumerate() {
        if i == 8 || i == 13 || i == 18 || i == 23 {
            if b != b'-' {
                return false;
            }
        } else if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

pub fn mint_uuid_v4() -> String {
    let mut bytes = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        use std::io::Read;
        let _ = f.read_exact(&mut bytes);
    } else {
        let u = ulid::Ulid::new();
        let ub = u.to_bytes();
        bytes.copy_from_slice(&ub);
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6], bytes[7],
        bytes[8], bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

pub fn validate_service_name(name: &str) -> Result<(), AppError> {
    if name == "xmsg" {
        return Err(AppError::BadRequest(
            "service name 'xmsg' is reserved".to_string(),
        ));
    }
    if name.contains('@') || name.contains('·') || name.contains('~') || name.contains('.') {
        return Err(AppError::BadRequest(format!(
            "service name '{name}' contains forbidden character (@, ·, ~, or .)"
        )));
    }
    if is_uuid_shaped(name) {
        return Err(AppError::BadRequest(format!(
            "service name '{name}' is UUID-shaped"
        )));
    }
    Ok(())
}

pub fn get_active_claim_for_proc(
    conn: &Connection,
    pid: u32,
    proc_start: &str,
) -> Result<Option<InstanceClaim>> {
    let mut stmt = conn.prepare(
        "SELECT id, pid, proc_start, suffix, released, created_at FROM instance_claims WHERE pid = ?1 AND proc_start = ?2 AND released = 0 LIMIT 1",
    )?;
    let mut rows = stmt.query(params![pid, proc_start])?;
    if let Some(row) = rows.next()? {
        Ok(Some(InstanceClaim {
            id: row.get(0)?,
            pid: row.get(1)?,
            proc_start: row.get(2)?,
            suffix: row.get(3)?,
            released: row.get::<_, i64>(4)? != 0,
            created_at: row.get(5)?,
        }))
    } else {
        Ok(None)
    }
}

pub fn get_active_claims_for_id(conn: &Connection, id: &str) -> Result<Vec<InstanceClaim>> {
    let mut stmt = conn.prepare(
        "SELECT id, pid, proc_start, suffix, released, created_at FROM instance_claims WHERE id = ?1 AND released = 0 ORDER BY suffix ASC",
    )?;
    let mut rows = stmt.query(params![id])?;
    let mut claims = Vec::new();
    while let Some(row) = rows.next()? {
        claims.push(InstanceClaim {
            id: row.get(0)?,
            pid: row.get(1)?,
            proc_start: row.get(2)?,
            suffix: row.get(3)?,
            released: row.get::<_, i64>(4)? != 0,
            created_at: row.get(5)?,
        });
    }
    Ok(claims)
}

pub fn get_all_claims_for_id(conn: &Connection, id: &str) -> Result<Vec<InstanceClaim>> {
    let mut stmt = conn.prepare(
        "SELECT id, pid, proc_start, suffix, released, created_at FROM instance_claims WHERE id = ?1 ORDER BY created_at ASC, suffix ASC",
    )?;
    let mut rows = stmt.query(params![id])?;
    let mut claims = Vec::new();
    while let Some(row) = rows.next()? {
        claims.push(InstanceClaim {
            id: row.get(0)?,
            pid: row.get(1)?,
            proc_start: row.get(2)?,
            suffix: row.get(3)?,
            released: row.get::<_, i64>(4)? != 0,
            created_at: row.get(5)?,
        });
    }
    Ok(claims)
}

#[derive(Debug, Clone)]
pub struct ForkNoticeToSend {
    pub id: String,
    pub pid1: u32,
    pub proc_start1: String,
    pub pid2: u32,
    pub proc_start2: String,
    pub recipient_pid: u32,
    pub recipient_proc_start: String,
    pub target_session_id: String,
    pub socket_path: PathBuf,
    pub from_name: String,
    pub text: String,
}

pub fn is_claude_proc_live(proc_root: &Path, pid: u32, expected: &str) -> bool {
    process::claude_proc_start_matches(proc_root, pid, expected)
}

#[allow(clippy::type_complexity)]
pub fn sync_claims_db(
    conn: &Connection,
    proc_root: &Path,
    sessions_dirs: &(impl SessionDirs + ?Sized),
    host_label: &str,
) -> Result<
    (
        Vec<(InstanceClaim, SessionFileEntry)>,
        Vec<ForkNoticeToSend>,
    ),
    AppError,
> {
    let raw_entries = registry::read_session_entries(sessions_dirs);
    sync_claims_entries(conn, proc_root, raw_entries, host_label)
}

/// One claim scan over session file entries in the order given. The scan
/// itself orders new observations by `(procStart, pid)`, so the result does
/// not depend on the order of `raw_entries` (which is `read_dir` order in
/// production).
#[allow(clippy::type_complexity)]
pub fn sync_claims_entries(
    conn: &Connection,
    proc_root: &Path,
    raw_entries: Vec<SessionFileEntry>,
    host_label: &str,
) -> Result<
    (
        Vec<(InstanceClaim, SessionFileEntry)>,
        Vec<ForkNoticeToSend>,
    ),
    AppError,
> {
    // One transaction per scan: an error part-way rolls back every claim and
    // every notice claim it made, so none is recorded without being returned.
    let tx = conn
        .unchecked_transaction()
        .map_err(|e| AppError::Internal(e.to_string()))?;

    // Filter to live entries where process is still alive and proc_start matches
    let live_entries: Vec<SessionFileEntry> = raw_entries
        .into_iter()
        .filter(|e| is_claude_proc_live(proc_root, e.pid, &e.proc_start))
        .collect();

    // 1. Recheck all active claims (released = 0)
    let mut stmt = conn
        .prepare("SELECT id, pid, proc_start, suffix, released, created_at FROM instance_claims WHERE released = 0")
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let active_claim_rows = stmt
        .query_map([], |row| {
            Ok(InstanceClaim {
                id: row.get(0)?,
                pid: row.get(1)?,
                proc_start: row.get(2)?,
                suffix: row.get(3)?,
                released: row.get::<_, i64>(4)? != 0,
                created_at: row.get(5)?,
            })
        })
        .map_err(|e| AppError::Internal(e.to_string()))?;

    for claim_res in active_claim_rows {
        let claim = claim_res.map_err(|e| AppError::Internal(e.to_string()))?;
        let still_presented = live_entries.iter().any(|e| {
            e.pid == claim.pid && e.proc_start == claim.proc_start && e.session_id == claim.id
        });

        if !still_presented {
            conn.execute(
                "UPDATE instance_claims SET released = 1 WHERE id = ?1 AND pid = ?2 AND proc_start = ?3",
                params![claim.id, claim.pid, claim.proc_start],
            )
            .map_err(|e| AppError::Internal(e.to_string()))?;
        }
    }

    // 2. Discover newly observed processes
    let mut new_observations_by_id: HashMap<String, Vec<SessionFileEntry>> = HashMap::new();
    for entry in &live_entries {
        let mut check_stmt = conn
            .prepare("SELECT COUNT(*) FROM instance_claims WHERE id = ?1 AND pid = ?2 AND proc_start = ?3")
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let count: i64 = check_stmt
            .query_row(
                params![entry.session_id, entry.pid, entry.proc_start],
                |r| r.get(0),
            )
            .map_err(|e| AppError::Internal(e.to_string()))?;

        if count == 0 {
            new_observations_by_id
                .entry(entry.session_id.clone())
                .or_default()
                .push(entry.clone());
        }
    }

    // 3. For each ID with new observations, sort by (procStart, pid) and assign suffixes
    let now = now_epoch_secs();
    for (id, mut entries) in new_observations_by_id {
        entries.sort_by(|a, b| {
            cmp_proc_start(&a.proc_start, &b.proc_start).then_with(|| a.pid.cmp(&b.pid))
        });

        for entry in entries {
            // Is plain ID (suffix 1) currently held by a live process?
            let mut plain_held_stmt = conn
                .prepare("SELECT COUNT(*) FROM instance_claims WHERE id = ?1 AND suffix = 1 AND released = 0")
                .map_err(|e| AppError::Internal(e.to_string()))?;
            let plain_held_count: i64 = plain_held_stmt
                .query_row(params![id], |r| r.get(0))
                .map_err(|e| AppError::Internal(e.to_string()))?;

            let suffix = if plain_held_count == 0 {
                1
            } else {
                let mut max_suffix_stmt = conn
                    .prepare("SELECT COALESCE(MAX(suffix), 1) FROM instance_claims WHERE id = ?1")
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                let max_suffix: u32 = max_suffix_stmt
                    .query_row(params![id], |r| r.get(0))
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                std::cmp::max(2, max_suffix + 1)
            };

            conn.execute(
                "INSERT INTO instance_claims (id, pid, proc_start, suffix, released, created_at) VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![id, entry.pid, entry.proc_start, suffix, now],
            )
            .map_err(|e| AppError::Internal(e.to_string()))?;
        }
    }

    // 4. Gather all currently active claims paired with their live session file entries
    let mut active_pairs = Vec::new();
    let mut claims_by_id: HashMap<String, Vec<InstanceClaim>> = HashMap::new();

    let mut stmt = conn
        .prepare("SELECT id, pid, proc_start, suffix, released, created_at FROM instance_claims WHERE released = 0 ORDER BY suffix ASC")
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let active_claim_rows = stmt
        .query_map([], |row| {
            Ok(InstanceClaim {
                id: row.get(0)?,
                pid: row.get(1)?,
                proc_start: row.get(2)?,
                suffix: row.get(3)?,
                released: row.get::<_, i64>(4)? != 0,
                created_at: row.get(5)?,
            })
        })
        .map_err(|e| AppError::Internal(e.to_string()))?;

    for claim_res in active_claim_rows {
        let claim = claim_res.map_err(|e| AppError::Internal(e.to_string()))?;
        if let Some(entry) = live_entries.iter().find(|e| {
            e.pid == claim.pid && e.proc_start == claim.proc_start && e.session_id == claim.id
        }) {
            active_pairs.push((claim.clone(), entry.clone()));
            claims_by_id
                .entry(claim.id.clone())
                .or_default()
                .push(claim);
        }
    }

    // 5. Fork notices: one per (pair, recipient process). Each notice is
    //    claimed here, inside this scan's transaction and under the caller's
    //    DB lock, so two overlapping scans cannot both send it.
    //    `deliver_fork_notices` releases the claim if delivery fails, so a
    //    later scan sends it once.
    let mut notices_to_send = Vec::new();
    for (id, claims) in &claims_by_id {
        if claims.len() < 2 {
            continue;
        }
        let plain_addr = claims
            .iter()
            .find(|c| c.suffix == 1)
            .map_or_else(|| id.clone(), |c| c.address());

        for i in 0..claims.len() {
            for j in (i + 1)..claims.len() {
                let (c1, c2) = if (claims[i].pid, &claims[i].proc_start)
                    <= (claims[j].pid, &claims[j].proc_start)
                {
                    (&claims[i], &claims[j])
                } else {
                    (&claims[j], &claims[i])
                };

                for (me, other) in [(c1, c2), (c2, c1)] {
                    let Some(entry) = live_entries.iter().find(|e| {
                        e.pid == me.pid && e.proc_start == me.proc_start && e.session_id == me.id
                    }) else {
                        continue;
                    };
                    let claimed = conn
                        .execute(
                            "INSERT OR IGNORE INTO fork_notices (id, pid1, proc_start1, pid2, proc_start2, recipient_pid, recipient_proc_start, created_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            params![id, c1.pid, c1.proc_start, c2.pid, c2.proc_start, me.pid, me.proc_start, now],
                        )
                        .map_err(|e| AppError::Internal(e.to_string()))?;
                    if claimed == 0 {
                        continue;
                    }
                    notices_to_send.push(ForkNoticeToSend {
                        id: id.clone(),
                        pid1: c1.pid,
                        proc_start1: c1.proc_start.clone(),
                        pid2: c2.pid,
                        proc_start2: c2.proc_start.clone(),
                        recipient_pid: me.pid,
                        recipient_proc_start: me.proc_start.clone(),
                        target_session_id: me.address(),
                        socket_path: PathBuf::from(&entry.messaging_socket_path),
                        from_name: format!("xmsg@{host_label}"),
                        text: format!(
                            "xmsg: conversation {} is open in two processes on {}. This process is {}; the other is {}. A send to {} reaches {}.",
                            id, host_label, me.address(), other.address(), id, plain_addr
                        ),
                    });
                }
            }
        }
    }

    tx.commit().map_err(|e| AppError::Internal(e.to_string()))?;
    Ok((active_pairs, notices_to_send))
}

/// Delivers notices claimed by `sync_claims_db`. A delivered notice is
/// stored as a message; a failed one has its claim released so a later scan
/// delivers it.
pub async fn deliver_fork_notices(
    db: &std::sync::Mutex<Connection>,
    notices: Vec<ForkNoticeToSend>,
) {
    if notices.is_empty() {
        return;
    }

    let mut outcomes = Vec::new();
    for notice in notices {
        let res = match inbox::encode_transport_line(&notice.from_name, &notice.text) {
            Ok(line) => inbox::deliver_to_socket(&notice.socket_path, &line).await,
            Err(e) => Err(e),
        };
        if let Err(e) = &res {
            warn!(
                "Failed delivering fork notice to {}: {}",
                notice.socket_path.display(),
                e
            );
        }
        outcomes.push((notice, res.is_ok()));
    }

    // A poisoned lock still holds a usable connection; recover it so a failed
    // notice's claim is released rather than kept forever.
    let conn = db.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    for (notice, delivered) in outcomes {
        let res = if delivered {
            let msg_id = ulid::Ulid::new().to_string();
            storage::insert_message(
                &conn,
                &MessageRecord {
                    id: msg_id.clone(),
                    created_at: now_epoch_secs(),
                    session_id: notice.target_session_id.clone(),
                    from_name: notice.from_name.clone(),
                    bytes: notice.text.len(),
                    outcome: "delivered".to_string(),
                    recipient_harness: "claude".to_string(),
                    return_harness: None,
                    return_session_id: None,
                    push_replies: false,
                    thread_id: msg_id,
                    return_host: None,
                },
            )
        } else {
            conn.execute(
                "DELETE FROM fork_notices WHERE id = ?1 AND pid1 = ?2 AND proc_start1 = ?3 AND pid2 = ?4 AND proc_start2 = ?5 AND recipient_pid = ?6 AND recipient_proc_start = ?7",
                params![notice.id, notice.pid1, notice.proc_start1, notice.pid2, notice.proc_start2, notice.recipient_pid, notice.recipient_proc_start],
            )
            .map(|_| ())
        };
        if let Err(e) = res {
            warn!(
                "Fork notice bookkeeping failed for {}: {}",
                notice.target_session_id, e
            );
        }
    }
}

/// Runs one claim scan, then `f` over its live (claim, session file) pairs,
/// under the DB lock; then delivers the fork notices that scan claimed.
/// Every surface that observes the claim table goes through here, so none can
/// observe a fork and drop its notice.
///
/// Delivery runs on its own task, so dropping this future (a client that
/// disconnects mid-request) neither loses a claimed notice nor keeps its
/// claim. A caller that runs to completion returns after delivery.
pub async fn with_claims<T>(
    db: &Arc<Mutex<Connection>>,
    proc_root: &Path,
    sessions_dirs: &(impl SessionDirs + ?Sized),
    host_label: &str,
    f: impl FnOnce(&Connection, &[(InstanceClaim, SessionFileEntry)]) -> T,
) -> Result<T, AppError> {
    let (out, notices) = {
        let conn = db.lock().map_err(|e| AppError::Internal(e.to_string()))?;
        let (pairs, notices) = sync_claims_db(&conn, proc_root, sessions_dirs, host_label)?;
        (f(&conn, &pairs), notices)
    };
    if !notices.is_empty() {
        let db = Arc::clone(db);
        let delivery = tokio::spawn(async move { deliver_fork_notices(&db, notices).await });
        if let Err(e) = delivery.await {
            warn!("Fork notice delivery task failed: {}", e);
        }
    }
    Ok(out)
}

pub async fn sync_claims(
    db: &Arc<Mutex<Connection>>,
    proc_root: &Path,
    sessions_dirs: &(impl SessionDirs + ?Sized),
    host_label: &str,
) -> Result<Vec<(InstanceClaim, SessionFileEntry)>, AppError> {
    with_claims(db, proc_root, sessions_dirs, host_label, |_, pairs| {
        pairs.to_vec()
    })
    .await
}

/// Resolves a Claude session reference against one scan's live pairs.
pub fn resolve_claude_in(
    conn: &Connection,
    sessions_dirs: &(impl SessionDirs + ?Sized),
    active_pairs: &[(InstanceClaim, SessionFileEntry)],
    ref_str: &str,
) -> Result<(Session, PathBuf), AppError> {
    // 1. Check if ref_str specifies a fork suffix e.g. "X~2"
    if let Some((base_id, suffix_str)) = ref_str.split_once('~') {
        if let Ok(suffix) = suffix_str.parse::<u32>() {
            for (claim, entry) in active_pairs {
                if claim.id == base_id && claim.suffix == suffix {
                    let mut session: Session = entry.clone().into();
                    session.session_id = claim.address();
                    return Ok((session, PathBuf::from(&entry.messaging_socket_path)));
                }
            }

            // Check if it was ever registered and is now gone
            let mut stmt = conn
                .prepare("SELECT pid, released FROM instance_claims WHERE id = ?1 AND suffix = ?2 ORDER BY created_at DESC LIMIT 1")
                .map_err(|e| AppError::Internal(e.to_string()))?;
            let mut rows = stmt
                .query(params![base_id, suffix])
                .map_err(|e| AppError::Internal(e.to_string()))?;
            if let Some(row) = rows.next().map_err(|e| AppError::Internal(e.to_string()))? {
                let pid: u32 = row.get(0).map_err(|e| AppError::Internal(e.to_string()))?;
                return Err(AppError::Gone {
                    session_id: ref_str.to_string(),
                    pid,
                });
            }

            return Err(AppError::NotFound(ref_str.to_string()));
        }
    }

    // 2. Check for bare ID match
    let id_matches: Vec<&(InstanceClaim, SessionFileEntry)> = active_pairs
        .iter()
        .filter(|(claim, entry)| claim.id == ref_str || entry.session_id == ref_str)
        .collect();

    if !id_matches.is_empty() {
        if let Some((plain_claim, plain_entry)) = id_matches.iter().find(|(c, _)| c.suffix == 1) {
            let mut session: Session = plain_entry.clone().into();
            session.session_id = plain_claim.address();
            return Ok((session, PathBuf::from(&plain_entry.messaging_socket_path)));
        } else {
            // Suffix 1 is free/dead, but active forks exist: ambiguous!
            let candidates: Vec<String> = id_matches.iter().map(|(c, _)| c.address()).collect();
            return Err(AppError::Ambiguous(candidates.join(", ")));
        }
    }

    // 3. Check for PID match
    if let Ok(pid) = ref_str.parse::<u32>() {
        for (claim, entry) in active_pairs {
            if entry.pid == pid {
                let mut session: Session = entry.clone().into();
                session.session_id = claim.address();
                return Ok((session, PathBuf::from(&entry.messaging_socket_path)));
            }
        }
    }

    // 4. Check for session name match
    let name_matches: Vec<&(InstanceClaim, SessionFileEntry)> = active_pairs
        .iter()
        .filter(|(_, entry)| entry.name.as_deref() == Some(ref_str))
        .collect();

    if name_matches.len() == 1 {
        let (claim, entry) = name_matches[0];
        let mut session: Session = entry.clone().into();
        session.session_id = claim.address();
        return Ok((session, PathBuf::from(&entry.messaging_socket_path)));
    } else if name_matches.len() > 1 {
        let candidates: Vec<String> = name_matches.iter().map(|(c, _)| c.address()).collect();
        return Err(AppError::Ambiguous(candidates.join(", ")));
    }

    // 5. If not active, check if this ID had a previous claim that is now gone
    let mut stmt = conn
        .prepare("SELECT pid FROM instance_claims WHERE id = ?1 ORDER BY created_at DESC LIMIT 1")
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let mut rows = stmt
        .query(params![ref_str])
        .map_err(|e| AppError::Internal(e.to_string()))?;
    if let Some(row) = rows.next().map_err(|e| AppError::Internal(e.to_string()))? {
        let pid: u32 = row.get(0).map_err(|e| AppError::Internal(e.to_string()))?;
        return Err(AppError::Gone {
            session_id: ref_str.to_string(),
            pid,
        });
    }

    // Check dead session files on disk
    let raw_entries = registry::read_session_entries(sessions_dirs);
    let ref_pid = ref_str.parse::<u32>().ok();
    for entry in raw_entries {
        if entry.session_id == ref_str
            || entry.name.as_deref() == Some(ref_str)
            || Some(entry.pid) == ref_pid
        {
            return Err(AppError::Gone {
                session_id: entry.session_id,
                pid: entry.pid,
            });
        }
    }

    Err(AppError::NotFound(ref_str.to_string()))
}

pub async fn resolve_claude_session_and_deliver(
    db: &Arc<Mutex<Connection>>,
    proc_root: &Path,
    sessions_dirs: &(impl SessionDirs + ?Sized),
    host_label: &str,
    ref_str: &str,
) -> Result<(Session, PathBuf), AppError> {
    with_claims(db, proc_root, sessions_dirs, host_label, |conn, pairs| {
        resolve_claude_in(conn, sessions_dirs, pairs, ref_str)
    })
    .await?
}
