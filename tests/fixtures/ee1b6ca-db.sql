-- A database created by the deployed xmsg binary, ee1b6cad323d44a88b5f2748510b7c437e9828e8
-- (`xmsg serve --db-path ...`, then SIGTERM), with one row inserted into each of its
-- eight message-state tables, then written out by `sqlite3 <db> .dump`. user_version is 0.
-- XA3 oracle 9 (tests/gate_xa3_core.rs) migrates it to schema N.
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE messages (
            id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            session_id TEXT NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            recipient_harness TEXT NOT NULL DEFAULT 'claude',
            return_harness TEXT,
            return_session_id TEXT,
            push_replies INTEGER NOT NULL DEFAULT 1,
            thread_id TEXT NOT NULL DEFAULT '',
            return_host TEXT NOT NULL DEFAULT ''
        );
INSERT INTO messages VALUES('01OLDMSG',1760000000,'sess-old','xmsg@cortex · claude:old',3,'delivered','claude','claude','sess-old-sender',1,'01OLDMSG','');
CREATE TABLE outbound (
            id TEXT PRIMARY KEY,
            peer TEXT NOT NULL,
            target_ref TEXT NOT NULL,
            outcome TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
INSERT INTO outbound VALUES('01OLDOUT','bitstream','svc:genie-guard','delivered',1760000000);
CREATE TABLE replies (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL REFERENCES messages(id),
            created_at INTEGER NOT NULL,
            replier_session_id TEXT NOT NULL,
            text TEXT NOT NULL,
            push_outcome TEXT,
            pushed_message_id TEXT
        );
INSERT INTO replies VALUES(1,'01OLDMSG',1760000001,'sess-old','old reply','pushed','01OLDPUSH');
CREATE TABLE pi_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER
        );
INSERT INTO pi_pending_messages VALUES('01OLDPI','pi-old',1760000000,'xmsg@cortex · claude:old',3,'old','env',NULL);
CREATE TABLE agy_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER
        );
INSERT INTO agy_pending_messages VALUES('01OLDAGY','agy-old',1760000000,'xmsg@cortex · claude:old',3,'old','env',NULL);
CREATE TABLE svc_pending_messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            from_name TEXT NOT NULL,
            bytes INTEGER NOT NULL,
            text TEXT NOT NULL,
            envelope TEXT NOT NULL,
            delivered_at INTEGER,
            origin TEXT NOT NULL DEFAULT '{"kind":"anonymous"}'
        );
INSERT INTO svc_pending_messages VALUES('01OLDSVC','genie-guard',1760000000,'xmsg@cortex · claude:old',3,'old','env',NULL,'{"kind":"local","harness":"claude","sessionId":"sess-old"}');
CREATE TABLE idempotency_keys (
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
INSERT INTO idempotency_keys VALUES('session:sess-old','k1','old','01OLDMSG','sess-old','xmsg@cortex · claude:old',3,'delivered',1760000000);
CREATE TABLE peer_mailbox (
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
INSERT INTO peer_mailbox VALUES('01OLDMBX','leaf','svc:genie-guard',1760000000,'xmsg@cortex · claude:old',3,'old','{}','{"kind":"local","harness":"claude","sessionId":"sess-old"}');
PRAGMA writable_schema=ON;
CREATE TABLE IF NOT EXISTS sqlite_sequence(name,seq);
DELETE FROM sqlite_sequence;
INSERT INTO sqlite_sequence VALUES('replies',1);
CREATE INDEX idx_outbound_created_at ON outbound(created_at);
CREATE INDEX idx_replies_message_seq ON replies(message_id, seq);
CREATE INDEX idx_replies_created_at ON replies(created_at);
CREATE INDEX idx_pi_pending_session ON pi_pending_messages(session_id, delivered_at);
CREATE INDEX idx_pi_pending_created_at ON pi_pending_messages(created_at);
CREATE INDEX idx_agy_pending_session ON agy_pending_messages(session_id, delivered_at);
CREATE INDEX idx_agy_pending_created_at ON agy_pending_messages(created_at);
CREATE INDEX idx_svc_pending_session ON svc_pending_messages(session_id, delivered_at);
CREATE INDEX idx_svc_pending_created_at ON svc_pending_messages(created_at);
CREATE INDEX idx_idempotency_created_at ON idempotency_keys(created_at);
CREATE INDEX idx_peer_mailbox_peer_created ON peer_mailbox(peer, created_at);
PRAGMA writable_schema=OFF;
COMMIT;
