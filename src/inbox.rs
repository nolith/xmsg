use regex::Regex;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::time::timeout;
use unicode_general_category::{get_general_category, GeneralCategory};

use crate::error::AppError;

pub const SOCKET_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxLine {
    pub r#type: String, // "user"
    pub message: InboxMessage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InboxMessage {
    pub role: String, // "user"
    pub content: String,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessageRequest {
    #[serde(default)]
    pub from: String,
    pub text: String,
    #[serde(default = "default_true", alias = "pushReplies")]
    pub push_replies: bool,
    #[serde(
        default,
        alias = "idempotencyKey",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DeliveryResponse {
    pub session_id: String,
    pub from_name: String,
    pub bytes: usize,
    #[serde(alias = "message_id")]
    pub message_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<String>,
}

/// The most characters of a sender's self-described label that a label shows. Only that
/// part is bounded: the attested part (`xmsg@<node>`) is always emitted whole (A12).
pub const LABEL_MAX_CHARS: usize = 64;

/// Sanitizes the `from` parameter for unauthenticated HTTP senders:
/// - Rejects ':' and '/' outright (returns AppError::BadSender).
/// - Enforces strictly printable ASCII characters (0x20 to 0x7E).
/// - Strips quotes (") and angle brackets (<, >).
/// - Collapses whitespace runs to a single space.
/// - Trims leading and trailing whitespace.
/// - Truncates the name to `LABEL_MAX_CHARS` and prefixes `xmsg@<node-name> · `, which is
///   never cut: the result is at most `LABEL_MAX_CHARS + 8` characters plus the node name.
pub fn sanitize_from(node_name: &str, raw_from: &str) -> Result<String, AppError> {
    if raw_from.contains(':') || raw_from.contains('/') {
        return Err(AppError::BadSender(
            "sender name cannot contain ':' or '/'".to_string(),
        ));
    }

    if raw_from.chars().any(|c| !(' '..='~').contains(&c)) {
        return Err(AppError::BadSender(
            "sender name must contain only printable ASCII characters".to_string(),
        ));
    }

    let mut cleaned = String::with_capacity(raw_from.len());
    let mut prev_whitespace = false;

    for c in raw_from.chars() {
        if c == '"' || c == '<' || c == '>' {
            continue;
        }

        if c == ' ' {
            if !prev_whitespace {
                cleaned.push(' ');
                prev_whitespace = true;
            }
        } else {
            cleaned.push(c);
            prev_whitespace = false;
        }
    }

    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return Err(AppError::BadSender(
            "sender name is empty after sanitization".to_string(),
        ));
    }

    let label: String = trimmed.chars().take(LABEL_MAX_CHARS).collect();
    Ok(format!("xmsg@{node_name} · {}", label.trim_end()))
}

/// Sanitizes an attested caller session name and formats the attested badge:
/// `xmsg@<node-name> · <harness>:<cleaned_caller_name>`.
/// - Strips `"`, `<`, `>`, `\n`, `\r`, and Unicode categories Cc, Cf, Cs, Zl, Zp.
/// - Collapses whitespace runs to a single space.
/// - Trims leading and trailing whitespace.
/// - Caps the name to `LABEL_MAX_CHARS`; `xmsg@<node-name> · <harness>:` is never cut.
pub fn sanitize_attested_from(node_name: &str, harness: &str, caller_name: &str) -> String {
    let mut cleaned = String::with_capacity(caller_name.len());
    let mut prev_whitespace = false;

    for c in caller_name.chars() {
        if c == '"' || c == '<' || c == '>' || c == '\n' || c == '\r' {
            continue;
        }

        if !c.is_ascii() {
            cleaned.push('_');
            prev_whitespace = false;
            continue;
        }

        let cat = get_general_category(c);
        if matches!(
            cat,
            GeneralCategory::Control
                | GeneralCategory::Format
                | GeneralCategory::Surrogate
                | GeneralCategory::LineSeparator
                | GeneralCategory::ParagraphSeparator
        ) {
            continue;
        }

        if c.is_whitespace() {
            if !prev_whitespace {
                cleaned.push(' ');
                prev_whitespace = true;
            }
        } else {
            cleaned.push(c);
            prev_whitespace = false;
        }
    }

    let trimmed = cleaned.trim();
    let name_part = if trimmed.is_empty() { "agent" } else { trimmed };
    let harness_prefix = format!("{harness}:");
    let name_stripped = name_part.strip_prefix(&harness_prefix).unwrap_or(name_part);
    let name_final = if name_stripped.is_empty() {
        "agent"
    } else {
        name_stripped
    };

    let label: String = name_final.chars().take(LABEL_MAX_CHARS).collect();
    format!("xmsg@{node_name} · {harness}:{}", label.trim_end())
}

/// Sanitizes the message body:
/// Every `<` that begins `/?cross-session-message` (case-insensitive) anywhere in the body becomes `<\`.
/// Nothing else changes.
pub fn sanitize_body(body: &str) -> String {
    // Regex matching case-insensitive '<' followed by optional '/' and 'cross-session-message'
    // Replaces '<' with '<\', preserving the captured tag text.
    let re = Regex::new(r"(?i)<(/?cross-session-message)").expect("valid regex");
    re.replace_all(body, r"<\$1").to_string()
}

/// Assembles the inner envelope string.
/// Escapes `"` in `from_name` to prevent XML attribute breakout.
pub fn assemble_envelope(from_name: &str, sanitized_body: &str) -> String {
    let safe_from_name = from_name.replace('"', "&quot;");
    format!("<cross-session-message from-name=\"{safe_from_name}\">\n{sanitized_body}\n</cross-session-message>")
}

/// Encodes the complete transport line:
/// Serializes typed `InboxLine` into JSON with serde_json, appending a single `\n`.
/// Never uses format! or string concatenation for the JSON structure.
pub fn encode_transport_line(from_name: &str, raw_body: &str) -> Result<String, AppError> {
    let sanitized_body = sanitize_body(raw_body);
    let envelope = assemble_envelope(from_name, &sanitized_body);

    let line_struct = InboxLine {
        r#type: "user".to_string(),
        message: InboxMessage {
            role: "user".to_string(),
            content: envelope,
        },
    };

    let mut json_str = serde_json::to_string(&line_struct)
        .map_err(|e| AppError::Internal(format!("failed to serialize inbox line: {e}")))?;
    json_str.push('\n');
    Ok(json_str)
}

/// Delivers a single newline-delimited JSON line to the target Unix domain socket.
pub async fn deliver_to_socket(socket_path: &Path, line: &str) -> Result<(), AppError> {
    let connect_fut = UnixStream::connect(socket_path);
    let mut stream = match timeout(SOCKET_TIMEOUT, connect_fut).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            return Err(AppError::InboxUnavailable(format!(
                "socket connect failed: {e}"
            )))
        }
        Err(_) => return Err(AppError::InboxTimeout),
    };

    let write_fut = async {
        stream.write_all(line.as_bytes()).await?;
        stream.flush().await?;
        Ok::<(), std::io::Error>(())
    };

    match timeout(SOCKET_TIMEOUT, write_fut).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(AppError::InboxUnavailable(format!(
            "socket write failed: {e}"
        ))),
        Err(_) => Err(AppError::InboxTimeout),
    }
}
