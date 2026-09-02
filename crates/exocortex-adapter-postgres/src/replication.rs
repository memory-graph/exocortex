//! The minimal Postgres logical-replication session (D20).
//!
//! First-party BY NECESSITY, recorded in PUBLISHING.md per rule 9:
//! `pgwire-replication` needs rustc 1.88 against the pinned 1.85
//! floor, and `tokio-postgres` exposes no replication mode — the
//! remaining correct option is a small client on `postgres-protocol`
//! (the same codecs and SCRAM machinery tokio-postgres itself uses).
//!
//! What it deliberately does NOT do: TLS (v1 is loopback /
//! private-network Postgres, the Falkor live-leg pattern), cleartext
//! or MD5 authentication (SCRAM-SHA-256 only — a CDC credential never
//! rides the wire unhashed), or full statement pipelines. It
//! connects, authenticates, creates the logical slot, starts
//! replication with wal2json (format-version 2) filtered to the
//! declared tables, and drives the CopyBoth stream — XLogData
//! payloads handed to the caller as strings, keepalives answered with
//! a standby status update at the last consumed LSN.
//!
//! Live coverage: `tests/cdc_live.rs` (feature `integration`, gated
//! on `POSTGRES_URL`, exactly like the Falkor/Redis live legs). The
//! parse/mapping layers it drives are hermetic and always run.

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use postgres_protocol::authentication::sasl;
use postgres_protocol::message::backend::Message as BackendMessage;
use postgres_protocol::message::frontend;

/// One stream event surfaced to the caller.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    /// A wal2json change payload with its WAL LSN (walStart).
    Change { lsn: u64, payload: String },
    /// A server keepalive (answered internally with a status update).
    KeepAlive,
}

/// The stream-phase message set this session understands. The
/// replication protocol's CopyBothResponse ('W') has no variant in
/// postgres-protocol's backend enum, so the stream frames are parsed
/// here; everything unknown is skipped, never guessed.
enum StreamMessage {
    CopyBoth,
    CopyData(Vec<u8>),
    CopyDone,
    Error(String),
    Other,
}

pub struct ReplicationSession {
    stream: TcpStream,
    buffer: BytesMut,
    last_lsn: u64,
}

struct Endpoint {
    user: String,
    password: String,
    host: String,
    port: u16,
}

fn parse_endpoint(url: &str) -> Result<Endpoint> {
    let rest = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .ok_or_else(|| anyhow!("POSTGRES_URL must be a postgres:// URL"))?;
    let (authority, _path) = rest.split_once('/').unwrap_or((rest, ""));
    let (credentials, hostport) = authority
        .split_once('@')
        .ok_or_else(|| anyhow!("POSTGRES_URL must carry user:password@host"))?;
    let (user, password) = credentials
        .split_once(':')
        .ok_or_else(|| anyhow!("POSTGRES_URL must carry user:password"))?;
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>().context("parsing POSTGRES_URL port")?,
        ),
        None => (hostport, 5432),
    };
    Ok(Endpoint {
        user: user.to_string(),
        password: password.to_string(),
        host: host.to_string(),
        port,
    })
}

impl ReplicationSession {
    /// Connect in replication mode and authenticate with
    /// SCRAM-SHA-256.
    pub async fn connect(url: &str) -> Result<Self> {
        let endpoint = parse_endpoint(url)?;
        let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
            .await
            .with_context(|| format!("connecting to {}:{}", endpoint.host, endpoint.port))?;
        let mut buffer = BytesMut::new();

        // Startup with the replication parameter — the one thing the
        // standard clients cannot send.
        frontend::startup_message(
            [
                ("user", endpoint.user.as_str()),
                ("replication", "database"),
            ],
            &mut buffer,
        )
        .map_err(|e| anyhow!("encoding startup: {e}"))?;
        stream.write_all(&buffer).await?;
        buffer.clear();

        let mut scram: Option<sasl::ScramSha256> = None;
        loop {
            let message = read_backend_message(&mut stream, &mut buffer).await?;
            match message {
                BackendMessage::AuthenticationOk => break,
                BackendMessage::AuthenticationSasl(body) => {
                    let mut mechanisms = Vec::new();
                    let mut iterator = body.mechanisms();
                    while let Some(mechanism) = iterator.next()? {
                        mechanisms.push(mechanism.to_string());
                    }
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        bail!(
                            "the server offers no SCRAM-SHA-256 ({mechanisms:?}); cleartext and \
                             MD5 authentication are refused — a CDC credential never rides the \
                             wire unhashed"
                        );
                    }
                    let secret = sasl::ScramSha256::new(
                        endpoint.password.as_bytes(),
                        sasl::ChannelBinding::unsupported(),
                    );
                    buffer.clear();
                    frontend::sasl_initial_response("SCRAM-SHA-256", secret.message(), &mut buffer)
                        .map_err(|e| anyhow!("encoding the SASL initial response: {e}"))?;
                    stream.write_all(&buffer).await?;
                    buffer.clear();
                    scram = Some(secret);
                }
                BackendMessage::AuthenticationSaslContinue(body) => {
                    let mut secret = scram
                        .take()
                        .ok_or_else(|| anyhow!("SASL continue before the challenge"))?;
                    secret.update(body.data())?;
                    buffer.clear();
                    frontend::sasl_response(secret.message(), &mut buffer)
                        .map_err(|e| anyhow!("encoding the SASL response: {e}"))?;
                    stream.write_all(&buffer).await?;
                    buffer.clear();
                    scram = Some(secret);
                }
                BackendMessage::AuthenticationSaslFinal(body) => {
                    let mut secret = scram
                        .take()
                        .ok_or_else(|| anyhow!("SASL final without an exchange"))?;
                    secret.finish(body.data())?;
                }
                BackendMessage::ErrorResponse(body) => {
                    bail!("authentication failed: {}", error_text(&body));
                }
                _ => bail!("unexpected message during authentication"),
            }
        }
        // Consume parameters and ready-for-query before commands.
        loop {
            match read_backend_message(&mut stream, &mut buffer).await? {
                BackendMessage::ReadyForQuery(_) => break,
                BackendMessage::ParameterStatus(..)
                | BackendMessage::BackendKeyData(..)
                | BackendMessage::NoticeResponse(..) => continue,
                BackendMessage::ErrorResponse(body) => {
                    bail!("startup failed: {}", error_text(&body))
                }
                _ => bail!("unexpected post-auth message"),
            }
        }
        Ok(Self {
            stream,
            buffer,
            last_lsn: 0,
        })
    }

    /// Create a DURABLE logical slot with wal2json (idempotent: an
    /// existing slot is tolerated so restarts keep the cursor).
    pub async fn create_slot_if_not_exists(&mut self, slot: &str) -> Result<()> {
        let statement = format!("CREATE_REPLICATION_SLOT {slot} LOGICAL wal2json");
        match self.simple_command(&statement).await {
            Ok(()) => Ok(()),
            Err(error)
                if error.to_string().contains("42710")
                    || error.to_string().contains("already exists") =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Drop a slot (test hygiene for the live leg).
    pub async fn drop_slot(&mut self, slot: &str) -> Result<()> {
        let statement = format!("DROP_REPLICATION_SLOT {slot}");
        self.simple_command(&statement).await
    }

    async fn simple_command(&mut self, statement: &str) -> Result<()> {
        self.buffer.clear();
        frontend::query(statement, &mut self.buffer);
        self.stream.write_all(&self.buffer).await?;
        self.buffer.clear();
        loop {
            let message = read_backend_message(&mut self.stream, &mut self.buffer).await?;
            match message {
                BackendMessage::ReadyForQuery(_) => return Ok(()),
                BackendMessage::ErrorResponse(body) => {
                    // Drain to ready before surfacing the failure.
                    loop {
                        match read_backend_message(&mut self.stream, &mut self.buffer).await? {
                            BackendMessage::ReadyForQuery(_) => break,
                            _ => continue,
                        }
                    }
                    return Err(anyhow!(
                        "command `{statement}` failed: {}",
                        error_text(&body)
                    ));
                }
                _ => continue,
            }
        }
    }

    /// Start replication from `start_lsn` (0 = the slot's confirmed
    /// LSN), filtered to `tables`, and drive the CopyBoth stream
    /// until `on_event` errors or the stream ends. Returns the last
    /// consumed LSN.
    pub async fn stream_changes<F>(
        mut self,
        slot: &str,
        start_lsn: u64,
        tables: &[String],
        mut on_event: F,
    ) -> Result<u64>
    where
        F: FnMut(StreamEvent) -> Result<()>,
    {
        let mut options =
            String::from("(\"format-version\" \"2\" \"include-transaction\" \"false\"");
        if !tables.is_empty() {
            let filters: Vec<String> = tables.iter().map(|t| format!("^{t}$")).collect();
            options.push_str(" \"filter-tables\" \"");
            options.push_str(&filters.join("|"));
            options.push('"');
        }
        options.push(')');
        let statement = match start_lsn {
            0 => format!("START_REPLICATION SLOT {slot} LOGICAL {options}"),
            lsn => format!(
                "START_REPLICATION SLOT {slot} LOGICAL {}/{:08X} {options}",
                lsn >> 32,
                lsn & 0xFFFF_FFFF
            ),
        };
        self.buffer.clear();
        frontend::query(&statement, &mut self.buffer);
        self.stream.write_all(&self.buffer).await?;
        self.buffer.clear();

        // CopyBothResponse ('W') opens the stream; postgres-protocol's
        // enum has no variant for it, so the stream phase parses its
        // own frames.
        loop {
            match read_stream_message(&mut self.stream, &mut self.buffer).await? {
                StreamMessage::CopyBoth => break,
                StreamMessage::Error(text) => bail!("START_REPLICATION failed: {text}"),
                StreamMessage::Other => continue,
                _ => bail!("unexpected frame before CopyBothResponse"),
            }
        }

        loop {
            match read_stream_message(&mut self.stream, &mut self.buffer).await? {
                StreamMessage::CopyData(data) => match data.first() {
                    Some(b'w') if data.len() >= 25 => {
                        let wal_start = u64::from_be_bytes(data[1..9].try_into().expect("8 bytes"));
                        let payload = &data[25..];
                        self.last_lsn = self.last_lsn.max(wal_start);
                        let text = String::from_utf8(payload.to_vec()).with_context(|| {
                            format!("wal2json payload at lsn {wal_start:x} is not UTF-8")
                        })?;
                        on_event(StreamEvent::Change {
                            lsn: wal_start,
                            payload: text,
                        })?;
                        self.send_status().await?;
                    }
                    Some(b'k') if data.len() >= 17 => {
                        self.last_lsn = self
                            .last_lsn
                            .max(u64::from_be_bytes(data[1..9].try_into().expect("8 bytes")));
                        on_event(StreamEvent::KeepAlive)?;
                        // Reply-requested keepalives get a status frame.
                        if data.last() == Some(&1) {
                            self.send_status().await?;
                        }
                    }
                    // Unknown CopyData kinds are skipped, not guessed.
                    _ => {}
                },
                StreamMessage::CopyDone => return Ok(self.last_lsn),
                StreamMessage::Error(text) => {
                    return Err(anyhow!("replication stream failed: {text}"))
                }
                StreamMessage::CopyBoth | StreamMessage::Other => continue,
            }
        }
    }

    /// Standby status update at the last consumed LSN (written ==
    /// flush == apply: this adapter only acknowledges what it has
    /// handed to the caller).
    async fn send_status(&mut self) -> Result<()> {
        let lsn = self.last_lsn;
        let mut body = Vec::with_capacity(34);
        body.push(b'r');
        body.extend_from_slice(&lsn.to_be_bytes());
        body.extend_from_slice(&lsn.to_be_bytes());
        body.extend_from_slice(&lsn.to_be_bytes());
        body.extend_from_slice(&0i64.to_be_bytes());
        body.push(0);
        // CopyData frame: 'd' + i32 length (4 + body) + body.
        self.buffer.clear();
        self.buffer.extend_from_slice(b"d");
        self.buffer
            .extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        self.buffer.extend_from_slice(&body);
        self.stream.write_all(&self.buffer).await?;
        Ok(())
    }
}

fn error_text(body: &postgres_protocol::message::backend::ErrorResponseBody) -> String {
    let mut fields = body.fields();
    loop {
        match fields.next() {
            Ok(Some(field)) if field.type_() == b'M' => {
                return String::from_utf8_lossy(field.value_bytes()).into_owned();
            }
            Ok(Some(_)) => continue,
            Ok(None) => return "unknown error".to_string(),
            Err(_) => return "unreadable error".to_string(),
        }
    }
}

/// Read one framed backend message through postgres-protocol's
/// canonical streaming parser (startup/auth/command phases).
async fn read_backend_message(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<BackendMessage> {
    loop {
        if let Some(message) = BackendMessage::parse(buffer).map_err(|e| anyhow!("{e}"))? {
            return Ok(message);
        }
        let read = stream.read_buf(buffer).await?;
        if read == 0 {
            bail!("the server closed the connection mid-message");
        }
    }
}

/// Read one stream-phase frame with this module's own tag mapping
/// ('W' CopyBothResponse has no enum variant upstream).
async fn read_stream_message(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
) -> Result<StreamMessage> {
    loop {
        let frame = read_frame(stream, buffer).await?;
        if let Some(bytes) = frame {
            let tag = bytes[0];
            let body = &bytes[5..];
            return Ok(match tag {
                b'W' => StreamMessage::CopyBoth,
                b'd' => StreamMessage::CopyData(body.to_vec()),
                b'c' => StreamMessage::CopyDone,
                b'E' => StreamMessage::Error(error_message_from_body(body)),
                _ => StreamMessage::Other,
            });
        }
    }
}

/// Extract the human message ('M') from an ErrorResponse body: a
/// sequence of (field-type byte, NUL-terminated value) pairs.
fn error_message_from_body(body: &[u8]) -> String {
    let mut at = 0usize;
    while at < body.len() {
        let field_type = body[at];
        let Some(end) = body[at + 1..]
            .iter()
            .position(|b| *b == 0)
            .map(|p| at + 1 + p)
        else {
            break;
        };
        if field_type == b'M' {
            return String::from_utf8_lossy(&body[at + 1..end]).into_owned();
        }
        at = end + 1;
    }
    "unreadable error".into()
}

/// Read one length-framed message (tag byte + i32 length + body),
/// returning None when more bytes are needed.
async fn read_frame(stream: &mut TcpStream, buffer: &mut BytesMut) -> Result<Option<bytes::Bytes>> {
    while buffer.len() < 5 {
        let read = stream.read_buf(buffer).await?;
        if read == 0 {
            bail!("the server closed the connection mid-message");
        }
    }
    let length = i32::from_be_bytes(buffer[1..5].try_into().expect("4 bytes")) as usize;
    let total = 1 + length;
    while buffer.len() < total {
        let read = stream.read_buf(buffer).await?;
        if read == 0 {
            bail!("the server closed the connection mid-message");
        }
    }
    Ok(Some(buffer.split_to(total).freeze()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_parsing_is_strict() {
        let endpoint = parse_endpoint("postgres://cdc:secret@db.internal:5433/warehouse").unwrap();
        assert_eq!(endpoint.user, "cdc");
        assert_eq!(endpoint.password, "secret");
        assert_eq!(endpoint.host, "db.internal");
        assert_eq!(endpoint.port, 5433);
        assert!(parse_endpoint("postgres://no-host").is_err());
        assert!(parse_endpoint("mysql://x").is_err());
    }
}
