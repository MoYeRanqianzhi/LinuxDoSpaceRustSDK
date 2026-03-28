//! LinuxDoSpace Rust SDK.
//!
//! This crate implements one strict runtime model:
//! - one `Client` owns one upstream HTTPS stream
//! - the client parses every mail event once
//! - local mailbox bindings run in-memory and do not create extra upstream connections

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, TimeZone, Utc};
use regex::Regex;
use reqwest::blocking::Client as BlockingHttpClient;
use reqwest::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client as HttpClient, Response};
use serde::Deserialize;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

const DEFAULT_BASE_URL: &str = "https://api.linuxdo.space";
const STREAM_PATH: &str = "/v1/token/email/stream";
const STREAM_FILTERS_PATH: &str = "/v1/token/email/filters";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_millis(300);
const LISTENER_BUFFER_SIZE: usize = 128;

/// One mail suffix value.
///
/// `Suffix::linuxdo_space()` is semantic rather than literal: after the stream
/// bootstrap exposes `ready.owner_username`, the SDK resolves it to the current
/// owner's canonical `@<owner>-mail.<default-root>` namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Suffix {
    /// The default semantic first-party LinuxDoSpace mail suffix.
    LinuxdoSpace,
    /// One semantic `-mail<fragment>` namespace under the default LinuxDoSpace root.
    LinuxdoSpaceWithFragment(String),
    /// Any managed suffix string.
    Custom(String),
}

impl Suffix {
    /// Returns the default `linuxdo.space` suffix variant.
    pub fn linuxdo_space() -> Self {
        Self::LinuxdoSpace
    }

    /// Returns the same semantic root with one normalized dynamic mail suffix fragment.
    pub fn with_suffix(self, fragment: impl AsRef<str>) -> Result<Self, LinuxDoSpaceError> {
        let normalized_fragment = normalize_mail_suffix_fragment(fragment.as_ref())?;
        match self {
            Self::LinuxdoSpace | Self::LinuxdoSpaceWithFragment(_) => {
                Ok(Self::LinuxdoSpaceWithFragment(normalized_fragment))
            }
            Self::Custom(_) => Err(LinuxDoSpaceError::validation(
                "with_suffix() is only supported for Suffix::linuxdo_space()",
            )),
        }
    }

    fn mail_suffix_fragment(&self) -> Option<&str> {
        match self {
            Self::LinuxdoSpace => Some(""),
            Self::LinuxdoSpaceWithFragment(fragment) => Some(fragment.as_str()),
            Self::Custom(_) => None,
        }
    }

    fn normalized(&self) -> Result<String, LinuxDoSpaceError> {
        let raw = match self {
            Self::LinuxdoSpace | Self::LinuxdoSpaceWithFragment(_) => "linuxdo.space",
            Self::Custom(value) => value.as_str(),
        };
        let value = raw.trim().to_ascii_lowercase();
        if value.is_empty() {
            return Err(LinuxDoSpaceError::validation("suffix must not be empty"));
        }
        Ok(value)
    }
}

fn normalize_mail_suffix_fragment(raw: &str) -> Result<String, LinuxDoSpaceError> {
    let trimmed = raw.trim().to_ascii_lowercase();
    if trimmed.is_empty() {
        return Ok(String::new());
    }

    let mut normalized = String::new();
    let mut last_was_dash = false;
    for character in trimmed.chars() {
        if character.is_ascii_lowercase() || character.is_ascii_digit() {
            normalized.push(character);
            last_was_dash = false;
            continue;
        }
        if !last_was_dash {
            normalized.push('-');
            last_was_dash = true;
        }
    }

    let normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        return Err(LinuxDoSpaceError::validation(
            "mail suffix fragment does not contain any valid dns characters",
        ));
    }
    if normalized.contains('.') {
        return Err(LinuxDoSpaceError::validation(
            "mail suffix fragment must stay inside one dns label",
        ));
    }
    if normalized.len() > 48 {
        return Err(LinuxDoSpaceError::validation(
            "mail suffix fragment must be 48 characters or fewer",
        ));
    }
    Ok(normalized)
}

impl Display for Suffix {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LinuxdoSpace | Self::LinuxdoSpaceWithFragment(_) => {
                f.write_str("linuxdo.space")
            }
            Self::Custom(value) => f.write_str(value),
        }
    }
}

impl FromStr for Suffix {
    type Err = LinuxDoSpaceError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Err(LinuxDoSpaceError::validation("suffix must not be empty"));
        }
        if normalized == "linuxdo.space" {
            return Ok(Self::LinuxdoSpace);
        }
        Ok(Self::Custom(normalized))
    }
}

/// One parsed mail event.
#[derive(Debug, Clone)]
pub struct MailMessage {
    pub address: String,
    pub sender: String,
    pub recipients: Vec<String>,
    pub received_at: DateTime<Utc>,
    pub subject: String,
    pub message_id: Option<String>,
    pub date: Option<DateTime<Utc>>,
    pub from_header: String,
    pub to_header: String,
    pub cc_header: String,
    pub reply_to_header: String,
    pub from_addresses: Vec<String>,
    pub to_addresses: Vec<String>,
    pub cc_addresses: Vec<String>,
    pub reply_to_addresses: Vec<String>,
    pub text: String,
    pub html: String,
    pub headers: HashMap<String, String>,
    pub raw: String,
    pub raw_bytes: Vec<u8>,
}

/// Authentication failure wrapper.
#[derive(Debug, Clone)]
pub struct AuthenticationError {
    pub message: String,
}

impl Display for AuthenticationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AuthenticationError {}

/// Stream failure wrapper.
#[derive(Debug, Clone)]
pub struct StreamError {
    pub message: String,
}

impl Display for StreamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StreamError {}

/// SDK top-level error.
#[derive(Debug, Clone)]
pub enum LinuxDoSpaceError {
    Authentication(AuthenticationError),
    Stream(StreamError),
    Validation(String),
    Closed(String),
}

impl LinuxDoSpaceError {
    fn auth(message: impl Into<String>) -> Self {
        Self::Authentication(AuthenticationError {
            message: message.into(),
        })
    }

    fn stream(message: impl Into<String>) -> Self {
        Self::Stream(StreamError {
            message: message.into(),
        })
    }

    fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }

    fn closed(message: impl Into<String>) -> Self {
        Self::Closed(message.into())
    }
}

impl Display for LinuxDoSpaceError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authentication(err) => write!(f, "authentication error: {}", err),
            Self::Stream(err) => write!(f, "stream error: {}", err),
            Self::Validation(err) => write!(f, "validation error: {}", err),
            Self::Closed(err) => write!(f, "closed: {}", err),
        }
    }
}

impl std::error::Error for LinuxDoSpaceError {}

#[derive(Debug, Clone)]
enum ListenerItem {
    Message(MailMessage),
}

#[derive(Debug)]
enum StreamLoopError {
    Fatal(LinuxDoSpaceError),
    Recoverable(LinuxDoSpaceError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BindingMode {
    Exact,
    Pattern,
}

#[derive(Debug, Clone)]
struct BindingEntry {
    id: usize,
    mode: BindingMode,
    allow_overlap: bool,
    prefix: Option<String>,
    regex: Option<Regex>,
    mailbox: Weak<MailboxInner>,
}

impl BindingEntry {
    fn matches(&self, local_part: &str) -> bool {
        match self.mode {
            BindingMode::Exact => self.prefix.as_deref() == Some(local_part),
            BindingMode::Pattern => self.regex.as_ref().is_some_and(|regex| {
                regex
                    .find(local_part)
                    .is_some_and(|m| m.start() == 0 && m.end() == local_part.len())
            }),
        }
    }
}

#[derive(Debug)]
struct ClientInner {
    token: String,
    base_url: String,
    http: HttpClient,
    filter_http: BlockingHttpClient,
    closed: AtomicBool,
    connected: AtomicBool,
    shutdown: CancellationToken,
    runner: Mutex<Option<JoinHandle<()>>>,
    fatal_error: Mutex<Option<LinuxDoSpaceError>>,
    full_listeners: Mutex<HashMap<usize, SyncSender<ListenerItem>>>,
    full_dropped: AtomicU64,
    bindings_by_suffix: Mutex<HashMap<String, Vec<BindingEntry>>>,
    owner_username: Mutex<Option<String>>,
    synced_mailbox_suffix_fragments: Mutex<Option<Vec<String>>>,
    next_id: AtomicUsize,
}

/// Top-level SDK client.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    /// Creates one client and immediately validates the initial upstream stream connection.
    pub fn new(token: impl AsRef<str>, base_url: Option<&str>) -> Result<Self, LinuxDoSpaceError> {
        let normalized_token = token.as_ref().trim().to_string();
        if normalized_token.is_empty() {
            return Err(LinuxDoSpaceError::validation("token must not be empty"));
        }

        let normalized_base_url = normalize_base_url(base_url.unwrap_or(DEFAULT_BASE_URL))?;
        let http = HttpClient::builder()
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .build()
            .map_err(|err| {
                LinuxDoSpaceError::stream(format!("failed to build http client: {err}"))
            })?;
        let filter_http = BlockingHttpClient::builder()
            .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
            .build()
            .map_err(|err| {
                LinuxDoSpaceError::stream(format!("failed to build filter http client: {err}"))
            })?;

        let inner = Arc::new(ClientInner {
            token: normalized_token,
            base_url: normalized_base_url,
            http,
            filter_http,
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            shutdown: CancellationToken::new(),
            runner: Mutex::new(None),
            fatal_error: Mutex::new(None),
            full_listeners: Mutex::new(HashMap::new()),
            full_dropped: AtomicU64::new(0),
            bindings_by_suffix: Mutex::new(HashMap::new()),
            owner_username: Mutex::new(None),
            synced_mailbox_suffix_fragments: Mutex::new(None),
            next_id: AtomicUsize::new(1),
        });

        let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), LinuxDoSpaceError>>(1);
        let thread_inner = Arc::clone(&inner);
        let handle = thread::spawn(move || {
            let runtime = match RuntimeBuilder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(err) => {
                    let _ = ready_tx.send(Err(LinuxDoSpaceError::stream(format!(
                        "failed to build tokio runtime: {err}"
                    ))));
                    return;
                }
            };
            runtime.block_on(run_stream_loop(thread_inner, Some(ready_tx)));
        });
        *inner.runner.lock().expect("runner lock poisoned") = Some(handle);

        let client = Self { inner };

        match ready_rx.recv_timeout(DEFAULT_CONNECT_TIMEOUT + Duration::from_secs(1)) {
            Ok(result) => match result {
                Ok(()) => Ok(client),
                Err(error) => {
                    let _ = client.close();
                    Err(error)
                }
            },
            Err(_) => {
                let _ = client.close();
                Err(LinuxDoSpaceError::stream(
                    "timed out while opening the LinuxDoSpace HTTPS mail stream",
                ))
            }
        }
    }

    /// Returns whether the shared upstream stream is alive.
    pub fn connected(&self) -> bool {
        self.inner.connected.load(Ordering::SeqCst)
            && !self.inner.closed.load(Ordering::SeqCst)
            && self
                .inner
                .fatal_error
                .lock()
                .expect("fatal_error lock poisoned")
                .is_none()
    }

    /// Returns the terminal fatal error, if the client stopped because of one.
    pub fn error(&self) -> Option<LinuxDoSpaceError> {
        self.inner
            .fatal_error
            .lock()
            .expect("fatal_error lock poisoned")
            .clone()
    }

    /// Returns the number of full-stream messages dropped because listeners were full.
    pub fn dropped(&self) -> u64 {
        self.inner.full_dropped.load(Ordering::SeqCst)
    }

    /// Closes the client and all listeners.
    pub fn close(&self) -> Result<(), LinuxDoSpaceError> {
        let was_closed = self.inner.closed.swap(true, Ordering::SeqCst);
        self.inner.connected.store(false, Ordering::SeqCst);
        if !was_closed {
            self.inner.shutdown.cancel();
            close_local_state(&self.inner);
        }
        if let Some(handle) = self
            .inner
            .runner
            .lock()
            .expect("runner lock poisoned")
            .take()
        {
            let _ = handle.join();
        }
        Ok(())
    }

    /// Registers one exact mailbox binding.
    pub fn bind_prefix(
        &self,
        prefix: impl AsRef<str>,
        suffix: Suffix,
        allow_overlap: bool,
    ) -> Result<Mailbox, LinuxDoSpaceError> {
        let normalized_prefix = normalize_prefix(prefix.as_ref())?;
        self.bind_internal(
            BindingMode::Exact,
            self.resolve_binding_suffix(&suffix)?,
            allow_overlap,
            Some(normalized_prefix),
            None,
        )
    }

    /// Registers one regex mailbox binding.
    pub fn bind_pattern(
        &self,
        pattern: impl AsRef<str>,
        suffix: Suffix,
        allow_overlap: bool,
    ) -> Result<Mailbox, LinuxDoSpaceError> {
        let pattern_text = pattern.as_ref().trim().to_string();
        if pattern_text.is_empty() {
            return Err(LinuxDoSpaceError::validation("pattern must not be empty"));
        }
        let regex = Regex::new(&pattern_text).map_err(|err| {
            LinuxDoSpaceError::validation(format!("invalid regex pattern: {err}"))
        })?;
        self.bind_internal(
            BindingMode::Pattern,
            self.resolve_binding_suffix(&suffix)?,
            allow_overlap,
            None,
            Some(regex),
        )
    }

    fn bind_internal(
        &self,
        mode: BindingMode,
        suffix: String,
        allow_overlap: bool,
        prefix: Option<String>,
        regex: Option<Regex>,
    ) -> Result<Mailbox, LinuxDoSpaceError> {
        self.ensure_open()?;
        let mailbox_id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);

        let mailbox_inner = Arc::new(MailboxInner {
            id: mailbox_id,
            client: Arc::downgrade(&self.inner),
            mode,
            suffix: suffix.clone(),
            allow_overlap,
            prefix: prefix.clone(),
            pattern_text: regex.as_ref().map(|item| item.as_str().to_string()),
            listener_sender: Mutex::new(None),
            closed: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
        });

        let entry = BindingEntry {
            id: mailbox_id,
            mode,
            allow_overlap,
            prefix,
            regex,
            mailbox: Arc::downgrade(&mailbox_inner),
        };

        {
            let mut binding_map = self
                .inner
                .bindings_by_suffix
                .lock()
                .expect("bindings_by_suffix lock poisoned");
            binding_map.entry(suffix.clone()).or_default().push(entry);
        }

        if let Err(error) = sync_remote_mailbox_filters(&self.inner, true) {
            unregister_binding(&self.inner, mailbox_id);
            return Err(error);
        }

        Ok(Mailbox {
            inner: mailbox_inner,
        })
    }

    fn resolve_binding_suffix(&self, suffix: &Suffix) -> Result<String, LinuxDoSpaceError> {
        let normalized_suffix = suffix.normalized()?;
        let Some(mail_suffix_fragment) = suffix.mail_suffix_fragment() else {
            return Ok(normalized_suffix);
        };

        let owner_username = self
            .inner
            .owner_username
            .lock()
            .expect("owner_username lock poisoned")
            .clone()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        if owner_username.is_empty() {
            let error_message = if mail_suffix_fragment.is_empty() {
                "stream bootstrap did not provide owner_username required to resolve Suffix::linuxdo_space()"
            } else {
                "stream bootstrap did not provide owner_username required to resolve Suffix::linuxdo_space().with_suffix(...)"
            };
            return Err(LinuxDoSpaceError::stream(error_message));
        }

        Ok(format!(
            "{owner_username}-mail{mail_suffix_fragment}.{normalized_suffix}"
        ))
    }

    /// Returns mailbox bindings matched by one message address using current local binding state.
    pub fn route(&self, message: &MailMessage) -> Vec<Mailbox> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Vec::new();
        }
        let normalized = message.address.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Vec::new();
        }
        match_bindings(&self.inner, &normalized)
            .into_iter()
            .filter_map(|binding| binding.mailbox.upgrade().map(|inner| Mailbox { inner }))
            .collect()
    }

    /// Full intake listener for all stream messages.
    pub fn listen(&self, timeout: Option<Duration>) -> Result<ClientListener, LinuxDoSpaceError> {
        self.ensure_open()?;
        if let Some(error) = self
            .inner
            .fatal_error
            .lock()
            .expect("fatal_error lock poisoned")
            .clone()
        {
            return Err(error);
        }
        let listener_id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::sync_channel::<ListenerItem>(LISTENER_BUFFER_SIZE);
        self.inner
            .full_listeners
            .lock()
            .expect("full_listeners lock poisoned")
            .insert(listener_id, tx);
        Ok(ClientListener {
            client: Arc::downgrade(&self.inner),
            receiver: rx,
            listener_id,
            deadline: timeout.map(|item| Instant::now() + item),
            finished: false,
        })
    }

    fn ensure_open(&self) -> Result<(), LinuxDoSpaceError> {
        if let Some(error) = self
            .inner
            .fatal_error
            .lock()
            .expect("fatal_error lock poisoned")
            .clone()
        {
            return Err(error);
        }
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(LinuxDoSpaceError::closed("client is already closed"));
        }
        Ok(())
    }
}

/// Client full-stream listener iterator.
pub struct ClientListener {
    client: Weak<ClientInner>,
    receiver: Receiver<ListenerItem>,
    listener_id: usize,
    deadline: Option<Instant>,
    finished: bool,
}

impl Iterator for ClientListener {
    type Item = Result<MailMessage, LinuxDoSpaceError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match recv_listener_item(&self.receiver, self.deadline) {
            Ok(ListenerItem::Message(message)) => Some(Ok(message)),
            Err(RecvTimeoutError::Timeout) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
                if let Some(client) = self.client.upgrade() {
                    if let Some(error) = client
                        .fatal_error
                        .lock()
                        .expect("fatal_error lock poisoned")
                        .clone()
                    {
                        return Some(Err(error));
                    }
                }
                None
            }
        }
    }
}

impl Drop for ClientListener {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client
                .full_listeners
                .lock()
                .expect("full_listeners lock poisoned")
                .remove(&self.listener_id);
        }
    }
}

#[derive(Debug)]
struct MailboxInner {
    id: usize,
    client: Weak<ClientInner>,
    mode: BindingMode,
    suffix: String,
    allow_overlap: bool,
    prefix: Option<String>,
    pattern_text: Option<String>,
    listener_sender: Mutex<Option<SyncSender<ListenerItem>>>,
    closed: AtomicBool,
    dropped: AtomicU64,
}

/// One local mailbox binding.
#[derive(Debug, Clone)]
pub struct Mailbox {
    inner: Arc<MailboxInner>,
}

impl Mailbox {
    pub fn mode(&self) -> &'static str {
        match self.inner.mode {
            BindingMode::Exact => "exact",
            BindingMode::Pattern => "pattern",
        }
    }

    pub fn suffix(&self) -> &str {
        &self.inner.suffix
    }

    pub fn allow_overlap(&self) -> bool {
        self.inner.allow_overlap
    }

    pub fn prefix(&self) -> Option<&str> {
        self.inner.prefix.as_deref()
    }

    pub fn pattern(&self) -> Option<&str> {
        self.inner.pattern_text.as_deref()
    }

    pub fn address(&self) -> Option<String> {
        self.inner
            .prefix
            .as_ref()
            .map(|prefix| format!("{prefix}@{}", self.inner.suffix))
    }

    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Returns the number of messages dropped because this mailbox listener was full.
    pub fn dropped(&self) -> u64 {
        self.inner.dropped.load(Ordering::SeqCst)
    }

    /// Starts mailbox listener. One mailbox allows one active listener at a time.
    pub fn listen(&self, timeout: Option<Duration>) -> Result<MailboxListener, LinuxDoSpaceError> {
        if self.inner.closed.load(Ordering::SeqCst) {
            return Err(LinuxDoSpaceError::closed("mailbox is already closed"));
        }

        let mut guard = self
            .inner
            .listener_sender
            .lock()
            .expect("mailbox listener_sender lock poisoned");
        if guard.is_some() {
            return Err(LinuxDoSpaceError::validation(
                "mailbox already has an active listener",
            ));
        }
        let (tx, rx) = mpsc::sync_channel::<ListenerItem>(LISTENER_BUFFER_SIZE);
        *guard = Some(tx);
        drop(guard);

        Ok(MailboxListener {
            mailbox: Arc::downgrade(&self.inner),
            receiver: rx,
            deadline: timeout.map(|item| Instant::now() + item),
            finished: false,
        })
    }

    /// Explicitly unregisters this mailbox.
    pub fn close(&self) -> Result<(), LinuxDoSpaceError> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Some(client) = self.inner.client.upgrade() {
            let removed = unregister_binding(&client, self.inner.id);
            if removed {
                let _ = sync_remote_mailbox_filters(&client, false);
            }
        }

        self.inner
            .listener_sender
            .lock()
            .expect("mailbox listener_sender lock poisoned")
            .take();
        Ok(())
    }
}

/// Mailbox listener iterator.
pub struct MailboxListener {
    mailbox: Weak<MailboxInner>,
    receiver: Receiver<ListenerItem>,
    deadline: Option<Instant>,
    finished: bool,
}

impl Iterator for MailboxListener {
    type Item = Result<MailMessage, LinuxDoSpaceError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        match recv_listener_item(&self.receiver, self.deadline) {
            Ok(ListenerItem::Message(message)) => Some(Ok(message)),
            Err(RecvTimeoutError::Timeout) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
                if let Some(mailbox) = self.mailbox.upgrade() {
                    if let Some(client) = mailbox.client.upgrade() {
                        if let Some(error) = client
                            .fatal_error
                            .lock()
                            .expect("fatal_error lock poisoned")
                            .clone()
                        {
                            return Some(Err(error));
                        }
                    }
                }
                None
            }
        }
    }
}

impl Drop for MailboxListener {
    fn drop(&mut self) {
        if let Some(mailbox) = self.mailbox.upgrade() {
            mailbox
                .listener_sender
                .lock()
                .expect("mailbox listener_sender lock poisoned")
                .take();
        }
    }
}

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(flatten)]
    payload: HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
struct ParsedEnvelope {
    sender: String,
    recipients: Vec<String>,
    received_at: DateTime<Utc>,
    subject: String,
    message_id: Option<String>,
    date: Option<DateTime<Utc>>,
    from_header: String,
    to_header: String,
    cc_header: String,
    reply_to_header: String,
    from_addresses: Vec<String>,
    to_addresses: Vec<String>,
    cc_addresses: Vec<String>,
    reply_to_addresses: Vec<String>,
    text: String,
    html: String,
    headers: HashMap<String, String>,
    raw: String,
    raw_bytes: Vec<u8>,
}

impl ParsedEnvelope {
    fn to_message(&self, address: String) -> MailMessage {
        MailMessage {
            address,
            sender: self.sender.clone(),
            recipients: self.recipients.clone(),
            received_at: self.received_at,
            subject: self.subject.clone(),
            message_id: self.message_id.clone(),
            date: self.date,
            from_header: self.from_header.clone(),
            to_header: self.to_header.clone(),
            cc_header: self.cc_header.clone(),
            reply_to_header: self.reply_to_header.clone(),
            from_addresses: self.from_addresses.clone(),
            to_addresses: self.to_addresses.clone(),
            cc_addresses: self.cc_addresses.clone(),
            reply_to_addresses: self.reply_to_addresses.clone(),
            text: self.text.clone(),
            html: self.html.clone(),
            headers: self.headers.clone(),
            raw: self.raw.clone(),
            raw_bytes: self.raw_bytes.clone(),
        }
    }
}

fn recv_listener_item(
    receiver: &Receiver<ListenerItem>,
    deadline: Option<Instant>,
) -> Result<ListenerItem, RecvTimeoutError> {
    if let Some(deadline) = deadline {
        let now = Instant::now();
        if now >= deadline {
            return Err(RecvTimeoutError::Timeout);
        }
        receiver.recv_timeout(deadline - now)
    } else {
        receiver.recv().map_err(|_| RecvTimeoutError::Disconnected)
    }
}

async fn run_stream_loop(
    client: Arc<ClientInner>,
    ready_tx: Option<SyncSender<Result<(), LinuxDoSpaceError>>>,
) {
    let mut ready_sent = false;
    let mut ready_tx = ready_tx;

    while !client.closed.load(Ordering::SeqCst) {
        match consume_stream_once(&client, &mut ready_sent, &mut ready_tx).await {
            Ok(()) => {
                if client.closed.load(Ordering::SeqCst) {
                    return;
                }
            }
            Err(StreamLoopError::Fatal(error)) => {
                client.connected.store(false, Ordering::SeqCst);
                if !ready_sent {
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(Err(error.clone()));
                    }
                }
                set_fatal_error(&client, error);
                return;
            }
            Err(StreamLoopError::Recoverable(error)) => {
                client.connected.store(false, Ordering::SeqCst);
                if !ready_sent {
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(Err(error));
                    }
                    return;
                }
                tokio::select! {
                    _ = client.shutdown.cancelled() => return,
                    _ = sleep(RECONNECT_DELAY) => {}
                }
            }
        }
    }
}

async fn consume_stream_once(
    client: &Arc<ClientInner>,
    ready_sent: &mut bool,
    ready_tx: &mut Option<SyncSender<Result<(), LinuxDoSpaceError>>>,
) -> Result<(), StreamLoopError> {
    let mut response = open_stream(client).await?;
    client.connected.store(true, Ordering::SeqCst);

    let mut buffered = Vec::<u8>::new();
    loop {
        tokio::select! {
            _ = client.shutdown.cancelled() => {
                return Ok(());
            }
            chunk_result = timeout(DEFAULT_IDLE_TIMEOUT, response.chunk()) => {
                let chunk_result = chunk_result.map_err(|_| {
                    StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
                        "mail stream stalled and will reconnect",
                    ))
                })?;

                match chunk_result {
                    Ok(Some(chunk)) => {
                        buffered.extend_from_slice(&chunk);
                        while let Some(line) = take_next_line(&mut buffered) {
                            let ready_received = handle_stream_line(client, &line)?;
                            if ready_received && !*ready_sent {
                                if let Some(tx) = ready_tx.take() {
                                    let _ = tx.send(Ok(()));
                                }
                                *ready_sent = true;
                            }
                        }
                    }
                    Ok(None) => {
                        if let Some(line) = take_final_line(&mut buffered) {
                            let ready_received = handle_stream_line(client, &line)?;
                            if ready_received && !*ready_sent {
                                if let Some(tx) = ready_tx.take() {
                                    let _ = tx.send(Ok(()));
                                }
                                *ready_sent = true;
                            }
                        }
                        if !*ready_sent {
                            return Err(StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
                                "initial mail stream ended before ready event",
                            )));
                        }
                        return Err(StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
                            "stream closed by remote peer; reconnecting",
                        )));
                    }
                    Err(err) => {
                        return Err(StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
                            format!("failed while reading stream: {err}"),
                        )));
                    }
                }
            }
        }
    }
}

async fn open_stream(client: &ClientInner) -> Result<Response, StreamLoopError> {
    let url = format!("{}{}", client.base_url, STREAM_PATH);
    let request = client
        .http
        .get(url)
        .header(AUTHORIZATION, format!("Bearer {}", client.token))
        .header(ACCEPT, "application/x-ndjson")
        .header(CACHE_CONTROL, "no-store")
        .header(USER_AGENT, "linuxdospace-rust-sdk/0.1.0");

    let response = timeout(DEFAULT_CONNECT_TIMEOUT, request.send())
        .await
        .map_err(|_| {
            StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
                "timed out while opening the LinuxDoSpace HTTPS mail stream",
            ))
        })?
        .map_err(|err| {
            StreamLoopError::Recoverable(LinuxDoSpaceError::stream(format!(
                "failed to open stream: {err}"
            )))
        })?;

    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(StreamLoopError::Fatal(LinuxDoSpaceError::auth(
            "api token was rejected by backend",
        )));
    }
    if !status.is_success() {
        return Err(StreamLoopError::Recoverable(LinuxDoSpaceError::stream(
            format!("unexpected stream status code: {}", status.as_u16()),
        )));
    }
    Ok(response)
}

fn handle_stream_line(client: &ClientInner, line: &[u8]) -> Result<bool, StreamLoopError> {
    let trimmed = std::str::from_utf8(line)
        .map_err(|err| {
            StreamLoopError::Fatal(LinuxDoSpaceError::stream(format!(
                "received invalid UTF-8 from stream: {err}"
            )))
        })?
        .trim();
    if trimmed.is_empty() {
        return Ok(false);
    }

    let event: StreamEvent = serde_json::from_str(trimmed).map_err(|err| {
        StreamLoopError::Fatal(LinuxDoSpaceError::stream(format!(
            "received invalid JSON from stream: {err}"
        )))
    })?;
    match event.event_type.as_str() {
        "ready" => {
            let owner_username = event
                .payload
                .get("owner_username")
                .and_then(|value| value.as_str())
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if owner_username.is_empty() {
                return Err(StreamLoopError::Fatal(LinuxDoSpaceError::stream(
                    "ready event did not include owner_username",
                )));
            }
            *client
                .owner_username
                .lock()
                .expect("owner_username lock poisoned") = Some(owner_username);
            Ok(true)
        }
        "heartbeat" => Ok(false),
        "mail" => {
            let envelope = parse_mail_event(&event).map_err(StreamLoopError::Fatal)?;
            dispatch_envelope(client, envelope);
            Ok(false)
        }
        _ => Ok(false),
    }
}

fn take_next_line(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    let newline_index = buffer.iter().position(|item| *item == b'\n')?;
    let mut line = buffer.drain(..=newline_index).collect::<Vec<_>>();
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    Some(line)
}

fn take_final_line(buffer: &mut Vec<u8>) -> Option<Vec<u8>> {
    if buffer.is_empty() {
        return None;
    }
    let mut line = std::mem::take(buffer);
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    if line.is_empty() {
        return None;
    }
    Some(line)
}

fn set_fatal_error(client: &ClientInner, error: LinuxDoSpaceError) {
    {
        let mut fatal_error = client
            .fatal_error
            .lock()
            .expect("fatal_error lock poisoned");
        if fatal_error.is_none() {
            *fatal_error = Some(error);
        }
    }
    client.closed.store(true, Ordering::SeqCst);
    client.connected.store(false, Ordering::SeqCst);
    client.shutdown.cancel();
    close_local_state(client);
}

fn close_local_state(client: &ClientInner) {
    client
        .full_listeners
        .lock()
        .expect("full_listeners lock poisoned")
        .clear();

    let bindings = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned")
        .drain()
        .flat_map(|(_, entries)| entries)
        .collect::<Vec<_>>();

    let mut seen = HashSet::<usize>::new();
    for binding in bindings {
        if !seen.insert(binding.id) {
            continue;
        }
        if let Some(mailbox) = binding.mailbox.upgrade() {
            mailbox.closed.store(true, Ordering::SeqCst);
            mailbox
                .listener_sender
                .lock()
                .expect("mailbox listener_sender lock poisoned")
                .take();
        }
    }
}

fn dispatch_envelope(client: &ClientInner, envelope: ParsedEnvelope) {
    let primary_address = envelope.recipients.first().cloned().unwrap_or_default();
    broadcast_to_full_listeners(
        client,
        ListenerItem::Message(envelope.to_message(primary_address)),
    );

    let mut delivered = HashSet::<String>::new();
    for recipient in &envelope.recipients {
        let normalized = recipient.trim().to_ascii_lowercase();
        if normalized.is_empty() || !delivered.insert(normalized.clone()) {
            continue;
        }
        let message = envelope.to_message(normalized.clone());
        for binding in match_bindings(client, &normalized) {
            if let Some(mailbox) = binding.mailbox.upgrade() {
                enqueue_mailbox_message(&mailbox, ListenerItem::Message(message.clone()));
            }
        }
    }
}

fn broadcast_to_full_listeners(client: &ClientInner, item: ListenerItem) {
    let listeners = client
        .full_listeners
        .lock()
        .expect("full_listeners lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    for sender in listeners {
        match sender.try_send(item.clone()) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                client.full_dropped.fetch_add(1, Ordering::SeqCst);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

fn enqueue_mailbox_message(mailbox: &MailboxInner, item: ListenerItem) {
    let sender = mailbox
        .listener_sender
        .lock()
        .expect("mailbox listener_sender lock poisoned")
        .clone();
    if let Some(sender) = sender {
        match sender.try_send(item) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                mailbox.dropped.fetch_add(1, Ordering::SeqCst);
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }
}

fn match_bindings(client: &ClientInner, address: &str) -> Vec<BindingEntry> {
    let mut parts = address.split('@');
    let local_part = parts.next().unwrap_or_default();
    let suffix = parts.next().unwrap_or_default();
    if local_part.is_empty() || suffix.is_empty() || parts.next().is_some() {
        return Vec::new();
    }

    let chain = {
        let bindings = client
            .bindings_by_suffix
            .lock()
            .expect("bindings_by_suffix lock poisoned");
        let direct = bindings.get(suffix).cloned().unwrap_or_default();
        if !direct.is_empty() {
            direct
        } else {
            let owner_username = client
                .owner_username
                .lock()
                .expect("owner_username lock poisoned")
                .clone()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            if owner_username.is_empty() {
                Vec::new()
            } else {
                let semantic_legacy_suffix = format!("{owner_username}.linuxdo.space");
                let semantic_mail_suffix = format!("{owner_username}-mail.linuxdo.space");
                if suffix == semantic_legacy_suffix {
                    bindings
                        .get(&semantic_mail_suffix)
                        .cloned()
                        .unwrap_or_default()
                } else {
                    Vec::new()
                }
            }
        }
    };

    let mut matched = Vec::<BindingEntry>::new();
    for binding in chain {
        if !binding.matches(local_part) {
            continue;
        }
        matched.push(binding.clone());
        if !binding.allow_overlap {
            break;
        }
    }
    matched
}

fn unregister_binding(client: &ClientInner, binding_id: usize) -> bool {
    let mut map = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned");
    let mut removed = false;
    let suffixes = map.keys().cloned().collect::<Vec<_>>();
    for suffix in suffixes {
        if let Some(entries) = map.get_mut(&suffix) {
            let previous_len = entries.len();
            entries.retain(|entry| entry.id != binding_id);
            if entries.len() != previous_len {
                removed = true;
            }
            if entries.is_empty() {
                map.remove(&suffix);
            }
        }
    }
    removed
}

fn sync_remote_mailbox_filters(
    client: &ClientInner,
    strict: bool,
) -> Result<(), LinuxDoSpaceError> {
    if client.closed.load(Ordering::SeqCst) {
        return Ok(());
    }

    let owner_username = client
        .owner_username
        .lock()
        .expect("owner_username lock poisoned")
        .clone()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    if owner_username.is_empty() {
        return Ok(());
    }

    let fragments = collect_remote_mailbox_suffix_fragments(client, &owner_username);
    let mut synced_guard = client
        .synced_mailbox_suffix_fragments
        .lock()
        .expect("synced_mailbox_suffix_fragments lock poisoned");
    if fragments.is_empty() && synced_guard.is_none() {
        return Ok(());
    }
    if synced_guard.as_ref().is_some_and(|item| item == &fragments) {
        return Ok(());
    }

    let response = client
        .filter_http
        .put(format!("{}{}", client.base_url, STREAM_FILTERS_PATH))
        .header(AUTHORIZATION, format!("Bearer {}", client.token))
        .header(ACCEPT, "application/json")
        .header(CONTENT_TYPE, "application/json")
        .json(&serde_json::json!({ "suffixes": fragments }))
        .send();

    match response {
        Ok(response) => {
            if !response.status().is_success() {
                let error = LinuxDoSpaceError::stream(format!(
                    "unexpected mailbox filter sync status code: {}",
                    response.status().as_u16()
                ));
                if strict {
                    return Err(error);
                }
                return Ok(());
            }
            *synced_guard = Some(fragments);
            Ok(())
        }
        Err(err) => {
            if strict {
                return Err(LinuxDoSpaceError::stream(format!(
                    "failed to synchronize remote mailbox filters: {err}"
                )));
            }
            Ok(())
        }
    }
}

fn collect_remote_mailbox_suffix_fragments(
    client: &ClientInner,
    owner_username: &str,
) -> Vec<String> {
    let canonical_prefix = format!("{owner_username}-mail");
    let suffixes = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    let mut fragments = HashSet::<String>::new();
    for suffix in suffixes {
        let normalized_suffix = suffix.trim().to_ascii_lowercase();
        if !normalized_suffix.ends_with(".linuxdo.space") {
            continue;
        }
        let label = &normalized_suffix[..normalized_suffix.len() - ".linuxdo.space".len()];
        if label.contains('.') || !label.starts_with(&canonical_prefix) {
            continue;
        }
        fragments.insert(label[canonical_prefix.len()..].to_string());
    }

    let mut sorted_fragments = fragments.into_iter().collect::<Vec<_>>();
    sorted_fragments.sort();
    sorted_fragments
}

fn normalize_base_url(raw: &str) -> Result<String, LinuxDoSpaceError> {
    let value = raw.trim().trim_end_matches('/').to_string();
    if value.is_empty() {
        return Err(LinuxDoSpaceError::validation("base_url must not be empty"));
    }

    let parsed = reqwest::Url::parse(&value)
        .map_err(|err| LinuxDoSpaceError::validation(format!("invalid base_url: {err}")))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(LinuxDoSpaceError::validation(
            "base_url must use http or https",
        ));
    }
    let host = parsed
        .host_str()
        .map(|item| item.to_ascii_lowercase())
        .ok_or_else(|| LinuxDoSpaceError::validation("base_url must include host"))?;
    if scheme == "http" {
        let localhost_allowed = host == "localhost"
            || host == "127.0.0.1"
            || host == "::1"
            || host.ends_with(".localhost");
        if !localhost_allowed {
            return Err(LinuxDoSpaceError::validation(
                "non-local base_url must use https",
            ));
        }
    }
    Ok(value)
}

fn normalize_prefix(raw: &str) -> Result<String, LinuxDoSpaceError> {
    let value = raw.trim().to_ascii_lowercase();
    if value.is_empty() {
        return Err(LinuxDoSpaceError::validation("prefix must not be empty"));
    }
    if value.contains('@') {
        return Err(LinuxDoSpaceError::validation(
            "prefix must be mailbox local-part only",
        ));
    }
    Ok(value)
}

fn parse_mail_event(event: &StreamEvent) -> Result<ParsedEnvelope, LinuxDoSpaceError> {
    let recipients = event
        .payload
        .get("original_recipients")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|value| value.as_str())
                .map(|value| value.trim().to_ascii_lowercase())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let sender = event
        .payload
        .get("original_envelope_from")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();

    let received_at_raw = event
        .payload
        .get("received_at")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();
    let received_at = DateTime::parse_from_rfc3339(&received_at_raw)
        .map(|item| item.with_timezone(&Utc))
        .map_err(|err| {
            LinuxDoSpaceError::stream(format!("invalid received_at timestamp: {err}"))
        })?;

    let raw_message_base64 = event
        .payload
        .get("raw_message_base64")
        .and_then(|value| value.as_str())
        .unwrap_or_default()
        .trim()
        .to_string();
    if raw_message_base64.is_empty() {
        return Err(LinuxDoSpaceError::stream(
            "mail event did not include raw_message_base64",
        ));
    }
    let raw_bytes = BASE64_STANDARD
        .decode(raw_message_base64.as_bytes())
        .map_err(|err| LinuxDoSpaceError::stream(format!("invalid base64 mail payload: {err}")))?;

    let parsed = mailparse::parse_mail(&raw_bytes)
        .map_err(|err| LinuxDoSpaceError::stream(format!("failed to parse MIME message: {err}")))?;

    let headers = parsed
        .headers
        .iter()
        .map(|header| (header.get_key().to_string(), header.get_value()))
        .collect::<HashMap<_, _>>();

    let from_header = headers.get("From").cloned().unwrap_or_default();
    let to_header = headers.get("To").cloned().unwrap_or_default();
    let cc_header = headers.get("Cc").cloned().unwrap_or_default();
    let reply_to_header = headers.get("Reply-To").cloned().unwrap_or_default();

    let from_addresses = parse_addresses(&from_header);
    let to_addresses = parse_addresses(&to_header);
    let cc_addresses = parse_addresses(&cc_header);
    let reply_to_addresses = parse_addresses(&reply_to_header);

    let message_id = headers
        .get("Message-ID")
        .cloned()
        .filter(|item| !item.trim().is_empty());
    let date = headers
        .get("Date")
        .and_then(|value| mailparse::dateparse(value).ok())
        .and_then(|timestamp| Utc.timestamp_opt(timestamp, 0).single());

    let subject = headers.get("Subject").cloned().unwrap_or_default();

    let (text, html) = extract_message_bodies(&parsed);
    let raw = String::from_utf8_lossy(&raw_bytes).to_string();

    Ok(ParsedEnvelope {
        sender,
        recipients,
        received_at,
        subject,
        message_id,
        date,
        from_header,
        to_header,
        cc_header,
        reply_to_header,
        from_addresses,
        to_addresses,
        cc_addresses,
        reply_to_addresses,
        text,
        html,
        headers,
        raw,
        raw_bytes,
    })
}

fn parse_addresses(header_value: &str) -> Vec<String> {
    if header_value.trim().is_empty() {
        return Vec::new();
    }
    let parsed = mailparse::addrparse(header_value);
    let Ok(addresses) = parsed else {
        return Vec::new();
    };

    let mut result = Vec::<String>::new();
    for item in addresses.iter() {
        flatten_address(item, &mut result);
    }
    result
}

fn flatten_address(address: &mailparse::MailAddr, out: &mut Vec<String>) {
    match address {
        mailparse::MailAddr::Single(info) => {
            let value = info.addr.trim().to_ascii_lowercase();
            if !value.is_empty() {
                out.push(value);
            }
        }
        mailparse::MailAddr::Group(group) => {
            for item in &group.addrs {
                let value = item.addr.trim().to_ascii_lowercase();
                if !value.is_empty() {
                    out.push(value);
                }
            }
        }
    }
}

fn extract_message_bodies(parsed: &mailparse::ParsedMail<'_>) -> (String, String) {
    let mut text_parts = Vec::<String>::new();
    let mut html_parts = Vec::<String>::new();
    collect_message_parts(parsed, &mut text_parts, &mut html_parts);
    (
        text_parts.join("\n").trim().to_string(),
        html_parts.join("\n").trim().to_string(),
    )
}

fn collect_message_parts(
    parsed: &mailparse::ParsedMail<'_>,
    text_parts: &mut Vec<String>,
    html_parts: &mut Vec<String>,
) {
    if parsed.subparts.is_empty() {
        let ctype = parsed.ctype.mimetype.to_ascii_lowercase();
        let disposition = parsed.get_content_disposition().disposition;
        if disposition == mailparse::DispositionType::Attachment {
            return;
        }
        if let Ok(body) = parsed.get_body() {
            if ctype == "text/html" {
                html_parts.push(body);
            } else if ctype == "text/plain" || ctype.starts_with("text/") {
                text_parts.push(body);
            }
        }
        return;
    }

    for part in &parsed.subparts {
        collect_message_parts(part, text_parts, html_parts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::Read;
    use std::io::Write;
    use std::net::TcpListener;
    use std::sync::Arc;

    const TEST_OWNER_USERNAME: &str = "testuser";
    const TEST_NAMESPACE_SUFFIX: &str = "testuser.linuxdo.space";

    #[test]
    fn suffix_parses() {
        assert_eq!(
            Suffix::from_str("linuxdo.space").unwrap(),
            Suffix::LinuxdoSpace
        );
        assert_eq!(
            Suffix::from_str("OpenApi.Best").unwrap(),
            Suffix::Custom("openapi.best".to_string())
        );
    }

    #[test]
    fn semantic_suffix_with_suffix_normalizes_fragment() {
        assert_eq!(
            Suffix::linuxdo_space().with_suffix(" Foo__Bar ").unwrap(),
            Suffix::LinuxdoSpaceWithFragment("foo-bar".to_string())
        );
    }

    #[test]
    fn base_url_validation_enforces_https_for_remote() {
        let err = normalize_base_url("http://example.com")
            .unwrap_err()
            .to_string();
        assert!(err.contains("non-local base_url must use https"));
        assert!(normalize_base_url("http://localhost:8787").is_ok());
    }

    #[test]
    fn close_interrupts_idle_stream_promptly() {
        let server = TestServer::spawn(vec![ConnectionScript::ReadyThenIdle]);
        let client = Client::new("token", Some(&server.base_url())).unwrap();

        let started = Instant::now();
        client.close().unwrap();

        assert!(
            started.elapsed() < Duration::from_secs(1),
            "close() should not block on an idle upstream stream"
        );
    }

    #[test]
    fn invalid_json_after_connect_becomes_fatal() {
        let server = TestServer::spawn(vec![ConnectionScript::ReadyThenInvalidJson]);
        let client = Client::new("token", Some(&server.base_url())).unwrap();

        wait_until(Duration::from_secs(2), || client.error().is_some());
        let error = client.error().expect("fatal error should be recorded");
        assert!(
            error.to_string().contains("invalid JSON"),
            "unexpected fatal error: {error}"
        );
        assert!(client.listen(None).is_err());
        client.close().unwrap();
    }

    #[test]
    fn bounded_queues_report_drops() {
        let server = TestServer::spawn(vec![
            ConnectionScript::BurstMail { count: 512 },
            ConnectionScript::ReadyThenIdle,
        ]);
        let client = Client::new("token", Some(&server.base_url())).unwrap();
        let _all_listener = client.listen(None).unwrap();
        let mailbox = client
            .bind_prefix("alice", Suffix::linuxdo_space(), false)
            .unwrap();
        let _mailbox_listener = mailbox.listen(None).unwrap();

        wait_until(Duration::from_secs(2), || {
            client.dropped() > 0 || mailbox.dropped() > 0
        });

        assert!(client.dropped() > 0 || mailbox.dropped() > 0);
        client.close().unwrap();
    }

    #[test]
    fn semantic_suffix_also_matches_mail_namespace() {
        let server = TestServer::spawn(vec![ConnectionScript::ReadyThenIdle]);
        let client = Client::new("token", Some(&server.base_url())).unwrap();
        let mailbox = client
            .bind_prefix("alice", Suffix::linuxdo_space(), false)
            .unwrap();
        assert_eq!(
            mailbox.address().as_deref(),
            Some("alice@testuser-mail.linuxdo.space")
        );

        let message = MailMessage {
            address: "alice@testuser.linuxdo.space".to_string(),
            sender: "sender@example.com".to_string(),
            recipients: vec!["alice@testuser.linuxdo.space".to_string()],
            received_at: Utc::now(),
            subject: String::new(),
            message_id: None,
            date: None,
            from_header: String::new(),
            to_header: String::new(),
            cc_header: String::new(),
            reply_to_header: String::new(),
            from_addresses: Vec::new(),
            to_addresses: Vec::new(),
            cc_addresses: Vec::new(),
            reply_to_addresses: Vec::new(),
            text: String::new(),
            html: String::new(),
            headers: HashMap::new(),
            raw: String::new(),
            raw_bytes: Vec::new(),
        };

        let matched = client.route(&message);
        assert_eq!(matched.len(), 1);
        assert_eq!(
            matched[0].address().as_deref(),
            Some("alice@testuser-mail.linuxdo.space")
        );
        client.close().unwrap();
    }

    #[test]
    fn semantic_suffix_syncs_remote_mailbox_filters() {
        let server = TestServer::spawn(vec![ConnectionScript::ReadyThenIdle]);
        let client = Client::new("token", Some(&server.base_url())).unwrap();
        let default_mailbox = client
            .bind_prefix("alice", Suffix::linuxdo_space(), false)
            .unwrap();
        let dynamic_mailbox = client
            .bind_prefix(
                "ops",
                Suffix::linuxdo_space().with_suffix("Foo Bar").unwrap(),
                false,
            )
            .unwrap();

        assert_eq!(
            dynamic_mailbox.address().as_deref(),
            Some("ops@testuser-mailfoo-bar.linuxdo.space")
        );
        wait_until(Duration::from_secs(5), || server.filter_requests().len() >= 2);
        let filter_requests = server.filter_requests();
        assert_eq!(filter_requests[0], vec![String::new()]);
        assert_eq!(
            filter_requests[1],
            vec![String::new(), "foo-bar".to_string()]
        );

        default_mailbox.close().unwrap();
        wait_until(Duration::from_secs(5), || server.filter_requests().len() >= 3);
        let filter_requests = server.filter_requests();
        assert_eq!(filter_requests[2], vec!["foo-bar".to_string()]);
        client.close().unwrap();
    }

    fn wait_until(timeout: Duration, predicate: impl Fn() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("timed out while waiting for condition");
    }

    enum ConnectionScript {
        ReadyThenIdle,
        ReadyThenInvalidJson,
        BurstMail { count: usize },
    }

    struct TestServer {
        base_url: String,
        shutdown: Arc<AtomicBool>,
        filter_requests: Arc<Mutex<Vec<Vec<String>>>>,
        handle: Option<JoinHandle<()>>,
    }

    impl TestServer {
        fn spawn(scripts: Vec<ConnectionScript>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let address = listener.local_addr().unwrap();
            let shutdown = Arc::new(AtomicBool::new(false));
            let scripts = Arc::new(Mutex::new(VecDeque::from(scripts)));
            let filter_requests = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
            let thread_shutdown = Arc::clone(&shutdown);
            let thread_scripts = Arc::clone(&scripts);
            let thread_filter_requests = Arc::clone(&filter_requests);

            let handle = thread::spawn(move || {
                while !thread_shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let shutdown = Arc::clone(&thread_shutdown);
                            let scripts = Arc::clone(&thread_scripts);
                            let filter_requests = Arc::clone(&thread_filter_requests);
                            thread::spawn(move || {
                                let request = read_http_request(&mut stream).unwrap_or_default();
                                if request.starts_with("PUT /v1/token/email/filters ") {
                                    handle_filter_request(
                                        &mut stream,
                                        &request,
                                        &filter_requests,
                                    );
                                    return;
                                }
                                let script = scripts
                                    .lock()
                                    .expect("scripts lock poisoned")
                                    .pop_front()
                                    .unwrap_or(ConnectionScript::ReadyThenIdle);
                                handle_connection(&mut stream, script, &shutdown);
                            });
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => return,
                    }
                }
            });

            Self {
                base_url: format!("http://{}", address),
                shutdown,
                filter_requests,
                handle: Some(handle),
            }
        }

        fn base_url(&self) -> String {
            self.base_url.clone()
        }

        fn filter_requests(&self) -> Vec<Vec<String>> {
            self.filter_requests
                .lock()
                .expect("filter_requests lock poisoned")
                .clone()
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = std::net::TcpStream::connect(self.base_url.trim_start_matches("http://"));
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn handle_connection(
        stream: &mut std::net::TcpStream,
        script: ConnectionScript,
        shutdown: &AtomicBool,
    ) {
        match script {
            ConnectionScript::ReadyThenIdle => {
                write_stream_headers(stream);
                write_chunk(stream, ready_line().as_bytes());
                let _ = stream.flush();
                while !shutdown.load(Ordering::SeqCst) {
                    thread::sleep(Duration::from_millis(25));
                }
            }
            ConnectionScript::ReadyThenInvalidJson => {
                write_stream_headers(stream);
                write_chunk(stream, ready_line().as_bytes());
                write_chunk(stream, b"{not-json}\n");
                write_stream_end(stream);
            }
            ConnectionScript::BurstMail { count } => {
                write_stream_headers(stream);
                write_chunk(stream, ready_line().as_bytes());
                for index in 0..count {
                    let event = mail_line(
                        &format!("alice@{TEST_NAMESPACE_SUFFIX}"),
                        &format!("Subject {index}"),
                    );
                    write_chunk(stream, event.as_bytes());
                }
                write_stream_end(stream);
            }
        }
    }

    fn handle_filter_request(
        stream: &mut std::net::TcpStream,
        request: &str,
        filter_requests: &Arc<Mutex<Vec<Vec<String>>>>,
    ) {
        let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
        let payload = serde_json::from_str::<serde_json::Value>(body).unwrap_or_default();
        let suffixes = payload
            .get("suffixes")
            .and_then(|value| value.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|value| value.as_str())
                    .map(|value| value.to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        filter_requests
            .lock()
            .expect("filter_requests lock poisoned")
            .push(suffixes.clone());

        let domains = suffixes
            .iter()
            .map(|fragment| format!("{TEST_OWNER_USERNAME}-mail{fragment}.linuxdo.space"))
            .collect::<Vec<_>>();
        let response = serde_json::json!({
            "suffixes": suffixes,
            "domains": domains,
        })
        .to_string();
        write_json_response(stream, &response);
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> std::io::Result<String> {
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut buffer = [0_u8; 4096];
        let mut received = Vec::new();
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    received.extend_from_slice(&buffer[..read]);
                    if let Some(header_end) =
                        received.windows(4).position(|window| window == b"\r\n\r\n")
                    {
                        let header_end = header_end + 4;
                        let header_text = String::from_utf8_lossy(&received[..header_end]);
                        let content_length = header_text
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                if !name.eq_ignore_ascii_case("content-length") {
                                    return None;
                                }
                                value.trim().parse::<usize>().ok()
                            })
                            .unwrap_or(0);
                        if received.len() >= header_end + content_length {
                            break;
                        }
                    }
                }
                Err(err)
                    if err.kind() == std::io::ErrorKind::WouldBlock
                        || err.kind() == std::io::ErrorKind::TimedOut =>
                {
                    break;
                }
                Err(err) => return Err(err),
            }
        }
        Ok(String::from_utf8_lossy(&received).to_string())
    }

    fn write_stream_headers(stream: &mut std::net::TcpStream) {
        let _ = stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        );
    }

    fn write_chunk(stream: &mut std::net::TcpStream, payload: &[u8]) {
        let _ = stream.write_all(format!("{:X}\r\n", payload.len()).as_bytes());
        let _ = stream.write_all(payload);
        let _ = stream.write_all(b"\r\n");
    }

    fn write_stream_end(stream: &mut std::net::TcpStream) {
        let _ = stream.write_all(b"0\r\n\r\n");
        let _ = stream.flush();
    }

    fn write_json_response(stream: &mut std::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    fn ready_line() -> String {
        serde_json::json!({
            "type": "ready",
            "token_public_id": "tok_123",
            "owner_username": TEST_OWNER_USERNAME
        })
        .to_string()
            + "\n"
    }

    fn mail_line(recipient: &str, subject: &str) -> String {
        let raw = format!(
            "From: sender@example.com\r\nTo: {recipient}\r\nSubject: {subject}\r\n\r\nHello"
        );
        serde_json::json!({
            "type": "mail",
            "original_envelope_from": "sender@example.com",
            "original_recipients": [recipient],
            "received_at": "2026-03-20T10:11:12Z",
            "raw_message_base64": BASE64_STANDARD.encode(raw.as_bytes()),
        })
        .to_string()
            + "\n"
    }
}
