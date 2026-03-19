//! LinuxDoSpace Rust SDK.
//!
//! This crate implements one strict runtime model:
//! - one `Client` owns one upstream HTTPS stream
//! - the client parses every mail event once
//! - local mailbox bindings run in-memory and do not create extra upstream connections

use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::io::{BufRead, BufReader};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use chrono::{DateTime, TimeZone, Utc};
use regex::Regex;
use reqwest::blocking::{Client as HttpClient, Response};
use reqwest::header::{ACCEPT, AUTHORIZATION, CACHE_CONTROL, USER_AGENT};
use serde::Deserialize;

const DEFAULT_BASE_URL: &str = "https://api.linuxdo.space";
const STREAM_PATH: &str = "/v1/token/email/stream";
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_millis(300);

/// One mail suffix value.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Suffix {
    /// The default LinuxDoSpace mail suffix.
    LinuxdoSpace,
    /// Any managed suffix string.
    Custom(String),
}

impl Suffix {
    /// Returns the default `linuxdo.space` suffix variant.
    pub fn linuxdo_space() -> Self {
        Self::LinuxdoSpace
    }

    fn normalized(&self) -> Result<String, LinuxDoSpaceError> {
        let raw = match self {
            Self::LinuxdoSpace => "linuxdo.space",
            Self::Custom(value) => value.as_str(),
        };
        let value = raw.trim().to_ascii_lowercase();
        if value.is_empty() {
            return Err(LinuxDoSpaceError::validation("suffix must not be empty"));
        }
        Ok(value)
    }
}

impl Display for Suffix {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LinuxdoSpace => f.write_str("linuxdo.space"),
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
    Failure(LinuxDoSpaceError),
    Closed,
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
    closed: AtomicBool,
    connected: AtomicBool,
    fatal_error: Mutex<Option<LinuxDoSpaceError>>,
    full_listeners: Mutex<HashMap<usize, Sender<ListenerItem>>>,
    bindings_by_suffix: Mutex<HashMap<String, Vec<BindingEntry>>>,
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
            .timeout(DEFAULT_READ_TIMEOUT)
            .build()
            .map_err(|err| {
                LinuxDoSpaceError::stream(format!("failed to build http client: {err}"))
            })?;

        let inner = Arc::new(ClientInner {
            token: normalized_token,
            base_url: normalized_base_url,
            http,
            closed: AtomicBool::new(false),
            connected: AtomicBool::new(false),
            fatal_error: Mutex::new(None),
            full_listeners: Mutex::new(HashMap::new()),
            bindings_by_suffix: Mutex::new(HashMap::new()),
            next_id: AtomicUsize::new(1),
        });

        let (ready_tx, ready_rx) = mpsc::channel::<Result<(), LinuxDoSpaceError>>();
        let thread_inner = Arc::clone(&inner);
        thread::spawn(move || run_stream_loop(thread_inner, Some(ready_tx)));

        match ready_rx.recv_timeout(DEFAULT_CONNECT_TIMEOUT + Duration::from_secs(1)) {
            Ok(result) => {
                result?;
                Ok(Self { inner })
            }
            Err(_) => Err(LinuxDoSpaceError::stream(
                "timed out while opening the LinuxDoSpace HTTPS mail stream",
            )),
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

    /// Closes the client and all listeners.
    pub fn close(&self) -> Result<(), LinuxDoSpaceError> {
        if self.inner.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.inner.connected.store(false, Ordering::SeqCst);
        broadcast_control(&self.inner, ListenerItem::Closed);
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
            suffix.normalized()?,
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
            suffix.normalized()?,
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
            binding_map.entry(suffix).or_default().push(entry);
        }

        Ok(Mailbox {
            inner: mailbox_inner,
        })
    }

    /// Returns mailbox bindings matched by one message address using current local binding state.
    pub fn route(&self, message: &MailMessage) -> Vec<Mailbox> {
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
        let (tx, rx) = mpsc::channel::<ListenerItem>();
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
            Ok(ListenerItem::Failure(error)) => {
                self.finished = true;
                Some(Err(error))
            }
            Ok(ListenerItem::Closed) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Timeout) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
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
    listener_sender: Mutex<Option<Sender<ListenerItem>>>,
    closed: AtomicBool,
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
        let (tx, rx) = mpsc::channel::<ListenerItem>();
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
            unregister_binding(&client, self.inner.id);
        }

        if let Some(sender) = self
            .inner
            .listener_sender
            .lock()
            .expect("mailbox listener_sender lock poisoned")
            .take()
        {
            let _ = sender.send(ListenerItem::Closed);
        }
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
            Ok(ListenerItem::Failure(error)) => {
                self.finished = true;
                Some(Err(error))
            }
            Ok(ListenerItem::Closed) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Timeout) => {
                self.finished = true;
                None
            }
            Err(RecvTimeoutError::Disconnected) => {
                self.finished = true;
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

fn run_stream_loop(
    client: Arc<ClientInner>,
    ready_tx: Option<Sender<Result<(), LinuxDoSpaceError>>>,
) {
    let mut ready_sent = false;
    let mut ready_tx = ready_tx;

    while !client.closed.load(Ordering::SeqCst) {
        let stream_result = consume_stream_once(&client);
        match stream_result {
            Ok(()) => {
                if !ready_sent {
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(Ok(()));
                    }
                    ready_sent = true;
                }
            }
            Err(error) => {
                client.connected.store(false, Ordering::SeqCst);

                if !ready_sent {
                    if let Some(tx) = ready_tx.take() {
                        let _ = tx.send(Err(error.clone()));
                    }
                    ready_sent = true;
                }

                match &error {
                    LinuxDoSpaceError::Authentication(_) => {
                        *client
                            .fatal_error
                            .lock()
                            .expect("fatal_error lock poisoned") = Some(error.clone());
                        broadcast_control(&client, ListenerItem::Failure(error));
                        return;
                    }
                    _ => {
                        if client.closed.load(Ordering::SeqCst) {
                            return;
                        }
                        thread::sleep(RECONNECT_DELAY);
                    }
                }
            }
        }
    }
}

fn consume_stream_once(client: &ClientInner) -> Result<(), LinuxDoSpaceError> {
    let mut response = open_stream(client)?;
    client.connected.store(true, Ordering::SeqCst);

    let mut reader = BufReader::new(&mut response);
    let mut line = String::new();
    loop {
        if client.closed.load(Ordering::SeqCst) {
            return Ok(());
        }
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => {
                return Err(LinuxDoSpaceError::stream(
                    "stream closed by remote peer; reconnecting",
                ));
            }
            Ok(_) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let event: StreamEvent = serde_json::from_str(trimmed).map_err(|err| {
                    LinuxDoSpaceError::stream(format!("received invalid JSON from stream: {err}"))
                })?;
                match event.event_type.as_str() {
                    "ready" | "heartbeat" => continue,
                    "mail" => {
                        let envelope = parse_mail_event(&event)?;
                        dispatch_envelope(client, envelope);
                    }
                    _ => continue,
                }
            }
            Err(err) => {
                return Err(LinuxDoSpaceError::stream(format!(
                    "failed while reading stream: {err}"
                )));
            }
        }
    }
}

fn open_stream(client: &ClientInner) -> Result<Response, LinuxDoSpaceError> {
    let url = format!("{}{}", client.base_url, STREAM_PATH);
    let response = client
        .http
        .get(url)
        .header(AUTHORIZATION, format!("Bearer {}", client.token))
        .header(ACCEPT, "application/x-ndjson")
        .header(CACHE_CONTROL, "no-store")
        .header(USER_AGENT, "linuxdospace-rust-sdk/0.1.0")
        .send()
        .map_err(|err| LinuxDoSpaceError::stream(format!("failed to open stream: {err}")))?;

    let status = response.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(LinuxDoSpaceError::auth("api token was rejected by backend"));
    }
    if !status.is_success() {
        return Err(LinuxDoSpaceError::stream(format!(
            "unexpected stream status code: {}",
            status.as_u16()
        )));
    }
    Ok(response)
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
        let _ = sender.send(item.clone());
    }
}

fn broadcast_control(client: &ClientInner, item: ListenerItem) {
    let full_listeners = client
        .full_listeners
        .lock()
        .expect("full_listeners lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();

    let all_bindings = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned")
        .values()
        .flat_map(|entries| entries.iter().cloned())
        .collect::<Vec<_>>();

    for sender in full_listeners {
        let _ = sender.send(item.clone());
    }
    for binding in all_bindings {
        if let Some(mailbox) = binding.mailbox.upgrade() {
            enqueue_mailbox_message(&mailbox, item.clone());
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
        let _ = sender.send(item);
    }
}

fn match_bindings(client: &ClientInner, address: &str) -> Vec<BindingEntry> {
    let mut parts = address.split('@');
    let local_part = parts.next().unwrap_or_default();
    let suffix = parts.next().unwrap_or_default();
    if local_part.is_empty() || suffix.is_empty() || parts.next().is_some() {
        return Vec::new();
    }

    let chain = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned")
        .get(suffix)
        .cloned()
        .unwrap_or_default();

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

fn unregister_binding(client: &ClientInner, binding_id: usize) {
    let mut map = client
        .bindings_by_suffix
        .lock()
        .expect("bindings_by_suffix lock poisoned");
    let suffixes = map.keys().cloned().collect::<Vec<_>>();
    for suffix in suffixes {
        if let Some(entries) = map.get_mut(&suffix) {
            entries.retain(|entry| entry.id != binding_id);
            if entries.is_empty() {
                map.remove(&suffix);
            }
        }
    }
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
    fn base_url_validation_enforces_https_for_remote() {
        let err = normalize_base_url("http://example.com")
            .unwrap_err()
            .to_string();
        assert!(err.contains("non-local base_url must use https"));
        assert!(normalize_base_url("http://localhost:8787").is_ok());
    }
}
