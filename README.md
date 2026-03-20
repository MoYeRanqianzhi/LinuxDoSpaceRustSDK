# LinuxDoSpace Rust SDK

Rust SDK for LinuxDoSpace token mail streaming over HTTPS.

The SDK follows the shared contract in:

- `https://github.com/MoYeRanqianzhi/LinuxDoSpace/blob/main/sdk/spec/MAIL_STREAM_PROTOCOL.md`

Core runtime behavior:

- one `Client` owns one upstream stream to `/v1/token/email/stream`
- the upstream stream runs inside a dedicated background thread with a Tokio runtime
- full stream intake is available through `client.listen(...)`
- exact and regex mailbox bindings share one ordered chain per suffix
- `allow_overlap = false` stops matching at first hit
- `allow_overlap = true` continues matching to later bindings
- mailbox queues activate only while mailbox `listen(...)` is active
- stream open timeout and post-connect idle timeout are handled separately
- `client.close()` actively cancels the live stream instead of waiting for the socket to wake up
- protocol-decode failures after connect are treated as fatal, not endlessly retried

Important:

- `Suffix::linuxdo_space()` is semantic, not literal
- the SDK resolves it to `<owner_username>.linuxdo.space` after `ready.owner_username`
- listener queues are bounded; dropped full-stream messages can be inspected through `client.dropped()`

## Install (local workspace)

```bash
cargo check
```

## Quick start

```rust
use linuxdospace::{Client, Suffix};
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new("your-token", None)?;

    let mailbox = client.bind_prefix("alice", Suffix::linuxdo_space(), false)?;
    let mut listener = mailbox.listen(Some(Duration::from_secs(60)))?;

    while let Some(item) = listener.next() {
        let message = item?;
        println!("{} {}", message.address, message.subject);
    }

    mailbox.close()?;
    client.close()?;
    Ok(())
}
```

## Public API summary

- `Client`
- `Suffix`
- `MailMessage`
- `AuthenticationError`
- `StreamError`
- `LinuxDoSpaceError`
- `Mailbox`
- `ClientListener`
- `MailboxListener`

Useful observability helpers:

- `Client::error() -> Option<LinuxDoSpaceError>`
- `Client::dropped() -> u64`
- `Mailbox::dropped() -> u64`

## Validation commands

```bash
cargo fmt -- --check
cargo check
cargo test
```
