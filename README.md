# LinuxDoSpace Rust SDK

Rust SDK for LinuxDoSpace token mail streaming over HTTPS.

The SDK follows the shared contract in:

- `sdk/spec/MAIL_STREAM_PROTOCOL.md`

Core runtime behavior:

- one `Client` owns one upstream stream to `/v1/token/email/stream`
- full stream intake is available through `client.listen(...)`
- exact and regex mailbox bindings share one ordered chain per suffix
- `allow_overlap = false` stops matching at first hit
- `allow_overlap = true` continues matching to later bindings
- mailbox queues activate only while mailbox `listen(...)` is active

## Install (local workspace)

```bash
cd sdk/rust
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

## Validation commands

```bash
cd sdk/rust
cargo fmt -- --check
cargo check
cargo test
```
