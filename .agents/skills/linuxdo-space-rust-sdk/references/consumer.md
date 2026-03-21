# Consumer Guide

## Integrate

This crate is a library crate. Prefer path dependencies, workspace integration,
or GitHub Release crate assets over CLI-style installation wording.

Import shape:

```rust
use linuxdospace::{Client, Suffix};
```

## Full stream

```rust
let client = Client::new("lds_pat...", None)?;
let mut listener = client.listen(Some(std::time::Duration::from_secs(30)))?;

for item in &mut listener {
    let message = item?;
    println!("{}", message.address);
}

client.close()?;
```

## Mailbox binding

```rust
let mailbox = client.bind_prefix("alice", Suffix::linuxdo_space(), false)?;
let mut listener = mailbox.listen(Some(std::time::Duration::from_secs(30)))?;
```

## Key semantics

- Timeouts return `None` from the iterator instead of raising an error.
- `route(&MailMessage)` is local matching only.
- Full-stream messages use a primary projection address.
- Mailbox messages use matched-recipient projection addresses.
