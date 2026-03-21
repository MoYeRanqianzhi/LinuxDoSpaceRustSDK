# API Reference

## Paths

- SDK root: `../../../`
- Cargo metadata: `../../../Cargo.toml`
- Core implementation: `../../../src/lib.rs`
- Consumer README: `../../../README.md`

## Public surface

- Types:
  - `Suffix`
  - `MailMessage`
  - `AuthenticationError`
  - `StreamError`
  - `LinuxDoSpaceError`
  - `Client`
  - `ClientListener`
  - `Mailbox`
  - `MailboxListener`
- Client:
  - `Client::new(...)`
  - `connected()`
  - `error()`
  - `dropped()`
  - `listen(timeout)`
  - `bind_prefix(...)`
  - `bind_pattern(...)`
  - `route(&MailMessage)`
  - `close()`
- Mailbox:
  - `listen(timeout)`
  - `close()`
  - metadata accessors such as `mode()`, `suffix()`, `pattern()`, `address()`

## Semantics

- `Suffix::linuxdo_space()` is semantic, not literal.
- Exact and regex bindings share one ordered chain per suffix.
- Iterator timeouts end iteration with `None`.
- `allow_overlap=false` stops at the first match.
