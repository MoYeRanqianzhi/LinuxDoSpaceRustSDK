---
name: linuxdo-space-rust-sdk
description: Use when writing or fixing Rust code that consumes or maintains the LinuxDoSpace Rust SDK under sdk/rust. Use for crate integration, Client::new usage, full-stream listeners, mailbox bindings, allow_overlap semantics, lifecycle/error handling, release guidance, and local validation.
---

# LinuxDoSpace Rust SDK

Read [references/consumer.md](references/consumer.md) first for normal SDK usage.
Read [references/api.md](references/api.md) for exact public Rust API names.
Read [references/examples.md](references/examples.md) for task-shaped snippets.
Read [references/development.md](references/development.md) only when editing `sdk/rust`.

## Workflow

1. Treat this crate as a library crate, not a CLI.
2. The SDK root relative to this `SKILL.md` is `../../../`.
3. Preserve these invariants:
   - one `Client` owns one upstream HTTPS stream
   - `Client::listen(timeout)` is the full-stream iterator
   - `bind_prefix(...)` / `bind_pattern(...)` create local mailbox bindings
   - `mailbox.listen(timeout)` consumes one mailbox iterator
   - mailbox queues activate only while mailbox listen is active
   - `Suffix::linuxdo_space()` is semantic and resolves after `ready.owner_username`
   - exact and regex bindings share one ordered chain per suffix
   - `allow_overlap=false` stops at first match; `true` continues
   - remote non-local `http://` base URLs are invalid
4. Keep README, Cargo metadata, source, tests, and workflows aligned when behavior changes.
5. Validate with the commands in `references/development.md`.

## Do Not Regress

- Do not document `cargo install`; this crate is a library.
- Do not document crates.io publication; current release output is GitHub Release `.crate` assets.
- Do not add hidden pre-listen mailbox buffering.
