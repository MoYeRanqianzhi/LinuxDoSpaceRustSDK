# Task Templates

## Create one exact mailbox

```rust
let alice = client.bind_prefix("alice", Suffix::linuxdo_space(), false)?;
```

## Create one catch-all

```rust
let catch_all = client.bind_pattern(".*", Suffix::linuxdo_space(), true)?;
```

## Route one message locally

```rust
let targets = client.route(&message);
```
