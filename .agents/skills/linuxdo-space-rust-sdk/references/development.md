# Development Guide

## Workdir

```bash
cd sdk/rust
```

## Validate

```bash
cargo fmt --all -- --check
cargo check --locked
cargo test --locked
cargo package --locked
```

## Release model

- Workflow file: `../../../.github/workflows/release.yml`
- Trigger: push tag `v*`
- Current release output is a `.crate` asset uploaded to GitHub Release
- There is no crates.io publication in the current workflow

## Keep aligned

- `../../../Cargo.toml`
- `../../../src/lib.rs`
- `../../../README.md`
- `../../../.github/workflows/ci.yml`
- `../../../.github/workflows/release.yml`
