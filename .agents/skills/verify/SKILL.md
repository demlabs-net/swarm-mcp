---
name: verify
description: Build and CI verification for swarm-mcp. Use when asked to build, run tests, check formatting, run clippy, or verify the code passes the GitLab CI pipeline (quality/rustsec jobs).
---

# Verify swarm-mcp

The GitLab pipeline (`.gitlab-ci.yml`) runs three sequential stages. Reproduce them locally in this exact order:

```bash
cargo test --locked --all-targets              # 1. tests job
cargo fmt --all -- --check                     # 2. quality: formatting
cargo clippy --locked --all-targets -- -D warnings   # 3. quality: clippy, warnings denied
cargo build --locked --release                 # 4. quality: release build (image build sanity)
cargo audit --file Cargo.lock                  # 5. rustsec job (cargo-audit 0.22.2 on CI)
cargo llvm-cov --locked --all-targets --fail-under-lines 70 --summary-only  # 6. coverage job
```

## Toolchain notes

- `Cargo.toml` declares `rust-version = "1.89"`; the CI image is pinned `rust:1.89-bookworm` (see `.gitlab-ci.yml` and `Dockerfile`).
- The local toolchain may be newer. Known drift: clippy ≥ 1.90 adds `clippy::implicit_clone`, which used to fail `src/dispatch.rs` on newer toolchains while passing on CI's 1.89 (fixed — `header.clone()`). If a clippy failure mentions a lint you don't see in CI, that is toolchain drift — still fix it, and consider adding a `rust-toolchain.toml` to pin the toolchain.
- Use `--locked` so `Cargo.lock` is respected (it is committed and CI depends on it).

## CI pipeline layout

- `tests` (stage `test`) — `cargo test --locked --all-targets`.
- `quality` (stage `verify`) — fmt check, clippy with `-D warnings`, release build.
- `rustsec` (stage `audit`) — installs `cargo-audit` 0.22.2 into `$CARGO_HOME/bin` (cached via `.cargo/bin/` cache path) and audits `Cargo.lock`.
- `coverage` (stage `audit`) — `cargo-llvm-cov` (also cached in `.cargo/bin/`) with a 70% line threshold; run `cargo llvm-cov --summary-only` locally to see per-module numbers before pushing.
- Stages run sequentially, so three concurrent cargo builds never fight over the shared `target/` cache on a single runner.
- `CARGO_HOME` is `$CI_PROJECT_DIR/.cargo` so the `.cargo/registry/` and `.cargo/bin/` cache paths take effect; `CARGO_TERM_COLOR=always` keeps logs readable.

## Commands

- `swarm-mcp serve` — run the MCP server (default command).
- `swarm-mcp healthcheck [--port N]` — hits `/ready` on loopback; used by the Docker HEALTHCHECK. Needs `SWARM_MCP_PORT` if `--port` is not given.
- `swarm-mcp probe [--base-url URL]` — verifies tool/resource catalogs per role and cross-role token isolation against a running server.
- `swarm-mcp activity-probe [--base-url URL]` — verifies passive activity ingestion and supervisor visibility.

Probes require the full configuration environment (they load `Config::from_env`), so run them inside the deployed container or with the swarm `.env` loaded.

## Test layout

- Unit tests live in `#[cfg(test)]` modules inside each source file; they run without any external service (temp SQLite files, no network).
- `cargo test` must pass with no network access: store tests use `std::env::temp_dir()` + random UUID filenames and clean up `.db`, `-wal`, `-shm` files.
