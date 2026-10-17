# Development

How to build, test, gate and release the Hop v3 workspace. See
[index.md](index.md) for the other documents; the crate structure itself is in
[architecture.md](architecture.md), the HopOS side in [hopos.md](hopos.md) and
the shipped apps in [apps.md](apps.md).

## Toolchain

`rust-toolchain.toml` pins the compiler for everything (local builds, the gate
and CI):

```toml
[toolchain]
channel = "1.93.0"
components = ["clippy", "rustfmt"]
targets = ["aarch64-unknown-none-softfloat", "riscv64gc-unknown-none-elf"]
```

`[workspace.package]` in the root `Cargo.toml` sets `edition = "2024"`,
`rust-version = "1.93"`, `license = "MIT"` and the shared `version`. Every
crate inherits these (`version.workspace = true`, ...).

Extra targets for host cross builds (`x86_64-unknown-linux-musl`,
`aarch64-unknown-linux-musl`) are not in the toolchain file; add them with
`rustup target add`.

## Workspace layout

One Cargo workspace (`resolver = "3"`), one `Cargo.lock`, one set of lints, one
release profile. The members fall into three groups:

| Group | Crates | Notes |
| --- | --- | --- |
| Logic (`no_std` + `alloc`) | `types`, `config`, `auth`, `leader`, `agent`, `api`, `discovery`, `runner`, `hopos-runner`, `hop-http`, `agentd-hopos` | `#![cfg_attr(not(test), no_std)]`; `runner` is `std` only with its `std` feature (the host backends: processes, docker, downloads, isolation). The same code runs as HopOS resident and as host daemon. |
| Host (`std`) | `hostnet`, `store`, `agentd`, `cli` (binary `hop`) | Sockets, lease/state storage, the daemon and the CLI. Excluded from the `no_std` build step of the gate. |
| Plugins and apps | `hoplib`, `apps/hopdns`, `apps/hoplb`, `apps/hopprom`, `apps/hoplockserver`, `apps/hop-gui/server` (crate `hop-gui`) | Each has a `std` (host daemon) and a `hopos` (resident) feature, and its own `tools/gate.sh`. |

`hoplib` and the apps were merged into this repository as subtrees (commit
`ed7cde8`); they now depend on Hop's crates by path instead of on a Hop tag.

### Dependency rules

- **Core never depends on `hoplib` or `apps/`.** The apps stand on Hop (`types`,
  `auth`, `hostnet`), never the other way round. `tools/gate.sh` enforces this.
- **Sibling repositories come in by git tag, never by path.** lean and HopOS are
  separate products, pinned once for the whole workspace in
  `[workspace.dependencies]`:

  | Crate | Repository | Tag |
  | --- | --- | --- |
  | `leanhttp`, `leanhttps`, `leantls`, `leans3`, `leans3http` | `github.com/xinix00/lean` | `v3.1.12` |
  | `applib`, `sync`, `abi` | `github.com/xinix00/HopOS` | `v3.0.13` |

  Members use `{ workspace = true }`. Within the repository, dependencies are
  paths. The gate rejects a git dependency without `tag =` and any `path =`
  that resolves outside the repository.

Bumping lean or HopOS means changing the tag on every line of that group in the
root `Cargo.toml` (they must stay equal: one `applib`/`sync` in the image).

## Lints and conventions

Code follows the haas.software Rust handbook (`rustdoc/README.md`, a sibling of
this repository, not part of it). The gate is handbook section 9; the "hard set"
of lints is section 12, configured once in the root `Cargo.toml` and inherited
by every crate with `[lints] workspace = true`.

| Table | Deny | Warn |
| --- | --- | --- |
| `rust` | `unsafe_op_in_unsafe_fn`, `non_ascii_idents` | `missing_docs`, `rust_2018_idioms`, `unreachable_pub` |
| `rustdoc` | | `missing_crate_level_docs`, `unescaped_backticks` |
| `clippy` | `undocumented_unsafe_blocks`, `await_holding_refcell_ref`, `await_holding_lock`, `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `large_stack_arrays` | `all`, `unnecessary_safety_comment`, `unnecessary_safety_doc`, `ptr_as_ptr`, `ptr_cast_constness`, `ref_as_ptr`, `as_ptr_cast_mut`, `as_underscore`, `cast_lossless`, `mut_mut`, `needless_bitwise_bool`, `no_mangle_with_rust_abi`, `ignored_unit_patterns` |

Clippy runs with `-D warnings` in the gate, so every warning is an error there.

Conventions that follow from this:

- No `unwrap`, `expect`, `panic!`, `todo!` or `unimplemented!` in non-test
  code. Errors are values; in `types` every allocation is fallible.
- Every `unsafe` block carries a `// SAFETY:` comment.
- Everything public is documented; crate-level docs are required.
- Identifiers are ASCII. Code comments are mostly Dutch; documentation under
  `docs/` is English.
- Test files opt out of the panic lints explicitly, with a reason, e.g.
  `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, reason = "een test faalt luid")]`.
- Nothing in the logic crates blocks or spins an executor inside a task
  (handbook section 4); the resident uses `.await` on kernel calls.

### Release profile

```toml
[profile.release]
panic = "abort"
overflow-checks = true
lto = true
codegen-units = 1
debug = true
```

Release builds keep overflow checks and debug info. CI overrides the debug info
per job (see [Release](#release)).

## Building

### Host binaries

```sh
cargo build --release -p agentd -p cli          # target/release/agentd, target/release/hop
```

Apps build with their `std` feature (default for `hoplb` and `hoplockserver`,
explicit for the others):

```sh
cargo build --release -p hoplockserver -p hoplb -p hopdns -p hopprom -p hop-gui \
  --features hopdns/std,hopprom/std,hop-gui/std
```

### Cross builds (static Linux)

musl targets link statically with `rust-lld`; no cross-gcc needed:

```sh
rustup target add aarch64-unknown-linux-musl
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
  cargo build --release -p agentd -p cli --target aarch64-unknown-linux-musl
```

The same works for `x86_64-unknown-linux-musl` with
`CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld`.

### HopOS residents

Residents are built for `aarch64-unknown-none-softfloat` (the toolchain also
carries `riscv64gc-unknown-none-elf`). The `build.rs` of `agentd-hopos` and of
each app passes
`-Thopapp.ld` (the app linker script that `applib` provides) only when
`target_os = "none"`; on the host the resident binary is an empty `main` that
exists so clippy and tests can typecheck it.

Hop itself:

```sh
cargo build --release --target aarch64-unknown-none-softfloat -p agentd-hopos
```

The release artifacts `hop-arm64.elf` / `hop-riscv64.elf` are **not** built
here: `hop-os/tools/hop-build.sh` in the HopOS repository builds them against
the `applib` of the HopOS working tree.

The apps (binary name `<app>-hopos`, feature `hopos`):

```sh
T=aarch64-unknown-none-softfloat
cargo build --release --target $T -p hopdns  --features hopos --bin hopdns-hopos
cargo build --release --target $T -p hopprom --features hopos --bin hopprom-hopos
cargo build --release --target $T -p hop-gui --features hopos --bin hop-gui-hopos
cargo build --release --target $T -p hoplb         --no-default-features --features hopos --bin hoplb-hopos
cargo build --release --target $T -p hoplockserver --no-default-features --features hopos --bin hoplockserver-hopos
```

`hoplb` and `hoplockserver` default to `std`, hence `--no-default-features`.

Residents for distribution are built **without debug info**
(`CARGO_PROFILE_RELEASE_DEBUG=false`) rather than stripped afterwards: the
`rust-objcopy` on the GitHub runner lacks its `libLLVM`, which broke the
v3.0.10 release. Symbols are kept, as with `--strip-debug`.

## The gate: `tools/gate.sh`

`sh tools/gate.sh` is the gate for the whole workspace. Any failure is red
(`set -e`); it ends with `poort groen` ("gate green"). CI runs exactly this.

Steps, in order:

1. **Edges** (`== randen`)
   - `cargo tree --workspace` over the core (all app crates excluded, normal,
     build and dev edges) must not contain `hoplib`, `hopdns`, `hoplb`,
     `hopprom`, `hoplockserver` or `hop-gui`.
   - No `git =` dependency without `tag =` in any tracked `Cargo.toml`.
   - No `path =` dependency that resolves outside the repository.
2. **Host tests**: `cargo test --quiet --workspace`.
3. **Clippy**: `cargo clippy --workspace --all-targets --quiet -- -D warnings`.
4. **rustfmt**: `cargo fmt --all --check`.
5. **no_std build**: `cargo build --workspace --target aarch64-unknown-none-softfloat`,
   excluding the host crates (`hostnet`, `store`, `agentd`, `cli`) and the app
   crates. Without `agentd` in the build, `runner`'s `std` feature is off, so it
   builds exactly as the resident uses it.
6. **Per-app gates**: `sh <dir>/tools/gate.sh` for `hoplib`, `apps/hopdns`,
   `apps/hoplb`, `apps/hopprom`, `apps/hoplockserver`, `apps/hop-gui`.
   Skipped when `HOP_GATE_CORE` is set (`HOP_GATE_CORE=1 sh tools/gate.sh`
   runs the core only).

### Per-app gates

Each tests its `std` and `hopos` shapes separately. All run `cargo fmt --check`
and clippy with `-D warnings`.

| Gate | Tests | Clippy | Target builds | Extras |
| --- | --- | --- | --- | --- |
| `hoplib` | no features; `--all-features` | no features; `--all-features` | `no_std` core and `--features hopos` | |
| `apps/hopdns` | `--features std`; `--features hopos --bin hopdns-hopos` | core, `std`, `hopos`; resident on target | `--lib`; `hopdns-hopos` (release) | prints binary size; prints bench lines from `cargo test --release --test bench` |
| `apps/hoplb` | core (`--no-default-features`, single-threaded, captured); `std`; `hopos` on host | core, host, resident (host and target), core (target) | `hoplb-hopos` (release); host `hoplb` (release) | prints bench lines from the core test run |
| `apps/hopprom` | core; `--all-features` | core; `--all-features` | `--lib`; `hopprom-hopos` (release) | host `hopprom` (release) |
| `apps/hoplockserver` | `cargo test` (default `std`) | `no_std` lib; all targets; resident (host and target) | `hoplockserver-hopos` (release) | `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --lib` |
| `apps/hop-gui` | `-p hop-gui --all-features` | `--all-features --all-targets` | `hop-gui-hopos` (release) | `sh -n` on `tools/*.sh`, `py_compile` on `tools/dashboard-api.py` |

The app QEMU runs (`apps/*/tools/qemu-test.sh`), `apps/hoplockserver/tools/e2e-host.sh`
and `apps/hop-gui/tools/host-test.sh` need sibling repositories and/or QEMU and
stay outside the gate.

## Other scripts in `tools/`

| Script | What it does | Needs |
| --- | --- | --- |
| `e2e-host.sh` | Builds `agentd` and `cli` (release) unless `HOP_BIN=<dir>` points at existing binaries. Starts `agentd` standalone with a temp config (file store, API key `e2e-secret`, port `HOP_E2E_PORT`, default 18080; leader on port + 1000), waits for `HOP_UP`, then `hop apply` of a `sleep 30` process job, waits for `1 running` in `hop jobs`, runs `hop status` and `hop agents`, `hop delete`, and checks the `sleep` process is gone. The log stays in `/tmp/hop-e2e.*`. | nothing extra |
| `e2e-linux.sh` | Cross-builds `agentd` and `cli` for `aarch64-unknown-linux-musl` (`rust-lld`), then runs `e2e-host.sh` inside an `alpine:latest` container with `HOP_BIN` set. The container runs as root, so the chroot isolation is really exercised (on macOS it is `sandbox-exec`). | the musl target, Docker on an arm64 host |
| `qemu-test-cluster.sh` | One HopOS node on QEMU plus a host `agentd`, a `hoplockserver` and an SNTP server on the host, in one cluster. Builds the HopOS kernel (`board-qemuvirt`) and `appspike` in `$HOPOS_DIR`, `agentd-hopos` for the target, and `agentd`, `cli`, `hoplockserver` for the host. Checks, phase by phase: clock sync, lock up, host leads, HopOS node registers across nodes, `/v1/*` forwarding (buffered and SSE), placement on the HopOS node by affinity and its logs, failover to the HopOS node with the committed state, the host agent rejoining, and a job placed from the HopOS leader on the host with `hop jobs`, `hop logs` and `hop logs -f`. Any `HOPOS_PANIC`, `HOPOS_EXCEPTION`, `HOPOS_HOP_FAULT`, `HOPOS_HOP_EXIT`, `HOPOS_HOP_FAIL`, `HOP_LOCK_BAD` or `HOP_SPAWN_FAIL` is immediately red. Env: `HOPOS_DIR` (default `../../hop-os`), `TIMEOUT` per phase (default 120 s), `KEEP_LOG=<dir>` to keep green logs. | the HopOS repository, `qemu-system-aarch64`, `python3`; a kernel that passes the cluster keys to Hop (`hop_env`) and reads them from QEMU bootargs |
| `github-download.sh` | Runs the ignored test `agentd-hopos --lib real_github_release_download`: the resident's real artifact download (`HttpImages`: leanhttp, leantls with chain verification against the built-in Mozilla roots, the github.com redirect) over std sockets and the system clock. Green when the reply is 200, the length matches and the bytes start with an ELF header. Default URL is the HopOS welcome app; pass another HTTPS URL as the first argument (`HOP_TEST_URL`). | internet (hence not in the gate) |

See [hopos.md](hopos.md) and [configuration.md](configuration.md) for the
cluster keys the QEMU run uses.

## Tests

```sh
cargo test --workspace                     # what the gate runs
cargo test -p leader                       # one crate
cargo test -p agentd --test streams        # one integration suite
HOP_TEST_DOCKER=1 cargo test -p runner --features std   # include the docker tests
```

Notable suites:

- `agentd/tests/e2e.rs`: the daemon standalone with the file store; apply a
  `sleep 30` process job, see it run, delete it, see the process gone. The
  requests are those of `hop`, signed with the same HMAC.
- `agentd/tests/streams.rs`: `/v1/events` as SSE, the `/v1/tasks` round trip,
  and a task log through the leader, first as a snapshot (`?follow=0`) then live
  (`?follow=1`) until the task stops. All on the leader port, like the CLI.
- `agentd/tests/dashboard.rs`: the hop-gui call sequence against a real daemon
  on the agent port, with `X-Hop-Auth`, an `Origin` and the CORS headers on
  every reply (preflight, `/leader`, status, agents, jobs, priority `PATCH`,
  SSE, logs, `DELETE`).
- `runner` docker tests run only with `HOP_TEST_DOCKER=1`.
- `agentd-hopos` tests drive the resident against the fake kernel
  (`hopos-runner` feature `fake`).
- `apps/hoplockserver/tests/{http,client}.rs` (feature `std`).

### Timing assertions

Some app tests are benchmarks with an assertion and print a measurement line
that the app gate greps:

- `apps/hoplb` (`src/*/tests.rs`, helper `testutil::bench`): lines
  `bench <name>: <ns> ns/op (<n> ops)`. They run in a test build (unoptimised
  unless `cargo test --release`), so the thresholds are generous. Where the
  release threshold is tight, the test switches on `cfg!(debug_assertions)`;
  e.g. `benchmark_get_healthy_backend` allows 1 µs in a debug build and 100 ns
  in release (the CI runner measured 108.7 ns in debug). Route matching allows
  10 µs.
- `apps/hopdns/tests/bench.rs`: lines `bench <name>: <ns>/op, <allocs>/op`, with
  a per-thread counting allocator; hot paths (a cache read, a whole query) must
  make zero allocations, which is asserted. The meaningful numbers come from
  `cargo test --release --test bench -- --nocapture` (the gate prints them);
  debug builds are about ten times slower.

When adding a timing test that must pass on shared CI runners, give it a debug
threshold via `cfg!(debug_assertions)`.

## CI and release

### `.github/workflows/test.yml` (CI)

Triggers: push to `main`, pull requests against `main`. One job on
`ubuntu-latest`: checkout, `rustup show` (installs the pinned toolchain,
components and targets from `rust-toolchain.toml`), `sh tools/gate.sh` (core
plus all app gates).

### `.github/workflows/release.yml` (Release)

One tag, one release for the whole workspace: Hop and the apps it ships.
Trigger: push of a tag matching `v*`. Permission `contents: write`.

Jobs:

1. **`gate`** (ubuntu-latest): `rustup show`, `sh tools/gate.sh`.
2. **`host`** (needs `gate`), matrix:

   | `name` | Runner | Target |
   | --- | --- | --- |
   | `linux-amd64` | ubuntu-latest | `x86_64-unknown-linux-musl` |
   | `linux-arm64` | ubuntu-latest | `aarch64-unknown-linux-musl` |
   | `darwin-arm64` | macos-latest | `aarch64-apple-darwin` |

   Env: `rust-lld` as linker for both musl targets (static binaries),
   `CARGO_PROFILE_RELEASE_STRIP=debuginfo`. Builds
   `-p agentd -p cli -p hoplockserver -p hoplb -p hopdns -p hopprom -p hop-gui --features hopdns/std,hopprom/std,hop-gui/std`
   and packs one tarball per product:

   | Tarball | Contents |
   | --- | --- |
   | `hop` | `agentd`, `hop` |
   | `hoplockserver`, `hoplb`, `hopdns`, `hopprom`, `hop-gui` | the binary of the same name |

   Each product is published twice, with identical bytes:

   - `<name>-<tag>-<platform>.tar.gz`, containing the directory
     `<name>-<tag>-<platform>/`;
   - `<name>-<platform>.tar.gz`, without version, for stable
     `/releases/latest/download/<file>` links (gethop.org links these).

3. **`hopos`** (needs `gate`, ubuntu-latest): with
   `CARGO_PROFILE_RELEASE_DEBUG=false`, builds the five app residents for
   `aarch64-unknown-none-softfloat` (commands as in
   [HopOS residents](#hopos-residents)) and renames them to
   `<app>-arm64.elf`: `hopdns-arm64.elf`, `hopprom-arm64.elf`,
   `hop-gui-arm64.elf`, `hoplb-arm64.elf`, `hoplockserver-arm64.elf`. Hop's own
   resident is not included (built by `hop-os/tools/hop-build.sh`).
4. **`release`** (needs `host` and `hopos`): downloads all artifacts into
   `dist/`, writes `SHA256SUMS` (`sha256sum *`), and publishes everything with
   `softprops/action-gh-release@v2` and generated release notes.

### Cutting a release

1. Bump `version` in `[workspace.package]` in the root `Cargo.toml` (all crates
   inherit it; Hop and the apps share one version). Let `Cargo.lock` update
   (`cargo build` or `cargo check`).
2. Run `sh tools/gate.sh` locally.
3. Commit (by convention the message starts with the version, e.g.
   `3.0.11: ...`), then tag and push:

   ```sh
   git tag v3.0.11
   git push origin main v3.0.11
   ```

The tag name becomes `<tag>` in the tarball names (`GITHUB_REF_NAME`, so it
includes the `v`).

Context from the history: v3.0.10 was the first release of the whole workspace
(Hop and the apps on one version); its `hopos` job failed because the runner's
`rust-objcopy` lacks `libLLVM`, so residents are now built without debug info
instead of being stripped, and v3.0.11 is the first complete release.
