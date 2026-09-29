#!/bin/sh
# Dezelfde end-to-end-run op Linux: cross-bouw voor aarch64-unknown-linux-musl
# (statisch, gelinkt met rust-lld) en draai tools/e2e-host.sh in een Alpine-
# container. De container draait als root, dus hier loopt de isolatie
# (chroot) echt; op macOS is dat sandbox-exec.
#
# Vraagt: rustup target aarch64-unknown-linux-musl, docker op een arm64-host.
set -e
cd "$(dirname "$0")/.."
CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
	cargo build --quiet --release -p agentd -p cli --target aarch64-unknown-linux-musl
docker run --rm -v "$PWD:/repo:ro" -e HOP_BIN=/repo/target/aarch64-unknown-linux-musl/release \
	alpine:latest sh /repo/tools/e2e-host.sh
