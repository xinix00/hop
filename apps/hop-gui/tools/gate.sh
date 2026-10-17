#!/bin/sh
# The gate of hop-gui (rustdoc handbook §9): host tests, clippy with the
# hard set, rustfmt, the HopOS build for the target, and the scripts parse.
# Red is red. The QEMU run (tools/qemu-test.sh) and the host run
# (tools/host-test.sh) need the sibling repos and stay outside the gate.
set -e
cd "$(dirname "$0")/.."
echo "== host: cargo test (std and hopos)"
cargo test --quiet --all-features
echo "== host: cargo clippy"
cargo clippy --quiet --all-features --all-targets -- -D warnings
echo "== rustfmt"
cargo fmt --check
echo "== target: hop-gui-hopos (aarch64)"
cargo build --quiet --release --target aarch64-unknown-none-softfloat --features hopos --bin hop-gui-hopos
echo "== scripts"
for s in tools/*.sh; do sh -n "$s"; done
python3 -m py_compile tools/dashboard-api.py
rm -rf tools/__pycache__
echo "gate green"
