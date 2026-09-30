#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, en de
# no_std-bouw voor het target. Rood is rood.
#
# De tests en clippy draaien met alle features: de kern, de host-module
# (tegen een echte socket) en de hopos-module (tegen een nep-verbinding door
# leanhttp). De no_std-stap bouwt de kern met `hopos` voor het target van
# een slot; `std` hoort daar niet bij.
set -e
cd "$(dirname "$0")/.."
echo "== host: cargo test (kern)"
cargo test --quiet
echo "== host: cargo test (alle features)"
cargo test --quiet --all-features
echo "== host: cargo clippy"
cargo clippy --all-targets --quiet -- -D warnings
cargo clippy --all-targets --all-features --quiet -- -D warnings
echo "== rustfmt"
cargo fmt --check
echo "== target: no_std (aarch64, hopos)"
cargo build --quiet --target aarch64-unknown-none-softfloat
cargo build --quiet --target aarch64-unknown-none-softfloat --features hopos
echo "poort groen"
