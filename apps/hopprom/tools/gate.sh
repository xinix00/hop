#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, de
# no_std-kern en de bewoner voor het target, en de host-bin. Rood is rood.
#
# De kern draagt geen feature en moet zo ook op het bare-metal target
# bouwen (no_std met alloc); `std` en `hopos` bouwen elk hun bin. Op de
# host bouwt `hopos` mee (applib heeft host-stubs), zodat clippy en de
# toetsen van de bewoner-helpers daar ook draaien.
set -e
cd "$(dirname "$0")/.."
TARGET=aarch64-unknown-none-softfloat
echo "== host: cargo test (kern)"
cargo test --quiet
echo "== host: cargo test (alle features)"
cargo test --quiet --all-features
echo "== host: cargo clippy"
cargo clippy --quiet --all-targets -- -D warnings
cargo clippy --quiet --all-targets --all-features -- -D warnings
echo "== rustfmt"
cargo fmt --check
echo "== target: de kern, no_std ($TARGET)"
cargo build --quiet --lib --target "$TARGET"
echo "== target: de bewoner ($TARGET)"
cargo build --quiet --release --target "$TARGET" --features hopos --bin hopprom-hopos
echo "== host: de bin"
cargo build --quiet --release --features std --bin hopprom
echo "poort groen"
