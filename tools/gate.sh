#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, en de
# no_std-bouw voor het target. Rood is rood.
#
# De no_std-stap geldt alleen voor de logica-crates en de HopOS-bewoner; de
# host-crates (hostnet, store, agentd, cli) zijn std en blijven erbuiten.
# Zonder `agentd` in die bouw staat de feature `std` van `runner` uit, dus
# bouwt runner daar precies zoals de bewoner hem gebruikt. De docker-tests
# van runner draaien alleen met HOP_TEST_DOCKER=1.
set -e
cd "$(dirname "$0")/.."
HOST_CRATES="--exclude hostnet --exclude store --exclude agentd --exclude cli"
echo "== host: cargo test"
cargo test --quiet --workspace
echo "== host: cargo clippy"
cargo clippy --workspace --all-targets --quiet -- -D warnings
echo "== rustfmt"
cargo fmt --check
echo "== target: no_std (aarch64)"
# shellcheck disable=SC2086
cargo build --quiet --workspace $HOST_CRATES --target aarch64-unknown-none-softfloat
echo "poort groen"
