#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, de
# no_std-kern en de bewoner voor het target, en de meetregels. Rood is rood.
#
# De kern (de bibliotheek zonder features) is no_std en bouwt voor
# aarch64-unknown-none-softfloat; `std` is de host-daemon, `hopos` de
# bewoner. De QEMU-kring is tools/qemu-test.sh (die vraagt hop-os en hop
# naast deze repo, en QEMU).
set -e
cd "$(dirname "$0")/.."
TARGET=aarch64-unknown-none-softfloat
echo "== host: cargo test (kern, host-daemon, benchmarks)"
cargo test --quiet --features std
echo "== host: cargo test (bewoner)"
cargo test --quiet --features hopos --bin hopdns-hopos
echo "== host: cargo clippy (kern, std, hopos)"
cargo clippy --quiet --all-targets -- -D warnings
cargo clippy --quiet --features std --all-targets -- -D warnings
cargo clippy --quiet --features hopos --all-targets -- -D warnings
echo "== rustfmt"
cargo fmt --check
echo "== target: de no_std-kern en de bewoner ($TARGET)"
cargo build --quiet --lib --target "$TARGET"
cargo clippy --quiet --release --features hopos --target "$TARGET" --bin hopdns-hopos -- -D warnings
cargo build --quiet --release --features hopos --target "$TARGET" --bin hopdns-hopos
ls -l "target/$TARGET/release/hopdns-hopos" | awk '{print "   hopdns-hopos: " $5 " bytes (met debug-info)"}'
echo "== meetregels (release)"
cargo test --release --test bench -- --nocapture --test-threads=1 2>/dev/null |
	grep -o 'bench [a-z_/0-9]*: .*' | sed 's/^/   /'
echo "poort groen"
