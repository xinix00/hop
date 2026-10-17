#!/bin/sh
# De poort van hoplockserver (rustdoc/README.md §9): rood is rood. De
# randen van de afhankelijkheden bewaakt de poort van de hop-werkruimte.
#
#   1. cargo fmt --check;
#   2. clippy met -D warnings: de bibliotheek zonder std (no_std + alloc),
#      de host-vorm met alle toetsen, en de bewoner op de host (een lege
#      main) en op zijn target;
#   3. cargo test: de Go-tests van store, server en client, naam voor naam,
#      plus de socket- en clienttoetsen;
#   4. de docs zonder waarschuwing;
#   5. de bewoner bouwen voor aarch64-unknown-none-softfloat (release).
#
# De kringen met de echte Hop staan apart: tools/e2e-host.sh (agentd op de
# host) en tools/qemu-test.sh (Hop op HopOS in QEMU).
set -eu
cd "$(dirname "$0")/.."
TARGET=aarch64-unknown-none-softfloat

echo "== fmt"
cargo fmt --check

echo "== clippy: bibliotheek (no_std), host, bewoner"
cargo clippy --quiet --lib --no-default-features -- -D warnings
cargo clippy --quiet --all-targets -- -D warnings
cargo clippy --quiet --no-default-features --features hopos --bin hoplockserver-hopos -- -D warnings
cargo clippy --quiet --release --target "$TARGET" --no-default-features --features hopos \
	--bin hoplockserver-hopos -- -D warnings

echo "== test"
out="$(cargo test --quiet 2>&1)" || { printf '%s\n' "$out"; exit 1; }
printf '%s\n' "$out" | grep -E "^test result" | sed 's/^/   /'

echo "== doc"
RUSTDOCFLAGS="-D warnings" cargo doc --quiet --no-deps --lib

echo "== bewoner: $TARGET"
cargo build --quiet --release --target "$TARGET" --no-default-features --features hopos \
	--bin hoplockserver-hopos
ls -l "../../target/$TARGET/release/hoplockserver-hopos" | awk '{print "   " $5 " bytes (met debug-info)"}'

echo "gate groen"
