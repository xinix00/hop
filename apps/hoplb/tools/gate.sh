#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, de
# bewoner voor zijn target, en de randen van de afhankelijkheden. Rood is
# rood.
#
# Drie vormen van dezelfde crate, elk apart getoetst:
#
#   kern    --no-default-features       no_std + alloc, sans-I/O (de tests
#                                       van OLD/ en de benchmarks)
#   host    --features std (standaard)  de bin `hoplb`
#   bewoner --features hopos            de bin `hoplb-hopos`, op de host als
#                                       lege main (clippy en de toetsen van
#                                       zijn configuratie), en voor
#                                       aarch64-unknown-none-softfloat echt
#
# De meetregels van de benchmarks (`bench <naam>: <ns> ns/op`) staan aan het
# eind; het zijn test-builds, de getallen zijn de lat, niet de release.
set -e
cd "$(dirname "$0")/.."
TARGET=aarch64-unknown-none-softfloat
LOG="$(mktemp -t hoplb-gate.XXXXXX)"
trap 'rm -f "$LOG"' EXIT

echo "== randen: alleen git-tags, nooit een pad over de repo-grens"
if grep -nE '^[^#]*path *=' Cargo.toml | grep -v -E '^\S*:(path = "src/)'; then
	echo "ROOD: een pad-dependency in Cargo.toml"
	exit 1
fi
if grep -nE '^[^#]*git *=' Cargo.toml | grep -v 'tag *='; then
	echo "ROOD: een git-dependency zonder tag"
	exit 1
fi

echo "== kern: cargo test (no_std-bibliotheek, host-tests)"
cargo test --quiet --no-default-features -- --nocapture --test-threads=1 >"$LOG" 2>&1 || {
	cat "$LOG"
	exit 1
}
grep -E '^test result' "$LOG"
echo "== host: cargo test --features std"
cargo test --quiet
echo "== bewoner: cargo test --features hopos (op de host)"
cargo test --quiet --no-default-features --features hopos

echo "== clippy: kern, host, bewoner (host), bewoner (target), kern (target)"
cargo clippy --quiet --no-default-features --all-targets -- -D warnings
cargo clippy --quiet --all-targets -- -D warnings
cargo clippy --quiet --no-default-features --features hopos --all-targets -- -D warnings
cargo clippy --quiet --release --no-default-features --features hopos --target "$TARGET" --bin hoplb-hopos -- -D warnings
cargo clippy --quiet --no-default-features --target "$TARGET" --lib -- -D warnings

echo "== rustfmt"
cargo fmt --check

echo "== target: de bewoner ($TARGET, release)"
cargo build --quiet --release --no-default-features --features hopos --target "$TARGET" --bin hoplb-hopos
ls -l "target/$TARGET/release/hoplb-hopos" | awk '{print "   hoplb-hopos: " $5 " bytes (met debug-info; objcopy --strip-debug voor de artifact)"}'
echo "== host: de daemon (release)"
cargo build --quiet --release --bin hoplb
ls -l target/release/hoplb | awk '{print "   hoplb: " $5 " bytes"}'

echo "== meetregels (test-build)"
grep -E '^bench ' "$LOG" | sed 's/^/   /'
echo "poort groen"
