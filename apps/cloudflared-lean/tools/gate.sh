#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt en de
# bewoner voor beide targets. Rood is rood. De randen van de afhankelijkheden
# bewaakt de poort van de hop-werkruimte (tools/gate.sh in de root).
#
# Op de host is cloudflared-lean een lege binary met de toetsen van zijn
# protocollagen (TLS-keten, HTTP/2, Cap'n Proto, ingress); de bewoner bestaat
# alleen voor een bare-metal target.
set -e
cd "$(dirname "$0")/.."
echo "== host: cargo test"
cargo test --quiet
echo "== clippy: host en beide targets"
cargo clippy --quiet --all-targets -- -D warnings
for t in aarch64-unknown-none-softfloat riscv64gc-unknown-none-elf; do
	cargo clippy --quiet --release --target "$t" --bin cloudflared-lean -- -D warnings
done
echo "== rustfmt"
cargo fmt --check
for t in aarch64-unknown-none-softfloat riscv64gc-unknown-none-elf; do
	echo "== target: de bewoner ($t, release)"
	cargo build --quiet --release --target "$t" --bin cloudflared-lean
	ls -l "../../target/$t/release/cloudflared-lean" | awk '{print "   cloudflared-lean: " $5 " bytes (met debug-info)"}'
done
echo "poort groen"
