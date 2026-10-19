#!/bin/sh
# De poort (handboek §9): host-tests, clippy met de harde set, rustfmt, en de
# no_std-bouw voor het target. Rood is rood.
#
# De no_std-stap geldt alleen voor de logica-crates en de HopOS-bewoner; de
# host-crates (hostnet, store, agentd, cli) zijn std en blijven erbuiten.
# Zonder `agentd` in die bouw staat de feature `std` van `runner` uit, dus
# bouwt runner daar precies zoals de bewoner hem gebruikt. De docker-tests
# van runner draaien alleen met HOP_TEST_DOCKER=1.
#
# hoplib en de apps (apps/*) hebben elk hun eigen poort, met hun features
# `std` en `hopos` apart getoetst; deze poort draait ze aan het eind. Met
# HOP_GATE_CORE=1 alleen de kern.
set -e
cd "$(dirname "$0")/.."
APPS="hoplib apps/hopdns apps/hoplb apps/hopprom apps/hoplockserver apps/hop-gui apps/cloudflared-lean"
HOST_CRATES="--exclude hostnet --exclude store --exclude agentd --exclude cli"
APP_CRATES="--exclude hoplib --exclude hopdns --exclude hoplb --exclude hopprom --exclude hoplockserver --exclude hop-gui --exclude cloudflared-lean"

echo "== randen"
# De kern hangt nooit van hoplib of een app af: die staan op Hop, niet andersom.
# shellcheck disable=SC2086
tree="$(cargo tree --quiet --workspace $APP_CRATES -e normal,build,dev --prefix none)"
if printf '%s\n' "$tree" | grep -E '^(hoplib|hopdns|hoplb|hopprom|hoplockserver|hop-gui|cloudflared-lean) '; then
	echo "ROOD: een kern-crate hangt van hoplib of een app af"
	exit 1
fi
# Buur-repo's (lean, HopOS) alleen via een tag, en geen pad over de repo-grens.
manifests="$(git ls-files '*Cargo.toml')"
# shellcheck disable=SC2086
if grep -nE '^[^#]*git *=' $manifests | grep -v 'tag *='; then
	echo "ROOD: een git-dependency zonder tag"
	exit 1
fi
for m in $manifests; do
	for p in $(grep -oE '^[^#]*\{[^}]*path *= *"[^"]*"' "$m" | sed -E 's/.*path *= *"([^"]*)"/\1/'); do
		case "$(cd "$(dirname "$m")/$p" 2>/dev/null && pwd)" in
		"$PWD" | "$PWD"/*) ;;
		*)
			echo "ROOD: $m: pad $p buiten de repo"
			exit 1
			;;
		esac
	done
done

echo "== host: cargo test"
cargo test --quiet --workspace
echo "== host: cargo clippy"
cargo clippy --workspace --all-targets --quiet -- -D warnings
echo "== rustfmt"
cargo fmt --all --check
echo "== target: no_std (aarch64)"
# shellcheck disable=SC2086
cargo build --quiet --workspace $HOST_CRATES $APP_CRATES --target aarch64-unknown-none-softfloat

if [ -z "$HOP_GATE_CORE" ]; then
	for a in $APPS; do
		echo "==== $a"
		sh "$a/tools/gate.sh"
	done
fi
echo "poort groen"
