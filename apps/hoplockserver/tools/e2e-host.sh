#!/bin/sh
# De end-to-end-run op de host: deze server met Hop's echte `agentd`.
#
# Twee agentd's (uit $HOP_DIR, standaard ../hop/hop, `cargo build --release
# -p agentd --locked`) vormen één cluster met `cluster.lock` op deze
# hoplockserver. Groen alleen als:
#
#   de server    HOPLOCK_UP keys=0 op een verse datamap;
#   node 1       HOP_LEADER, en de lease staat als kaal bestand in de
#                datamap (`leases/e2e`, Go's hoplock.State met node 1 als
#                eigenaar); een GET met de sleutel geeft die body met als
#                ETag de sha256 van het bestand, zonder sleutel 401;
#   vernieuwing  de ETag van de lease verandert terwijl node 1 leidt (elke
#                vernieuwing is een PUT met If-Match op de vorige ETag);
#   node 2       HOP_UP, en zolang node 1 leeft geen HOP_LEADER;
#   failover     node 1 hard gedood (geen afscheid, zoals een stroomuitval):
#                na de TTL wordt node 2 leider (HOP_LEADER), de lease op de
#                schijf noemt node 2 met een hogere generatie;
#   herstart     de server gestopt en opnieuw gestart op dezelfde map:
#                HOPLOCK_UP keys>=1, en node 2 blijft leider (vernieuwt).
#
#   tools/e2e-host.sh                 bouwt release; laat de logs staan bij rood
#   HOP_DIR=pad tools/e2e-host.sh     de hop-repo
#   EXT_TARGET=pad                    waar agentd gebouwd wordt (standaard
#                                     target/ext in deze repo: in de hop-repo
#                                     verandert niets)
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOP_DIR="${HOP_DIR:-$DIR/../hop/hop}"
EXT_TARGET="${EXT_TARGET:-$DIR/target/ext}"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/hoplock-e2e.XXXXXX")"
KEY="e2e-lock-secret"
LPID=""
N1=""
N2=""
cleanup() {
	for p in $N1 $N2 $LPID; do kill "$p" 2>/dev/null || true; done
	true
}
trap cleanup EXIT INT TERM

# Een vrije poort p van het OS waarvoor p+1000 ook vrij is (agent en leader).
ports() {
	python3 - "$1" <<'PY'
import socket, sys
pair = sys.argv[1] == "pair"
while True:
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]
    if not pair:
        s.close(); print(p); break
    if p + 1000 > 65535:
        s.close(); continue
    t = socket.socket()
    try:
        t.bind(("127.0.0.1", p + 1000)); t.close(); s.close(); print(p); break
    except OSError:
        t.close(); s.close()
PY
}
LOCKPORT="$(ports single)"
P1="$(ports pair)"
P2="$(ports pair)"

echo "== bouwen: hoplockserver, en agentd in $HOP_DIR (target $EXT_TARGET/hop)"
cargo build --quiet --release --manifest-path "$DIR/Cargo.toml" --bin hoplockserver
CARGO_TARGET_DIR="$EXT_TARGET/hop" cargo build --quiet --release --locked \
	--manifest-path "$HOP_DIR/Cargo.toml" -p agentd
LOCK="$DIR/target/release/hoplockserver"
AGENTD="$EXT_TARGET/hop/release/agentd"

fail() {
	echo "   ROOD $*"
	for f in lock.log lock2.log n1.log n2.log; do
		[ -f "$TMP/$f" ] && { echo "== $f"; cat "$TMP/$f"; }
	done
	echo "== logs bewaard in $TMP"
	exit 1
}
# wacht <bestand> <regex> <seconden>
wait_for() {
	i=0
	while ! grep -q -E "$2" "$1" 2>/dev/null; do
		i=$((i + 1))
		[ "$i" -lt $(($3 * 5)) ] || return 1
		sleep 0.2
	done
}
line() { grep -m1 -E "$2" "$1"; }
sha() { printf '"%s"' "$(shasum -a 256 "$1" | cut -d' ' -f1)"; }
curl_etag() {
	curl -s -m 5 -D - -o "$TMP/body" -H "X-API-Key: $KEY" "http://127.0.0.1:$LOCKPORT/leases/e2e" |
		tr -d '\r' | awk -F': ' 'tolower($1) == "etag" {print $2}'
}

start_lock() {
	"$LOCK" -listen "127.0.0.1:$LOCKPORT" -data "$TMP/data" -api-key "$KEY" 2>"$TMP/$1" &
	LPID=$!
}
node_config() {
	cat >"$TMP/$1.json" <<JSON
{"node": {"id": "e2e-$1", "ip": "127.0.0.1", "port": $2},
 "cluster": {"name": "e2e", "lock": {"type": "hoplockserver", "url": "http://127.0.0.1:$LOCKPORT", "api_key": "$KEY"}},
 "timeouts": {"leader_lease": "9s"},
 "paths": {"state_file": "$TMP/$1/state.json", "rootfs_base": "$TMP/$1/tasks"},
 "api_key": "e2e-hop-secret"}
JSON
}

echo "== hoplockserver op 127.0.0.1:$LOCKPORT, data $TMP/data"
start_lock lock.log
wait_for "$TMP/lock.log" "HOPLOCK_UP keys=0" 10 || fail "HOPLOCK_UP keys=0 ontbreekt"
echo "   ok  $(line "$TMP/lock.log" HOPLOCK_UP)"

echo "== node 1 (agent :$P1, leader :$((P1 + 1000)))"
node_config n1 "$P1"
"$AGENTD" --config "$TMP/n1.json" 2>"$TMP/n1.log" &
N1=$!
wait_for "$TMP/n1.log" "HOP_LEADER$" 30 || wait_for "$TMP/n1.log" "HOP_LEADER" 1 || fail "node 1: geen HOP_LEADER"
echo "   ok  node 1: $(line "$TMP/n1.log" 'HOP_LEADER')"
LEASE="$TMP/data/leases/e2e"
[ -f "$LEASE" ] || fail "de lease staat niet in de datamap ($LEASE)"
echo "   ok  lease op de schijf: $(cat "$LEASE")"
grep -q "\"owner\":\"127.0.0.1:$((P1 + 1000))\"" "$LEASE" || fail "de lease noemt node 1 niet als eigenaar"
grep -q '"generation":1,' "$LEASE" || fail "de eerste lease heeft geen generatie 1"

E1="$(curl_etag)"
[ "$E1" = "$(sha "$LEASE")" ] || fail "GET gaf ETag $E1, het bestand heeft $(sha "$LEASE")"
cmp -s "$TMP/body" "$LEASE" || fail "GET gaf een andere body dan het bestand"
echo "   ok  GET /leases/e2e: ETag $E1 = sha256 van het bestand"
CODE="$(curl -s -m 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$LOCKPORT/leases/e2e")"
[ "$CODE" = 401 ] || fail "GET zonder sleutel gaf $CODE, geen 401"
echo "   ok  GET zonder X-API-Key: 401"

i=0
E2="$E1"
while [ "$E2" = "$E1" ]; do
	i=$((i + 1))
	[ "$i" -lt 60 ] || fail "de lease werd in 12 s niet vernieuwd"
	sleep 0.2
	E2="$(curl_etag)"
done
echo "   ok  vernieuwd: ETag $E1 -> $E2 (PUT met If-Match)"

echo "== node 2 (agent :$P2, leader :$((P2 + 1000)))"
node_config n2 "$P2"
"$AGENTD" --config "$TMP/n2.json" 2>"$TMP/n2.log" &
N2=$!
wait_for "$TMP/n2.log" "HOP_UP" 30 || fail "node 2: geen HOP_UP"
echo "   ok  node 2: $(line "$TMP/n2.log" HOP_UP)"
sleep 5
if grep -q "HOP_LEADER$" "$TMP/n2.log" || grep -q "became leader" "$TMP/n2.log"; then
	fail "node 2 werd leider terwijl node 1 de lease hield"
fi
grep -q "\"owner\":\"127.0.0.1:$((P1 + 1000))\"" "$LEASE" || fail "de lease verloor node 1 terwijl hij leefde"
echo "   ok  node 2 leidt niet zolang node 1 de lease houdt (5 s)"

echo "== failover: node 1 hard gedood"
kill -9 "$N1"
wait "$N1" 2>/dev/null || true
N1=""
wait_for "$TMP/n2.log" "became leader" 40 || fail "node 2 nam de lease niet over na de TTL"
echo "   ok  node 2: $(line "$TMP/n2.log" 'became leader')"
grep -q "\"owner\":\"127.0.0.1:$((P2 + 1000))\"" "$LEASE" || fail "de lease noemt node 2 niet na de overname"
GEN="$(sed -n 's/.*"generation":\([0-9]*\).*/\1/p' "$LEASE")"
[ "${GEN:-0}" -ge 2 ] || fail "de generatie steeg niet bij de overname ($GEN)"
echo "   ok  lease na de overname: $(cat "$LEASE")"

echo "== herstart van de server op dezelfde map"
kill "$LPID"
wait "$LPID" 2>/dev/null || true
start_lock lock2.log
wait_for "$TMP/lock2.log" "HOPLOCK_UP keys=[1-9]" 10 || fail "na de herstart geen HOPLOCK_UP keys>=1"
echo "   ok  $(line "$TMP/lock2.log" HOPLOCK_UP)"
E3="$(curl_etag)"
i=0
E4="$E3"
while [ "$E4" = "$E3" ]; do
	i=$((i + 1))
	[ "$i" -lt 60 ] || fail "node 2 vernieuwde de lease niet na de herstart van de server"
	sleep 0.2
	E4="$(curl_etag)"
done
grep -q "\"owner\":\"127.0.0.1:$((P2 + 1000))\"" "$LEASE" || fail "na de herstart hoort de lease niet meer bij node 2"
echo "   ok  node 2 vernieuwt na de herstart: ETag $E3 -> $E4"
if grep -q "HOP_LEADER_STOPPED" "$TMP/n2.log"; then
	fail "node 2 trad af: $(line "$TMP/n2.log" HOP_LEADER_STOPPED)"
fi

echo "== markers"
grep -h -E "HOPLOCK_|HOP_" "$TMP/lock.log" "$TMP/lock2.log" "$TMP/n1.log" "$TMP/n2.log" | sed 's/^/   /'
echo "e2e groen; logs in $TMP"
