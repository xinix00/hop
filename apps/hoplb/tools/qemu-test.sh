#!/bin/sh
# De kring van hoplb op QEMU: HopOS start Hop, Hop plaatst welcome en
# hoplb, en van buiten staat de bunny-pagina achter hoplb.
#
# De kern boot met agentd-hopos in slot 1 (hop-os/image/qemu-run.sh met
# APP=hop). Van buiten gaan twee jobspecs naar de leader van Hop, allebei
# van een artifact-server op de host (voor de gast 10.0.2.2):
#
#   welcome  "ports":{"http":8081}, "tags":{"hoplb-urlprefix":"welcome.local"}
#            (poort 80 van de node is voor hoplb; welcome publiceert 8081)
#   hoplb    "ports":{"http":80,"admin":9091}; de agent is http://HOP:9080,
#            de leader-API van Hop in zijn eigen slot
#
# hoplb leest via hoplib de agents, jobs en taken, bouwt de route
# welcome.local -> 10.0.2.15:8081 (het node-IP uit het endpoint van de
# agent; de switch legt dat om naar het slot van welcome, hairpin), en
# QEMU's hostfwd brengt 127.0.0.1:$WEBPORT naar poort 80 van de gast. De
# admin-poort 9091 komt er met een tweede hostfwd bij, gezet over een
# eigen QEMU-monitor (qemu-run.sh geeft extra argumenten aan QEMU door;
# er verandert niets in hop-os). Groen alleen als:
#
#   kern      HOPOS_BOOT, HOPOS_NET_UP, HOPOS_SYSTEM_UP, HOPOS_HOP_START slot=1;
#   Hop       HOP_UP en HOP_LEADER via de servicer van slot 1;
#   welcome   HOP_JOB_PLACED slot=2, HOPOS_WELCOME_UP port=8081;
#   hoplb     HOP_JOB_PLACED slot=3, 2 poorten gepubliceerd,
#             HOPOS_HOPLB_UP port=80 admin=9091, HOPOS_HOPLB_ROUTES n=1;
#   van buiten curl -H 'Host: welcome.local' http://127.0.0.1:$WEBPORT/ geeft
#             200 met de bunny ("( -.-)"), een onbekende host 502, en
#             /metrics op 127.0.0.1:$ADMINPORT telt het verzoek:
#             hoplb_requests_total{domain="welcome.local",...,code="200"} 1;
#   de stroom DELETE /v1/jobs/welcome: de SSE-stroom van de agent meldt het,
#             hoplb zet HOPOS_HOPLB_ROUTES n=0, en welcome.local geeft 502.
#
# Een HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_APP_PANIC, HOPOS_HOP_FAULT,
# HOPOS_HOP_EXIT, HOPOS_HOP_FAIL of HOPOS_SLOT_PUBLISH_FAIL is meteen rood.
# Rood bewaart de console (en drukt hem af).
#
#   tools/qemu-test.sh                    TIMEOUT=120 standaard, in seconden
#   HOPOS_DIR=pad                         de hop-os-repo (standaard ../../hop-os)
#   HOP_DIR=pad                           de hop-repo (standaard ../hop)
#   KEEP_LOG=pad tools/qemu-test.sh       bewaart ook een groene console
#   SYSPORT/AGENTPORT/LEADERPORT/ARTPORT/WEBPORT/ADMINPORT
#                                         de host-poorten; bezet = een vrije
#                                         poort van het OS, luid
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMEOUT="${TIMEOUT:-120}"
HOPOS_DIR="$(cd "${HOPOS_DIR:-$DIR/../../hop-os}" && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop}" && pwd)"
TARGET=aarch64-unknown-none-softfloat
LOG="$(mktemp -t hoplb-qemu.XXXXXX)"
ART="$(mktemp -d -t hoplb-art.XXXXXX)"
DISK="$ART/disk.img"
PAGE="$ART/page.html"
MON="$ART/monitor.sock"
QPID=""
HPID=""
cleanup() {
	[ -n "$QPID" ] && kill "$QPID" 2>/dev/null
	[ -n "$HPID" ] && kill "$HPID" 2>/dev/null
	rm -rf "$LOG" "$ART"
	true
}
trap cleanup EXIT INT TERM

# Een host-poort: de gevraagde als hij vrij is, anders een vrije van het OS.
port() {
	python3 - "$1" "$2" <<'PY'
import socket, sys
want, name = int(sys.argv[1]), sys.argv[2]
s = socket.socket()
try:
    s.bind(("127.0.0.1", want))
    print(want)
except OSError:
    s.close()
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    got = s.getsockname()[1]
    print(f"   {name} {want} is taken, using {got}", file=sys.stderr)
    print(got)
s.close()
PY
}
SYSPORT="$(port "${SYSPORT:-10100}" SYSPORT)"
AGENTPORT="$(port "${AGENTPORT:-8080}" AGENTPORT)"
LEADERPORT="$(port "${LEADERPORT:-9080}" LEADERPORT)"
ARTPORT="$(port "${ARTPORT:-8000}" ARTPORT)"
WEBPORT="$(port "${WEBPORT:-8081}" WEBPORT)"
ADMINPORT="$(port "${ADMINPORT:-9091}" ADMINPORT)"

echo "== bouwen: hoplb-hopos hier, welcome en hopos in $HOPOS_DIR, agentd-hopos in $HOP_DIR"
(cd "$DIR" && cargo build --quiet --release --target "$TARGET" --no-default-features --features hopos --bin hoplb-hopos)
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p welcome)

# De artifact-server: beide ELF's zonder debug-info; de symbolen blijven
# voor de plaatsing.
OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
strip_to() {
	if [ -n "$OBJCOPY" ]; then "$OBJCOPY" --strip-debug "$1" "$2"; else cp "$1" "$2"; fi
}
strip_to "$DIR/target/$TARGET/release/hoplb-hopos" "$ART/hoplb.elf"
strip_to "$HOPOS_DIR/target/$TARGET/release/welcome" "$ART/welcome.elf"
(cd "$ART" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$ART/http.log" 2>&1 &
HPID=$!

echo "== booten op QEMU virt met Hop (tot ${TIMEOUT}s; leader :$LEADERPORT, artifacts :$ARTPORT, web :$WEBPORT -> gast :80, admin :$ADMINPORT -> gast :9091)"
SYSPORT="$SYSPORT" AGENTPORT="$AGENTPORT" LEADERPORT="$LEADERPORT" WEBPORT="$WEBPORT" \
	HOP_DIR="$HOP_DIR" APP=hop DISK="$DISK" \
	sh "$HOPOS_DIR/image/qemu-run.sh" -monitor "unix:$MON,server,nowait" </dev/null >"$LOG" 2>&1 &
QPID=$!

has() { tr -d '\r' <"$LOG" | grep -q -E "$1"; }
all() {
	(
		IFS='|'
		for m in $1; do has "$m" || exit 1; done
	)
}

BOOT_MARKS="HOPOS_BOOT|HOPOS_NET_UP|HOPOS_SYSTEM_UP|HOPOS_HOP_START slot=1 |slot 1: .*HOP_LEADER|slot 1: .*HOP_UP"
WELCOME_MARKS="slot 1: .*HOP_JOB_PLACED slot=2|slot 2: .*HOPOS_WELCOME_UP port=8081"
HOPLB_MARKS="slot 1: .*HOP_JOB_PLACED slot=3|slot 3: 2 port\\(s\\) published tcp\\+udp on the uplink|slot 3: .*HOPOS_HOPLB_UP port=80 admin=9091|slot 3: .*HOPOS_HOPLB_ROUTES n=1 backends=1"
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_APP_PANIC|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOPOS_SLOT_PUBLISH_FAIL"

WELCOME='{"name":"welcome","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/welcome.elf"}],"memory_limit":33554432,"ports":{"http":8081},"tags":{"hoplb-urlprefix":"welcome.local"}}'
HOPLB='{"name":"hoplb","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/hoplb.elf"}],"memory_limit":67108864,"ports":{"http":80,"admin":9091},"env":{"HOPLB_VERBOSE":"1"}}'
POST_WELCOME=""
POST_HOPLB=""
FWD=""
PAGE_OK=""
NOROUTE=""
METRICS=""
START=$(date +%s)
elapsed=0
step() {
	sleep 0.2
	elapsed=$(($(date +%s) - START))
}
alive() {
	! has "$RED" && kill -0 "$QPID" 2>/dev/null && [ "$elapsed" -lt "$TIMEOUT" ]
}
post() {
	curl -s -m 20 -w ' HTTP %{http_code}' -X POST -H 'Content-Type: application/json' \
		-d "$1" "http://127.0.0.1:$LEADERPORT/v1/jobs" 2>&1 || echo "ROOD curl"
}

# 1. Boot, dan welcome naar de leader.
while alive && [ -z "$POST_WELCOME" ]; do
	all "$BOOT_MARKS" && POST_WELCOME="$(post "$WELCOME")"
	step
done

# 2. welcome staat, dan hoplb.
while alive && [ -z "$POST_HOPLB" ]; do
	all "$WELCOME_MARKS" && POST_HOPLB="$(post "$HOPLB")"
	step
done

# 3. De admin-poort op de host: een tweede hostfwd over de monitor.
if [ -n "$POST_HOPLB" ]; then
	FWD="$(python3 - "$MON" "$ADMINPORT" <<'PY'
import socket, sys, time
s = socket.socket(socket.AF_UNIX)
s.connect(sys.argv[1])
s.settimeout(2)
def drain():
    out = b""
    try:
        while True:
            b = s.recv(4096)
            if not b:
                break
            out += b
    except OSError:
        pass
    return out.decode(errors="replace")
drain()
s.sendall(f"hostfwd_add n0 tcp:127.0.0.1:{sys.argv[2]}-:9091\n".encode())
time.sleep(0.5)
out = drain()
print("error" if "rror" in out or "not" in out.lower() else "ok")
PY
)" || FWD="error"
fi

# 4. hoplb staat met een route: de pagina door hoplb heen, een onbekende
#    host, en de metrics.
while alive && [ -z "$PAGE_OK" ] && [ "$FWD" = ok ]; do
	if all "$HOPLB_MARKS"; then
		code="$(curl -s -m 5 -o "$PAGE" -w '%{http_code}' -H 'Host: welcome.local' "http://127.0.0.1:$WEBPORT/" 2>/dev/null || true)"
		if [ "$code" = 200 ] && grep -q -F '( -.-)' "$PAGE"; then
			PAGE_OK="HTTP $code, $(wc -c <"$PAGE" | tr -d ' ') bytes"
			NOROUTE="$(curl -s -m 5 -w ' HTTP %{http_code}' -H 'Host: nope.local' "http://127.0.0.1:$WEBPORT/" 2>&1 || true)"
			METRICS="$(curl -s -m 5 "http://127.0.0.1:$ADMINPORT/metrics" 2>&1 || true)"
			HEALTH="$(curl -s -m 5 "http://127.0.0.1:$ADMINPORT/health" 2>&1 || true)"
			break
		fi
	fi
	step
done

# 5. De stroom: welcome weg, en hoplb ziet het zonder dat iemand hem vraagt.
GONE_MARK="slot 3: .*HOPOS_HOPLB_ROUTES n=0 backends=0"
DELETED=""
GONE=""
if [ -n "$PAGE_OK" ]; then
	DELETED="$(curl -s -m 20 -w ' HTTP %{http_code}' -X DELETE "http://127.0.0.1:$LEADERPORT/v1/jobs/welcome" 2>&1 || true)"
	while alive && ! has "$GONE_MARK"; do step; done
	if has "$GONE_MARK"; then
		GONE="$(curl -s -m 5 -w ' HTTP %{http_code}' -H 'Host: welcome.local' "http://127.0.0.1:$WEBPORT/" 2>&1 || true)"
	fi
fi

kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true
QPID=""

fail=0
IFS_WAS="$IFS"
IFS='|'
for m in $BOOT_MARKS $WELCOME_MARKS $HOPLB_MARKS; do
	if has "$m"; then
		echo "   ok  $m: $(tr -d '\r' <"$LOG" | grep -m1 -E "$m")"
	else
		echo "   ROOD $m ontbreekt"
		fail=1
	fi
done
IFS="$IFS_WAS"
for p in "welcome:$POST_WELCOME" "hoplb:$POST_HOPLB"; do
	case "$p" in
	*"HTTP 2"*) echo "   ok  POST /v1/jobs ${p%%:*}: ${p#*:}" ;;
	*) echo "   ROOD POST /v1/jobs ${p%%:*}: ${p#*:}"; fail=1 ;;
	esac
done
if [ "$FWD" = ok ]; then
	echo "   ok  hostfwd 127.0.0.1:$ADMINPORT -> gast :9091 via de monitor"
else
	echo "   ROOD hostfwd voor de admin-poort: ${FWD:-nooit gezet}"
	fail=1
fi
if [ -n "$PAGE_OK" ]; then
	echo "   ok  GET -H 'Host: welcome.local' http://127.0.0.1:$WEBPORT/: $PAGE_OK, met de bunny, door hoplb"
else
	echo "   ROOD GET -H 'Host: welcome.local' http://127.0.0.1:$WEBPORT/: geen pagina met de bunny"
	fail=1
fi
case "$NOROUTE" in
*"no route for host"*"HTTP 502") echo "   ok  GET -H 'Host: nope.local': $(printf '%s' "$NOROUTE" | tr '\n' ' ')" ;;
*) echo "   ROOD GET -H 'Host: nope.local': ${NOROUTE:-nooit gevraagd}"; fail=1 ;;
esac
case "${HEALTH:-}" in
ok*) echo "   ok  GET :$ADMINPORT/health: $(printf '%s' "$HEALTH" | tr '\n' ' ')" ;;
*) echo "   ROOD GET :$ADMINPORT/health: ${HEALTH:-nooit gevraagd}"; fail=1 ;;
esac
COUNT="$(printf '%s\n' "$METRICS" | grep -E '^hoplb_requests_total\{domain="welcome.local",backend="[^"]+",code="200"\} 1$' || true)"
if [ -n "$COUNT" ]; then
	echo "   ok  /metrics telt het verzoek: $COUNT"
	printf '%s\n' "$METRICS" | grep -E '^hoplb_requests_total\{domain="nope.local"|_count\{domain="welcome.local"' | sed 's/^/          /'
else
	echo "   ROOD /metrics telt het verzoek niet:"
	printf '%s\n' "$METRICS" | sed 's/^/          /'
	fail=1
fi
case "$DELETED" in
*"HTTP 2"*) echo "   ok  DELETE /v1/jobs/welcome: $DELETED" ;;
*) echo "   ROOD DELETE /v1/jobs/welcome: ${DELETED:-nooit gedaan}"; fail=1 ;;
esac
if has "$GONE_MARK"; then
	echo "   ok  $GONE_MARK: $(tr -d '\r' <"$LOG" | grep -m1 -E "$GONE_MARK")"
else
	echo "   ROOD $GONE_MARK ontbreekt: de stroom bracht de stop niet"
	fail=1
fi
case "$GONE" in
*"no route for host"*"HTTP 502") echo "   ok  daarna GET -H 'Host: welcome.local': $(printf '%s' "$GONE" | tr '\n' ' ')" ;;
*) echo "   ROOD daarna GET -H 'Host: welcome.local': ${GONE:-nooit gevraagd}"; fail=1 ;;
esac
for elf in welcome hoplb; do
	if grep -q "GET /$elf.elf" "$ART/http.log" 2>/dev/null; then
		echo "   ok  artifact-server: $elf.elf gedownload"
	else
		echo "   ROOD artifact-server: $elf.elf nooit gevraagd"
		fail=1
	fi
done
if has "$RED"; then
	echo "   ROOD $(tr -d '\r' <"$LOG" | grep -m1 -E "$RED")"
	fail=1
fi
echo "   tijd: $(($(date +%s) - START)) s na de start van QEMU"
if [ "$fail" != 0 ]; then
	KEEP="$(mktemp -t hoplb-qemu-rood.XXXXXX)"
	tr -d '\r' <"$LOG" >"$KEEP"
	echo "== console bewaard in $KEEP"
	echo "== console (slot 3 en de laatste 60 regels):"
	grep -E 'slot 3|hoplb' "$KEEP" | head -40
	tail -60 "$KEEP"
	exit 1
fi
[ -n "${KEEP_LOG:-}" ] && tr -d '\r' <"$LOG" >"$KEEP_LOG"
echo "hoplb-kring groen"
