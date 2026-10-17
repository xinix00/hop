#!/bin/sh
# The dashboard hosted on HopOS, on QEMU: the kernel starts Hop, Hop places
# hop-gui-hopos with a published port, a browser-like client gets the page,
# and then runs the dashboard's API sequence against the agent on the node.
#
# The kernel boots with agentd-hopos in slot 1 (HopOS image/qemu-run.sh,
# APP=hop, built from $HOP_DIR). One jobspec goes to the AGENT port, as the
# dashboard's "New job" does: hop-gui-hopos from an artifact server on the
# host (10.0.2.2 for the guest), with "ports":{"http":80}. Hop sets
# ER_PORT_HTTP=80, the kernel forwards uplink port 80 to the slot (DNAT),
# and QEMU's hostfwd brings 127.0.0.1:$WEBPORT to guest port 80. Green only
# when:
#
#   kernel      HOPOS_BOOT, HOPOS_NET_UP, HOPOS_SYSTEM_UP, HOPOS_HOP_START
#               slot=1 and Hop's two ports (HOPOS_HOP_PUBLISH);
#   Hop         HOP_UP and HOP_LEADER from slot 1;
#   placement   HOP_JOB_PLACED slot=2, "slot 2: 1 port(s) published tcp+udp
#               on the uplink: :80 HOPOS_SLOT_PUBLISH" and
#               "slot 2: ... HOPOS_HOPGUI_UP files=N bytes=M port=80";
#   the page    GET / is index.html byte for byte, GET /app.js is app.js,
#               GET /vendor/material-symbols-outlined.woff2 is the 4 MB font,
#               and GET /app.js with its ETag in If-None-Match is a 304;
#   the API     tools/dashboard-api.py check against 127.0.0.1:$AGENTPORT,
#               every call signed with X-Hop-Auth (HMAC-SHA256, as app.js
#               does with Web Crypto) and with an Origin, every answer with
#               Access-Control-Allow-Origin: the preflight, /leader,
#               /v1/status, /v1/agents, /v1/jobs, the job status, the
#               capacity, the first SSE lines of /v1/events, the first
#               lines of the live log of the task (it must show the
#               HOPOS_HOPGUI_UP line) and PATCH .../priority;
#   the stop    DELETE /v1/jobs/hop-gui on the agent: the kernel withdraws
#               the port ("slot 2: ports withdrawn from the uplink
#               HOPOS_SLOT_UNPUBLISH").
#
# The QEMU board boots Hop with hopos.insecure=1 (HopOS hopos/src/config.rs,
# QEMU_CFG): the signature is sent but not checked there. tools/host-test.sh
# runs the same sequence against agentd with a real key.
#
# A HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_APP_PANIC, HOPOS_HOP_FAULT,
# HOPOS_HOP_EXIT, HOPOS_HOP_FAIL or HOPOS_SLOT_PUBLISH_FAIL is red at once.
# Red keeps the console (and prints it).
#
#   tools/qemu-test.sh                 TIMEOUT=90 by default, in seconds
#   HOPOS_DIR=path                     the HopOS repo (default ../../hop-os)
#   HOP_DIR=path                       the hop repo (default ../hop)
#   KEEP_LOG=path                      also keep a green console
#   AGENTPORT/LEADERPORT/SYSPORT/ARTPORT/WEBPORT   host ports; a taken one
#                                      becomes a free port of the OS, loudly
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMEOUT="${TIMEOUT:-90}"
HOPOS_DIR="$(cd "${HOPOS_DIR:-$DIR/../../hop-os}" && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop}" && pwd)"
TARGET=aarch64-unknown-none-softfloat
KEY="${HOP_KEY:-qemu-dashboard-key}"
LOG="$(mktemp -t hopgui-qemu.XXXXXX)"
ART="$(mktemp -d -t hopgui-art.XXXXXX)"
DISK="$ART/disk.img"
QPID=""
HPID=""
cleanup() {
	[ -n "$QPID" ] && kill "$QPID" 2>/dev/null
	[ -n "$HPID" ] && kill "$HPID" 2>/dev/null
	rm -rf "$LOG" "$ART"
	true
}
trap cleanup EXIT INT TERM

# A host port: the one asked for if it is free, otherwise a free one.
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

echo "== build: hop-gui-hopos here, hopos in $HOPOS_DIR, agentd-hopos in $HOP_DIR"
(cd "$DIR" && cargo build --quiet --release --target "$TARGET" --features hopos --bin hop-gui-hopos)
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p hopos --features board-qemuvirt)
(cd "$HOP_DIR" && cargo build --quiet --release --target "$TARGET" -p agentd-hopos)

# The artifact server: without debug info, the symbols stay for placement.
OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
ELF="$DIR/target/$TARGET/release/hop-gui-hopos"
if [ -n "$OBJCOPY" ]; then
	"$OBJCOPY" --strip-debug "$ELF" "$ART/hop-gui.elf"
else
	cp "$ELF" "$ART/hop-gui.elf"
fi
echo "   hop-gui.elf: $(wc -c <"$ART/hop-gui.elf" | tr -d ' ') bytes"
(cd "$ART" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$ART/http.log" 2>&1 &
HPID=$!

echo "== boot QEMU virt with Hop (up to ${TIMEOUT}s; agent :$AGENTPORT, leader :$LEADERPORT, artifacts :$ARTPORT, web :$WEBPORT -> guest :80)"
SYSPORT="$SYSPORT" AGENTPORT="$AGENTPORT" LEADERPORT="$LEADERPORT" WEBPORT="$WEBPORT" \
	HOP_DIR="$HOP_DIR" APP=hop DISK="$DISK" \
	sh "$HOPOS_DIR/image/qemu-run.sh" </dev/null >"$LOG" 2>&1 &
QPID=$!

has() { tr -d '\r' <"$LOG" | grep -q -E "$1"; }
all() {
	(
		IFS='|'
		for m in $1; do has "$m" || exit 1; done
	)
}

BOOT_MARKS="HOPOS_BOOT|HOPOS_NET_UP|HOPOS_SYSTEM_UP|HOPOS_HOP_START slot=1 |uplink tcp :8080 -> slot 1 :8080 HOPOS_HOP_PUBLISH|uplink tcp :9080 -> slot 1 :9080 HOPOS_HOP_PUBLISH|slot 1: .*HOP_LEADER|slot 1: .*HOP_UP"
PLACE_MARKS="slot 1: .*HOP_JOB_PLACED slot=2|slot 2: 1 port\\(s\\) published tcp\\+udp on the uplink: :80 HOPOS_SLOT_PUBLISH|slot 2: .*HOPOS_HOPGUI_UP files=[0-9]+ bytes=[0-9]+ port=80"
STOP_MARKS="slot 2: ports withdrawn from the uplink HOPOS_SLOT_UNPUBLISH"
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_APP_PANIC|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOPOS_SLOT_PUBLISH_FAIL"

API="python3 $DIR/tools/dashboard-api.py"
AGENT="http://127.0.0.1:$AGENTPORT"
JOB='{"name":"hop-gui","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/hop-gui.elf"}],"memory_limit":67108864,"ports":{"http":80}}'
POSTED=""
PAGE=""
API_OUT=""
API_OK=""
DELETED=""
START=$(date +%s)
elapsed=0
step() {
	sleep 0.2
	elapsed=$(($(date +%s) - START))
}
alive() {
	! has "$RED" && kill -0 "$QPID" 2>/dev/null && [ "$elapsed" -lt "$TIMEOUT" ]
}

# 1. Boot, then the jobspec to the agent (the dashboard's "New job").
while alive && [ -z "$POSTED" ]; do
	if all "$BOOT_MARKS"; then
		POSTED="$($API post --agent "$AGENT" --key "$KEY" --body "$JOB" 2>&1 || true)"
	fi
	step
done

# 2. The placement, then the page from outside.
fetch() { # fetch <path> <file>: the HTTP code
	curl -s -m 20 -o "$2" -w '%{http_code}' "http://127.0.0.1:$WEBPORT$1" 2>/dev/null || true
}
while alive && [ -z "$PAGE" ]; do
	if all "$PLACE_MARKS" && [ "$(fetch / "$ART/index.html")" = 200 ]; then
		PAGE="up"
	fi
	step
done
PAGE_LINES=""
page_fail=0
if [ -n "$PAGE" ]; then
	for f in index.html app.js appearance.js style.css vendor/material-symbols-outlined.woff2; do
		path="/$f"
		[ "$f" = index.html ] && path="/"
		out="$ART/got-$(basename "$f")"
		code="$(fetch "$path" "$out")"
		if [ "$code" = 200 ] && cmp -s "$out" "$DIR/$f"; then
			PAGE_LINES="$PAGE_LINES
   ok  GET $path: HTTP 200, $(wc -c <"$out" | tr -d ' ') bytes, identical to $f"
		else
			PAGE_LINES="$PAGE_LINES
   ROOD GET $path: HTTP ${code:-none}, not identical to $f"
			page_fail=1
		fi
	done
	etag="$(curl -s -m 10 -D - -o /dev/null "http://127.0.0.1:$WEBPORT/app.js" | tr -d '\r' | awk -F': ' 'tolower($1)=="etag"{print $2}')"
	code="$(curl -s -m 10 -o /dev/null -w '%{http_code}' -H "If-None-Match: $etag" "http://127.0.0.1:$WEBPORT/app.js" || true)"
	if [ -n "$etag" ] && [ "$code" = 304 ]; then
		PAGE_LINES="$PAGE_LINES
   ok  GET /app.js with If-None-Match $etag: HTTP 304"
	else
		PAGE_LINES="$PAGE_LINES
   ROOD GET /app.js with If-None-Match '$etag': HTTP ${code:-none}, want 304"
		page_fail=1
	fi

	# 3. The dashboard's API sequence against the agent on the node.
	if API_OUT="$($API wait --agent "$AGENT" --key "$KEY" --job hop-gui 2>&1 &&
		$API check --agent "$AGENT" --key "$KEY" --job hop-gui --log-marker HOPOS_HOPGUI_UP 2>&1)"; then
		API_OK=1
	fi

	# 4. The stop.
	DELETED="$($API delete --agent "$AGENT" --key "$KEY" --job hop-gui 2>&1 || true)"
	while alive && ! all "$STOP_MARKS"; do step; done
fi

kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true
QPID=""

fail=0
IFS_WAS="$IFS"
IFS='|'
for m in $BOOT_MARKS $PLACE_MARKS $STOP_MARKS; do
	if has "$m"; then
		echo "   ok  $m: $(tr -d '\r' <"$LOG" | grep -m1 -E "$m")"
	else
		echo "   ROOD $m missing"
		fail=1
	fi
done
IFS="$IFS_WAS"
case "$POSTED" in
*"ok  POST"*) echo "$POSTED" ;;
"") echo "   ROOD POST /v1/jobs never sent (Hop not up in time)"; fail=1 ;;
*) echo "$POSTED"; fail=1 ;;
esac
if [ -n "$PAGE" ]; then
	echo "$PAGE_LINES" | sed '/^$/d'
	[ "$page_fail" = 0 ] || fail=1
else
	echo "   ROOD GET http://127.0.0.1:$WEBPORT/: no page"
	fail=1
fi
if [ -n "$API_OUT" ]; then
	echo "$API_OUT"
fi
if [ -z "$API_OK" ]; then
	echo "   ROOD the dashboard's API sequence"
	fail=1
fi
case "$DELETED" in
*"ok  DELETE"*) echo "$DELETED" ;;
*) echo "   ROOD DELETE: ${DELETED:-never sent}"; fail=1 ;;
esac
if grep -q "GET /hop-gui.elf" "$ART/http.log" 2>/dev/null; then
	echo "   ok  artifact server: $(grep -c 'GET /hop-gui.elf' "$ART/http.log") download(s) of hop-gui.elf"
else
	echo "   ROOD artifact server: never asked"
	fail=1
fi
if has "$RED"; then
	echo "   ROOD $(tr -d '\r' <"$LOG" | grep -m1 -E "$RED")"
	fail=1
fi
echo "   time: $(($(date +%s) - START)) s after QEMU started"
if [ "$fail" != 0 ]; then
	KEEP="$(mktemp -t hopgui-qemu-red.XXXXXX)"
	tr -d '\r' <"$LOG" >"$KEEP"
	echo "== console kept in $KEEP"
	echo "== console:"
	cat "$KEEP"
	exit 1
fi
[ -n "${KEEP_LOG:-}" ] && tr -d '\r' <"$LOG" >"$KEEP_LOG"
echo "hop-gui on HopOS green"
