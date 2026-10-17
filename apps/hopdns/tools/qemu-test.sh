#!/bin/sh
# De kring van hopdns op QEMU: de kern van HopOS start Hop, Hop plaatst
# welcome en daarna hopdns, en hopdns zegt waar welcome woont.
#
# Van buiten gaan twee jobspecs naar de leader van Hop (zoals
# hop-os/tools/qemu-test-welcome.sh): welcome met "ports":{"http":80}, dan
# hopdns met "ports":{"dns":5353} en HOPDNS_SELFTEST=welcome. Beide komen
# van een artifact-server op de host (voor de gast 10.0.2.2). hopdns volgt
# zijn eigen Hop (de standaardpeer http://HOP:9080, HOP is slot 1), bouwt
# zijn cache uit /v1/agents en /v1/tasks, en vraagt dan zichzelf over UDP
# naar welcome.hop.local. Het antwoord moet het slot-adres van welcome zijn
# (10.100.0.<slot+1>): een buurslot bereikt welcome daar rechtstreeks.
#
# Waarom de vraag van binnen komt: image/qemu-run.sh van hop-os zet alleen
# TCP-hostfwd's (system-API, agent, leader, WEBPORT). Een vraag van de host
# naar poort 5353 van de gast vraagt een UDP-hostfwd
# (`hostfwd=udp:127.0.0.1:$DNSPORT-:5353`), en die knop hoort in hop-os,
# niet hier. Tot hij er is, doet de zelftoets van de bewoner de vraag over
# zijn eigen netstack.
#
# Groen alleen als:
#
#   kern        HOPOS_BOOT, HOPOS_NET_UP, HOPOS_SYSTEM_UP, HOPOS_HOP_START
#               slot=1 en Hop's twee poorten (HOPOS_HOP_PUBLISH);
#   Hop         HOP_UP en HOP_LEADER via de servicer van slot 1;
#   welcome     POST aangenomen, HOP_JOB_PLACED, HOPOS_SLOT_PUBLISH :80 en
#               HOPOS_WELCOME_UP port=80;
#   hopdns      POST aangenomen, HOP_JOB_PLACED, HOPOS_SLOT_PUBLISH :5353
#               (tcp+udp), HOPOS_HOPDNS_UP port=5353, HOPOS_HOPDNS_SSE_UP,
#               HOPOS_HOPDNS_CACHE jobs=N met N >= 2 (welcome en hopdns), en
#               HOPOS_HOPDNS_SELFTEST ok ip=<slot-adres van welcome>.
#
# Een HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_APP_PANIC, HOPOS_HOP_FAULT,
# HOPOS_HOP_EXIT, HOPOS_HOP_FAIL, HOPOS_SLOT_PUBLISH_FAIL, HOPOS_HOPDNS_FAIL
# of HOPOS_HOPDNS_SELFTEST fail is meteen rood. Rood bewaart de console.
#
#   tools/qemu-test.sh                     TIMEOUT=120 standaard, in seconden
#   KEEP_LOG=pad tools/qemu-test.sh        bewaart ook een groene console
#   HOPOS_DIR=pad                          de hop-os-repo (standaard ../../hop-os)
#   QEMU_RUN=pad DNSPORT=n                 een qemu-run.sh met een UDP-hostfwd
#                                          127.0.0.1:$DNSPORT -> gast :5353; dan
#                                          vraagt ook de host welcome.hop.local
#   HOP_DIR=pad                            de hop-repo (standaard ../hop)
#   SYSPORT/AGENTPORT/LEADERPORT/ARTPORT   de host-poorten; bezet = een vrije
#                                          poort van het OS, luid
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOPOS_DIR="$(cd "${HOPOS_DIR:-$DIR/../../hop-os}" && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop}" && pwd)"
TIMEOUT="${TIMEOUT:-120}"
TARGET=aarch64-unknown-none-softfloat
LOG="$(mktemp -t hopdns-qemu.XXXXXX)"
ART="$(mktemp -d -t hopdns-art.XXXXXX)"
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
QEMU_RUN="${QEMU_RUN:-$HOPOS_DIR/image/qemu-run.sh}"
DNSPORT="${DNSPORT:-}"

echo "== bouwen: hopdns-hopos hier, hopos (qemuvirt) en welcome in $HOPOS_DIR, agentd-hopos in $HOP_DIR"
(cd "$DIR" && cargo build --quiet --release --target "$TARGET" --features hopos --bin hopdns-hopos)
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p hopos --features board-qemuvirt)
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p welcome)
(cd "$HOP_DIR" && cargo build --quiet --release --target "$TARGET" -p agentd-hopos)

# De artifact-server: de images zonder debug-info, de symbolen blijven voor
# de plaatsing.
OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
strip_to() {
	if [ -n "$OBJCOPY" ]; then
		"$OBJCOPY" --strip-debug "$1" "$2"
	else
		cp "$1" "$2"
	fi
}
strip_to "$DIR/target/$TARGET/release/hopdns-hopos" "$ART/hopdns.elf"
strip_to "$HOPOS_DIR/target/$TARGET/release/welcome" "$ART/welcome.elf"
(cd "$ART" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$ART/http.log" 2>&1 &
HPID=$!

echo "== booten op QEMU virt met Hop (tot ${TIMEOUT}s; leader :$LEADERPORT, artifacts :$ARTPORT)"
SYSPORT="$SYSPORT" AGENTPORT="$AGENTPORT" LEADERPORT="$LEADERPORT" \
	HOP_DIR="$HOP_DIR" APP=hop DISK="$DISK" DNSPORT="$DNSPORT" \
	sh "$QEMU_RUN" </dev/null >"$LOG" 2>&1 &
QPID=$!

has() { tr -d '\r' <"$LOG" | grep -q -E "$1"; }
first() { tr -d '\r' <"$LOG" | grep -m1 -E "$1"; }
all() {
	(
		IFS='|'
		for m in $1; do has "$m" || exit 1; done
	)
}

BOOT_MARKS="HOPOS_BOOT|HOPOS_NET_UP|HOPOS_SYSTEM_UP|HOPOS_HOP_START slot=1 |uplink tcp :8080 -> slot 1 :8080 HOPOS_HOP_PUBLISH|uplink tcp :9080 -> slot 1 :9080 HOPOS_HOP_PUBLISH|slot 1: .*HOP_LEADER|slot 1: .*HOP_UP"
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_APP_PANIC|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOPOS_SLOT_PUBLISH_FAIL|HOPOS_HOPDNS_FAIL|HOPOS_HOPDNS_SELFTEST fail"

WELCOME='{"name":"welcome","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/welcome.elf"}],"memory_limit":33554432,"ports":{"http":80}}'
HOPDNS='{"name":"hopdns","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/hopdns.elf"}],"memory_limit":33554432,"ports":{"dns":5353},"env":{"HOPDNS_SELFTEST":"welcome"}}'
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
		-d "$1" "http://127.0.0.1:$LEADERPORT/v1/jobs" 2>&1 || true
}

# 1. Boot, dan welcome.
POSTED_WELCOME=""
while alive && [ -z "$POSTED_WELCOME" ]; do
	all "$BOOT_MARKS" && POSTED_WELCOME="$(post "$WELCOME")"
	step
done

# 2. welcome staat, dan hopdns.
POSTED_HOPDNS=""
WSLOT=""
while alive && [ -z "$POSTED_HOPDNS" ]; do
	if has "job welcome task .*HOP_JOB_PLACED slot=" && has "HOPOS_WELCOME_UP port=80"; then
		WSLOT="$(first "job welcome task .*HOP_JOB_PLACED slot=" | sed 's/.*HOP_JOB_PLACED slot=\([0-9]*\).*/\1/')"
		POSTED_HOPDNS="$(post "$HOPDNS")"
	fi
	step
done

# 3. hopdns: placement, poort, cache en de zelftoets.
while alive && ! has "HOPOS_HOPDNS_SELFTEST ok"; do step; done

# 4. Met een UDP-hostfwd: dezelfde vraag van de host, door de DNAT van de kern.
HOSTQ=""
if [ -n "$DNSPORT" ] && has "HOPOS_HOPDNS_SELFTEST ok"; then
	HOSTQ="$(python3 - "$DNSPORT" <<'PY'
import socket, struct, sys
port = int(sys.argv[1])
name = b"".join(bytes([len(l)]) + l for l in b"welcome.hop.local".split(b".")) + b"\0"
q = struct.pack(">HHHHHH", 0x4844, 0x0100, 1, 0, 0, 0) + name + struct.pack(">HH", 1, 1)
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(3)
for _ in range(5):
    try:
        s.sendto(q, ("127.0.0.1", port))
        r, _ = s.recvfrom(1500)
        break
    except socket.timeout:
        r = b""
if len(r) < 12:
    print("no answer")
    sys.exit()
an, rcode = struct.unpack(">H", r[6:8])[0], r[3] & 15
ips = []
for i in range(an):
    rec = r[len(q) + 16 * i: len(q) + 16 * (i + 1)]
    ips.append(".".join(str(b) for b in rec[12:16]))
print(f"rcode={rcode} ip={','.join(ips)}")
PY
)"
fi
DSLOT=""
has "job hopdns task .*HOP_JOB_PLACED slot=" &&
	DSLOT="$(first "job hopdns task .*HOP_JOB_PLACED slot=" | sed 's/.*HOP_JOB_PLACED slot=\([0-9]*\).*/\1/')"

kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true
QPID=""

WANT_IP=""
[ -n "$WSLOT" ] && WANT_IP="10.100.0.$((WSLOT + 1))"
MARKS="$BOOT_MARKS"
[ -n "$WSLOT" ] && MARKS="$MARKS|slot $WSLOT: 1 port\\(s\\) published tcp\\+udp on the uplink: :80 HOPOS_SLOT_PUBLISH|slot $WSLOT: .*HOPOS_WELCOME_UP port=80"
if [ -n "$DSLOT" ]; then
	MARKS="$MARKS|slot $DSLOT: 1 port\\(s\\) published tcp\\+udp on the uplink: :5353 HOPOS_SLOT_PUBLISH|slot $DSLOT: .*HOPOS_HOPDNS_UP port=5353|slot $DSLOT: .*HOPOS_HOPDNS_SSE_UP|slot $DSLOT: .*HOPOS_HOPDNS_CACHE jobs=|slot $DSLOT: .*HOPOS_HOPDNS_SELFTEST ok ip=$WANT_IP\$"
fi

fail=0
IFS_WAS="$IFS"
IFS='|'
for m in $MARKS; do
	if has "$m"; then
		echo "   ok  $m: $(first "$m")"
	else
		echo "   ROOD $m ontbreekt"
		fail=1
	fi
done
IFS="$IFS_WAS"
for job in welcome hopdns; do
	case "$job" in
	welcome) got="$POSTED_WELCOME" slot="$WSLOT" ;;
	*) got="$POSTED_HOPDNS" slot="$DSLOT" ;;
	esac
	case "$got" in
	*"HTTP 2"*) echo "   ok  POST /v1/jobs $job: $got" ;;
	*) echo "   ROOD POST /v1/jobs $job: ${got:-nooit gedaan}"; fail=1 ;;
	esac
	if [ -n "$slot" ]; then
		echo "   ok  $job in slot $slot"
	else
		echo "   ROOD $job nooit geplaatst"
		fail=1
	fi
done
if [ -n "$DSLOT" ]; then
	jobs="$(first "slot $DSLOT: .*HOPOS_HOPDNS_CACHE jobs=" | sed 's/.*HOPOS_HOPDNS_CACHE jobs=\([0-9]*\).*/\1/')"
	best="$(tr -d '\r' <"$LOG" | grep -E "slot $DSLOT: .*HOPOS_HOPDNS_CACHE jobs=" | sed 's/.*HOPOS_HOPDNS_CACHE jobs=\([0-9]*\).*/\1/' | sort -n | tail -1)"
	if [ "${best:-0}" -ge 2 ]; then
		echo "   ok  cache: jobs=$best (eerste regel jobs=$jobs)"
	else
		echo "   ROOD cache: jobs=${best:-geen}, verwacht welcome en hopdns"
		fail=1
	fi
fi
if [ -n "$DNSPORT" ]; then
	case "$HOSTQ" in
	*"rcode=0 ip=$WANT_IP") echo "   ok  host -> 127.0.0.1:$DNSPORT/udp welcome.hop.local: $HOSTQ" ;;
	*) echo "   ROOD host -> 127.0.0.1:$DNSPORT/udp welcome.hop.local: ${HOSTQ:-nooit gevraagd}"; fail=1 ;;
	esac
fi
for elf in welcome hopdns; do
	if grep -q "GET /$elf.elf" "$ART/http.log" 2>/dev/null; then
		echo "   ok  artifact-server: $(grep -c "GET /$elf.elf" "$ART/http.log") download(s) van $elf.elf"
	else
		echo "   ROOD artifact-server: $elf.elf nooit gevraagd"
		fail=1
	fi
done
if has "$RED"; then
	echo "   ROOD $(first "$RED")"
	fail=1
fi
echo "   tijd: $(($(date +%s) - START)) s na de start van QEMU"
if [ "$fail" != 0 ]; then
	KEEP="$(mktemp -t hopdns-qemu-rood.XXXXXX)"
	tr -d '\r' <"$LOG" >"$KEEP"
	echo "== console bewaard in $KEEP"
	echo "== console (staart):"
	tail -80 "$KEEP"
	exit 1
fi
[ -n "${KEEP_LOG:-}" ] && tr -d '\r' <"$LOG" >"$KEEP_LOG"
echo "hopdns-kring groen"
