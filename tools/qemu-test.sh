#!/bin/sh
# De kring van hopprom op QEMU: de kern start Hop, Hop plaatst hopprom met
# een gepubliceerde poort, en van buiten staat er een /metrics met het
# cluster erin.
#
# De kern en de QEMU-regel zijn die van hop-os (image/qemu-run.sh, APP=hop:
# agentd-hopos in slot 1); dit script bouwt alleen hopprom-hopos hier en
# neemt van de twee andere repo's alleen hun scripts en bestanden, geen
# code (PORT.md beslissing 6). Van buiten gaat er één jobspec naar de
# leader van Hop: hopprom-hopos van een artifact-server op de host (voor de
# gast 10.0.2.2), met "ports":{"metrics":9090}. Hop zet ER_PORT_METRICS=9090
# in de env en de kern zet uplink-poort 9090 door naar het slot (DNAT). De
# hostfwd van QEMU naar gastpoort 9090 komt er via QMP bij (hostfwd_add op
# netdev n0): qemu-run.sh kent alleen gastpoort 80, en zo blijft zijn
# QEMU-regel de enige. Groen alleen als:
#
#   kern        HOPOS_BOOT, HOPOS_NET_UP, HOPOS_SYSTEM_UP, HOPOS_HOP_START
#               slot=1 en Hop's twee poorten (HOPOS_HOP_PUBLISH);
#   Hop         HOP_UP en HOP_LEADER via de servicer van slot 1;
#   van buiten  POST http://127.0.0.1:$LEADERPORT/v1/jobs wordt aangenomen;
#   de plaatsing HOP_JOB_PLACED slot=2, "slot 2: 1 port(s) published tcp+udp
#               on the uplink: :9090 HOPOS_SLOT_PUBLISH",
#               "slot 2: ... HOPOS_HOPPROM_UP port=9090" en
#               "slot 2: ... HOPOS_HOPPROM_POLL agents=1 tasks=1";
#   van buiten  GET http://127.0.0.1:$WEBPORT/metrics geeft 200 met
#               hop_agents_total 1 en de taakmetrics van hopprom zelf
#               (hop_tasks_total{state="running"} 1,
#               hop_tasks_placed{job="hopprom"} 1, hop_job_healthy en
#               hop_task_starts_total voor job hopprom), en GET /health "ok";
#   de stop     DELETE /v1/jobs/hopprom: de kern trekt de poort in
#               ("slot 2: ports withdrawn from the uplink
#               HOPOS_SLOT_UNPUBLISH"), en daarna antwoordt /metrics niet meer.
#
# Een HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_APP_PANIC, HOPOS_HOP_FAULT,
# HOPOS_HOP_EXIT, HOPOS_HOP_FAIL of HOPOS_SLOT_PUBLISH_FAIL is meteen rood.
# Rood bewaart de console (en drukt hem af).
#
#   tools/qemu-test.sh                     TIMEOUT=90 standaard, in seconden
#   KEEP_LOG=pad tools/qemu-test.sh        bewaart ook een groene console
#   KEEP_METRICS=pad tools/qemu-test.sh    bewaart /metrics zoals curl hem gaf
#   HOPOS_DIR=pad                          de hop-os-repo (standaard ../../hop-os)
#   HOP_DIR=pad                            de hop-repo (standaard ../hop)
#   HOPPROM_AGENT=url tools/qemu-test.sh   de agent voor hopprom in de jobspec
#                                          (standaard die van de bewoner,
#                                          http://HOP:8080)
#   SYSPORT/AGENTPORT/LEADERPORT/ARTPORT/WEBPORT/QMPPORT   de host-poorten;
#                                          bezet = een vrije van het OS, luid
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMEOUT="${TIMEOUT:-90}"
HOPOS_DIR="$(cd "${HOPOS_DIR:-$DIR/../../hop-os}" && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop}" && pwd)"
TARGET=aarch64-unknown-none-softfloat
LOG="$(mktemp -t hopprom-qemu.XXXXXX)"
ART="$(mktemp -d -t hopprom-art.XXXXXX)"
DISK="$ART/disk.img"
METRICS="$ART/metrics.txt"
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
WEBPORT="$(port "${WEBPORT:-9090}" WEBPORT)"
QMPPORT="$(port "${QMPPORT:-4444}" QMPPORT)"

echo "== bouwen: hopprom-hopos hier; hopos (qemuvirt) in $HOPOS_DIR en agentd-hopos in $HOP_DIR via qemu-run.sh"
(cd "$DIR" && cargo build --quiet --release --target "$TARGET" --features hopos --bin hopprom-hopos)
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p hopos --features board-qemuvirt)
(cd "$HOP_DIR" && cargo build --quiet --release --target "$TARGET" -p agentd-hopos)

# De artifact-server: hopprom-hopos zonder debug-info, de symbolen blijven
# voor de plaatsing.
OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
if [ -n "$OBJCOPY" ]; then
	"$OBJCOPY" --strip-debug "$DIR/target/$TARGET/release/hopprom-hopos" "$ART/hopprom-hopos.elf"
else
	cp "$DIR/target/$TARGET/release/hopprom-hopos" "$ART/hopprom-hopos.elf"
fi
(cd "$ART" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$ART/http.log" 2>&1 &
HPID=$!

echo "== booten op QEMU virt met Hop (tot ${TIMEOUT}s; leader :$LEADERPORT, artifacts :$ARTPORT, metrics :$WEBPORT -> gast :9090)"
SYSPORT="$SYSPORT" AGENTPORT="$AGENTPORT" LEADERPORT="$LEADERPORT" \
	HOP_DIR="$HOP_DIR" APP=hop DISK="$DISK" \
	sh "$HOPOS_DIR/image/qemu-run.sh" -qmp "tcp:127.0.0.1:$QMPPORT,server=on,wait=off" \
	</dev/null >"$LOG" 2>&1 &
QPID=$!

# De hostfwd naar gastpoort 9090, via QMP op de draaiende QEMU.
hostfwd() {
	python3 - "$QMPPORT" "$WEBPORT" <<'PY'
import json, socket, sys
qmp, web = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", qmp), timeout=5)
f = s.makefile("rw")
def reply():
    while True:
        m = json.loads(f.readline())
        if "event" not in m:
            return m
json.loads(f.readline())  # de groet
for cmd in ({"execute": "qmp_capabilities"},
            {"execute": "human-monitor-command",
             "arguments": {"command-line": f"hostfwd_add n0 tcp:127.0.0.1:{web}-:9090"}}):
    f.write(json.dumps(cmd) + "\n"); f.flush()
    r = reply()
    if "error" in r or (isinstance(r.get("return"), str) and r["return"].strip()):
        print(f"qmp: {r}", file=sys.stderr); sys.exit(1)
PY
}

has() { tr -d '\r' <"$LOG" | grep -q -E "$1"; }
all() {
	(
		IFS='|'
		for m in $1; do has "$m" || exit 1; done
	)
}

BOOT_MARKS="HOPOS_BOOT|HOPOS_NET_UP|HOPOS_SYSTEM_UP|HOPOS_HOP_START slot=1 |uplink tcp :8080 -> slot 1 :8080 HOPOS_HOP_PUBLISH|uplink tcp :9080 -> slot 1 :9080 HOPOS_HOP_PUBLISH|slot 1: .*HOP_LEADER|slot 1: .*HOP_UP"
PLACE_MARKS="slot 1: .*HOP_JOB_PLACED slot=2|slot 2: 1 port\\(s\\) published tcp\\+udp on the uplink: :9090 HOPOS_SLOT_PUBLISH|slot 2: .*HOPOS_HOPPROM_UP port=9090|slot 2: .*HOPOS_HOPPROM_POLL agents=1 tasks=1"
STOP_MARKS="slot 2: ports withdrawn from the uplink HOPOS_SLOT_UNPUBLISH"
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_APP_PANIC|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOPOS_SLOT_PUBLISH_FAIL"
# Wat /metrics moet zeggen: het cluster (één agent) en de taak van hopprom zelf.
WANT='hop_agents_total 1
hop_agents_healthy 1
hop_tasks_total{state="running"} 1
hop_tasks_placed{job="hopprom"} 1
hop_jobs_total 1
hop_job_instances_placed{job="hopprom"} 1
hop_job_instances_expected{job="hopprom"} 1
hop_job_healthy{job="hopprom"} 1
hop_task_starts_total{job="hopprom"} 1'

# De env van de job: een ronde per seconde, en HOPPROM_AGENT als die hier gezet is.
JOBENV='"HOPPROM_INTERVAL":"1s"'
[ -n "${HOPPROM_AGENT:-}" ] && JOBENV="$JOBENV"',"HOPPROM_AGENT":"'"$HOPPROM_AGENT"'"'
JOB='{"name":"hopprom","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/hopprom-hopos.elf"}],"memory_limit":33554432,"ports":{"metrics":9090},"env":{'"$JOBENV"'}}'
FWD=""
POSTED=""
METRICS_OK=""
HEALTH=""
DELETED=""
GONE=""
START=$(date +%s)
elapsed=0
step() {
	sleep 0.2
	elapsed=$(($(date +%s) - START))
}
alive() {
	! has "$RED" && kill -0 "$QPID" 2>/dev/null && [ "$elapsed" -lt "$TIMEOUT" ]
}
# Elke regel van $WANT staat letterlijk in $METRICS.
complete() {
	printf '%s\n' "$WANT" | while IFS= read -r w; do
		grep -q -x -F "$w" "$METRICS" || exit 1
	done
}

# 1. Boot, de hostfwd, dan de jobspec naar de leader.
while alive && [ -z "$POSTED" ]; do
	if all "$BOOT_MARKS"; then
		if [ -z "$FWD" ]; then
			if hostfwd; then FWD="127.0.0.1:$WEBPORT -> guest :9090"; else FWD="ROOD"; break; fi
		fi
		if out="$(curl -s -m 20 -w ' HTTP %{http_code}' -X POST \
			-H 'Content-Type: application/json' -d "$JOB" \
			"http://127.0.0.1:$LEADERPORT/v1/jobs" 2>&1)"; then
			POSTED="$out"
		else
			POSTED="ROOD curl: $out"
		fi
	fi
	step
done

# 2. De plaatsing en de eerste ronde, dan /metrics en /health van buiten.
while alive && [ -z "$METRICS_OK" ] && [ "$FWD" != ROOD ]; do
	if all "$PLACE_MARKS"; then
		code="$(curl -s -m 5 -o "$METRICS" -w '%{http_code}' "http://127.0.0.1:$WEBPORT/metrics" 2>/dev/null || true)"
		if [ "$code" = 200 ] && complete; then
			METRICS_OK="HTTP $code, $(wc -l <"$METRICS" | tr -d ' ') lines"
			HEALTH="$(curl -s -m 5 -w ' HTTP %{http_code}' "http://127.0.0.1:$WEBPORT/health" 2>&1 || true)"
			break
		fi
	fi
	step
done
[ -n "${KEEP_METRICS:-}" ] && [ -s "$METRICS" ] && cp "$METRICS" "$KEEP_METRICS"

# 3. De stop: de job weg, de poort dicht.
if [ -n "$METRICS_OK" ]; then
	DELETED="$(curl -s -m 20 -w ' HTTP %{http_code}' -X DELETE \
		"http://127.0.0.1:$LEADERPORT/v1/jobs/hopprom" 2>&1 || true)"
	while alive && ! all "$STOP_MARKS"; do step; done
	if all "$STOP_MARKS"; then
		sleep 0.5
		code="$(curl -s -m 3 -o /dev/null -w '%{http_code}' "http://127.0.0.1:$WEBPORT/metrics" 2>/dev/null || true)"
		case "$code" in
		200) GONE="" ;;
		*) GONE="no metrics any more (curl code ${code:-none})" ;;
		esac
	fi
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
		echo "   ROOD $m ontbreekt"
		fail=1
	fi
done
IFS="$IFS_WAS"
case "$FWD" in
"" | ROOD) echo "   ROOD hostfwd via QMP: ${FWD:-nooit gedaan}"; fail=1 ;;
*) echo "   ok  hostfwd via QMP: $FWD" ;;
esac
case "$POSTED" in
*"HTTP 2"*) echo "   ok  POST /v1/jobs: $POSTED" ;;
"") echo "   ROOD POST /v1/jobs nooit gedaan (Hop niet op tijd op)"; fail=1 ;;
*) echo "   ROOD POST /v1/jobs: $POSTED"; fail=1 ;;
esac
if [ -n "$METRICS_OK" ]; then
	echo "   ok  GET http://127.0.0.1:$WEBPORT/metrics: $METRICS_OK, met:"
	printf '%s\n' "$WANT" | sed 's/^/         /'
else
	echo "   ROOD GET http://127.0.0.1:$WEBPORT/metrics: niet alles van:"
	printf '%s\n' "$WANT" | sed 's/^/         /'
	if [ -s "$METRICS" ]; then echo "   wat er wel kwam:"; sed 's/^/         /' "$METRICS"; fi
	fail=1
fi
case "$HEALTH" in
"ok"*"HTTP 200") echo "   ok  GET /health: $(printf '%s' "$HEALTH" | tr '\n' ' ')" ;;
*) echo "   ROOD GET /health: ${HEALTH:-nooit gevraagd}"; fail=1 ;;
esac
case "$DELETED" in
*"HTTP 2"*) echo "   ok  DELETE /v1/jobs/hopprom: $DELETED" ;;
*) echo "   ROOD DELETE /v1/jobs/hopprom: ${DELETED:-nooit gedaan}"; fail=1 ;;
esac
if [ -n "$GONE" ]; then
	echo "   ok  na de stop: $GONE"
else
	echo "   ROOD na de stop: /metrics antwoordde nog, of de stop kwam nooit"
	fail=1
fi
if grep -q "GET /hopprom-hopos.elf" "$ART/http.log" 2>/dev/null; then
	echo "   ok  artifact-server: $(grep -c 'GET /hopprom-hopos.elf' "$ART/http.log") download(s) van hopprom-hopos.elf"
else
	echo "   ROOD artifact-server: nooit gevraagd"
	fail=1
fi
if has "$RED"; then
	echo "   ROOD $(tr -d '\r' <"$LOG" | grep -m1 -E "$RED")"
	fail=1
fi
echo "   tijd: $(($(date +%s) - START)) s na de start van QEMU"
if [ "$fail" != 0 ]; then
	KEEP="$(mktemp -t hopprom-qemu-rood.XXXXXX)"
	tr -d '\r' <"$LOG" >"$KEEP"
	echo "== console bewaard in $KEEP"
	echo "== console:"
	cat "$KEEP"
	exit 1
fi
[ -n "${KEEP_LOG:-}" ] && tr -d '\r' <"$LOG" >"$KEEP_LOG"
echo "hopprom-kring groen"
