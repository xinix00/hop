#!/bin/sh
# De cluster van HopOS-nodes op QEMU: een HopOS-node en een agentd op de
# host vormen één cluster met een hoplockserver op de host als lock.
#
# Twee QEMU's naast elkaar bereiken elkaar niet over slirp; één QEMU en
# een host-agent wel: de host is voor de gast 10.0.2.2, en de gast is voor
# de host 127.0.0.1:$AP en :$AP+1000 (de hostfwd naar 8080 en 9080 van de
# gast, en de kern zet die poorten door naar het slot van Hop). Daarom zegt
# de HopOS-node `hopos.advertise=127.0.0.1:$AP` (zijn endpoint en zijn
# lease-adres zoals de host hem ziet), en zegt de host-agent
# `node.ip = 10.0.2.2` (zoals de gast hem ziet). Groen alleen als:
#
#   de klok      een lease is een tijd op de wandklok, en de kern zet de klok
#                vast tot SNTP lukt: een SNTP-server op de host (hopos.ntp,
#                de gast: 10.0.2.2) zet hem (HOP_CLOCK_SYNCED), en pas dan
#                doet de node mee (HOP_CLUSTER_JOIN);
#   de lock      HOPLOCK_UP op een verse datamap;
#   host leidt   de host-agent wordt leider (HOP_LEADER);
#   HopOS-node   HOP_CLUSTER en HOP_UP via de servicer van slot 1, geen
#                HOP_LEADER zolang de host leidt, en `hop agents` (bij de
#                host-leader) toont beide nodes: de HopOS-node registreerde
#                over het LAN bij een leader op een andere node;
#   doorgifte    de agent-poort van de HopOS-node geeft /v1/* door aan de
#                host-leader: `hop agents` gebufferd, `hop events` als stroom
#                (de ping, en de gebeurtenis van de job hieronder);
#   plaatsing    een job met `"affinity": {"node.os": "hopos"}` landt via de
#                host-leader op de HopOS-node (HOP_JOB_PLACED slot=2), en
#                `hop logs` van die taak komt via de leader;
#   failover     de host-agent hard gedood: de HopOS-node neemt de lease
#                over en wordt leider ("became leader ... HOP_LEADER"), met
#                de gecommitte staat van de host-leader (HOP_STATE_LOADED);
#   terug        de host-agent opnieuw gestart registreert bij de HopOS-leader
#                (via de DNAT-poort 9080 van de gast), `hop agents` bij de HopOS-leader
#                toont beide;
#   proxy        een job met `"affinity": {"node.id": "host-1"}` gaat van de
#                HopOS-leader over het LAN naar de host-agent (HOP_JOB_PLACED
#                in de host-log), `hop jobs` (de rondgang van /v1/tasks) toont
#                hem running, `hop logs` en `hop logs -f` (de stroom) geven
#                zijn regel via de HopOS-leader.
#
# Een HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_HOP_FAULT, HOPOS_HOP_EXIT,
# HOPOS_HOP_FAIL, HOP_LOCK_BAD of HOP_SPAWN_FAIL is meteen rood. Rood
# bewaart en toont de logs.
#
# De kern moet de sleutels van de cluster aan Hop doorgeven (`hop_env` in
# hopos/src/config.rs: hopos.lock.type, .url, .key, .apikey,
# hopos.lease_ttl, hopos.advertise, hopos.ntp) en ze op QEMU uit de
# bootargs lezen (`qemu_hop_cfg`), met hopos.node, hopos.cluster en
# hopos.apikey vóór de vaste QEMU_CFG; zonder dat zegt de toets het meteen.
#
#   tools/qemu-test-cluster.sh              TIMEOUT=120 per fase, in seconden
#   HOPOS_DIR=pad tools/qemu-test-cluster.sh   de HopOS-repo (standaard ../../hop-os)
#   HOPLOCK_DIR=pad                         de hoplockserver-repo (standaard
#                                           ../../hoplockserver); zijn bin wordt
#                                           gebouwd in target/ext/hoplock hier
#   KEEP_LOG=map                            bewaart ook groene logs daar
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOPOS_DIR="${HOPOS_DIR:-$DIR/../../hop-os}"
HOPLOCK_DIR="${HOPLOCK_DIR:-$DIR/../../hoplockserver}"
TIMEOUT="${TIMEOUT:-120}"
TARGET=aarch64-unknown-none-softfloat
TMP="$(mktemp -d "${TMPDIR:-/tmp}/hop-cluster.XXXXXX")"
KEY="cluster-hop-secret"
LOCKKEY="cluster-lock-secret"
CLUSTER="hopcl"
QPID=""
HPID=""
LPID=""
APID=""
NPID=""
cleanup() {
	for p in $QPID $HPID $LPID $APID $NPID; do kill "$p" 2>/dev/null || true; done
	# agentd handelt SIGTERM nog niet af (README): zijn taken blijven staan.
	pkill -f "$TMP/host/tasks" 2>/dev/null || true
	true
}
trap cleanup EXIT INT TERM

# Een vrije poort van het OS; `pair` eist ook p+1000 vrij (agent en leader).
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
NTPPORT="$(ports single)"
ARTPORT="$(ports single)"
HP="$(ports pair)"
HLEADER=$((HP + 1000))
# De hostfwd van de gast: $AP naar 8080 (agent), $AP+1000 naar 9080 (leader).
AP="$(ports pair)"
ALEADER=$((AP + 1000))

fail() {
	echo "   ROOD $*"
	for f in lock.log host1.log host2.log qemu.log; do
		[ -f "$TMP/$f" ] && { echo "== $f"; tr -d '\r' <"$TMP/$f" | tail -150; }
	done
	echo "== logs bewaard in $TMP"
	trap - EXIT
	cleanup
	exit 1
}
# wacht <bestand> <regex> <seconden>
wait_for() {
	i=0
	while ! tr -d '\r' <"$1" 2>/dev/null | grep -q -E "$2"; do
		red && fail "rood op de console: $(tr -d '\r' <"$TMP/qemu.log" | grep -m1 -E "$RED")"
		i=$((i + 1))
		[ "$i" -lt $(($3 * 5)) ] || return 1
		sleep 0.2
	done
}
line() { tr -d '\r' <"$1" | grep -m1 -E "$2"; }
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOP_LOCK_BAD|HOP_SPAWN_FAIL"
red() { [ -f "$TMP/qemu.log" ] && tr -d '\r' <"$TMP/qemu.log" | grep -q -E "$RED"; }
# poll <seconden> <regex> <commando...>: tot de uitvoer van het commando de regex bevat.
poll() {
	secs="$1"
	re="$2"
	shift 2
	i=0
	while :; do
		out="$("$@" 2>&1 || true)"
		printf '%s' "$out" | grep -q -E "$re" && return 0
		red && fail "rood op de console: $(tr -d '\r' <"$TMP/qemu.log" | grep -m1 -E "$RED")"
		i=$((i + 1))
		[ "$i" -lt "$secs" ] || { echo "$out"; return 1; }
		sleep 1
	done
}

echo "== bouwen: hopos (qemuvirt) en appspike in $HOPOS_DIR, agentd-hopos, agentd en hop, hoplockserver"
(cd "$HOPOS_DIR" && cargo build --quiet --release --target "$TARGET" -p hopos --features board-qemuvirt &&
	cargo build --quiet --release --target "$TARGET" -p appspike)
(cd "$DIR" && cargo build --quiet --release --target "$TARGET" -p agentd-hopos &&
	cargo build --quiet --release -p agentd -p cli)
CARGO_TARGET_DIR="$DIR/target/ext/hoplock" cargo build --quiet --release \
	--manifest-path "$HOPLOCK_DIR/Cargo.toml" --bin hoplockserver
KERNEL="$HOPOS_DIR/target/$TARGET/release/hopos"
LOCK="$DIR/target/ext/hoplock/release/hoplockserver"
AGENTD="$DIR/target/release/agentd"
HOP="$DIR/target/release/hop"

# De kern moet de cluster-sleutels aan Hop geven en op QEMU uit de bootargs lezen.
for s in hopos.lock.url HOPOS_LOCK_URL HOPOS_ADVERTISE HOPOS_NTP; do
	if ! grep -a -q "$s" "$KERNEL"; then
		echo "   ROOD de kern in $HOPOS_DIR kent '$s' niet: hop_env (hopos/src/config.rs) moet"
		echo "        hopos.lock.type/.url/.key/.apikey, hopos.lease_ttl, hopos.advertise en hopos.ntp"
		echo "        doorgeven, en QEMU moet ze uit de bootargs lezen (zie README: Een cluster van HopOS-nodes)"
		exit 1
	fi
done

OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
strip_to() {
	if [ -n "$OBJCOPY" ]; then "$OBJCOPY" --strip-debug "$1" "$2"; else cp "$1" "$2"; fi
}
strip_to "$DIR/target/$TARGET/release/agentd-hopos" "$TMP/agentd-hopos.elf"
mkdir -p "$TMP/art"
strip_to "$HOPOS_DIR/target/$TARGET/release/appspike" "$TMP/art/appspike.elf"
(cd "$TMP/art" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$TMP/art.log" 2>&1 &
APID=$!

echo "== SNTP op 127.0.0.1:$NTPPORT/udp (de gast: 10.0.2.2:$NTPPORT)"
# De tijd van de host als NTP-server: modus 4, stratum 2, het transmit-veld
# van de vraag als originate (dat toetst Hop), en nu als receive en transmit.
python3 - "$NTPPORT" >"$TMP/ntp.log" 2>&1 <<'PY' &
import socket, struct, sys, time
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("127.0.0.1", int(sys.argv[1])))
print("ntp up", flush=True)
def stamp(t):
    t += 2208988800
    return struct.pack("!II", int(t), int((t % 1) * (1 << 32)))
while True:
    req, addr = s.recvfrom(512)
    if len(req) < 48:
        continue
    now = time.time()
    resp = bytes([0x24, 2, 6, 0xEC]) + bytes(8) + b"LOCL" + stamp(now) + req[40:48] + stamp(now) + stamp(time.time())
    s.sendto(resp, addr)
    print("ntp answered", addr, flush=True)
PY
NPID=$!

echo "== hoplockserver op 127.0.0.1:$LOCKPORT (de gast: 10.0.2.2:$LOCKPORT)"
"$LOCK" -listen "127.0.0.1:$LOCKPORT" -data "$TMP/lockdata" -api-key "$LOCKKEY" 2>"$TMP/lock.log" &
LPID=$!
wait_for "$TMP/lock.log" "HOPLOCK_UP keys=0" 10 || fail "HOPLOCK_UP keys=0 ontbreekt"
echo "   ok  $(line "$TMP/lock.log" HOPLOCK_UP)"

cat >"$TMP/host.json" <<JSON
{"node": {"id": "host-1", "ip": "10.0.2.2", "port": $HP},
 "cluster": {"name": "$CLUSTER", "lock": {"type": "hoplockserver", "url": "http://127.0.0.1:$LOCKPORT", "api_key": "$LOCKKEY"}},
 "timeouts": {"leader_lease": "15s"},
 "paths": {"state_file": "$TMP/host/state.json", "rootfs_base": "$TMP/host/tasks"},
 "api_key": "$KEY"}
JSON
start_host() {
	"$AGENTD" --config "$TMP/host.json" 2>"$TMP/$1" &
	HPID=$!
}

echo "== host-agent host-1 (agent :$HP, leader :$HLEADER; de gast ziet 10.0.2.2)"
start_host host1.log
wait_for "$TMP/host1.log" "became leader" 30 || fail "host-1: geen HOP_LEADER"
echo "   ok  host-1: $(line "$TMP/host1.log" 'became leader')"

echo "== HopOS-node hopos-cl op QEMU virt (tot ${TIMEOUT}s; agent 127.0.0.1:$AP, leader 127.0.0.1:$ALEADER)"
DISK="$TMP/disk.img"
dd if=/dev/zero of="$DISK" bs=1048576 count=0 seek=64 2>/dev/null
SIZE=$(wc -c <"$TMP/agentd-hopos.elf" | tr -d ' ')
BOOTARGS="hopos.node=hopos-cl hopos.cluster=$CLUSTER hopos.apikey=$KEY hopos.advertise=127.0.0.1:$AP hopos.lock.url=http://10.0.2.2:$LOCKPORT hopos.lock.apikey=$LOCKKEY hopos.lease_ttl=15 hopos.ntp=10.0.2.2:$NTPPORT"
qemu-system-aarch64 -M virt,gic-version=3,highmem-ecam=off,virtualization=on \
	-cpu cortex-a53 -smp 4 -m 3G -nographic -monitor none -serial stdio \
	-global virtio-mmio.force-legacy=false \
	-device virtio-net-device,netdev=n0,bus=virtio-mmio-bus.0 \
	-netdev "user,id=n0,hostfwd=tcp:127.0.0.1:$AP-:8080,hostfwd=tcp:127.0.0.1:$ALEADER-:9080" \
	-drive "if=none,format=raw,file=$DISK,id=disk0" \
	-device virtio-blk-device,drive=disk0,bus=virtio-mmio-bus.1 \
	-device "loader,file=$TMP/agentd-hopos.elf,addr=0xb0200000,force-raw=on" \
	-device "loader,addr=0xb0100000,data=$SIZE,data-len=8" \
	-device "loader,addr=0xb0100008,data=1,data-len=8" \
	-append "$BOOTARGS" \
	-kernel "$KERNEL" </dev/null >"$TMP/qemu.log" 2>&1 &
QPID=$!
wait_for "$TMP/qemu.log" "slot 1: .*HOP_UP" "$TIMEOUT" || fail "de HopOS-node: geen HOP_UP"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_CLUSTER$')"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_UP')"
wait_for "$TMP/qemu.log" "slot 1: .*HOP_CLOCK_SYNCED" 30 || fail "de HopOS-node zette zijn klok niet (SNTP van de host)"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_CLOCK_SYNCED')"
wait_for "$TMP/qemu.log" "slot 1: .*HOP_CLUSTER_JOIN" 10 || fail "de HopOS-node deed niet mee na de klok"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_CLUSTER_JOIN')"

H1="$HOP --leader 127.0.0.1:$HLEADER --api-key $KEY"
poll "$TIMEOUT" "hopos-cl" $H1 agents || fail "hop agents bij de host-leader toont hopos-cl niet"
echo "   ok  hop agents (host-leader):"
$H1 agents | sed 's/^/        /'
if tr -d '\r' <"$TMP/qemu.log" | grep -q "became leader"; then
	fail "de HopOS-node werd leider terwijl de host de lease hield"
fi

echo "== doorgifte: de agent-poort van de HopOS-node (127.0.0.1:$AP) naar de host-leader"
HA="$HOP --leader 127.0.0.1:$AP --api-key $KEY"
poll 30 "host-1" $HA agents || fail "hop agents via de agent-poort van de HopOS-node (doorgifte naar de host-leader)"
echo "   ok  hop agents via de HopOS-agent: $($HA agents | grep -c -E 'hopos-cl|host-1') nodes"
$HA events >"$TMP/events.out" 2>&1 &
EPID=$!

echo "== plaatsing: spike (driver hop) met affinity node.os=hopos, via de host-leader"
cat >"$TMP/spike.json" <<JSON
{"name":"spike","driver":"hop","artifacts":[{"url":"http://10.0.2.2:$ARTPORT/appspike.elf"}],"memory_limit":33554432,"affinity":{"node.os":"hopos"}}
JSON
$H1 apply "$TMP/spike.json" || fail "hop apply spike bij de host-leader"
wait_for "$TMP/qemu.log" "slot 1: .*HOP_JOB_PLACED slot=2" "$TIMEOUT" || fail "spike landde niet op de HopOS-node"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_JOB_PLACED slot=2')"
poll "$TIMEOUT" "APPSPIKE" $H1 logs spike || fail "hop logs spike via de host-leader geeft niets van appspike"
echo "   ok  hop logs spike (host-leader): $($H1 logs spike 2>&1 | grep -m1 APPSPIKE)"
i=0
while ! grep -q "spike" "$TMP/events.out" 2>/dev/null; do
	i=$((i + 1))
	[ "$i" -lt 100 ] || { kill "$EPID" 2>/dev/null || true; fail "hop events via de HopOS-agent: geen gebeurtenis van spike: $(cat "$TMP/events.out")"; }
	sleep 0.2
done
kill "$EPID" 2>/dev/null || true
wait "$EPID" 2>/dev/null || true
grep -q "^ping" "$TMP/events.out" || fail "hop events via de HopOS-agent: geen ping: $(cat "$TMP/events.out")"
echo "   ok  hop events via de HopOS-agent (stroom naar de host-leader): $(grep -m1 spike "$TMP/events.out")"

echo "== failover: host-1 hard gedood"
kill -9 "$HPID"
wait "$HPID" 2>/dev/null || true
HPID=""
wait_for "$TMP/qemu.log" "slot 1: .*became leader.*HOP_LEADER" "$TIMEOUT" || fail "de HopOS-node nam de lease niet over"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*became leader')"
wait_for "$TMP/qemu.log" "slot 1: .*HOP_STATE_LOADED" 5 || fail "de HopOS-leader las de gecommitte staat niet"
echo "   ok  $(line "$TMP/qemu.log" 'slot 1: .*HOP_STATE_LOADED')"
grep -q "\"owner\":\"127.0.0.1:$ALEADER\"" "$TMP/lockdata/leases/$CLUSTER" ||
	fail "de lease noemt de HopOS-node niet: $(cat "$TMP/lockdata/leases/$CLUSTER")"
echo "   ok  lease: $(cat "$TMP/lockdata/leases/$CLUSTER")"

echo "== terug: host-1 opnieuw, registreert bij de HopOS-leader (127.0.0.1:$ALEADER)"
start_host host2.log
wait_for "$TMP/host2.log" "HOP_UP" 30 || fail "host-1: geen HOP_UP na de herstart"
H2="$HOP --leader 127.0.0.1:$ALEADER --api-key $KEY"
poll "$TIMEOUT" "host-1" $H2 agents || fail "hop agents bij de HopOS-leader toont host-1 niet"
$H2 agents | grep -q hopos-cl || fail "hop agents bij de HopOS-leader toont zichzelf niet"
echo "   ok  hop agents (HopOS-leader):"
$H2 agents | sed 's/^/        /'
if grep -q "became leader" "$TMP/host2.log"; then
	fail "host-1 werd leider naast de HopOS-leader"
fi

echo "== proxy: hello (proces) met affinity node.id=host-1, via de HopOS-leader"
cat >"$TMP/hello.json" <<'JSON'
{"name":"hello","command":"echo HOP_CLUSTER_HELLO from host-1; sleep 300","affinity":{"node.id":"host-1"}}
JSON
$H2 apply "$TMP/hello.json" || fail "hop apply hello bij de HopOS-leader"
wait_for "$TMP/host2.log" "job hello task .* started HOP_JOB_PLACED" "$TIMEOUT" ||
	fail "hello landde niet op host-1 (de HopOS-leader stuurde /run niet over het LAN)"
echo "   ok  host-1: $(line "$TMP/host2.log" 'job hello task .* HOP_JOB_PLACED')"
poll 30 "hello .*running" $H2 jobs || fail "hop jobs bij de HopOS-leader toont hello niet running"
echo "   ok  hop jobs (HopOS-leader, de rondgang van /v1/tasks):"
$H2 jobs | sed 's/^/        /'
poll 30 "HOP_CLUSTER_HELLO" $H2 logs hello || fail "hop logs hello via de HopOS-leader"
echo "   ok  hop logs hello (HopOS-leader): $($H2 logs hello 2>&1 | grep -m1 HOP_CLUSTER_HELLO)"
$H2 logs -f hello >"$TMP/follow.out" 2>&1 &
FPID=$!
i=0
while ! grep -q HOP_CLUSTER_HELLO "$TMP/follow.out" 2>/dev/null; do
	i=$((i + 1))
	[ "$i" -lt 100 ] || { kill "$FPID" 2>/dev/null || true; fail "hop logs -f hello via de HopOS-leader gaf niets: $(cat "$TMP/follow.out")"; }
	sleep 0.2
done
kill "$FPID" 2>/dev/null || true
wait "$FPID" 2>/dev/null || true
echo "   ok  hop logs -f hello (de stroom via de HopOS-leader): $(grep -m1 HOP_CLUSTER_HELLO "$TMP/follow.out")"
if red; then
	fail "rood op de console: $(tr -d '\r' <"$TMP/qemu.log" | grep -m1 -E "$RED")"
fi

echo "== markers"
tr -d '\r' <"$TMP/qemu.log" | grep -E "slot 1: .*HOP_" | sed 's/^/   /'
grep -h -E "HOP_(LEADER|UP|JOB_PLACED|STATE)" "$TMP/host1.log" "$TMP/host2.log" | sed 's/^/   /'
if [ -n "${KEEP_LOG:-}" ]; then
	mkdir -p "$KEEP_LOG" && cp "$TMP"/*.log "$KEEP_LOG"/
fi
echo "cluster groen; logs in $TMP"
