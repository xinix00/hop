#!/bin/sh
# De bewoner op QEMU: de kern start Hop, Hop plaatst hoplockserver-hopos met
# een gepubliceerde poort, en van buiten praat curl het CAS-protocol.
#
# De kern (HopOS uit $HOPOS_DIR, standaard ../hop-os) boot op QEMU virt met
# agentd-hopos (uit $HOP_DIR, standaard ../hop/hop) in slot 1, zoals
# tools/qemu-test-welcome.sh daar. Van buiten gaat één jobspec naar de
# leader van Hop: deze bewoner van een artifact-server op de host (voor de
# gast 10.0.2.2), met "ports":{"http":8090}, de sleutel in de env en het
# volume van de job. De kern zet uplink-poort 8090 door naar het slot
# (DNAT), en QEMU's hostfwd brengt 127.0.0.1:$LOCKPORT naar poort 8090 van
# de gast. Groen alleen als:
#
#   kern        HOPOS_BOOT, HOPOS_NET_UP, HOPOS_SYSTEM_UP, HOPOS_HOP_START
#               slot=1 en Hop's twee poorten (HOPOS_HOP_PUBLISH);
#   Hop         HOP_UP en HOP_LEADER via de servicer van slot 1;
#   de plaatsing HOP_JOB_PLACED slot=2, ":8090 HOPOS_SLOT_PUBLISH" en
#               "slot 2: ... HOPOS_HOPLOCK_UP keys=0";
#   het protocol PUT met If-None-Match: * geeft 200 en een ETag die de
#               sha256 van de body is; een tweede zo'n PUT 412; GET geeft
#               de body en dezelfde ETag; zonder X-API-Key 401; PUT met
#               If-Match op de ETag 200 en een nieuwe ETag; DELETE met de
#               oude ETag 412, met de nieuwe 204, en daarna GET 404;
#   de herstart  een tweede sleutel geschreven, de job weg (DELETE, de kern
#               trekt de poort in: HOPOS_SLOT_UNPUBLISH) en opnieuw
#               geplaatst: de bewoner komt weer op, en de sleutel staat er
#               nog met dezelfde ETag (het volume van de job).
#
# Een HOPOS_PANIC, HOPOS_EXCEPTION, HOPOS_APP_PANIC, HOPOS_HOP_FAULT,
# HOPOS_HOP_EXIT, HOPOS_HOP_FAIL, HOPOS_SLOT_PUBLISH_FAIL of
# HOPOS_HOPLOCK_FAIL is meteen rood. Rood bewaart de console.
#
#   tools/qemu-test.sh                   TIMEOUT=120 standaard, in seconden
#   KEEP_LOG=pad tools/qemu-test.sh      bewaart ook een groene console
#   HOPOS_DIR=pad HOP_DIR=pad            de repo's van HopOS en Hop
#   EXT_TARGET=pad                       waar kern en Hop gebouwd worden
#                                        (standaard target/ext in deze repo:
#                                        in de andere repo's verandert niets)
#   VOLUME=/volumes/hoplock              het volume van de job op /data
#                                        (standaard; VOLUME= toetst zonder)
#   SYSPORT/AGENTPORT/LEADERPORT/ARTPORT/LOCKPORT   de host-poorten;
#                                        standaard (en bezet) een vrije van het OS
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
TIMEOUT="${TIMEOUT:-120}"
HOPOS_DIR="$(cd "${HOPOS_DIR:-$DIR/../hop-os}" && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop/hop}" && pwd)"
EXT_TARGET="${EXT_TARGET:-$DIR/target/ext}"
TARGET=aarch64-unknown-none-softfloat
KEY="qemu-lock-secret"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/hoplock-qemu.XXXXXX")"
LOG="$WORK/console.log"
ART="$WORK/art"
DISK="$WORK/disk.img"
QPID=""
HPID=""
mkdir -p "$ART"
cleanup() {
	[ -n "$QPID" ] && kill "$QPID" 2>/dev/null
	[ -n "$HPID" ] && kill "$HPID" 2>/dev/null
	true
}
trap cleanup EXIT INT TERM

# Een host-poort: de gevraagde als hij vrij is, anders (en zonder vraag) een
# vrije van het OS. Geen vaste standaard: een tweede QEMU-kring op dezelfde
# machine (een andere repo, een andere agent) houdt 8080 en 9080 vaak al bezet.
port() {
	python3 - "$1" "$2" <<'PY2'
import socket, sys
want, name = int(sys.argv[1] or 0), sys.argv[2]
s = socket.socket()
try:
    s.bind(("127.0.0.1", want))
    print(s.getsockname()[1])
except OSError:
    s.close()
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    got = s.getsockname()[1]
    print(f"   {name} {want} is taken, using {got}", file=sys.stderr)
    print(got)
s.close()
PY2
}
SYSPORT="$(port "${SYSPORT:-}" SYSPORT)"
AGENTPORT="$(port "${AGENTPORT:-}" AGENTPORT)"
LEADERPORT="$(port "${LEADERPORT:-}" LEADERPORT)"
ARTPORT="$(port "${ARTPORT:-}" ARTPORT)"
LOCKPORT="$(port "${LOCKPORT:-}" LOCKPORT)"

echo "== bouwen: hoplockserver-hopos, de kern in $HOPOS_DIR, agentd-hopos in $HOP_DIR (target $EXT_TARGET)"
cargo build --quiet --release --manifest-path "$DIR/Cargo.toml" --target "$TARGET" \
	--no-default-features --features hopos --bin hoplockserver-hopos
(cd "$HOPOS_DIR" && CARGO_TARGET_DIR="$EXT_TARGET/hopos" cargo build --quiet --release --locked \
	--target "$TARGET" -p hopos --features board-qemuvirt)
(cd "$HOP_DIR" && CARGO_TARGET_DIR="$EXT_TARGET/hop" cargo build --quiet --release --locked \
	--target "$TARGET" -p agentd-hopos)
KERNEL="$EXT_TARGET/hopos/$TARGET/release/hopos"

# Zonder debug-info; de symbolen blijven, want de plaatsing leest ze.
OBJCOPY="$(ls "$(rustc --print sysroot)"/lib/rustlib/*/bin/rust-objcopy 2>/dev/null | head -1)"
strip_to() {
	if [ -n "$OBJCOPY" ]; then "$OBJCOPY" --strip-debug "$1" "$2"; else cp "$1" "$2"; fi
}
strip_to "$DIR/target/$TARGET/release/hoplockserver-hopos" "$ART/hoplockserver-hopos.elf"
HOP_ELF="$WORK/agentd-hopos.elf"
strip_to "$EXT_TARGET/hop/$TARGET/release/agentd-hopos" "$HOP_ELF"
HOP_SIZE=$(wc -c <"$HOP_ELF" | tr -d ' ')
(cd "$ART" && exec python3 -m http.server "$ARTPORT" --bind 127.0.0.1) >"$WORK/http.log" 2>&1 &
HPID=$!
dd if=/dev/zero of="$DISK" bs=1048576 count=0 seek=64 2>/dev/null

echo "== booten op QEMU virt met Hop (tot ${TIMEOUT}s; leader :$LEADERPORT, artifacts :$ARTPORT, lock :$LOCKPORT -> gast :8090)"
# De regel van image/qemu-run.sh in HopOS (APP=hop), met één hostfwd erbij.
FWD="hostfwd=tcp:127.0.0.1:${SYSPORT}-:10100,hostfwd=tcp:127.0.0.1:${AGENTPORT}-:8080"
FWD="$FWD,hostfwd=tcp:127.0.0.1:${LEADERPORT}-:9080,hostfwd=tcp:127.0.0.1:${LOCKPORT}-:8090"
qemu-system-aarch64 -M virt,gic-version=3,highmem-ecam=off,virtualization=on \
	-cpu cortex-a53 -smp 4 -m 3G -nographic -monitor none -serial stdio \
	-global virtio-mmio.force-legacy=false \
	-device virtio-net-device,netdev=n0,bus=virtio-mmio-bus.0 \
	-netdev "user,id=n0,$FWD" \
	-drive "if=none,format=raw,file=$DISK,id=disk0" \
	-device virtio-blk-device,drive=disk0,bus=virtio-mmio-bus.1 \
	-device "loader,file=$HOP_ELF,addr=0xb0200000,force-raw=on" \
	-device "loader,addr=0xb0100000,data=$HOP_SIZE,data-len=8" \
	-device "loader,addr=0xb0100008,data=1,data-len=8" \
	-kernel "$KERNEL" </dev/null >"$LOG" 2>&1 &
QPID=$!

has() { tr -d '\r' <"$LOG" | grep -q -E "$1"; }
count() { tr -d '\r' <"$LOG" | grep -c -E "$1" || true; }
all() {
	(
		IFS='|'
		for m in $1; do has "$m" || exit 1; done
	)
}

BOOT_MARKS="HOPOS_BOOT|HOPOS_NET_UP|HOPOS_SYSTEM_UP|HOPOS_HOP_START slot=1 |uplink tcp :8080 -> slot 1 :8080 HOPOS_HOP_PUBLISH|uplink tcp :9080 -> slot 1 :9080 HOPOS_HOP_PUBLISH|slot 1: .*HOP_LEADER|slot 1: .*HOP_UP"
PLACE_MARKS="slot 1: .*HOP_JOB_PLACED slot=2|slot 2: 1 port\\(s\\) published tcp\\+udp on the uplink: :8090 HOPOS_SLOT_PUBLISH|slot 2: .*HOPOS_HOPLOCK_UP keys=0"
STOP_MARKS="slot 2: ports withdrawn from the uplink HOPOS_SLOT_UNPUBLISH"
UP="slot [0-9]+: .*HOPOS_HOPLOCK_UP keys="
RED="HOPOS_PANIC|HOPOS_EXCEPTION|HOPOS_APP_PANIC|HOPOS_HOP_FAULT|HOPOS_HOP_EXIT|HOPOS_HOP_FAIL|HOPOS_SLOT_PUBLISH_FAIL|HOPOS_HOPLOCK_FAIL"

# Zonder volume staat /data in de eigen root van het slot, en die is bij
# elke start leeg (kern/src/rpc.rs): dan is de herstart-stap per definitie
# rood. Met een volume draagt START_SLOT de mount (HopOS alpha.11).
# Standaard een volume: sinds HopOS alpha.11 en Hop 88f1d3a gaan de volumes
# van een jobspec mee in START_SLOT, dus de sleutel overleeft een herstart
# van de job. VOLUME= (leeg) toetst de kale vorm zonder volume.
VOLUME="${VOLUME-/volumes/hoplock}"
VOLUMES=""
[ -n "${VOLUME:-}" ] && VOLUMES=',"volumes":{"'"$VOLUME"'":"/data"}'
JOB='{"name":"hoplock","driver":"hop","artifacts":[{"url":"http://10.0.2.2:'"$ARTPORT"'/hoplockserver-hopos.elf"}],"memory_limit":33554432,"ports":{"http":8090},"env":{"HOPLOCK_API_KEY":"'"$KEY"'"}'"$VOLUMES"'}'
LEADER="http://127.0.0.1:$LEADERPORT"
BASE="http://127.0.0.1:$LOCKPORT"
START=$(date +%s)
elapsed=0
step() {
	sleep 0.2
	elapsed=$(($(date +%s) - START))
}
alive() {
	! has "$RED" && kill -0 "$QPID" 2>/dev/null && [ "$elapsed" -lt "$TIMEOUT" ]
}

# Eén verzoek aan de bewoner: "<status> <etag>" en de body in $WORK/body.
# lock <methode> <pad> <body|-> [kop...]
lock() {
	m="$1" p="$2" b="$3"
	shift 3
	set -- -s -m 10 -X "$m" -D "$WORK/head" -o "$WORK/body" -w '%{http_code}' "$@"
	[ "$b" != - ] && set -- "$@" --data-binary "$b"
	code="$(curl "$@" "$BASE$p" 2>/dev/null || true)"
	etag="$(tr -d '\r' <"$WORK/head" 2>/dev/null | awk -F': ' 'tolower($1) == "etag" {print $2}')"
	echo "$code $etag"
}
sha() { printf '"%s"' "$(printf '%s' "$1" | shasum -a 256 | cut -d' ' -f1)"; }

OKS=""
REDS=""
ok() { OKS="$OKS
   ok  $*"; }
red() { REDS="$REDS
   ROOD $*"; }
expect() { # expect <wat> <gekregen> <verwacht-patroon>
	case "$2" in
	$3) ok "$1: $2" ;;
	*) red "$1: kreeg '$2', wilde '$3'" ;;
	esac
}

post_job() {
	curl -s -m 20 -w ' HTTP %{http_code}' -X POST -H 'Content-Type: application/json' \
		-d "$JOB" "$LEADER/v1/jobs" 2>&1 || true
}

# 1. Boot, dan de jobspec naar de leader.
POSTED=""
while alive && [ -z "$POSTED" ]; do
	all "$BOOT_MARKS" && POSTED="$(post_job)"
	step
done
case "$POSTED" in
*"HTTP 2"*) ok "POST /v1/jobs: $POSTED" ;;
*) red "POST /v1/jobs: ${POSTED:-nooit gedaan (Hop niet op tijd op)}" ;;
esac

# 2. De plaatsing, dan het protocol van buiten.
while alive && ! all "$PLACE_MARKS"; do step; done
PROTO=""
if all "$PLACE_MARKS"; then
	PROTO=1
	B1='{"owner":"qemu","generation":1}'
	B2='{"owner":"qemu","generation":2}'
	H="X-API-Key: $KEY"
	r="$(lock PUT /leases/qemu "$B1" -H "$H" -H 'If-None-Match: *')"
	E1="${r#* }"
	expect "PUT If-None-Match: * (aanmaak)" "$r" "200 $(sha "$B1")"
	expect "tweede PUT If-None-Match: *" "$(lock PUT /leases/qemu x -H "$H" -H 'If-None-Match: *')" "412 "
	r="$(lock GET /leases/qemu - -H "$H")"
	expect "GET" "$r" "200 $E1"
	expect "GET body" "$(cat "$WORK/body")" "$B1"
	expect "PUT zonder X-API-Key" "$(lock PUT /leases/qemu x -H 'If-None-Match: *')" "401 "
	r="$(lock PUT /leases/qemu "$B2" -H "$H" -H "If-Match: $E1")"
	E2="${r#* }"
	expect "PUT If-Match (vernieuwing)" "$r" "200 $(sha "$B2")"
	expect "DELETE If-Match oude ETag" "$(lock DELETE /leases/qemu - -H "$H" -H "If-Match: $E1")" "412 "
	expect "DELETE If-Match nieuwe ETag" "$(lock DELETE /leases/qemu - -H "$H" -H "If-Match: $E2")" "204 "
	expect "GET na DELETE" "$(lock GET /leases/qemu - -H "$H")" "404 "
	expect "GET /health" "$(lock GET /health -)" "200 "
fi

# 3. De herstart: een sleutel die moet blijven, de job weg en terug.
if [ -n "$PROTO" ]; then
	BS='{"jobs":["keep-me"]}'
	r="$(lock PUT /state/qemu "$BS" -H "$H" -H 'If-None-Match: *')"
	ES="${r#* }"
	expect "PUT /state/qemu (moet de herstart overleven)" "$r" "200 $(sha "$BS")"
	COMMITS="$(count HOPOS_FS_COMMIT)"
	DELETED="$(curl -s -m 20 -w ' HTTP %{http_code}' -X DELETE "$LEADER/v1/jobs/hoplock" 2>&1 || true)"
	case "$DELETED" in
	*"HTTP 2"*) ok "DELETE /v1/jobs/hoplock: $DELETED" ;;
	*) red "DELETE /v1/jobs/hoplock: $DELETED" ;;
	esac
	while alive && ! all "$STOP_MARKS"; do step; done
	all "$STOP_MARKS" && ok "de stop: $(tr -d '\r' <"$LOG" | grep -m1 -E "$STOP_MARKS")"
	# Een commit van hopfs na de stop (kern/src/rpc.rs, de committer: bij
	# de stop van een slot of binnen 10 s): wat de bewoner schreef, staat
	# dan op de schijf.
	i=0
	while alive && [ "$(count HOPOS_FS_COMMIT)" -le "$COMMITS" ] && [ "$i" -lt 75 ]; do
		i=$((i + 1))
		step
	done
	if [ "$(count HOPOS_FS_COMMIT)" -gt "$COMMITS" ]; then
		ok "hopfs na de stop: $(tr -d '\r' <"$LOG" | grep HOPOS_FS_COMMIT | tail -1)"
	else
		red "hopfs legde de boom na de stop niet vast (15 s)"
	fi
	REPOSTED="$(post_job)"
	case "$REPOSTED" in
	*"HTTP 2"*) ok "POST /v1/jobs opnieuw: $REPOSTED" ;;
	*) red "POST /v1/jobs opnieuw: $REPOSTED" ;;
	esac
	while alive && [ "$(count "$UP")" -lt 2 ]; do step; done
	if [ "$(count "$UP")" -ge 2 ]; then
		ok "weer op: $(tr -d '\r' <"$LOG" | grep -E "$UP" | tail -1)"
		# De poort staat pas open na de publicatie; even geduld voor de DNAT.
		i=0
		while :; do
			r="$(lock GET /state/qemu - -H "$H")"
			case "$r" in 000*) ;; *) break ;; esac
			i=$((i + 1))
			[ "$i" -lt 25 ] || break
			sleep 0.2
		done
		why=""
		[ -z "$VOLUMES" ] && why=" (zonder volume: Hop alpha.10 geeft een job op HopOS geen volume, en de eigen root van een slot is bij elke start leeg)"
		expect "GET /state/qemu na de herstart (het volume)$why" "$r" "200 $ES"
		KEYS="$(tr -d '\r' <"$LOG" | grep -E "$UP" | tail -1 | sed 's/.*keys=\([0-9]*\).*/\1/')"
		expect "HOPOS_HOPLOCK_UP na de herstart$why" "keys=$KEYS" "keys=1"
	else
		red "de bewoner kwam na de herstart niet meer op"
	fi
fi

kill "$QPID" 2>/dev/null || true
wait "$QPID" 2>/dev/null || true
QPID=""

IFS_WAS="$IFS"
IFS='|'
for m in $BOOT_MARKS $PLACE_MARKS $STOP_MARKS; do
	if has "$m"; then
		echo "   ok  $m: $(tr -d '\r' <"$LOG" | grep -m1 -E "$m")"
	else
		echo "   ROOD $m ontbreekt"
		REDS="$REDS
   ROOD $m ontbreekt"
	fi
done
IFS="$IFS_WAS"
printf '%s\n' "$OKS" | sed '/^$/d'
if grep -q "GET /hoplockserver-hopos.elf" "$WORK/http.log" 2>/dev/null; then
	echo "   ok  artifact-server: $(grep -c 'GET /hoplockserver-hopos.elf' "$WORK/http.log") download(s)"
else
	REDS="$REDS
   ROOD artifact-server: nooit gevraagd"
fi
if has "$RED"; then
	REDS="$REDS
   ROOD $(tr -d '\r' <"$LOG" | grep -m1 -E "$RED")"
fi
echo "== markers van de bewoner"
tr -d '\r' <"$LOG" | grep -E "HOPOS_HOPLOCK|HOPOS_FS_SAVED|HOP_JOB_PLACED" | sed 's/^/   /'
echo "   tijd: $(($(date +%s) - START)) s na de start van QEMU"
if [ -n "$REDS" ]; then
	printf '%s\n' "$REDS" | sed '/^$/d'
	echo "== console bewaard in $LOG"
	exit 1
fi
[ -n "${KEEP_LOG:-}" ] && tr -d '\r' <"$LOG" >"$KEEP_LOG"
echo "hoplock-kring groen"
