#!/bin/sh
# De end-to-end-run op de host met de echte binaries: agentd standalone met
# de file-store, `hop apply` met een proces-job (sleep 30), `hop jobs` moet
# hem running tonen, `hop delete`, en dan moet het proces weg zijn.
#
# Gebruik: sh tools/e2e-host.sh   (bouwt release; laat de log staan bij een fout)
# Met HOP_BIN=<map> gebruikt hij binaries die er al zijn (zie e2e-linux.sh).
set -e
cd "$(dirname "$0")/.."
if [ -z "$HOP_BIN" ]; then
	cargo build --quiet --release -p agentd -p cli
fi
BIN=${HOP_BIN:-target/release}
DIR=$(mktemp -d /tmp/hop-e2e.XXXXXX)
PORT=${HOP_E2E_PORT:-18080}
KEY=e2e-secret
cat > "$DIR/hop.json" <<JSON
{"node": {"id": "e2e", "ip": "127.0.0.1", "port": $PORT},
 "cluster": {"name": "e2e"},
 "paths": {"state_file": "$DIR/data/state.json", "rootfs_base": "$DIR/tasks"},
 "api_key": "$KEY"}
JSON
"$BIN/agentd" --config "$DIR/hop.json" 2> "$DIR/agentd.log" &
PID=$!
trap 'kill $PID 2>/dev/null || true' EXIT
HOP="$BIN/hop --leader 127.0.0.1:$((PORT + 1000)) --api-key $KEY"
i=0
until grep -q HOP_UP "$DIR/agentd.log"; do
	i=$((i + 1)); [ $i -lt 50 ] || { cat "$DIR/agentd.log"; exit 1; }
	sleep 0.2
done
printf '{"name":"sleeper","command":"sleep 30"}\n' > "$DIR/sleeper.json"
echo "== hop apply"; $HOP apply "$DIR/sleeper.json"
i=0
until $HOP jobs | grep -q "1 running"; do
	i=$((i + 1)); [ $i -lt 50 ] || { $HOP jobs; cat "$DIR/agentd.log"; exit 1; }
	sleep 0.2
done
echo "== hop jobs"; $HOP jobs
echo "== hop status"; $HOP status
echo "== hop agents"; $HOP agents
SPID=$($HOP jobs | awk '$2 == "sleeper" && $4 == "running" {print $5}')
kill -0 "$SPID"
echo "== hop delete"; $HOP delete sleeper
i=0
while kill -0 "$SPID" 2>/dev/null; do
	i=$((i + 1)); [ $i -lt 100 ] || { echo "sleep $SPID still alive"; exit 1; }
	sleep 0.2
done
echo "== hop jobs (after delete)"; $HOP jobs
grep HOP_ "$DIR/agentd.log"
echo "e2e groen (process $SPID gone); log in $DIR/agentd.log"
