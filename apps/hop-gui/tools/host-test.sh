#!/bin/sh
# The dashboard on a plain host: `hop-gui -listen :PORT` next to `agentd`
# (built in $HOP_DIR, standalone with the file store and an API key).
#
# Green only when:
#   hop-gui   HOPGUI_UP on stderr; GET / is index.html and GET /app.js is
#             app.js byte for byte;
#   agentd    HOP_UP; OPTIONS /v1/jobs on the agent port gives the CORS
#             headers (Allow-Origin *, Allow-Methods with PATCH and DELETE,
#             Allow-Headers with X-Hop-Auth and Content-Type);
#   the API   tools/dashboard-api.py post, check --secure and delete against
#             the agent port with the real key: every call signed as app.js
#             signs it, every answer with CORS, an unsigned call refused
#             with 401, the live log of a process job showing its line.
#
#   tools/host-test.sh                 HOP_DIR=path (default ../hop)
#   GUIPORT=3000 AGENTPORT=18380       the ports (the leader is AGENTPORT+1000)
set -eu

DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOP_DIR="$(cd "${HOP_DIR:-$DIR/../hop}" && pwd)"
GUIPORT="${GUIPORT:-3000}"
AGENTPORT="${AGENTPORT:-18380}"
KEY=host-dashboard-key
WORK="$(mktemp -d -t hopgui-host.XXXXXX)"
APID=""
GPID=""
cleanup() {
	[ -n "$APID" ] && kill "$APID" 2>/dev/null
	[ -n "$GPID" ] && kill "$GPID" 2>/dev/null
	true
}
trap cleanup EXIT INT TERM

echo "== build: hop-gui here, agentd in $HOP_DIR"
(cd "$DIR" && cargo build --quiet --release --features std --bin hop-gui)
(cd "$HOP_DIR" && cargo build --quiet --release -p agentd)

cat >"$WORK/hop.json" <<JSON
{"node": {"id": "host1", "ip": "127.0.0.1", "port": $AGENTPORT},
 "cluster": {"name": "host"},
 "paths": {"state_file": "$WORK/data/state.json", "rootfs_base": "$WORK/tasks"},
 "runner": {"isolate": false},
 "api_key": "$KEY"}
JSON
"$HOP_DIR/target/release/agentd" --config "$WORK/hop.json" 2>"$WORK/agentd.log" &
APID=$!
"$DIR/target/release/hop-gui" -listen "127.0.0.1:$GUIPORT" 2>"$WORK/hop-gui.log" &
GPID=$!

wait_log() { # wait_log <file> <marker>
	i=0
	until grep -q "$2" "$1"; do
		i=$((i + 1))
		if [ $i -ge 100 ]; then
			echo "   ROOD $2 not in $1:"
			cat "$1"
			exit 1
		fi
		sleep 0.1
	done
	echo "   ok  $(grep -m1 "$2" "$1")"
}
wait_log "$WORK/hop-gui.log" HOPGUI_UP
wait_log "$WORK/agentd.log" HOP_UP
wait_log "$WORK/agentd.log" HOP_LEADER

fail=0
for f in index.html app.js; do
	path="/$f"
	[ "$f" = index.html ] && path="/"
	code="$(curl -s -m 10 -o "$WORK/got" -w '%{http_code}' "http://127.0.0.1:$GUIPORT$path" || true)"
	if [ "$code" = 200 ] && cmp -s "$WORK/got" "$DIR/$f"; then
		echo "   ok  GET http://127.0.0.1:$GUIPORT$path: HTTP 200, $(wc -c <"$WORK/got" | tr -d ' ') bytes, identical to $f"
	else
		echo "   ROOD GET $path: HTTP ${code:-none}, not identical to $f"
		fail=1
	fi
done

HEADERS="$(curl -s -m 10 -o /dev/null -D - -X OPTIONS \
	-H "Origin: http://127.0.0.1:$GUIPORT" \
	-H 'Access-Control-Request-Method: DELETE' \
	-H 'Access-Control-Request-Headers: content-type,x-hop-auth' \
	"http://127.0.0.1:$AGENTPORT/v1/jobs" | tr -d '\r')"
echo "$HEADERS" | grep -i '^access-control' | sed 's/^/   | /'
if echo "$HEADERS" | grep -q '^HTTP/1.1 200' &&
	echo "$HEADERS" | grep -qi '^access-control-allow-origin: \*' &&
	echo "$HEADERS" | grep -i '^access-control-allow-methods:' | grep -q 'PATCH' &&
	echo "$HEADERS" | grep -i '^access-control-allow-methods:' | grep -q 'DELETE' &&
	echo "$HEADERS" | grep -i '^access-control-allow-headers:' | grep -q 'X-Hop-Auth'; then
	echo "   ok  OPTIONS http://127.0.0.1:$AGENTPORT/v1/jobs: the CORS headers"
else
	echo "   ROOD OPTIONS /v1/jobs: $HEADERS"
	fail=1
fi

API="python3 $DIR/tools/dashboard-api.py"
AGENT="http://127.0.0.1:$AGENTPORT"
JOB='{"name":"gui-probe","command":"echo hop-gui host probe; sleep 60"}'
$API post --agent "$AGENT" --key "$KEY" --body "$JOB" || fail=1
$API wait --agent "$AGENT" --key "$KEY" --job gui-probe || fail=1
$API check --agent "$AGENT" --key "$KEY" --job gui-probe --log-marker "hop-gui host probe" --secure || fail=1
$API delete --agent "$AGENT" --key "$KEY" --job gui-probe || fail=1

if [ "$fail" != 0 ]; then
	echo "== agentd log:"
	cat "$WORK/agentd.log"
	exit 1
fi
rm -rf "$WORK"
echo "hop-gui on the host green"
