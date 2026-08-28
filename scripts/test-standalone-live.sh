#!/bin/sh
# Live R6-R226 acceptance: installed wrapper -> supervised Falkor -> loopback
# backend -> MCP write/read, then a full wrapper restart -> durable read.
set -eu

bin_dir=${EXOCORTEX_BIN_DIR:-"$(pwd)/target/debug"}
installed_runtime="$bin_dir/../share/exocortex/standalone"
: "${EXOCORTEX_REDIS_SERVER:=$installed_runtime/redis-server}"
: "${EXOCORTEX_FALKORDB_MODULE:=$installed_runtime/falkordb.so}"
export EXOCORTEX_REDIS_SERVER EXOCORTEX_FALKORDB_MODULE
if [ -z "${EXOCORTEX_REDIS_SERVER:-}" ] || [ -z "${EXOCORTEX_FALKORDB_MODULE:-}" ]; then
  echo "UNEXECUTED: set EXOCORTEX_REDIS_SERVER and EXOCORTEX_FALKORDB_MODULE for live standalone validation" >&2
  exit 77
fi
if [ ! -x "$EXOCORTEX_REDIS_SERVER" ] || [ ! -f "$EXOCORTEX_FALKORDB_MODULE" ]; then
  echo "UNEXECUTED: standalone Redis binary or FalkorDB module is unavailable" >&2
  exit 77
fi

for binary in exocortex-node exocortex-mcp-client; do
  [ -x "$bin_dir/$binary" ] || {
    echo "build $binary before live standalone validation" >&2
    exit 2
  }
done

data_dir=$(mktemp -d "${TMPDIR:-/tmp}/exocortex-standalone-live.XXXXXX")
cleanup() {
  rm -rf "$data_dir"
}
trap cleanup EXIT HUP INT TERM

invoke() {
  EXOCORTEX_BIN_DIR=$bin_dir "${EXOCORTEX_WRAPPER:-scripts/exocortex}" \
    --mode mcp-standalone \
    --org standalone-live \
    --user live-user \
    --data-dir "$data_dir"
}

first=$(
  invoke <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"standalone-live","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"standalone-live-session","project_id":"standalone-live-project","team_id":null,"memories":[{"draft_key":"durable","memory_type":"General","title":"standalone live durable marker","content":"survives a complete wrapper restart","visibility":"org","tags":[]}],"edges":[]}}}
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"exocortex.search_memories","arguments":{"query":"standalone live durable marker","limit":10}}}
EOF
)
printf '%s\n' "$first" | grep -F 'standalone live durable marker' >/dev/null

second=$(
  invoke <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"standalone-live-restart","version":"1"}}}
{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.search_memories","arguments":{"query":"standalone live durable marker","limit":10}}}
EOF
)
printf '%s\n' "$second" | grep -F 'standalone live durable marker' >/dev/null
echo "standalone live persistence validation passed"
