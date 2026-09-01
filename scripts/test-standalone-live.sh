#!/bin/sh
# Live R6-R226 acceptance: archive or installed wrapper -> supervised Falkor ->
# loopback backend -> MCP write/read, then a full wrapper restart -> durable read.
set -eu

bin_dir=${EXOCORTEX_BIN_DIR:-"$(pwd)/target/debug"}
archive_runtime="$bin_dir/standalone-runtime"
installed_runtime="$bin_dir/../share/exocortex/standalone"
if [ -n "${EXOCORTEX_REDIS_SERVER:-}" ] || [ -n "${EXOCORTEX_FALKORDB_MODULE:-}" ]; then
  runtime_redis=${EXOCORTEX_REDIS_SERVER:-}
  runtime_module=${EXOCORTEX_FALKORDB_MODULE:-}
elif [ -x "$archive_runtime/redis-server" ] && [ -f "$archive_runtime/falkordb.so" ]; then
  runtime_redis="$archive_runtime/redis-server"
  runtime_module="$archive_runtime/falkordb.so"
elif [ -x "$installed_runtime/redis-server" ] && [ -f "$installed_runtime/falkordb.so" ]; then
  runtime_redis="$installed_runtime/redis-server"
  runtime_module="$installed_runtime/falkordb.so"
else
  runtime_redis=""
  runtime_module=""
fi
if [ ! -x "$runtime_redis" ] || [ ! -f "$runtime_module" ]; then
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

# D27: MCP tool calls are concurrent — a search piped behind an
# end_session is not ordered after it. Sequence by wall clock: write,
# let the submit commit and the SSE feed deliver it, then search in the
# same session, then close stdin (the client's EOF drain answers any
# still-in-flight call before exiting).
first=$(
  {
    printf '%s\n' \
'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"standalone-live","version":"1"}}}' \
'{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.end_session","arguments":{"session_id":"standalone-live-session","project_id":"standalone-live-project","team_id":null,"memories":[{"draft_key":"durable","memory_type":"General","title":"standalone live durable marker","content":"survives a complete wrapper restart","visibility":"org","tags":[]}],"edges":[]}}}'
    sleep 5
    printf '%s\n' \
'{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"exocortex.search_memories","arguments":{"query":"standalone live durable marker","limit":10}}}'
    sleep 2
  } | invoke
)
# The ack rides JSON-in-JSON, so the quotes arrive escaped.
if ! printf '%s\n' "$first" | grep -F '\"accepted\":1' >/dev/null; then
  echo "FAILED: the submit was not acked (dropped response or rejected batch — see docs/bug-prd-standalone-submit-hang.md)" >&2
  printf '%s\n' "$first" | tail -3 >&2
  exit 1
fi
if ! printf '%s\n' "$first" | grep -F 'standalone live durable marker' >/dev/null; then
  echo "FAILED: first session did not round-trip the durable marker (write, SSE-fed read, or the submit hang — see docs/bug-prd-standalone-submit-hang.md)" >&2
  printf '%s\n' "$first" | tail -3 >&2
  exit 1
fi

second=$(
  {
    printf '%s\n' \
'{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"standalone-live-restart","version":"1"}}}' \
'{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}' \
'{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"exocortex.search_memories","arguments":{"query":"standalone live durable marker","limit":10}}}'
    sleep 2
  } | invoke
)
if ! printf '%s\n' "$second" | grep -F 'standalone live durable marker' >/dev/null; then
  echo "FAILED: restart did not durably serve the marker" >&2
  printf '%s\n' "$second" | tail -3 >&2
  exit 1
fi
echo "standalone live persistence validation passed"
