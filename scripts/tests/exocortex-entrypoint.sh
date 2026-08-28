#!/bin/sh
set -eu

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
log=$tmp/invocations

cat >"$tmp/exocortex-mcp-client" <<'EOF'
#!/bin/sh
printf 'client:%s\n' "$*" >>"$EXOCORTEX_ENTRYPOINT_LOG"
if [ "${EXOCORTEX_DEPLOYMENT_MODE:-}" = mcp-standalone ]; then
  attempts=0
  while ! grep -F 'node:--mode mcp-standalone' "$EXOCORTEX_ENTRYPOINT_LOG" >/dev/null; do
    attempts=$((attempts + 1))
    [ "$attempts" -lt 100 ] || exit 1
    sleep 0.01
  done
fi
EOF
cat >"$tmp/exocortex-node" <<'EOF'
#!/bin/sh
printf 'node:%s\n' "$*" >>"$EXOCORTEX_ENTRYPOINT_LOG"
case " $* " in
  *" --mode mcp-standalone "*)
    runtime=""
    while [ "$#" -gt 0 ]; do
      if [ "$1" = --standalone-runtime-file ]; then runtime=$2; shift 2; else shift; fi
    done
    printf "EXOCORTEX_BACKEND='http://127.0.0.1:43119'\nEXOCORTEX_SSE_KEY='0000000000000000000000000000000000000000000000000000000000000000'\n" >"$runtime"
    trap 'exit 0' TERM INT
    while :; do sleep 1; done
    ;;
esac
EOF
chmod +x "$tmp/exocortex-mcp-client" "$tmp/exocortex-node"
export EXOCORTEX_BIN_DIR=$tmp
export EXOCORTEX_ENTRYPOINT_LOG=$log

scripts/exocortex --mode mcp-client --probe client
scripts/exocortex --mode mcp-standalone --probe standalone
scripts/exocortex --mode backend-node --probe backend

grep -F 'client:--probe client' "$log"
grep -F 'node:--mode mcp-standalone' "$log"
grep -F 'client:--backend http://127.0.0.1:43119 --probe standalone' "$log"
grep -F 'node:--mode backend-node --probe backend' "$log"

rm "$tmp/exocortex-node"
if scripts/exocortex --mode backend-node --probe broken >/dev/null 2>&1; then
  echo 'broken backend topology unexpectedly succeeded' >&2
  exit 1
fi

echo 'exocortex entrypoint topology tests passed'
