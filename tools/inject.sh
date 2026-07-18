#!/bin/bash
# inject.sh — Submit a job directly to the jobengine (bypasses proxy/go-micro)
#
# Usage:
#   ./inject.sh <pipeline> [key=value ...]
#   ./inject.sh build-pod repo=opencloud branch=kosmos git_base=https://github.com/KOSMOS-OpenCloud
#   ./inject.sh test-echo
#
# Environment:
#   JOBENGINE_URL   Internal jobengine URL (default: http://127.0.0.1:9310)
#   JOBENGINE_HOST  SSH host for remote inject (default: empty = local)
#
# This script talks directly to the jobengine HTTP port, without going through
# the OpenCloud proxy. Use this when the proxy is broken or for debugging.

set -euo pipefail

URL="${JOBENGINE_URL:-http://127.0.0.1:9310}"
HOST="${JOBENGINE_HOST:-}"
PIPELINE="${1:-}"
shift 2>/dev/null || true

if [ -z "$PIPELINE" ]; then
    echo "Usage: inject.sh <pipeline> [key=value ...]"
    echo ""
    echo "Examples:"
    echo "  inject.sh test-echo"
    echo "  inject.sh build-pod repo=opencloud branch=kosmos"
    echo "  JOBENGINE_HOST=root@cloud.brandis.eu inject.sh build-pod repo=opencloud"
    echo ""
    echo "Set JOBENGINE_HOST to inject via SSH into a container:"
    echo "  JOBENGINE_HOST=root@cloud.brandis.eu inject.sh ..."
    exit 1
fi

# Build params JSON from key=value args
PARAMS="{}"
for arg in "$@"; do
    key="${arg%%=*}"
    value="${arg#*=}"
    PARAMS=$(echo "$PARAMS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
d['$key'] = '$value'
print(json.dumps(d))
" 2>/dev/null || echo "$PARAMS")
done

BODY=$(python3 -c "
import json
print(json.dumps({
    'pipeline': '$PIPELINE',
    'resources': [],
    'params': $PARAMS,
    'priority': 5,
}))
")

echo "Injecting: pipeline=$PIPELINE"
echo "Params: $PARAMS"
echo ""

if [ -n "$HOST" ]; then
    # Remote inject via SSH into the container
    CONTAINER="${JOBENGINE_CONTAINER:-opencloud_full-opencloud-1}"
    RESULT=$(ssh "$HOST" "podman exec $CONTAINER curl -s -m 10 -X POST -H 'Content-Type: application/json' -d '$BODY' '$URL/api/v0/jobs/'" 2>&1)
else
    RESULT=$(curl -s -m 10 -X POST -H "Content-Type: application/json" -d "$BODY" "$URL/api/v0/jobs/" 2>&1)
fi

echo "$RESULT" | python3 -c "
import sys, json
try:
    j = json.load(sys.stdin)
    if 'jobId' in j:
        print(f\"Job: {j['jobId']}\")
        print(f\"Status: {j['status']}\")
        print(f\"ValidTill: {j.get('validTill', '?')}\")
    elif 'error' in j:
        print(f\"ERROR: {j['error']}\")
        sys.exit(1)
    else:
        print(json.dumps(j, indent=2))
except:
    print(sys.stdin.read() if hasattr(sys.stdin, 'read') else '')
    print('ERROR: no JSON response')
    sys.exit(1)
" || echo "Raw: $RESULT"
