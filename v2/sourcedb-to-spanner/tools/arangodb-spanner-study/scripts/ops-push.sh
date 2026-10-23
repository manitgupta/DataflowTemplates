#!/usr/bin/env bash
# Copy repo data/ and scripts/ to arango-ops:~/am/
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
gcloud compute ssh arango-ops --zone $ZONE --tunnel-through-iap --quiet --command 'mkdir -p ~/am' 2>/dev/null
gcloud compute scp --recurse "$ROOT/data" "$ROOT/scripts" arango-ops:~/am/ --zone $ZONE --tunnel-through-iap --quiet 2>&1 | grep -v -i -e numpy -e tcp_upload -e '^WARNING' || true
echo pushed
