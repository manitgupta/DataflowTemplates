#!/usr/bin/env bash
# Usage: 06-omni-restart.sh <server name>...   - graceful rolling restart of spanneromni on the given Omni servers,
# one at a time; waits until every server in the deployment is READY again before the next one.
# Used to clear tablets stuck in unload ("RET_CHECK ... TabletState::PREUNLOAD") whose reads hang.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
SP="sudo docker exec spanneromni /google/spanner/bin/spanner"
zone_of() { gcloud compute instances list --filter="name=$1" --format='value(zone.basename())'; }
ready() { gcloud compute ssh arango-omni-00 --zone $ZONE --tunnel-through-iap --quiet \
  --command "$SP deployment servers list --zone=us-central1-a 2>&1 | grep servers/ | grep -vc READY" 2>/dev/null || true; }
for n in "$@"; do
  z=$(zone_of $n)
  echo "$(date -u +%T) restarting $n ($z)"
  gcloud compute ssh $n --zone $z --tunnel-through-iap --quiet --command "sudo docker restart -t 300 spanneromni >/dev/null" 2>/dev/null
  for i in $(seq 1 60); do
    sleep 10
    nr=$(ready | tail -1)
    [ "$nr" = "0" ] && { echo "$(date -u +%T) all servers READY"; break; }
  done
done
