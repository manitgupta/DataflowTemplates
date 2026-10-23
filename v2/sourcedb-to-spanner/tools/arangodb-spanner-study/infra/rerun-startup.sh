#!/usr/bin/env bash
# Usage: rerun-startup.sh <first> <last>  - push current startup script to arango-db-<i> and re-run it.
set -euo pipefail
source "$(dirname "$0")/env.sh"
DIR=$(cd "$(dirname "$0")" && pwd)
for i in $(seq $1 $2); do
  name=$(printf "%s-%02d" $ARANGO_NODE_PREFIX $i)
  ( gcloud compute instances add-metadata $name --zone $ZONE --metadata-from-file=startup-script=$DIR/arango-node-startup.sh >/dev/null &&
    gcloud compute ssh $name --zone $ZONE --tunnel-through-iap --quiet \
      --command 'sudo google_metadata_script_runner startup >/dev/null 2>&1; systemctl is-active arango-starter' 2>/dev/null | sed "s/^/$name: /" ) &
done
wait
