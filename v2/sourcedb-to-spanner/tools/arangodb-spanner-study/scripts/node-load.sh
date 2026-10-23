#!/usr/bin/env bash
# Print load average + arangod CPU for every arango-db node (parallel).
source "$(dirname "$0")/../infra/env.sh"
for i in $(seq -w 0 23); do
  ( out=$(gcloud compute ssh arango-db-$i --zone $ZONE --tunnel-through-iap --quiet --command \
      "cat /proc/loadavg | cut -d' ' -f1-3; ps -C arangod -o %cpu=,rss= | tr '\n' ' '" 2>/dev/null | tr '\n' ' ')
    echo "arango-db-$i $out" ) &
done | sort
wait
