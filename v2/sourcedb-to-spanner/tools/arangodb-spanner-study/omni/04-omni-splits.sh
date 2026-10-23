#!/usr/bin/env bash
# Usage: [TOTAL_SPLITS=200] 04-omni-splits.sh <db>  - add the shop10t split points (scripts/gen_splits.py) to the Omni database.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
DB=$1
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TMPD=$(mktemp -d)
python3 "$ROOT/scripts/gen_splits.py" "$TMPD" --total-splits ${TOTAL_SPLITS:-200} --type-zoo-splits ${TYPE_ZOO_SPLITS:-0}
# scp --recurse into an existing dir nests it: clear the remote copy first
gcloud compute ssh arango-omni-00 --zone $ZONE --tunnel-through-iap --quiet --command "rm -rf /tmp/splits" 2>/dev/null
gcloud compute scp --recurse "$TMPD" arango-omni-00:/tmp/splits --zone $ZONE --tunnel-through-iap --quiet >/dev/null 2>&1
gcloud compute ssh arango-omni-00 --zone $ZONE --tunnel-through-iap --quiet --command \
  "sudo rm -rf /mnt/spanner/omni/splits && sudo cp -r /tmp/splits /mnt/spanner/omni/splits && for f in \$(sudo ls /mnt/spanner/omni/splits); do
     sudo docker exec spanneromni /google/spanner/bin/spanner databases splits add $DB --splits-file=/spanner/splits/\$f 2>&1 | grep -iv -e license -e forms.gle -e 'Upgrade to' -e '^\s*\$' | tail -1; done" 2>/dev/null
