#!/usr/bin/env bash
# Usage: arangosh-remote.sh <local-js-file> [db]  - copy JS to arango-db-00 and run it with arangosh there.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
JS=$1; DB=${2:-_system}
gcloud compute scp "$JS" arango-db-00:/tmp/run.js --zone $ZONE --tunnel-through-iap --quiet >/dev/null 2>&1
gcloud compute ssh arango-db-00 --zone $ZONE --tunnel-through-iap --quiet --command \
  "/opt/arangodb3/bin/arangosh --server.endpoint tcp://127.0.0.1:8529 --server.password '' --server.database $DB \
   --javascript.startup-directory /opt/arangodb3/usr/share/arangodb3/js --javascript.execute /tmp/run.js" 2>&1 | grep -v -i -e numpy -e 'tcp_upload' -e '^WARNING: *$' -e '^$'
