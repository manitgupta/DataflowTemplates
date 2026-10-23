#!/usr/bin/env bash
# Usage: 03-omni-db-lb.sh <db> <ddl.sql> <num_servers>
# Create a database on the multi-server Omni deployment and an internal TCP load balancer (10.30.2.100:15000)
# over all servers (health check :15012/healthz). Idempotent.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
DB=$1; DDL=$2; N=$3
SP="sudo docker exec spanneromni /google/spanner/bin/spanner"
ssh_() { gcloud compute ssh "$1" --zone $ZONE --tunnel-through-iap --quiet --command "$2" 2>/dev/null; }
if ! ssh_ arango-omni-00 "$SP databases list" | grep -qw "$DB"; then
  gcloud compute scp "$DDL" arango-omni-00:/tmp/ddl.sql --zone $ZONE --tunnel-through-iap --quiet >/dev/null 2>&1
  ssh_ arango-omni-00 "sudo cp /tmp/ddl.sql /mnt/spanner/omni/ddl.sql && $SP databases create $DB --ddl-file=/spanner/ddl.sql --timeout=10m 2>&1 | tail -2"
fi
ssh_ arango-omni-00 "$SP databases list 2>&1 | grep -E 'NAME|$DB'"
# internal passthrough TCP LB
g() { gcloud --project "$PROJECT" "$@"; }
g compute instance-groups unmanaged describe arango-omni-ig --zone $ZONE >/dev/null 2>&1 || {
  g compute instance-groups unmanaged create arango-omni-ig --zone $ZONE >/dev/null
  g compute instance-groups unmanaged add-instances arango-omni-ig --zone $ZONE \
    --instances=$(for i in $(seq 0 $((N-1))); do printf "arango-omni-%02d," $i; done | sed 's/,$//') >/dev/null; }
g compute health-checks describe arango-omni-hc --region $REGION >/dev/null 2>&1 || \
  g compute health-checks create http arango-omni-hc --region $REGION --port 15012 --request-path /healthz >/dev/null
g compute backend-services describe arango-omni-bs --region $REGION >/dev/null 2>&1 || {
  g compute backend-services create arango-omni-bs --region $REGION --load-balancing-scheme=INTERNAL --protocol=TCP \
    --health-checks=arango-omni-hc --health-checks-region=$REGION >/dev/null
  g compute backend-services add-backend arango-omni-bs --region $REGION --instance-group=arango-omni-ig \
    --instance-group-zone=$ZONE >/dev/null; }
g compute forwarding-rules describe arango-omni-fr --region $REGION >/dev/null 2>&1 || \
  g compute forwarding-rules create arango-omni-fr --region $REGION --load-balancing-scheme=INTERNAL --network=$NET \
    --subnet=$SUBNET --address=10.30.2.100 --ip-protocol=TCP --ports=15000 --backend-service=arango-omni-bs \
    --backend-service-region=$REGION >/dev/null
g compute firewall-rules describe arango-allow-hc >/dev/null 2>&1 || \
  g compute firewall-rules create arango-allow-hc --network $NET --allow tcp:15012,tcp:15000 \
    --source-ranges 35.191.0.0/16,130.211.0.0/22 >/dev/null
echo "LB: 10.30.2.100:15000"
