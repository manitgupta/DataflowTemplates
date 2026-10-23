#!/usr/bin/env bash
# Usage: 99-cleanup.sh compute|spanner|all
#   compute: delete arango-db-* fleet, arango-ops, arango-omni (single server), the multi-server Omni deployment
#            (arango-omni-NN in any us-central1 zone + its internal LB), GKE arango-flink, arango-dataproc, e2e instance arango-spanner
#   spanner: delete the 10 TB target arango-spanner-e1
#   all:     compute + spanner + bucket + network (arango-vpc, subnets, routers/NATs, firewalls)
set -euo pipefail
source "$(dirname "$0")/env.sh"
WHAT=${1:?compute|spanner|all}
del_compute() {
  vms=$(gcloud compute instances list --filter="name~^arango-db- OR name=arango-ops OR name=arango-omni" \
    --format='value(name)')
  [ -n "$vms" ] && gcloud compute instances delete $vms --zone $ZONE -q
  # multi-server Omni: LB first (forwarding rule -> backend service -> health check), then instance groups and servers
  gcloud compute forwarding-rules delete arango-omni-fr --region $REGION -q 2>/dev/null || true
  gcloud compute backend-services delete arango-omni-bs --region $REGION -q 2>/dev/null || true
  gcloud compute health-checks delete arango-omni-hc --region $REGION -q 2>/dev/null || true
  gcloud compute instance-groups unmanaged list --filter='name~^arango-omni-ig' --format='value(name,zone.basename())' |
    while read ig z; do gcloud compute instance-groups unmanaged delete $ig --zone $z -q </dev/null; done
  gcloud compute instances list --filter='name~^arango-omni-[0-9]+$' --format='value(name,zone.basename())' |
    while read n z; do echo "$n --zone $z"; done | xargs -r -P 16 -L 1 gcloud compute instances delete -q
  gcloud dataproc clusters delete arango-dataproc --region $REGION -q 2>/dev/null || true
  gcloud container clusters delete arango-flink --zone $ZONE -q 2>/dev/null || true
  rm -f ~/.kube/arango-flink.config
  gcloud spanner instances delete arango-spanner -q 2>/dev/null || true
  gcloud compute disks delete arango-omni-data --zone $ZONE -q 2>/dev/null || true
}
del_spanner() { gcloud spanner instances delete arango-spanner-e1 -q 2>/dev/null || true; }
case $WHAT in
  compute) del_compute ;;
  spanner) del_spanner ;;
  all)
    del_compute; del_spanner
    gcloud storage rm -r -q $BUCKET || true
    for fw in arango-allow-internal arango-allow-iap-ssh arango-allow-internal-e1 arango-allow-hc arango-allow-gke-pods; do gcloud compute firewall-rules delete $fw -q || true; done
    gcloud compute routers delete arango-router --region $REGION -q || true
    gcloud compute routers delete arango-router-e1 --region us-east1 -q || true
    gcloud compute networks subnets delete arango-subnet --region $REGION -q || true
    gcloud compute networks subnets delete arango-subnet-e1 --region us-east1 -q || true
    gcloud compute networks delete $NET -q || true ;;
esac
echo "cleanup $WHAT done"
