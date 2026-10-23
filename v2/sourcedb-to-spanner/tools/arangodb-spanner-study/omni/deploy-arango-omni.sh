#!/bin/bash
# Deploy Spanner Omni (single server, plaintext) on arango-omni (10.30.15.30) and create a database from a DDL file.
# Usage: deploy-arango-omni.sh <db> <ddl.sql>     (idempotent)
# Disks are pd-standard because the project's SSD_TOTAL_GB quota is exhausted (small e2e only).
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
DB=$1; DDL=$2
VM=arango-omni; DISK=arango-omni-data; IP=10.30.15.30
HERE="$(cd "$(dirname "$0")" && pwd)"
SSH="timeout 600 gcloud compute ssh $VM --zone $ZONE --tunnel-through-iap --quiet --command"
MT=${MT:-n2-standard-16}; DISK_TYPE=${DISK_TYPE:-pd-standard}; DISK_GB=${DISK_GB:-500}
gcloud compute disks describe $DISK --zone $ZONE >/dev/null 2>&1 || gcloud compute disks create $DISK --zone $ZONE --size ${DISK_GB}GB --type $DISK_TYPE
gcloud compute instances describe $VM --zone $ZONE >/dev/null 2>&1 || gcloud compute instances create $VM --zone $ZONE \
  --machine-type $MT --subnet $SUBNET --private-network-ip $IP --no-address \
  --image-family ubuntu-2204-lts --image-project ubuntu-os-cloud --boot-disk-size 50GB --boot-disk-type pd-standard \
  --disk "name=$DISK,device-name=spanner-data,mode=rw,auto-delete=yes" --labels=env=arango \
  --service-account omni-runner@$PROJECT.iam.gserviceaccount.com --scopes cloud-platform \
  --metadata-from-file startup-script="$HERE/spanner-omni-startup.sh"
for i in $(seq 1 60); do
  $SSH "sudo docker exec spanneromni /google/spanner/bin/spanner databases list" >/dev/null 2>&1 && break
  echo "waiting for spanner omni ($i)"; sleep 20
done
SP="sudo docker exec spanneromni /google/spanner/bin/spanner"
if ! $SSH "$SP databases list" 2>/dev/null | grep -qw "$DB"; then
  gcloud compute scp "$DDL" "$VM:/tmp/ddl.sql" --zone $ZONE --tunnel-through-iap --quiet
  $SSH "sudo docker cp /tmp/ddl.sql spanneromni:/tmp/ddl.sql && $SP databases create $DB --ddl-file=/tmp/ddl.sql --timeout=5m"
fi
$SSH "$SP databases list; sudo ss -ltnp | grep -E ':15000 '"
