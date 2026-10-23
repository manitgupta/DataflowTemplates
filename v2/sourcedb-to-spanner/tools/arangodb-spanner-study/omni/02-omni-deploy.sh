#!/usr/bin/env bash
# Usage: 02-omni-deploy.sh <num_servers>
# After 01-omni-cluster.sh: create the multi-server deployment on root 00, start the join servers, list servers.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
N=${1:?num servers}
DIR=$(cd "$(dirname "$0")" && pwd)
ssh_() { gcloud compute ssh "$1" --zone $ZONE --tunnel-through-iap --quiet --command "$2" 2>/dev/null; }
SP="sudo docker exec spanneromni /google/spanner/bin/spanner"
for n in 00 01 02; do echo "root $n: $(ssh_ arango-omni-$n 'sudo docker ps --format "{{.Status}}"; sudo docker logs spanneromni 2>&1 | tail -1' | tr '\n' ' ')"; done
# root hosts follow IP_PREFIX (same convention as 01-omni-cluster.sh)
IPP=${IP_PREFIX:-10.30.2}
sed "s/10\.30\.2\.1\([012]\)/$IPP.1\1/" "$DIR/deployment.yaml" > /tmp/arango-omni-deployment.yaml
gcloud compute scp /tmp/arango-omni-deployment.yaml arango-omni-00:/tmp/deployment.yaml --zone $ZONE --tunnel-through-iap --quiet >/dev/null 2>&1
if ! ssh_ arango-omni-00 "$SP deployment zones list" 2>/dev/null | grep -q us-central1-a; then
  ssh_ arango-omni-00 "sudo cp /tmp/deployment.yaml /mnt/spanner/omni/deployment.yaml && $SP deployment create --config-file=/spanner/deployment.yaml 2>&1 | tail -5"
fi
ssh_ arango-omni-00 "$SP deployment zones list 2>&1 | tail -3"
for i in $(seq 3 $((N-1))); do
  name=$(printf "arango-omni-%02d" $i)
  ( gcloud compute instances add-metadata $name --zone $ZONE --metadata=omni-start=true >/dev/null 2>&1 &&
    ssh_ $name 'sudo google_metadata_script_runner startup >/dev/null 2>&1; sudo docker ps --format "{{.Status}}"' | sed "s/^/$name: /" ) &
done
wait
sleep 30
ssh_ arango-omni-00 "$SP deployment servers list --zone=us-central1-a 2>&1 | tail -15"
