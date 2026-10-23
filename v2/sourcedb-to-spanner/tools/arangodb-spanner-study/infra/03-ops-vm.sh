#!/usr/bin/env bash
# arango-ops: utility VM (arangosh, generators, validation). Idempotent.
set -euo pipefail
source "$(dirname "$0")/env.sh"
gcloud compute instances describe arango-ops --zone $ZONE >/dev/null 2>&1 && { echo exists; exit 0; }
gcloud compute instances create arango-ops --project $PROJECT --zone $ZONE --machine-type n2-standard-8 \
  --network-interface=subnet=$SUBNET,private-network-ip=10.30.0.5,no-address \
  --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=200GB \
  --service-account=omni-runner@${PROJECT}.iam.gserviceaccount.com --scopes=cloud-platform --labels=env=arango
