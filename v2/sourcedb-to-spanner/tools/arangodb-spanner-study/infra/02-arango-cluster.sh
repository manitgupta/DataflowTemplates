#!/usr/bin/env bash
# Usage: 02-arango-cluster.sh <num_nodes> [machine_type]
# Creates arango-db-00..NN (idempotent) with static IPs 10.30.1.(10+i). The first 3 run agents.
set -euo pipefail
source "$(dirname "$0")/env.sh"
N=${1:?num nodes}
MT=${2:-z3-highmem-16-highlssd}
SA_EMAIL=${SA_EMAIL:-omni-runner@${PROJECT}.iam.gserviceaccount.com}
DIR=$(cd "$(dirname "$0")" && pwd)
TARBALL=$BUCKET/bin/arangodb3-linux-${ARANGO_VERSION}_x86_64.tar.gz
gcloud storage ls $TARBALL >/dev/null 2>&1 || { curl -sL -o /tmp/arango.tgz $ARANGO_TARBALL_URL && gcloud storage cp /tmp/arango.tgz $TARBALL; }
JWTF=$DIR/.arango-jwt
[ -f $JWTF ] || head -c 48 /dev/urandom | base64 | tr -d '\n/+=' > $JWTF
JOIN="${ARANGO_IP_BASE}10,${ARANGO_IP_BASE}11,${ARANGO_IP_BASE}12"
pids=()
for i in $(seq 0 $((N-1))); do
  name=$(printf "%s-%02d" $ARANGO_NODE_PREFIX $i)
  ip=${ARANGO_IP_BASE}$((10+i))
  if gcloud compute instances describe $name --zone $ZONE >/dev/null 2>&1; then echo "exists $name"; continue; fi
  gcloud compute instances create $name --project $PROJECT --zone $ZONE --machine-type $MT \
    --network-interface=subnet=$SUBNET,private-network-ip=$ip,no-address,nic-type=GVNIC \
    --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud --boot-disk-size=100GB --boot-disk-type=pd-balanced \
    --maintenance-policy=MIGRATE --service-account=$SA_EMAIL --scopes=cloud-platform \
    --labels=env=arango,fleet=arango-db \
    --metadata-from-file=startup-script=$DIR/arango-node-startup.sh,arango-jwt=$JWTF \
    --metadata="^;^arango-join=$JOIN;arango-tarball=$TARBALL" >/dev/null &
  pids+=($!)
done
for p in "${pids[@]}"; do wait $p; done
echo "created/verified $N nodes; coordinators at ${ARANGO_IP_BASE}10..$((10+N-1)):8529"
