#!/usr/bin/env bash
# Usage: 01-omni-cluster.sh <num_servers> [machine_type] [disk_gb]
# Creates arango-omni-00..NN (10.30.2.(10+i)); 00-02 are root servers, the rest join. Idempotent.
# Env: MAINTENANCE (TERMINATE; Z3 local-SSD machines require MIGRATE), OMNI_ZONE, IP_PREFIX (default 10.30.2), START (first index, default 0), DISK_TYPE (default hyperdisk-balanced; pd-* for families without Hyperdisk;
#      local-ssd = no data disk, LSSD_COUNT x 375 GB local NVMe SSDs RAID0, default 8;
#      bundled = machine type with built-in local SSD, e.g. z3-highmem-44-highlssd).
# Data disk: Hyperdisk Balanced (device-name spanner-data); onHostMaintenance=TERMINATE (TrueTime needs no live migration).
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
N=${1:?num servers}; MT=${2:-n4-standard-32}; DISK=${3:-3000}; ZONE=${OMNI_ZONE:-$ZONE}
DIR=$(cd "$(dirname "$0")" && pwd)
IPP=${IP_PREFIX:-10.30.2}
ROOTS="$IPP.10:15000,$IPP.11:15000,$IPP.12:15000"
pids=()
DT=${DISK_TYPE:-hyperdisk-balanced}
HD=; [ "$DT" = hyperdisk-balanced ] && HD=",provisioned-iops=20000,provisioned-throughput=800"
BT=$DT; [ "$DT" = hyperdisk-balanced ] || BT=pd-balanced
DISK_ARGS="--create-disk=name=\$name-data,device-name=spanner-data,type=$DT,size=${DISK}GB$HD,auto-delete=yes"
[ "$DT" = local-ssd ] && DISK_ARGS=$(printf -- "--local-ssd=interface=NVME %.0s" $(seq 1 ${LSSD_COUNT:-8}))
[ "$DT" = bundled ] && DISK_ARGS=""
START_META=; [ "${START:-0}" -gt 0 ] && START_META=";omni-start=true"
for i in $(seq ${START:-0} $((N-1))); do
  name=$(printf "arango-omni-%02d" $i); ip=$IPP.$((10+i))
  role=join; [ $i -lt 3 ] && role=root
  gcloud compute instances describe $name --zone $ZONE >/dev/null 2>&1 && { echo "exists $name"; continue; }
  gcloud compute instances create $name --project $PROJECT --zone $ZONE --machine-type $MT \
    --network-interface=subnet=$SUBNET,private-network-ip=$ip,no-address,nic-type=GVNIC \
    --image-family=ubuntu-2204-lts --image-project=ubuntu-os-cloud \
    --boot-disk-size=50GB --boot-disk-type=$BT \
    $(eval echo "$DISK_ARGS") \
    --maintenance-policy=${MAINTENANCE:-TERMINATE} --service-account=omni-runner@${PROJECT}.iam.gserviceaccount.com --scopes=cloud-platform \
    --labels=env=arango,fleet=arango-omni \
    --metadata-from-file=startup-script=$DIR/omni-cluster-startup.sh \
    --metadata="^;^omni-role=$role;omni-roots=$ROOTS$START_META" >/dev/null &
  pids+=($!)
done
rc=0
for p in "${pids[@]}"; do wait $p || rc=1; done
echo "created/verified $N omni servers (rc=$rc)"
