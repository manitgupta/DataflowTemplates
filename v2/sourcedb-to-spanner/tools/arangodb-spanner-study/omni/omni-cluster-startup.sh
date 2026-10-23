#!/bin/bash
# GCE startup-script for a multi-server Spanner Omni node (single zone). Metadata:
#   omni-role  = root | join          (root servers are listed in deployment.yaml; join servers use --join-servers)
#   omni-roots = 10.30.2.10:15000,...  (root server endpoints, for join servers)
#   omni-start = true                  (join servers only start once the deployment exists)
# Data disk (Hyperdisk Balanced, device-name spanner-data, or RAID0 of local NVMe SSDs) is ext4 at /mnt/spanner; Docker data-root lives there.
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1" || true; }
IP=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
ROLE=$(md omni-role); ROOTS=$(md omni-roots); START=$(md omni-start)
OMNI_IMAGE=us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts
DATA_DEV=/dev/disk/by-id/google-spanner-data
MNT=/mnt/spanner

LSSD=$(ls /dev/disk/by-id/google-local-nvme-ssd-* 2>/dev/null || true)
if [ -n "$LSSD" ]; then
  # Local SSD servers (no spanner-data disk): RAID0 all local NVMe SSDs.
  DATA_DEV=/dev/md0
  [ -e "$DATA_DEV" ] || mdadm --create "$DATA_DEV" --run --level=0 --raid-devices=$(echo "$LSSD" | wc -w) $LSSD
fi
for i in $(seq 1 60); do [ -e "$DATA_DEV" ] && break; sleep 2; done
blkid "$DATA_DEV" >/dev/null 2>&1 || mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$DATA_DEV"
mkdir -p "$MNT"
grep -q " $MNT " /etc/fstab || echo "UUID=$(blkid -s UUID -o value "$DATA_DEV") $MNT ext4 discard,defaults,nofail 0 2" >> /etc/fstab
mountpoint -q "$MNT" || mount "$MNT"
mkdir -p /etc/docker "$MNT/docker" "$MNT/omni"
[ -f /etc/docker/daemon.json ] || echo "{\"data-root\": \"$MNT/docker\"}" > /etc/docker/daemon.json
command -v docker >/dev/null || { apt-get update -y; DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io; }
systemctl enable --now docker
docker image inspect "$OMNI_IMAGE" >/dev/null 2>&1 || docker pull "$OMNI_IMAGE"

case "$ROLE" in
  root) ARGS="start --root --server-address=$IP --zone=us-central1-a --base-dir=/spanner" ;;
  join) [ "$START" = "true" ] || exit 0
        ARGS="start --server-address=$IP --join-servers=$ROOTS --zone=us-central1-a --base-dir=/spanner" ;;
  *) exit 0 ;;
esac
if docker ps -a --format '{{.Names}}' | grep -qx spanneromni; then
  docker start spanneromni || true
else
  # shellcheck disable=SC2086
  docker run -d --network host --restart unless-stopped --stop-timeout 300 --name spanneromni \
    --ulimit nofile=1048576:1048576 -v "$MNT/omni:/spanner" "$OMNI_IMAGE" $ARGS
fi
