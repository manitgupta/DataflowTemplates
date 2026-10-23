#!/bin/bash
# GCE startup-script (also usable as cloud-init user-data on EC2 / Azure custom-data).
# Idempotent: formats + mounts the data disk, installs Docker with its data-root on the data
# disk, and runs Spanner Omni single-server (host network, plaintext gRPC on :15000).
set -euxo pipefail
OMNI_IMAGE="${OMNI_IMAGE:-us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts}"
# GCE: /dev/disk/by-id/google-<device-name>. EC2 NVMe EBS: /dev/nvme1n1; Azure: /dev/disk/azure/scsi1/lun0
DATA_DEV="${DATA_DEV:-/dev/disk/by-id/google-spanner-data}"
MNT=/mnt/spanner

for i in $(seq 1 60); do [ -e "$DATA_DEV" ] && break; sleep 2; done
if ! blkid "$DATA_DEV" >/dev/null 2>&1; then
  mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$DATA_DEV"
fi
mkdir -p "$MNT"
if ! grep -q " $MNT " /etc/fstab; then
  echo "UUID=$(blkid -s UUID -o value "$DATA_DEV") $MNT ext4 discard,defaults,nofail 0 2" >> /etc/fstab
fi
mountpoint -q "$MNT" || mount "$MNT"

# Docker, with all images/volumes on the data disk.
mkdir -p /etc/docker "$MNT/docker"
if [ ! -f /etc/docker/daemon.json ]; then
  echo "{\"data-root\": \"$MNT/docker\"}" > /etc/docker/daemon.json
fi
if ! command -v docker >/dev/null; then
  apt-get update -y
  DEBIAN_FRONTEND=noninteractive apt-get install -y docker.io
fi
systemctl enable --now docker

# NOTE: start-single-server listens on localhost only by default (--listen-addresses "localhost");
# other VMs (Flink workers, validation) need 0.0.0.0.
OMNI_ARGS="${OMNI_ARGS:-start-single-server --listen-addresses=0.0.0.0}"
docker volume inspect spanner >/dev/null 2>&1 || docker volume create spanner
if docker ps -a --format '{{.Names}}' | grep -qx spanneromni; then
  # Recreate if image/args changed (data lives in the "spanner" volume, so it is preserved).
  if [ "$(docker inspect -f '{{.Config.Image}} {{join .Config.Cmd " "}}' spanneromni)" != "$OMNI_IMAGE $OMNI_ARGS" ]; then
    docker rm -f spanneromni
  fi
fi
if ! docker ps -a --format '{{.Names}}' | grep -qx spanneromni; then
  docker pull "$OMNI_IMAGE"
  # shellcheck disable=SC2086
  docker run -d --network host --restart unless-stopped --name spanneromni \
    --ulimit nofile=1048576:1048576 \
    -v "spanner:/spanner" "$OMNI_IMAGE" $OMNI_ARGS
else
  docker start spanneromni || true
fi
