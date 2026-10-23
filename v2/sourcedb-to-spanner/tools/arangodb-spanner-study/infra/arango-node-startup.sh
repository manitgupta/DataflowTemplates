#!/usr/bin/env bash
# GCE startup script for an ArangoDB 3.11 (Apache-2.0) cluster node, started via the arangodb starter.
# Idempotent per boot: assembles/creates RAID0 over local NVMe, mounts it, installs the tarball, writes a
# systemd unit for the starter. Metadata keys: arango-join (comma list of first 3 node IPs), arango-jwt,
# arango-tarball (gs:// URL).
set -euxo pipefail
md() { curl -sf -H 'Metadata-Flavor: Google' "http://metadata.google.internal/computeMetadata/v1/instance/attributes/$1"; }
MYIP=$(curl -sf -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
JOIN=$(md arango-join)
TARBALL=$(md arango-tarball)
MNT=/mnt/arango

# 1. Local NVMe RAID0 -> XFS
if ! mountpoint -q $MNT; then
  mkdir -p $MNT
  if [ ! -e /dev/md0 ]; then
    mdadm --assemble --scan || true
  fi
  if [ ! -e /dev/md0 ]; then
    DEVS=$(ls /dev/disk/by-id/google-local-nvme-ssd-* | grep -v part)
    N=$(echo $DEVS | wc -w)
    if [ "$N" -gt 1 ]; then
      mdadm --create /dev/md0 --run --level=0 --chunk=256 --raid-devices=$N $DEVS
    else
      ln -sf $DEVS /dev/md0
    fi
  fi
  blkid /dev/md0 | grep -q xfs || mkfs.xfs -f -K /dev/md0
  mount -o noatime,nodiscard /dev/md0 $MNT
fi

# 2. OS tuning (ArangoDB recommendations)
sysctl -w vm.max_map_count=2048000 vm.overcommit_memory=0 vm.zone_reclaim_mode=0
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
echo madvise > /sys/kernel/mm/transparent_hugepage/defrag

# 3. Install tarball
if [ ! -x /opt/arangodb3/bin/arangodb ]; then
  mkdir -p /opt/arangodb3
  gcloud storage cp "$TARBALL" /tmp/arangodb3.tgz
  tar xzf /tmp/arangodb3.tgz -C /opt/arangodb3 --strip-components=1
fi
md arango-jwt > /etc/arangodb.secret
chmod 600 /etc/arangodb.secret
mkdir -p $MNT/data

# 4. Starter as systemd unit (agents on the first 3 nodes, coordinator + dbserver everywhere)
MEM_GB=$(awk '/MemTotal/ {printf "%d", $2/1024/1024}' /proc/meminfo)
cat > /etc/systemd/system/arango-starter.service <<EOF
[Unit]
Description=ArangoDB starter
After=network-online.target
[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
Environment=PATH=/opt/arangodb3/bin:/opt/arangodb3/usr/sbin:/usr/bin:/bin
ExecStart=/opt/arangodb3/bin/arangodb \
  --starter.data-dir=$MNT/data --starter.address=$MYIP --starter.join=$JOIN \
  --auth.jwt-secret=/etc/arangodb.secret \
  --server.arangod=/opt/arangodb3/usr/sbin/arangod \
  --server.js-dir=/opt/arangodb3/usr/share/arangodb3/js \
  --args.all.server.statistics=false \
  --args.all.cache.size=2147483648 \
  --args.dbservers.rocksdb.write-buffer-size=268435456 \
  --args.dbservers.rocksdb.max-write-buffer-number=24 \
  --args.dbservers.rocksdb.total-write-buffer-size=$(( MEM_GB / 5 ))G \
  --args.dbservers.rocksdb.max-total-wal-size=2147483648 \
  --args.dbservers.rocksdb.block-cache-size=$(( MEM_GB / 6 ))G \
  --args.dbservers.rocksdb.max-subcompactions=4 \
  --args.dbservers.rocksdb.level0-slowdown-trigger=40 \
  --args.dbservers.rocksdb.pending-compactions-slowdown-trigger=68719476736 \
  --args.dbservers.rocksdb.pending-compactions-stop-trigger=274877906944 \
  --args.coordinators.query.memory-limit=0
Restart=always
RestartSec=5
[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now arango-starter
