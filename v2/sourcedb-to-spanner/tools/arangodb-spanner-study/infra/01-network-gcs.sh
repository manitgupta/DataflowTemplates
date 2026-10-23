#!/usr/bin/env bash
# Idempotent: VPC, subnet, NAT, firewall, bucket. All arango-* named.
set -euo pipefail
source "$(dirname "$0")/env.sh"
g() { gcloud --project "$PROJECT" "$@"; }
g compute networks describe $NET >/dev/null 2>&1 || g compute networks create $NET --subnet-mode=custom
g compute networks subnets describe $SUBNET --region $REGION >/dev/null 2>&1 || \
  g compute networks subnets create $SUBNET --network $NET --region $REGION --range $SUBNET_RANGE --enable-private-ip-google-access
g compute routers describe arango-router --region $REGION >/dev/null 2>&1 || g compute routers create arango-router --network $NET --region $REGION
g compute routers nats describe arango-nat --router arango-router --region $REGION >/dev/null 2>&1 || \
  g compute routers nats create arango-nat --router arango-router --region $REGION --auto-allocate-nat-external-ips \
    --nat-all-subnet-ip-ranges --min-ports-per-vm=4096
g compute firewall-rules describe arango-allow-internal >/dev/null 2>&1 || \
  g compute firewall-rules create arango-allow-internal --network $NET --allow tcp,udp,icmp --source-ranges $SUBNET_RANGE
g compute firewall-rules describe arango-allow-iap-ssh >/dev/null 2>&1 || \
  g compute firewall-rules create arango-allow-iap-ssh --network $NET --allow tcp:22 --source-ranges 35.235.240.0/20
gcloud storage buckets describe $BUCKET >/dev/null 2>&1 || \
  gcloud storage buckets create $BUCKET --project $PROJECT --location $REGION --uniform-bucket-level-access
echo OK
