#!/usr/bin/env bash
# us-east1 footprint for the 10 TB import (user-approved twice, 2026-10-18): the us-central1 Spanner node quota
# (100/region) is used by another instance, so the target instance + Dataflow workers run in us-east1. Idempotent.
set -euo pipefail
source "$(dirname "$0")/env.sh"
R=us-east1
g() { gcloud --project "$PROJECT" "$@"; }
g compute networks subnets describe arango-subnet-e1 --region $R >/dev/null 2>&1 || \
  g compute networks subnets create arango-subnet-e1 --network $NET --region $R --range 10.31.0.0/20 --enable-private-ip-google-access
g compute routers describe arango-router-e1 --region $R >/dev/null 2>&1 || g compute routers create arango-router-e1 --network $NET --region $R
g compute routers nats describe arango-nat-e1 --router arango-router-e1 --region $R >/dev/null 2>&1 || \
  g compute routers nats create arango-nat-e1 --router arango-router-e1 --region $R --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges
g compute firewall-rules describe arango-allow-internal-e1 >/dev/null 2>&1 || \
  g compute firewall-rules create arango-allow-internal-e1 --network $NET --allow tcp,udp,icmp --source-ranges 10.31.0.0/20,$SUBNET_RANGE
g spanner instances describe arango-spanner-e1 >/dev/null 2>&1 || \
  g spanner instances create arango-spanner-e1 --config=regional-us-east1 --nodes=${NODES:-100} \
    --description="arango-10tb-target-e1" --instance-type=provisioned
echo OK
