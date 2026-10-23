#!/usr/bin/env bash
# Usage: joblog.sh [jobId] [grep-regex]  - print (filtered) driver output of a Dataproc job (default: latest).
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
J=${1:-}
if [ -z "$J" ] || [ "$J" = latest ]; then
  J=$(gcloud dataproc jobs list --region $REGION --cluster arango-dataproc --limit 20 --format='value(reference.jobId,status.stateStartTime)' | sort -k2 | tail -1 | cut -f1)
fi
RE=${2:-.}
URI=$(gcloud dataproc jobs describe $J --region $REGION --format='value(driverOutputResourceUri)')
echo "job $J state=$(gcloud dataproc jobs describe $J --region $REGION --format='value(status.state)')"
gcloud storage cat "${URI}*" 2>/dev/null | grep -E "$RE" || true
