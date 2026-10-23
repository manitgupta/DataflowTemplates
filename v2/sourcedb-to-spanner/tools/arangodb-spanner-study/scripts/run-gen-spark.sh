#!/usr/bin/env bash
# Usage: run-gen-spark.sh <scale> [extra gen_spark.py args...]   (async Dataproc job; prints job id)
# Env: DB (default shop10t), NCOORD (coordinators 10.30.1.10.., default 24), EXECUTORS (default 64)
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
ROOT=$(cd "$(dirname "$0")/.." && pwd)
SCALE=$1; shift
N=${NCOORD:-24}
EP=$(for i in $(seq 10 $((9 + N))); do printf "10.30.1.%d:8529," $i; done | sed 's/,$//')
gcloud dataproc jobs submit pyspark "$ROOT/data/gen_spark.py" --cluster arango-dataproc --region $REGION --async \
  --properties="spark.executor.instances=${EXECUTORS:-64},spark.executor.cores=8,spark.executor.memory=20g,spark.task.maxFailures=10" \
  --format='value(reference.jobId)' -- --db "${DB:-shop10t}" --scale "$SCALE" --endpoints "$EP" "$@"
