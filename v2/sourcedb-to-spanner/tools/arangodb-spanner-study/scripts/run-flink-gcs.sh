#!/usr/bin/env bash
# Usage: run-flink-gcs.sh <name> <export root gs://...> <omni db> [tms=32] [slots=16] [extra beam args...]
# ArangoDB bulk-format export -> Spanner Omni with the optimized portable jar on Flink (GKE arango-flink), using the
# Spanner Migration Omni POC's Flink profile (beam/docs/optimization.md "Flink parity"). Same writer args as Dataflow.
set -euo pipefail
NAME=$1; ROOT=$2; DB=$3; TMS=${4:-32}; SLOTS=${5:-16}; shift $(( $# > 5 ? 5 : $# ))
HERE=$(cd "$(dirname "$0")/.." && pwd)
export IMAGE=${IMAGE:-us-central1-docker.pkg.dev/terraform-demo-host/omni/beam-flink:1.20-arango1}
# Memory: the POC's MySQL profile (managed 0.4, network 0.25/12g) left ~5 GB heap per 16-slot TaskManager and
# OOMed on the larger JSON documents; managed 0.2 + network 4g leaves ~20 GB heap.
export CFG=$ROOT TASK_OFFHEAP=${TASK_OFFHEAP:-8g} MANAGED_FRACTION=${MANAGED_FRACTION:-0.2} NET_FRACTION=${NET_FRACTION:-0.1} \
  NET_MAX=${NET_MAX:-4g} SEGMENT=8kb REQ_PER_GATE=64 \
  JM_CPU=6 JM_MEM=24g JM_MEM_K8S=26Gi JM_POOL=jm-pool
OMNI=${OMNI:-http://10.30.2.100:15000}
bash "$HERE/flink/flink/flink-job.sh" "$NAME" arango "$DB" "$TMS" "$SLOTS" \
  --sourceDbDialect=GCS --spannerOmniEndpoint=$OMNI \
  --useDataStreamForBatch=true --objectReuse=true --maxBundleSize=100000 --maxBundleTimeMills=10000 \
  --writerMode=COMMIT --writerBatchCells=79000 --writerBatchBytes=16777216 --writerBatchRows=8000 \
  --maxCommitDelay=100 --writerConcurrency=8 "$@"
