#!/usr/bin/env bash
# Usage: run-export.sh <db> <exportId> [exporter args...]   - ArangoDB -> GCS export on arango-dataproc (sync).
# Env: EXECUTORS (default 64), NCOORD (number of coordinators to use, default 24), ASYNC=1 to not wait.
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
DB=$1; EID=$2; shift 2
N=${NCOORD:-24}
EP=$(for i in $(seq 10 $((9 + N))); do printf "10.30.1.%d:8529," $i; done | sed 's/,$//')
gcloud dataproc jobs submit spark --cluster arango-dataproc --region $REGION ${ASYNC:+--async} \
  --class com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  --jars $BUCKET/jars/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar \
  --properties="${SPARK_MASTER:+spark.master=$SPARK_MASTER,}spark.jars.packages=,spark.executor.instances=${EXECUTORS:-64},spark.executor.cores=8,spark.executor.memory=20g,spark.task.maxFailures=10,spark.scheduler.mode=FAIR,spark.sql.files.maxRecordsPerFile=0" \
  -- --endpoints=$EP --database=$DB --output=$BUCKET/exports/$DB --exportId=$EID "$@"
