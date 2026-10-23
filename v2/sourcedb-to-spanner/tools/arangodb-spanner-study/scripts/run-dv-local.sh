#!/usr/bin/env bash
# Usage (workstation): run-dv-local.sh <gcs export root> <spanner-db> <workers> [extra args]
# Launches gcs-spanner-dv on Dataflow as the user's ADC (workers run as the default compute SA, which has BigQuery
# job permissions; omni-runner does not). Quota project forced to terraform-demo-host.
set -euo pipefail
PROJECT=terraform-demo-host; BUCKET=gs://arango-terraform-demo-host
ROOT=$1; DB=$2; W=$3; shift 3
INSTANCE=${INSTANCE:-arango-spanner-e1}; REGION=${REGION:-us-east1}; SUBNET=${SUBNET:-arango-subnet-e1}
MT=${MT:-n2-standard-16}
JAR=${JAR:-/usr/local/google/home/manitgupta/DataflowTemplates/.claude/worktrees/arangodb-gcs-spanner/v2/gcs-spanner-dv/target/gcs-spanner-dv-1.0-SNAPSHOT.jar}
SA=${SA:-797816831047-compute@developer.gserviceaccount.com}
RUN=arango-dv-$(echo $DB | tr '_' '-')-$(date +%m%d%H%M%S)
RUN_ID=${RUN_ID:-$(echo $RUN | tr '-' '_')}
LOG=$(cd "$(dirname "$0")/.." && pwd)/results/$RUN.log
GOOGLE_CLOUD_QUOTA_PROJECT=$PROJECT /usr/lib/jvm/java-17-openjdk-amd64/bin/java -cp "$JAR" \
  com.google.cloud.teleport.v2.templates.GCSSpannerDV \
  --runner=DataflowRunner --project=$PROJECT --region=$REGION --jobName=$RUN --serviceAccount=$SA \
  --subnetwork=regions/$REGION/subnetworks/$SUBNET --usePublicIps=false \
  --tempLocation=$BUCKET/tmp --stagingLocation=$BUCKET/staging \
  --workerMachineType=$MT --numWorkers=$W --maxNumWorkers=$W --autoscalingAlgorithm=NONE \
  --gcsInputDirectory=$ROOT --projectId=$PROJECT --instanceId=$INSTANCE --databaseId=$DB \
  --bigQueryDataset=arango_dv --runId=$RUN_ID "$@" > "$LOG" 2>&1 || { grep -E '"message"|Caused by' "$LOG" | tail -5 | cut -c1-400; exit 1; }
grep -oE "jobs/$REGION/[0-9_-]+" "$LOG" | head -1 || true
echo "RUN=$RUN runId=$RUN_ID log=$LOG"
