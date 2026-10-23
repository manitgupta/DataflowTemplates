#!/usr/bin/env bash
# Usage (on arango-ops): run-dv.sh <gcs export root> <spanner-db> <workers> [extra args]
# Runs the gcs-spanner-dv validation template (GCS bulk-format Avro vs Spanner) on Dataflow; results -> BigQuery.
# Env: INSTANCE (default arango-spanner-e1), REGION (us-east1), SUBNET (arango-subnet-e1), MT, JAR, RUN_ID
set -euo pipefail
PROJECT=terraform-demo-host; BUCKET=gs://arango-terraform-demo-host
ROOT=$1; DB=$2; W=$3; shift 3
INSTANCE=${INSTANCE:-arango-spanner-e1}; REGION=${REGION:-us-east1}; SUBNET=${SUBNET:-arango-subnet-e1}
MT=${MT:-n2-standard-16}; JAR=${JAR:-$HOME/am/jars/gcs-spanner-dv.jar}
RUN=arango-dv-$(echo $DB | tr '_' '-')-$(date +%m%d%H%M%S)
RUN_ID=${RUN_ID:-$(echo $RUN | tr '-' '_')}
mkdir -p ~/am/logs
java -cp "$JAR" com.google.cloud.teleport.v2.templates.GCSSpannerDV \
  --runner=DataflowRunner --project=$PROJECT --region=$REGION --jobName=$RUN \
  --serviceAccount=${SA:-omni-runner@${PROJECT}.iam.gserviceaccount.com} \
  --subnetwork=regions/$REGION/subnetworks/$SUBNET --usePublicIps=false \
  --tempLocation=$BUCKET/tmp --stagingLocation=$BUCKET/staging \
  --workerMachineType=$MT --numWorkers=$W --maxNumWorkers=$W --autoscalingAlgorithm=NONE \
  --gcsInputDirectory=$ROOT --projectId=$PROJECT --instanceId=$INSTANCE --databaseId=$DB \
  --bigQueryDataset=arango_dv --runId=$RUN_ID "$@" > ~/am/logs/$RUN.log 2>&1 || { tail -40 ~/am/logs/$RUN.log; exit 1; }
grep -oE "jobs/$REGION/[0-9_-]+" ~/am/logs/$RUN.log | head -1 || true
echo "RUN=$RUN runId=$RUN_ID log=~/am/logs/$RUN.log"
