#!/usr/bin/env bash
# Usage (on arango-ops): run-dataflow-gcs.sh <manifest gs:// URI> <spanner-db> <workers> [extra template args]
# Runs the sourcedb-to-spanner template (GCS dialect) on Dataflow from the bundled jar (main class SourceDbToSpanner).
# Env: INSTANCE (default arango-spanner), MT (worker machine type, default n2-standard-16),
#      JAR (default ~/am/jars/sourcedb-to-spanner-gcs-v1.jar), OMNI (Spanner Omni endpoint; overrides instance)
set -euo pipefail
PROJECT=terraform-demo-host; BUCKET=gs://arango-terraform-demo-host
REGION=${REGION:-us-central1}; SUBNET=${SUBNET:-arango-subnet}
MANIFEST=$1; DB=$2; W=$3; shift 3
INSTANCE=${INSTANCE:-arango-spanner}; MT=${MT:-n2-standard-16}
JAR=${JAR:-$HOME/am/jars/sourcedb-to-spanner-gcs-v1.jar}
RUN=arango-df-$(echo $DB | tr '_' '-')-$(date +%m%d%H%M%S)
mkdir -p ~/am/logs
OMNI_ARGS=()
[ -n "${OMNI:-}" ] && OMNI_ARGS=(--spannerOmniEndpoint=$OMNI)
java -cp "$JAR" com.google.cloud.teleport.v2.templates.SourceDbToSpanner \
  --runner=DataflowRunner --project=$PROJECT --region=$REGION --jobName=$RUN \
  --serviceAccount=omni-runner@${PROJECT}.iam.gserviceaccount.com \
  --subnetwork=regions/$REGION/subnetworks/$SUBNET --usePublicIps=false \
  --tempLocation=$BUCKET/tmp --stagingLocation=$BUCKET/staging \
  --workerMachineType=$MT --numWorkers=$W --maxNumWorkers=$W --autoscalingAlgorithm=NONE \
  --sourceDbDialect=GCS --sourceConfigURL=$MANIFEST \
  --projectId=$PROJECT --instanceId=$INSTANCE --databaseId=$DB \
  --outputDirectory=$BUCKET/dlq/$RUN "${OMNI_ARGS[@]}" "$@" > ~/am/logs/$RUN.log 2>&1 || { tail -40 ~/am/logs/$RUN.log; exit 1; }
grep -oE "jobs/$REGION/[0-9_-]+" ~/am/logs/$RUN.log | head -1 || true
grep -oE '"Submitted job: [^"]*' ~/am/logs/$RUN.log | head -1 || true
echo "RUN=$RUN log=~/am/logs/$RUN.log"
