#!/usr/bin/env bash
# Usage (on arango-ops): run-dataflow-portable.sh <export root gs://...> <omni db> [workers=32] [extra beam args...]
# Same optimized portable jar + writer args as run-flink-gcs.sh, on Dataflow (n2-standard-16 = Flink TaskManager).
# Env: WRITER_CONCURRENCY (in-flight commits per writer, default 8), JAR (default ~/am/jars/omni-migration-dataflow-arango1.jar), OMNI (default http://10.30.2.100:15000), MT.
set -euo pipefail
ROOT=$1; DB=$2; W=${3:-32}; shift $(( $# > 3 ? 3 : $# ))
PROJECT=terraform-demo-host; BUCKET=gs://arango-terraform-demo-host; REGION=us-central1
JAR=${JAR:-$HOME/am/jars/omni-migration-dataflow-arango1.jar}; MT=${MT:-n2-standard-16}
OMNI=${OMNI:-http://10.30.2.100:15000}
RUN=arango-dfp-$(echo $DB | tr '_' '-')-$(date +%m%d%H%M%S)
mkdir -p ~/am/logs
nohup java -cp "$JAR" com.google.cloud.teleport.v2.templates.SourceDbToSpannerPortableMain \
  --runner=DataflowRunner --project=$PROJECT --region=$REGION --jobName=$RUN \
  --serviceAccount=omni-runner@${PROJECT}.iam.gserviceaccount.com \
  --subnetwork=regions/$REGION/subnetworks/arango-subnet --usePublicIps=false \
  --tempLocation=$BUCKET/tmp --stagingLocation=$BUCKET/staging \
  --workerMachineType=$MT --numWorkers=$W --maxNumWorkers=$W --autoscalingAlgorithm=NONE \
  --sourceDbDialect=GCS --sourceConfigURL=$ROOT --projectId=$PROJECT --instanceId=default --databaseId=$DB \
  --spannerOmniEndpoint=$OMNI --outputDirectory=$BUCKET/dlq/$RUN \
  --writerMode=COMMIT --writerBatchCells=79000 --writerBatchBytes=16777216 --writerBatchRows=8000 \
  --maxCommitDelay=100 --writerConcurrency=${WRITER_CONCURRENCY:-8} "$@" > ~/am/logs/$RUN.log 2>&1 < /dev/null &
sleep 90
grep -oE "jobs/$REGION/[0-9_-]+" ~/am/logs/$RUN.log | head -1 || tail -20 ~/am/logs/$RUN.log
echo "RUN=$RUN log=~/am/logs/$RUN.log"
