#!/usr/bin/env bash
# Usage: 04-dataproc.sh [num_workers] [worker_machine]  - Dataproc 2.2 (Spark 3.5, Scala 2.12) in arango-subnet.
set -euo pipefail
source "$(dirname "$0")/env.sh"
NW=${1:-32}
MT=${2:-n2-standard-16}
gcloud dataproc clusters describe arango-dataproc --region $REGION >/dev/null 2>&1 && { echo exists; exit 0; }
gcloud dataproc clusters create arango-dataproc --project $PROJECT --region $REGION --zone $ZONE \
  --image-version=2.2-debian12 --subnet=$SUBNET --no-address \
  --service-account=omni-runner@${PROJECT}.iam.gserviceaccount.com --scopes=cloud-platform \
  --bucket=${BUCKET#gs://} --temp-bucket=${BUCKET#gs://} \
  --master-machine-type=n2-standard-8 --master-boot-disk-size=200GB \
  --num-workers=$NW --worker-machine-type=$MT --worker-boot-disk-size=500GB --worker-boot-disk-type=pd-standard --master-boot-disk-type=pd-standard \
  --labels=env=arango \
  --properties='spark:spark.jars.packages=com.arangodb:arangodb-spark-datasource-3.5_2.12:1.10.0,spark:spark.scheduler.mode=FAIR,spark:spark.dynamicAllocation.enabled=false'
