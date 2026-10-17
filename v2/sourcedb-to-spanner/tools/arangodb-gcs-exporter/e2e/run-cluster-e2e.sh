#!/usr/bin/env bash
# Copyright (C) 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# End-to-end check of the exporter against a REAL ArangoDB 3.11 cluster (no Docker):
#   1. seed fixtures (incl. hostile_keys) into a dedicated database
#   2. plan-only + partition verification on hostile_keys (RANGE and HASH):
#      sum of per-partition AQL COUNTs must equal the collection count
#   3. full export with partition verification
#   4. optional: create the Spanner/Omni schema from the generated DDL (apply_ddl.py),
#      template import, validate.py (set SPANNER_PROJECT etc.)
#
# Spark runs either locally (SPARK_MODE=local, needs SPARK_HOME of Spark 3.5.x) or on a
# Dataproc on GCE cluster (SPARK_MODE=dataproc, DATAPROC_CLUSTER, DATAPROC_REGION).
#
# Usage (from any directory):
#   ARANGO_URL=http://10.30.1.10:8529 SPARK_HOME=~/spark-3.5.3-bin-hadoop3 ./run-cluster-e2e.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXP="$(cd "${HERE}/.." && pwd)"
W="$(cd "${EXP}/../../../.." && pwd)"

ARANGO_URL="${ARANGO_URL:-http://10.30.1.10:8529}"
ARANGO_HOSTPORT="${ARANGO_URL#*://}"
ARANGO_ENDPOINTS="${ARANGO_ENDPOINTS:-${ARANGO_HOSTPORT%/}}"
ARANGO_DB="${ARANGO_DB:-exporter_e2e}"
export ARANGO_PASSWORD="${ARANGO_PASSWORD:-}"
DOCS="${DOCS:-10000}"
SHARDS="${SHARDS:-16}"
SCALA_BIN="${SCALA_BIN:-2.12}"          # Dataproc 2.2 / spark-3.5.x-bin-hadoop3 are Scala 2.12
SPARK_MODE="${SPARK_MODE:-local}"
if [[ "${SPARK_MODE}" == "dataproc" ]]; then
  # Executors and driver run on different machines: the export must go to shared storage.
  if [[ -z "${OUTPUT:-}" ]]; then
    : "${GCS_STAGING:?SPARK_MODE=dataproc needs OUTPUT=gs://... or GCS_STAGING=gs://...}"
    OUTPUT="${GCS_STAGING%/}/exports"
  fi
  if [[ "${OUTPUT}" != gs://* ]]; then
    echo "SPARK_MODE=dataproc needs OUTPUT on gs:// (got ${OUTPUT})" >&2
    exit 2
  fi
fi
OUTPUT="${OUTPUT:-file:///tmp/arango-export}"
EXPORT_ID="${EXPORT_ID:-e2e$(date -u +%Y%m%dT%H%M%SZ)}"
PARTITIONS_HOSTILE="${PARTITIONS_HOSTILE:-64}"
JAR="${EXP}/target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala${SCALA_BIN}.jar"
MAIN=com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export MAVEN_SKIP_RC=1

log() { echo "=== $*" >&2; }

if [[ ! -f "${JAR}" || "${REBUILD:-0}" == "1" ]]; then
  log "Building exporter (Scala ${SCALA_BIN})"
  profile=()
  [[ "${SCALA_BIN}" == "2.12" ]] && profile=(-Pscala-2.12)
  mvn -B -q -f "${EXP}/pom.xml" -s "${W}/.mvn/settings.xml" package -DskipTests "${profile[@]}"
fi

submit() {
  # Runs the exporter with the given arguments.
  case "${SPARK_MODE}" in
    local)
      : "${SPARK_HOME:?set SPARK_HOME to a Spark 3.5.x (Scala ${SCALA_BIN}) distribution}"
      "${SPARK_HOME}/bin/spark-submit" --master "${SPARK_MASTER:-local[4]}" \
        --conf spark.scheduler.mode=FAIR \
        --conf "spark.driver.extraJavaOptions=-Dlog4j.rootLogger=WARN" \
        --class "${MAIN}" "${JAR}" "$@"
      ;;
    dataproc)
      : "${DATAPROC_CLUSTER:?}" "${DATAPROC_REGION:?}"
      local staged="${DATAPROC_JAR_URI:-}"
      if [[ -z "${staged}" ]]; then
        : "${GCS_STAGING:?set GCS_STAGING=gs://bucket/path to stage the jar}"
        staged="${GCS_STAGING%/}/$(basename "${JAR}")"
        gcloud storage cp "${JAR}" "${staged}" >&2
      fi
      gcloud dataproc jobs submit spark --cluster="${DATAPROC_CLUSTER}" \
        --region="${DATAPROC_REGION}" --class="${MAIN}" --jars="${staged}" \
        --properties=spark.scheduler.mode=FAIR \
        -- "$@"
      ;;
    *) echo "unknown SPARK_MODE ${SPARK_MODE}" >&2; exit 2 ;;
  esac
}

COMMON=(--endpoints="${ARANGO_ENDPOINTS}" --database="${ARANGO_DB}" --protocol=http2
        --output="${OUTPUT}")
if [[ -n "${ARANGO_PASSWORD_FILE:-}" ]]; then
  COMMON+=(--passwordFile="${ARANGO_PASSWORD_FILE}")
fi

log "1. Seeding ${ARANGO_URL} database ${ARANGO_DB} (docs=${DOCS}, shards=${SHARDS})"
python3 "${HERE}/seed.py" --arango-url "${ARANGO_URL}" --password "${ARANGO_PASSWORD}" \
  --database "${ARANGO_DB}" --docs "${DOCS}" --shards "${SHARDS}"

log "2a. RANGE plan + verify on hostile_keys (${PARTITIONS_HOSTILE} partitions)"
submit "${COMMON[@]}" --exportId="${EXPORT_ID}-plan-range" --collections=hostile_keys \
  --partitions="${PARTITIONS_HOSTILE}" --verifyPartitions=true --planOnly=true

log "2b. HASH plan + verify on hostile_keys (8 partitions)"
submit "${COMMON[@]}" --exportId="${EXPORT_ID}-plan-hash" --collections=hostile_keys \
  --partitioning=HASH --partitions=8 --verifyPartitions=true --planOnly=true

log "3. Full export to ${OUTPUT}/${EXPORT_ID}"
submit "${COMMON[@]}" --exportId="${EXPORT_ID}" \
  --collections=user-profiles,knows,hostile_keys \
  --partitions="${PARTITIONS:-16}" --rangesPerWrite="${RANGES_PER_WRITE:-4}" \
  --verifyPartitions=true

if [[ "${OUTPUT}" == file://* ]]; then
  cat "${OUTPUT#file://}/${EXPORT_ID}/manifest.json"
else
  gcloud storage cat "${OUTPUT%/}/${EXPORT_ID}/manifest.json"
fi

if [[ -n "${SPANNER_PROJECT:-}" ]]; then
  log "4. Import with the template and validate (SPANNER_* set)"
  : "${SPANNER_INSTANCE:?}" "${SPANNER_DATABASE:?}"
  OMNI_ARGS=()
  VALIDATE_ARGS=()
  if [[ -n "${SPANNER_OMNI_ENDPOINT:-}" ]]; then
    OMNI_ARGS=(--spannerOmniEndpoint="${SPANNER_OMNI_ENDPOINT}")
    VALIDATE_ARGS=(--spanner-endpoint "${SPANNER_OMNI_ENDPOINT#*://}")
  fi
  export_dir="${OUTPUT%/}/${EXPORT_ID}"
  log "4a. Create/check the Spanner schema from ${export_dir}/spanner-ddl-googlesql.sql"
  # Without the tables the template would fail (or, with 'tables', import nothing).
  python3 "${HERE}/apply_ddl.py" --export-dir "${export_dir}" --project "${SPANNER_PROJECT}" \
    --instance "${SPANNER_INSTANCE}" --database "${SPANNER_DATABASE}" "${VALIDATE_ARGS[@]}"
  log "4b. Template import"
  TEMPLATE_OUT="${TEMPLATE_OUT:-/tmp/arango-out/}"
  (cd "${W}" && mvn -B -q install -pl v2/sourcedb-to-spanner -am -DskipTests -Djib.skip -DskipShade \
    && mvn -B -pl v2/sourcedb-to-spanner exec:java -Dexec.classpathScope=test \
      -Dexec.mainClass=com.google.cloud.teleport.v2.templates.SourceDbToSpanner \
      -Dexec.args="--runner=DirectRunner --sourceDbDialect=GCS \
        --sourceConfigURL=${OUTPUT%/}/${EXPORT_ID}/manifest.json \
        --projectId=${SPANNER_PROJECT} --instanceId=${SPANNER_INSTANCE} \
        --databaseId=${SPANNER_DATABASE} ${OMNI_ARGS[*]:-} --outputDirectory=${TEMPLATE_OUT}")
  log "4c. Validate"
  python3 "${HERE}/validate.py" --export-dir "${export_dir}" --arango-url "${ARANGO_URL}" \
    --arango-password "${ARANGO_PASSWORD}" --project "${SPANNER_PROJECT}" \
    --instance "${SPANNER_INSTANCE}" --database "${SPANNER_DATABASE}" \
    --sample "${SAMPLE:-200}" --dlq-dir "${TEMPLATE_OUT%/}/dlq" "${VALIDATE_ARGS[@]}"
fi
log "done"
