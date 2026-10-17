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
# Local end-to-end recipe with Docker: ArangoDB 3.11.14 + Spark 3.5.3 (local) + Spanner Omni,
# then the SourceDB to Spanner template on the DirectRunner, then validate.py.
# Prerequisites: Docker, JDK 17, Python 3 with `pip install google-cloud-spanner`.
# For a real cluster without Docker use run-cluster-e2e.sh instead.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXP="$(cd "${HERE}/.." && pwd)"
W="$(cd "${EXP}/../../../.." && pwd)"
WORK="${WORK:-/tmp/arango-e2e}"
SCALA_BIN="${SCALA_BIN:-2.12}"
SPARK_DIST="spark-3.5.3-bin-hadoop3"
[[ "${SCALA_BIN}" == "2.13" ]] && SPARK_DIST="spark-3.5.3-bin-hadoop3-scala2.13"
ARANGO_IMAGE="arangodb/arangodb:3.11.14"   # never 3.12+ (license limits)
OMNI_IMAGE="${OMNI_IMAGE:-us-docker.pkg.dev/spanner-omni/images/spanner-omni:2026.r4-lts}"
EXPORT_ID="${EXPORT_ID:-e2e}"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-amd64}"
export MAVEN_SKIP_RC=1
export ARANGO_PASSWORD=test
mkdir -p "${WORK}"

log() { echo "=== $*" >&2; }

log "1. ArangoDB ${ARANGO_IMAGE} + seed"
docker rm -f arango-e2e >/dev/null 2>&1 || true
docker run -d --name arango-e2e -p 8529:8529 -e ARANGO_ROOT_PASSWORD=test "${ARANGO_IMAGE}"
until curl -sf -u root:test http://localhost:8529/_api/version >/dev/null; do sleep 2; done
python3 "${HERE}/seed.py" --arango-url http://localhost:8529 --password test \
  --database shop --docs "${DOCS:-10000}"

log "2. Build exporter and run it on local Spark 3.5.3 (Scala ${SCALA_BIN})"
profile=()
[[ "${SCALA_BIN}" == "2.12" ]] && profile=(-Pscala-2.12)
mvn -B -q -f "${EXP}/pom.xml" -s "${W}/.mvn/settings.xml" package -DskipTests "${profile[@]}"
if [[ ! -d "${WORK}/${SPARK_DIST}" ]]; then
  curl -sL "https://archive.apache.org/dist/spark/spark-3.5.3/${SPARK_DIST}.tgz" \
    | tar xz -C "${WORK}"
fi
rm -rf "${WORK}/export/${EXPORT_ID}"
"${WORK}/${SPARK_DIST}/bin/spark-submit" --master 'local[4]' --conf spark.scheduler.mode=FAIR \
  --class com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  "${EXP}/target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala${SCALA_BIN}.jar" \
  --endpoints=localhost:8529 --database=shop --output="file://${WORK}/export" \
  --exportId="${EXPORT_ID}" --partitions=4 --rangesPerWrite=2 --verifyPartitions=true
cat "${WORK}/export/${EXPORT_ID}/manifest.json"

log "3. Spanner Omni + DDL"
docker rm -f spanneromni >/dev/null 2>&1 || true
docker volume create spanner >/dev/null
docker run -d --network host --name spanneromni --ulimit nofile=1048576:1048576 \
  -v spanner:/spanner "${OMNI_IMAGE}" start-single-server --listen-addresses=0.0.0.0
sleep 20
docker cp "${WORK}/export/${EXPORT_ID}/spanner-ddl-googlesql.sql" spanneromni:/tmp/ddl.sql
docker exec spanneromni /google/spanner/bin/spanner databases create shop \
  --ddl-file=/tmp/ddl.sql --timeout=5m

log "4. Template on the DirectRunner against Omni"
rm -rf "${WORK}/out"
(cd "${W}" && mvn -B -q install -pl v2/sourcedb-to-spanner -am -DskipTests -Djib.skip -DskipShade \
  && mvn -B -pl v2/sourcedb-to-spanner exec:java -Dexec.classpathScope=test \
    -Dexec.mainClass=com.google.cloud.teleport.v2.templates.SourceDbToSpanner \
    -Dexec.args="--runner=DirectRunner --sourceDbDialect=GCS \
      --sourceConfigURL=${WORK}/export/${EXPORT_ID}/manifest.json \
      --projectId=default --instanceId=default --databaseId=shop \
      --spannerOmniEndpoint=http://localhost:15000 --outputDirectory=${WORK}/out/")

log "5. Validate"
python3 "${HERE}/validate.py" --export-dir "${WORK}/export/${EXPORT_ID}" \
  --arango-url http://localhost:8529 --arango-password test \
  --spanner-endpoint localhost:15000 --project default --instance default \
  --database shop --sample 200 --dlq-dir "${WORK}/out/dlq"
