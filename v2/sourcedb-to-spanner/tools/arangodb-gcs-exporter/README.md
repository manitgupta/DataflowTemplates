# ArangoDB to GCS exporter (Spark)

A Spark 3.5 job that exports ArangoDB collections to Avro files plus a `manifest.json` on
any Hadoop-compatible file system (`gs://`, `hdfs://`, `file://`). The SourceDB to Spanner
template then imports the export with `sourceDbDialect=GCS` and
`sourceConfigURL=<output>/<exportId>/manifest.json`.

Every document is exported losslessly as a fixed envelope. Documents become
`key, id, rev, doc`; edges become `key, id, rev, from_id, to_id, doc`. Here `doc` is
`JSON_STRINGIFY(d)`: the full document, system attributes included, serialised by the
ArangoDB server. The exporter also writes suggested Spanner DDL for GoogleSQL and PostgreSQL.

This is a standalone Maven project. It has no parent and is not part of the repository's
`<modules>`.

Supported: ArangoDB 3.11 (tested target 3.11.14), single server or cluster.
ArangoDB Spark connector 1.10.0, Java driver 7.28.0, Spark 3.5.x.

## Build

```bash
cd v2/sourcedb-to-spanner/tools/arangodb-gcs-exporter
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 MAVEN_SKIP_RC=1
mvn -s ../../../../.mvn/settings.xml package -Pscala-2.12   # Dataproc on GCE 2.2, spark-3.5.x-bin-hadoop3
mvn -s ../../../../.mvn/settings.xml package                # Scala 2.13 (default profile): Dataproc Serverless 2.2
```

Each profile adds a classifier jar to `target/`, and the two jars can sit side by side:

| Jar | Spark runtime |
|---|---|
| `target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar` | Dataproc on Compute Engine 2.2 (Spark 3.5, Scala 2.12), Apache `spark-3.5.x-bin-hadoop3` |
| `target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.13.jar` | Dataproc Serverless runtime 2.2, Apache `spark-3.5.x-bin-hadoop3-scala2.13` |

The shaded jar bundles the ArangoDB connector, the shaded driver and `spark-avro`. It does
**not** bundle Spark, Scala, Hadoop, Avro, Jackson or SLF4J; the Spark runtime provides those.
The `META-INF/services` `DataSourceRegister` files of the connector and of spark-avro are
merged, so both `format("com.arangodb.spark")` and `format("avro")` resolve.

Unit tests run without Docker. The Testcontainers IT (`ArangoDbToGcsExporterIT`, image
`arangodb/arangodb:3.11.14`) needs Docker:
`mvn -s ../../../../.mvn/settings.xml verify -Pit`.

## Run

Dataproc on Compute Engine 2.2 (Scala 2.12):

```bash
gcloud storage cp target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar gs://$B/jars/
gcloud dataproc jobs submit spark --cluster=$CLUSTER --region=$REGION \
  --class=com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  --jars=gs://$B/jars/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar \
  --properties=spark.scheduler.mode=FAIR \
  -- --endpoints=10.30.1.10:8529,10.30.1.11:8529,10.30.1.12:8529 --database=shop \
     --passwordFile=gs://$B/secrets/arango.pw --output=gs://$B/exports/shop
```

Dataproc Serverless 2.2 (Scala 2.13):

```bash
gcloud dataproc batches submit spark --region=$REGION --version=2.2 --subnet=$SUBNET \
  --jars=gs://$B/jars/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.13.jar \
  --class=com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  --properties=spark.scheduler.mode=FAIR -- \
  --endpoints=$HOST:8529 --database=shop --passwordFile=gs://$B/secrets/arango.pw \
  --output=gs://$B/exports/shop
```

Local:

```bash
ARANGO_PASSWORD=... $SPARK_HOME/bin/spark-submit --master 'local[4]' \
  --class com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar \
  --endpoints=localhost:8529 --database=shop --output=file:///tmp/arango-export --exportId=e2e
```

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | Invalid options or input, for example unknown collections or an existing export. |
| 2 | Inconsistent export: the source changed, counts differ, or partition verification failed. No manifest is written. |
| 3 | Any other failure. |

### Options

| Option | Default | Meaning |
|---|---|---|
| `--endpoints` | required | Comma-separated `host:port` of coordinators or servers. |
| `--database` | `_system` | Database to export. |
| `--user` | `root` | |
| `--passwordFile` | | Hadoop URI (`gs://`, `file://`) of a file whose trimmed content is the password. Preferred. |
| `--passwordEnv` | `ARANGO_PASSWORD` | Environment variable read when `--passwordFile` is absent. |
| `--password` | | Literal password. Discouraged (logs WARN). If none of the three is set, the password is empty. |
| `--protocol` | `http2` | `http`, `http2` or `vst`. |
| `--sslEnabled`, `--sslCertValue` (base64 CA), `--sslTrustStorePath`, `--sslTrustStorePassword`, `--sslVerifyHost` | `false`, -, -, -, `true` | TLS settings. |
| `--acquireHostList` | `false` | Discover the other coordinators. |
| `--collections` / `--excludeCollections` | all non-system / none | Comma-separated collection names. |
| `--output` | required | Hadoop URI root. The export goes to `<output>/<exportId>/`. |
| `--exportId` | UTC `yyyyMMdd'T'HHmmss'Z'` | Must match `^[A-Za-z0-9_-]+$`. |
| `--partitioning` | `RANGE` | `RANGE` (primary-key ranges, one index pass) or `HASH` (`HASH(_key) % N`, one full scan **per partition**; only for small collections). |
| `--partitions` | `0` (auto) | Partitions per collection. Auto is `ceil(count / targetDocsPerPartition)`, clamped to `[1, maxPartitions]`. For HASH, auto is also capped at 64. |
| `--targetDocsPerPartition` | `2000000` | |
| `--maxPartitions` | `20000` | |
| `--rangesPerWrite` | `64` | Partition queries unioned into one Spark write (one `b<nnnnn>/` directory). |
| `--parallelWrites` | `8` | Concurrent Spark writes (shared by all collections). Use `spark.scheduler.mode=FAIR`. |
| `--parallelCollections` | `1` | Collections exported concurrently. |
| `--batchSize` | `1000` | Connector cursor batch size for the export queries (metadata queries use at least 10000). A batch holds about `2 x batchSize x average document size` on the coordinator and on the Spark task (document plus its `JSON_STRINGIFY` copy): aim for 32-64 MB, e.g. keep 1000 up to ~16 KB documents and use ~50 for 1 MB documents. |
| `--cursorTtlSeconds` | `600` | AQL cursor TTL. |
| `--maxRecordsPerFile` | `2000000` | |
| `--compression` | `snappy` | `snappy`, `deflate` or `uncompressed`. |
| `--ddlEdgeHelpers` | `true` | Generated `from_coll/from_key/to_coll/to_key` columns and `from_id`/`to_id` indexes in the GoogleSQL DDL. |
| `--allowLiveSource` | `false` | Write the manifest with `consistent=false` instead of failing when the source changed. |
| `--verifyPartitions` | `false` | Count every partition with an index-only `COLLECT WITH COUNT` that uses the same FILTER text, and require the sum to equal the collection count before exporting. |
| `--verifyRangePlans` | `true` | Explain the first, a middle and the last RANGE query of every collection and fail unless each is a primary-index range scan with no residual `FilterNode` or full scan (either would compare in AQL collation order and could drop or duplicate keys). |
| `--planOnly` | `false` | Plan (and verify) the partitions, then stop. Nothing is written. |
| `--connectorOption.<key>=<v>` | | Passed verbatim to the Spark connector, and overrides the options above. Example: `--connectorOption.timeout=900000`. |

## How it works

For each collection the exporter does the following:

1. Records the document count and the collection revision.
2. Plans partitions.
3. Writes batches of partition queries as independent Spark jobs to
   `tables/<table>/b<nnnnn>/`.
4. Reads the written files back, which is the ground truth for row counts.
5. Records the count and revision again.

If the count or revision changed, or if the number of exported rows differs from the count,
the export fails. The only exception is `--allowLiveSource=true`. `spanner-ddl-*.sql` is
written next, and `manifest.json` is written **last**: its presence marks a complete export.

### RANGE partitioning (default)

`FOR d IN c FILTER d._key >= "lo" AND d._key < "hi"` is served by the primary index on
every shard. All the ranges together read the collection once. HASH partitioning, by
contrast, costs one full scan per partition.

One hazard affects key ranges. At run time the primary index compares keys **by bytes**.
The AQL optimizer, however, folds constant bounds with **AQL collation**, in which `'B' < 'a'`
is false. A range whose bounds are ordered differently in the two orders comes back empty.
To avoid this, `KeyRangePlanner` does the following:

1. Samples about `20 * n` keys (at least 1,000, at most 2,000,000) with an index-only
   `FILTER RAND() < p`.
2. Sorts the sample by UTF-8 bytes and takes `n - 1` quantiles.
3. Keeps the longest subsequence that is also strictly increasing in AQL collation order
   (ranked by the server with `SORT`).
4. Re-checks consecutive bounds with the server's `<` operator.

Dropping a boundary only merges two ranges, so the ranges always stay disjoint and complete.
Bounds are inlined as JSON-escaped AQL string literals, because connector query mode has no
bind parameters. Run `--verifyPartitions=true --planOnly=true` to prove completeness on a
live server.

### Sizing a large export (multi-TB)

As an example, take 5 billion documents with the defaults. The exporter plans about 2,500
ranges of about 2M documents, in 40 write batches of 64 ranges each. With
`--parallelWrites=8`, up to 512 read tasks run at once, so give the cluster that many
executor cores, or lower `parallelWrites`. Each range ends up as roughly one Avro file.

Raise `--cursorTtlSeconds` if executors are slow to drain cursors. Quiesce the source during
the export, because the consistency guard fails on any write.

## Output format

```
<output>/<exportId>/
  manifest.json                    written last (formatVersion 1)
  spanner-ddl-googlesql.sql
  spanner-ddl-postgresql.sql
  tables/<table>/b<nnnnn>/part-<nnnnn>-<uuid>-c000.avro   (snappy-compressed Avro; _SUCCESS ignored)
```

Spark's Avro writer names files `...-c000.avro`, with no `.snappy` infix. The manifest lists
every data file explicitly with its row count. The template reads exactly those files.

The manifest fields, the Avro writer schemas (`ArangoDocumentRow`, `ArangoEdgeRow`) and the
DDL conventions are specified in section 3 of the design (`docs/arangodb-via-gcs.md` in this
repository). Table names are derived from collection names: lower case, `[^a-z0-9_]` becomes
`_`, a `t_` prefix is added if the name does not start with a letter, and names are limited
to 128 characters. A name collision fails the export.

The DDL files contain no comments. To expose typed attributes, add stored generated
columns, for example:

```sql
ALTER TABLE `user_profiles` ADD COLUMN `name` STRING(MAX) AS (JSON_VALUE(`doc`, '$.name')) STORED;
```

## End-to-end tests

- `e2e/seed.py` uses only the standard library. It seeds `user-profiles` (with hostile JSON
  fixtures), `knows` (with a dangling edge) and `hostile_keys` (every legal key character
  class) into a dedicated database. Defaults: `--arango-url http://10.30.1.10:8529`,
  `--database exporter_e2e`, empty password.
- `e2e/validate.py` (`pip install google-cloud-spanner`) checks the following:
  - the manifest and source counts;
  - that Spanner `COUNT(*)` plus severe DLQ records equals `rowCount`;
  - sampled documents (canonical JSON; Spanner number normalisation is reported as WARN);
  - the edge helper columns.

  A manifest table missing in Spanner is a FAIL (unless `--allow-missing-tables`), and at
  least one table must pass the count check. `--dlq-dir` counts only `severe` records.
- `e2e/apply_ddl.py` creates the Spanner/Omni database from the generated DDL, applies it to
  an empty existing database, or checks that the existing tables match it (fails otherwise).
- `e2e/run-cluster-e2e.sh` runs against a real cluster without Docker. It seeds, runs RANGE
  and HASH plan and verify on `hostile_keys`, runs a full export with `--verifyPartitions`,
  and optionally creates the schema (`apply_ddl.py`), imports with the template and
  validates. Spark runs locally (`SPARK_HOME`) or on Dataproc (`SPARK_MODE=dataproc`, which
  needs `OUTPUT=gs://...` or `GCS_STAGING`).
- `e2e/run-local-e2e.sh` runs the same flow with Docker: `arangodb/arangodb:3.11.14`, local
  Spark 3.5.3 and Spanner Omni.
