# Migrating ArangoDB to Spanner (or Spanner Omni) through GCS

This guide covers moving an ArangoDB database into Cloud Spanner or a self-managed
Spanner Omni deployment. It runs in two independent stages:

1. **Export.** The ArangoDB Spark exporter
   ([`tools/arangodb-gcs-exporter`](../tools/arangodb-gcs-exporter)) runs on Spark,
   for example Dataproc. It reads every collection through the ArangoDB Spark connector
   and writes Avro files, two Spanner DDL files and an `arangodb-export.json` summary to
   GCS, or to any Hadoop filesystem. The Avro files are in exactly the format the template
   itself writes with `gcsOutputDirectory` (the "bulk migration Avro format").
2. **Import.** The `Sourcedb_to_Spanner_Flex` template runs on Dataflow, or on any Beam
   runner, with `sourceDbDialect=GCS` and `sourceConfigURL` set to the export directory.
   It reads every table directory under it and writes the rows to Spanner.

The template side doesn't depend on ArangoDB. Any directory in the bulk migration Avro
format can be imported with the `GCS` dialect, including the `gcsOutputDirectory` output
of an earlier `Sourcedb_to_Spanner_Flex` run (for example a MySQL migration), and the same
files can be validated with the `Avro_to_Spanner_Data_Validator` template (`v2/gcs-spanner-dv`).

> **ArangoDB version and licensing.** The exporter is developed and tested against
> ArangoDB **3.11** (3.11.14). Do **not** use ArangoDB 3.12+ binaries or images, including
> for testing: the 3.12 Community License limits a deployment to 100 GB of data. Every
> container in tests and docs uses `arangodb/arangodb:3.11.14`. Version 1.10.0 of the
> Spark connector works against 3.11 servers.

## 1. What gets exported

Each collection becomes one table with a fixed envelope. Column names are always
lowercase.

| Collection type | Columns (Avro string fields) |
|---|---|
| Document | `key`, `id`, `rev`, `doc` |
| Edge | `key`, `id`, `rev`, `from_id`, `to_id`, `doc` |

- `doc` holds the whole document, including system attributes. ArangoDB itself serialises
  it with `JSON_STRINGIFY(d)`, so values aren't reinterpreted by Spark or the connector.
- `key` is `_key` and is the primary key, for edges too.
- Table names are the sanitised collection names: lowercase, `[^a-z0-9_]` replaced with
  `_`, a `t_` prefix when the name doesn't start with a letter, and at most 128
  characters. The export fails if two names collide. The original name is kept in
  `arangodb-export.json` as `sourceName`.

The export is laid out as follows:

```
<output>/<exportId>/
  <table>/<md5Hex(schema)>-NNNNN-of-MMMMM.avro   bulk migration Avro format
  spanner-ddl-googlesql.sql
  spanner-ddl-postgresql.sql
  arangodb-export.json          summary (source info, per-table row counts and files),
                                written LAST; if it exists, the export is complete
```

Every record has the schema the template's own sink uses (`SourceRowWithMetadata`), here
for a document collection:

```
{tableName: string, shardId: ["string","null"], primaryKeys: array<string>,
 payload: record "payload" {key, id, rev: ["null","string"],
                            doc: ["null",{"type":"string","logicalType":"json"}]}}
```

with `tableName` = the table name, `shardId` = null and `primaryKeys` = `["key"]`. Edge
collections add `from_id` and `to_id` before `doc`. The exact schema texts are
`tools/arangodb-gcs-exporter/src/main/resources/avro/arangodb-{document,edge}.avsc`; a
unit test of the template checks that they equal the template's own sink schema.

The template treats every first-level directory of the export that contains `*.avro`
files (at any depth, so `<table>/<shardId>/*.avro` works too) as a table. Other files,
such as `arangodb-export.json` and the DDL files, are ignored. The payload schema and the
primary key of a table come from the header and first record of its files; files of one
table must all have the same payload schema. A table whose files are all empty is read
as an empty table.

## 2. Partitioning: RANGE (default) and HASH

The exporter splits each collection into partitions. Each partition is a separate AQL
query. The choice of partitioning matters a lot at multi-terabyte scale. The facts below
were verified on a live 3.11.14 cluster with a 16-shard collection.

**RANGE (`--partitioning=RANGE`, the default).**

- **Query.** Each partition runs
  `FOR d IN c FILTER d._key >= lo AND d._key < hi RETURN {...}`. This uses
  `IndexNode(primary)` on every shard with a parallel gather. The scanned index entries
  equal the rows returned, and `scannedFull = 0`. So the whole collection is read **once**,
  whatever the number of partitions.
- **Boundaries.** They come from a cheap index-only sample,
  `FOR d IN c FILTER RAND() < p RETURN d._key`. The sampled keys are sorted by UTF-8 bytes
  and n-1 quantiles are taken.
- **Ordering hazard.** AQL string comparison uses collation order: `SORT` gives
  `A, a, B, b, ...`. The primary-index range scan compares bytes at runtime, but the
  optimiser folds constant bounds using collation order. Because of this, both
  `['B','a')` and `['a','B')` returned **nothing** on a collection containing `a`, `B`,
  `Z` and `_x`. A range `[lo, hi)` is only reliable when `lo < hi` in **both** byte order
  and AQL collation order.
- **How the planner avoids it.** It ranks the boundaries with a single
  `FOR b IN @b SORT b RETURN b` query, then keeps the longest subsequence that increases
  in both orders. Dropped boundaries only merge neighbouring ranges, so correctness is
  unaffected. Then:
  - the first range is `d._key < b0`;
  - the last range is `d._key >= bLast`;
  - boundaries are written into the query as escaped AQL string literals, because the
    connector's query mode has no bind variables.
- **Partition count.** `--partitions=0` (auto) uses
  `ceil(count / targetDocsPerPartition)` (default 2,000,000), clamped to
  `[1, maxPartitions]` (default 20,000). A single partition runs one query without a
  filter.
- **Write batches.** Ranges are grouped into batches of `--rangesPerWrite` (default 64).
  Each batch is a separate Spark write to `tables/<table>/b<NNNNN>/`. Batches run
  concurrently, up to `--parallelWrites` (default 8). Set `spark.scheduler.mode=FAIR`.
- **Checking the plan.** `--verifyPartitions=true` checks that the per-range AQL counts
  add up to the collection count. `--planOnly=true` prints the plan without exporting.

**HASH (`--partitioning=HASH`, opt-in).** Each partition runs
`FILTER HASH(d._key) % N == i`. On 3.11 that is an `EnumerateCollection` **full scan for
every partition**, so N partitions cost N full scans. Forcing the primary index with
`OPTIONS {indexHint: 'primary', forceIndexHint: true}` raises an error. Use HASH only for
small collections. Auto mode caps it at 64 partitions.

**Correctness backstops (both modes).**

- The exporter counts the records of the final Avro files. The export fails unless that
  count equals the collection count taken before the export.
- It records each collection's count and revision before and after the export. If either
  changed, it fails (exit code 2), unless you pass `--allowLiveSource=true`. In that case
  it writes `consistent=false` to `arangodb-export.json`.
- The template counts the rows it reads for each table (counter
  `gcs_source_rows_read_<table>`); compare it with the table's `rowCount` in
  `arangodb-export.json`, or run the end-to-end validator (§6).

## 3. Running the exporter

Build the exporter. It's a standalone Maven project and isn't part of the template
reactor.

```bash
cd v2/sourcedb-to-spanner/tools/arangodb-gcs-exporter
# Scala 2.13 (default): Dataproc Serverless 2.2, local Spark 3.5.x for Scala 2.13
mvn -s ../../../../.mvn/settings.xml package
# Scala 2.12: Dataproc on Compute Engine 2.2 images
mvn -s ../../../../.mvn/settings.xml package -Pscala-2.12
gsutil cp target/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.13.jar gs://$BUCKET/jars/
```

Store the password in a file, for example on GCS with restricted IAM. The exporter
looks for it in this order: `--passwordFile`, then the environment variable named by
`--passwordEnv` (default `ARANGO_PASSWORD`), then `--password`, which logs a warning. For
a cluster whose root password is empty, use an empty file or an empty environment
variable.

**Dataproc Serverless (Spark 3.5, Scala 2.13):**

```bash
gcloud dataproc batches submit spark --region=$REGION --version=2.2 --subnet=$SUBNET \
  --jars=gs://$BUCKET/jars/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.13.jar \
  --class=com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  --properties=spark.scheduler.mode=FAIR \
  -- \
  --endpoints=10.30.1.10:8529,10.30.1.11:8529,10.30.1.12:8529 --database=shop \
  --passwordFile=gs://$BUCKET/secrets/arango.pw \
  --output=gs://$BUCKET/exports/shop --exportId=run1
```

**Dataproc on Compute Engine (2.2 image, Scala 2.12):**

```bash
gcloud dataproc jobs submit spark --cluster=$CLUSTER --region=$REGION \
  --jars=gs://$BUCKET/jars/arangodb-gcs-exporter-1.0.0-SNAPSHOT-scala2.12.jar \
  --class=com.google.cloud.teleport.v2.tools.arangodb.ArangoDbToGcsExporter \
  --properties=spark.scheduler.mode=FAIR \
  -- --endpoints=10.30.1.10:8529 --database=shop --passwordFile=gs://$BUCKET/secrets/arango.pw \
     --output=gs://$BUCKET/exports/shop
```

The Dataproc subnet must be able to reach the ArangoDB coordinators on port 8529.

Main options:

| Option | Default | Notes |
|---|---|---|
| `--endpoints` | required | `host:port[,host:port...]` (coordinators). |
| `--database` | `_system` | |
| `--user` | `root` | |
| `--output` | required | Root URI; the export goes to `<output>/<exportId>/`. |
| `--exportId` | UTC timestamp `yyyyMMdd'T'HHmmss'Z'` | Must match `^[A-Za-z0-9_-]+$`. The exporter refuses an existing export. |
| `--collections` / `--excludeCollections` | all non-system / none | Comma-separated. |
| `--partitioning` | `RANGE` | `RANGE` or `HASH` (§2). |
| `--partitions` | `0` (auto) | Partitions per collection. |
| `--targetDocsPerPartition` / `--maxPartitions` | `2000000` / `20000` | Auto sizing. |
| `--rangesPerWrite` / `--parallelWrites` | `64` / `8` | Write batching (§2). |
| `--parallelCollections` | `1` | Collections exported concurrently. |
| `--maxRecordsPerFile` | `2000000` | |
| `--compression` | `snappy` | `snappy`, `deflate` or `uncompressed`. |
| `--batchSize` / `--cursorTtlSeconds` | `1000` / `600` | Connector cursor settings. Each row carries the document plus its `JSON_STRINGIFY` copy, so a batch holds about `2 x batchSize x average document size` on the coordinator and on the Spark task. Aim for 32-64 MB per batch: keep 1000 for documents up to ~16 KB, lower it for bigger documents (for example 50 for 1 MB documents). |
| `--allowLiveSource` | `false` | See §2, correctness backstops. |
| `--verifyPartitions` / `--planOnly` | `false` / `false` | Plan checks. |
| `--verifyRangePlans` | `true` | Explains the first, a middle and the last RANGE query of every collection and fails unless each is a primary-index range scan with no residual `FilterNode` (which would re-check bounds in collation order and drop keys). |
| `--ddlEdgeHelpers` | `true` | Generated `from_*`/`to_*` columns and indexes for edges. |
| `--protocol`, `--ssl*`, `--acquireHostList` | | Connection settings. |
| `--connectorOption.<key>=<value>` | | Passed unchanged to the Spark connector. |

**Sizing for about 10 TB.**

- Expect 2.5 to 10 billion documents, in collections with up to 256 shards.
- RANGE with the auto defaults gives `ceil(count / 2,000,000)` partitions per collection:
  500 per billion documents, so about 1,250 to 5,000 for the whole 2.5 to 10 billion
  document dataset (capped at `maxPartitions=20000` per collection). That is still a
  single index scan of each collection.
- Scale Dataproc executors with `--parallelWrites` × `--rangesPerWrite` concurrent
  queries in mind, and make sure the coordinators can serve that many cursors.

## 4. Creating the Spanner schema

The exporter writes `spanner-ddl-googlesql.sql` and `spanner-ddl-postgresql.sql` into
the export directory:

- `doc` is `JSON` (or `jsonb`);
- `key` is `STRING(254)` and is the primary key;
- edges get stored generated columns `from_coll`, `from_key`, `to_coll` and `to_key`,
  plus indexes on `from_id` and `to_id`;
- there are no foreign keys, because ArangoDB allows dangling edges.

You can edit the files. Add typed attributes as **stored generated columns** over `doc`;
the template skips generated columns:

```sql
ALTER TABLE `user_profiles` ADD COLUMN `name` STRING(MAX) AS (JSON_VALUE(`doc`, '$.name')) STORED;
```

Create the database on Cloud Spanner:

```bash
gsutil cp gs://$BUCKET/exports/shop/run1/spanner-ddl-googlesql.sql .
gcloud spanner databases create shop --instance=$INSTANCE --ddl-file=spanner-ddl-googlesql.sql
```

Or on Spanner Omni:

```bash
docker cp spanner-ddl-googlesql.sql spanneromni:/tmp/ddl.sql
docker exec spanneromni /google/spanner/bin/spanner databases create shop \
  --ddl-file=/tmp/ddl.sql --timeout=5m
```

For large tables on Cloud Spanner, consider adding split points on `key` before the
import (see limit 5 in §7).

## 5. Running the template on Dataflow

The `GCS` dialect and the `spannerOmniEndpoint` / `spannerOmniPlainText` parameters are
not in the Google-hosted `gs://dataflow-templates-$REGION/latest/flex/Sourcedb_to_Spanner_Flex`
template until this change is released. Until then, build and stage the template from
this branch and use the staged spec:

```bash
mvn clean package -PtemplatesStage -DskipTests \
  -DprojectId=$PROJECT -DbucketName=$BUCKET -DstagePrefix=templates/arangodb \
  -DtemplateName=Sourcedb_to_Spanner_Flex -pl v2/sourcedb-to-spanner -am
TEMPLATE_SPEC=gs://$BUCKET/templates/arangodb/flex/Sourcedb_to_Spanner_Flex
```

Once the feature ships, `TEMPLATE_SPEC` can be the public
`gs://dataflow-templates-$REGION/latest/flex/Sourcedb_to_Spanner_Flex`.

```bash
gcloud dataflow flex-template run "arangodb-shop-$(date +%Y%m%d-%H%M%S)" \
  --project=$PROJECT --region=$REGION \
  --template-file-gcs-location=$TEMPLATE_SPEC \
  --parameters sourceDbDialect=GCS \
  --parameters sourceConfigURL=gs://$BUCKET/exports/shop/run1 \
  --parameters projectId=$PROJECT,instanceId=$INSTANCE,databaseId=shop \
  --parameters outputDirectory=gs://$BUCKET/migration/shop/
```

- `sourceConfigURL` is the root directory of the export; a trailing slash is optional.
- `tables` restricts the import to some of the tables. Names are matched against the
  table directories without regard to case.
- Every table of the export must have a matching Spanner table. A table that would not be
  migrated is logged and fails the launch, unless you restricted the import with
  `tables`, in which case it is only logged. Session files, schema-overrides files and
  table/column overrides still work for renames.
- Rows that Spanner rejects, such as oversized documents or invalid JSON, go to
  `<outputDirectory>/dlq/severe/` with `_metadata_source_type=mysql`. Replay them with the
  usual DLQ tooling (`datastreamSourceType=mysql`).
- `gcsOutputDirectory` is accepted but usually pointless here: it writes the rows back to
  GCS as Avro. The template logs a warning when it's set.
- The source has no JDBC settings, so `numPartitions`, `fetchSize`, `maxConnections`,
  `jdbcDriver*` and `uniformizationStageCountHint` are ignored. Parallelism comes from the
  number of Avro files and their size.
- The read timestamp of every row is the launch time of the template.
- The shard id of every row is the record's `shardId` (null for ArangoDB exports).

Useful metrics:

- `gcs_source_rows_read_<table>`;
- the usual Spanner write and DLQ counters.

## 6. Running the template against Spanner Omni

Set `spannerOmniEndpoint` to the Omni gRPC endpoint. The template then ignores
`projectId` and `instanceId` and uses `default` for both. `spannerOmniPlainText` defaults
to `true`, because Omni single-server deployments use plaintext gRPC without
authentication.

**Dataflow.** The Omni endpoint must be reachable from the Dataflow workers' VPC:

```bash
gcloud dataflow flex-template run "arangodb-shop-omni-$(date +%Y%m%d-%H%M%S)" \
  --project=$PROJECT --region=$REGION --subnetwork=$SUBNET \
  --template-file-gcs-location=$TEMPLATE_SPEC \
  --parameters sourceDbDialect=GCS \
  --parameters sourceConfigURL=gs://$BUCKET/exports/shop/run1 \
  --parameters projectId=$PROJECT,instanceId=unused,databaseId=shop \
  --parameters spannerOmniEndpoint=http://10.20.0.30:15000 \
  --parameters outputDirectory=gs://$BUCKET/migration/shop-omni/
```

**DirectRunner** (local or small tests):

```bash
mvn -pl v2/sourcedb-to-spanner exec:java -Dexec.classpathScope=test \
  -Dexec.mainClass=com.google.cloud.teleport.v2.templates.SourceDbToSpanner \
  -Dexec.args="--runner=DirectRunner --sourceDbDialect=GCS \
    --sourceConfigURL=/tmp/arango-export/e2e \
    --projectId=x --instanceId=x --databaseId=shop \
    --spannerOmniEndpoint=http://localhost:15000 --outputDirectory=/tmp/arango-out/"
```

**Flink or Spark.** Use the portable launcher from the Spanner Migration Omni project with
the same arguments.

To validate the result, run the exporter's end-to-end validator
(`tools/arangodb-gcs-exporter/e2e/validate.py`). It compares per-table counts and a
sample of `doc` values against ArangoDB:

```bash
python3 tools/arangodb-gcs-exporter/e2e/validate.py --export-dir <export dir> \
  --arango-url http://10.30.1.10:8529 --spanner-endpoint localhost:15000 \
  --project default --instance default --database shop --sample 200
```

## 7. Known limits

1. **No cross-collection snapshot.** The count and revision guard detects writes during
   the export, but it doesn't prevent them. Quiesce the source.
2. **Spanner JSON normalisation.** Spanner's JSON type can change:
   - integers above 2^53 or 2^63;
   - `-0.0`;
   - key order and whitespace.

   The validator reports these as warnings. If you need byte-exact text, change the DDL to
   `doc STRING(MAX)`; the template accepts that with no code change.

   **JSON numbers Spanner rejects.** Mutations parse JSON in Spanner's exact number mode, which
   rejects a double whose text "cannot round-trip through string representation":
   - ArangoDB's `JSON_STRINGIFY` sometimes prints doubles in a longer form than needed
     (`0.46245000000000003`, `-9223372036854775808.0`). The GCS source rewrites such numbers
     to their shortest round-trip form before writing (`JsonNumberCanonicalizer`), which is
     the same double, so nothing is lost.
   - A few doubles are rejected by Spanner in every textual form (in the 10 TB test, about
     0.01% of rows, for example `0.601049`). Those rows go to the severe DLQ. Replay them with
     `tools/arangodb-gcs-exporter/e2e/replay_dlq.py`, which upserts each DLQ row with
     `PARSE_JSON(@doc, wide_number_mode=>'round')` so Spanner rounds just those numbers (by at
     most one ulp, e.g. `0.601049` becomes `0.6010490000000001`).
   - **Throughput impact.** When a batch contains one such row, SpannerIO retries every row of
     that batch as a single-row commit. Collections with many doubles per document can then
     become commit-latency bound (in the 10 TB test `users`/`follows` dropped to ~135 k rows/s
     with the CPU of both Spanner and the workers idle). Use smaller batches and more harness
     threads for such tables, for example `--batchSizeForSpannerMutations=131072
     --numberOfWorkerHarnessThreads=48` on `n2-highmem-16` workers, which restored ~700 k rows/s.
3. **Size limits.** A JSON cell is limited to about 10 MiB and a commit to 100 MB.
   - Oversized documents go to the severe DLQ, and the exporter lists them in
     `stats.oversizedKeys`.
   - For large documents, lower `batchSizeForSpannerMutations`.
4. **HASH partitioning cost.** Each HASH partition is a full collection scan (§2). Use
   RANGE for anything that isn't small.
5. **Write hotspots.** ArangoDB's traditional `_key` generator is monotonic, which can
   create write hotspots on Cloud Spanner. Add split points or accept them. It doesn't
   matter for single-server Omni.
6. **Deletes aren't propagated on re-runs.** Re-runs are upserts, so documents deleted at
   the source stay in Spanner. Use a fresh database, or a fresh `exportId` plus cleanup.
7. **Unmatched collections fail the launch.** An export table with no matching Spanner
   table fails the job before it starts, unless `tables` is set (then it is only logged).
   Create the DDL first (§4).
