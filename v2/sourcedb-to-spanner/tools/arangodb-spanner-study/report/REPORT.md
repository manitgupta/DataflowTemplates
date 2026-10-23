# ArangoDB → GCS → Spanner: 10 TB migration report

Date: 2026-10-17/18. Project `terraform-demo-host`; every resource is prefixed `arango-`.
Code: DataflowTemplates worktree branch `worktree-arangodb-gcs-spanner`.

## 1. What was built

| Piece | Where | What |
|---|---|---|
| ArangoDB → GCS exporter | `v2/sourcedb-to-spanner/tools/arangodb-gcs-exporter` (standalone Maven, Spark 3.5, Scala 2.12/2.13 jars) | Uses the **ArangoDB Spark connector 1.10.0** in query mode. Writes one Avro "envelope" row per document (`key, id, rev, [from_id, to_id], doc = JSON_STRINGIFY(d)`), a `manifest.json` (explicit file list + counts) and Spanner DDL (GoogleSQL + PG). |
| GCS source in sourcedb-to-spanner | `v2/sourcedb-to-spanner/src/main/java/.../source/gcs/**` + `spanner-common` `GcsSourceConfig` | New dialect `sourceDbDialect=GCS`, `sourceConfigURL=<manifest.json>`. Reads Avro with `FileIO`/`AvroIO`, maps manifest types to unified types, per-table row-count check against the manifest, normal Spanner write + DLQ. |
| Spanner Omni options | `SourceDbToSpannerOptions` | `spannerOmniEndpoint`, `spannerOmniPlainText` (same names as the Omni POC). |
| JSON number canonicalizer | `source/gcs/mappings/JsonNumberCanonicalizer` | Rewrites over-long double texts (`0.46245000000000003` → `0.46245`) that Spanner's exact JSON mode rejects; lossless. |
| DLQ replay | `tools/arangodb-gcs-exporter/e2e/replay_dlq.py` | Upserts severe-DLQ rows with `PARSE_JSON(@doc, wide_number_mode=>'round')`. |

Tests: sourcedb-to-spanner 804 unit tests, spanner-common 500, exporter 75 — all pass; spotless/checkstyle clean.

## 2. Key design decisions (and why)

1. **ArangoDB 3.11.14 (Apache-2.0).** 3.12+ Community binaries cap datasets at 100 GB (incl. testing, aggregated across
   servers) and claim to govern prior versions once installed, so no 3.12 binary was used.
2. **RANGE partitioning on `_key` (default).** Connector *table* mode reads one partition per shard but can only project
   named attributes (no whole document); *query* mode returns `JSON_STRINGIFY(d)` but is a single partition. The exporter
   issues many query-mode reads, each `FILTER d._key >= lo AND d._key < hi` → primary-index range scan on every shard
   (verified `scannedFull=0`). `HASH(_key) % N` is a full scan per partition, so it is opt-in only.
3. **Collation hazard handled.** AQL compares strings by collation, but the primary-index range scan compares bytes, and
   the optimizer folds constant bounds with collation (both `['B','a')` and `['a','B')` returned empty). Boundaries are
   sorted bytewise and filtered with an LIS over AQL collation ranks so every range is increasing in both orders; on a
   hostile-key collection (every legal key char class) 27/63 boundaries were dropped and per-range counts summed exactly.
4. **Rotate one coordinator per query.** The connector's driver uses the first endpoint for a query-mode partition; with
   the full list every query hit `arango-db-00`. Pinning one coordinator per partition raised export throughput
   ~0.6 M → ~3.5 M docs/s.
5. **Whole document as JSON + envelope columns.** Lossless for schemaless documents; typed attributes can be added as
   Spanner generated columns over `doc`. Edge tables get `from_coll/from_key/to_coll/to_key` generated columns.
6. **Target region.** us-central1's Spanner node quota (100/region) was fully used by another project instance, so the
   10 TB target is `arango-spanner-e1` (100 nodes, regional-us-east1) with Dataflow in us-east1 (user-approved).

## 3. Environment

| Component | Value |
|---|---|
| ArangoDB | 24 × `z3-highmem-16-highlssd` (`arango-db-00..23`), 3.11.14 starter cluster (3 agents, 24 coordinators, 24 DB servers), RAID0 local NVMe XFS, RF=1, 256 shards/collection |
| Generator / exporter | Dataproc 2.2 `arango-dataproc` (32 × n2-standard-16, 64 executors × 8 cores) |
| Import | Dataflow, us-east1, 40 × n2-standard-16, sourcedb-to-spanner GCS dialect from the bundled jar |
| Target | Cloud Spanner `arango-spanner-e1` (100 nodes, regional-us-east1), database `shop10t`, 200 pre-split points |
| Small-scale Omni test | Spanner Omni 2026.r4-lts single server `arango-omni` |

## 4. Dataset (`shop10t`)

| Collection | Kind | Docs | avg JSON | logical JSON |
|---|---|---:|---:|---:|
| users | document | 675,000,000 | 1,850 B | 1.249 TB |
| products | document | 168,750,000 | 5,771 B | 0.974 TB |
| orders | document | 843,750,000 | 2,604 B | 2.197 TB |
| events | document | 3,149,550,000 | 1,341 B | 4.223 TB |
| follows | edge | 2,700,000,000 | 503 B | 1.359 TB |
| purchased | edge | 2,700,000,000 | 473 B | 1.278 TB |
| type_zoo | document (mixed/hostile types) | 13,500,000 | 1,019 B | 0.014 TB |
| hostile_keys | document | 33 | 96 B | 0 |
| **total** | | **10,250,550,033** | | **11.293 TB** |

RocksDB `documentsSize` after compaction: 5.79 TB (LZ4). Generated with Spark through the ArangoDB connector at
~1.9–2.2 M docs/s (~3.5 GB/s) in ~1.5 h; exact counts == plan for every collection.

## 5. Results

### 5.1 Small-scale end-to-end (205 k docs incl. 2 MB doc, hostile JSON, dangling edge, hostile keys)
- Cloud Spanner: validate.py 9/9 PASS (counts exact, DLQ empty, 1,500 sampled documents identical as canonical JSON).
- Spanner Omni (via `--spannerOmniEndpoint`): 9/9 PASS.

### 5.2 10 TB export (ArangoDB → GCS)
- Export `full2`: **10,250,550,033 rows in 51 min**, 7,404 Avro files, **4.25 TB** (snappy) at
  `gs://arango-terraform-demo-host/exports/shop10t/full2/` (manifest `consistent=true`).
- Per collection: partition verification (index-only per-range COUNT sums == collection count) and exported rows ==
  source count before == after.

### 5.3 10 TB import (GCS → Spanner)
**Final state: every table in `arango-spanner-e1/shop10t` has exactly the ArangoDB count — 10,250,550,033 rows**
(`results/shop10t-spanner-counts-final.txt`):

| Table | ArangoDB | Spanner |
|---|---:|---:|
| users | 675,000,000 | 675,000,000 |
| products | 168,750,000 | 168,750,000 |
| orders | 843,750,000 | 843,750,000 |
| events | 3,149,550,000 | 3,149,550,000 |
| follows | 2,700,000,000 | 2,700,000,000 |
| purchased | 2,700,000,000 | 2,700,000,000 |
| type_zoo | 13,500,000 | 13,500,000 |
| hostile_keys | 33 | 33 |
| **total** | **10,250,550,033** | **10,250,550,033** |

Content validation (`validate.py --sample 1000`, `results/shop10t-validate.txt`): **24 PASS, 0 FAIL** — for all 8
tables: manifest rowCount == ArangoDB count, Spanner count == rowCount, and 1,000 random documents per table equal the
live ArangoDB document as canonical JSON. The 222 WARNs are all `type_zoo.big = -9223372036854775808.0`, which
ArangoDB stores as a *double* and Spanner prints as `-9.223372036854776e+18` — the same IEEE double (the validator
compares number texts as decimals and reports this as Spanner normalisation, not as a mismatch).

How it got there (all Dataflow in us-east1, sourcedb-to-spanner `sourceDbDialect=GCS`, 100-node target):

| Step | Time (UTC) | Outcome |
|---|---|---|
| Run #1 (40 × n2-standard-16) | 04:35–04:47 | Cancelled: Spanner rejected ArangoDB's over-long double texts (`0.46245000000000003`) → added `JsonNumberCanonicalizer`. |
| Run #2 (40 × n2-standard-16, all tables) | 05:04–06:46 | ~2 M rows/s for the first ~50 min (3.56 B rows in 30 min); **events, purchased, orders, products loaded exactly** (6.86 B rows). Then `users`/`follows` crawled (~135 k rows/s) with Spanner and workers idle → cancelled. |
| Run #3 (`--tables=users,follows,type_zoo`) | 06:44–06:59 | type_zoo exact (canonicalizer fix for `5e-324`); diagnosed the crawl: ~5 % of batches contain one row with a double Spanner rejects in every textual form, and SpannerIO then retries the whole batch row by row. |
| Run #4 (60 × n2-highmem-16, `--batchSizeForSpannerMutations=131072 --numberOfWorkerHarnessThreads=48`, users+follows) | 07:07–08:33 | ~600–700 k rows/s, Spanner CPU ~60 %; 4,582,486 rows (0.045 %) to the severe DLQ. |
| DLQ replay (`replay_dlq.py`, re-read by key from ArangoDB, `PARSE_JSON(..., wide_number_mode=>'round')`) | 08:44–08:50 | 4,987,757 DLQ rows upserted in 6 min, 0 missing in source → all counts exact. |

Pure pipeline time for the data that finally landed: run #2 (102 min) + run #4 (86 min) + replay (6 min) ≈ 3 h 14 min;
with the right settings from the start a single run is estimated at ~1.5–2 h (the bulk ran at ~2 M rows/s).
Spanner storage after load: 23.0 TB (includes versions from re-written rows; shrinks after version GC/compaction).

## 6. Current state (2026-10-18 ~09:00 UTC)

- **Kept:** `arango-spanner-e1` (scaled 100 → 4 nodes; database `shop10t` with all 10.25 B rows, 23 TB),
  bucket `gs://arango-terraform-demo-host` (export `full2`, 4.25 TB, jars, DLQs), network `arango-vpc`
  (+ us-east1 subnet/router/NAT).
- **Deleted:** ArangoDB fleet `arango-db-00..23`, `arango-ops`, `arango-omni` (+ disk), `arango-dataproc`,
  e2e instance `arango-spanner`. The source dataset is deterministic (`data/gen_spark.py --scale 1.35` +
  `data/gen_arango.py --only type_zoo`), and the full export is still in GCS.
- Remove the rest: `infra/99-cleanup.sh spanner` (target instance) / `infra/99-cleanup.sh all` (everything).
- Code commits (local branch `worktree-arangodb-gcs-spanner`, not pushed — the only remote is the public upstream):
  `664df2817` feature, `b661280db` exporter endpoint rotation, `ab82ebbb6` JSON canonicalization + replay tool,
  `54f07879f` docs, `f5413aef2` full STRING/JSON values in mutation DLQ records.

## 7. Findings / issues hit

1. Spark connector query mode + default driver load balancing → single-coordinator bottleneck (fixed in exporter).
2. AQL collation vs. primary-index byte order for `_key` ranges (handled by planner + verification).
3. Spanner exact JSON number mode rejects (a) non-shortest double texts that ArangoDB emits — canonicalized losslessly
   in the template — and (b) a small set of doubles in every textual form (e.g. `0.601049`) — replayed from the DLQ
   with `wide_number_mode=>'round'` (≤ 1 ulp change, Spanner's documented behaviour).
4. One bad row per batch makes SpannerIO (`FailureMode.REPORT_FAILURES`) retry the whole batch row by row; with
   ~0.01 % bad rows this throttled `users`/`follows` to ~135 k rows/s. Smaller batches + more harness threads fixed it.
5. The template's mutation DLQ stored `Value.toString()` (abbreviated) for STRING/JSON → not replayable; fixed
   (`f5413aef2`), and the replay tool can re-read documents from ArangoDB by key.
6. Environment: Spanner regional node quota, SSD quota (pd-balanced counted), Dataproc secondary-worker disk type,
   auto-mode permission gates for new-region / large resources.

## 8. Follow-up (2026-10-19): export format aligned with the bulk template's GCS sink; one shared GCS reader

**Validated:** the original export used a custom envelope + `manifest.json`, *not* the format sourcedb-to-spanner's
`gcsOutputDirectory` sink writes. Now (commits `451394250`, `ef98c4065`):

- **Exporter** writes exactly the bulk sink format: `<root>/<table>/<md5(schemaJson)>-NNNNN-of-MMMMM.avro`, records
  `SourceRowWithMetadata{tableName, shardId=null, primaryKeys=["key"], payload{key,id,rev,[from_id,to_id],doc(json)}}`.
  The writer schema text is generated by the template's own `SourceRow.gcsSchema()`; a template test pins the
  exporter's `.avsc` files byte-for-byte to it. On a real GCS export every file's embedded `avro.schema` was
  byte-identical to the golden schema and the file-name prefix equals its md5 (as `AvroFileNaming` computes).
  `arangodb-export.json` (counts/stats) replaces `manifest.json` and is not needed by any reader.
- **Shared reader:** gcs-spanner-dv's reader (glob patterns + `AvroIO.parseFilesGenericRecords` + `GenericRecordCoder`)
  moved to `spanner-common` (`BulkGcsFormat`, `BulkGcsAvroReader`) and is used by **both** gcs-spanner-dv and the
  sourcedb-to-spanner `GCS` source, which now reads any bulk GCS export (tables from directories, payload schema from
  the Avro header, keys from `primaryKeys`). A round-trip test writes rows with the template's own `writeToGCS` sink and
  reads them back through the GCS source → identical payloads.
- **gcs-spanner-dv fix:** JSON values are hashed in canonical form (sorted keys, numbers by value); before, Spanner's JSON
  normalization made every JSON row a false mismatch (first DV run: 0 of 205,012 rows matched).
- Finding: Spanner round-mode storage of the "unstorable" doubles kept the exact value (`0.6010490000000001` is the same
  IEEE double as `0.601049`), confirmed by gcs-spanner-dv matching the DLQ-replayed rows.

**Re-run tests (all PASS):**

| Test | Result |
|---|---|
| Unit: spanner-common / gcs-spanner-dv / sourcedb-to-spanner / exporter (2.13, 2.12) | 518 / 148 / 794 / 78 / 78 — 0 failures; spotless + checkstyle clean |
| Hostile e2e (205,012 docs, 2 MB doc, hostile keys/JSON, dangling edge) → `arango-spanner-e1/e2ebulk` | validate.py 12/12 PASS; **gcs-spanner-dv MATCH 205,012/205,012** |
| Scaled e2e `shop1pct` (75,930,033 docs, same generator at scale 0.01) → `arango-spanner-e1/shop1pct` | counts exact for all 8 tables (36,744 DLQ rows replayed straight from the DLQ, which now keeps full values); validate.py 32/32 PASS; **gcs-spanner-dv MATCH 75,930,033/75,930,033** |

The 10 TB data loaded on 2026-10-18 (§5) was produced from the earlier export format; it was not re-exported in the
new format (source fleet deleted). Re-running at 10 TB would use the same code paths validated above.

State after follow-up: test compute deleted again; `arango-spanner-e1` at 4 nodes (databases shop10t, shop1pct,
e2ebulk); BigQuery dataset `arango_dv` (DV results); bucket and network kept.

## 9. Same test on Spanner Omni, side by side with Cloud Spanner (2026-10-19)

Same exports (bulk format), same sourcedb-to-spanner jar (v4), same Dataflow settings; only the target differs.

| | Cloud Spanner | Spanner Omni |
|---|---|---|
| Target | 20 nodes, regional-us-east1 | single server 2026.r4-lts on n2-standard-32 + 2 TB pd-ssd (us-central1) |
| Import 1 % run (75,930,033 docs) | 38.5 min (~32.9 k rows/s) | 35.3 min (~35.8 k rows/s) |
| Import hostile set (205,012 docs) | 3.1 min | 4.9 min |
| DLQ rows (JSON doubles Spanner rejects) | 36,744 → replayed in 14 s | the same 36,744 rows → replayed in 7 s (PARSE_JSON round works on Omni) |
| Final counts | exact on all 8 tables (75,930,033) | exact on all 8 tables (75,930,033) |
| gcs-spanner-dv (every row) | MATCH 75,930,033 / 0 (5.3 min) · hostile MATCH 205,012 | MATCH 75,930,033 / 0 (12.7 min) · hostile MATCH 205,012 |
| Target CPU during import | avg 12 %, peak 29 % | ~42 % |

Code added for this: gcs-spanner-dv `--spannerOmniEndpoint/--spannerOmniPlainText` (`fbb421d0e`) and streaming reads
when targeting Omni (`c82508393`); replay_dlq.py `--omni`.

Omni findings: (1) partitioned (batch) reads of large tables stalled on the single server, so DV reads each table with
one streaming query against Omni; (2) abandoned long scans (cancelled jobs / killed COUNTs) left the server busy and
made `events`/`orders` scans hang until a clean `docker restart -t 300`; (3) both runs were pipeline-bound at this size
(neither database saturated); Cloud Spanner's Dataflow job read the export cross-region. The 10 TB run was not repeated
on Omni (single server; multi-server deployment not attempted in this pass).

## 10. Full 10 TB run in the final format: Cloud Spanner and multi-server Spanner Omni side by side (2026-10-20)

Fresh source (`gen_spark.py --scale 1.35` on 24 ArangoDB nodes, ~90 min), one export in the bulk-template format,
then **both imports at the same time from the same export** with the same jar (`sourcedb-to-spanner-gcs-v4`) and the
same Dataflow settings (60 × n2-highmem-16, `--batchSizeForSpannerMutations=131072 --numberOfWorkerHarnessThreads=48`).

| | Cloud Spanner | Spanner Omni |
|---|---|---|
| Target | 100 nodes, regional-us-east1, 200 split points | 2026.r4-lts multi-server, single zone (3 root + 23 join servers, 740 vCPU, ~76 TB disk), 200 split points, internal TCP LB |
| Export (shared) | 59 min 14 s, 10,250,550,033 docs (2.89 M docs/s), 7,477 Avro files, 4.27 TB, consistent (counts + revisions equal before/after) | ← same files |
| Import wall-clock | **141.3 min** (1.21 M rows/s avg) | **211.8 min** (0.81 M rows/s avg; first ~45 min on 9 servers) |
| Peak import rate | ~3.0 M rows/s (edge tables) | ~2.2 M rows/s (after scale-out to 26 servers) |
| Dataflow vCPU-hours | 2,233 | 3,358 |
| Rows written / to DLQ | 10,245,562,276 / 4,987,757 | 10,245,562,276 / 4,987,757 (identical) |
| DLQ replay (PARSE_JSON round) | 4.5 min (64 procs, 200-row txns) | 46 min (48 procs, 1-row txns — see finding 2) |
| Final row counts | exact on all 8 tables (10,250,550,033) | exact on all 8 tables (10,250,550,033; DV destination counts) |
| gcs-spanner-dv, every row | **MATCH 10,250,550,033 / 0** (26.1 min, 60 × n2-highmem-16) | **MATCH 10,250,550,033 / 0** (31.1 min, 60 × n2-highmem-16, partitioned reads; after the fix in finding 5) |
| Target CPU during import | avg 52 %, p95 97 % (85 % avg in the bulk phase) | servers avg 67 %, p95 93 % (81–82 % in the bulk phases) |
| Storage after load | 36.2 TB (Spanner metric) | 13.7 TB physical across servers |

Omni cluster growth during the run (all quota-driven): started with 9 × c3-standard-22 + 3 TB Hyperdisk Balanced
(CPU-bound at ~82 k rows/s, servers 88–98 % busy). C3 vCPU (300), Hyperdisk Balanced (40,960 GB) and SSD (40,960 GB)
regional quotas were exhausted, so 17 join servers were added on local-SSD machines (8 × n2d-standard-32, 4 ×
c2d-standard-32, 4 × n2-standard-32, 8 × 375 GB NVMe RAID0 each; plus 1 × c3d-standard-30 Hyperdisk) in
us-central1-a/b/c — all in one logical Omni zone. Omni rebalanced splits onto the new servers within minutes and
throughput rose ~10× (to 1.5–2.2 M rows/s). Scripts: `omni/01-omni-cluster.sh` (`START`, `IP_PREFIX`, `DISK_TYPE=local-ssd`),
`omni/05-omni-lb-add.sh` (one instance group per zone behind the LB).

Findings:

1. Both imports end with only `follows` left and slow down (~0.5–0.8 M rows/s): `follows.weight` doubles are the bulk of
   the JSON rows Spanner's exact mode rejects (4.14 M of 4.99 M DLQ rows), and each reject makes SpannerIO retry its
   batch row by row. Identical DLQ rows on both targets — shared Spanner behaviour, not an Omni difference.
2. **DLQ replay on multi-server Omni needs single-row transactions.** A 200-row (even 20-row) batch DML transaction of
   `users` rows — keys scattered over the whole table — was aborted on every retry (`409 Transaction was aborted`), while
   the same batch of key-clustered `follows` rows committed in 0.5 s and single-row transactions always committed.
   (Single-server Omni and Cloud Spanner commit the 200-row batches.) `replay_dlq.py --batch 1` replayed all 4,987,757
   rows; the tool now re-raises worker errors readably and retries aborts for up to 30 min.
3. The Python client (3.71) must use multiplexed sessions with Omni (`Only Multiplexed sessions are supported`).
4. gcs-spanner-dv against Omni: new options `--spannerOmniPartitionedReads=true` (commit `0cc84abea`; one streaming
   query per table — the single-server workaround — would take hours for the 3.15 B-row `events` table) and
   `--spannerOmniReadTimeoutMinutes` (commit `fdebc624a`; a hung partition stream fails and is retried).
5. **Stuck tablets after the mid-run scale-out.** The first two Omni DV runs read 10.0 B of 10.25 B rows and then ~9
   partition streams (in events, orders, follows, products, users) never returned. Plain queries on those key ranges hung
   too (e.g. `COUNT(*) FROM products WHERE key >= 'p000151875000'`), while other ranges answered in seconds. Three servers
   (`arango-omni-02/04/20`) had logged `INTERNAL: RET_CHECK failure (Omni server) tablet_->state() >
   TabletState::PREUNLOAD (8 vs. 9)` every ~40 s since 05:16 / 06:39 / 07:46 — tablets stuck half-unloaded during the
   rebalancing onto the new servers (those servers also held 0.8–1.4 TB vs ~0.45 TB elsewhere). A graceful rolling
   `docker restart -t 300` of just those three servers (`omni/06-omni-restart.sh`, ~1.5 min each, cluster stayed
   READY) cleared it: the range returned 16,875,000 rows in 27 s and `products` counted exactly. Writes were not
   affected (the import and the replay completed with identical counts).
6. Dataflow workers auto-assign IPs from the same subnet as the Omni servers; new servers need addresses outside the
   range in use (`IP_PREFIX=10.30.8`).

State after the 10 TB run (2026-10-20 ~12:45 UTC): ArangoDB fleet, `arango-ops`, the 26-server Omni deployment
(+ LB, instance groups) and Dataproc deleted (`infra/99-cleanup.sh compute`); `arango-spanner-e1` scaled 100 → 4 nodes
(databases `shop10tb` — this run, 10.25 B rows — plus `shop10t`, `shop1pct`, `e2ebulk`); export
`gs://arango-terraform-demo-host/exports/shop10t/bulk10t/` (4.27 TB) and DLQs kept; DV results in BigQuery
`arango_dv` (run ids `arango_dv_shop10tb_1020071915`, `arango_dv_shop10t_1020115006`). New code commits on the
worktree branch: `0cc84abea`, `fdebc624a` (gcs-spanner-dv Omni read options). Deck updated (slides 2, 8, 10, 12–15, 17–19).

## 11. Spanner Omni target on Apache Flink vs Dataflow (2026-10-21/22)

The customer runs Spanner Omni, so the 10 TB import was redone on **Apache Flink** with the Spanner Migration Omni
POC's optimized pipeline (recipe and code from the `spanner-migration-beam-runners` session:
`~/spanner-migration-omni/beam/pipeline`, Flink profile from `beam/docs/optimization.md`), and compared with
**Dataflow running the same jar** against the **same Spanner Omni cluster**.

Setup (all `arango-*`):

- **Spanner Omni:** 12 × z3-highmem-44-highlssd (528 vCPU, ~216 TB local NVMe), 2026.r4-lts, single zone, internal TCP
  LB `10.30.2.100:15000` (Z3 regional quota capped it at 12; servers `arango-omni-00..11`, 10.30.9.10–21).
  - One empty database per run: `arango_flink`, `arango_df`.
  - 640 proportional split points each; `arango_df` also has 24 for type_zoo.
- **Flink:** GKE `arango-flink` (arango-vpc, private nodes) running Flink Kubernetes Operator 1.16.1 (application mode).
  - 32 TaskManagers (n2-standard-16) × 16 slots = parallelism 512, plus a JobManager pool.
  - Image `beam-flink:1.20-arango1`.
  - `infra/06-gke-flink.sh`, `scripts/run-flink-gcs.sh`, `flink/` (copy of the POC wrapper + launcher).
- **Dataflow:** 32 × n2-standard-16 (`scripts/run-dataflow-portable.sh`), same jar (`omni-migration-dataflow-arango1.jar`).
- **Jar:** the POC wrapper (OmniSpannerWrite, compact coders, portable launcher) built against this branch's template
  modules from a private Maven repo (`~/.m2-arango`). Changes in the wrapper copy:
  - Ported the full-value DLQ fix into its `DeadLetterQueue` copy.
  - Removed duplicate Omni options (now inherited from the template).
  - New in OmniSpannerWrite: data errors (OUT_OF_RANGE/INVALID_ARGUMENT/FAILED_PRECONDITION) **bisect** the batch instead
    of writing it row by row with retries.
- **Writer args (both runners):** `--writerMode=COMMIT --writerBatchCells=79000 --writerBatchBytes=16777216
  --writerBatchRows=8000 --maxCommitDelay=100`, with `--writerConcurrency=8` on Flink and **2** on Dataflow (see findings).

| | Apache Flink (GKE) | Dataflow |
|---|---|---|
| Import 10.25 B docs | **139.5 min** (8,367 s), 1.23 M rows/s avg, 1.68 M best 5 min | **164 min**, 1.04 M rows/s avg, 3.7 M best 5 min |
| Rows written / to DLQ | 10,245,560,846 / 4,989,187 | 10,245,561,771 / 4,988,262 |
| DLQ replay (1-row txns, 48 procs) | 48 min | 45.5 min |
| gcs-spanner-dv, every row | **MATCH 10,250,550,033 / 0** (31.6 min) | MATCH 10,250,550,033 / 0 (27 min) |
| Omni CPU during import | avg 73 %, p95 92 % | avg 55 %, p95 82 % |
| Omni server crashes during import | **0** | 7 |

Findings:

1. **Flink memory:** the POC's MySQL profile (managed 0.4, network 0.25/12 g) left ~5 GB heap per 16-slot TaskManager
   and OOMed on the larger JSON rows. Managed 0.2 + network 4 g fixed it.
2. **GKE pods → Omni:** pod IPs (10.40.0.0/14) need a firewall rule to the Omni ports (`arango-allow-gke-pods`).
3. **Omni 2026.r4-lts large-value crash** (a server CHECK failure: `CANCELLED: Fiber cancelled before
   read started; LargeValue{...}`), the same bug the POC hit at 50 TB:
   - **Never** during the Flink import, its replay or its validation.
   - **Repeatedly** during every Dataflow import with the same writer settings: 2 attempts at concurrency 8 crashed 1–3
     servers each and wedged the cluster (rolling restarts needed).
   - Mechanism: Dataflow batch puts a shuffle barrier before the writer, so it reads first and then bursts commits.
     Commits exceeded the 120 s client deadline and were cancelled while reading large JSON values. Flink's pipelined
     shuffles pace writes behind reads.
   - With `--writerConcurrency=2` Dataflow completed (7 crashes during the run).
4. **Stuck tablets:** `RET_CHECK ... TabletState::PREUNLOAD` reappeared on two servers; a rolling restart before the
   Dataflow run cleared it (`omni/06-omni-restart.sh`).
5. **DLQ replay:** the replay tool now retries per-RPC DEADLINE_EXCEEDED/UNAVAILABLE with backoff (it raised out of
   `run_in_transaction` and killed the pool once).
6. **Split script bug:** `omni/04-omni-splits.sh` re-applied the first run's split files on later runs (`scp --recurse`
   into an existing dir). Fixed by clearing the remote directory first.

The Spanner Omni issues above were reported to the Spanner Omni team.
