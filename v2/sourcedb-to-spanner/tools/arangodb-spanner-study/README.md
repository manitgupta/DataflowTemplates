# ArangoDB → Spanner / Spanner Omni: 10 TB feasibility study

Infrastructure scripts, data generator, run scripts, report and slide builder from the end-to-end test of migrating
ArangoDB to Spanner and Spanner Omni.

- **Export:** the ArangoDB Spark exporter (`../arangodb-gcs-exporter`) writes ArangoDB to GCS in the bulk-template Avro
  format.
- **Import:** `sourcedb-to-spanner` with `--sourceDbDialect=GCS`, on Dataflow and on Apache Flink.
- **Validation:** `gcs-spanner-dv`.

The results are in [`report/REPORT.md`](report/REPORT.md):

- Sections 1–9: the template work and the Cloud Spanner runs.
- Section 10: the 10 TB run, Cloud Spanner vs multi-server Spanner Omni.
- Section 11: the 10 TB run into Spanner Omni, Apache Flink vs Dataflow.

All cloud resources used by the study have been deleted; the scripts recreate them (prefix `arango-`).

| Path | What |
|---|---|
| `infra/` | VPC/GCS (`01`), ArangoDB 3.11 cluster on Z3 local NVMe (`02`, `arango-node-startup.sh`), ops VM (`03`), Dataproc (`04`), us-east1 subnet + Spanner (`05`), GKE Flink cluster with the Flink Kubernetes Operator (`06`), cleanup (`99`) |
| `omni/` | Multi-server Spanner Omni on GCE: servers (`01`), deployment (`02`), database + internal TCP LB (`03`), split points (`04`), LB for multi-zone servers (`05`), graceful rolling restart (`06`); single-server variant |
| `data/` | Dataset generator (Spark through the ArangoDB connector, plus the hostile `type_zoo` / `hostile_keys` sets) and Spanner DDL |
| `scripts/` | Export / import / validation launchers (Dataflow, Flink, gcs-spanner-dv), DLQ replay (`replay_dlq.py`), progress, metrics and CPU probes, Omni SQL / range probes |
| `report/` | `REPORT.md`; `deck/` builds the Google Slides deck and its Sheets charts with `gwscli` (`build_charts.py <sheet>`, `build_deck.py <pres> <sheet>`, `review.sh`) |
| `results/` | Row counts, validation summaries, job metrics and the throughput samples behind the charts |

Not included:

- The optimized portable pipeline wrapper and Flink launcher used for the Flink-vs-Dataflow runs (section 11):
  OmniSpannerWrite, compact coders, the `SourceDbToSpannerPortableMain` launcher and `flink-job.sh`. They come from the
  separate Spanner Migration Omni proof of concept. `scripts/run-flink-gcs.sh` and `scripts/run-dataflow-portable.sh`
  expect them under `flink/`.
- Raw launch logs and the detailed Spanner Omni crash logs (reported to the Spanner Omni team).
