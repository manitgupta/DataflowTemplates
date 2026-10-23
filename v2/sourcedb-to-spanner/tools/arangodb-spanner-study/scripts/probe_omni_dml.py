#!/usr/bin/env python3
"""Time one small round-mode upsert batch (rows from the first DLQ file) against a Spanner Omni endpoint.

Usage: probe_omni_dml.py <dlq gs:// dir> <db> <host:port> [rows] [table]
"""
import os, sys, time

dlq, db_name, host = sys.argv[1:4]
n = int(sys.argv[4]) if len(sys.argv) > 4 else 10
only = sys.argv[5] if len(sys.argv) > 5 else None
os.environ['SPANNER_OMNI'] = host
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import replay_dlq as r  # noqa: E402
from google.cloud import spanner, storage  # noqa: E402

gcs = storage.Client()
stmts = []
for bucket, name in r.list_files(dlq):
    for line in gcs.bucket(bucket).blob(name).download_as_text().splitlines():
        if line.strip() and len(stmts) < n:
            table, row = r.parse_line(line)
            if only and table != only:
                continue
            cols = [c for c in ('key', 'id', 'rev', 'from_id', 'to_id', 'doc') if c in row]
            stmts.append((r.stmt_for(table, cols), {c: row[c] for c in cols},
                          {c: spanner.param_types.STRING for c in cols}))
    if len(stmts) >= n:
        break
database = r.spanner_database('default', db_name)
for i in range(int(os.environ.get('PROBE_TXNS', '1'))):
    t0 = time.time()
    database.run_in_transaction(lambda t: t.batch_update(stmts), timeout_secs=60)
    print(f'{host}: txn {i + 1}: {len(stmts)} rows ({table}) upserted in {time.time() - t0:.2f}s', flush=True)
