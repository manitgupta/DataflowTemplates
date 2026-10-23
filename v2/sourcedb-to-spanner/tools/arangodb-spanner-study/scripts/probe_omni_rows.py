#!/usr/bin/env python3
"""Upsert DLQ rows one per transaction (10 s retry budget each) to find rows that Spanner Omni keeps aborting.

Usage: probe_omni_rows.py <dlq gs:// dir> <db> <host:port> [max rows] [file index]
"""
import os, sys, time

dlq, db_name, host = sys.argv[1:4]
limit = int(sys.argv[4]) if len(sys.argv) > 4 else 2000
fidx = int(sys.argv[5]) if len(sys.argv) > 5 else 0
os.environ['SPANNER_OMNI'] = host
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import replay_dlq as r  # noqa: E402
from google.cloud import spanner, storage  # noqa: E402

bucket, name = r.list_files(dlq)[fidx]
lines = [l for l in storage.Client().bucket(bucket).blob(name).download_as_text().splitlines() if l.strip()][:limit]
database = r.spanner_database('default', db_name)
ok = bad = 0
t0 = time.time()
for i, line in enumerate(lines):
    table, row = r.parse_line(line)
    cols = [c for c in ('key', 'id', 'rev', 'from_id', 'to_id', 'doc') if c in row]
    stmt = (r.stmt_for(table, cols), {c: row[c] for c in cols}, {c: spanner.param_types.STRING for c in cols})
    try:
        database.run_in_transaction(lambda t: t.batch_update([stmt]), timeout_secs=10)
        ok += 1
    except Exception as e:
        bad += 1
        print(f'row {i} {table} key={row["key"]} -> {type(e).__name__}: {str(e)[:160]} doc={row.get("doc", "")[:300]}',
              flush=True)
        if bad >= 5:
            break
print(f'{name}: ok={ok} bad={bad} in {time.time() - t0:.1f}s', flush=True)
