#!/usr/bin/env python3
"""Run replay_dlq's worker in-process (no multiprocessing fork) on the first N DLQ files, timing it.

Usage: probe_replay_nofork.py <dlq gs:// dir> <db> <host:port> [files] [batch]
"""
import os, sys, time

dlq, db_name, host = sys.argv[1:4]
nfiles = int(sys.argv[4]) if len(sys.argv) > 4 else 5
batch = int(sys.argv[5]) if len(sys.argv) > 5 else 100
os.environ['SPANNER_OMNI'] = host
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import replay_dlq as r  # noqa: E402

files = r.list_files(dlq)[:nfiles]
t0 = time.time()
stats = r._worker((files, 'default', db_name, batch, False, None, {}))
print(f'{nfiles} files, {sum(stats.values()):,} rows {dict(stats)} in {time.time() - t0:.1f}s', flush=True)
