#!/usr/bin/env python3
"""Per-table COUNT(*) against Spanner Omni with a timeout each, to find tables whose scans stall.

Usage: omni_table_probe.py <database> <host:port> [timeout_s]
"""
import os, sys, threading, time

db_name, host = sys.argv[1], sys.argv[2]
timeout = int(sys.argv[3]) if len(sys.argv) > 3 else 240
os.environ['SPANNER_EMULATOR_HOST'] = host
from google.cloud import spanner  # noqa: E402
from google.auth.credentials import AnonymousCredentials  # noqa: E402

db = spanner.Client(project='default', credentials=AnonymousCredentials()).instance('default').database(db_name)
with db.snapshot() as snap:
    tables = [r[0] for r in snap.execute_sql(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='' AND TABLE_TYPE='BASE TABLE'")]
for t in tables:
    res = {}

    def run():
        try:
            with db.snapshot() as s:
                res['n'] = list(s.execute_sql(f'SELECT COUNT(*) FROM `{t}`'))[0][0]
        except Exception as e:  # noqa: BLE001
            res['err'] = str(e)[:200]

    th = threading.Thread(target=run, daemon=True)
    t0 = time.time()
    th.start()
    th.join(timeout)
    el = time.time() - t0
    if th.is_alive():
        print(f'{t:13s} TIMEOUT after {timeout}s', flush=True)
    else:
        print(f"{t:13s} {res.get('n', res.get('err'))} ({el:.1f}s)", flush=True)
os._exit(0)
