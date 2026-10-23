#!/usr/bin/env python3
"""COUNT(*) a table in N key ranges (benchmark keys '<prefix>' + 12 digits), each in its own process with a timeout,
to find key ranges whose reads never return on Spanner Omni.

Usage: omni_range_probe.py <db> <host:port> <table> <prefix> <rows> [ranges=10] [timeout_s=90]
"""
import multiprocessing as mp, os, sys, time

db_name, host, table, prefix, rows = sys.argv[1:6]
rows = int(rows)
n = int(sys.argv[6]) if len(sys.argv) > 6 else 10
tmo = int(sys.argv[7]) if len(sys.argv) > 7 else 90


def count(lo_hi):
    lo, hi = lo_hi
    os.environ['SPANNER_EMULATOR_HOST'] = host
    from google.cloud import spanner
    from google.auth.credentials import AnonymousCredentials
    db = spanner.Client(project='default', credentials=AnonymousCredentials()).instance('default').database(db_name)
    with db.snapshot() as snap:
        return list(snap.execute_sql(f'SELECT COUNT(*) FROM `{table}` WHERE key >= @lo AND key < @hi',
                                     params={'lo': lo, 'hi': hi},
                                     param_types={'lo': spanner.param_types.STRING,
                                                  'hi': spanner.param_types.STRING}))[0][0]


if __name__ == '__main__':
    bounds = [f'{prefix}{i * rows // n:012d}' for i in range(n)] + [f'{prefix}~']
    with mp.Pool(n) as pool:
        t0 = time.time()
        res = [pool.apply_async(count, ((bounds[i], bounds[i + 1]),)) for i in range(n)]
        for i, r in enumerate(res):
            try:
                c = r.get(timeout=max(1, tmo - (time.time() - t0)))
                print(f'{table} [{bounds[i]}, {bounds[i + 1]}) count={c:,} ({time.time() - t0:.0f}s)', flush=True)
            except mp.TimeoutError:
                print(f'{table} [{bounds[i]}, {bounds[i + 1]}) TIMEOUT', flush=True)
        pool.terminate()
