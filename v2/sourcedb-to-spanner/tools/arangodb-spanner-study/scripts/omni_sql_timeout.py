#!/usr/bin/env python3
"""Run SQL statements against Spanner Omni, each in its own process with a timeout (prints TIMEOUT if it hangs).

Usage: omni_sql_timeout.py <db> <host:port> <timeout_s> "<sql>" ["<sql>" ...]
"""
import multiprocessing as mp, os, sys, time

db_name, host, tmo = sys.argv[1], sys.argv[2], int(sys.argv[3])


def run(sql):
    os.environ['SPANNER_EMULATOR_HOST'] = host
    from google.cloud import spanner
    from google.auth.credentials import AnonymousCredentials
    db = spanner.Client(project='default', credentials=AnonymousCredentials()).instance('default').database(db_name)
    with db.snapshot() as snap:
        return [tuple(r) for r in snap.execute_sql(sql)]


if __name__ == '__main__':
    with mp.Pool(len(sys.argv) - 4) as pool:
        t0 = time.time()
        jobs = [(sql, pool.apply_async(run, (sql,))) for sql in sys.argv[4:]]
        for sql, j in jobs:
            try:
                print(f'{j.get(timeout=max(1, tmo - (time.time() - t0)))} ({time.time() - t0:.0f}s) <- {sql}', flush=True)
            except mp.TimeoutError:
                print(f'TIMEOUT <- {sql}', flush=True)
        pool.terminate()
