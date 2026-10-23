#!/usr/bin/env python3
"""Create the target DB on arango-spanner and pre-split every table evenly over its key range.

Benchmark keys are '<prefix>' + 12-digit zero-padded ids (see data/gen_spark.py), so even split points are exact.
Split points per table are proportional to its logical size. AddSplitPoints allows 20 points/minute.

Usage: prepare_spanner_db.py <db> <ddl.sql> --scale 1.35 [--total-splits 400] [--instance arango-spanner]
"""
import argparse, os, subprocess, tempfile, time

# table -> (key prefix, docs at scale 1.0, avg JSON bytes)
TABLES = {'users': ('u', 500_000_000, 1850), 'products': ('p', 125_000_000, 5771), 'orders': ('o', 625_000_000, 2605),
          'events': ('e', 2_333_000_000, 1342), 'follows': ('f', 2_000_000_000, 503),
          'purchased': ('b', 2_000_000_000, 473)}


def gcloud(*args, check=True):
    env = dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni')
    return subprocess.run(['gcloud', *args], env=env, capture_output=True, text=True, check=check)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('db')
    ap.add_argument('ddl')
    ap.add_argument('--scale', type=float, default=1.35)
    ap.add_argument('--total-splits', type=int, default=400)
    ap.add_argument('--instance', default='arango-spanner')
    a = ap.parse_args()
    total_bytes = sum(c * b for _, c, b in TABLES.values())
    splits = []
    for t, (p, c, b) in TABLES.items():
        n = int(c * a.scale)
        k = max(1, round(a.total_splits * c * b / total_bytes))
        splits += [f"TABLE {t} ('{p}{i * n // (k + 1):012d}')" for i in range(1, k + 1)]
    print(f'{len(splits)} split points', flush=True)
    if gcloud('spanner', 'databases', 'describe', a.db, f'--instance={a.instance}', check=False).returncode:
        gcloud('spanner', 'databases', 'create', a.db, f'--instance={a.instance}', f'--ddl-file={a.ddl}')
        print('created', a.db, flush=True)
    exp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(time.time() + 7 * 86400))
    for i in range(0, len(splits), 20):
        with tempfile.NamedTemporaryFile('w', suffix='.txt', delete=False) as f:
            f.write('\n'.join(splits[i:i + 20]) + '\n')
        while True:
            r = gcloud('spanner', 'databases', 'splits', 'add', a.db, f'--instance={a.instance}',
                       f'--splits-file={f.name}', '--initiator=arango-migration',
                       f'--split-expiration-date={exp}', check=False)
            if r.returncode == 0:
                break
            if 'per minute' not in r.stderr:
                raise SystemExit('split add failed: ' + r.stderr[-500:])
            print('retry:', r.stderr.strip()[-200:], flush=True)
            time.sleep(20)
        print(f'added {min(i + 20, len(splits))}/{len(splits)}', flush=True)
        time.sleep(61)
    print('SPLITS_DONE', flush=True)


if __name__ == '__main__':
    main()
