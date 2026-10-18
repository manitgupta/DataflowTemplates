#!/usr/bin/env python3
"""Replay the sourcedb-to-spanner severe DLQ of a GCS (ArangoDB envelope) import with JSON round mode.

Spanner's exact JSON number mode rejects some doubles ("cannot round-trip through string representation"); those
rows end up in the template's DLQ. This tool upserts every DLQ row with DML, wrapping the JSON column in
PARSE_JSON(@doc, wide_number_mode=>'round') so Spanner itself rounds the (rare) numbers it cannot store exactly.

Usage: replay_dlq.py <dlq gs:// dir> --instance I --database D [--procs 32] [--batch 200] [--dry-run]
"""
import argparse, collections, json, multiprocessing as mp, os, sys, time

from google.cloud import spanner, storage

ENVELOPE = {'key', 'id', 'rev', 'from_id', 'to_id', 'doc'}


def list_files(uri):
    bucket, _, prefix = uri[len('gs://'):].partition('/')
    client = storage.Client()
    return [(bucket, b.name) for b in client.list_blobs(bucket, prefix=prefix.rstrip('/') + '/')
            if not b.name.endswith('/') and '/tmp/' not in b.name and b.size > 0]


def parse_line(line):
    """DLQ line -> (table, {column: value}). Handles the FailsafeElement wrapper ({"message": ...})."""
    rec = json.loads(line)
    msg = rec.get('message', rec)
    if isinstance(msg, str):
        msg = json.loads(msg)
    table = msg['_metadata_table']
    row = {k: v for k, v in msg.items() if k in ENVELOPE}
    return table, row


def stmt_for(table, cols):
    names = ', '.join(f'`{c}`' for c in cols)
    vals = ', '.join("PARSE_JSON(@doc, wide_number_mode=>'round')" if c == 'doc' else f'@{c}' for c in cols)
    return f'INSERT OR UPDATE INTO `{table}` ({names}) VALUES ({vals})'


def worker(args):
    files, inst, db, batch, dry = args
    gcs = storage.Client()
    database = spanner.Client().instance(inst).database(db)
    stats = collections.Counter()
    pending = []

    def flush():
        if not pending or dry:
            pending.clear()
            return
        stmts = list(pending)
        pending.clear()

        def txn(t):
            status, counts = t.batch_update(stmts)
            if status.code != 0:
                raise RuntimeError(f'batch_update failed: {status}')
        database.run_in_transaction(txn)

    for bucket, name in files:
        for line in gcs.bucket(bucket).blob(name).download_as_text().splitlines():
            if not line.strip():
                continue
            table, row = parse_line(line)
            cols = [c for c in ('key', 'id', 'rev', 'from_id', 'to_id', 'doc') if c in row]
            params = {c: row[c] for c in cols}
            types = {c: spanner.param_types.STRING for c in cols}
            pending.append((stmt_for(table, cols), params, types))
            stats[table] += 1
            if len(pending) >= batch:
                flush()
    flush()
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dlq')
    ap.add_argument('--instance', required=True)
    ap.add_argument('--database', required=True)
    ap.add_argument('--procs', type=int, default=32)
    ap.add_argument('--batch', type=int, default=200)
    ap.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    files = list_files(a.dlq)
    print(f'{len(files)} DLQ files under {a.dlq}', flush=True)
    if not files:
        print('REPLAY_DONE rows=0')
        return
    chunks = [files[i::a.procs] for i in range(min(a.procs, len(files)))]
    t0 = time.time()
    total = collections.Counter()
    with mp.Pool(len(chunks)) as pool:
        for s in pool.imap_unordered(worker, [(c, a.instance, a.database, a.batch, a.dry_run) for c in chunks]):
            total.update(s)
            print(f'progress {sum(total.values()):,} rows {dict(total)}', flush=True)
    print(f'REPLAY_DONE rows={sum(total.values()):,} per_table={dict(total)} elapsed={time.time() - t0:,.0f}s '
          f'dry_run={a.dry_run}', flush=True)


if __name__ == '__main__':
    main()
