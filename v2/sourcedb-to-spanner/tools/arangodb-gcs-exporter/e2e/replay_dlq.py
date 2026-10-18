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


def fetch_from_arango(arango, table_to_coll, table, keys):
    """Re-reads full documents by key (the DLQ only keeps a truncated preview of JSON values)."""
    import base64, urllib.request
    url, db, auth = arango
    q = ('FOR k IN @keys LET d = DOCUMENT(@@c, k) FILTER d != null RETURN {key: d._key, id: d._id, rev: d._rev, '
         'from_id: d._from, to_id: d._to, doc: JSON_STRINGIFY(d)}')
    body = json.dumps({'query': q, 'bindVars': {'keys': keys, '@c': table_to_coll.get(table, table)},
                       'batchSize': 10000}).encode()
    req = urllib.request.Request(f'{url}/_db/{db}/_api/cursor', data=body, method='POST',
                                 headers={'Authorization': 'Basic ' + base64.b64encode(auth.encode()).decode()})
    res = json.load(urllib.request.urlopen(req, timeout=600))
    out = res['result']
    while res.get('hasMore'):
        req = urllib.request.Request(f"{url}/_db/{db}/_api/cursor/{res['id']}", data=b'', method='POST',
                                     headers={'Authorization': 'Basic ' + base64.b64encode(auth.encode()).decode()})
        res = json.load(urllib.request.urlopen(req, timeout=600))
        out += res['result']
    return out


def worker(args):
    files, inst, db, batch, dry, arango, table_to_coll = args
    gcs = storage.Client()
    database = spanner.Client().instance(inst).database(db)
    stats = collections.Counter()
    pending = []
    if arango:
        return worker_refetch(files, database, batch, dry, arango, table_to_coll, gcs)

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


def upsert(database, table, rows, dry):
    """Upserts envelope rows with DML, JSON column via PARSE_JSON(..., wide_number_mode=>'round')."""
    if dry or not rows:
        return
    stmts = []
    for r in rows:
        cols = [c for c in ('key', 'id', 'rev', 'from_id', 'to_id', 'doc') if r.get(c) is not None]
        stmts.append((stmt_for(table, cols), {c: r[c] for c in cols}, {c: spanner.param_types.STRING for c in cols}))

    def txn(t):
        status, _ = t.batch_update(stmts)
        if status.code != 0:
            raise RuntimeError(f'batch_update failed: {status}')
    database.run_in_transaction(txn)


def worker_refetch(files, database, batch, dry, arango, table_to_coll, gcs):
    stats = collections.Counter()
    keys = collections.defaultdict(list)

    def drain(table, final=False):
        while keys[table] and (final or len(keys[table]) >= 1000):
            chunk, keys[table] = keys[table][:1000], keys[table][1000:]
            rows = fetch_from_arango(arango, table_to_coll, table, chunk)
            stats[table + ':missing_in_source'] += len(chunk) - len(rows)
            for i in range(0, len(rows), batch):
                upsert(database, table, rows[i:i + batch], dry)
            stats[table] += len(rows)

    for bucket, name in files:
        for line in gcs.bucket(bucket).blob(name).download_as_text().splitlines():
            if line.strip():
                table, row = parse_line(line)
                keys[table].append(row['key'])
                drain(table)
    for table in list(keys):
        drain(table, final=True)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('dlq')
    ap.add_argument('--instance', required=True)
    ap.add_argument('--database', required=True)
    ap.add_argument('--procs', type=int, default=32)
    ap.add_argument('--batch', type=int, default=200)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--arango-url', help='re-read full documents from ArangoDB by key (e.g. http://10.30.1.10:8529)')
    ap.add_argument('--arango-db', default='shop10t')
    ap.add_argument('--arango-auth', default='root:')
    ap.add_argument('--table-map', default='', help='table=collection,... when names differ')
    a = ap.parse_args()
    table_to_coll = dict(kv.split('=') for kv in a.table_map.split(',') if kv)
    coords = a.arango_url.split(',') if a.arango_url else []
    files = list_files(a.dlq)
    print(f'{len(files)} DLQ files under {a.dlq}', flush=True)
    if not files:
        print('REPLAY_DONE rows=0')
        return
    chunks = [files[i::a.procs] for i in range(min(a.procs, len(files)))]
    t0 = time.time()
    total = collections.Counter()
    with mp.Pool(len(chunks)) as pool:
        jobs = [(c, a.instance, a.database, a.batch, a.dry_run,
                 (coords[i % len(coords)], a.arango_db, a.arango_auth) if coords else None, table_to_coll)
                for i, c in enumerate(chunks)]
        for s in pool.imap_unordered(worker, jobs):
            total.update(s)
            print(f'progress {sum(total.values()):,} rows {dict(total)}', flush=True)
    print(f'REPLAY_DONE rows={sum(total.values()):,} per_table={dict(total)} elapsed={time.time() - t0:,.0f}s '
          f'dry_run={a.dry_run}', flush=True)


if __name__ == '__main__':
    main()
