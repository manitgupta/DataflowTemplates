#!/usr/bin/env python3
"""Batch document API throughput: pre-built ~1.3 KB docs, POST arrays of 1000 docs from K threads/processes."""
import json, multiprocessing as mp, random, sys, time, urllib.request
sys.path.insert(0, '../data')
import gen_arango as g  # noqa: E402

DB, COLL, BATCH = 'genprobe2', 'pb', 1000
body_txt = ' '.join(random.choice(g.WORDS) for _ in range(150))


def worker(args):
    wid, nbatches = args
    coord = g.COORDS[wid % len(g.COORDS)]
    for b in range(nbatches):
        base = (wid * 10_000 + b) * BATCH
        docs = [{'_key': str(base + i), 'b': body_txt, 'n': base + i} for i in range(BATCH)]
        data = json.dumps(docs).encode()
        r = urllib.request.Request(f'http://{coord}/_db/{DB}/_api/document/{COLL}?overwriteMode=ignore&silent=true',
                                   data=data, method='POST', headers={'Authorization': g.AUTH})
        urllib.request.urlopen(r, timeout=600).read()
    return nbatches * BATCH


if __name__ == '__main__':
    for k in [int(x) for x in sys.argv[1:]] or [8, 32, 64]:
        g.req('DELETE', f'/_db/{DB}/_api/collection/{COLL}')
        g.req('POST', f'/_db/{DB}/_api/collection', {'name': COLL, 'numberOfShards': 24})
        t0 = time.time()
        with mp.Pool(k) as p:
            n = sum(p.map(worker, [(w, 30) for w in range(k)]))
        el = time.time() - t0
        print(f'procs={k:3d} docs={n:,} {n / el:,.0f} docs/s {n * 1300 / el / 1e6:,.0f} MB/s', flush=True)
