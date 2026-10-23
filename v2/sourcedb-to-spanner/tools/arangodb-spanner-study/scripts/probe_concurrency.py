#!/usr/bin/env python3
"""Aggregate insert throughput vs number of concurrent AQL queries (words-150 docs)."""
import concurrent.futures as cf, sys, time
sys.path.insert(0, '../data')
import gen_arango as g  # noqa: E402

DB = 'genprobe2'
for k in [int(x) for x in sys.argv[1:]] or [1, 8, 24, 48]:
    coll = f'pc{k}'
    g.req('DELETE', f'/_db/{DB}/_api/collection/{coll}')
    g.req('POST', f'/_db/{DB}/_api/collection', {'name': coll, 'numberOfShards': 24})
    n = 100_000
    q = (f"FOR i IN @lo..@hi INSERT {{_key: TO_STRING(i), b: CONCAT_SEPARATOR(' ', (FOR j IN 1..150 RETURN "
         f"@w[FLOOR(RAND() * @nw)]))}} INTO {coll}")
    t0 = time.time()
    with cf.ThreadPoolExecutor(k) as ex:
        list(ex.map(lambda i: g.aql(DB, q, {'lo': i * n, 'hi': i * n + n - 1, 'w': g.WORDS, 'nw': len(g.WORDS)},
                                    g.COORDS[i % len(g.COORDS)]), range(k)))
    el = time.time() - t0
    print(f'concurrency={k:3d} docs={k * n:,} {k * n / el:,.0f} docs/s', flush=True)
