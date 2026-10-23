#!/usr/bin/env python3
"""Server-side generator for the ArangoDB 10 TB benchmark dataset (stdlib only).

Documents are produced inside ArangoDB by range-chunked AQL `FOR i IN lo..hi INSERT ... OPTIONS {overwriteMode:
'ignore'}` queries, spread round-robin over all coordinators by a thread pool. Every chunk is idempotent (deterministic
keys + ignore), so a run can be restarted at any time; completed chunks are recorded in a local state file.

  gen_arango.py create  --db shop10t --scale 1.0 [--shards 256]
  gen_arango.py gen     --db shop10t --scale 1.0 [--threads 96] [--only users,orders]
  gen_arango.py counts  --db shop10t --scale 1.0         # exact counts vs plan -> JSON
  gen_arango.py sizes   --db shop10t                     # collection figures (disk) + logical JSON size estimate
  gen_arango.py plan    --scale 1.0
"""
import argparse, base64, concurrent.futures as cf, json, os, sys, threading, time, urllib.request, urllib.error

COORDS = os.environ.get('ARANGO_COORDS', ','.join(f'10.30.1.{10 + i}:8529' for i in range(3))).split(',')
AUTH = 'Basic ' + base64.b64encode(f"root:{os.environ.get('ARANGO_PASSWORD', '')}".encode()).decode()

WORDS = ("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore "
         "magna aliqua enim ad minim veniam quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo "
         "consequat duis aute irure in reprehenderit voluptate velit esse cillum fugiat nulla pariatur excepteur sint "
         "occaecat cupidatat non proident sunt culpa qui officia deserunt mollit anim id est laborum graph vertex edge "
         "shard cluster spanner arango document migration bulk export import river mountain ocean forest desert city "
         "harbor bridge tower garden market station library museum theatre stadium avenue boulevard canal island "
         "über café naïve résumé façade jalapeño 東京 北京 서울 Москва القاهرة 🚀 🎉 ✓").split()

# name -> (kind, docs at scale 1.0, AQL object expression over loop variable i). `@w` = WORDS, `@nw` = len(WORDS).
_TXT_N = [0]


class _Txt:
    """Text subquery template; every use gets its own loop variable (AQL forbids reusing names)."""

    def format(self, n):
        _TXT_N[0] += 1
        v = f'w{_TXT_N[0]}'
        return f"CONCAT_SEPARATOR(' ', (FOR {v} IN 1..{n} RETURN @w[FLOOR(RAND() * @nw)]))"


TXT = _Txt()
K = lambda p, e='i': f"CONCAT('{p}', RIGHT(CONCAT('000000000000', {e}), 12))"
COLLECTIONS = {
    'users': ('DOCUMENT', 500_000_000, f"""{{
        _key: {K('u')}, name: {TXT.format(n=2)}, email: CONCAT('user', i, '@example.com'),
        age: 18 + i % 70, active: i % 7 != 0, score: RAND() * 1000, created: DATE_ISO8601(1500000000000 + i * 1000),
        tags: (FOR j IN 1..(i % 6) RETURN @w[(i + j * 7) % @nw]),
        address: {{street: {TXT.format(n=3)}, city: @w[i % @nw], zip: RIGHT(CONCAT('0000', i % 99999), 5),
                   geo: [RAND() * 180 - 90, RAND() * 360 - 180]}},
        prefs: {{lang: ['en','de','fr','ja','ko','ar'][i % 6], newsletter: i % 3 == 0, theme: i % 2 ? 'dark' : null}},
        bio: {TXT.format(n=220)} }}"""),
    'products': ('DOCUMENT', 125_000_000, f"""{{
        _key: {K('p')}, title: {TXT.format(n=5)}, sku: CONCAT('SKU-', i), price: ROUND(RAND() * 100000) / 100,
        stock: i % 1000, categories: [@w[i % @nw], @w[(i * 3) % @nw]],
        attrs: {{color: @w[(i * 5) % @nw], weight_g: i % 5000, dims: [i % 100, i % 50, i % 25], fragile: i % 11 == 0}},
        description: {TXT.format(n=600)},
        reviews: (FOR j IN 1..(3 + i % 5) RETURN {{user: {K('u', '(i * 7 + j) % 500000000')}, rating: 1 + (i + j) % 5,
                  text: {TXT.format(n=40)}}}) }}"""),
    'orders': ('DOCUMENT', 625_000_000, f"""{{
        _key: {K('o')}, user: {K('u', 'i % 500000000')}, status: ['NEW','PAID','SHIPPED','DELIVERED','RETURNED'][i % 5],
        ts: DATE_ISO8601(1600000000000 + i * 37), currency: ['USD','EUR','JPY'][i % 3],
        items: (FOR j IN 1..(1 + i % 8) RETURN {{product: {K('p', '(i * 13 + j) % 125000000')}, qty: 1 + j % 4,
                price: ROUND(RAND() * 50000) / 100, gift: j % 5 == 0}}),
        total: ROUND(RAND() * 300000) / 100,
        shipping: {{name: {TXT.format(n=2)}, street: {TXT.format(n=4)}, city: @w[(i * 11) % @nw],
                    express: i % 4 == 0, notes: i % 9 == 0 ? null : {TXT.format(n=30)}}},
        notes: {TXT.format(n=280)} }}"""),
    'events': ('DOCUMENT', 2_333_000_000, f"""{{
        _key: {K('e')}, user: {K('u', 'i % 500000000')}, type: ['view','click','cart','search','login','logout'][i % 6],
        ts: DATE_ISO8601(1650000000000 + i * 3), session: MD5(TO_STRING(FLOOR(i / 20))),
        payload: i % 3 == 0 ? {{query: {TXT.format(n=4)}, results: i % 100}}
               : (i % 3 == 1 ? {{product: {K('p', 'i % 125000000')}, ms: i % 3000}} : [i % 10, 'x', null, true]),
        ua: {TXT.format(n=12)}, body: {TXT.format(n=150)} }}"""),
    'follows': ('EDGE', 2_000_000_000, f"""{{
        _key: {K('f')}, _from: CONCAT('users/', {K('u', 'i % 500000000')}),
        _to: CONCAT('users/', {K('u', '(i * 7919 + 13) % 500000000')}),
        since: DATE_ISO8601(1400000000000 + i * 11), weight: RAND(), muted: i % 13 == 0,
        note: {TXT.format(n=45)} }}"""),
    'purchased': ('EDGE', 2_000_000_000, f"""{{
        _key: {K('b')}, _from: CONCAT('users/', {K('u', 'i % 500000000')}),
        _to: CONCAT('products/', {K('p', '(i * 104729) % 125000000')}),
        order: {K('o', 'i % 625000000')}, qty: 1 + i % 5, ts: DATE_ISO8601(1600000000000 + i * 41),
        note: {TXT.format(n=40)} }}"""),
    # Mixed/hostile JSON types: same attribute with different types across docs, missing vs null, big numbers.
    'type_zoo': ('DOCUMENT', 10_000_000, f"""{{
        _key: {K('z')},
        v: [i, TO_STRING(i), i % 2 == 0, null, [i, 'a', [null]], {{n: i}}, 9007199254740993, 1e300, -0.0, 0.1][i % 10],
        maybe: i % 4 == 0 ? null : (i % 4 == 1 ? 'present' : (i % 4 == 2 ? {{deep: {{deeper: [1, {{x: null}}]}}}} : [])),
        big: [9223372036854775807, -9223372036854775808, 18446744073709551615, 1.7976931348623157e308, 5e-324][i % 5],
        uni: ['emoji 🚀🎉', 'quote " backslash \\\\ slash /', 'tab\\tnewline\\n', '東京 서울 القاهرة', ''][i % 5],
        `attr-with-dash`: i, `attr.with.dot`: i % 2, `attr with space`: 'x',
        filler: {TXT.format(n=120)} }}"""),
}
HOSTILE_KEYS = ['A', 'B', 'Z', 'a', 'b', 'z', '0', '9', '_x', '-x', ':x', '.x', '@x', '(x', ')x', '+x', ',x', '=x',
                ';x', '$x', '!x', '*x', "'x", '%x', 'Aa', 'aA', 'a_b', 'a-b', 'a.b', 'A0', 'a0', '0a', 'x' * 254]
CHUNK = 250_000


def req(method, path, body=None, coord=None, timeout=3600):
    url = f'http://{coord or COORDS[0]}{path}'
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method, headers={'Authorization': AUTH, 'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return json.loads(resp.read() or b'{}')
    except urllib.error.HTTPError as e:
        return json.loads(e.read() or b'{}') | {'error': True, 'code': e.code}


def aql(db, query, bind=None, coord=None, options=None):
    body = {'query': query, 'bindVars': bind or {}, 'batchSize': 1000, 'options': options or {}}
    res = req('POST', f'/_db/{db}/_api/cursor', body, coord)
    if res.get('error'):
        raise RuntimeError(f"{res.get('errorNum')} {res.get('errorMessage')}")
    out = res.get('result', [])
    while res.get('hasMore'):
        res = req('POST', f"/_db/{db}/_api/cursor/{res['id']}", None, coord)
        out += res.get('result', [])
    return out, res.get('extra', {})


def plan(scale, only=None):
    p = {n: max(1, int(c * scale)) for n, (_, c, _) in COLLECTIONS.items() if not only or n in only}
    return p


def cmd_create(a):
    r = req('POST', '/_api/database', {'name': a.db})
    print('create db:', r.get('errorMessage', 'ok'))
    for name, (kind, _, _) in COLLECTIONS.items():
        r = req('POST', f'/_db/{a.db}/_api/collection', {'name': name, 'type': 3 if kind == 'EDGE' else 2,
                'numberOfShards': a.shards, 'replicationFactor': 1, 'writeConcern': 1})
        print('create', name, r.get('errorMessage', 'ok'))
    r = req('POST', f'/_db/{a.db}/_api/collection', {'name': 'hostile_keys', 'numberOfShards': 16, 'replicationFactor': 1})
    print('create hostile_keys', r.get('errorMessage', 'ok'))
    docs = [{'_key': k, 'k': k, 'n': n} for n, k in enumerate(HOSTILE_KEYS)]
    print('hostile insert', req('POST', f'/_db/{a.db}/_api/document/hostile_keys?overwriteMode=ignore', docs) and 'ok')


def cmd_gen(a):
    only = set(a.only.split(',')) if a.only else None
    state_file = f'gen-state-{a.db}.json'
    done = set(json.load(open(state_file))) if os.path.exists(state_file) else set()
    lock = threading.Lock()
    tasks = []
    for name, total in plan(a.scale, only).items():
        expr = COLLECTIONS[name][2]
        for lo in range(0, total, CHUNK):
            hi = min(total, lo + CHUNK) - 1
            tid = f'{name}:{lo}'
            if tid not in done:
                tasks.append((tid, name, expr, lo, hi))
    # interleave collections so every shard set is busy
    tasks.sort(key=lambda t: (t[3], t[1]))
    print(f'{len(tasks)} chunks to do ({len(done)} already done), threads={a.threads}', flush=True)
    t0, stats = time.time(), {'docs': 0, 'n': 0}

    def run(idx_task):
        idx, (tid, name, expr, lo, hi) = idx_task
        q = f'FOR i IN @lo..@hi INSERT {expr} INTO {name} OPTIONS {{overwriteMode: "ignore"}}'
        coord = COORDS[idx % len(COORDS)]
        for attempt in range(8):
            try:
                aql(a.db, q, {'lo': lo, 'hi': hi, 'w': WORDS, 'nw': len(WORDS)}, coord,
                    {'intermediateCommitCount': 50000, 'stream': False})
                break
            except Exception as e:  # noqa: BLE001 - retry anything (timeouts, 503 during compaction)
                print(f'retry {tid} #{attempt}: {e}', flush=True)
                time.sleep(5 * (attempt + 1))
                coord = COORDS[(idx + attempt + 1) % len(COORDS)]
        else:
            print(f'FAILED {tid}', flush=True)
            return
        with lock:
            done.add(tid)
            stats['docs'] += hi - lo + 1
            stats['n'] += 1
            if stats['n'] % 20 == 0 or stats['n'] == len(tasks):
                json.dump(sorted(done), open(state_file + '.tmp', 'w'))
                os.replace(state_file + '.tmp', state_file)
                el = time.time() - t0
                print(f"{time.strftime('%H:%M:%S')} chunks {stats['n']}/{len(tasks)} docs {stats['docs']:,} "
                      f"rate {stats['docs'] / el:,.0f} docs/s elapsed {el:,.0f}s", flush=True)

    with cf.ThreadPoolExecutor(a.threads) as ex:
        list(ex.map(run, enumerate(tasks)))
    json.dump(sorted(done), open(state_file, 'w'))
    print(f'GEN_DONE elapsed {time.time() - t0:,.0f}s', flush=True)


def cmd_counts(a):
    p = plan(a.scale)
    out, ok = {}, True
    for name, expected in p.items():
        c = req('GET', f'/_db/{a.db}/_api/collection/{name}/count').get('count')
        out[name] = c
        ok &= c == expected
        print(f'{name:10s} count={c:,} plan={expected:,} {"OK" if c == expected else "MISMATCH"}')
    out['hostile_keys'] = req('GET', f'/_db/{a.db}/_api/collection/hostile_keys/count').get('count')
    json.dump(out, open(f'source-counts-{a.db}.json', 'w'), indent=1)
    print('COUNTS_OK' if ok else 'COUNTS_MISMATCH')


def cmd_sizes(a):
    tot_disk = tot_logical = 0
    for name in list(COLLECTIONS) + ['hostile_keys']:
        f = req('GET', f'/_db/{a.db}/_api/collection/{name}/figures?details=true').get('figures', {})
        cnt = req('GET', f'/_db/{a.db}/_api/collection/{name}/count').get('count', 0)
        disk = f.get('documentsSize', 0) + f.get('indexes', {}).get('size', 0)
        # logical JSON size: average JSON_STRINGIFY length over a sample, times count
        sample, _ = aql(a.db, f'FOR d IN {name} LIMIT 2000 RETURN LENGTH(JSON_STRINGIFY(d))') if cnt else ([], {})
        avg = sum(sample) / len(sample) if sample else 0
        logical = avg * cnt
        tot_disk += disk
        tot_logical += logical
        print(f'{name:12s} count={cnt:>14,} disk={disk / 1e12:8.3f} TB avgJson={avg:7.0f} B logical={logical / 1e12:8.3f} TB')
    print(f'TOTAL disk(documentsSize+indexes)={tot_disk / 1e12:.3f} TB logical JSON={tot_logical / 1e12:.3f} TB')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('cmd', choices=['create', 'gen', 'counts', 'sizes', 'plan'])
    ap.add_argument('--db', default='shop10t')
    ap.add_argument('--scale', type=float, default=1.0)
    ap.add_argument('--shards', type=int, default=256)
    ap.add_argument('--threads', type=int, default=96)
    ap.add_argument('--only')
    a = ap.parse_args()
    if a.cmd == 'plan':
        for k, v in plan(a.scale).items():
            print(f'{k:10s} {v:>15,}')
        return
    {'create': cmd_create, 'gen': cmd_gen, 'counts': cmd_counts, 'sizes': cmd_sizes}[a.cmd](a)


if __name__ == '__main__':
    main()
