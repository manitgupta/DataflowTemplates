#!/usr/bin/env python3
"""PySpark generator for the ArangoDB 10 TB dataset; writes through the ArangoDB Spark connector.

All values are pure Spark SQL expressions (no Python UDFs) derived deterministically from the row id, so re-runs
produce identical documents; writes use overwriteMode=ignore, so any (partial) re-run is idempotent.

  gcloud dataproc jobs submit pyspark gen_spark.py --cluster arango-dataproc --region us-central1 -- \
     --db shop10t --scale 1.0 --endpoints 10.30.1.10:8529,... [--only users,orders] [--docs-per-partition 1000000]
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

WORDS = ("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore "
         "magna aliqua enim ad minim veniam quis nostrud exercitation ullamco laboris nisi aliquip ex ea commodo "
         "consequat duis aute irure in reprehenderit voluptate velit esse cillum fugiat nulla pariatur excepteur sint "
         "occaecat cupidatat non proident sunt culpa qui officia deserunt mollit anim id est laborum graph vertex edge "
         "shard cluster spanner arango document migration bulk export import river mountain ocean forest desert city "
         "harbor bridge tower garden market station library museum theatre stadium avenue boulevard canal island "
         "über café naïve résumé façade jalapeño 東京 北京 서울 Москва القاهرة 🚀 🎉 ✓").split()
NW = len(WORDS)
WARR = "array(" + ",".join("'" + w.replace("'", "\\'") + "'" for w in WORDS) + ")"

# Same document counts as gen_arango.py (scale 1.0 ~= 10 TB of logical JSON).
COUNTS = {'users': 500_000_000, 'products': 125_000_000, 'orders': 625_000_000, 'events': 2_333_000_000,
          'follows': 2_000_000_000, 'purchased': 2_000_000_000}
EDGES = {'follows', 'purchased'}


def key(prefix, e='id'):
    return f"concat('{prefix}', lpad(cast({e} as string), 12, '0'))"


def txt(n, salt):
    """n pseudo-random words, deterministic in (id, salt, position)."""
    return (f"concat_ws(' ', transform(sequence(1, {n}), x -> element_at({WARR}, "
            f"cast(pmod(xxhash64(id, {salt}, x), {NW}) as int) + 1)))")


def ts(base_ms, step):
    return f"date_format(timestamp_millis({base_ms} + id * {step}), \"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\")"


def frac(salt):
    return f"cast(pmod(xxhash64(id, {salt}), 1000000) / 1000000.0 as double)"


EXPRS = {
    'users': {
        '_key': key('u'), 'name': txt(2, 1), 'email': "concat('user', id, '@example.com')",
        'age': '18 + id % 70', 'active': 'id % 7 != 0', 'score': f'{frac(2)} * 1000', 'created': ts(1500000000000, 1000),
        'tags': f"filter(transform(sequence(1, 5), j -> element_at({WARR}, cast(pmod(id + j * 7, {NW}) as int) + 1)), "
                f"(v, j) -> j < id % 6)",
        'address': f"named_struct('street', {txt(3, 3)}, 'city', element_at({WARR}, cast(pmod(id, {NW}) as int) + 1), "
                   f"'zip', lpad(cast(id % 99999 as string), 5, '0'), 'geo', array({frac(4)} * 180 - 90, {frac(5)} * 360 - 180))",
        'prefs': "named_struct('lang', element_at(array('en','de','fr','ja','ko','ar'), cast(id % 6 as int) + 1), "
                 "'newsletter', id % 3 = 0, 'theme', if(id % 2 = 1, 'dark', null))",
        'bio': txt(220, 6),
    },
    'products': {
        '_key': key('p'), 'title': txt(5, 11), 'sku': "concat('SKU-', id)", 'price': f'cast(round({frac(12)} * 1000, 2) as double)',
        'stock': 'id % 1000',
        'categories': f"array(element_at({WARR}, cast(pmod(id, {NW}) as int) + 1), element_at({WARR}, cast(pmod(id * 3, {NW}) as int) + 1))",
        'attrs': f"named_struct('color', element_at({WARR}, cast(pmod(id * 5, {NW}) as int) + 1), 'weight_g', id % 5000, "
                 "'dims', array(id % 100, id % 50, id % 25), 'fragile', id % 11 = 0)",
        'description': txt(600, 13),
        'reviews': f"filter(transform(sequence(1, 7), j -> named_struct('user', {key('u', 'pmod(id * 7 + j, 500000000)')}, "
                   f"'rating', cast(1 + (id + j) % 5 as int), 'text', concat_ws(' ', transform(sequence(1, 40), x -> "
                   f"element_at({WARR}, cast(pmod(xxhash64(id, 14, j, x), {NW}) as int) + 1))))), (v, j) -> j < 3 + id % 5)",
    },
    'orders': {
        '_key': key('o'), 'user': key('u', 'id % 500000000'),
        'status': "element_at(array('NEW','PAID','SHIPPED','DELIVERED','RETURNED'), cast(id % 5 as int) + 1)",
        'ts': ts(1600000000000, 37), 'currency': "element_at(array('USD','EUR','JPY'), cast(id % 3 as int) + 1)",
        'items': f"filter(transform(sequence(1, 8), j -> named_struct('product', {key('p', 'pmod(id * 13 + j, 125000000)')}, "
                 f"'qty', cast(1 + j % 4 as int), 'price', cast(pmod(xxhash64(id, 21, j), 50000) / 100.0 as double), 'gift', j % 5 = 0)), "
                 "(v, j) -> j < 1 + id % 8)",
        'total': f'cast(round({frac(22)} * 3000, 2) as double)',
        'shipping': f"named_struct('name', {txt(2, 23)}, 'street', {txt(4, 24)}, "
                    f"'city', element_at({WARR}, cast(pmod(id * 11, {NW}) as int) + 1), 'express', id % 4 = 0, "
                    f"'notes', if(id % 9 = 0, null, {txt(30, 25)}))",
        'notes': txt(280, 26),
    },
    'events': {
        '_key': key('e'), 'user': key('u', 'id % 500000000'),
        'type': "element_at(array('view','click','cart','search','login','logout'), cast(id % 6 as int) + 1)",
        'ts': ts(1650000000000, 3), 'session': 'md5(cast(floor(id / 20) as string))',
        'payload': f"named_struct('query', if(id % 3 = 0, {txt(4, 31)}, null), 'results', if(id % 3 = 0, id % 100, null), "
                   f"'product', if(id % 3 = 1, {key('p', 'id % 125000000')}, null), 'ms', if(id % 3 = 1, id % 3000, null))",
        'ua': txt(12, 32), 'body': txt(150, 33),
    },
    'follows': {
        '_key': key('f'), '_from': f"coalesce(concat('users/', {key('u', 'id % 500000000')}), '')",
        '_to': f"coalesce(concat('users/', {key('u', 'pmod(id * 7919 + 13, 500000000)')}), '')",
        'since': ts(1400000000000, 11), 'weight': frac(41), 'muted': 'id % 13 = 0', 'note': txt(45, 42),
    },
    'purchased': {
        '_key': key('b'), '_from': f"coalesce(concat('users/', {key('u', 'id % 500000000')}), '')",
        '_to': f"coalesce(concat('products/', {key('p', 'pmod(id * 104729, 125000000)')}), '')",
        'order': key('o', 'id % 625000000'), 'qty': 'cast(1 + id % 5 as int)', 'ts': ts(1600000000000, 41),
        'note': txt(40, 43),
    },
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default='shop10t')
    ap.add_argument('--scale', type=float, default=1.0)
    ap.add_argument('--endpoints', required=True)
    ap.add_argument('--only')
    ap.add_argument('--start-fraction', type=float, default=0.0, help='skip the first fraction of ids (resume aid)')
    ap.add_argument('--docs-per-partition', type=int, default=1_000_000)
    ap.add_argument('--batch-size', type=int, default=2000)
    a = ap.parse_args()
    spark = SparkSession.builder.appName('arango-gen').config('spark.sql.session.timeZone', 'UTC').getOrCreate()
    names = [n for n in COUNTS if not a.only or n in a.only.split(',')]
    for name in names:
        total = max(1, int(COUNTS[name] * a.scale))
        start = int(total * a.start_fraction)
        parts = max(1, (total - start) // a.docs_per_partition)
        df = spark.range(start, total, numPartitions=parts).selectExpr(
            *[f'{expr} AS `{col}`' for col, expr in EXPRS[name].items()])
        t0 = time.time()
        (df.write.format('com.arangodb.spark').mode('append')
         .option('endpoints', a.endpoints).option('database', a.db).option('table', name)
         .option('table.type', 'edge' if name in EDGES else 'document')
         .option('overwriteMode', 'ignore').option('batchSize', str(a.batch_size))
         .option('protocol', 'http2').option('contentType', 'json').option('waitForSync', 'false')
         .save())
        el = time.time() - t0
        print(f'GEN {name} docs={total - start:,} elapsed={el:,.0f}s rate={(total - start) / el:,.0f} docs/s', flush=True)
    spark.stop()


if __name__ == '__main__':
    main()
