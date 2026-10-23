#!/usr/bin/env bash
# Usage: spanner_counts.sh [instance] [db]  - exact COUNT(*) of every shop10t table.
set -euo pipefail
export CLOUDSDK_ACTIVE_CONFIG_NAME=omni
I=${1:-arango-spanner-e1}; D=${2:-shop10t}
date -u +%H:%M:%S
gcloud spanner databases execute-sql $D --instance=$I --format=json --sql="SELECT
 (SELECT COUNT(*) FROM users) AS users, (SELECT COUNT(*) FROM products) AS products,
 (SELECT COUNT(*) FROM orders) AS orders, (SELECT COUNT(*) FROM events) AS events,
 (SELECT COUNT(*) FROM follows) AS follows, (SELECT COUNT(*) FROM purchased) AS purchased,
 (SELECT COUNT(*) FROM type_zoo) AS type_zoo, (SELECT COUNT(*) FROM hostile_keys) AS hostile_keys" | python3 -c "
import json,sys
d=json.load(sys.stdin); names=[f['name'] for f in d['metadata']['rowType']['fields']]; vals=[int(v) for v in d['rows'][0]]
for n,v in zip(names,vals): print(f'{n:13s} {v:>15,}')
print(f'TOTAL         {sum(vals):>15,}')"
