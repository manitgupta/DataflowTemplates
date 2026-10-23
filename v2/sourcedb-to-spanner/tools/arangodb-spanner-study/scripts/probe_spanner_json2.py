#!/usr/bin/env python3
"""Bulk-test which JSON number texts Spanner accepts (SAFE.PARSE_JSON -> NULL when rejected).

For N pseudo-random doubles (shaped like the dataset: k/1e6 fractions, *180-90, *1000 rounded, etc.) test
several candidate text forms and report the acceptance rate of each form.
"""
import json, os, random, subprocess, sys

random.seed(int(sys.argv[1]) if len(sys.argv) > 1 else 7)
N = int(sys.argv[2]) if len(sys.argv) > 2 else 120
ds = []
for i in range(N):
    k = random.randrange(1000000)
    ds.append([k / 1e6, k / 1e6 * 180 - 90, round(k / 1e6 * 1000, 2), random.random(), random.random() * 1e6,
               k / 100.0][i % 6])
forms = {
    'shortest': lambda d: repr(d),
    'g17': lambda d: '%.17g' % d,
    'g16': lambda d: '%.16g' % d,
    'g15': lambda d: '%.15g' % d,
}


def run(texts):
    cols = ', '.join(f"SAFE.PARSE_JSON('[{t}]') IS NOT NULL AS c{i}" for i, t in enumerate(texts))
    r = subprocess.run(['gcloud', 'spanner', 'databases', 'execute-sql', 'e2e', '--instance=arango-spanner',
                        f'--sql=SELECT {cols}', '--format=json'], capture_output=True, text=True,
                       env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni'))
    if r.returncode:
        raise SystemExit(r.stderr[-500:])
    row = json.loads(r.stdout)['rows'][0]
    return [v in (True, 'True', 'true') for v in row]


res = {}
for name, f in forms.items():
    texts = [f(d) for d in ds]
    acc = run(texts)
    res[name] = acc
    print(f'{name:9s} accepted {sum(acc)}/{len(acc)}', flush=True)
# value-preserving check + examples where shortest fails
for i, d in enumerate(ds):
    if not res['shortest'][i]:
        print('shortest REJECT', repr(d), 'g17', '%.17g' % d, 'g17-ok', res['g17'][i],
              'g17-same-value', float('%.17g' % d) == d)
        if i > 60:
            break
