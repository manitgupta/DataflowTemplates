#!/usr/bin/env python3
"""Find doubles whose shortest JSON text Spanner rejects, then test alternative forms for exactly those doubles.

Usage: probe_spanner_json3.py [num_candidates]
"""
import json, os, random, subprocess, sys

random.seed(11)
N = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
BATCH = 150


def accepted(texts):
    out = []
    for i in range(0, len(texts), BATCH):
        chunk = texts[i:i + BATCH]
        cols = ', '.join(f"SAFE.PARSE_JSON('[{t}]') IS NOT NULL AS c{j}" for j, t in enumerate(chunk))
        r = subprocess.run(['gcloud', 'spanner', 'databases', 'execute-sql', 'e2e', '--instance=arango-spanner',
                            f'--sql=SELECT {cols}', '--format=json'], capture_output=True, text=True,
                           env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni'))
        if r.returncode:
            raise SystemExit(r.stderr[-500:])
        out += [v in (True, 'True', 'true') for v in json.loads(r.stdout)['rows'][0]]
    return out


ds = []
for i in range(N):
    k = random.randrange(1000000)
    ds.append([k / 1e6, k / 1e6 * 180 - 90, k / 1e6 * 360 - 180, round(k / 1e6 * 1000, 2), random.random(),
               k / 1e6 * 1000][i % 6])
short = [repr(d) for d in ds]
acc = accepted(short)
rej = [d for d, a in zip(ds, acc) if not a]
print(f'shortest rejected for {len(rej)}/{N} doubles', flush=True)
if not rej:
    sys.exit()
forms = {'g17': lambda d: '%.17g' % d, 'g16': lambda d: '%.16g' % d, 'shortest+0': lambda d: repr(d) + '0',
         'e17': lambda d: '%.16e' % d}
for name, f in forms.items():
    texts = [f(d) for d in rej]
    a = accepted(texts)
    lossless = sum(float(t) == d for t, d in zip(texts, rej))
    print(f'{name:11s} accepted {sum(a)}/{len(rej)} lossless {lossless}/{len(rej)}', flush=True)
print('examples:', [repr(d) for d in rej[:12]])
