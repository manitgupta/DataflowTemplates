#!/usr/bin/env python3
"""Summarize Spanner write error reasons of a Dataflow job from Cloud Logging.

Usage: df-errors.py <jobId> [filter-substring] [limit]
"""
import collections, json, os, re, subprocess, sys

job = sys.argv[1]
sub = sys.argv[2] if len(sys.argv) > 2 else 'Failed to write'
limit = sys.argv[3] if len(sys.argv) > 3 else '200'
flt = (f'resource.type="dataflow_step" AND resource.labels.job_id="{job}" AND severity>=WARNING '
       f'AND jsonPayload.message:"{sub}"')
out = subprocess.run(['gcloud', 'logging', 'read', flt, '--limit', limit, '--freshness=3h', '--format=json'],
                     capture_output=True, text=True, env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni')).stdout
c = collections.Counter()
for e in json.loads(out or '[]'):
    p = e.get('jsonPayload', {})
    x = str(p.get('exception', ''))
    table = re.search(r'insert_or_update\((\w+)\{', p.get('message', ''))
    m = re.search(r'(OUT_OF_RANGE|INVALID_ARGUMENT|FAILED_PRECONDITION)[^\n]*?: ([^\n]*?)( - RequestId|$)', x, re.M)
    reason = re.sub(r'[-0-9.e+]{6,}', 'N', m.group(2))[:160] if m else x[:160]
    c[(table.group(1) if table else '?', reason)] += 1
for (t, r), v in c.most_common():
    print(v, t, r)
