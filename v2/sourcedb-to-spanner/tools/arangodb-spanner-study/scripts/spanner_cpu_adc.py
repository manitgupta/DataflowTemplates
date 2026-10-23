#!/usr/bin/env python3
"""Cloud Spanner instance CPU utilization over a time window, via Application Default Credentials.

Usage: spanner_cpu_adc.py <instance> <start ISO8601 UTC> <end ISO8601 UTC>   -> avg / p95 / peak (1-min points)
       spanner_cpu_adc.py gce:<vm name prefix> <start> <end>   -> mean GCE VM CPU across matching VMs (Spanner Omni)
       spanner_cpu_adc.py storage:<instance> <start> <end>    -> Cloud Spanner storage used (TB, last point)
"""
import sys

import google.auth
from google.auth.transport.requests import AuthorizedSession

inst, start, end = sys.argv[1:4]
creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/monitoring.read'])
s = AuthorizedSession(creds)
if inst.startswith('gce:'):
    flt = (f'metric.type="compute.googleapis.com/instance/cpu/utilization" '
           f'AND metric.labels.instance_name=starts_with("{inst[4:]}")')
    reducer, group = 'REDUCE_MEAN', 'resource.labels.project_id'
elif inst.startswith('storage:'):
    flt = (f'metric.type="spanner.googleapis.com/instance/storage/used_bytes" '
           f'AND resource.labels.instance_id="{inst[8:]}"')
    reducer, group = 'REDUCE_SUM', 'resource.labels.instance_id'
else:
    flt = (f'metric.type="spanner.googleapis.com/instance/cpu/utilization" '
           f'AND resource.labels.instance_id="{inst}"')
    reducer, group = 'REDUCE_SUM', 'resource.labels.instance_id'
params = {'filter': flt,
          'interval.startTime': start, 'interval.endTime': end,
          'aggregation.alignmentPeriod': '60s', 'aggregation.perSeriesAligner': 'ALIGN_MEAN',
          'aggregation.crossSeriesReducer': reducer, 'aggregation.groupByFields': group}
r = s.get('https://monitoring.googleapis.com/v3/projects/terraform-demo-host/timeSeries', params=params,
          headers={'x-goog-user-project': 'terraform-demo-host'})
r.raise_for_status()
pts = [p for ts in r.json().get('timeSeries', []) for p in ts['points']]
if inst.startswith('storage:'):
    v = pts[0]['value']  # newest point first
    print(f"{inst} at {pts[0]['interval']['endTime']}: {float(v.get('doubleValue', v.get('int64Value', 0))) / 1e12:.2f} TB")
    sys.exit(0)
vals = sorted(p['value']['doubleValue'] for p in pts)
if not vals:
    sys.exit('no points')
print(f'{inst} {start}..{end} points={len(vals)} avg={100 * sum(vals) / len(vals):.1f}% '
      f'p95={100 * vals[int(0.95 * (len(vals) - 1))]:.1f}% peak={100 * vals[-1]:.1f}%')
