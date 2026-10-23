#!/usr/bin/env python3
"""Cloud Spanner instance CPU utilization (max/avg) over a UTC time window, via the Monitoring REST API.

Usage (on a VM with the SA): spanner_cpu_window.py <instance> <start ISO> <end ISO>
"""
import json, subprocess, sys, urllib.parse, urllib.request

inst, start, end = sys.argv[1:4]
tok = subprocess.run(['gcloud', 'auth', 'print-access-token'], capture_output=True, text=True).stdout.strip()
q = urllib.parse.urlencode({
    'filter': f'metric.type="spanner.googleapis.com/instance/cpu/utilization" AND resource.labels.instance_id="{inst}"',
    'interval.startTime': start, 'interval.endTime': end,
    'aggregation.alignmentPeriod': '60s', 'aggregation.perSeriesAligner': 'ALIGN_MEAN',
    'aggregation.crossSeriesReducer': 'REDUCE_SUM'})
req = urllib.request.Request(f'https://monitoring.googleapis.com/v3/projects/terraform-demo-host/timeSeries?{q}',
                             headers={'Authorization': f'Bearer {tok}'})
pts = [p['value']['doubleValue'] for ts in json.load(urllib.request.urlopen(req)).get('timeSeries', []) for p in ts['points']]
print(f'points={len(pts)} max={max(pts):.3f} avg={sum(pts)/len(pts):.3f}' if pts else 'no data')
