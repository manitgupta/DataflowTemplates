#!/usr/bin/env python3
"""Print recent Spanner instance CPU utilization (high-priority, smoothed) from Cloud Monitoring REST API.

Usage: spanner_cpu.py <instance> [minutes]
"""
import urllib.error
import json, os, subprocess, sys, time, urllib.parse, urllib.request

inst = sys.argv[1]
mins = int(sys.argv[2]) if len(sys.argv) > 2 else 10
tok = subprocess.run(['gcloud', 'auth', 'print-access-token'], capture_output=True, text=True,
                     env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni')).stdout.strip()
now = time.time()
fmt = lambda t: time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t))
for metric in ['spanner.googleapis.com/instance/cpu/utilization', 'spanner.googleapis.com/instance/storage/used_bytes']:
    q = urllib.parse.urlencode({
        'filter': f'metric.type="{metric}" AND resource.labels.instance_id="{inst}"',
        'interval.startTime': fmt(now - mins * 60), 'interval.endTime': fmt(now)})
    req = urllib.request.Request(f'https://monitoring.googleapis.com/v3/projects/terraform-demo-host/timeSeries?{q}',
                                 headers={'Authorization': f'Bearer {tok}', 'x-goog-user-project': 'terraform-demo-host'})
    try:
        resp = json.load(urllib.request.urlopen(req))
    except urllib.error.HTTPError as e:
        print("HTTP", e.code, e.read()[:300]); continue
    for ts in resp.get('timeSeries', []):
        pts = ts['points'][:3]
        vals = [p['value'].get('doubleValue', p['value'].get('int64Value')) for p in pts]
        print(metric.split('/')[-2] + '/' + metric.split('/')[-1], ts['metric'].get('labels', {}), vals)
