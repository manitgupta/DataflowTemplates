#!/usr/bin/env python3
"""Collect comparable metrics for Dataflow import jobs (duration, rows, rows/s, DLQ rows, vCPU-hours).

Usage: job_metrics.py <label>=<region>:<jobId> ...   -> prints a table and writes ../results/job-metrics.json
"""
import datetime as dt, json, os, subprocess, sys

ENV = dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni')


def gc(*args):
    r = subprocess.run(['gcloud', *args], capture_output=True, text=True, env=ENV)
    if r.returncode:
        raise SystemExit(r.stderr[-800:])
    return json.loads(r.stdout)


def ts(s):
    return dt.datetime.fromisoformat(s.replace('Z', '+00:00'))


out = {}
for arg in sys.argv[1:]:
    label, rest = arg.split('=', 1)
    region, job = rest.split(':', 1)
    d = gc('dataflow', 'jobs', 'describe', job, '--region', region, '--full', '--format=json')
    start = ts(d.get('startTime') or d['createTime'])
    end = ts(d['currentStateTime'])
    m = gc('beta', 'dataflow', 'metrics', 'list', job, '--region', region, '--format=json')
    vals = {}
    for x in m:
        n = x['name']['name']
        ctx = x['name'].get('context', {})
        if ctx.get('tentative') == 'true' or x.get('scalar') is None:
            continue
        vals[n] = max(vals.get(n, 0), float(x['scalar']))
    rows = vals.get('mutation_groups_write_success', 0)
    dlq = vals.get('failed_mutation_errors', 0)
    secs = (end - start).total_seconds()
    out[label] = {'job': job, 'region': region, 'state': d['currentState'], 'start': start.isoformat(),
                  'end': end.isoformat(), 'wall_s': secs, 'rows_written': rows, 'dlq_rows': dlq,
                  'rows_per_s': rows / secs if secs else 0, 'vcpu_hours': vals.get('TotalVcpuTime', 0) / 3600,
                  'workers': d.get('environment', {}).get('workerPools', [{}])[0].get('numWorkers'),
                  'machine': d.get('environment', {}).get('workerPools', [{}])[0].get('machineType')}
    o = out[label]
    print(f"{label:22s} {o['state']:16s} wall={secs/60:6.1f} min rows={rows:>14,.0f} dlq={dlq:>9,.0f} "
          f"rows/s={o['rows_per_s']:>10,.0f} vCPU-h={o['vcpu_hours']:7.1f} workers={o['workers']} {o['machine']}")
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'results', 'job-metrics.json')
prev = json.load(open(path)) if os.path.exists(path) else {}
prev.update(out)
json.dump(prev, open(path, 'w'), indent=1)
