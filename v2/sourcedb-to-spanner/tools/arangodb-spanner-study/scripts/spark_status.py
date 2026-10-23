#!/usr/bin/env python3
"""Run on arango-dataproc-m: summarize the running Spark app (active tasks, executors, bytes) via the YARN proxy."""
import json, urllib.request

RM = 'http://localhost:8088'
get = lambda u: json.load(urllib.request.urlopen(u, timeout=30))
apps = get(f'{RM}/ws/v1/cluster/apps?states=RUNNING')['apps'] or {'app': []}
for app in apps['app']:
    base = f"{RM}/proxy/{app['id']}/api/v1/applications/{app['id']}"
    try:
        stages = get(f'{base}/stages?status=active')
        ex = get(f'{base}/executors')
        done = get(f'{base}/stages?status=complete')
    except Exception as e:  # noqa: BLE001
        print(app['id'], app['name'], 'no spark api:', e)
        continue
    print(app['id'], app['name'],
          f"executors={len(ex) - 1} activeTasks={sum(s['numActiveTasks'] for s in stages)}",
          f"activeStages={len(stages)} completedStages={len(done)}",
          f"completedTasks={sum(s['numCompleteTasks'] for s in stages) + sum(s['numCompleteTasks'] for s in done)}",
          f"outputGB={(sum(s['outputBytes'] for s in stages) + sum(s['outputBytes'] for s in done)) / 1e9:.1f}",
          f"inputRecords(M)={(sum(s.get('outputRecords', 0) for s in stages) + sum(s.get('outputRecords', 0) for s in done)) / 1e6:.1f}")
