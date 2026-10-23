#!/usr/bin/env python3
"""Print ElementCount of Dataflow PCollections whose name contains a substring. Usage: df_elements.py <region> <job> <substr>"""
import json, os, subprocess, sys

region, job, sub = sys.argv[1:4]
r = subprocess.run(['gcloud', 'beta', 'dataflow', 'metrics', 'list', job, '--region', region, '--source=service', '--format=json'],
                   capture_output=True, text=True, env=dict(os.environ, CLOUDSDK_ACTIVE_CONFIG_NAME='omni'))
for m in json.loads(r.stdout or '[]'):
    n = m['name']
    ctx = n.get('context', {})
    if n['name'] == 'ElementCount' and ctx.get('tentative') != 'true' and sub in ctx.get('original_name', ''):
        print(f"{int(m.get('scalar', 0)):>14,}  {ctx.get('original_name', '')[:150]}")
