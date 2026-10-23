#!/usr/bin/env bash
# Usage: flink-exceptions.sh <deployment> [n=3]  - latest task failures (task + Caused-by chain) of the running job.
export PATH=~/bin:$PATH KUBECONFIG=${KUBECONFIG:-$HOME/.kube/arango-flink.config}
P=$(kubectl -n flink get pods -o name | grep "$1" | grep -v taskmanager | head -1)
J=$(kubectl -n flink exec $P -- curl -s localhost:8081/jobs | python3 -c "import json,sys;print(json.load(sys.stdin)['jobs'][0]['id'])")
kubectl -n flink exec $P -- curl -s localhost:8081/jobs/$J/exceptions | N=${2:-3} python3 -c "
import json,os,sys; d=json.load(sys.stdin)
for e in (d.get('exceptionHistory',{}).get('entries') or [])[:int(os.environ['N'])]:
  lines=[l for l in e.get('stacktrace','').splitlines() if 'Caused by' in l or 'Exception:' in l][:8]
  print(e.get('taskName','')[:120]); print('\n'.join(l[:300] for l in lines)); print('---')"
