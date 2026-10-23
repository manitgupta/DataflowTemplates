#!/usr/bin/env bash
# Usage: flink-progress.sh <deployment>  - job state, elapsed, and records into the Spanner write / read vertices.
export PATH=~/bin:$PATH KUBECONFIG=${KUBECONFIG:-$HOME/.kube/arango-flink.config}
P=$(kubectl -n flink get pods -o name | grep "$1" | grep -v taskmanager | head -1)
J=$(kubectl -n flink exec $P -- curl -s localhost:8081/jobs 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin)['jobs'][0]['id'])" 2>/dev/null)
[ -z "$J" ] && { echo "no job yet ($(kubectl -n flink get flinkdeployment $1 -o jsonpath='{.status.jobStatus.state}'))"; exit 0; }
kubectl -n flink exec $P -- curl -s localhost:8081/jobs/$J 2>/dev/null | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('state=%s elapsed=%ds vertices=%d' % (d['state'], d['duration']//1000, len(d['vertices'])))
for v in d['vertices']:
  n=v['name']; m=v['metrics']
  if any(k in n for k in ('WriteToSpanner','OmniSpannerWrite','Parse Files')):
    print('  %-8s p=%d in=%s out=%s %s' % (v['status'][:8], v['parallelism'], m.get('read-records'), m.get('write-records'), n[:110]))"
