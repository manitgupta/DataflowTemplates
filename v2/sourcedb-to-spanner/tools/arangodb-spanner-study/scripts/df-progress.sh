#!/usr/bin/env bash
# Usage: df-progress.sh <jobId> [region]  - job state + key user counters (rows written / failed).
set -euo pipefail
export CLOUDSDK_ACTIVE_CONFIG_NAME=omni
J=$1; R=${2:-us-east1}
echo "state=$(gcloud dataflow jobs describe $J --region $R --format='value(currentState)')"
gcloud beta dataflow metrics list $J --region $R --source=user --format=json 2>/dev/null | python3 -c "
import json,sys
agg={}
for m in json.load(sys.stdin):
  n=m['name']['name']; v=m.get('scalar')
  if v is None or m['name'].get('context',{}).get('tentative')=='true': continue
  if any(k in n.lower() for k in ['success','fail','received','rows_read','completed','mismatch','dlq','error']):
    agg[n]=max(agg.get(n,0),int(v))
for k in sorted(agg): print(f'{k}={agg[k]:,}')"
