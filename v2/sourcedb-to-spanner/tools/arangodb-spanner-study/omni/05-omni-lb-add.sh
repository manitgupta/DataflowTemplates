#!/usr/bin/env bash
# Usage: 05-omni-lb-add.sh  - add every arango-omni-* server (any us-central1 zone) to the Omni internal LB backend,
# one unmanaged instance group per zone (arango-omni-ig for the original zone, arango-omni-ig-<zone suffix> otherwise).
set -euo pipefail
source "$(dirname "$0")/../infra/env.sh"
g() { gcloud --project "$PROJECT" "$@"; }
for z in $(g compute instances list --filter='name~^arango-omni-' --format='value(zone.basename())' | sort -u); do
  ig=arango-omni-ig; [ "$z" = "$ZONE" ] || ig=arango-omni-ig-${z##*-}
  g compute instance-groups unmanaged describe $ig --zone $z >/dev/null 2>&1 ||
    g compute instance-groups unmanaged create $ig --zone $z >/dev/null
  have=$(g compute instance-groups unmanaged list-instances $ig --zone $z --format='value(instance.basename())' | sort)
  want=$(g compute instances list --filter="name~^arango-omni- AND zone:$z" --format='value(name)' | sort)
  add=$(comm -13 <(echo "$have") <(echo "$want") | paste -sd,)
  [ -z "$add" ] || g compute instance-groups unmanaged add-instances $ig --zone $z --instances=$add >/dev/null
  # attach after the group has instances (an empty group has no network)
  g compute backend-services describe arango-omni-bs --region $REGION --format='value(backends[].group)' | grep -q "/$z/instanceGroups/$ig" ||
    g compute backend-services add-backend arango-omni-bs --region $REGION --instance-group=$ig --instance-group-zone=$z >/dev/null
  echo "$z $ig +[$add]"
done
