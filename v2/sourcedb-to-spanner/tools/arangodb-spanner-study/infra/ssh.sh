#!/usr/bin/env bash
# Usage: ssh.sh <vm> <command...>   (IAP tunnel, no external IPs)
set -euo pipefail
source "$(dirname "$0")/env.sh"
vm=$1; shift
gcloud compute ssh "$vm" --zone $ZONE --tunnel-through-iap --quiet --command "$*"
