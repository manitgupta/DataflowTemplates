#!/usr/bin/env bash
# Usage: rate.sh [interval_s]  - total doc count in shop10t across collections, sampled twice -> docs/s.
set -euo pipefail
I=${1:-120}
CMD='tot(){ t=0; for c in users products orders events follows purchased type_zoo; do n=$(curl -s -u root: http://10.30.1.10:8529/_db/shop10t/_api/collection/$c/count | python3 -c "import json,sys;print(json.load(sys.stdin)[\"count\"])"); echo -n "$c=$n " >&2; t=$((t+n)); done; echo >&2; echo $t; }; a=$(tot); sleep '$I'; b=$(tot); echo "total=$b rate=$(( (b-a)/'$I' )) docs/s"'
bash "$(dirname "$0")/../infra/ssh.sh" arango-ops "$CMD" 2>&1 | grep -v -i -e numpy -e tcp_upload -e '^WARNING' -e '^$'
