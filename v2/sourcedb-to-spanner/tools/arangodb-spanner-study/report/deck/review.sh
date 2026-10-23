#!/usr/bin/env bash
# Usage: review.sh <presentationId> <outdir>  - download every slide thumbnail and build two montages
# (<outdir>/montage-1.png = slides 1-12, montage-2.png = slides 13+) for a quick visual check.
set -euo pipefail
PID=$1; OUT=$2
HERE=$(cd "$(dirname "$0")" && pwd)
rm -rf "$OUT/th"; mkdir -p "$OUT"
python3 "$HERE/thumbs.py" "$PID" "$OUT/th" >/dev/null
mapfile -t F < <(ls "$OUT"/th/*.png | sort)
montage -geometry 800x450+4+4 -tile 3x "${F[@]:0:12}" "$OUT/montage-1.png"
[ ${#F[@]} -gt 12 ] && montage -geometry 800x450+4+4 -tile 3x "${F[@]:12}" "$OUT/montage-2.png"
echo "${#F[@]} slides -> $OUT/montage-1.png $OUT/montage-2.png"
