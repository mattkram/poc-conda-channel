#!/usr/bin/env bash
# mirror_loop.sh — keep mirroring 20 packages per channel until 0 upload errors.
# Stops when every channel either has 0 errors or has nothing new to upload.
# Usage: bash scripts/mirror_loop.sh [count_per_batch]
set -euo pipefail

COUNT=${1:-20}
BATCH=0
CHANNELS=(
  "mattkram/pkgs-main|https://repo.anaconda.com/pkgs/main"
  "mattkram/pkgs-r|https://repo.anaconda.com/pkgs/r"
  "mattkram/conda-forge|https://conda.anaconda.org/conda-forge"
  "mattkram/bioconda|https://conda.anaconda.org/bioconda"
  "mattkram/pytorch|https://conda.anaconda.org/pytorch"
)

while true; do
  BATCH=$((BATCH + 1))
  echo ""
  echo "========================================"
  echo "  Batch $BATCH — $(date '+%H:%M:%S')"
  echo "========================================"

  PIDS=()
  STATS=()

  # Launch all channels in parallel
  for entry in "${CHANNELS[@]}"; do
    CHAN="${entry%%|*}"
    SRC="${entry##*|}"
    SLUG="${CHAN//\//-}"
    STATFILE="scripts/stats-loop-batch${BATCH}-${SLUG}.json"
    STATS+=("$STATFILE")

    python3 scripts/mirror.py \
      --channel "$CHAN" \
      --source "$SRC" \
      --subdirs noarch \
      --count "$COUNT" \
      --workers 4 \
      --no-wait \
      --stats "$STATFILE" &
    PIDS+=($!)
  done

  # Wait for all
  for pid in "${PIDS[@]}"; do
    wait "$pid" || true
  done

  # Summarise results
  TOTAL_OK=0
  TOTAL_ERR=0
  TOTAL_NEW=0

  for f in "${STATS[@]}"; do
    if [[ ! -f "$f" ]]; then
      echo "  WARNING: stats file missing: $f"
      continue
    fi
    read -r ch ok err new_pkgs < <(python3 - "$f" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
r = d["run"]
print(r["channel"], r["n_ok"], r["n_err"], r["n_selected"])
EOF
    )
    echo "  $ch: $ok/$new_pkgs ok, $err errors"
    for errline in $(python3 - "$f" <<'EOF'
import json, sys
d = json.load(open(sys.argv[1]))
for p in d["packages"]:
    if p["error"]:
        print(f"    ERROR {p['filename']}: {p['error']}")
EOF
    ); do
      echo "  $errline"
    done
    TOTAL_OK=$((TOTAL_OK + ok))
    TOTAL_ERR=$((TOTAL_ERR + err))
    TOTAL_NEW=$((TOTAL_NEW + new_pkgs))
  done

  echo ""
  echo "  Batch $BATCH total: $TOTAL_OK/$TOTAL_NEW uploaded, $TOTAL_ERR errors"

  if [[ $TOTAL_ERR -eq 0 && $TOTAL_NEW -eq 0 ]]; then
    echo "  Nothing new to upload across all channels. Done."
    break
  fi

  if [[ $TOTAL_ERR -eq 0 ]]; then
    echo "  Zero errors! Continuing to next batch..."
  else
    echo "  $TOTAL_ERR error(s) — retrying next batch..."
  fi
done

echo ""
echo "All done after $BATCH batch(es)."
