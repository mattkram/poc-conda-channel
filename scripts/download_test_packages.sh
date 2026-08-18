#!/usr/bin/env bash
# Download a handful of small conda-forge packages for end-to-end testing.
# Packages are saved under scripts/fixtures/<subdir>/<filename>.
#
# Usage:
#   bash scripts/download_test_packages.sh
#
# Packages chosen for being tiny and noarch (no OS-specific build needed):
#   - tzdata        ~120kB  noarch  (pure timezone data, no Python deps)
#   - ca-certificates ~150kB noarch  (root CA bundle)
#
# If you want a linux-64 package add one with --subdir linux-64 below.

set -euo pipefail

FIXTURES_DIR="$(dirname "$0")/fixtures"
CONDA_FORGE="https://conda.anaconda.org/conda-forge"

download_latest() {
    local subdir="$1"
    local pkg_name="$2"
    local out_dir="$FIXTURES_DIR/$subdir"

    mkdir -p "$out_dir"

    echo "Fetching repodata for $subdir..."
    local repodata
    repodata=$(curl -fsSL "$CONDA_FORGE/$subdir/repodata.json")

    # Find the most recent filename for this package (last entry wins when sorted by build number)
    local filename
    filename=$(echo "$repodata" | python3 -c "
import json, sys
data = json.load(sys.stdin)
pkgs = {k: v for k, v in data.get('packages.conda', {}).items() if v.get('name') == '$pkg_name'}
if not pkgs:
    pkgs = {k: v for k, v in data.get('packages', {}).items() if v.get('name') == '$pkg_name'}
if not pkgs:
    print('', end='')
    sys.exit(0)
# Pick highest version + build_number
best = sorted(pkgs.items(), key=lambda x: (x[1].get('version',''), x[1].get('build_number', 0)))[-1]
print(best[0])
")

    if [[ -z "$filename" ]]; then
        echo "  WARNING: $pkg_name not found in $subdir repodata, skipping."
        return
    fi

    local dest="$out_dir/$filename"
    if [[ -f "$dest" ]]; then
        echo "  Already downloaded: $dest"
        return
    fi

    echo "  Downloading $filename..."
    curl -fsSL -o "$dest" "$CONDA_FORGE/$subdir/$filename"
    echo "  Saved: $dest ($(du -sh "$dest" | cut -f1))"
}

download_latest noarch tzdata
download_latest noarch ca-certificates

echo ""
echo "Fixtures ready in $FIXTURES_DIR:"
find "$FIXTURES_DIR" -type f | sort
