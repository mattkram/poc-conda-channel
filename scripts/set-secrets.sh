#!/usr/bin/env bash
# set-secrets.sh
# Interactively set all app secrets in GitHub Actions (production environment)
# AND directly on the Cloudflare Worker via wrangler secret put.
# Reads defaults from .dev.vars if present.
# Run from the repo root: ./scripts/set-secrets.sh

set -euo pipefail

REPO="mattkram/poc-conda-wit"
GH_ENV="production"

# Format: GH_NAME:CF_NAME:DEVVARS_KEY
SECRETS=(
  "GH_CLIENT_ID:GITHUB_CLIENT_ID:GITHUB_CLIENT_ID"
  "GH_CLIENT_SECRET:GITHUB_CLIENT_SECRET:GITHUB_CLIENT_SECRET"
  "UPLOAD_TOKEN_SECRET:UPLOAD_TOKEN_SECRET:UPLOAD_TOKEN_SECRET"
  "INTERNAL_SECRET:INTERNAL_SECRET:INTERNAL_SECRET"
  "R2_ACCESS_KEY_ID:R2_ACCESS_KEY_ID:R2_ACCESS_KEY_ID"
  "R2_SECRET_ACCESS_KEY:R2_SECRET_ACCESS_KEY:R2_SECRET_ACCESS_KEY"
  "R2_ACCOUNT_ID:R2_ACCOUNT_ID:R2_ACCOUNT_ID"
)

# Load .dev.vars into an associative array
declare -A DEVVARS
if [[ -f .dev.vars ]]; then
  while IFS='=' read -r key val; do
    [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
    DEVVARS["$key"]="$val"
  done < .dev.vars
fi

echo "Setting secrets in:"
echo "  GitHub: ${REPO} / environment: ${GH_ENV}"
echo "  Cloudflare Worker: $(grep '^name' wrangler.toml | head -1 | cut -d'"' -f2)"
echo

for entry in "${SECRETS[@]}"; do
  gh_name="${entry%%:*}"
  rest="${entry#*:}"
  cf_name="${rest%%:*}"
  dv_key="${rest##*:}"

  default="${DEVVARS[$dv_key]:-}"
  if [[ -n "$default" ]]; then
    printf "Enter %s [found in .dev.vars, press Enter to use]: " "$gh_name"
  else
    printf "Enter %s (input hidden): " "$gh_name"
  fi
  read -rs value
  echo

  # Fall back to .dev.vars value if user just pressed Enter
  if [[ -z "$value" && -n "$default" ]]; then
    value="$default"
    echo "  Using value from .dev.vars"
  fi

  if [[ -z "$value" ]]; then
    echo "  Skipped (empty)"
    echo
    continue
  fi

  echo "  → GitHub (${GH_ENV}) as ${gh_name}..."
  echo "$value" | gh secret set "$gh_name" --repo "$REPO" --env "$GH_ENV"

  echo "  → Cloudflare Worker as ${cf_name}..."
  echo "$value" | npx wrangler secret put "$cf_name"

  echo "  ✓ done"
  echo
done

echo "All secrets set."
