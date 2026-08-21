#!/usr/bin/env bash
# set-secrets.sh
# Interactively set all app secrets in GitHub Actions (production environment)
# AND directly on the Cloudflare Worker via wrangler secret put.
# Run from the repo root: ./scripts/set-secrets.sh
#
# GitHub doesn't allow secrets prefixed with GITHUB_, so those are stored
# under GH_ names and mapped to the correct Cloudflare secret name on deploy.
#
# Format: "GH_SECRET_NAME:CLOUDFLARE_SECRET_NAME"
# (single name = same in both places)

set -euo pipefail

REPO="mattkram/poc-conda-wit"
GH_ENV="production"

# Format: GH_NAME:CF_NAME  (or just NAME if both are the same)
SECRETS=(
  "GH_CLIENT_ID:GITHUB_CLIENT_ID"
  "GH_CLIENT_SECRET:GITHUB_CLIENT_SECRET"
  "GITHUB_ORG:GITHUB_ORG"
  "UPLOAD_TOKEN_SECRET:UPLOAD_TOKEN_SECRET"
  "INTERNAL_SECRET:INTERNAL_SECRET"
  "R2_ACCESS_KEY_ID:R2_ACCESS_KEY_ID"
  "R2_SECRET_ACCESS_KEY:R2_SECRET_ACCESS_KEY"
  "R2_ACCOUNT_ID:R2_ACCOUNT_ID"
)

echo "Setting secrets in:"
echo "  GitHub: ${REPO} / environment: ${GH_ENV}"
echo "  Cloudflare Worker: $(grep '^name' wrangler.toml | head -1 | cut -d'"' -f2)"
echo

for entry in "${SECRETS[@]}"; do
  gh_name="${entry%%:*}"
  cf_name="${entry##*:}"

  printf "Enter %s (input hidden): " "$cf_name"
  read -rs value
  echo
  if [[ -z "$value" ]]; then
    echo "  Skipped (empty)"
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
