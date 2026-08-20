#!/usr/bin/env bash
# seed-local-db.sh — Populate local D1 with representative fake data for UI development.
# Run once after `wrangler dev` has initialised the local state directory.
#
# Usage:
#   bash scripts/seed-local-db.sh          # seed everything
#   bash scripts/seed-local-db.sh --reset  # drop + re-seed (destructive)
#
# Requires wrangler to be on PATH (npx wrangler works too).

set -euo pipefail

WRANGLER="${WRANGLER:-npx wrangler}"
DB="conda-channel-meta"
LOCAL_FLAG="--local"

run() {
  $WRANGLER d1 execute "$DB" $LOCAL_FLAG --command "$1" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Optional reset
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--reset" ]]; then
  echo "Resetting local D1..."
  run "DELETE FROM trusted_publishers;"
  run "DELETE FROM packages;"
  run "DELETE FROM channels;"
  echo "Reset done."
fi

# ---------------------------------------------------------------------------
# Apply migrations (idempotent — IF NOT EXISTS on all DDL)
# ---------------------------------------------------------------------------
echo "Applying migrations..."
$WRANGLER d1 execute "$DB" $LOCAL_FLAG --file migrations/0001_initial.sql 2>/dev/null || true
$WRANGLER d1 execute "$DB" $LOCAL_FLAG --file migrations/0002_trusted_publishers.sql 2>/dev/null || true

# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------
echo "Seeding channels..."

run "INSERT OR IGNORE INTO channels (name, owner, visibility, created_at) VALUES
  ('mattkram/main',                    'mattkram', 'public',  unixepoch()*1000),
  ('mattkram/dev',                     'mattkram', 'public',  unixepoch()*1000),
  ('mattkram/private-channel',         'mattkram', 'private', unixepoch()*1000),
  ('mattkram/trusted-publishing-test', 'mattkram', 'public',  unixepoch()*1000),
  ('anaconda/pkgs-main',               'anaconda', 'public',  unixepoch()*1000),
  ('conda-forge/main',                 'conda-forge', 'public', unixepoch()*1000),
  ('bioconda/main',                    'bioconda', 'public',  unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — mattkram/main (general scientific stack)
# ---------------------------------------------------------------------------
echo "Seeding packages for mattkram/main..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('mattkram/main','numpy','2.1.3',
   'Fundamental package for array computing with Python',
   'BSD-3-Clause','https://numpy.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','pandas','2.2.3',
   'Powerful data structures for data analysis, time series, and statistics',
   'BSD-3-Clause','https://pandas.pydata.org',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','scipy','1.14.1',
   'Scientific library for Python',
   'BSD-3-Clause','https://scipy.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','matplotlib','3.9.2',
   'Publication quality figures in Python',
   'PSF','https://matplotlib.org',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','scikit-learn','1.5.2',
   'A set of python modules for machine learning and data mining',
   'BSD-3-Clause','https://scikit-learn.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','pytorch','2.4.1',
   'PyTorch is an optimized tensor library for deep learning',
   'BSD-3-Clause','https://pytorch.org',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','tensorflow','2.17.0',
   'TensorFlow is an end-to-end open source platform for machine learning',
   'Apache-2.0','https://tensorflow.org',
   '[\"linux-64\",\"osx-64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','requests','2.32.3',
   'Python HTTP for Humans',
   'Apache-2.0','https://requests.readthedocs.io',
   '[\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','flask','3.0.3',
   'A simple framework for building complex web applications',
   'BSD-3-Clause','https://flask.palletsprojects.com',
   '[\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','fastapi','0.115.0',
   'FastAPI framework, high performance, easy to learn, fast to code, ready for production',
   'MIT','https://fastapi.tiangolo.com',
   '[\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','sqlalchemy','2.0.36',
   'Database Abstraction Library',
   'MIT','https://www.sqlalchemy.org',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','pydantic','2.9.2',
   'Data validation using Python type hints',
   'MIT','https://docs.pydantic.dev',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/main','pillow','10.4.0',
   'Pillow is the friendly PIL fork by Alex Clark and Contributors',
   'HPND','https://python-pillow.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','cryptography','43.0.1',
   'cryptography is a package designed to expose cryptographic primitives and recipes to Python developers',
   'Apache-2.0 OR BSD-3-Clause','https://cryptography.io',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('mattkram/main','boto3','1.35.36',
   'The AWS SDK for Python',
   'Apache-2.0','https://aws.amazon.com/sdk-for-python',
   '[\"noarch\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — mattkram/dev (pre-release / dev builds)
# ---------------------------------------------------------------------------
echo "Seeding packages for mattkram/dev..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('mattkram/dev','numpy','2.2.0.dev0',
   'Fundamental package for array computing with Python (dev build)',
   'BSD-3-Clause','https://numpy.org',
   '[\"linux-64\",\"osx-arm64\"]',
   unixepoch()*1000),
  ('mattkram/dev','my-internal-tool','0.4.2',
   'Internal data processing utility',
   'Proprietary',NULL,
   '[\"linux-64\",\"osx-arm64\"]',
   unixepoch()*1000),
  ('mattkram/dev','conda-wit-client','0.1.0',
   'CLI client for conda-wit channel server',
   'MIT','https://github.com/mattkram/conda-wit',
   '[\"noarch\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — mattkram/private-channel (private)
# ---------------------------------------------------------------------------
echo "Seeding packages for mattkram/private-channel..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('mattkram/private-channel','ca-certificates','2026.7.22',
   'Certificates for use with other packages',
   'ISC',NULL,
   '[\"noarch\"]',
   unixepoch()*1000),
  ('mattkram/private-channel','tzdata','2024a',
   'Timezone data for use with other packages',
   'LicenseRef-Public-Domain',NULL,
   '[\"noarch\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — mattkram/trusted-publishing-test
# ---------------------------------------------------------------------------
echo "Seeding packages for mattkram/trusted-publishing-test..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('mattkram/trusted-publishing-test','test-trusted-publishing','0.1.11',
   'Minimal test package for trusted publishing CI',
   NULL,'https://github.com/mattkram/poc-conda-channel',
   '[\"noarch\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — anaconda/pkgs-main (representative subset, multiple subdirs)
# ---------------------------------------------------------------------------
echo "Seeding packages for anaconda/pkgs-main..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('anaconda/pkgs-main','python','3.12.7',
   'General purpose programming language',
   'PSF-2.0','https://www.python.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','conda','24.9.2',
   'OS-agnostic, system-level binary package manager and ecosystem',
   'BSD-3-Clause','https://conda.io',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','numpy','1.26.4',
   'Fundamental package for array computing with Python',
   'BSD-3-Clause','https://numpy.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','pandas','2.1.4',
   'Powerful data structures for data analysis, time series, and statistics',
   'BSD-3-Clause','https://pandas.pydata.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','scipy','1.13.1',
   'Scientific library for Python',
   'BSD-3-Clause','https://scipy.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','openssl','3.3.2',
   'OpenSSL is an open-source implementation of the SSL and TLS protocols',
   'Apache-2.0','https://www.openssl.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','ca-certificates','2024.9.24',
   'Certificates for use with other packages',
   'ISC','https://certifi.io',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','pip','24.2',
   'The PyPA recommended tool for installing Python packages',
   'MIT','https://pip.pypa.io',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\",\"noarch\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','setuptools','75.1.0',
   'Download, build, install, upgrade, and uninstall Python packages',
   'MIT','https://github.com/pypa/setuptools',
   '[\"noarch\"]',
   unixepoch()*1000),
  ('anaconda/pkgs-main','zlib','1.2.13',
   'Data compression library',
   'Zlib','https://zlib.net',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — conda-forge/main
# ---------------------------------------------------------------------------
echo "Seeding packages for conda-forge/main..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('conda-forge/main','polars','1.9.0',
   'Dataframes powered by a multithreaded, vectorized query engine, written in Rust',
   'MIT','https://www.pola.rs',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('conda-forge/main','rust','1.81.0',
   'A systems programming language focused on safety, speed and concurrency',
   'MIT OR Apache-2.0','https://www.rust-lang.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('conda-forge/main','ripgrep','14.1.1',
   'ripgrep recursively searches directories for a regex pattern',
   'MIT','https://github.com/BurntSushi/ripgrep',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\",\"win-64\"]',
   unixepoch()*1000),
  ('conda-forge/main','hypothesis','6.112.2',
   'A library for property-based testing',
   'MPL-2.0','https://hypothesis.works',
   '[\"noarch\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Packages — bioconda/main
# ---------------------------------------------------------------------------
echo "Seeding packages for bioconda/main..."

run "INSERT OR IGNORE INTO packages (channel, name, version, summary, license, home, subdirs, updated_at) VALUES
  ('bioconda/main','samtools','1.21',
   'Tools for manipulating next-generation sequencing data',
   'MIT','https://www.htslib.org',
   '[\"linux-64\",\"linux-aarch64\",\"osx-64\",\"osx-arm64\"]',
   unixepoch()*1000),
  ('bioconda/main','bwa','0.7.18',
   'Burrow-Wheeler Aligner for short-read alignment',
   'GPL-3.0','http://bio-bwa.sourceforge.net',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\"]',
   unixepoch()*1000),
  ('bioconda/main','bowtie2','2.5.4',
   'Fast and sensitive read alignment',
   'GPL-3.0','http://bowtie-bio.sourceforge.net/bowtie2',
   '[\"linux-64\",\"osx-64\",\"osx-arm64\"]',
   unixepoch()*1000);"

# ---------------------------------------------------------------------------
# Trusted publisher rules
# ---------------------------------------------------------------------------
echo "Seeding trusted publisher rules..."

run "INSERT OR IGNORE INTO trusted_publishers
  (channel, repository, workflow, environment, package_name, require_trusted, created_at, created_by)
  VALUES
  ('mattkram/trusted-publishing-test',
   'mattkram/poc-conda-channel',
   'mattkram/poc-conda-channel/.github/workflows/trusted-publish.yml',
   NULL, 'test-trusted-publishing', 1, unixepoch()*1000, 'mattkram'),
  ('mattkram/trusted-publishing-test',
   'mattkram/poc-conda-channel',
   'mattkram/poc-conda-channel/.github/workflows/trusted-publish-wrong-pkg.yml',
   NULL, 'test-trusted-publishing', 1, unixepoch()*1000, 'mattkram');"

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
echo ""
echo "Local DB seeded successfully."
echo ""
echo "  Channels:  $(npx wrangler d1 execute $DB $LOCAL_FLAG --command 'SELECT COUNT(*) as n FROM channels;' 2>/dev/null | grep -o '[0-9]*' | tail -1)"
echo "  Packages:  $(npx wrangler d1 execute $DB $LOCAL_FLAG --command 'SELECT COUNT(*) as n FROM packages;' 2>/dev/null | grep -o '[0-9]*' | tail -1)"
echo "  TP rules:  $(npx wrangler d1 execute $DB $LOCAL_FLAG --command 'SELECT COUNT(*) as n FROM trusted_publishers;' 2>/dev/null | grep -o '[0-9]*' | tail -1)"
echo ""
echo "Run: npm run dev"
echo "Open: http://localhost:8787"
