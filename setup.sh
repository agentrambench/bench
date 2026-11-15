#!/bin/sh
# Install the pinned harness versions into ./harnesses: npm packages from package-lock.json, Goose release binary (sha256-checked).
set -e
cd "$(dirname "$0")/harnesses"
npm ci --no-fund --no-audit
if [ ! -x goose/goose ]; then
  mkdir -p goose
  curl -fsSL -o goose/goose.tgz https://github.com/aaif-goose/goose/releases/download/v1.59.1/goose-x86_64-unknown-linux-gnu.tar.gz
  echo "04c08900ab2ae78b8e2f556f1ed535a91b61c080a3ff62e9fd8b280cceb544ef  goose/goose.tgz" | sha256sum -c -
  tar xzf goose/goose.tgz -C goose
  rm goose/goose.tgz
fi
for c in node python3 tmux systemd-run; do command -v $c >/dev/null || { echo "missing: $c" >&2; exit 1; }; done
systemd-run --user --scope --quiet true || { echo "systemd user scopes unavailable (needed for per-harness cgroups)" >&2; exit 1; }
echo ok
