#!/usr/bin/env bash
# Builds in Docker, then runs scripts/e2e.py against the real binary.
set -euo pipefail
cd "$(dirname "$0")/.."
docker image inspect fluxingest-dev >/dev/null 2>&1 || docker build -t fluxingest-dev docker/
docker run --rm -v "$PWD":/src -w /src \
  --tmpfs /small:size=1m -e FLUX_SMALL_DIR=/small \
  fluxingest-dev bash -c "
    set -e
    cmake -S . -B build/docker-release -G Ninja -DCMAKE_BUILD_TYPE=Release >/dev/null
    cmake --build build/docker-release --target fluxingest flux_loadgen
    python3 scripts/e2e.py build/docker-release
  "
