#!/usr/bin/env bash
# Builds everything (including the Arrow/Parquet parts) and runs the full
# test suite inside the fluxingest-dev image.
#   scripts/docker_build.sh            # Release build + tests
#   scripts/docker_build.sh asan       # ASan + UBSan
#   scripts/docker_build.sh tsan       # ThreadSanitizer
set -euo pipefail
cd "$(dirname "$0")/.."
PRESET="${1:-release}"
docker image inspect fluxingest-dev >/dev/null 2>&1 || docker build -t fluxingest-dev docker/
# --tmpfs /small: a 1 MiB filesystem for the real disk-full test.
docker run --rm -t \
  -v "$PWD":/src -w /src \
  --tmpfs /small:size=1m -e FLUX_SMALL_DIR=/small \
  fluxingest-dev bash -c "
    set -e
    cmake -S . -B build/docker-$PRESET -G Ninja \
      -DCMAKE_BUILD_TYPE=\$( [ $PRESET = release ] && echo Release || echo RelWithDebInfo ) \
      -DFLUX_SANITIZE=\$( case $PRESET in asan) echo address;; tsan) echo thread;; *) echo '';; esac ) >/dev/null
    cmake --build build/docker-$PRESET
    ctest --test-dir build/docker-$PRESET --output-on-failure
  "
