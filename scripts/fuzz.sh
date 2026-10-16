#!/usr/bin/env bash
# Fuzzes the line protocol parser with libFuzzer + ASan + UBSan in Docker.
#   scripts/fuzz.sh [seconds]   (default 600)
set -euo pipefail
cd "$(dirname "$0")/.."
SECS="${1:-600}"
docker run --rm -v "$PWD":/src -w /src fluxingest-dev bash -c "
  set -e
  CXX=clang++ cmake -S . -B build/fuzz -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DFLUX_BUILD_FUZZ=ON -DFLUX_BUILD_TESTS=OFF -DFLUX_BUILD_BENCH=OFF >/dev/null
  cmake --build build/fuzz --target fuzz_line_protocol
  mkdir -p build/fuzz/corpus
  ./build/fuzz/fuzz_line_protocol build/fuzz/corpus fuzz/corpus \
    -max_total_time=$SECS -max_len=4096 -print_final_stats=1
"
