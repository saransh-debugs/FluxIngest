#!/usr/bin/env bash
# Linux benchmarks inside the Docker image (Parquet sink, UDP + shards).
# Same rule as bench_mac.sh: a result counts only if the host CPU wasn't
# thermally throttled before or after (macOS host; skipped elsewhere).
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=bench/results/docker
mkdir -p "$OUT"
speed() { command -v pmset >/dev/null && pmset -g therm | awk '/CPU_Speed_Limit/{print $3}' || echo 100; }
cool() { until [ "$(speed)" = "100" ]; do sleep 10; done; sleep 20; }

docker run --rm -v "$PWD":/src -w /src fluxingest-dev bash -c \
  "cmake -S . -B build/docker-release -G Ninja -DCMAKE_BUILD_TYPE=Release >/dev/null && cmake --build build/docker-release >/dev/null"

: > "$OUT/results.txt"
for what in pipeline udp1 udp2; do
  for attempt in 1 2 3 4 5 6; do
    cool
    res=$(docker run --rm -v "$PWD":/src -w /src fluxingest-dev python3 scripts/bench_docker_inner.py build/docker-release $what)
    if [ "$(speed)" = "100" ]; then echo "$res" | tee -a "$OUT/results.txt"; break; fi
    echo "$what throttled, retrying ($attempt)"
  done
done
{ echo; echo "docker VM: $(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}} bytes'), host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"; } >> "$OUT/results.txt"
