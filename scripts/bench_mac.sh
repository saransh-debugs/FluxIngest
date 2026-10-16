#!/usr/bin/env bash
# Runs the Mac benchmarks one at a time, only while the CPU is not
# thermally throttled. Laptops throttle within a minute of full load, and a
# throttled run can be 3-4x slower, so a result only counts if
# CPU_Speed_Limit was 100 both before and after it.
#   scripts/bench_mac.sh   (needs: cmake --preset release && cmake --build --preset release)
set -euo pipefail
cd "$(dirname "$0")/.."
B=build/release
OUT=bench/results/mac
mkdir -p "$OUT"

speed() { pmset -g therm | awk '/CPU_Speed_Limit/{print $3}'; }
cool() { until [ "$(speed)" = "100" ]; do sleep 10; done; sleep 20; }

# run NAME CMD... : retries until a run starts and ends unthrottled.
run() {
  local name=$1; shift
  for attempt in 1 2 3 4 5 6; do
    cool
    "$@" > "$OUT/$name.tmp" 2>&1
    if [ "$(speed)" = "100" ]; then
      mv "$OUT/$name.tmp" "$OUT/$name.txt"
      echo "ok   $name"
      return
    fi
    echo "     $name throttled during run, retrying ($attempt)"
  done
  echo "FAIL $name: never got an unthrottled run"; exit 1
}

GB="--benchmark_repetitions=3 --benchmark_report_aggregates_only=true"
for f in 'Transfer/Spsc' 'Transfer/Mutex' 'PushFull' 'PushNotFull' 'Overloaded'; do
  run "ring-${f//\//_}" $B/bench_ring --benchmark_filter="$f" $GB
done
for f in 'ParseLine' 'Hash' 'InternHit' 'IngestLine'; do
  run "parse-$f" $B/bench_parse --benchmark_filter="$f" $GB
done
for pol in drop-oldest drop-newest; do
  for s in 1000 100000; do
    run "pipeline-$pol-$s" $B/bench_pipeline --seconds=3 --series=$s --overflow=$pol
  done
done

# Merge into the two summary files the README points at.
{ for f in "$OUT"/ring-*.txt; do grep median "$f"; done; } > "$OUT/bench_ring.txt"
{ for f in "$OUT"/parse-*.txt; do grep median "$f"; done; } > "$OUT/bench_parse.txt"
{ for f in "$OUT"/pipeline-*.txt; do cat "$f"; done; } > "$OUT/bench_pipeline.txt"
rm -f "$OUT"/ring-*.txt "$OUT"/parse-*.txt "$OUT"/pipeline-*.txt "$OUT"/*.json
echo "done: $OUT/bench_{ring,parse,pipeline}.txt"
