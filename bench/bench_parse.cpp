// Producer-side micro-benchmarks: parse, hash, intern, and the full
// line -> ring path.

#include <benchmark/benchmark.h>

#include <string>
#include <vector>

#include "flux/ingestor.hpp"
#include "flux/spsc_ring.hpp"

namespace flux {
namespace {

const std::string kLine =
    "cpu,host=server-042,region=us-west-2,service=api usage_user=12.5,usage_system=3.25,idle=84i "
    "1700000000000000000";

void BM_ParseLine(benchmark::State& state) {
  ParsedLine p;
  for (auto _ : state) {
    benchmark::DoNotOptimize(parse_line(kLine, p));
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(kLine.size()));
}
BENCHMARK(BM_ParseLine);

void BM_Hash(benchmark::State& state) {
  const std::string s = "host=server-042,region=us-west-2,service=api";
  for (auto _ : state) benchmark::DoNotOptimize(hash_bytes(s, 12345));
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_Hash);

void BM_InternHit(benchmark::State& state) {
  const auto n = static_cast<std::uint32_t>(state.range(0));
  StringInterner in(n, std::size_t{n} * 64, 7);
  std::vector<std::string> keys;
  for (std::uint32_t i = 0; i < n; ++i) keys.push_back("host=server-" + std::to_string(i) + ",region=us-west-2");
  for (auto& k : keys) (void)in.intern(k);
  std::size_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(in.intern(keys[i]));
    if (++i == keys.size()) i = 0;
  }
  state.SetItemsProcessed(state.iterations());
}
// 1k fits in L1/L2; 1M doesn't fit in L3, so each lookup is a cache miss.
BENCHMARK(BM_InternHit)->Arg(1'000)->Arg(1'000'000);

// Full producer path for one line with 3 fields: parse, canonicalize tags,
// intern, 3 pushes. The ring is drained on the same thread every 256 lines.
template <Overflow P>
void BM_IngestLine(benchmark::State& state) {
  using Ring = SpscRing<Sample, P>;
  StringInterner metrics(1024, 1 << 16, 1);
  StringInterner tags(1024, 1 << 16, 2);
  Ring ring(4096);
  ProducerStats stats;
  LineIngestor<Ring> ing(metrics, tags, ring, stats);
  std::vector<Sample> out(4096);
  std::uint64_t n = 0;
  for (auto _ : state) {
    ing.ingest_line(kLine, 0);
    if ((++n & 255) == 0) ring.pop(out.data(), out.size());
  }
  state.SetItemsProcessed(state.iterations());
  state.counters["samples/s"] =
      benchmark::Counter(static_cast<double>(state.iterations() * 3), benchmark::Counter::kIsRate);
}
BENCHMARK(BM_IngestLine<Overflow::kDropNewest>)->Name("IngestLine/DropNewest");
BENCHMARK(BM_IngestLine<Overflow::kDropOldest>)->Name("IngestLine/DropOldest");

}  // namespace
}  // namespace flux

BENCHMARK_MAIN();
