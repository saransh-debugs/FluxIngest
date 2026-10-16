// Whole-shard throughput, no network: the calling thread is the producer and
// feeds pre-built packets as fast as it can. Measures how many samples per
// second one shard (3 threads) can parse, batch, and hand to the sink, and
// where samples get dropped when something can't keep up.
//
//   bench_pipeline [--seconds=5] [--series=10000] [--sink=null|parquet] [--out=DIR]
//                  [--overflow=drop-oldest|drop-newest] [--rate=SAMPLES_PER_S]

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "flux/clock.hpp"
#include "pipeline.hpp"
#if defined(FLUX_WITH_ARROW)
#include "parquet_sink.hpp"
#endif

namespace {

using namespace flux;

// Discards rows but still reads every column and resolves the dictionaries,
// so the consumer->writer path does the same memory traffic as a real sink.
class NullSink final : public BatchSink {
 public:
  Status open(std::int64_t, std::uint64_t) override { return Status::ok(); }
  Status write(const Batch& b, const StringInterner& m, const StringInterner& t) override {
    std::uint64_t acc = 0;
    for (std::uint32_t i = 0; i < b.rows; ++i) {
      acc += static_cast<std::uint64_t>(b.ts[i]) ^ static_cast<std::uint64_t>(b.metric_idx[i]) ^
             static_cast<std::uint64_t>(b.tags_idx[i]);
    }
    for (std::uint32_t i = 0; i < b.n_metrics; ++i) acc += m.get(b.metric_dict[i]).size();
    for (std::uint32_t i = 0; i < b.n_tags; ++i) acc += t.get(b.tags_dict[i]).size();
    sink_.store(acc, std::memory_order_relaxed);
    return Status::ok();
  }
  Status commit(std::uint64_t& bytes) override {
    bytes = 0;
    return Status::ok();
  }
  void abort() noexcept override {}

 private:
  std::atomic<std::uint64_t> sink_{0};
};

std::string arg(int argc, char** argv, const char* name, const char* def) {
  const std::size_t n = std::strlen(name);
  for (int i = 1; i < argc; ++i) {
    if (std::strncmp(argv[i], name, n) == 0 && argv[i][n] == '=') return argv[i] + n + 1;
  }
  return def;
}

}  // namespace

int main(int argc, char** argv) {
  const double seconds = std::stod(arg(argc, argv, "--seconds", "5"));
  const std::uint64_t series = std::stoull(arg(argc, argv, "--series", "10000"));
  const std::string sink_kind = arg(argc, argv, "--sink", "null");
  const std::string out = arg(argc, argv, "--out", "/tmp/flux-bench");
  const std::string overflow = arg(argc, argv, "--overflow", "drop-oldest");
  const double rate = std::stod(arg(argc, argv, "--rate", "0"));  // samples/s, 0 = flat out

  // 2000 packets x 20 lines x 3 fields, cycling through `series` tag sets.
  std::vector<std::string> packets;
  std::uint64_t s = 0;
  for (int p = 0; p < 2000; ++p) {
    std::string pkt;
    for (int l = 0; l < 20; ++l, ++s) {
      const std::uint64_t id = s % series;
      pkt += "cpu,host=server-" + std::to_string(id % 500) + ",region=us-west-" + std::to_string(id % 4) +
             ",id=" + std::to_string(id) + " usage_user=" + std::to_string(s % 100) +
             ".25,usage_system=3.5,idle=" + std::to_string(s % 97) + "i 1700000000" + std::to_string(100000000 + s % 899999999) +
             "\n";
    }
    packets.push_back(std::move(pkt));
  }
  const std::uint64_t samples_per_packet = 60;

  ShardConfig c;
  c.overflow = overflow == "drop-newest" ? Overflow::kDropNewest : Overflow::kDropOldest;
  c.max_tagsets = static_cast<std::uint32_t>(series + 100);
  c.tagsets_arena = series * 64 + (1 << 20);
  c.window_ns = 3600'000'000'000;

  std::unique_ptr<BatchSink> sink;
  if (sink_kind == "parquet") {
#if defined(FLUX_WITH_ARROW)
    std::filesystem::remove_all(out);
    sink = std::make_unique<ParquetSink>(ParquetSinkConfig{out, "bench", 3, false});
#else
    std::fprintf(stderr, "built without Arrow; use --sink=null\n");
    return 2;
#endif
  } else {
    sink = std::make_unique<NullSink>();
  }

  auto shard = make_shard(c, std::move(sink));
  shard->start();
  std::uint64_t sent = 0;
  const std::int64_t t0 = mono_ns();
  const std::int64_t end = t0 + static_cast<std::int64_t>(seconds * 1e9);
  std::size_t i = 0;
  while (true) {
    for (int k = 0; k < 64; ++k) {
      shard->ingest_packet(packets[i], 0);
      if (++i == packets.size()) i = 0;
    }
    sent += 64 * samples_per_packet;
    const std::int64_t now = mono_ns();
    if (now >= end) break;
    if (rate > 0) {
      const auto due = t0 + static_cast<std::int64_t>(static_cast<double>(sent) / rate * 1e9);
      if (due > now) std::this_thread::sleep_for(std::chrono::nanoseconds(due - now));
    }
  }
  const double produce_s = static_cast<double>(mono_ns() - t0) / 1e9;
  const bool clean = shard->stop(std::chrono::seconds(60));
  const double total_s = static_cast<double>(mono_ns() - t0) / 1e9;
  const ShardStats st = shard->stats();

  auto pct = [&](std::uint64_t v) { return 100.0 * static_cast<double>(v) / static_cast<double>(st.samples_offered); };
  std::printf("sink=%s overflow=%s series=%llu\n", sink_kind.c_str(), overflow.c_str(),
              static_cast<unsigned long long>(series));
  std::printf("  producer:   %.2fM samples/s offered (%.2fM lines/s) over %.1fs\n",
              static_cast<double>(st.samples_offered) / produce_s / 1e6,
              static_cast<double>(st.lines) / produce_s / 1e6, produce_s);
  std::printf("  written:    %.2fM samples/s end to end (%.1fs incl. drain + final commit)\n",
              static_cast<double>(st.written) / total_s / 1e6, total_s);
  std::printf("  dropped:    ring %.2f%%  batch %.2f%%  write_err %.2f%%\n", pct(st.dropped_ring),
              pct(st.dropped_batch), pct(st.dropped_write_error));
  std::printf("  row groups: %llu, files: %llu, bytes: %.1f MiB (%.2f bytes/sample)\n",
              static_cast<unsigned long long>(st.row_groups), static_cast<unsigned long long>(st.files_committed),
              static_cast<double>(st.bytes_committed) / (1 << 20),
              st.written ? static_cast<double>(st.bytes_committed) / static_cast<double>(st.written) : 0.0);
  std::printf("  writer busy: %.2fs of %.1fs\n", static_cast<double>(st.write_ns) / 1e9, total_s);
  return clean ? 0 : 1;
}
