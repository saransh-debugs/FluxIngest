// fluxingest: UDP line protocol in, Parquet files out.
//
//   fluxingest --port=8089 --out=./data --shards=2 --window=60

#include <arrow/memory_pool.h>

#include <atomic>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "flux/clock.hpp"
#include "log.hpp"
#include "parquet_sink.hpp"
#include "pipeline.hpp"

namespace flux {
namespace {

// Signal handlers may only touch lock-free atomics.
std::atomic<bool> g_stop{false};
static_assert(std::atomic<bool>::is_always_lock_free);
extern "C" void on_signal(int) { g_stop.store(true); }

struct Options {
  std::string bind = "0.0.0.0";
  std::uint16_t port = 8089;
  std::uint32_t shards = 1;
  std::string out = "./data";
  std::int64_t window_s = 60;
  std::uint32_t batch_rows = 1 << 18;
  std::uint32_t batches = 4;
  std::uint64_t ring = 1 << 20;
  std::uint32_t max_metrics = 100'000;
  std::uint64_t metric_arena_mb = 8;
  std::uint32_t max_tagsets = 1'000'000;
  std::uint64_t tagset_arena_mb = 128;
  std::uint64_t rcvbuf_mb = 16;
  Overflow overflow = Overflow::kDropOldest;
  int zstd = 3;
  bool fsync = true;
  std::int64_t stats_interval_s = 5;
  std::string stats_json;
  std::int64_t shutdown_timeout_s = 10;
  std::int64_t debug_write_delay_ms = 0;
};

void usage() {
  std::fprintf(stderr, R"(usage: fluxingest [flags]

  --bind=ADDR              UDP bind address            (0.0.0.0)
  --port=N                 UDP port                    (8089)
  --shards=N               independent pipelines, SO_REUSEPORT (1)
  --out=DIR                output directory            (./data)
  --window=SECONDS         one Parquet file per window (60)
  --batch-rows=N           rows per row group / batch  (262144)
  --batches=N              batch buffers per shard, >=3 (4)
  --ring=N                 SPSC ring slots, power of 2 (1048576)
  --overflow=MODE          drop-oldest | drop-newest   (drop-oldest)
  --max-metrics=N          distinct metric names cap   (100000)
  --metric-arena-mb=N      bytes for metric names      (8)
  --max-tagsets=N          distinct tag sets cap       (1000000)
  --tagset-arena-mb=N      bytes for tag sets          (128)
  --rcvbuf-mb=N            socket receive buffer       (16)
  --zstd=LEVEL             zstd level                  (3)
  --no-fsync               skip fsync on commit (faster, not crash safe)
  --stats-interval=SECONDS stats line period, 0 = off  (5)
  --stats-json=PATH        write final counters as JSON on exit
  --shutdown-timeout=SECONDS  max wait for the writer at exit (10)
)");
}

template <class T>
bool parse_num(std::string_view s, T& out) {
  const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
  return ec == std::errc{} && p == s.data() + s.size();
}

bool parse_args(int argc, char** argv, Options& o) {
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg == "-h" || arg == "--help") return false;
    if (arg == "--no-fsync") {
      o.fsync = false;
      continue;
    }
    const auto eq = arg.find('=');
    if (!arg.starts_with("--") || eq == std::string_view::npos) {
      std::fprintf(stderr, "bad argument: %s\n", argv[i]);
      return false;
    }
    const std::string_view k = arg.substr(2, eq - 2);
    const std::string_view v = arg.substr(eq + 1);
    bool ok = true;
    if (k == "bind") o.bind = v;
    else if (k == "port") ok = parse_num(v, o.port);
    else if (k == "shards") ok = parse_num(v, o.shards) && o.shards > 0;
    else if (k == "out") o.out = v;
    else if (k == "window") ok = parse_num(v, o.window_s) && o.window_s > 0;
    else if (k == "batch-rows") ok = parse_num(v, o.batch_rows) && o.batch_rows > 0;
    else if (k == "batches") ok = parse_num(v, o.batches) && o.batches >= 3;
    else if (k == "ring") ok = parse_num(v, o.ring) && is_pow2(o.ring);
    else if (k == "max-metrics") ok = parse_num(v, o.max_metrics) && o.max_metrics > 0;
    else if (k == "metric-arena-mb") ok = parse_num(v, o.metric_arena_mb);
    else if (k == "max-tagsets") ok = parse_num(v, o.max_tagsets) && o.max_tagsets > 0;
    else if (k == "tagset-arena-mb") ok = parse_num(v, o.tagset_arena_mb);
    else if (k == "rcvbuf-mb") ok = parse_num(v, o.rcvbuf_mb);
    else if (k == "zstd") ok = parse_num(v, o.zstd);
    else if (k == "stats-interval") ok = parse_num(v, o.stats_interval_s);
    else if (k == "stats-json") o.stats_json = v;
    else if (k == "shutdown-timeout") ok = parse_num(v, o.shutdown_timeout_s);
    else if (k == "debug-write-delay-ms") ok = parse_num(v, o.debug_write_delay_ms);  // test hook: fake slow disk
    else if (k == "overflow") {
      if (v == "drop-oldest") o.overflow = Overflow::kDropOldest;
      else if (v == "drop-newest") o.overflow = Overflow::kDropNewest;
      else ok = false;
    } else {
      std::fprintf(stderr, "unknown flag: --%.*s\n", static_cast<int>(k.size()), k.data());
      return false;
    }
    if (!ok) {
      std::fprintf(stderr, "bad value for --%.*s\n", static_cast<int>(k.size()), k.data());
      return false;
    }
  }
  return true;
}

// Test hook: makes every write slow, to exercise backpressure end to end.
class DelaySink final : public BatchSink {
 public:
  DelaySink(std::unique_ptr<BatchSink> inner, std::chrono::milliseconds d) : inner_(std::move(inner)), d_(d) {}
  Status open(std::int64_t w, std::uint64_t s) override { return inner_->open(w, s); }
  Status write(const Batch& b, const StringInterner& m, const StringInterner& t) override {
    std::this_thread::sleep_for(d_);
    return inner_->write(b, m, t);
  }
  Status commit(std::uint64_t& bytes) override { return inner_->commit(bytes); }
  void abort() noexcept override { inner_->abort(); }

 private:
  std::unique_ptr<BatchSink> inner_;
  std::chrono::milliseconds d_;
};

std::string human(double v) {
  char buf[32];
  if (v >= 1e9) std::snprintf(buf, sizeof buf, "%.2fG", v / 1e9);
  else if (v >= 1e6) std::snprintf(buf, sizeof buf, "%.2fM", v / 1e6);
  else if (v >= 1e3) std::snprintf(buf, sizeof buf, "%.1fk", v / 1e3);
  else std::snprintf(buf, sizeof buf, "%.0f", v);
  return buf;
}

void print_stats(const ShardStats& cur, const ShardStats& prev, double dt_s) {
  const double rate = static_cast<double>(cur.samples_offered - prev.samples_offered) / dt_s;
  const double fill = cur.ring_capacity ? 100.0 * static_cast<double>(cur.ring_size) / static_cast<double>(cur.ring_capacity) : 0;
  std::fprintf(stderr,
               "[stats] in=%s/s offered=%s written=%s | dropped ring=%s batch=%s write_err=%s cardinality=%s "
               "| parse_err=%s kernel_drops=%s | ring=%.0f%% queued=%llu files=%llu (%.1f MiB) "
               "| series metrics=%llu tagsets=%llu | arrow_pool=%.1f MiB\n",
               human(rate).c_str(), human(static_cast<double>(cur.samples_offered)).c_str(),
               human(static_cast<double>(cur.written)).c_str(), human(static_cast<double>(cur.dropped_ring)).c_str(),
               human(static_cast<double>(cur.dropped_batch)).c_str(),
               human(static_cast<double>(cur.dropped_write_error)).c_str(),
               human(static_cast<double>(cur.cardinality_rejected)).c_str(),
               human(static_cast<double>(cur.parse_errors)).c_str(),
               human(static_cast<double>(cur.kernel_drops)).c_str(), fill,
               static_cast<unsigned long long>(cur.batches_queued),
               static_cast<unsigned long long>(cur.files_committed),
               static_cast<double>(cur.bytes_committed) / (1 << 20),
               static_cast<unsigned long long>(cur.metrics_interned),
               static_cast<unsigned long long>(cur.tagsets_interned),
               static_cast<double>(arrow::default_memory_pool()->bytes_allocated()) / (1 << 20));
}

void write_json(const std::string& path, const ShardStats& t, bool clean) {
  std::FILE* f = std::fopen(path.c_str(), "w");
  if (!f) {
    FLUX_ERROR("cannot write %s", path.c_str());
    return;
  }
  auto u = [](std::uint64_t v) { return static_cast<unsigned long long>(v); };
  std::fprintf(f,
               "{\"clean_shutdown\": %s, \"datagrams\": %llu, \"lines\": %llu, \"parse_errors\": %llu, "
               "\"fields_skipped\": %llu, \"samples_offered\": %llu, \"cardinality_rejected\": %llu, "
               "\"recv_errors\": %llu, \"truncated_datagrams\": %llu, \"kernel_drops\": %llu, "
               "\"dropped_ring\": %llu, \"popped\": %llu, \"batches_sealed\": %llu, \"dropped_batch\": %llu, "
               "\"batches_dropped\": %llu, \"written\": %llu, \"dropped_write_error\": %llu, "
               "\"files_committed\": %llu, \"files_failed\": %llu, \"row_groups\": %llu, "
               "\"bytes_committed\": %llu, \"write_ns\": %llu, \"metrics_interned\": %llu, "
               "\"tagsets_interned\": %llu, \"arrow_pool_peak_bytes\": %lld}\n",
               clean ? "true" : "false", u(t.datagrams), u(t.lines), u(t.parse_errors), u(t.fields_skipped),
               u(t.samples_offered), u(t.cardinality_rejected), u(t.recv_errors), u(t.truncated_datagrams),
               u(t.kernel_drops), u(t.dropped_ring), u(t.popped), u(t.batches_sealed), u(t.dropped_batch),
               u(t.batches_dropped), u(t.written), u(t.dropped_write_error), u(t.files_committed),
               u(t.files_failed), u(t.row_groups), u(t.bytes_committed), u(t.write_ns), u(t.metrics_interned),
               u(t.tagsets_interned), static_cast<long long>(arrow::default_memory_pool()->max_memory()));
  std::fclose(f);
}

ShardStats total(const std::vector<std::unique_ptr<Shard>>& shards) {
  ShardStats t{};
  for (const auto& s : shards) t += s->stats();
  return t;
}

int run(const Options& o) {
  std::random_device rd;
  const std::uint64_t seed = (std::uint64_t{rd()} << 32) ^ rd();

  ShardConfig base;
  base.overflow = o.overflow;
  base.ring_capacity = o.ring;
  base.batch_rows = o.batch_rows;
  base.n_batches = o.batches;
  base.window_ns = o.window_s * 1'000'000'000;
  base.max_metrics = o.max_metrics;
  base.metrics_arena = o.metric_arena_mb << 20;
  base.max_tagsets = o.max_tagsets;
  base.tagsets_arena = o.tagset_arena_mb << 20;
  UdpConfig udp;
  udp.bind_addr = o.bind;
  udp.port = o.port;
  udp.reuseport = o.shards > 1;
  udp.rcvbuf_bytes = static_cast<int>(o.rcvbuf_mb << 20);
  base.udp = udp;

  const std::size_t per_shard = shard_memory_bytes(base);
  FLUX_INFO("memory budget: %.1f MiB per shard x %u shards = %.1f MiB, allocated now, fixed after start",
            static_cast<double>(per_shard) / (1 << 20), o.shards,
            static_cast<double>(per_shard * o.shards) / (1 << 20));
  FLUX_INFO("overflow policy: %s, window %llds, batch %u rows x %u buffers, ring %llu slots",
            o.overflow == Overflow::kDropOldest ? "drop-oldest" : "drop-newest",
            static_cast<long long>(o.window_s), o.batch_rows, o.batches,
            static_cast<unsigned long long>(o.ring));
#if defined(__APPLE__)
  if (o.shards > 1) FLUX_WARN("macOS SO_REUSEPORT does not load-balance UDP; extra shards may get no traffic");
#endif

  std::vector<std::unique_ptr<Shard>> shards;
  for (std::uint32_t i = 0; i < o.shards; ++i) {
    ShardConfig c = base;
    c.id = i;
    c.hash_seed = seed + i;
    auto parquet = std::make_unique<ParquetSink>(
        ParquetSinkConfig{o.out, "shard" + std::to_string(i), o.zstd, o.fsync});
    if (parquet->removed_tmp_files() != 0) {
      FLUX_WARN("shard %u: removed %zu unfinished .tmp files from a previous run", i, parquet->removed_tmp_files());
    }
    std::unique_ptr<BatchSink> sink = std::move(parquet);
    if (o.debug_write_delay_ms > 0) {
      sink = std::make_unique<DelaySink>(std::move(sink), std::chrono::milliseconds(o.debug_write_delay_ms));
    }
    shards.push_back(make_shard(c, std::move(sink)));
  }
  FLUX_INFO("listening on udp://%s:%u (socket buffer %d KiB), writing to %s", o.bind.c_str(),
            shards[0]->udp_port(), shards[0]->udp_rcvbuf() / 1024, o.out.c_str());
  for (auto& s : shards) s->start();

  struct sigaction sa {};
  sa.sa_handler = on_signal;
  sigemptyset(&sa.sa_mask);
  ::sigaction(SIGINT, &sa, nullptr);
  ::sigaction(SIGTERM, &sa, nullptr);

  ShardStats prev{};
  std::int64_t last = mono_ns();
  while (!g_stop.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const std::int64_t now = mono_ns();
    if (o.stats_interval_s > 0 && now - last >= o.stats_interval_s * 1'000'000'000) {
      const ShardStats cur = total(shards);
      print_stats(cur, prev, static_cast<double>(now - last) / 1e9);
      prev = cur;
      last = now;
    }
  }

  FLUX_INFO("shutting down: draining queues and committing open files");
  bool clean = true;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(o.shutdown_timeout_s);
  for (auto& s : shards) {
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
    clean = s->stop(std::max(left, std::chrono::milliseconds(0))) && clean;
  }
  const ShardStats t = total(shards);
  print_stats(t, prev, static_cast<double>(mono_ns() - last) / 1e9);
  if (!o.stats_json.empty()) write_json(o.stats_json, t, clean);
  if (!clean) {
    // A writer is stuck in I/O. Destructors would wait for it forever, so
    // leave without running them. Committed files are already durable.
    FLUX_ERROR("writer did not finish within %llds; exiting without waiting",
               static_cast<long long>(o.shutdown_timeout_s));
    std::fflush(stderr);
    std::_Exit(3);
  }
  return 0;
}

}  // namespace
}  // namespace flux

int main(int argc, char** argv) {
  flux::Options o;
  if (!flux::parse_args(argc, argv, o)) {
    flux::usage();
    return 2;
  }
  try {
    return flux::run(o);
  } catch (const std::exception& e) {
    FLUX_ERROR("%s", e.what());
    return 1;
  }
}
