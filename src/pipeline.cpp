#include "pipeline.hpp"

#include <atomic>
#include <future>
#include <pthread.h>
#include <stdexcept>
#include <thread>

#include "batch.hpp"
#include "consumer.hpp"
#include "flux/clock.hpp"
#include "flux/ingestor.hpp"
#include "flux/interner.hpp"

namespace flux {

namespace {

void name_thread(const char* role, std::uint32_t id) {
  char name[16];
  std::snprintf(name, sizeof name, "flux-%s-%u", role, id);
#if defined(__APPLE__)
  pthread_setname_np(name);
#else
  pthread_setname_np(pthread_self(), name);
#endif
}

template <Overflow P>
class ShardImpl final : public Shard {
  using Ring = SpscRing<Sample, P>;

 public:
  ShardImpl(const ShardConfig& cfg, std::unique_ptr<BatchSink> sink)
      : cfg_(cfg),
        sink_(std::move(sink)),
        metrics_(cfg.max_metrics, cfg.metrics_arena, cfg.hash_seed ^ 0x6d65747269637321ull),
        tags_(cfg.max_tagsets, cfg.tagsets_arena, cfg.hash_seed ^ 0x7461677365747321ull),
        ring_(cfg.ring_capacity),
        ingestor_(metrics_, tags_, ring_, pstats_),
        exchange_(cfg.n_batches, cfg.batch_rows) {
    if (cfg.udp) receiver_ = std::make_unique<UdpReceiver>(*cfg.udp, pstats_);
    WriterConfig w = cfg.writer;
    w.window_ns = cfg.window_ns;
    cfg_.writer = w;
  }

  ~ShardImpl() override {
    producer_stop_.store(true);
    consumer_stop_.store(true, std::memory_order_release);
    for (std::thread* t : {&receiver_thread_, &consumer_thread_, &writer_thread_}) {
      if (t->joinable()) t->join();
    }
  }

  void start() override {
    writer_thread_ = std::thread([this] {
      name_thread("wr", cfg_.id);
      Writer w(exchange_, *sink_, metrics_, tags_, wstats_, cfg_.writer);
      w.run();
      writer_done_.set_value();
    });
    consumer_thread_ = std::thread([this] {
      name_thread("cons", cfg_.id);
      Consumer<Ring> c(ring_, exchange_, cstats_,
                       ConsumerConfig{cfg_.window_ns, cfg_.max_metrics, cfg_.max_tagsets});
      c.run(consumer_stop_);
    });
    if (receiver_) {
      receiver_thread_ = std::thread([this] {
        name_thread("rx", cfg_.id);
        while (!producer_stop_.load(std::memory_order_relaxed)) {
          const std::size_t n = receiver_->receive(100);
          if (n == 0) continue;
          const std::int64_t now = wall_ns();  // one clock read per syscall batch
          for (std::size_t i = 0; i < n; ++i) {
            const std::string_view p = receiver_->packet(i);
            if (!p.empty()) ingestor_.ingest_packet(p, now);
          }
        }
      });
    }
  }

  void ingest_packet(std::string_view packet, std::int64_t now_ns) override {
    ingestor_.ingest_packet(packet, now_ns);
  }

  bool stop(std::chrono::milliseconds timeout) override {
    // Order matters: the producer stops first, so when the consumer sees
    // its stop flag and an empty ring, nothing more can arrive.
    producer_stop_.store(true);
    if (receiver_thread_.joinable()) receiver_thread_.join();
    consumer_stop_.store(true, std::memory_order_release);
    if (consumer_thread_.joinable()) consumer_thread_.join();  // drains the ring; never blocks on I/O
    if (!writer_thread_.joinable()) return true;
    if (writer_future_.wait_for(timeout) != std::future_status::ready) return false;
    writer_thread_.join();
    return true;
  }

  ShardStats stats() const override {
    ShardStats s{};
    s.datagrams = pstats_.datagrams.load();
    s.lines = pstats_.lines.load();
    s.parse_errors = pstats_.parse_errors.load();
    s.fields_skipped = pstats_.fields_skipped.load();
    s.samples_offered = pstats_.samples_offered.load();
    s.cardinality_rejected = pstats_.cardinality_rejected.load();
    s.recv_errors = pstats_.recv_errors.load();
    s.truncated_datagrams = pstats_.truncated_datagrams.load();
    s.kernel_drops = pstats_.kernel_drops.load();
    s.dropped_ring = ring_.dropped();
    s.ring_size = ring_.size_approx();
    s.ring_capacity = ring_.capacity();
    s.popped = cstats_.popped.load();
    s.batches_sealed = cstats_.batches_sealed.load();
    s.dropped_batch = cstats_.dropped_batch.load();
    s.batches_dropped = cstats_.batches_dropped.load();
    s.batches_queued = exchange_.queued();
    s.written = wstats_.written.load();
    s.dropped_write_error = wstats_.dropped_write_error.load();
    s.files_committed = wstats_.files_committed.load();
    s.files_failed = wstats_.files_failed.load();
    s.row_groups = wstats_.row_groups.load();
    s.bytes_committed = wstats_.bytes_committed.load();
    s.write_ns = wstats_.write_ns.load();
    s.metrics_interned = metrics_.size();
    s.tagsets_interned = tags_.size();
    return s;
  }

  std::uint16_t udp_port() const override { return receiver_ ? receiver_->port() : 0; }
  int udp_rcvbuf() const override { return receiver_ ? receiver_->rcvbuf_actual() : 0; }

 private:
  ShardConfig cfg_;
  std::unique_ptr<BatchSink> sink_;
  StringInterner metrics_;
  StringInterner tags_;
  Ring ring_;
  ProducerStats pstats_;
  ConsumerStats cstats_;
  WriterStats wstats_;
  LineIngestor<Ring> ingestor_;
  BatchExchange exchange_;
  std::unique_ptr<UdpReceiver> receiver_;
  std::atomic<bool> producer_stop_{false};
  std::atomic<bool> consumer_stop_{false};
  std::promise<void> writer_done_;
  std::future<void> writer_future_ = writer_done_.get_future();
  std::thread receiver_thread_;
  std::thread consumer_thread_;
  std::thread writer_thread_;
};

}  // namespace

ShardStats& ShardStats::operator+=(const ShardStats& o) {
  auto* a = reinterpret_cast<std::uint64_t*>(this);
  const auto* b = reinterpret_cast<const std::uint64_t*>(&o);
  static_assert(sizeof(ShardStats) % sizeof(std::uint64_t) == 0);
  for (std::size_t i = 0; i < sizeof(ShardStats) / sizeof(std::uint64_t); ++i) a[i] += b[i];
  return *this;
}

std::unique_ptr<Shard> make_shard(const ShardConfig& cfg, std::unique_ptr<BatchSink> sink) {
  if (!sink) throw std::invalid_argument("make_shard: null sink");
  if (cfg.window_ns <= 0) throw std::invalid_argument("window must be > 0");
  if (cfg.overflow == Overflow::kDropOldest) {
    return std::make_unique<ShardImpl<Overflow::kDropOldest>>(cfg, std::move(sink));
  }
  return std::make_unique<ShardImpl<Overflow::kDropNewest>>(cfg, std::move(sink));
}

std::size_t shard_memory_bytes(const ShardConfig& cfg) {
  std::size_t b = SpscRing<Sample, Overflow::kDropOldest>::memory_bytes(cfg.ring_capacity);
  b += std::size_t{cfg.n_batches} * Batch::memory_bytes(cfg.batch_rows);
  b += StringInterner::memory_bytes(cfg.max_metrics, cfg.metrics_arena);
  b += StringInterner::memory_bytes(cfg.max_tagsets, cfg.tagsets_arena);
  b += (std::size_t{cfg.max_metrics} + cfg.max_tagsets) * sizeof(std::uint64_t);  // consumer remap tables
  if (cfg.udp) b += UdpReceiver::memory_bytes(*cfg.udp);
  return b;
}

}  // namespace flux
