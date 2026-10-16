#pragma once

// Consumer thread: drains the SPSC ring into column batches.
//
// Per sample it does two array lookups (global id -> per-batch dictionary
// index) and four column stores. No allocation, no locks, no I/O. It never
// touches the disk; sealed batches go to the writer thread. If the consumer
// wrote Parquet itself, the ring would fill and drop data during every
// multi-second flush.
//
// A batch is sealed when it's full (bounds memory) or when the time window
// ends (bounds latency to disk). Sealing only on time, as in "flush every
// 60 s", would let one window grow to 60 s x ingest rate rows: at 1M
// samples/s that's ~1.5 GB, i.e. the OOM this design exists to prevent.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>

#include "batch.hpp"
#include "flux/clock.hpp"
#include "flux/counters.hpp"
#include "flux/cpu.hpp"
#include "flux/idle.hpp"
#include "flux/sample.hpp"

namespace flux {

struct ConsumerConfig {
  std::int64_t window_ns = 60'000'000'000;
  std::uint32_t max_metrics = 0;  // interner capacities: sizes the remap tables
  std::uint32_t max_tagsets = 0;
};

// Maps a global interner id to a small per-batch index.
//
// Each entry packs (epoch << 32 | local index). Starting a new batch just
// bumps the epoch, which invalidates every entry at once. Clearing a
// million-entry table per batch would cost ~1 ms; an unordered_map would
// allocate.
class DictRemap {
 public:
  explicit DictRemap(std::uint32_t max_ids) : slots_(make_aligned<std::uint64_t>(max_ids)), size_(max_ids) {}

  void next_epoch() noexcept {
    if (++epoch_ == 0) {  // wrapped after 4 billion batches: clear once
      std::fill_n(slots_.get(), size_, std::uint64_t{0});
      epoch_ = 1;
    }
  }

  std::int32_t local(std::uint32_t id, std::uint32_t* dict, std::uint32_t& n) noexcept {
    std::uint64_t& s = slots_[id];
    if ((s >> 32) != epoch_) {
      s = (std::uint64_t{epoch_} << 32) | n;
      dict[n] = id;
      return static_cast<std::int32_t>(n++);
    }
    return static_cast<std::int32_t>(static_cast<std::uint32_t>(s));
  }

 private:
  AlignedArray<std::uint64_t> slots_;
  std::uint32_t epoch_ = 1;
  std::size_t size_;
};

template <class Ring>
class Consumer {
 public:
  using Clock = std::function<std::int64_t()>;

  Consumer(Ring& ring, BatchExchange& ex, ConsumerStats& stats, const ConsumerConfig& cfg,
           Clock clock = &wall_ns)
      : ring_(ring),
        ex_(ex),
        stats_(stats),
        window_ns_(cfg.window_ns),
        metrics_(cfg.max_metrics),
        tags_(cfg.max_tagsets),
        clock_(std::move(clock)) {}

  // Runs until `stop` is set AND the ring is empty. The producer must have
  // stopped before `stop` is set, so "empty" really means drained.
  void run(const std::atomic<bool>& stop) {
    std::int64_t window = window_of(clock_());
    Batch* cur = take(window);
    BackoffIdle idle;

    for (;;) {
      const bool stopping = stop.load(std::memory_order_acquire);
      const std::size_t want = std::min<std::size_t>(buf_.size(), cur->capacity - cur->rows);
      const std::size_t n = ring_.pop(buf_.data(), want);
      if (n != 0) {
        append(*cur, n);
        stats_.popped.add(n);
        if (cur->full()) {
          seal(cur, false);
          cur = take(window);
        }
      } else if (stopping) {
        break;
      }
      // A short pop counts as idle: back off (spin, yield, then a 200 us
      // sleep) and let samples pile up, so we drain them in big runs instead
      // of trailing the producer a few samples at a time. In bench_ring's
      // tight two-thread loop, trailing that closely made cache lines bounce
      // between cores (~2.8x slower). In the full pipeline the producer is
      // slower (it parses), and measured throughput was the same either way
      // (~5M samples/s), so this mostly keeps the consumer from spinning.
      // The extra latency doesn't matter: batches sit in memory for seconds.
      if (n >= kBigPop) {
        idle.reset();
      } else if (n < want) {
        idle.idle();
      }

      const std::int64_t now_window = window_of(clock_());
      if (now_window != window) {
        seal(cur, true);  // submitted even if empty: tells the writer to close the file
        window = now_window;
        cur = take(window);
      }
    }
    seal(cur, true);
    ex_.close();
  }

 private:
  std::int64_t window_of(std::int64_t t) const noexcept { return t - (t % window_ns_); }

  Batch* take(std::int64_t window) {
    for (;;) {
      BatchExchange::Acquired a = ex_.acquire();
      if (a.evicted) {
        stats_.batches_dropped.add();
        stats_.dropped_batch.add(a.evicted_rows);
      }
      if (a.batch != nullptr) {
        a.batch->reset(window, seq_++);
        metrics_.next_epoch();
        tags_.next_epoch();
        return a.batch;
      }
      // Unreachable with >= 3 batches; don't spin hot if it ever happens.
      stats_.no_free_batch.add();
      cpu_relax();
    }
  }

  void seal(Batch* b, bool closes_window) {
    b->closes_window = closes_window;
    stats_.batches_sealed.add();
    ex_.submit(b);
  }

  void append(Batch& b, std::size_t n) noexcept {
    std::uint32_t r = b.rows;
    for (std::size_t i = 0; i < n; ++i) {
      const Sample& s = buf_[i];
      b.ts[r] = s.ts_ns;
      b.value[r] = s.value;
      b.metric_idx[r] = metrics_.local(s.metric_id, b.metric_dict.get(), b.n_metrics);
      b.tags_idx[r] = tags_.local(s.tags_id, b.tags_dict.get(), b.n_tags);
      ++r;
    }
    b.rows = r;
  }

  Ring& ring_;
  BatchExchange& ex_;
  ConsumerStats& stats_;
  const std::int64_t window_ns_;
  DictRemap metrics_;
  DictRemap tags_;
  Clock clock_;
  std::uint64_t seq_ = 0;
  static constexpr std::size_t kBigPop = 256;
  std::array<Sample, 1024> buf_{};
};

}  // namespace flux
