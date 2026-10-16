// Ring buffer micro-benchmarks.
//
//  Transfer/*   two threads move N samples producer -> consumer. Items/s.
//               Compares our ring (both policies) with the two mutex queues
//               people usually write first.
//  PushFull/*   ring is permanently full, no consumer. Cost of one push on
//               the overflow path: a cheap reject (DropNewest) vs a CAS that
//               evicts the oldest (DropOldest).
//  Overloaded/* consumer is alive but too slow, so the producer and consumer
//               fight over the head index. The worst case for DropOldest.
//
// macOS has no API to pin threads to cores, so numbers here include
// scheduler noise. Run several repetitions.

#include <benchmark/benchmark.h>

#include <array>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "flux/cpu.hpp"
#include "flux/sample.hpp"
#include "flux/spsc_ring.hpp"

namespace flux {
namespace {

constexpr std::size_t kCap = 1 << 16;
constexpr std::uint64_t kItems = 10'000'000;

// Baseline 1: mutex + fixed array. Same memory as our ring, only the sync
// differs.
class MutexRing {
 public:
  explicit MutexRing(std::size_t cap) : buf_(cap), mask_(cap - 1) {}
  bool push(const Sample& s) {
    std::lock_guard lk(mu_);
    if (tail_ - head_ == buf_.size()) return false;
    buf_[tail_++ & mask_] = s;
    return true;
  }
  std::size_t pop(Sample* out, std::size_t max) {
    std::lock_guard lk(mu_);
    std::size_t n = 0;
    while (n < max && head_ != tail_) out[n++] = buf_[head_++ & mask_];
    return n;
  }

 private:
  std::mutex mu_;
  std::vector<Sample> buf_;
  std::size_t mask_;
  std::uint64_t head_ = 0;
  std::uint64_t tail_ = 0;
};

// Baseline 2: mutex + std::deque, the "obvious" version. deque also
// allocates a new 512-byte block every ~21 samples.
class MutexDeque {
 public:
  explicit MutexDeque(std::size_t cap) : cap_(cap) {}
  bool push(const Sample& s) {
    std::lock_guard lk(mu_);
    if (q_.size() == cap_) return false;
    q_.push_back(s);
    return true;
  }
  std::size_t pop(Sample* out, std::size_t max) {
    std::lock_guard lk(mu_);
    std::size_t n = 0;
    while (n < max && !q_.empty()) {
      out[n++] = q_.front();
      q_.pop_front();
    }
    return n;
  }

 private:
  std::mutex mu_;
  std::deque<Sample> q_;
  std::size_t cap_;
};

// Producer retries on full so every item gets through; this measures
// throughput, not drop behavior.
template <class Q>
void BM_Transfer(benchmark::State& state) {
  const auto batch = static_cast<std::size_t>(state.range(0));
  for (auto _ : state) {
    auto q = std::make_unique<Q>(kCap);
    std::atomic<bool> go{false};
    std::thread consumer([&] {
      std::vector<Sample> buf(batch);
      std::uint64_t got = 0;
      while (!go.load(std::memory_order_acquire)) cpu_relax();
      while (got < kItems) {
        const std::size_t n = q->pop(buf.data(), batch);
        got += n;
        // Back off briefly on a short batch. Without this the consumer
        // catches up and reads the cache line the producer is still
        // writing, and that line bounces between cores on every sample.
        // Measured: DropNewest/256 went from ~100M/s to ~280M/s.
        // The real consumer (src/consumer.cpp) does the same.
        if (n < batch) for (int k = 0; k < 32; ++k) cpu_relax();
      }
      benchmark::DoNotOptimize(buf.data());
    });
    go.store(true, std::memory_order_release);
    for (std::uint64_t i = 0; i < kItems; ++i) {
      const Sample s{static_cast<std::int64_t>(i), 1.0, 1, 2};
      if constexpr (requires { Q::kPolicy; }) {
        while (!q->try_push(s)) cpu_relax();
      } else {
        while (!q->push(s)) cpu_relax();
      }
    }
    consumer.join();
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * kItems));
}

using Newest = SpscRing<Sample, Overflow::kDropNewest>;
using Oldest = SpscRing<Sample, Overflow::kDropOldest>;

BENCHMARK(BM_Transfer<Newest>)->Name("Transfer/SpscDropNewest")->Arg(1)->Arg(256)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Transfer<Oldest>)->Name("Transfer/SpscDropOldest")->Arg(1)->Arg(256)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Transfer<MutexRing>)->Name("Transfer/MutexRing")->Arg(1)->Arg(256)->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Transfer<MutexDeque>)->Name("Transfer/MutexDeque")->Arg(1)->Arg(256)->UseRealTime()->Unit(benchmark::kMillisecond);

template <class Q>
void BM_PushFull(benchmark::State& state) {
  Q q(1024);
  for (int i = 0; i < 1024; ++i) (void)q.push(Sample{i, 1.0, 1, 2});
  std::int64_t i = 0;
  for (auto _ : state) benchmark::DoNotOptimize(q.push(Sample{i++, 1.0, 1, 2}));
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_PushFull<Newest>)->Name("PushFull/DropNewest");
BENCHMARK(BM_PushFull<Oldest>)->Name("PushFull/DropOldest");

template <class Q>
void BM_PushNotFull(benchmark::State& state) {
  Q q(1024);
  std::array<Sample, 256> out{};
  std::int64_t i = 0;
  for (auto _ : state) {
    benchmark::DoNotOptimize(q.push(Sample{i++, 1.0, 1, 2}));
    if ((i & 255) == 0) q.pop(out.data(), out.size());  // same thread, keeps it from filling
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_PushNotFull<Newest>)->Name("PushNotFull/DropNewest");
BENCHMARK(BM_PushNotFull<Oldest>)->Name("PushNotFull/DropOldest");

// Consumer pops 64 at a time then pauses, so the ring sits full and the
// producer's overflow path races the consumer's head updates.
template <class Q>
void BM_Overloaded(benchmark::State& state) {
  for (auto _ : state) {
    auto q = std::make_unique<Q>(4096);
    std::atomic<bool> done{false};
    std::thread consumer([&] {
      std::array<Sample, 64> buf{};
      while (!done.load(std::memory_order_relaxed)) {
        q->pop(buf.data(), buf.size());
        for (int k = 0; k < 50; ++k) cpu_relax();
      }
    });
    for (std::uint64_t i = 0; i < kItems; ++i) {
      benchmark::DoNotOptimize(q->push(Sample{static_cast<std::int64_t>(i), 1.0, 1, 2}));
    }
    done = true;
    consumer.join();
    state.counters["drop_pct"] = 100.0 * static_cast<double>(q->dropped()) / static_cast<double>(kItems);
  }
  state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * kItems));
}
BENCHMARK(BM_Overloaded<Newest>)->Name("Overloaded/DropNewest")->UseRealTime()->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Overloaded<Oldest>)->Name("Overloaded/DropOldest")->UseRealTime()->Unit(benchmark::kMillisecond);

}  // namespace
}  // namespace flux

BENCHMARK_MAIN();
