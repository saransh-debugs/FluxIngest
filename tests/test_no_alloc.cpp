// Proves the per-sample hot path never touches the heap.
//
// We replace global operator new/delete with versions that count calls, then
// run the producer path (parse + intern + push) and the consumer path (pop)
// a million times after warm-up. The count must not move.
//
// Warm-up matters: the first time a series is seen it gets interned, which
// copies bytes into the preallocated arena (still no malloc). We intern
// everything once first so the measured loop is the steady state.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

#include "flux/ingestor.hpp"
#include "flux/spsc_ring.hpp"

namespace {
std::atomic<std::uint64_t> g_allocs{0};
}

void* operator new(std::size_t n) {
  g_allocs.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void* operator new[](std::size_t n) {
  g_allocs.fetch_add(1, std::memory_order_relaxed);
  if (void* p = std::malloc(n ? n : 1)) return p;
  throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

namespace flux {
namespace {

template <Overflow P>
void check_hot_path() {
  using Ring = SpscRing<Sample, P>;
  StringInterner metrics{1024, 1 << 16, 1};
  StringInterner tags{1024, 1 << 16, 2};
  Ring ring{1024};
  ProducerStats stats;
  LineIngestor<Ring> ing{metrics, tags, ring, stats};
  std::vector<Sample> out(256);

  // Build a packet with 100 lines over 10 series. Built before counting.
  std::string packet;
  for (int i = 0; i < 100; ++i) {
    packet += "cpu,host=h" + std::to_string(i % 10) + ",region=us\\ east usage=" + std::to_string(i) +
              ".5,idle=" + std::to_string(i) + "i,msg=\"skip me\" 1700000000000000000\n";
  }
  ing.ingest_packet(packet, 0);  // warm-up: interns all series
  while (ring.pop(out.data(), out.size()) != 0) {
  }

  const std::uint64_t before = g_allocs.load();
  for (int round = 0; round < 5000; ++round) {  // 500k lines, 1M samples
    ing.ingest_packet(packet, round);
    while (ring.pop(out.data(), out.size()) != 0) {
    }
  }
  const std::uint64_t after = g_allocs.load();

  EXPECT_EQ(after - before, 0u) << "heap allocations on the hot path";
  EXPECT_EQ(stats.samples_offered.load(), 5001u * 200u);
}

TEST(NoAlloc, DropNewestHotPath) { check_hot_path<Overflow::kDropNewest>(); }
TEST(NoAlloc, DropOldestHotPath) { check_hot_path<Overflow::kDropOldest>(); }

TEST(NoAlloc, CounterActuallyCounts) {
  const std::uint64_t before = g_allocs.load();
  // volatile stops the compiler from eliding the new/delete pair, which
  // C++14 allows it to do.
  int* volatile p = new int(5);
  delete p;
  EXPECT_EQ(g_allocs.load() - before, 1u);
}

}  // namespace
}  // namespace flux
