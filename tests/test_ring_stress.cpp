// Two real threads hammering the ring. Run under ThreadSanitizer (tsan
// preset) to catch data races, and in release builds for volume.
//
// Each sample encodes its sequence number in all three fields. If the
// consumer ever reads a slot while the producer is overwriting it (a "torn"
// read), the fields won't agree and the test fails.

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <thread>
#include <vector>

#include "flux/cpu.hpp"
#include "flux/sample.hpp"
#include "flux/spsc_ring.hpp"

namespace flux {
namespace {

std::uint64_t stress_count() {
  if (const char* e = std::getenv("FLUX_STRESS_N")) return std::strtoull(e, nullptr, 10);
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
  return 2'000'000;
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
  return 2'000'000;
#else
  return 50'000'000;
#endif
#else
  return 50'000'000;
#endif
}

Sample encode(std::uint64_t seq) {
  return Sample{static_cast<std::int64_t>(seq), static_cast<double>(seq),
                static_cast<std::uint32_t>(seq), static_cast<std::uint32_t>(seq >> 32) ^ 0xabcdu};
}

bool consistent(const Sample& s) {
  const auto seq = static_cast<std::uint64_t>(s.ts_ns);
  return s.value == static_cast<double>(seq) && s.metric_id == static_cast<std::uint32_t>(seq) &&
         s.tags_id == (static_cast<std::uint32_t>(seq >> 32) ^ 0xabcdu);
}

struct Result {
  std::uint64_t popped = 0;
  std::uint64_t dropped = 0;
  std::uint64_t torn = 0;
  std::uint64_t out_of_order = 0;
  std::uint64_t gaps = 0;  // seq jumps (only legal when something was dropped)
  std::int64_t last = -1;
};

// slow_consumer: spin a bit per pop so the ring overflows constantly.
template <Overflow P>
Result run(std::uint64_t n, std::size_t cap, bool slow_consumer) {
  SpscRing<Sample, P> ring(cap);
  std::atomic<bool> done{false};
  Result r;

  std::thread consumer([&] {
    std::vector<Sample> buf(256);
    std::int64_t last = -1;
    for (;;) {
      const bool finished = done.load(std::memory_order_acquire);
      const std::size_t got = ring.pop(buf.data(), buf.size());
      for (std::size_t i = 0; i < got; ++i) {
        const Sample& s = buf[i];
        if (!consistent(s)) ++r.torn;
        if (s.ts_ns <= last) ++r.out_of_order;
        if (s.ts_ns != last + 1) ++r.gaps;
        last = s.ts_ns;
      }
      r.popped += got;
      r.last = last;
      if (got == 0) {
        if (finished) break;
        cpu_relax();
      } else if (slow_consumer) {
        for (int k = 0; k < 200; ++k) cpu_relax();
      }
    }
  });

  for (std::uint64_t i = 0; i < n; ++i) (void)ring.push(encode(i));
  done.store(true, std::memory_order_release);
  consumer.join();
  r.dropped = ring.dropped();
  return r;
}

template <class R>
class StressTest : public ::testing::Test {};

struct NewestTag {
  static constexpr Overflow kP = Overflow::kDropNewest;
};
struct OldestTag {
  static constexpr Overflow kP = Overflow::kDropOldest;
};
using Tags = ::testing::Types<NewestTag, OldestTag>;
TYPED_TEST_SUITE(StressTest, Tags);

TYPED_TEST(StressTest, FastConsumerConservesEverySample) {
  const std::uint64_t n = stress_count();
  const Result r = run<TypeParam::kP>(n, 1u << 16, false);
  EXPECT_EQ(r.torn, 0u);
  EXPECT_EQ(r.out_of_order, 0u);
  EXPECT_EQ(r.popped + r.dropped, n);
  if (r.dropped == 0) {
    EXPECT_EQ(r.gaps, 0u);
  }
}

TYPED_TEST(StressTest, OverloadedConsumerConservesEverySample) {
  const std::uint64_t n = stress_count() / 4;
  // Tiny ring + slow consumer: almost every push hits the full path.
  const Result r = run<TypeParam::kP>(n, 64, true);
  EXPECT_EQ(r.torn, 0u);
  EXPECT_EQ(r.out_of_order, 0u);
  EXPECT_EQ(r.popped + r.dropped, n);
  EXPECT_GT(r.dropped, 0u) << "test didn't actually overload the ring";
  if constexpr (TypeParam::kP == Overflow::kDropOldest) {
    // Drop-oldest always keeps the newest sample.
    EXPECT_EQ(r.last, static_cast<std::int64_t>(n - 1));
  }
}

}  // namespace
}  // namespace flux
