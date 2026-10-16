#include "flux/spsc_ring.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <vector>

#include "flux/sample.hpp"

namespace flux {
namespace {

Sample make(std::int64_t i) { return Sample{i, static_cast<double>(i), static_cast<std::uint32_t>(i), 7}; }

template <class R>
class RingTest : public ::testing::Test {};
using Policies = ::testing::Types<SpscRing<Sample, Overflow::kDropNewest>, SpscRing<Sample, Overflow::kDropOldest>>;
TYPED_TEST_SUITE(RingTest, Policies);

TYPED_TEST(RingTest, RejectsNonPowerOfTwo) {
  EXPECT_THROW(TypeParam(0), std::invalid_argument);
  EXPECT_THROW(TypeParam(6), std::invalid_argument);
  EXPECT_NO_THROW(TypeParam(8));
}

TYPED_TEST(RingTest, EmptyPopReturnsZero) {
  TypeParam r(8);
  std::array<Sample, 4> out{};
  EXPECT_EQ(r.pop(out.data(), out.size()), 0u);
}

TYPED_TEST(RingTest, KeepsOrderAcrossWrapAround) {
  TypeParam r(8);
  std::array<Sample, 8> out{};
  std::int64_t next_in = 0;
  std::int64_t next_out = 0;
  // Push 5, pop 3, repeatedly: indexes wrap the 8-slot buffer many times.
  for (int round = 0; round < 100; ++round) {
    for (int k = 0; k < 5 && r.size_approx() < 8; ++k) {
      ASSERT_EQ(r.push(make(next_in++)), PushResult::kPushed);
    }
    const std::size_t n = r.pop(out.data(), 3);
    for (std::size_t k = 0; k < n; ++k) {
      EXPECT_EQ(out[k].ts_ns, next_out);
      EXPECT_EQ(out[k].value, static_cast<double>(next_out));
      ++next_out;
    }
  }
  while (std::size_t n = r.pop(out.data(), out.size())) {
    for (std::size_t k = 0; k < n; ++k) EXPECT_EQ(out[k].ts_ns, next_out++);
  }
  EXPECT_EQ(next_in, next_out);
  EXPECT_EQ(r.dropped(), 0u);
}

TYPED_TEST(RingTest, PopRespectsMax) {
  TypeParam r(16);
  for (int i = 0; i < 10; ++i) r.push(make(i));
  std::array<Sample, 16> out{};
  EXPECT_EQ(r.pop(out.data(), 4), 4u);
  EXPECT_EQ(out[3].ts_ns, 3);
  EXPECT_EQ(r.pop(out.data(), 16), 6u);
  EXPECT_EQ(out[0].ts_ns, 4);
}

TYPED_TEST(RingTest, TryPushNeverDrops) {
  TypeParam r(4);
  for (int i = 0; i < 4; ++i) EXPECT_TRUE(r.try_push(make(i)));
  EXPECT_FALSE(r.try_push(make(4)));
  EXPECT_EQ(r.dropped(), 0u);
  std::array<Sample, 4> out{};
  ASSERT_EQ(r.pop(out.data(), out.size()), 4u);
  EXPECT_EQ(out[0].ts_ns, 0);
  EXPECT_TRUE(r.try_push(make(5)));
}

TEST(DropNewest, FullRingRejectsNewSamples) {
  SpscRing<Sample, Overflow::kDropNewest> r(4);
  for (int i = 0; i < 4; ++i) EXPECT_EQ(r.push(make(i)), PushResult::kPushed);
  EXPECT_EQ(r.push(make(4)), PushResult::kDroppedNewest);
  EXPECT_EQ(r.push(make(5)), PushResult::kDroppedNewest);
  EXPECT_EQ(r.dropped(), 2u);
  std::array<Sample, 8> out{};
  ASSERT_EQ(r.pop(out.data(), out.size()), 4u);
  for (int i = 0; i < 4; ++i) EXPECT_EQ(out[static_cast<std::size_t>(i)].ts_ns, i);  // oldest kept
}

TEST(DropOldest, FullRingEvictsOldestSamples) {
  SpscRing<Sample, Overflow::kDropOldest> r(4);
  for (int i = 0; i < 4; ++i) EXPECT_EQ(r.push(make(i)), PushResult::kPushed);
  EXPECT_EQ(r.push(make(4)), PushResult::kOverwroteOldest);
  EXPECT_EQ(r.push(make(5)), PushResult::kOverwroteOldest);
  EXPECT_EQ(r.dropped(), 2u);
  std::array<Sample, 8> out{};
  ASSERT_EQ(r.pop(out.data(), out.size()), 4u);
  for (int i = 0; i < 4; ++i) EXPECT_EQ(out[static_cast<std::size_t>(i)].ts_ns, i + 2);  // newest kept
}

TEST(DropOldest, ManyOverwritesKeepLastCapacity) {
  SpscRing<Sample, Overflow::kDropOldest> r(8);
  for (int i = 0; i < 1000; ++i) r.push(make(i));
  EXPECT_EQ(r.dropped(), 992u);
  std::array<Sample, 8> out{};
  ASSERT_EQ(r.pop(out.data(), out.size()), 8u);
  for (int i = 0; i < 8; ++i) EXPECT_EQ(out[static_cast<std::size_t>(i)].ts_ns, 992 + i);
}

TEST(DropNewest, FreedSpaceIsReusable) {
  SpscRing<Sample, Overflow::kDropNewest> r(2);
  r.push(make(0));
  r.push(make(1));
  EXPECT_EQ(r.push(make(2)), PushResult::kDroppedNewest);
  std::array<Sample, 1> out{};
  ASSERT_EQ(r.pop(out.data(), 1), 1u);
  EXPECT_EQ(r.push(make(3)), PushResult::kPushed);
}

}  // namespace
}  // namespace flux
