#include "flux/interner.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <set>
#include <string>
#include <thread>

namespace flux {
namespace {

TEST(Interner, SameStringSameId) {
  StringInterner in(16, 1024, 42);
  const auto a = in.intern("host=a");
  const auto b = in.intern("host=b");
  EXPECT_NE(a, b);
  EXPECT_EQ(in.intern("host=a"), a);
  EXPECT_EQ(in.intern(std::string("host=") + "b"), b);
  EXPECT_EQ(in.get(a), "host=a");
  EXPECT_EQ(in.get(b), "host=b");
  EXPECT_EQ(in.size(), 2u);
}

TEST(Interner, IdsAreDense) {
  StringInterner in(100, 4096, 1);
  for (std::uint32_t i = 0; i < 100; ++i) EXPECT_EQ(in.intern("s" + std::to_string(i)), i);
}

TEST(Interner, EmptyStringIsValid) {
  StringInterner in(4, 16, 1);
  const auto id = in.intern("");
  ASSERT_NE(id, StringInterner::kFull);
  EXPECT_EQ(in.get(id), "");
  EXPECT_EQ(in.intern(""), id);
}

TEST(Interner, CountCapReturnsFullButKeepsOldIds) {
  StringInterner in(3, 1024, 1);
  EXPECT_EQ(in.intern("a"), 0u);
  EXPECT_EQ(in.intern("b"), 1u);
  EXPECT_EQ(in.intern("c"), 2u);
  EXPECT_EQ(in.intern("d"), StringInterner::kFull);
  EXPECT_EQ(in.intern("b"), 1u);  // existing strings still resolve
  EXPECT_EQ(in.size(), 3u);
}

TEST(Interner, ArenaCapReturnsFull) {
  StringInterner in(100, 10, 1);
  EXPECT_NE(in.intern("12345"), StringInterner::kFull);
  EXPECT_NE(in.intern("67890"), StringInterner::kFull);
  EXPECT_EQ(in.intern("x"), StringInterner::kFull);
  EXPECT_EQ(in.arena_used(), 10u);
}

TEST(Interner, PrefixesAreDistinct) {
  StringInterner in(16, 1024, 7);
  const auto a = in.intern("abc");
  const auto b = in.intern("ab");
  const auto c = in.intern("abcd");
  EXPECT_EQ(std::set<std::uint32_t>({a, b, c}).size(), 3u);
}

TEST(Interner, HandlesManyCollidingTags) {
  // Fill to capacity (table is 50% full at most), then look everything up.
  constexpr std::uint32_t kN = 50'000;
  StringInterner in(kN, kN * 16, 99);
  for (std::uint32_t i = 0; i < kN; ++i) ASSERT_EQ(in.intern("series-" + std::to_string(i)), i);
  for (std::uint32_t i = 0; i < kN; ++i) ASSERT_EQ(in.intern("series-" + std::to_string(i)), i);
  EXPECT_EQ(in.intern("one-more"), StringInterner::kFull);
}

TEST(Hash, SeedChangesHash) {
  EXPECT_NE(hash_bytes("host=a", 1), hash_bytes("host=a", 2));
  EXPECT_NE(hash_bytes("host=a", 1), hash_bytes("host=b", 1));
  // Every length path (0, 1-3, 4-7, 8-16, >16) runs and differs.
  std::set<std::uint64_t> seen;
  std::string s;
  for (int i = 0; i < 40; ++i) {
    seen.insert(hash_bytes(s, 5));
    s.push_back('x');
  }
  EXPECT_EQ(seen.size(), 40u);
}

// One writer interning, one reader reading every id below size(). Under TSan
// this proves the release/acquire publication is enough: the reader never
// sees a half-written entry.
TEST(Interner, ConcurrentReaderSeesCompleteEntries) {
  constexpr std::uint32_t kN = 20'000;
  StringInterner in(kN, kN * 16, 3);
  std::atomic<bool> done{false};
  std::uint64_t bad = 0;

  std::thread reader([&] {
    char expect[32];
    std::uint32_t checked = 0;
    while (!done.load(std::memory_order_acquire) || checked < in.size()) {
      const std::uint32_t n = in.size();
      for (; checked < n; ++checked) {
        const int len = std::snprintf(expect, sizeof expect, "v%u", checked);
        if (in.get(checked) != std::string_view(expect, static_cast<std::size_t>(len))) ++bad;
      }
    }
  });
  for (std::uint32_t i = 0; i < kN; ++i) (void)in.intern("v" + std::to_string(i));
  done.store(true, std::memory_order_release);
  reader.join();
  EXPECT_EQ(bad, 0u);
}

}  // namespace
}  // namespace flux
