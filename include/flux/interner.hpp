#pragma once

// String interner: maps each distinct string to a small dense id (0, 1, 2...).
//
// Single writer, many readers.
//  - Only the producer thread calls intern(). So the hash index needs no
//    locks and no atomics. A concurrent hash map would pay for many writers
//    we don't have.
//  - Strings are copied into an Arena and never move or get freed. So a
//    string_view returned by get() stays valid for the interner's lifetime,
//    and any thread can read an entry once it has seen its id.
//  - The entry count is published with a release store. A reader that
//    acquire-loads size() can read every entry below it.
//
// Fixed capacity, set at startup. Unbounded tag values ("cardinality
// explosion": someone puts a request id in a tag) is the classic way metrics
// systems run out of memory. When the table or the arena is full, intern()
// returns kFull and the caller drops the sample and counts it.
//
// Hash index: open addressing, linear probing, kept at most 50% full, so a
// lookup almost always touches one cache line. Each 8-byte slot holds the top
// 32 bits of the hash and the id, so most mismatches are rejected without
// reading the string.

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>

#include "flux/arena.hpp"
#include "flux/config.hpp"
#include "flux/hash.hpp"

namespace flux {

class StringInterner {
 public:
  static constexpr std::uint32_t kFull = 0xffffffffu;

  StringInterner(std::uint32_t max_strings, std::size_t arena_bytes, std::uint64_t seed)
      : max_(max_strings),
        table_mask_(next_pow2(std::uint64_t{max_strings} * 2) - 1),
        table_(new std::uint64_t[table_mask_ + 1]()),
        entries_(new Entry[max_strings]()),
        arena_(arena_bytes),
        seed_(seed) {}

  StringInterner(const StringInterner&) = delete;
  StringInterner& operator=(const StringInterner&) = delete;

  std::uint64_t hash(std::string_view s) const noexcept { return hash_bytes(s, seed_); }

  // Producer thread only. Returns the id, or kFull.
  [[nodiscard]] std::uint32_t intern(std::string_view s) noexcept { return intern(s, hash(s)); }

  [[nodiscard]] std::uint32_t intern(std::string_view s, std::uint64_t h) noexcept {
    const std::uint64_t tag = h >> 32;
    std::uint64_t i = h & table_mask_;
    for (;;) {
      const std::uint64_t slot = table_[i];
      if (slot == 0) break;  // empty: not present
      if ((slot >> 32) == tag) {
        const auto id = static_cast<std::uint32_t>(slot) - 1;
        const Entry& e = entries_[id];
        if (e.len == s.size() && std::memcmp(arena_.base() + e.offset, s.data(), s.size()) == 0) {
          return id;
        }
      }
      i = (i + 1) & table_mask_;
    }

    // Insert. Copy the bytes first, then the entry, then publish the count.
    if (local_count_ == max_) return kFull;
    char* dst = arena_.allocate(s.size());
    if (dst == nullptr) return kFull;
    if (!s.empty()) std::memcpy(dst, s.data(), s.size());
    const std::uint32_t id = local_count_++;
    entries_[id] = Entry{static_cast<std::uint64_t>(dst - arena_.base()),
                         static_cast<std::uint32_t>(s.size())};
    table_[i] = (tag << 32) | (std::uint64_t{id} + 1);
    count_.store(local_count_, std::memory_order_release);
    return id;
  }

  // Any thread. `id` must be < size(), or have been received through
  // something that synchronizes with the producer (e.g. the SPSC ring).
  std::string_view get(std::uint32_t id) const noexcept {
    const Entry& e = entries_[id];
    return {arena_.base() + e.offset, e.len};
  }

  std::uint32_t size() const noexcept { return count_.load(std::memory_order_acquire); }
  std::uint32_t capacity() const noexcept { return max_; }
  std::size_t arena_used() const noexcept { return arena_.used(); }

  static std::size_t memory_bytes(std::uint32_t max_strings, std::size_t arena_bytes) noexcept {
    return next_pow2(std::uint64_t{max_strings} * 2) * sizeof(std::uint64_t) +
           std::size_t{max_strings} * sizeof(Entry) + arena_bytes;
  }

 private:
  struct Entry {
    std::uint64_t offset;
    std::uint32_t len;
  };

  const std::uint32_t max_;
  const std::uint64_t table_mask_;
  const std::unique_ptr<std::uint64_t[]> table_;
  const std::unique_ptr<Entry[]> entries_;
  Arena arena_;
  const std::uint64_t seed_;
  std::uint32_t local_count_ = 0;  // producer's private copy of count_
  std::atomic<std::uint32_t> count_{0};
};

}  // namespace flux
