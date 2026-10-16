#pragma once

// Lock-free single-producer / single-consumer ring buffer with a lossy
// overflow policy.
//
// Why not std::mutex + std::queue: a mutex costs two atomic RMWs per
// operation even uncontended, and when contended the loser is put to sleep by
// the kernel (microseconds). std::queue/deque also mallocs as it grows. Here a
// push is one plain store of the data plus one release store of the index.
//
// Indexes are 64-bit counters that only ever go up (they'd take ~58 years to
// wrap at 10 G ops/s). The slot is `index & mask`. Because the counters never
// wrap, "full" is just `tail - head == capacity`. No wasted slot, no extra flag.
//
// Two overflow policies, picked at compile time:
//
//  kDropNewest  Full ring rejects the new sample. Wait-free: push and pop
//               finish in a fixed number of steps no matter what the other
//               thread does. Each index has exactly one writer thread.
//
//  kDropOldest  Full ring evicts the oldest sample to make room. This breaks
//               the SPSC rule "only the consumer moves head": the producer
//               must move it too. So head becomes a CAS'd variable, and the
//               consumer may be copying a slot at the same moment the producer
//               overwrites it. The protocol (same idea as a seqlock):
//
//    producer, ring full:  CAS head h -> h+1 ("steal" index h)
//                          release fence
//                          write the slot (index h+cap reuses h's slot)
//    consumer:             copy slots [h, h+n) with relaxed atomic loads
//                          acquire fence
//                          CAS head h -> h+n
//                          on failure head is now h' > h: the producer stole
//                          [h, h'). Our copies of [h', h+n) are still good,
//                          keep those and CAS again from h'.
//
//  Why the copies of [h', h+n) are safe: if a relaxed load had seen data the
//  producer wrote after stealing index j, then the producer's release fence
//  synchronizes with our acquire fence, so the steal of j happens-before our
//  CAS, so the CAS must read a head > j. Contrapositive: every j >= h' (the
//  value our CAS read) was copied before anyone stole it.
//
//  Slot words are std::atomic<uint64_t> accessed with memory_order_relaxed.
//  On x86 and ARM64 that compiles to an ordinary load/store, so it is free,
//  but it makes the racy read legal C++ (a plain memcpy would be a data race,
//  which is undefined behavior, and ThreadSanitizer would rightly flag it).
//
//  Cost: the producer pays one CAS per push while the ring is full, and the
//  consumer pays one CAS per batch instead of a plain store. bench_ring.cpp
//  measures both policies.

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <stdexcept>
#include <type_traits>

#include "flux/config.hpp"

namespace flux {

enum class Overflow : std::uint8_t { kDropNewest, kDropOldest };

enum class PushResult : std::uint8_t {
  kPushed,           // stored, nothing lost
  kDroppedNewest,    // ring full, this sample was rejected
  kOverwroteOldest,  // ring full, stored by evicting the oldest sample
};

template <class T, Overflow P>
class SpscRing {
  static_assert(std::is_trivially_copyable_v<T>, "ring moves raw bytes");
  static_assert(sizeof(T) % 8 == 0, "slots are copied as 8-byte words");

  static constexpr std::size_t kWords = sizeof(T) / 8;
  struct AtomicSlot {
    std::array<std::atomic<std::uint64_t>, kWords> w;
  };
  using Slot = std::conditional_t<P == Overflow::kDropOldest, AtomicSlot, T>;

 public:
  using value_type = T;
  static constexpr Overflow kPolicy = P;

  // Allocates and zero-fills every slot once, up front. Zero-filling touches
  // every page now, so the hot path never takes a first-touch page fault.
  explicit SpscRing(std::size_t capacity)
      : cap_(capacity), mask_(capacity - 1), slots_(new Slot[capacity]()) {
    if (!is_pow2(capacity)) {
      throw std::invalid_argument("SpscRing capacity must be a power of two");
    }
  }

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  // ---- producer thread only ----

  PushResult push(const T& v) noexcept {
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);
    PushResult result = PushResult::kPushed;

    // head_cache_ is a stale copy of head_. head_ only grows, so if even the
    // stale copy says "not full", it isn't. We only touch the consumer's cache
    // line when the ring looks full. Without this, every push would pull the
    // consumer's line across cores.
    if (t - head_cache_ >= cap_) [[unlikely]] {
      head_cache_ = head_.load(std::memory_order_acquire);
      if (t - head_cache_ >= cap_) {
        if constexpr (P == Overflow::kDropNewest) {
          bump(dropped_);
          return PushResult::kDroppedNewest;
        } else {
          std::uint64_t h = head_cache_;
          if (head_.compare_exchange_strong(h, h + 1, std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
            bump(dropped_);
            head_cache_ = h + 1;
            result = PushResult::kOverwroteOldest;
            // Pairs with the consumer's acquire fence (see header comment).
            std::atomic_thread_fence(std::memory_order_release);
          } else {
            // The consumer advanced head in the meantime, so there's room.
            // Its CAS (release) synchronized with our failed CAS (acquire),
            // so it is done reading the slot we're about to reuse.
            head_cache_ = h;
          }
        }
      }
    }

    store_slot(slots_[t & mask_], v);
    tail_.store(t + 1, std::memory_order_release);
    return result;
  }

  // Non-lossy push: returns false when full and changes nothing. For callers
  // that would rather wait or retry than drop (and for benchmarks that must
  // move every item). Same fast path as push().
  [[nodiscard]] bool try_push(const T& v) noexcept {
    const std::uint64_t t = tail_.load(std::memory_order_relaxed);
    if (t - head_cache_ >= cap_) [[unlikely]] {
      head_cache_ = head_.load(std::memory_order_acquire);
      if (t - head_cache_ >= cap_) return false;
    }
    store_slot(slots_[t & mask_], v);
    tail_.store(t + 1, std::memory_order_release);
    return true;
  }

  // ---- consumer thread only ----

  // Copies up to `max` samples into `out`, oldest first. Returns how many.
  // One index update per batch, not per sample: the producer sees one cache
  // line transfer per batch.
  std::size_t pop(T* out, std::size_t max) noexcept {
    if constexpr (P == Overflow::kDropNewest) {
      return pop_exclusive(out, max);
    } else {
      return pop_contended(out, max);
    }
  }

  // ---- any thread ----

  std::uint64_t dropped() const noexcept { return dropped_.load(std::memory_order_relaxed); }
  std::size_t capacity() const noexcept { return cap_; }
  std::size_t size_approx() const noexcept {
    const std::uint64_t h = head_.load(std::memory_order_acquire);
    const std::uint64_t t = tail_.load(std::memory_order_acquire);
    return t > h ? static_cast<std::size_t>(std::min<std::uint64_t>(t - h, cap_)) : 0;
  }

  static constexpr std::size_t memory_bytes(std::size_t capacity) noexcept {
    return capacity * sizeof(Slot) + sizeof(SpscRing);
  }

 private:
  // Single-writer counter: a load + store instead of fetch_add. fetch_add is a
  // locked instruction on x86 (~20 cycles); we don't need it with one writer.
  static void bump(std::atomic<std::uint64_t>& c) noexcept {
    c.store(c.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
  }

  static void store_slot(Slot& s, const T& v) noexcept {
    if constexpr (P == Overflow::kDropNewest) {
      s = v;
    } else {
      std::array<std::uint64_t, kWords> w;
      std::memcpy(w.data(), &v, sizeof(T));
      for (std::size_t i = 0; i < kWords; ++i) s.w[i].store(w[i], std::memory_order_relaxed);
    }
  }

  static void load_slot(const AtomicSlot& s, T& out) noexcept {
    std::array<std::uint64_t, kWords> w;
    for (std::size_t i = 0; i < kWords; ++i) w[i] = s.w[i].load(std::memory_order_relaxed);
    std::memcpy(&out, w.data(), sizeof(T));
  }

  std::size_t pop_exclusive(T* out, std::size_t max) noexcept {
    const std::uint64_t h = head_.load(std::memory_order_relaxed);
    std::uint64_t avail = tail_cache_ - h;
    if (avail < max) {
      tail_cache_ = tail_.load(std::memory_order_acquire);
      avail = tail_cache_ - h;
      if (avail == 0) return 0;
    }
    const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(avail, max));
    const std::size_t start = static_cast<std::size_t>(h & mask_);
    const std::size_t first = std::min(n, cap_ - start);
    // Two memcpys handle the wrap-around. memcpy is SIMD; a per-element loop
    // with a mask on every index would not vectorize as well.
    std::memcpy(out, &slots_[start], first * sizeof(T));
    std::memcpy(out + first, &slots_[0], (n - first) * sizeof(T));
    head_.store(h + n, std::memory_order_release);
    return n;
  }

  std::size_t pop_contended(T* out, std::size_t max) noexcept {
    std::uint64_t h = head_.load(std::memory_order_acquire);
    const std::uint64_t t = tail_.load(std::memory_order_acquire);
    if (t <= h) return 0;
    const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(t - h, max));
    for (std::size_t i = 0; i < n; ++i) load_slot(slots_[(h + i) & mask_], out[i]);

    std::atomic_thread_fence(std::memory_order_acquire);

    const std::uint64_t start = h;
    const std::uint64_t end = h + n;
    // Each failed CAS means the producer stole at least one more of our
    // samples, so this loop runs at most n+1 times and never re-copies.
    // Re-copying on failure instead would livelock under sustained overload:
    // a big copy is always slower than the producer's next steal.
    while (!head_.compare_exchange_weak(h, end, std::memory_order_acq_rel,
                                        std::memory_order_acquire)) {
      if (h >= end) return 0;  // everything we copied was evicted
    }
    if (h > start) {
      std::memmove(out, out + (h - start), static_cast<std::size_t>(end - h) * sizeof(T));
    }
    return static_cast<std::size_t>(end - h);
  }

  // Producer-owned cache line.
  alignas(kCacheLine) std::atomic<std::uint64_t> tail_{0};
  std::uint64_t head_cache_ = 0;
  std::atomic<std::uint64_t> dropped_{0};

  // Consumer-owned cache line. (kDropOldest: the producer CASes head_ too, but
  // only while the ring is full.)
  alignas(kCacheLine) std::atomic<std::uint64_t> head_{0};
  std::uint64_t tail_cache_ = 0;

  // Read-only after construction, so both cores can cache it.
  alignas(kCacheLine) const std::size_t cap_;
  const std::uint64_t mask_;
  const std::unique_ptr<Slot[]> slots_;
};

}  // namespace flux
