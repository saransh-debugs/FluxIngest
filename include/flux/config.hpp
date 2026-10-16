#pragma once

#include <cstddef>
#include <cstdint>

namespace flux {

// 128, not 64. Intel's L2 "adjacent line" prefetcher fetches cache lines in
// pairs, so two hot atomics 64 bytes apart still bounce between cores.
// folly and crossbeam pad to 128 on x86 for the same reason.
// (std::hardware_destructive_interference_size would say 64, and Apple's
// libc++ doesn't define it anyway.)
inline constexpr std::size_t kCacheLine = 128;

// Parser limits. Fixed so every buffer can live inside a preallocated object.
inline constexpr std::size_t kMaxTags = 32;
inline constexpr std::size_t kMaxFields = 32;
inline constexpr std::size_t kMaxTagsetBytes = 1024;  // canonical "k=v,k2=v2"
inline constexpr std::size_t kMaxMetricBytes = 256;   // "measurement.field"

// Largest UDP payload we accept. 64 KiB is the IPv4 datagram limit.
inline constexpr std::size_t kMaxDatagram = 65536;

constexpr bool is_pow2(std::uint64_t x) noexcept { return x != 0 && (x & (x - 1)) == 0; }

constexpr std::uint64_t next_pow2(std::uint64_t x) noexcept {
  std::uint64_t p = 1;
  while (p < x) p <<= 1;
  return p;
}

}  // namespace flux
