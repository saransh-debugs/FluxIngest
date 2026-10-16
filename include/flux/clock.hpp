#pragma once

#include <chrono>
#include <cstdint>

namespace flux {

// Wall clock, ns since the Unix epoch. Used for sample timestamps and for
// naming files by time window.
inline std::int64_t wall_ns() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

// Monotonic clock for deadlines and durations. Never jumps when NTP adjusts
// the wall clock.
inline std::int64_t mono_ns() noexcept {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace flux
