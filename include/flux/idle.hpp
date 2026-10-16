#pragma once

// What a polling thread does when there's no work.
//
// Pure busy-spinning gives the lowest latency but burns a whole core at 100%
// forever, even when no metrics arrive. Sleeping right away adds ~50-100 us
// of wake-up latency to the first sample after a quiet period.
// So we escalate: spin with `pause` a little, then yield, then sleep with a
// short fixed period. Any work resets it. (Aeron's BackoffIdleStrategy.)

#include <chrono>
#include <cstdint>
#include <thread>

#include "flux/cpu.hpp"

namespace flux {

class BackoffIdle {
 public:
  explicit BackoffIdle(std::chrono::microseconds max_sleep = std::chrono::microseconds(200))
      : max_sleep_(max_sleep) {}

  void idle() noexcept {
    if (n_ < kSpins) {
      cpu_relax();
    } else if (n_ < kSpins + kYields) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(max_sleep_);
    }
    if (n_ < kSpins + kYields) ++n_;
  }

  void reset() noexcept { n_ = 0; }

 private:
  static constexpr std::uint32_t kSpins = 64;
  static constexpr std::uint32_t kYields = 16;
  std::chrono::microseconds max_sleep_;
  std::uint32_t n_ = 0;
};

}  // namespace flux
