#pragma once

namespace flux {

// Tells the CPU we're in a spin loop. On x86 `pause` stops the core from
// speculating ahead in the loop and saves power; on a hyperthreaded core it
// gives cycles to the sibling thread.
inline void cpu_relax() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  asm volatile("yield" ::: "memory");
#endif
}

}  // namespace flux
