#pragma once

// Small seeded string hash in the style of wyhash: reads 8 bytes at a time and
// mixes with one 64x64->128 multiply.
//
// Why not std::hash<string_view>: libc++ and libstdc++ use unseeded hashes.
// A client that knows the hash can send tag values that all land in one
// bucket, turning every lookup into a linear scan (hash flooding). The seed is
// random per process, so collisions can't be precomputed.
// Why not FNV-1a: it does one multiply per byte; this does one per 16 bytes.

#include <cstdint>
#include <cstring>
#include <string_view>

namespace flux {

namespace detail {
__extension__ using u128 = unsigned __int128;

inline std::uint64_t mix(std::uint64_t a, std::uint64_t b) noexcept {
  const u128 r = static_cast<u128>(a) * b;
  return static_cast<std::uint64_t>(r) ^ static_cast<std::uint64_t>(r >> 64);
}
inline std::uint64_t read64(const char* p) noexcept {
  std::uint64_t v;
  std::memcpy(&v, p, 8);
  return v;
}
inline std::uint64_t read32(const char* p) noexcept {
  std::uint32_t v;
  std::memcpy(&v, p, 4);
  return v;
}
}  // namespace detail

inline std::uint64_t hash_bytes(std::string_view s, std::uint64_t seed) noexcept {
  using detail::mix;
  using detail::read32;
  using detail::read64;
  constexpr std::uint64_t k0 = 0xa0761d6478bd642full;
  constexpr std::uint64_t k1 = 0xe7037ed1a0b428dbull;
  constexpr std::uint64_t k2 = 0x8ebc6af09c88c6e3ull;

  const char* p = s.data();
  std::size_t n = s.size();
  std::uint64_t h = mix(seed ^ k0, static_cast<std::uint64_t>(n) ^ k1);
  while (n > 16) {
    h = mix(read64(p) ^ k1, read64(p + 8) ^ h);
    p += 16;
    n -= 16;
  }
  std::uint64_t a = 0;
  std::uint64_t b = 0;
  if (n >= 8) {
    a = read64(p);
    b = read64(p + n - 8);
  } else if (n >= 4) {
    a = read32(p);
    b = read32(p + n - 4);
  } else if (n > 0) {
    const auto* u = reinterpret_cast<const unsigned char*>(p);
    a = (static_cast<std::uint64_t>(u[0]) << 16) | (static_cast<std::uint64_t>(u[n >> 1]) << 8) | u[n - 1];
  }
  h = mix(a ^ k1, b ^ h);
  return mix(h ^ k2, k0);
}

}  // namespace flux
