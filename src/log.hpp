#pragma once

// Minimal logging to stderr. Only used off the hot path (startup, errors on
// the writer thread, periodic stats).

#include <cstdio>

#define FLUX_LOG(level, ...)                     \
  do {                                           \
    std::fprintf(stderr, "[fluxingest] " level ": " __VA_ARGS__); \
    std::fputc('\n', stderr);                    \
  } while (0)

#define FLUX_INFO(...) FLUX_LOG("info", __VA_ARGS__)
#define FLUX_WARN(...) FLUX_LOG("warn", __VA_ARGS__)
#define FLUX_ERROR(...) FLUX_LOG("error", __VA_ARGS__)
