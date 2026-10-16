#pragma once

#include <cstdint>
#include <type_traits>

namespace flux {

// One metric sample as it travels through the SPSC ring.
//
// 24 bytes, no pointers, no strings. Strings (metric name, tag set) are
// interned by the producer, so the ring only ever moves small fixed-size
// values. A std::string here would mean a malloc per sample and a pointer the
// consumer has to chase into another core's cache.
struct Sample {
  std::int64_t ts_ns;     // Unix time, nanoseconds
  double value;
  std::uint32_t metric_id;  // StringInterner id of "measurement.field"
  std::uint32_t tags_id;    // StringInterner id of canonical "k=v,k2=v2"
};

static_assert(sizeof(Sample) == 24);
static_assert(std::is_trivially_copyable_v<Sample>);

}  // namespace flux
