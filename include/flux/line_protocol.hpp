#pragma once

// InfluxDB line protocol parser.
//
//   cpu,host=a,region=us-west usage=0.5,idle=99i 1700000000000000000
//   └─┬─┘└────────┬────────┘ └─────────┬──────┘ └────────┬────────┘
//  measurement   tag set           field set       timestamp (ns, optional)
//
// Zero-copy and zero-allocation: tokens are string_views into the input
// buffer, and the result goes into a caller-owned ParsedLine with fixed-size
// arrays. A parser that builds std::strings would malloc several times per
// line, which at millions of lines/s is the bottleneck.
//
// Escapes: `\,` `\ ` in measurements; `\,` `\=` `\ ` in tag keys, tag values
// and field keys. A backslash before any other char is a literal backslash
// (same as InfluxDB). Tokens keep their escaped form; `escaped` says whether
// unescape() is needed.
//
// Field values: floats (1.5, -2e3), integers (42i), unsigned (42u), booleans
// (t, true, F, false...) become doubles. String fields ("...") are parsed and
// skipped: this pipeline stores numbers only.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "flux/config.hpp"

namespace flux {

enum class ParseStatus : std::uint8_t {
  kOk,
  kBlank,            // empty or whitespace-only line (not an error)
  kComment,          // line starts with '#' (not an error)
  kBadMeasurement,
  kBadTag,
  kDuplicateTag,
  kTooManyTags,
  kBadField,
  kBadFieldValue,
  kTooManyFields,
  kNoNumericFields,  // only string fields, nothing to store
  kBadTimestamp,
  kKeyTooLong,       // canonical tag set or metric name over the limit
  kCount_,
};

const char* to_string(ParseStatus s) noexcept;

struct Token {
  std::string_view raw;  // as it appears on the wire, escapes included
  bool escaped = false;  // raw contains at least one escape sequence
};

struct TagKV {
  Token key;
  Token value;
};

struct FieldKV {
  Token key;
  double value = 0;
};

struct ParsedLine {
  Token measurement;
  std::array<TagKV, kMaxTags> tags;
  std::array<FieldKV, kMaxFields> fields;
  std::uint32_t n_tags = 0;
  std::uint32_t n_fields = 0;
  std::uint32_t skipped_fields = 0;  // string fields
  std::int64_t timestamp_ns = 0;
  bool has_timestamp = false;
};

// Parses one line (no trailing '\n'; a trailing '\r' is tolerated).
// On any status other than kOk, `out` is unspecified.
ParseStatus parse_line(std::string_view line, ParsedLine& out) noexcept;

// Writes `t` without escape backslashes into dst. Returns the length, or
// SIZE_MAX if it doesn't fit in `cap` bytes. `is_measurement` selects the
// measurement escape set (no `\=`).
std::size_t unescape(Token t, char* dst, std::size_t cap, bool is_measurement) noexcept;

// Calls fn(line) for every line in a packet. Handles \n and \r\n, skips
// empty lines, and handles a missing final newline.
template <class Fn>
void for_each_line(std::string_view buf, Fn&& fn) {
  std::size_t pos = 0;
  while (pos < buf.size()) {
    std::size_t nl = buf.find('\n', pos);
    if (nl == std::string_view::npos) nl = buf.size();
    if (nl > pos) fn(buf.substr(pos, nl - pos));
    pos = nl + 1;
  }
}

}  // namespace flux
