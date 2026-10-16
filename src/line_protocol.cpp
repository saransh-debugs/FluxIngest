#include "flux/line_protocol.hpp"

#include <array>
#include <cmath>
#include <system_error>

#include "fast_float/fast_float.h"

namespace flux {

namespace {

// 256-entry lookup table: "is this byte a delimiter?" is one load instead
// of a loop over the delimiter list. The scanner runs once per input byte,
// so this is the parser's inner loop. (string_view::find per byte made the
// whole parse ~2x slower.)
struct CharSet {
  std::array<bool, 256> has{};
  constexpr explicit CharSet(std::string_view chars) {
    for (const char c : chars) has[static_cast<unsigned char>(c)] = true;
  }
  constexpr bool operator()(char c) const { return has[static_cast<unsigned char>(c)]; }
};

constexpr CharSet kMeasurementStops{", "};
constexpr CharSet kKeyStops{",= "};  // tag keys, tag values, field keys

// Advances from i to the first unescaped char in `stops`. A backslash only
// escapes chars that are in `stops`; otherwise it's a literal backslash.
std::size_t scan(std::string_view s, std::size_t i, const CharSet& stops, bool& escaped) noexcept {
  const std::size_t n = s.size();
  while (i < n) {
    const char c = s[i];
    if (stops(c)) break;
    if (c == '\\' && i + 1 < n && stops(s[i + 1])) {
      escaped = true;
      i += 2;
      continue;
    }
    ++i;
  }
  return i;
}

// fast_float's integer parser, not std::from_chars: Apple's libc++
// from_chars took ~50 ns for a 19-digit timestamp vs ~18 ns here.
template <class Int>
bool parse_int_exact(std::string_view s, Int& out) noexcept {
  if (s.empty()) return false;
  const auto [p, ec] = fast_float::from_chars(s.data(), s.data() + s.size(), out);
  return ec == std::errc{} && p == s.data() + s.size();
}

bool parse_value(std::string_view s, double& out) noexcept {
  if (s.empty()) return false;
  const char last = s.back();
  if (last == 'i') {
    std::int64_t v = 0;
    if (!parse_int_exact(s.substr(0, s.size() - 1), v)) return false;
    out = static_cast<double>(v);
    return true;
  }
  if (last == 'u') {
    std::uint64_t v = 0;
    if (!parse_int_exact(s.substr(0, s.size() - 1), v)) return false;
    out = static_cast<double>(v);
    return true;
  }
  const char first = s.front();
  if (first == 't' || first == 'T' || first == 'f' || first == 'F') {
    if (s == "t" || s == "T" || s == "true" || s == "True" || s == "TRUE") {
      out = 1.0;
      return true;
    }
    if (s == "f" || s == "F" || s == "false" || s == "False" || s == "FALSE") {
      out = 0.0;
      return true;
    }
    return false;
  }
  const auto [p, ec] = fast_float::from_chars(s.data(), s.data() + s.size(), out);
  // Line protocol has no NaN/Inf. They'd also poison Parquet min/max stats.
  return ec == std::errc{} && p == s.data() + s.size() && std::isfinite(out);
}

}  // namespace

const char* to_string(ParseStatus s) noexcept {
  switch (s) {
    case ParseStatus::kOk: return "ok";
    case ParseStatus::kBlank: return "blank";
    case ParseStatus::kComment: return "comment";
    case ParseStatus::kBadMeasurement: return "bad_measurement";
    case ParseStatus::kBadTag: return "bad_tag";
    case ParseStatus::kDuplicateTag: return "duplicate_tag";
    case ParseStatus::kTooManyTags: return "too_many_tags";
    case ParseStatus::kBadField: return "bad_field";
    case ParseStatus::kBadFieldValue: return "bad_field_value";
    case ParseStatus::kTooManyFields: return "too_many_fields";
    case ParseStatus::kNoNumericFields: return "no_numeric_fields";
    case ParseStatus::kBadTimestamp: return "bad_timestamp";
    case ParseStatus::kKeyTooLong: return "key_too_long";
    case ParseStatus::kCount_: break;
  }
  return "unknown";
}

ParseStatus parse_line(std::string_view s, ParsedLine& out) noexcept {
  while (!s.empty() && (s.back() == '\r' || s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
  const std::size_t n = s.size();
  std::size_t i = 0;
  while (i < n && (s[i] == ' ' || s[i] == '\t')) ++i;
  if (i == n) return ParseStatus::kBlank;
  if (s[i] == '#') return ParseStatus::kComment;

  out.n_tags = 0;
  out.n_fields = 0;
  out.skipped_fields = 0;
  out.has_timestamp = false;

  // Measurement.
  bool esc = false;
  std::size_t j = scan(s, i, kMeasurementStops, esc);
  if (j == i) return ParseStatus::kBadMeasurement;
  out.measurement = {s.substr(i, j - i), esc};
  i = j;

  // Tag set.
  while (i < n && s[i] == ',') {
    ++i;
    esc = false;
    j = scan(s, i, kKeyStops, esc);
    if (j == i || j == n || s[j] != '=') return ParseStatus::kBadTag;
    const Token key{s.substr(i, j - i), esc};
    i = j + 1;
    esc = false;
    j = scan(s, i, kKeyStops, esc);
    if (j == i || (j < n && s[j] == '=')) return ParseStatus::kBadTag;
    if (out.n_tags == kMaxTags) return ParseStatus::kTooManyTags;
    out.tags[out.n_tags++] = TagKV{key, Token{s.substr(i, j - i), esc}};
    i = j;
  }

  // Exactly one field set must follow, after at least one space.
  if (i == n || s[i] != ' ') return ParseStatus::kBadField;
  while (i < n && s[i] == ' ') ++i;
  if (i == n) return ParseStatus::kBadField;

  for (;;) {
    esc = false;
    j = scan(s, i, kKeyStops, esc);
    if (j == i || j == n || s[j] != '=') return ParseStatus::kBadField;
    const Token key{s.substr(i, j - i), esc};
    i = j + 1;
    if (i == n) return ParseStatus::kBadFieldValue;

    if (s[i] == '"') {
      // String field: find the closing quote, honoring \" and \\.
      std::size_t k = i + 1;
      bool closed = false;
      while (k < n) {
        if (s[k] == '\\' && k + 1 < n && (s[k + 1] == '"' || s[k + 1] == '\\')) {
          k += 2;
          continue;
        }
        if (s[k] == '"') {
          closed = true;
          break;
        }
        ++k;
      }
      if (!closed) return ParseStatus::kBadFieldValue;
      i = k + 1;
      ++out.skipped_fields;
    } else {
      j = i;
      while (j < n && s[j] != ',' && s[j] != ' ') ++j;
      double v = 0;
      if (!parse_value(s.substr(i, j - i), v)) return ParseStatus::kBadFieldValue;
      if (out.n_fields == kMaxFields) return ParseStatus::kTooManyFields;
      out.fields[out.n_fields++] = FieldKV{key, v};
      i = j;
    }

    if (i == n || s[i] == ' ') break;
    if (s[i] != ',') return ParseStatus::kBadField;  // e.g. junk after a closing quote
    ++i;
  }

  // Optional timestamp.
  while (i < n && s[i] == ' ') ++i;
  if (i < n) {
    if (!parse_int_exact(s.substr(i), out.timestamp_ns)) return ParseStatus::kBadTimestamp;
    out.has_timestamp = true;
  }

  if (out.n_fields == 0) return ParseStatus::kNoNumericFields;
  return ParseStatus::kOk;
}

std::size_t unescape(Token t, char* dst, std::size_t cap, bool is_measurement) noexcept {
  const std::string_view s = t.raw;
  if (!t.escaped) {
    if (s.size() > cap) return SIZE_MAX;
    for (std::size_t i = 0; i < s.size(); ++i) dst[i] = s[i];
    return s.size();
  }
  const CharSet& stops = is_measurement ? kMeasurementStops : kKeyStops;
  std::size_t o = 0;
  for (std::size_t i = 0; i < s.size(); ++i) {
    char c = s[i];
    if (c == '\\' && i + 1 < s.size() && stops(s[i + 1])) c = s[++i];
    if (o == cap) return SIZE_MAX;
    dst[o++] = c;
  }
  return o;
}

}  // namespace flux
