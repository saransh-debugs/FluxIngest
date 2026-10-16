#pragma once

// Producer-side logic: one line of text in, zero or more Samples into the
// ring. Runs on the receiver thread. Allocation-free: all scratch space is
// members of this object, which is created once at startup.

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "flux/config.hpp"
#include "flux/counters.hpp"
#include "flux/interner.hpp"
#include "flux/line_protocol.hpp"
#include "flux/sample.hpp"

namespace flux {

template <class Ring>
class LineIngestor {
 public:
  LineIngestor(StringInterner& metrics, StringInterner& tagsets, Ring& ring, ProducerStats& stats) noexcept
      : metrics_(metrics), tagsets_(tagsets), ring_(ring), stats_(stats) {}

  // One datagram may hold many lines.
  void ingest_packet(std::string_view packet, std::int64_t now_ns) noexcept {
    stats_.datagrams.add();
    for_each_line(packet, [&](std::string_view l) { ingest_line(l, now_ns); });
  }

  ParseStatus ingest_line(std::string_view text, std::int64_t now_ns) noexcept {
    ParseStatus st = parse_line(text, line_);
    if (st == ParseStatus::kBlank || st == ParseStatus::kComment) return st;
    if (st == ParseStatus::kOk) st = build_tagset();
    if (st != ParseStatus::kOk) return reject(st);

    stats_.lines.add();
    if (line_.skipped_fields != 0) stats_.fields_skipped.add(line_.skipped_fields);

    const std::string_view tagset(tagbuf_.data(), tagset_len_);
    const std::uint32_t tags_id = tagsets_.intern(tagset);
    if (tags_id == StringInterner::kFull) {
      stats_.cardinality_rejected.add(line_.n_fields);
      return ParseStatus::kOk;
    }

    // Metric name = unescaped measurement + '.' + unescaped field key.
    std::size_t prefix = unescape(line_.measurement, metricbuf_.data(), metricbuf_.size() - 1, true);
    if (prefix == SIZE_MAX) return reject(ParseStatus::kKeyTooLong);
    metricbuf_[prefix++] = '.';

    const std::int64_t ts = line_.has_timestamp ? line_.timestamp_ns : now_ns;
    for (std::uint32_t f = 0; f < line_.n_fields; ++f) {
      const FieldKV& fk = line_.fields[f];
      const std::size_t flen =
          unescape(fk.key, metricbuf_.data() + prefix, metricbuf_.size() - prefix, false);
      if (flen == SIZE_MAX) {
        reject(ParseStatus::kKeyTooLong);
        continue;
      }
      const std::uint32_t metric_id = metrics_.intern(std::string_view(metricbuf_.data(), prefix + flen));
      if (metric_id == StringInterner::kFull) {
        stats_.cardinality_rejected.add();
        continue;
      }
      // Ring overflow is counted inside the ring; the ingestor never blocks.
      (void)ring_.push(Sample{ts, fk.value, metric_id, tags_id});
      stats_.samples_offered.add();
    }
    return ParseStatus::kOk;
  }

  std::uint64_t status_count(ParseStatus s) const noexcept {
    return by_status_[static_cast<std::size_t>(s)].load();
  }

 private:
  ParseStatus reject(ParseStatus st) noexcept {
    stats_.parse_errors.add();
    by_status_[static_cast<std::size_t>(st)].add();
    return st;
  }

  // Canonical tag set: tags sorted by key, joined as "k=v,k2=v2", kept in
  // escaped form so it stays unambiguous (a value may contain ',' or '=').
  // Sorting makes "host=a,dc=1" and "dc=1,host=a" the same series.
  ParseStatus build_tagset() noexcept {
    auto* first = line_.tags.data();
    auto* last = first + line_.n_tags;
    // n_tags <= 32 and std::sort never allocates; for small n it's an
    // insertion sort.
    std::sort(first, last, [](const TagKV& a, const TagKV& b) { return a.key.raw < b.key.raw; });
    std::size_t len = 0;
    for (std::uint32_t t = 0; t < line_.n_tags; ++t) {
      const TagKV& kv = line_.tags[t];
      if (t > 0 && kv.key.raw == line_.tags[t - 1].key.raw) return ParseStatus::kDuplicateTag;
      const std::size_t need = (t > 0 ? 1 : 0) + kv.key.raw.size() + 1 + kv.value.raw.size();
      if (len + need > tagbuf_.size()) return ParseStatus::kKeyTooLong;
      if (t > 0) tagbuf_[len++] = ',';
      std::memcpy(tagbuf_.data() + len, kv.key.raw.data(), kv.key.raw.size());
      len += kv.key.raw.size();
      tagbuf_[len++] = '=';
      std::memcpy(tagbuf_.data() + len, kv.value.raw.data(), kv.value.raw.size());
      len += kv.value.raw.size();
    }
    tagset_len_ = len;
    return ParseStatus::kOk;
  }

  StringInterner& metrics_;
  StringInterner& tagsets_;
  Ring& ring_;
  ProducerStats& stats_;
  ParsedLine line_;
  std::array<char, kMaxTagsetBytes> tagbuf_{};
  std::array<char, kMaxMetricBytes> metricbuf_{};
  std::size_t tagset_len_ = 0;
  std::array<OwnedCounter, static_cast<std::size_t>(ParseStatus::kCount_)> by_status_{};
};

}  // namespace flux
