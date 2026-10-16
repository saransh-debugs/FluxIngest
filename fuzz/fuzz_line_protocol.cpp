// libFuzzer target for everything that touches untrusted bytes: line
// splitting, parsing, tag canonicalization, unescaping, interning.
//
//   scripts/fuzz.sh 600     # 10 minutes under ASan + UBSan

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "flux/ingestor.hpp"
#include "flux/spsc_ring.hpp"

namespace {
using Ring = flux::SpscRing<flux::Sample, flux::Overflow::kDropOldest>;

struct State {
  flux::StringInterner metrics{4096, 1 << 20, 1};
  flux::StringInterner tags{4096, 1 << 20, 2};
  Ring ring{1024};
  flux::ProducerStats stats;
  flux::LineIngestor<Ring> ing{metrics, tags, ring, stats};
  flux::Sample out[1024];
};
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  static State* s = new State();  // persistent: interner caps get exercised too
  const std::string_view in(reinterpret_cast<const char*>(data), size);

  flux::ParsedLine p;
  if (flux::parse_line(in, p) == flux::ParseStatus::kOk) {
    char buf[64];
    (void)flux::unescape(p.measurement, buf, sizeof buf, true);
    for (std::uint32_t i = 0; i < p.n_tags; ++i) (void)flux::unescape(p.tags[i].value, buf, sizeof buf, false);
  }
  s->ing.ingest_packet(in, 0);
  while (s->ring.pop(s->out, 1024) != 0) {
  }
  return 0;
}
