#include "flux/ingestor.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

#include "flux/spsc_ring.hpp"

namespace flux {
namespace {

using Ring = SpscRing<Sample, Overflow::kDropOldest>;

struct Fixture {
  StringInterner metrics{64, 4096, 11};
  StringInterner tags{64, 4096, 12};
  Ring ring{64};
  ProducerStats stats;
  LineIngestor<Ring> ing{metrics, tags, ring, stats};

  std::vector<Sample> drain() {
    std::vector<Sample> out(64);
    out.resize(ring.pop(out.data(), out.size()));
    return out;
  }
};

TEST(Ingestor, OneSamplePerNumericField) {
  Fixture f;
  ASSERT_EQ(f.ing.ingest_line("cpu,host=a usage=0.5,idle=99i,name=\"x\" 100", 999), ParseStatus::kOk);
  auto s = f.drain();
  ASSERT_EQ(s.size(), 2u);
  EXPECT_EQ(f.metrics.get(s[0].metric_id), "cpu.usage");
  EXPECT_EQ(f.metrics.get(s[1].metric_id), "cpu.idle");
  EXPECT_EQ(s[0].tags_id, s[1].tags_id);
  EXPECT_EQ(f.tags.get(s[0].tags_id), "host=a");
  EXPECT_EQ(s[0].ts_ns, 100);
  EXPECT_DOUBLE_EQ(s[1].value, 99.0);
  EXPECT_EQ(f.stats.samples_offered.load(), 2u);
  EXPECT_EQ(f.stats.fields_skipped.load(), 1u);
}

TEST(Ingestor, MissingTimestampUsesReceiveTime) {
  Fixture f;
  f.ing.ingest_line("cpu v=1", 12345);
  EXPECT_EQ(f.drain().at(0).ts_ns, 12345);
}

TEST(Ingestor, TagOrderDoesNotMatter) {
  Fixture f;
  f.ing.ingest_line("cpu,region=us,host=a v=1", 0);
  f.ing.ingest_line("cpu,host=a,region=us v=2", 0);
  auto s = f.drain();
  ASSERT_EQ(s.size(), 2u);
  EXPECT_EQ(s[0].tags_id, s[1].tags_id);
  EXPECT_EQ(f.tags.get(s[0].tags_id), "host=a,region=us");
}

TEST(Ingestor, TagsetKeepsEscapes) {
  Fixture f;
  f.ing.ingest_line(R"(my\ cpu,dc=us\,east v=1)", 0);
  auto s = f.drain();
  ASSERT_EQ(s.size(), 1u);
  EXPECT_EQ(f.metrics.get(s[0].metric_id), "my cpu.v");  // names are unescaped
  EXPECT_EQ(f.tags.get(s[0].tags_id), R"(dc=us\,east)");  // tag set stays unambiguous
}

TEST(Ingestor, DuplicateTagRejected) {
  Fixture f;
  EXPECT_EQ(f.ing.ingest_line("cpu,host=a,host=b v=1", 0), ParseStatus::kDuplicateTag);
  EXPECT_EQ(f.stats.parse_errors.load(), 1u);
  EXPECT_TRUE(f.drain().empty());
}

TEST(Ingestor, TagsetTooLongRejected) {
  Fixture f;
  std::string line = "cpu,k=" + std::string(kMaxTagsetBytes, 'x') + " v=1";
  EXPECT_EQ(f.ing.ingest_line(line, 0), ParseStatus::kKeyTooLong);
  EXPECT_EQ(f.ing.status_count(ParseStatus::kKeyTooLong), 1u);
}

TEST(Ingestor, BadLinesCountedAndSkipped) {
  Fixture f;
  f.ing.ingest_packet("cpu v=1\ngarbage\n# comment\n\ncpu v=2\n", 0);
  EXPECT_EQ(f.drain().size(), 2u);
  EXPECT_EQ(f.stats.lines.load(), 2u);
  EXPECT_EQ(f.stats.parse_errors.load(), 1u);
  EXPECT_EQ(f.stats.datagrams.load(), 1u);
}

TEST(Ingestor, CardinalityCapDropsAndCounts) {
  StringInterner metrics{64, 4096, 1};
  StringInterner tags{2, 4096, 2};  // only 2 tag sets allowed
  Ring ring{64};
  ProducerStats stats;
  LineIngestor<Ring> ing{metrics, tags, ring, stats};
  ing.ingest_line("cpu,id=1 a=1,b=2", 0);
  ing.ingest_line("cpu,id=2 a=1,b=2", 0);
  ing.ingest_line("cpu,id=3 a=1,b=2", 0);  // over the cap: both fields dropped
  ing.ingest_line("cpu,id=1 a=1", 0);      // existing series still accepted
  EXPECT_EQ(stats.samples_offered.load(), 5u);
  EXPECT_EQ(stats.cardinality_rejected.load(), 2u);
  EXPECT_EQ(ring.size_approx(), 5u);
}

}  // namespace
}  // namespace flux
