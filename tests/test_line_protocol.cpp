#include "flux/line_protocol.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace flux {
namespace {

ParseStatus parse(std::string_view s, ParsedLine& out) { return parse_line(s, out); }

std::string unesc(Token t, bool measurement = false) {
  char buf[256];
  const std::size_t n = unescape(t, buf, sizeof buf, measurement);
  return n == SIZE_MAX ? std::string("<overflow>") : std::string(buf, n);
}

TEST(LineProtocol, FullLine) {
  ParsedLine p;
  ASSERT_EQ(parse("cpu,host=a,region=us usage=0.5,idle=99i 1700000000000000000", p), ParseStatus::kOk);
  EXPECT_EQ(p.measurement.raw, "cpu");
  ASSERT_EQ(p.n_tags, 2u);
  EXPECT_EQ(p.tags[0].key.raw, "host");
  EXPECT_EQ(p.tags[0].value.raw, "a");
  EXPECT_EQ(p.tags[1].key.raw, "region");
  EXPECT_EQ(p.tags[1].value.raw, "us");
  ASSERT_EQ(p.n_fields, 2u);
  EXPECT_EQ(p.fields[0].key.raw, "usage");
  EXPECT_DOUBLE_EQ(p.fields[0].value, 0.5);
  EXPECT_EQ(p.fields[1].key.raw, "idle");
  EXPECT_DOUBLE_EQ(p.fields[1].value, 99.0);
  EXPECT_TRUE(p.has_timestamp);
  EXPECT_EQ(p.timestamp_ns, 1700000000000000000);
}

TEST(LineProtocol, NoTagsNoTimestamp) {
  ParsedLine p;
  ASSERT_EQ(parse("mem free=1024u", p), ParseStatus::kOk);
  EXPECT_EQ(p.n_tags, 0u);
  EXPECT_FALSE(p.has_timestamp);
  EXPECT_DOUBLE_EQ(p.fields[0].value, 1024.0);
}

TEST(LineProtocol, ValueTypes) {
  struct Case {
    const char* line;
    double want;
  };
  const std::vector<Case> cases = {
      {"m v=1", 1.0},       {"m v=-1.5", -1.5},   {"m v=2e3", 2000.0}, {"m v=-7i", -7.0},
      {"m v=7u", 7.0},      {"m v=t", 1.0},       {"m v=TRUE", 1.0},   {"m v=True", 1.0},
      {"m v=f", 0.0},       {"m v=false", 0.0},   {"m v=.5", 0.5},     {"m v=1E-2", 0.01},
  };
  for (const auto& c : cases) {
    ParsedLine p;
    ASSERT_EQ(parse(c.line, p), ParseStatus::kOk) << c.line;
    EXPECT_DOUBLE_EQ(p.fields[0].value, c.want) << c.line;
  }
}

TEST(LineProtocol, StringFieldsAreSkipped) {
  ParsedLine p;
  ASSERT_EQ(parse(R"(log,app=x msg="hello, world = \"hi\"",level=3i 5)", p), ParseStatus::kOk);
  EXPECT_EQ(p.skipped_fields, 1u);
  ASSERT_EQ(p.n_fields, 1u);
  EXPECT_EQ(p.fields[0].key.raw, "level");
  EXPECT_EQ(p.timestamp_ns, 5);

  EXPECT_EQ(parse(R"(log msg="only strings")", p), ParseStatus::kNoNumericFields);
}

TEST(LineProtocol, Escapes) {
  ParsedLine p;
  ASSERT_EQ(parse(R"(my\ cpu\,x,ho\=st=a\ b\,c fi\ eld=1)", p), ParseStatus::kOk);
  EXPECT_TRUE(p.measurement.escaped);
  EXPECT_EQ(unesc(p.measurement, true), "my cpu,x");
  EXPECT_EQ(unesc(p.tags[0].key), "ho=st");
  EXPECT_EQ(unesc(p.tags[0].value), "a b,c");
  EXPECT_EQ(unesc(p.fields[0].key), "fi eld");
}

TEST(LineProtocol, BackslashBeforeOrdinaryCharIsLiteral) {
  ParsedLine p;
  ASSERT_EQ(parse(R"(path,dir=C:\temp v=1)", p), ParseStatus::kOk);
  EXPECT_EQ(unesc(p.tags[0].value), R"(C:\temp)");
}

TEST(LineProtocol, TrailingWhitespaceAndCR) {
  ParsedLine p;
  ASSERT_EQ(parse("m v=1 10 \r", p), ParseStatus::kOk);
  EXPECT_EQ(p.timestamp_ns, 10);
}

TEST(LineProtocol, BlankAndComment) {
  ParsedLine p;
  EXPECT_EQ(parse("", p), ParseStatus::kBlank);
  EXPECT_EQ(parse("   \t", p), ParseStatus::kBlank);
  EXPECT_EQ(parse("# a comment", p), ParseStatus::kComment);
}

TEST(LineProtocol, Errors) {
  struct Case {
    const char* line;
    ParseStatus want;
  };
  const std::vector<Case> cases = {
      {",host=a v=1", ParseStatus::kBadMeasurement},
      {"cpu", ParseStatus::kBadField},
      {"cpu,host=a", ParseStatus::kBadField},
      {"cpu,host v=1", ParseStatus::kBadTag},
      {"cpu,=a v=1", ParseStatus::kBadTag},
      {"cpu,host= v=1", ParseStatus::kBadTag},
      {"cpu,host=a=b v=1", ParseStatus::kBadTag},
      {"cpu,host=a, v=1", ParseStatus::kBadTag},
      {"cpu v", ParseStatus::kBadField},
      {"cpu =1", ParseStatus::kBadField},
      {"cpu v=", ParseStatus::kBadFieldValue},
      {"cpu v=abc", ParseStatus::kBadFieldValue},
      {"cpu v=1.5i", ParseStatus::kBadFieldValue},
      {"cpu v=-3u", ParseStatus::kBadFieldValue},
      {"cpu v=nan", ParseStatus::kBadFieldValue},
      {"cpu v=inf", ParseStatus::kBadFieldValue},
      {"cpu v=1e999", ParseStatus::kBadFieldValue},
      {R"(cpu v="unterminated)", ParseStatus::kBadFieldValue},
      {R"(cpu v="x"y=1)", ParseStatus::kBadField},
      {"cpu v=1,", ParseStatus::kBadField},
      {"cpu v=1 12x", ParseStatus::kBadTimestamp},
      {"cpu v=1 1 2", ParseStatus::kBadTimestamp},
      {"cpu v=1 99999999999999999999", ParseStatus::kBadTimestamp},
  };
  for (const auto& c : cases) {
    ParsedLine p;
    EXPECT_EQ(parse(c.line, p), c.want) << '"' << c.line << "\" -> " << to_string(parse(c.line, p));
  }
}

TEST(LineProtocol, TooManyTagsAndFields) {
  std::string tags = "m";
  for (std::size_t i = 0; i <= kMaxTags; ++i) tags += ",t" + std::to_string(i) + "=v";
  tags += " v=1";
  ParsedLine p;
  EXPECT_EQ(parse(tags, p), ParseStatus::kTooManyTags);

  std::string fields = "m ";
  for (std::size_t i = 0; i <= kMaxFields; ++i) fields += (i ? ",f" : "f") + std::to_string(i) + "=1";
  EXPECT_EQ(parse(fields, p), ParseStatus::kTooManyFields);
}

TEST(LineProtocol, ForEachLineSplitsPacket) {
  std::vector<std::string> lines;
  for_each_line("a v=1\nb v=2\r\n\n\nc v=3", [&](std::string_view l) { lines.emplace_back(l); });
  ASSERT_EQ(lines.size(), 3u);
  EXPECT_EQ(lines[0], "a v=1");
  EXPECT_EQ(lines[1], "b v=2\r");
  EXPECT_EQ(lines[2], "c v=3");
}

TEST(LineProtocol, UnescapeOverflow) {
  char buf[3];
  EXPECT_EQ(unescape(Token{"abcd", false}, buf, sizeof buf, false), SIZE_MAX);
  EXPECT_EQ(unescape(Token{R"(a\ bcd)", true}, buf, sizeof buf, false), SIZE_MAX);
  EXPECT_EQ(unescape(Token{R"(a\ b)", true}, buf, sizeof buf, false), 3u);
}

}  // namespace
}  // namespace flux
