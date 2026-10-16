// ParquetSink against real files. Docker only (needs Arrow).

#include "parquet_sink.hpp"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/file_reader.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "batch.hpp"
#include "flux/interner.hpp"

namespace flux {
namespace {

namespace fs = std::filesystem;
constexpr std::int64_t kWindow = 1'700'000'000'000'000'000;

struct TempDir {
  fs::path path;
  TempDir() {
    path = fs::temp_directory_path() / ("flux_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter++));
    fs::remove_all(path);
    fs::create_directories(path);
  }
  ~TempDir() { fs::remove_all(path); }
  std::vector<std::string> files() const {
    std::vector<std::string> out;
    for (const auto& e : fs::directory_iterator(path)) out.push_back(e.path().filename().string());
    std::sort(out.begin(), out.end());
    return out;
  }
  static inline int counter = 0;
};

// Fills a batch the same way the consumer does.
void fill(Batch& b, StringInterner& m, StringInterner& t, int n, int offset) {
  b.reset(kWindow, 0);
  std::vector<std::int64_t> seen_m(1000, -1), seen_t(1000, -1);
  for (int i = 0; i < n; ++i) {
    const auto mid = m.intern("metric." + std::to_string((i + offset) % 3));
    const auto tid = t.intern("host=h" + std::to_string((i + offset) % 5));
    if (seen_m[mid] < 0) {
      seen_m[mid] = b.n_metrics;
      b.metric_dict[b.n_metrics++] = mid;
    }
    if (seen_t[tid] < 0) {
      seen_t[tid] = b.n_tags;
      b.tags_dict[b.n_tags++] = tid;
    }
    const auto r = b.rows++;
    b.ts[r] = kWindow + i + offset;
    b.value[r] = (i + offset) * 0.5;
    b.metric_idx[r] = static_cast<std::int32_t>(seen_m[mid]);
    b.tags_idx[r] = static_cast<std::int32_t>(seen_t[tid]);
  }
}

std::shared_ptr<arrow::Table> read_table(const fs::path& p, int* row_groups = nullptr) {
  auto in = arrow::io::ReadableFile::Open(p.string()).ValueOrDie();
  auto reader = parquet::arrow::OpenFile(in, arrow::default_memory_pool()).ValueOrDie();
  if (row_groups) *row_groups = reader->num_row_groups();
  std::shared_ptr<arrow::Table> table;
  EXPECT_TRUE(reader->ReadTable(&table).ok());
  return table;
}

std::string dict_value(const arrow::ChunkedArray& col, std::int64_t row) {
  for (const auto& chunk : col.chunks()) {
    if (row < chunk->length()) {
      const auto& d = static_cast<const arrow::DictionaryArray&>(*chunk);
      const auto& dict = static_cast<const arrow::StringArray&>(*d.dictionary());
      return std::string(dict.GetView(d.GetValueIndex(row)));
    }
    row -= chunk->length();
  }
  return "<out of range>";
}

TEST(ParquetSink, RoundTripWithDifferentDictionaryPerRowGroup) {
  TempDir dir;
  StringInterner m(100, 4096, 1), t(100, 4096, 2);
  Batch b1(100), b2(100);
  fill(b1, m, t, 50, 0);
  fill(b2, m, t, 30, 1);  // different first-seen order -> different dictionaries
  ParquetSink sink({dir.path.string(), "metrics", 3, true});
  ASSERT_TRUE(sink.open(kWindow, 7).is_ok());
  ASSERT_TRUE(sink.write(b1, m, t).is_ok());
  ASSERT_TRUE(sink.write(b2, m, t).is_ok());
  std::uint64_t bytes = 0;
  auto st = sink.commit(bytes);
  ASSERT_TRUE(st.is_ok()) << st.message();
  EXPECT_GT(bytes, 0u);

  auto files = dir.files();
  ASSERT_EQ(files.size(), 1u);
  EXPECT_EQ(files[0], "metrics-20231114T221320Z-000007.parquet");
  EXPECT_EQ(fs::file_size(dir.path / files[0]), bytes);

  int row_groups = 0;
  auto table = read_table(dir.path / files[0], &row_groups);
  EXPECT_EQ(row_groups, 2);
  ASSERT_EQ(table->num_rows(), 80);
  EXPECT_TRUE(table->schema()->field(1)->type()->Equals(arrow::dictionary(arrow::int32(), arrow::utf8())));
  EXPECT_EQ(static_cast<const arrow::TimestampType&>(*table->schema()->field(0)->type()).timezone(), "UTC");

  auto ts = table->GetColumnByName("ts");
  auto value = table->GetColumnByName("value");
  auto metric = table->GetColumnByName("metric");
  auto tags = table->GetColumnByName("tags");
  for (std::int64_t row = 0; row < 80; ++row) {
    const int i = row < 50 ? static_cast<int>(row) : static_cast<int>(row - 50);
    const int off = row < 50 ? 0 : 1;
    auto tsv = std::static_pointer_cast<arrow::TimestampScalar>(ts->GetScalar(row).ValueOrDie());
    auto vv = std::static_pointer_cast<arrow::DoubleScalar>(value->GetScalar(row).ValueOrDie());
    EXPECT_EQ(tsv->value, kWindow + i + off);
    EXPECT_DOUBLE_EQ(vv->value, (i + off) * 0.5);
    EXPECT_EQ(dict_value(*metric, row), "metric." + std::to_string((i + off) % 3));
    EXPECT_EQ(dict_value(*tags, row), "host=h" + std::to_string((i + off) % 5));
  }
}

TEST(ParquetSink, UsesRequestedEncodingsAndCompression) {
  TempDir dir;
  StringInterner m(100, 4096, 1), t(100, 4096, 2);
  Batch b(1000);
  fill(b, m, t, 1000, 0);
  ParquetSink sink({dir.path.string(), "metrics", 3, false});
  ASSERT_TRUE(sink.open(kWindow, 0).is_ok());
  ASSERT_TRUE(sink.write(b, m, t).is_ok());
  std::uint64_t bytes = 0;
  ASSERT_TRUE(sink.commit(bytes).is_ok());

  auto reader = parquet::ParquetFileReader::OpenFile((dir.path / dir.files()[0]).string());
  auto rg = reader->metadata()->RowGroup(0);
  auto has = [](const std::vector<parquet::Encoding::type>& v, parquet::Encoding::type e) {
    return std::find(v.begin(), v.end(), e) != v.end();
  };
  EXPECT_TRUE(has(rg->ColumnChunk(0)->encodings(), parquet::Encoding::DELTA_BINARY_PACKED));
  EXPECT_TRUE(has(rg->ColumnChunk(1)->encodings(), parquet::Encoding::RLE_DICTIONARY));
  EXPECT_TRUE(has(rg->ColumnChunk(3)->encodings(), parquet::Encoding::BYTE_STREAM_SPLIT));
  for (int c = 0; c < 4; ++c) EXPECT_EQ(rg->ColumnChunk(c)->compression(), parquet::Compression::ZSTD);
}

TEST(ParquetSink, AbortLeavesNothingBehind) {
  TempDir dir;
  StringInterner m(100, 4096, 1), t(100, 4096, 2);
  Batch b(10);
  fill(b, m, t, 10, 0);
  ParquetSink sink({dir.path.string(), "metrics", 3, true});
  ASSERT_TRUE(sink.open(kWindow, 0).is_ok());
  ASSERT_TRUE(sink.write(b, m, t).is_ok());
  EXPECT_EQ(dir.files().size(), 1u);  // the .tmp
  EXPECT_TRUE(dir.files()[0].ends_with(".parquet.tmp"));
  sink.abort();
  EXPECT_TRUE(dir.files().empty());
}

TEST(ParquetSink, StartupRemovesLeftoverTmpFiles) {
  TempDir dir;
  std::ofstream(dir.path / "metrics-20231114T221320Z-000001.parquet.tmp") << "half written";
  std::ofstream(dir.path / "metrics-20231114T221320Z-000000.parquet") << "keep";
  std::ofstream(dir.path / "other.parquet.tmp") << "not ours";
  ParquetSink sink({dir.path.string(), "metrics", 3, true});
  EXPECT_EQ(sink.removed_tmp_files(), 1u);
  EXPECT_EQ(dir.files(), (std::vector<std::string>{"metrics-20231114T221320Z-000000.parquet", "other.parquet.tmp"}));
}

TEST(ParquetSink, WriteWithoutOpenIsAnError) {
  TempDir dir;
  StringInterner m(10, 100, 1), t(10, 100, 2);
  Batch b(1);
  ParquetSink sink({dir.path.string(), "metrics", 3, true});
  EXPECT_FALSE(sink.write(b, m, t).is_ok());
  std::uint64_t bytes = 0;
  EXPECT_FALSE(sink.commit(bytes).is_ok());
}

// Needs a tiny filesystem: docker run --tmpfs /small:size=1m -e FLUX_SMALL_DIR=/small
TEST(ParquetSink, DiskFullReturnsErrorAndAbortCleansUp) {
  const char* small = std::getenv("FLUX_SMALL_DIR");
  if (!small) GTEST_SKIP() << "set FLUX_SMALL_DIR to a tiny tmpfs to run";
  const fs::path dir = fs::path(small) / "diskfull";
  fs::remove_all(dir);
  StringInterner m(100, 4096, 1), t(1'000'000, 64 << 20, 2);
  // Random-ish values and unique tags don't compress: ~1M rows easily beat 1 MiB.
  Batch b(1 << 18);
  b.reset(kWindow, 0);
  for (std::uint32_t i = 0; i < b.capacity; ++i) {
    b.ts[i] = kWindow + static_cast<std::int64_t>(i) * 7919 * 7919;
    b.value[i] = static_cast<double>(i) * 1.000001 + 1e-9 * (i * 2654435761u % 1000);
    b.metric_idx[i] = 0;
    b.tags_idx[i] = static_cast<std::int32_t>(i);
    b.tags_dict[i] = t.intern("id=" + std::to_string(i * 2654435761u));
  }
  b.metric_dict[0] = m.intern("x.y");
  b.n_metrics = 1;
  b.n_tags = b.capacity;
  b.rows = b.capacity;

  ParquetSink sink({dir.string(), "metrics", 1, true});
  Status st = sink.open(kWindow, 0);
  bool failed = !st.is_ok();
  for (int i = 0; i < 20 && !failed; ++i) {
    st = sink.write(b, m, t);
    failed = !st.is_ok();
  }
  if (!failed) {
    std::uint64_t bytes = 0;
    st = sink.commit(bytes);
    failed = !st.is_ok();
  }
  ASSERT_TRUE(failed) << "expected ENOSPC on a 1 MiB filesystem";
  EXPECT_NE(st.message().find("space"), std::string::npos) << st.message();
  sink.abort();
  EXPECT_TRUE(fs::is_empty(dir));
}

}  // namespace
}  // namespace flux
