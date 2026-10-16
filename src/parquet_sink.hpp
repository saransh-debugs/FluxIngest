#pragma once

// Writes batches as Parquet files, one file per time window, one row group
// per batch.
//
// Crash safety: data goes to "<name>.parquet.tmp". On commit we write the
// footer, fsync the file, rename it to "<name>.parquet", and fsync the
// directory so the rename itself survives a power cut. A Parquet file is
// unreadable until its footer is written, so readers (DuckDB, Spark) must
// never see a half-written one. They glob "*.parquet", which skips ".tmp".
//
// Schema:
//   ts      timestamp[ns, UTC]   DELTA_BINARY_PACKED (timestamps grow, deltas are tiny)
//   metric  dictionary<string>   one small dictionary per row group
//   tags    dictionary<string>   canonical "k=v,k2=v2"
//   value   double               BYTE_STREAM_SPLIT (helps zstd compress floats)

#include <memory>
#include <string>

#include "sink.hpp"

namespace arrow {
class Schema;
namespace io {
class FileOutputStream;
}
}  // namespace arrow
namespace parquet::arrow {
class FileWriter;
}

namespace flux {

struct ParquetSinkConfig {
  std::string dir;
  std::string prefix = "metrics";
  int zstd_level = 3;
  bool fsync = true;
};

class ParquetSink final : public BatchSink {
 public:
  // Creates `dir` and deletes leftover *.parquet.tmp files from a previous
  // crash. Throws on failure (startup only).
  explicit ParquetSink(ParquetSinkConfig cfg);
  ~ParquetSink() override;

  Status open(std::int64_t window_start_ns, std::uint64_t file_seq) override;
  Status write(const Batch& b, const StringInterner& metrics, const StringInterner& tags) override;
  Status commit(std::uint64_t& bytes_out) override;
  void abort() noexcept override;

  static std::shared_ptr<arrow::Schema> schema();
  std::size_t removed_tmp_files() const noexcept { return removed_tmp_; }

 private:
  ParquetSinkConfig cfg_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::io::FileOutputStream> out_;
  std::unique_ptr<parquet::arrow::FileWriter> writer_;
  std::string tmp_path_;
  std::string final_path_;
  std::size_t removed_tmp_ = 0;
};

}  // namespace flux
