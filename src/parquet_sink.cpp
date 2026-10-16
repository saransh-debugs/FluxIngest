#include "parquet_sink.hpp"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <fcntl.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <limits>

#include "log.hpp"

namespace flux {

namespace {

Status arrow_error(const char* what, const arrow::Status& s) {
  return Status::error(std::string(what) + ": " + s.ToString());
}

Status errno_error(const char* what, const std::string& path) {
  return Status::error(std::string(what) + " " + path + ": " + std::strerror(errno));
}

// Non-owning arrow::Buffer over a batch column. Zero-copy: Arrow reads our
// memory directly. Safe because the writer thread owns the batch until
// write() returns, and by then Parquet has encoded and copied the data into
// its own pages.
std::shared_ptr<arrow::Buffer> wrap(const void* p, std::size_t bytes) {
  return std::make_shared<arrow::Buffer>(static_cast<const std::uint8_t*>(p),
                                         static_cast<std::int64_t>(bytes));
}

// Builds the per-batch dictionary: only the strings this batch uses.
arrow::Result<std::shared_ptr<arrow::Array>> build_dict(const std::uint32_t* ids, std::uint32_t n,
                                                        const StringInterner& in) {
  std::int64_t bytes = 0;
  for (std::uint32_t i = 0; i < n; ++i) bytes += static_cast<std::int64_t>(in.get(ids[i]).size());
  arrow::StringBuilder sb;
  ARROW_RETURN_NOT_OK(sb.Reserve(n));
  ARROW_RETURN_NOT_OK(sb.ReserveData(bytes));
  for (std::uint32_t i = 0; i < n; ++i) sb.UnsafeAppend(in.get(ids[i]));
  return sb.Finish();
}

std::string window_name(std::int64_t ns) {
  const std::time_t t = static_cast<std::time_t>(ns / 1'000'000'000);
  std::tm g{};
  gmtime_r(&t, &g);
  char buf[32];
  std::strftime(buf, sizeof buf, "%Y%m%dT%H%M%SZ", &g);
  return buf;
}

// fsync on a directory makes a rename inside it durable. Without it, a
// power cut right after rename() can leave the old name (or no file).
Status fsync_dir(const std::string& dir) {
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) return errno_error("open dir", dir);
  const int rc = ::fsync(fd);
  const int saved = errno;
  ::close(fd);
  if (rc != 0) {
    errno = saved;
    return errno_error("fsync dir", dir);
  }
  return Status::ok();
}

}  // namespace

std::shared_ptr<arrow::Schema> ParquetSink::schema() {
  return arrow::schema({
      arrow::field("ts", arrow::timestamp(arrow::TimeUnit::NANO, "UTC"), false),
      arrow::field("metric", arrow::dictionary(arrow::int32(), arrow::utf8()), false),
      arrow::field("tags", arrow::dictionary(arrow::int32(), arrow::utf8()), false),
      arrow::field("value", arrow::float64(), false),
  });
}

ParquetSink::ParquetSink(ParquetSinkConfig cfg) : cfg_(std::move(cfg)), schema_(schema()) {
  namespace fs = std::filesystem;
  fs::create_directories(cfg_.dir);
  for (const auto& e : fs::directory_iterator(cfg_.dir)) {
    const std::string name = e.path().filename().string();
    if (name.size() > 12 && name.ends_with(".parquet.tmp") && name.starts_with(cfg_.prefix + "-")) {
      std::error_code ec;
      if (fs::remove(e.path(), ec)) ++removed_tmp_;
    }
  }
}

ParquetSink::~ParquetSink() { abort(); }

Status ParquetSink::open(std::int64_t window_start_ns, std::uint64_t file_seq) {
  char seq[16];
  std::snprintf(seq, sizeof seq, "%06llu", static_cast<unsigned long long>(file_seq));
  final_path_ = cfg_.dir + "/" + cfg_.prefix + "-" + window_name(window_start_ns) + "-" + seq + ".parquet";
  tmp_path_ = final_path_ + ".tmp";

  auto out = arrow::io::FileOutputStream::Open(tmp_path_);
  if (!out.ok()) return arrow_error("open", out.status());
  out_ = *out;

  parquet::WriterProperties::Builder pb;
  pb.compression(parquet::Compression::ZSTD)
      ->compression_level(cfg_.zstd_level)
      // We decide row group boundaries (one per batch), not Parquet.
      ->max_row_group_length(std::numeric_limits<std::int64_t>::max())
      ->disable_dictionary("ts")
      ->encoding("ts", parquet::Encoding::DELTA_BINARY_PACKED)
      ->disable_dictionary("value")
      ->encoding("value", parquet::Encoding::BYTE_STREAM_SPLIT);
  // store_schema: embeds the Arrow schema so readers get the dictionary type
  // and the UTC timezone back.
  auto arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

  auto w = parquet::arrow::FileWriter::Open(*schema_, arrow::default_memory_pool(), out_, pb.build(),
                                            arrow_props);
  if (!w.ok()) {
    Status s = arrow_error("parquet open", w.status());
    abort();
    return s;
  }
  writer_ = std::move(*w);
  return Status::ok();
}

Status ParquetSink::write(const Batch& b, const StringInterner& metrics, const StringInterner& tags) {
  if (!writer_) return Status::error("write without open file");
  const std::int64_t rows = b.rows;
  const auto n = static_cast<std::size_t>(rows);

  auto ts = std::make_shared<arrow::TimestampArray>(schema_->field(0)->type(), rows,
                                                    wrap(b.ts.get(), n * sizeof(std::int64_t)));
  auto value = std::make_shared<arrow::DoubleArray>(rows, wrap(b.value.get(), n * sizeof(double)));
  auto metric_idx = std::make_shared<arrow::Int32Array>(rows, wrap(b.metric_idx.get(), n * sizeof(std::int32_t)));
  auto tags_idx = std::make_shared<arrow::Int32Array>(rows, wrap(b.tags_idx.get(), n * sizeof(std::int32_t)));

  auto metric_dict = build_dict(b.metric_dict.get(), b.n_metrics, metrics);
  if (!metric_dict.ok()) return arrow_error("metric dictionary", metric_dict.status());
  auto tags_dict = build_dict(b.tags_dict.get(), b.n_tags, tags);
  if (!tags_dict.ok()) return arrow_error("tags dictionary", tags_dict.status());

  // FromArrays checks every index is inside the dictionary: one cheap pass
  // that turns a consumer bug into an error instead of a corrupt file.
  auto metric = arrow::DictionaryArray::FromArrays(schema_->field(1)->type(), metric_idx, *metric_dict);
  if (!metric.ok()) return arrow_error("metric column", metric.status());
  auto tag = arrow::DictionaryArray::FromArrays(schema_->field(2)->type(), tags_idx, *tags_dict);
  if (!tag.ok()) return arrow_error("tags column", tag.status());

  auto table = arrow::Table::Make(schema_, {ts, *metric, *tag, value}, rows);
  // chunk_size = rows: exactly one row group per batch.
  const arrow::Status st = writer_->WriteTable(*table, rows > 0 ? rows : 1);
  if (!st.ok()) return arrow_error("write row group", st);
  return Status::ok();
}

Status ParquetSink::commit(std::uint64_t& bytes_out) {
  if (!writer_) return Status::error("commit without open file");
  arrow::Status st = writer_->Close();  // writes the footer
  if (!st.ok()) return arrow_error("write footer", st);
  writer_.reset();

  if (cfg_.fsync && ::fsync(out_->file_descriptor()) != 0) return errno_error("fsync", tmp_path_);
  auto pos = out_->Tell();
  if (!pos.ok()) return arrow_error("tell", pos.status());
  st = out_->Close();
  if (!st.ok()) return arrow_error("close", st);
  out_.reset();

  if (::rename(tmp_path_.c_str(), final_path_.c_str()) != 0) return errno_error("rename", tmp_path_);
  tmp_path_.clear();
  if (cfg_.fsync) {
    // The file is complete and visible at this point, so its rows count as
    // written. Only the rename's durability is in doubt: warn, don't fail.
    Status s = fsync_dir(cfg_.dir);
    if (!s.is_ok()) FLUX_WARN("%s", s.message().c_str());
  }
  bytes_out = static_cast<std::uint64_t>(*pos);
  return Status::ok();
}

void ParquetSink::abort() noexcept {
  // Close the stream before dropping the writer, so nothing (e.g. a footer)
  // gets written to a file we're about to delete.
  if (out_) (void)out_->Close();
  writer_.reset();
  out_.reset();
  if (!tmp_path_.empty()) {
    ::unlink(tmp_path_.c_str());
    tmp_path_.clear();
  }
}

}  // namespace flux
