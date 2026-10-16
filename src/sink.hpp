#pragma once

// Where sealed batches go. ParquetSink is the real one; tests use fakes that
// can be slow or fail on demand.
//
// Lifecycle per file: open -> write (one row group per batch) ... -> commit.
// On any error the writer calls abort(), which must remove partial output.

#include <cstdint>
#include <string>
#include <utility>

#include "batch.hpp"
#include "flux/interner.hpp"

namespace flux {

class Status {
 public:
  static Status ok() { return Status(); }
  static Status error(std::string msg) { return Status(std::move(msg)); }
  bool is_ok() const noexcept { return ok_; }
  const std::string& message() const noexcept { return msg_; }

 private:
  Status() = default;
  explicit Status(std::string m) : ok_(false), msg_(std::move(m)) {}
  bool ok_ = true;
  std::string msg_;
};

class BatchSink {
 public:
  virtual ~BatchSink() = default;
  virtual Status open(std::int64_t window_start_ns, std::uint64_t file_seq) = 0;
  virtual Status write(const Batch& b, const StringInterner& metrics, const StringInterner& tags) = 0;
  // Makes the file durable and visible. Sets bytes_out on success.
  virtual Status commit(std::uint64_t& bytes_out) = 0;
  // Throws away the open file, leaving nothing behind. Must not throw.
  virtual void abort() noexcept = 0;
};

}  // namespace flux
