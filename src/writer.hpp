#pragma once

// Writer thread: takes sealed batches and hands them to a BatchSink.
//
// This is the only thread that blocks on the disk. The consumer and producer
// keep running no matter what happens here:
//  - Slow disk: batches queue up in BatchExchange; when the pool runs out the
//    consumer evicts the oldest queued batch (counted as dropped_batch).
//  - Write error (disk full, EIO): the open file is aborted (its .tmp is
//    deleted), its rows are counted as dropped_write_error, and we back off
//    exponentially (1 s, 2 s, 4 s ... 30 s) before opening another file.
//    Batches that arrive during the back-off are dropped and counted, not
//    queued: queuing them would just turn a disk problem into a memory one.
//
// A row counts as `written` only once its file is committed (renamed into
// place). Before that it isn't durable.

#include <chrono>
#include <cstdint>
#include <functional>

#include "batch.hpp"
#include "flux/counters.hpp"
#include "flux/interner.hpp"
#include "sink.hpp"

namespace flux {

struct WriterConfig {
  std::int64_t window_ns = 60'000'000'000;
  std::chrono::milliseconds poll{200};
  std::chrono::milliseconds min_backoff{1000};
  std::chrono::milliseconds max_backoff{30000};
};

class Writer {
 public:
  using Clock = std::function<std::int64_t()>;

  Writer(BatchExchange& ex, BatchSink& sink, const StringInterner& metrics, const StringInterner& tags,
         WriterStats& stats, const WriterConfig& cfg, Clock wall = nullptr, Clock mono = nullptr);

  // Runs until the exchange is closed and drained, then commits any open file.
  void run();

 private:
  void handle(Batch& b);
  void commit_file();
  void fail_file(const Status& s, std::uint64_t extra_rows);

  BatchExchange& ex_;
  BatchSink& sink_;
  const StringInterner& metrics_;
  const StringInterner& tags_;
  WriterStats& stats_;
  const WriterConfig cfg_;
  Clock wall_;
  Clock mono_;

  bool open_ = false;
  std::int64_t open_window_ = 0;
  std::uint64_t rows_in_file_ = 0;
  std::uint64_t file_seq_ = 0;
  std::chrono::milliseconds backoff_{0};
  std::int64_t retry_at_mono_ = 0;
};

}  // namespace flux
