#include "writer.hpp"

#include <algorithm>
#include <exception>

#include "flux/clock.hpp"
#include "log.hpp"

namespace flux {

Writer::Writer(BatchExchange& ex, BatchSink& sink, const StringInterner& metrics,
               const StringInterner& tags, WriterStats& stats, const WriterConfig& cfg, Clock wall,
               Clock mono)
    : ex_(ex),
      sink_(sink),
      metrics_(metrics),
      tags_(tags),
      stats_(stats),
      cfg_(cfg),
      wall_(wall ? std::move(wall) : Clock(&wall_ns)),
      mono_(mono ? std::move(mono) : Clock(&mono_ns)) {}

void Writer::run() {
  for (;;) {
    Batch* b = ex_.next(cfg_.poll);
    if (b == nullptr) {
      if (ex_.finished()) break;
      // Safety net: if the batch that should have closed this window was
      // evicted, close the file once the window is clearly over.
      if (open_ && wall_() > open_window_ + 2 * cfg_.window_ns) commit_file();
      continue;
    }
    handle(*b);
    ex_.release(b);
  }
  if (open_) commit_file();
}

void Writer::handle(Batch& b) {
  if (open_ && b.window_start_ns != open_window_) commit_file();

  if (b.rows != 0) {
    if (!open_) {
      if (mono_() < retry_at_mono_) {
        stats_.dropped_write_error.add(b.rows);  // still backing off after a failure
        return;
      }
      Status s = sink_.open(b.window_start_ns, file_seq_++);
      if (!s.is_ok()) {
        fail_file(s, b.rows);
        return;
      }
      open_ = true;
      open_window_ = b.window_start_ns;
      rows_in_file_ = 0;
    }
    const std::int64_t t0 = mono_ns();
    Status s = Status::ok();
    try {
      s = sink_.write(b, metrics_, tags_);
    } catch (const std::exception& e) {  // sinks shouldn't throw; don't let one kill the thread
      s = Status::error(e.what());
    }
    stats_.write_ns.add(static_cast<std::uint64_t>(mono_ns() - t0));
    if (!s.is_ok()) {
      fail_file(s, b.rows);
      return;
    }
    rows_in_file_ += b.rows;
    stats_.row_groups.add();
  }

  if (b.closes_window && open_) commit_file();
}

void Writer::commit_file() {
  std::uint64_t bytes = 0;
  const std::int64_t t0 = mono_ns();
  Status s = Status::ok();
  try {
    s = sink_.commit(bytes);
  } catch (const std::exception& e) {
    s = Status::error(e.what());
  }
  stats_.write_ns.add(static_cast<std::uint64_t>(mono_ns() - t0));
  if (!s.is_ok()) {
    fail_file(s, 0);
    return;
  }
  open_ = false;
  stats_.written.add(rows_in_file_);
  stats_.files_committed.add();
  stats_.bytes_committed.add(bytes);
  rows_in_file_ = 0;
  backoff_ = std::chrono::milliseconds{0};
}

void Writer::fail_file(const Status& s, std::uint64_t extra_rows) {
  sink_.abort();
  const std::uint64_t lost = rows_in_file_ + extra_rows;
  stats_.dropped_write_error.add(lost);
  stats_.files_failed.add();
  open_ = false;
  rows_in_file_ = 0;
  backoff_ = backoff_.count() == 0 ? cfg_.min_backoff : std::min(backoff_ * 2, cfg_.max_backoff);
  retry_at_mono_ = mono_() + std::chrono::duration_cast<std::chrono::nanoseconds>(backoff_).count();
  FLUX_ERROR("write failed (%s); dropped %llu rows; retrying in %lld ms", s.message().c_str(),
             static_cast<unsigned long long>(lost), static_cast<long long>(backoff_.count()));
}

}  // namespace flux
