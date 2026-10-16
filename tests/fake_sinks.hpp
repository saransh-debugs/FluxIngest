#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "sink.hpp"

namespace flux::testing {

struct Row {
  std::int64_t ts;
  double value;
  std::string metric;
  std::string tags;
};

struct File {
  std::int64_t window_start;
  std::vector<Row> rows;
  int row_groups = 0;
};

// Keeps everything in memory. Can be made slow, or fail on demand.
class MemorySink final : public BatchSink {
 public:
  std::chrono::milliseconds write_delay{0};
  std::atomic<int> fail_writes{0};   // fail the next N writes
  std::atomic<int> fail_opens{0};    // fail the next N opens
  std::atomic<int> fail_commits{0};  // fail the next N commits

  Status open(std::int64_t window_start_ns, std::uint64_t) override {
    if (fail_opens > 0) {
      --fail_opens;
      return Status::error("injected open failure");
    }
    std::lock_guard lk(mu_);
    current_ = File{window_start_ns, {}, 0};
    is_open_ = true;
    return Status::ok();
  }

  Status write(const Batch& b, const StringInterner& m, const StringInterner& t) override {
    if (write_delay.count() > 0) std::this_thread::sleep_for(write_delay);
    if (fail_writes > 0) {
      --fail_writes;
      return Status::error("injected write failure (ENOSPC)");
    }
    std::lock_guard lk(mu_);
    for (std::uint32_t r = 0; r < b.rows; ++r) {
      current_.rows.push_back(Row{b.ts[r], b.value[r],
                                  std::string(m.get(b.metric_dict[static_cast<std::uint32_t>(b.metric_idx[r])])),
                                  std::string(t.get(b.tags_dict[static_cast<std::uint32_t>(b.tags_idx[r])]))});
    }
    ++current_.row_groups;
    return Status::ok();
  }

  Status commit(std::uint64_t& bytes) override {
    if (fail_commits > 0) {
      --fail_commits;
      return Status::error("injected commit failure");
    }
    std::lock_guard lk(mu_);
    bytes = current_.rows.size() * 24;
    committed_.push_back(std::move(current_));
    is_open_ = false;
    return Status::ok();
  }

  void abort() noexcept override {
    std::lock_guard lk(mu_);
    ++aborts_;
    is_open_ = false;
    current_ = File{};
  }

  std::vector<File> files() const {
    std::lock_guard lk(mu_);
    return committed_;
  }
  std::size_t total_rows() const {
    std::lock_guard lk(mu_);
    std::size_t n = 0;
    for (const auto& f : committed_) n += f.rows.size();
    return n;
  }
  int aborts() const {
    std::lock_guard lk(mu_);
    return aborts_;
  }
  bool is_open() const {
    std::lock_guard lk(mu_);
    return is_open_;
  }

 private:
  mutable std::mutex mu_;
  File current_;
  std::vector<File> committed_;
  bool is_open_ = false;
  int aborts_ = 0;
};

}  // namespace flux::testing
