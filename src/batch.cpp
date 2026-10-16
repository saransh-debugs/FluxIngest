#include "batch.hpp"

#include <stdexcept>

namespace flux {

Batch::Batch(std::uint32_t cap)
    : ts(make_aligned<std::int64_t>(cap)),
      value(make_aligned<double>(cap)),
      metric_idx(make_aligned<std::int32_t>(cap)),
      tags_idx(make_aligned<std::int32_t>(cap)),
      metric_dict(make_aligned<std::uint32_t>(cap)),
      tags_dict(make_aligned<std::uint32_t>(cap)),
      capacity(cap) {}

BatchExchange::BatchExchange(std::uint32_t n_batches, std::uint32_t batch_rows) {
  if (n_batches < 3) throw std::invalid_argument("BatchExchange needs at least 3 batches");
  if (batch_rows == 0) throw std::invalid_argument("batch_rows must be > 0");
  all_.reserve(n_batches);
  free_.reserve(n_batches);
  pending_.buf.assign(n_batches, nullptr);
  for (std::uint32_t i = 0; i < n_batches; ++i) {
    all_.push_back(std::make_unique<Batch>(batch_rows));
    free_.push_back(all_.back().get());
  }
}

BatchExchange::Acquired BatchExchange::acquire() {
  std::lock_guard lk(mu_);
  Acquired a;
  if (!free_.empty()) {
    a.batch = free_.back();
    free_.pop_back();
  } else if (pending_.count != 0) {
    // Writer is behind: sacrifice the oldest sealed batch.
    a.batch = pending_.pop();
    a.evicted = true;
    a.evicted_rows = a.batch->rows;
  }
  // Else: unreachable with n >= 3 (consumer holds none here, writer holds at
  // most one). The caller still handles nullptr.
  return a;
}

void BatchExchange::submit(Batch* b) {
  {
    std::lock_guard lk(mu_);
    pending_.push(b);
  }
  cv_.notify_one();  // outside the lock so the writer doesn't wake into a held mutex
}

void BatchExchange::close() {
  {
    std::lock_guard lk(mu_);
    closed_ = true;
  }
  cv_.notify_all();
}

Batch* BatchExchange::next(std::chrono::milliseconds timeout) {
  std::unique_lock lk(mu_);
  cv_.wait_for(lk, timeout, [&] { return pending_.count != 0 || closed_; });
  if (pending_.count == 0) return nullptr;
  return pending_.pop();
}

void BatchExchange::release(Batch* b) {
  std::lock_guard lk(mu_);
  free_.push_back(b);
}

bool BatchExchange::finished() const {
  std::lock_guard lk(mu_);
  return closed_ && pending_.count == 0;
}

std::size_t BatchExchange::queued() const {
  std::lock_guard lk(mu_);
  return pending_.count;
}

}  // namespace flux
