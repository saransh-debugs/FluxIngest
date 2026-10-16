#pragma once

// Column batches and the hand-off between the consumer and writer threads.
//
// A Batch is one future Parquet row group, stored as plain column arrays.
// All batches are allocated once at startup; they're reused forever.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <new>
#include <vector>

namespace flux {

// 64-byte aligned, zero-filled array. Arrow's own buffers are 64-byte
// aligned, and SIMD encoders run fastest on aligned input. Wrapping these
// arrays in arrow::Buffer later is zero-copy.
template <class T>
struct AlignedDelete {
  void operator()(T* p) const noexcept { ::operator delete[](p, std::align_val_t{64}); }
};
template <class T>
using AlignedArray = std::unique_ptr<T[], AlignedDelete<T>>;

template <class T>
AlignedArray<T> make_aligned(std::size_t n) {
  void* p = ::operator new[](n * sizeof(T), std::align_val_t{64});
  std::memset(p, 0, n * sizeof(T));  // touch every page now, not on the hot path
  return AlignedArray<T>(static_cast<T*>(p));
}

struct Batch {
  explicit Batch(std::uint32_t capacity);

  // Columns, `rows` entries each.
  AlignedArray<std::int64_t> ts;
  AlignedArray<double> value;
  AlignedArray<std::int32_t> metric_idx;  // index into metric_dict
  AlignedArray<std::int32_t> tags_idx;    // index into tags_dict

  // Per-batch dictionaries: local index -> global interner id. A batch only
  // references a few thousand distinct strings even if the interner holds a
  // million, so each row group gets a small dictionary.
  AlignedArray<std::uint32_t> metric_dict;
  AlignedArray<std::uint32_t> tags_dict;

  const std::uint32_t capacity;
  std::uint32_t rows = 0;
  std::uint32_t n_metrics = 0;
  std::uint32_t n_tags = 0;

  std::int64_t window_start_ns = 0;  // which file this batch belongs in
  bool closes_window = false;        // last batch of its window: writer closes the file after it
  std::uint64_t seq = 0;

  void reset(std::int64_t window_start, std::uint64_t sequence) noexcept {
    rows = n_metrics = n_tags = 0;
    window_start_ns = window_start;
    closes_window = false;
    seq = sequence;
  }
  bool full() const noexcept { return rows == capacity; }

  static std::size_t memory_bytes(std::uint32_t capacity) noexcept {
    return std::size_t{capacity} * (8 + 8 + 4 + 4 + 4 + 4) + sizeof(Batch);
  }
};

// Fixed pool of batches plus a FIFO of sealed batches waiting for the writer.
//
// Why a mutex here when the ring is lock-free: this lock is taken once per
// batch (every ~256K samples, or once a window), not once per sample. At that
// rate an uncontended mutex costs nothing measurable, and it gives us a
// condition variable so the writer sleeps instead of polling.
// The writer never holds the lock while doing I/O, so a stuck disk can never
// block the consumer.
//
// Backpressure: when the writer falls behind and no free batch is left,
// acquire() evicts the OLDEST sealed batch and hands it back for reuse
// (same drop-oldest policy as the ring). Memory stays fixed at n batches.
class BatchExchange {
 public:
  // n_batches >= 3: one being filled, one being written, at least one queued.
  BatchExchange(std::uint32_t n_batches, std::uint32_t batch_rows);

  struct Acquired {
    Batch* batch = nullptr;
    std::uint32_t evicted_rows = 0;  // rows lost by evicting a sealed batch
    bool evicted = false;
  };

  // ---- consumer side ----
  Acquired acquire();
  void submit(Batch* b);
  void close();  // no more submits; writer drains what's queued, then stops

  // ---- writer side ----
  // Returns the oldest sealed batch, or nullptr after `timeout` with nothing
  // queued, or nullptr once closed and drained (check finished()).
  Batch* next(std::chrono::milliseconds timeout);
  void release(Batch* b);
  bool finished() const;

  std::size_t queued() const;
  std::uint32_t size() const noexcept { return static_cast<std::uint32_t>(all_.size()); }

 private:
  // Fixed-capacity FIFO of batch pointers. std::deque would allocate.
  struct Fifo {
    std::vector<Batch*> buf;
    std::size_t head = 0, count = 0;
    void push(Batch* b) {
      buf[(head + count) % buf.size()] = b;
      ++count;
    }
    Batch* pop() {
      Batch* b = buf[head];
      head = (head + 1) % buf.size();
      --count;
      return b;
    }
  };

  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<std::unique_ptr<Batch>> all_;
  std::vector<Batch*> free_;  // reserved to n at startup, never reallocates
  Fifo pending_;
  bool closed_ = false;
};

}  // namespace flux
