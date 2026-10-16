#pragma once

// Statistics counters.
//
// Every counter has exactly one writer thread, so increments are a relaxed
// load + store, not fetch_add. fetch_add is a locked instruction on x86
// (~20 cycles and a full barrier); we'd pay it per sample for nothing.
//
// Counters are grouped by the thread that writes them, and each group sits on
// its own cache line(s). If the producer's and consumer's counters shared a
// line, every increment would steal the line from the other core
// ("false sharing"), even though they never touch the same variable.
// The stats printer reads them every few seconds; that cost is negligible.

#include <atomic>
#include <cstdint>

#include "flux/config.hpp"

namespace flux {

class OwnedCounter {
 public:
  void add(std::uint64_t n = 1) noexcept {
    v_.store(v_.load(std::memory_order_relaxed) + n, std::memory_order_relaxed);
  }
  std::uint64_t load() const noexcept { return v_.load(std::memory_order_relaxed); }

 private:
  std::atomic<std::uint64_t> v_{0};
};

// Written by the producer (receiver) thread.
struct alignas(kCacheLine) ProducerStats {
  OwnedCounter datagrams;
  OwnedCounter lines;
  OwnedCounter parse_errors;          // whole line rejected
  OwnedCounter fields_skipped;        // string fields: we only store numbers
  OwnedCounter samples_offered;       // pushed into the ring
  OwnedCounter cardinality_rejected;  // interner full, sample dropped
  OwnedCounter recv_errors;           // socket errors other than timeouts
  OwnedCounter truncated_datagrams;   // bigger than our buffer: dropped whole
  OwnedCounter kernel_drops;          // datagrams the kernel dropped (socket buffer full), Linux only
};

// Written by the consumer thread.
struct alignas(kCacheLine) ConsumerStats {
  OwnedCounter popped;           // taken out of the ring
  OwnedCounter batches_sealed;
  OwnedCounter dropped_batch;    // samples in batches evicted because the writer was behind
  OwnedCounter batches_dropped;
  OwnedCounter no_free_batch;    // consumer had to evict to get a buffer
};

// Written by the writer thread.
struct alignas(kCacheLine) WriterStats {
  OwnedCounter written;               // rows durably renamed into a .parquet file
  OwnedCounter dropped_write_error;   // rows lost because a write/close failed
  OwnedCounter files_committed;
  OwnedCounter files_failed;
  OwnedCounter row_groups;
  OwnedCounter bytes_committed;
  OwnedCounter write_ns;              // total time inside the sink
};

}  // namespace flux
