#pragma once

// One shard = one complete, independent pipeline:
//
//   [receiver thread] --SPSC ring--> [consumer thread] --BatchExchange--> [writer thread] --> sink
//
// Shards share nothing: own socket, ring, interners, batches, and files.
// Several shards on one port use SO_REUSEPORT, and the kernel spreads
// senders across them by hashing the source address. No locks, no shared
// cache lines between shards, so throughput scales with cores.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>

#include "flux/spsc_ring.hpp"
#include "sink.hpp"
#include "udp_receiver.hpp"
#include "writer.hpp"

namespace flux {

struct ShardConfig {
  std::uint32_t id = 0;
  Overflow overflow = Overflow::kDropOldest;
  std::size_t ring_capacity = 1 << 20;
  std::uint32_t batch_rows = 1 << 18;
  std::uint32_t n_batches = 4;
  std::int64_t window_ns = 60'000'000'000;
  std::uint32_t max_metrics = 100'000;
  std::size_t metrics_arena = 8u << 20;
  std::uint32_t max_tagsets = 1'000'000;
  std::size_t tagsets_arena = 128u << 20;
  std::uint64_t hash_seed = 0;
  std::optional<UdpConfig> udp;  // nullopt: caller feeds ingest_packet() itself
  WriterConfig writer;
};

// Plain-number snapshot of every counter in the shard.
struct ShardStats {
  // producer
  std::uint64_t datagrams, lines, parse_errors, fields_skipped, samples_offered, cardinality_rejected,
      recv_errors, truncated_datagrams, kernel_drops;
  // ring
  std::uint64_t dropped_ring, ring_size, ring_capacity;
  // consumer
  std::uint64_t popped, batches_sealed, dropped_batch, batches_dropped, batches_queued;
  // writer
  std::uint64_t written, dropped_write_error, files_committed, files_failed, row_groups, bytes_committed,
      write_ns;
  // interners
  std::uint64_t metrics_interned, tagsets_interned;

  ShardStats& operator+=(const ShardStats& o);
};

class Shard {
 public:
  virtual ~Shard() = default;
  virtual void start() = 0;
  // Only for shards without UDP. The caller becomes the single producer:
  // call it from one thread only.
  virtual void ingest_packet(std::string_view packet, std::int64_t now_ns) = 0;
  // Stops the receiver, drains the ring, flushes the last batch, and waits up
  // to `timeout` for the writer to commit. Returns false if the writer is
  // stuck (e.g. a hung disk); then the process should exit without running
  // destructors (std::_Exit), because the destructor would wait for it.
  virtual bool stop(std::chrono::milliseconds timeout) = 0;
  virtual ShardStats stats() const = 0;
  virtual std::uint16_t udp_port() const = 0;
  virtual int udp_rcvbuf() const = 0;  // what the kernel actually granted
};

// Throws on bad config or socket errors (startup only).
std::unique_ptr<Shard> make_shard(const ShardConfig& cfg, std::unique_ptr<BatchSink> sink);

// Everything make_shard() will allocate up front, in bytes.
std::size_t shard_memory_bytes(const ShardConfig& cfg);

}  // namespace flux
