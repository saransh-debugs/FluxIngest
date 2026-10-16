#pragma once

// UDP socket reader for the producer thread.
//
// Why UDP: telemetry is lossy by design (StatsD, InfluxDB UDP, collectd).
// A slow or dead ingester must never block or crash the services sending
// metrics. With TCP, a full receive window pushes back into the sender's
// write() calls; with UDP the sender never waits.
// It also means a network partition needs no special handling here: packets
// just stop arriving, and nothing in the pipeline waits for them.
//
// Linux: recvmmsg() reads up to `batch` datagrams per syscall. A syscall
// costs ~0.5-1 us, so one-per-packet caps a core at ~1M packets/s.
// SO_RXQ_OVFL attaches the socket's kernel drop counter to each message, so
// we can report packets lost before they ever reached us.
// macOS: no recvmmsg; falls back to one recvmsg() per datagram.

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "flux/counters.hpp"

namespace flux {

struct UdpConfig {
  std::string bind_addr = "0.0.0.0";
  std::uint16_t port = 8089;
  bool reuseport = false;               // needed for several shards on one port
  int rcvbuf_bytes = 16 << 20;          // absorbs bursts while the producer is busy
  std::uint32_t max_datagram = 65536;   // bigger datagrams are counted and dropped
  std::uint32_t batch = 64;             // datagrams per recvmmsg call
};

class UdpReceiver {
 public:
  // Opens and binds the socket. Throws std::system_error (startup only).
  UdpReceiver(const UdpConfig& cfg, ProducerStats& stats);
  ~UdpReceiver();
  UdpReceiver(const UdpReceiver&) = delete;
  UdpReceiver& operator=(const UdpReceiver&) = delete;

  // Waits up to timeout_ms for data, then reads every datagram available, up
  // to `batch`. Returns how many; 0 on timeout. Never throws. The timeout is
  // what lets the producer thread notice a shutdown request.
  std::size_t receive(int timeout_ms) noexcept;

  // Valid until the next receive(). Empty for truncated datagrams.
  std::string_view packet(std::size_t i) const noexcept;

  std::uint16_t port() const noexcept { return port_; }
  int rcvbuf_actual() const noexcept { return rcvbuf_actual_; }
  static std::size_t memory_bytes(const UdpConfig& cfg) noexcept;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  ProducerStats& stats_;
  int fd_ = -1;
  std::uint16_t port_ = 0;
  int rcvbuf_actual_ = 0;
};

}  // namespace flux
