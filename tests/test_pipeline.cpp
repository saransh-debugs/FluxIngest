// Whole shards: producer -> ring -> consumer -> writer -> fake sink.

#include "pipeline.hpp"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>

#include "fake_sinks.hpp"
#include "flux/clock.hpp"

namespace flux {
namespace {

using testing::MemorySink;

// A sink wrapper that lets the test keep a pointer after the shard owns it.
struct Owned {
  std::unique_ptr<MemorySink> sink = std::make_unique<MemorySink>();
  MemorySink* raw = sink.get();
};

ShardConfig small_config() {
  ShardConfig c;
  c.ring_capacity = 1 << 12;
  c.batch_rows = 256;
  c.n_batches = 4;
  c.window_ns = 3600'000'000'000;
  c.max_metrics = 1000;
  c.metrics_arena = 1 << 16;
  c.max_tagsets = 1000;
  c.tagsets_arena = 1 << 16;
  c.hash_seed = 42;
  c.writer.poll = std::chrono::milliseconds(5);
  return c;
}

void expect_conservation(const ShardStats& s) {
  EXPECT_EQ(s.samples_offered, s.dropped_ring + s.popped);
  EXPECT_EQ(s.popped, s.written + s.dropped_batch + s.dropped_write_error);
}

TEST(Shard, IngestsThroughEveryStage) {
  Owned o;
  auto shard = make_shard(small_config(), std::move(o.sink));
  shard->start();
  for (int i = 0; i < 1000; ++i) {
    shard->ingest_packet("cpu,host=a v=" + std::to_string(i) + "\nmem,host=a used=1i,free=2i\n", 0);
    if (i % 100 == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));  // let the consumer keep up
  }
  ASSERT_TRUE(shard->stop(std::chrono::seconds(10)));
  const ShardStats s = shard->stats();
  EXPECT_EQ(s.lines, 2000u);
  EXPECT_EQ(s.samples_offered, 3000u);
  EXPECT_EQ(s.metrics_interned, 3u);
  EXPECT_EQ(s.tagsets_interned, 1u);
  EXPECT_EQ(o.raw->total_rows(), s.written);
  expect_conservation(s);
}

TEST(Shard, BothOverflowPoliciesWork) {
  for (Overflow p : {Overflow::kDropNewest, Overflow::kDropOldest}) {
    Owned o;
    ShardConfig c = small_config();
    c.overflow = p;
    c.ring_capacity = 16;  // tiny: will overflow
    auto shard = make_shard(c, std::move(o.sink));
    shard->start();
    for (int i = 0; i < 20000; ++i) shard->ingest_packet("cpu v=1", 0);
    ASSERT_TRUE(shard->stop(std::chrono::seconds(10)));
    const ShardStats s = shard->stats();
    EXPECT_EQ(s.samples_offered, 20000u);
    expect_conservation(s);
  }
}

TEST(Shard, StopReportsAStuckWriter) {
  Owned o;
  o.raw->write_delay = std::chrono::milliseconds(1500);
  auto shard = make_shard(small_config(), std::move(o.sink));
  shard->start();
  shard->ingest_packet("cpu v=1", 0);
  EXPECT_FALSE(shard->stop(std::chrono::milliseconds(100)));
  // Destructor waits for the writer to finish its slow write.
}

int udp_sender(std::uint16_t port) {
  const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  ::inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
  ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
  return fd;
}

TEST(Shard, ReceivesOverUdp) {
  Owned o;
  ShardConfig c = small_config();
  UdpConfig u;
  u.bind_addr = "127.0.0.1";
  u.port = 0;  // any free port
  u.rcvbuf_bytes = 1 << 20;
  c.udp = u;
  auto shard = make_shard(c, std::move(o.sink));
  ASSERT_NE(shard->udp_port(), 0);
  shard->start();

  const int fd = udp_sender(shard->udp_port());
  for (int i = 0; i < 200; ++i) {
    const std::string p = "cpu,host=a v=" + std::to_string(i) + " " + std::to_string(i) + "\ncpu,host=b v=1 1\n";
    ASSERT_EQ(::send(fd, p.data(), p.size(), 0), static_cast<ssize_t>(p.size()));
    if (i % 50 == 0) std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  ::close(fd);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (shard->stats().datagrams < 200 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(shard->stop(std::chrono::seconds(10)));
  const ShardStats s = shard->stats();
  EXPECT_EQ(s.datagrams, 200u);  // loopback with a 1 MiB buffer: no loss expected
  EXPECT_EQ(s.samples_offered, 400u);
  EXPECT_EQ(s.written, 400u);
  EXPECT_EQ(s.tagsets_interned, 2u);
  expect_conservation(s);
}

TEST(Shard, OversizedDatagramIsCountedNotParsed) {
  Owned o;
  ShardConfig c = small_config();
  UdpConfig u;
  u.bind_addr = "127.0.0.1";
  u.port = 0;
  u.max_datagram = 64;
  c.udp = u;
  auto shard = make_shard(c, std::move(o.sink));
  shard->start();
  const int fd = udp_sender(shard->udp_port());
  const std::string big = "cpu,host=" + std::string(200, 'x') + " v=1\n";
  const std::string ok = "cpu v=1\n";
  ::send(fd, big.data(), big.size(), 0);
  ::send(fd, ok.data(), ok.size(), 0);
  ::close(fd);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (shard->stats().datagrams < 1 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  ASSERT_TRUE(shard->stop(std::chrono::seconds(10)));
  const ShardStats s = shard->stats();
  EXPECT_EQ(s.truncated_datagrams, 1u);
  EXPECT_EQ(s.samples_offered, 1u);  // a cut-off line would have been parsed as garbage
}

TEST(Shard, BindFailureThrowsAtStartup) {
  ShardConfig c = small_config();
  UdpConfig u;
  u.bind_addr = "not-an-ip";
  c.udp = u;
  EXPECT_THROW(make_shard(c, std::make_unique<MemorySink>()), std::system_error);
}

TEST(Shard, MemoryBudgetCountsTheBigParts) {
  ShardConfig c = small_config();
  c.ring_capacity = 1 << 20;
  c.batch_rows = 1 << 18;
  c.n_batches = 4;
  const std::size_t b = shard_memory_bytes(c);
  EXPECT_GE(b, (std::size_t{1} << 20) * 24 + 4 * (std::size_t{1} << 18) * 32);
}

}  // namespace
}  // namespace flux
