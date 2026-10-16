// Consumer + writer threads against fake sinks. No Arrow needed.

#include <gtest/gtest.h>

#include <atomic>
#include <thread>

#include "batch.hpp"
#include "consumer.hpp"
#include "fake_sinks.hpp"
#include "flux/ingestor.hpp"
#include "flux/spsc_ring.hpp"
#include "writer.hpp"

namespace flux {
namespace {

using testing::MemorySink;
using Ring = SpscRing<Sample, Overflow::kDropOldest>;
constexpr std::int64_t kSec = 1'000'000'000;

// Wires producer-side objects + consumer + writer, like one shard.
struct Rig {
  StringInterner metrics{1000, 1 << 16, 1};
  StringInterner tags{1000, 1 << 16, 2};
  Ring ring;
  ProducerStats pstats;
  ConsumerStats cstats;
  WriterStats wstats;
  LineIngestor<Ring> ing{metrics, tags, ring, pstats};
  BatchExchange ex;
  MemorySink sink;
  std::atomic<std::int64_t> now{1000 * kSec};  // fake wall clock
  std::atomic<bool> stop{false};
  std::thread consumer_thread;
  std::thread writer_thread;
  WriterConfig wcfg;

  Rig(std::size_t ring_cap, std::uint32_t n_batches, std::uint32_t batch_rows,
      std::int64_t window_ns = 60 * kSec)
      : ring(ring_cap), ex(n_batches, batch_rows) {
    wcfg.window_ns = window_ns;
    wcfg.poll = std::chrono::milliseconds(5);
    wcfg.min_backoff = std::chrono::milliseconds(20);
    wcfg.max_backoff = std::chrono::milliseconds(40);
    window = window_ns;
  }

  void start() {
    consumer_thread = std::thread([this] {
      Consumer<Ring> c(ring, ex, cstats, ConsumerConfig{window, 1000, 1000}, [this] { return now.load(); });
      c.run(stop);
    });
    writer_thread = std::thread([this] {
      Writer w(ex, sink, metrics, tags, wstats, wcfg, [this] { return now.load(); }, &mono_ns);
      w.run();
    });
  }

  void finish() {
    stop.store(true, std::memory_order_release);
    consumer_thread.join();
    writer_thread.join();
  }

  // Every sample offered to the ring ends up in exactly one bucket.
  void expect_conservation() const {
    const std::uint64_t offered = pstats.samples_offered.load();
    EXPECT_EQ(offered, ring.dropped() + cstats.popped.load());
    EXPECT_EQ(cstats.popped.load(),
              wstats.written.load() + cstats.dropped_batch.load() + wstats.dropped_write_error.load());
    EXPECT_EQ(wstats.written.load(), sink.total_rows());
  }

  std::int64_t window;
};

TEST(BatchExchange, EvictsOldestWhenWriterIsBehind) {
  BatchExchange ex(3, 10);
  auto a = ex.acquire();
  auto b = ex.acquire();
  auto c = ex.acquire();
  a.batch->rows = 1;
  b.batch->rows = 2;
  ex.submit(a.batch);
  ex.submit(b.batch);
  ex.submit(c.batch);
  auto d = ex.acquire();  // nothing free: oldest (a) gets evicted
  EXPECT_TRUE(d.evicted);
  EXPECT_EQ(d.batch, a.batch);
  EXPECT_EQ(d.evicted_rows, 1u);
  EXPECT_EQ(ex.next(std::chrono::milliseconds(0)), b.batch);  // FIFO order kept
}

TEST(BatchExchange, NeedsThreeBatches) { EXPECT_THROW(BatchExchange(2, 10), std::invalid_argument); }

TEST(BatchExchange, NextTimesOutAndFinishes) {
  BatchExchange ex(3, 10);
  EXPECT_EQ(ex.next(std::chrono::milliseconds(1)), nullptr);
  EXPECT_FALSE(ex.finished());
  ex.close();
  EXPECT_EQ(ex.next(std::chrono::milliseconds(1000)), nullptr);  // returns at once when closed
  EXPECT_TRUE(ex.finished());
}

TEST(ConsumerWriter, EveryRowArrivesWithItsStrings) {
  // 16 batches > the 11 this test can ever seal, so nothing can be evicted
  // even when the writer is slow (e.g. under ASan).
  Rig rig(1 << 12, 16, 100);
  rig.start();
  for (int i = 0; i < 1000; ++i) {
    std::string line = "cpu,host=h" + std::to_string(i % 7) + " v=" + std::to_string(i) + " " + std::to_string(i);
    rig.ing.ingest_line(line, 0);
    while (rig.ring.size_approx() > 3000) std::this_thread::yield();
  }
  rig.finish();

  auto files = rig.sink.files();
  ASSERT_EQ(files.size(), 1u);  // one window
  ASSERT_EQ(files[0].rows.size(), 1000u);
  EXPECT_GE(files[0].row_groups, 10);  // sealed on size every 100 rows
  for (int i = 0; i < 1000; ++i) {
    const auto& r = files[0].rows[static_cast<std::size_t>(i)];
    EXPECT_EQ(r.ts, i);
    EXPECT_EQ(r.value, i);
    EXPECT_EQ(r.metric, "cpu.v");
    EXPECT_EQ(r.tags, "host=h" + std::to_string(i % 7));
  }
  rig.expect_conservation();
  EXPECT_EQ(rig.cstats.dropped_batch.load(), 0u);
}

TEST(ConsumerWriter, WindowRolloverStartsNewFile) {
  Rig rig(1 << 12, 4, 1000, 10 * kSec);
  rig.now = 1000 * kSec;
  rig.start();
  rig.ing.ingest_line("a v=1", 0);
  rig.ing.ingest_line("a v=2", 0);
  // Wait until the consumer has taken them, then move the clock into the next window.
  while (rig.cstats.popped.load() < 2) std::this_thread::yield();
  rig.now = 1010 * kSec;
  while (rig.sink.files().empty()) std::this_thread::yield();  // first file closes on rollover
  rig.ing.ingest_line("a v=3", 0);
  rig.finish();

  auto files = rig.sink.files();
  ASSERT_EQ(files.size(), 2u);
  EXPECT_EQ(files[0].window_start, 1000 * kSec);
  EXPECT_EQ(files[0].rows.size(), 2u);
  EXPECT_EQ(files[1].window_start, 1010 * kSec);
  EXPECT_EQ(files[1].rows.size(), 1u);
  rig.expect_conservation();
}

TEST(ConsumerWriter, SlowDiskDropsOldBatchesButIngestionContinues) {
  Rig rig(1 << 10, 3, 64);
  rig.sink.write_delay = std::chrono::milliseconds(20);
  rig.start();
  // Push much faster than 64 rows / 20 ms.
  const auto t0 = std::chrono::steady_clock::now();
  for (int i = 0; i < 200'000; ++i) rig.ing.ingest_line("cpu,host=a v=1", 0);
  const auto push_time = std::chrono::steady_clock::now() - t0;
  rig.finish();

  EXPECT_GT(rig.cstats.dropped_batch.load(), 0u);  // writer couldn't keep up
  EXPECT_GT(rig.wstats.written.load(), 0u);        // but kept writing
  EXPECT_EQ(rig.pstats.samples_offered.load(), 200'000u);
  // The producer never waited on the disk: 200k lines in far less time than
  // the sink would need to write them (200k/64 batches x 20 ms = 62 s).
  EXPECT_LT(push_time, std::chrono::seconds(10));
  rig.expect_conservation();
}

TEST(ConsumerWriter, WriteErrorAbortsFileCountsRowsAndRecovers) {
  Rig rig(1 << 12, 4, 10);
  rig.sink.fail_writes = 1;
  rig.start();
  for (int i = 0; i < 25; ++i) rig.ing.ingest_line("cpu v=1", 0);
  while (rig.wstats.files_failed.load() == 0) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(60));  // past the back-off
  for (int i = 0; i < 25; ++i) rig.ing.ingest_line("cpu v=2", 0);
  rig.finish();

  EXPECT_GE(rig.sink.aborts(), 1);
  EXPECT_GT(rig.wstats.dropped_write_error.load(), 0u);
  EXPECT_GT(rig.wstats.written.load(), 0u);  // recovered after the back-off
  EXPECT_GE(rig.wstats.files_committed.load(), 1u);
  rig.expect_conservation();
}

TEST(ConsumerWriter, CommitFailureLosesOnlyThatFile) {
  Rig rig(1 << 12, 4, 10, 10 * kSec);
  rig.sink.fail_commits = 1;
  rig.start();
  for (int i = 0; i < 5; ++i) rig.ing.ingest_line("cpu v=1", 0);
  while (rig.cstats.popped.load() < 5) std::this_thread::yield();
  rig.now = rig.now + 10 * kSec;  // roll the window -> commit -> injected failure
  while (rig.wstats.files_failed.load() == 0) std::this_thread::yield();
  std::this_thread::sleep_for(std::chrono::milliseconds(60));
  for (int i = 0; i < 7; ++i) rig.ing.ingest_line("cpu v=2", 0);
  rig.finish();

  EXPECT_EQ(rig.wstats.dropped_write_error.load(), 5u);
  EXPECT_EQ(rig.wstats.written.load(), 7u);
  rig.expect_conservation();
}

TEST(ConsumerWriter, EmptyWindowsWriteNoFiles) {
  Rig rig(1 << 12, 4, 10, 1 * kSec);
  rig.start();
  for (int i = 0; i < 5; ++i) {
    rig.now = rig.now + kSec;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  rig.finish();
  EXPECT_TRUE(rig.sink.files().empty());
  EXPECT_EQ(rig.wstats.files_committed.load(), 0u);
}

TEST(DictRemap, EpochResetsWithoutClearing) {
  DictRemap m(100);
  std::uint32_t dict[100];
  std::uint32_t n = 0;
  EXPECT_EQ(m.local(42, dict, n), 0);
  EXPECT_EQ(m.local(7, dict, n), 1);
  EXPECT_EQ(m.local(42, dict, n), 0);
  EXPECT_EQ(n, 2u);
  EXPECT_EQ(dict[0], 42u);
  m.next_epoch();
  n = 0;
  EXPECT_EQ(m.local(7, dict, n), 0);  // fresh numbering in the new batch
  EXPECT_EQ(n, 1u);
}

}  // namespace
}  // namespace flux
