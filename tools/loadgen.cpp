// Sends InfluxDB line protocol over UDP at a target rate.
//
//   flux_loadgen --port=8089 --samples=1000000 --rate=200000 --series=1000
//
// Each line: loadgen,host=h<k>,region=r<k>,id=s<k> f0=<v>,f1=<v>i <ts>
// Packets hold --lines-per-packet lines (default 20, ~1.3 KB, under a
// 1500-byte MTU so nothing gets IP-fragmented). Several sockets
// (--sockets) give different source ports, so SO_REUSEPORT on the server
// spreads them across shards.
//
// Prints (and optionally writes as JSON) exactly how many samples it sent,
// so the end-to-end test can check every sample is accounted for.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "flux/clock.hpp"

namespace {

struct Options {
  std::string host = "127.0.0.1";
  std::uint16_t port = 8089;
  std::uint64_t samples = 1'000'000;  // 0 = run for --duration
  double duration_s = 0;
  double rate = 0;  // samples/s, 0 = as fast as possible
  std::uint64_t series = 1000;
  std::uint32_t fields = 2;
  std::uint32_t lines_per_packet = 20;
  std::uint32_t sockets = 4;
  bool timestamps = true;
  std::string summary_json;
};

template <class T>
bool num(std::string_view s, T& out) {
  if constexpr (std::is_floating_point_v<T>) {
    // Apple's libc++ has no floating-point from_chars; flags aren't a hot path.
    const std::string str(s);
    char* end = nullptr;
    out = std::strtod(str.c_str(), &end);
    return !str.empty() && end == str.c_str() + str.size();
  } else {
    const auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
  }
}

bool parse(int argc, char** argv, Options& o) {
  for (int i = 1; i < argc; ++i) {
    std::string_view a = argv[i];
    if (a == "--no-timestamps") {
      o.timestamps = false;
      continue;
    }
    const auto eq = a.find('=');
    if (!a.starts_with("--") || eq == std::string_view::npos) return false;
    const auto k = a.substr(2, eq - 2);
    const auto v = a.substr(eq + 1);
    bool ok = true;
    if (k == "host") o.host = v;
    else if (k == "port") ok = num(v, o.port);
    else if (k == "samples") ok = num(v, o.samples);
    else if (k == "duration") ok = num(v, o.duration_s);
    else if (k == "rate") ok = num(v, o.rate);
    else if (k == "series") ok = num(v, o.series) && o.series > 0;
    else if (k == "fields") ok = num(v, o.fields) && o.fields > 0 && o.fields <= 32;
    else if (k == "lines-per-packet") ok = num(v, o.lines_per_packet) && o.lines_per_packet > 0;
    else if (k == "sockets") ok = num(v, o.sockets) && o.sockets > 0;
    else if (k == "summary-json") o.summary_json = v;
    else ok = false;
    if (!ok) {
      std::fprintf(stderr, "bad flag: %s\n", argv[i]);
      return false;
    }
  }
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  Options o;
  if (!parse(argc, argv, o)) {
    std::fprintf(stderr,
                 "usage: flux_loadgen [--host=127.0.0.1] [--port=8089] [--samples=N | --duration=S] [--rate=SAMPLES_PER_S]\n"
                 "                    [--series=N] [--fields=N] [--lines-per-packet=N] [--sockets=N] [--no-timestamps]\n"
                 "                    [--summary-json=PATH]\n");
    return 2;
  }

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(o.port);
  if (::inet_pton(AF_INET, o.host.c_str(), &addr.sin_addr) != 1) {
    std::fprintf(stderr, "bad host %s\n", o.host.c_str());
    return 2;
  }
  std::vector<int> fds;
  for (std::uint32_t i = 0; i < o.sockets; ++i) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    // connect() fixes the destination once, so send() skips a route lookup per packet.
    if (fd < 0 || ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
      std::perror("socket/connect");
      return 1;
    }
    const int sndbuf = 4 << 20;
    (void)::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof sndbuf);
    fds.push_back(fd);
  }

  const std::uint64_t per_packet = std::uint64_t{o.lines_per_packet} * o.fields;
  std::uint64_t packets = 0, samples = 0, lines = 0, send_errors = 0, value = 0, next_series = 0;
  std::vector<char> buf(65536);
  const std::int64_t start = flux::mono_ns();
  const std::int64_t stop_at = o.duration_s > 0 ? start + static_cast<std::int64_t>(o.duration_s * 1e9) : 0;

  for (;;) {
    if (o.samples != 0 && samples + per_packet > o.samples) break;
    const std::int64_t now = flux::mono_ns();
    if (stop_at != 0 && now >= stop_at) break;
    if (o.rate > 0) {
      const double allowed = o.rate * static_cast<double>(now - start) / 1e9;
      if (static_cast<double>(samples) >= allowed) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        continue;
      }
    }

    // Build one packet.
    std::size_t len = 0;
    const std::int64_t ts = flux::wall_ns();
    for (std::uint32_t l = 0; l < o.lines_per_packet; ++l) {
      const std::uint64_t s = next_series++ % o.series;
      int w = std::snprintf(buf.data() + len, buf.size() - len, "loadgen,host=h%llu,region=r%llu,id=s%llu ",
                            static_cast<unsigned long long>(s % 100), static_cast<unsigned long long>(s % 7),
                            static_cast<unsigned long long>(s));
      len += static_cast<std::size_t>(w);
      for (std::uint32_t f = 0; f < o.fields; ++f) {
        ++value;
        w = (f % 2 == 0) ? std::snprintf(buf.data() + len, buf.size() - len, "%sf%u=%llu.5", f ? "," : "", f,
                                         static_cast<unsigned long long>(value))
                         : std::snprintf(buf.data() + len, buf.size() - len, "%sf%u=%llui", f ? "," : "", f,
                                         static_cast<unsigned long long>(value));
        len += static_cast<std::size_t>(w);
      }
      if (o.timestamps) {
        w = std::snprintf(buf.data() + len, buf.size() - len, " %lld", static_cast<long long>(ts + l));
        len += static_cast<std::size_t>(w);
      }
      buf[len++] = '\n';
    }

    const int fd = fds[packets % fds.size()];
    if (::send(fd, buf.data(), len, 0) == static_cast<ssize_t>(len)) {
      ++packets;
      samples += per_packet;
      lines += o.lines_per_packet;
    } else {
      // ECONNREFUSED: an ICMP "port unreachable" from an earlier packet
      // (server not up yet). ENOBUFS: local queue full. Either way the
      // packet wasn't sent; count it and keep going.
      ++send_errors;
      if (send_errors > 1000 && packets == 0) {
        std::fprintf(stderr, "loadgen: nothing is listening on %s:%u (%s)\n", o.host.c_str(), o.port, std::strerror(errno));
        return 1;
      }
    }
  }

  const double elapsed = static_cast<double>(flux::mono_ns() - start) / 1e9;
  std::printf("loadgen: sent %llu samples (%llu lines, %llu packets) in %.2fs = %.0f samples/s, %llu send errors\n",
              static_cast<unsigned long long>(samples), static_cast<unsigned long long>(lines),
              static_cast<unsigned long long>(packets), elapsed, static_cast<double>(samples) / elapsed,
              static_cast<unsigned long long>(send_errors));
  if (!o.summary_json.empty()) {
    if (std::FILE* f = std::fopen(o.summary_json.c_str(), "w")) {
      std::fprintf(f,
                   "{\"samples\": %llu, \"lines\": %llu, \"packets\": %llu, \"samples_per_packet\": %llu, "
                   "\"send_errors\": %llu, \"elapsed_s\": %.3f}\n",
                   static_cast<unsigned long long>(samples), static_cast<unsigned long long>(lines),
                   static_cast<unsigned long long>(packets), static_cast<unsigned long long>(per_packet),
                   static_cast<unsigned long long>(send_errors), elapsed);
      std::fclose(f);
    }
  }
  for (int fd : fds) ::close(fd);
  return 0;
}
