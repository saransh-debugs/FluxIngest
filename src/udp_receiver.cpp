#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1  // recvmmsg
#endif

#include "udp_receiver.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <system_error>
#include <vector>

namespace flux {

namespace {
[[noreturn]] void throw_errno(const char* what) { throw std::system_error(errno, std::generic_category(), what); }

#if defined(__linux__) && defined(SO_RXQ_OVFL)
[[maybe_unused]] constexpr bool kHaveOvfl = true;
constexpr std::size_t kCtrlBytes = CMSG_SPACE(sizeof(std::uint32_t));
#else
[[maybe_unused]] constexpr bool kHaveOvfl = false;
constexpr std::size_t kCtrlBytes = 0;
#endif
}  // namespace

// All buffers are allocated here, once. Nothing is allocated per packet.
struct UdpReceiver::Impl {
  std::uint32_t batch;
  std::uint32_t max_datagram;
  std::unique_ptr<char[]> data;  // batch * max_datagram
  std::vector<iovec> iov;
  std::vector<std::uint32_t> len;
  std::vector<char> ctrl;
#if defined(__linux__)
  std::vector<mmsghdr> msgs;
#else
  msghdr msg{};
#endif
  std::uint32_t last_ovfl = 0;

  Impl(std::uint32_t b, std::uint32_t md)
      : batch(b), max_datagram(md), data(new char[std::size_t{b} * md]()), iov(b), len(b), ctrl(std::size_t{b} * kCtrlBytes + 1) {
    for (std::uint32_t i = 0; i < b; ++i) {
      iov[i].iov_base = data.get() + std::size_t{i} * md;
      iov[i].iov_len = md;
    }
#if defined(__linux__)
    msgs.resize(b);
#endif
  }
};

UdpReceiver::UdpReceiver(const UdpConfig& cfg, ProducerStats& stats)
    : impl_(std::make_unique<Impl>(cfg.batch, cfg.max_datagram)), stats_(stats) {
  fd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
  if (fd_ < 0) throw_errno("socket");
  const int one = 1;
  if (cfg.reuseport && ::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &one, sizeof one) != 0) {
    ::close(fd_);
    throw_errno("SO_REUSEPORT");
  }
  // Best effort: Linux caps this at net.core.rmem_max. We read back what we
  // actually got and report it.
  (void)::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &cfg.rcvbuf_bytes, sizeof cfg.rcvbuf_bytes);
  socklen_t sl = sizeof rcvbuf_actual_;
  (void)::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf_actual_, &sl);
#if defined(__linux__) && defined(SO_RXQ_OVFL)
  (void)::setsockopt(fd_, SOL_SOCKET, SO_RXQ_OVFL, &one, sizeof one);
#endif

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(cfg.port);
  if (::inet_pton(AF_INET, cfg.bind_addr.c_str(), &addr.sin_addr) != 1) {
    ::close(fd_);
    throw std::system_error(EINVAL, std::generic_category(), "bad bind address " + cfg.bind_addr);
  }
  if (::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof addr) != 0) {
    const int e = errno;
    ::close(fd_);
    throw std::system_error(e, std::generic_category(), "bind");
  }
  socklen_t al = sizeof addr;
  (void)::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &al);
  port_ = ntohs(addr.sin_port);
}

UdpReceiver::~UdpReceiver() {
  if (fd_ >= 0) ::close(fd_);
}

std::size_t UdpReceiver::memory_bytes(const UdpConfig& cfg) noexcept {
  return std::size_t{cfg.batch} * (cfg.max_datagram + sizeof(iovec) + 64 + kCtrlBytes);
}

std::string_view UdpReceiver::packet(std::size_t i) const noexcept {
  return {static_cast<const char*>(impl_->iov[i].iov_base), impl_->len[i]};
}

std::size_t UdpReceiver::receive(int timeout_ms) noexcept {
  pollfd p{fd_, POLLIN, 0};
  const int pr = ::poll(&p, 1, timeout_ms);
  if (pr <= 0) {
    if (pr < 0 && errno != EINTR) stats_.recv_errors.add();
    return 0;
  }
  Impl& im = *impl_;

#if defined(__linux__)
  for (std::uint32_t i = 0; i < im.batch; ++i) {
    msghdr& h = im.msgs[i].msg_hdr;
    h = msghdr{};
    h.msg_iov = &im.iov[i];
    h.msg_iovlen = 1;
    if constexpr (kHaveOvfl) {
      h.msg_control = im.ctrl.data() + std::size_t{i} * kCtrlBytes;
      h.msg_controllen = kCtrlBytes;
    }
  }
  const int n = ::recvmmsg(fd_, im.msgs.data(), im.batch, MSG_DONTWAIT, nullptr);
  if (n < 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) stats_.recv_errors.add();
    return 0;
  }
  for (int i = 0; i < n; ++i) {
    const msghdr& h = im.msgs[static_cast<std::size_t>(i)].msg_hdr;
    std::uint32_t len = im.msgs[static_cast<std::size_t>(i)].msg_len;
    if (h.msg_flags & MSG_TRUNC) {
      stats_.truncated_datagrams.add();
      len = 0;
    }
    im.len[static_cast<std::size_t>(i)] = len;
#if defined(SO_RXQ_OVFL)
    for (cmsghdr* c = CMSG_FIRSTHDR(&h); c != nullptr; c = CMSG_NXTHDR(const_cast<msghdr*>(&h), c)) {
      if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SO_RXQ_OVFL) {
        std::uint32_t total;
        std::memcpy(&total, CMSG_DATA(c), sizeof total);
        // Cumulative counter for the socket; add only what's new.
        if (total != im.last_ovfl) {
          stats_.kernel_drops.add(total - im.last_ovfl);
          im.last_ovfl = total;
        }
      }
    }
#endif
  }
  return static_cast<std::size_t>(n);
#else
  std::size_t n = 0;
  while (n < im.batch) {
    im.msg = msghdr{};
    im.msg.msg_iov = &im.iov[n];
    im.msg.msg_iovlen = 1;
    const ssize_t r = ::recvmsg(fd_, &im.msg, MSG_DONTWAIT);
    if (r < 0) {
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) stats_.recv_errors.add();
      break;
    }
    if (im.msg.msg_flags & MSG_TRUNC) {
      stats_.truncated_datagrams.add();
      im.len[n] = 0;
    } else {
      im.len[n] = static_cast<std::uint32_t>(r);
    }
    ++n;
  }
  return n;
#endif
}

}  // namespace flux
