#pragma once

// Bump allocator over one buffer that is allocated once at startup.
//
// allocate() is a pointer bump: no locks, no free lists, no syscalls. It
// returns nullptr when full instead of growing, so memory use has a hard
// ceiling. Nothing is ever freed individually; that's fine for interned
// strings, which live as long as the process.

#include <cstddef>
#include <memory>

namespace flux {

class Arena {
 public:
  // Value-initialization zero-fills the buffer, which touches every page now
  // instead of taking page faults later on the hot path.
  explicit Arena(std::size_t bytes) : buf_(new char[bytes]()), cap_(bytes) {}

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;

  // Returns nullptr if the arena is out of space.
  [[nodiscard]] char* allocate(std::size_t n) noexcept {
    if (n > cap_ - used_) return nullptr;
    char* p = buf_.get() + used_;
    used_ += n;
    return p;
  }

  const char* base() const noexcept { return buf_.get(); }
  std::size_t used() const noexcept { return used_; }
  std::size_t capacity() const noexcept { return cap_; }

 private:
  std::unique_ptr<char[]> buf_;
  std::size_t cap_;
  std::size_t used_ = 0;
};

}  // namespace flux
