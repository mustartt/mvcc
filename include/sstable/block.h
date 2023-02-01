//
// Created by henry on 2023-01-31.
//

#ifndef MVCC_INCLUDE_SSTABLE_BLOCK_H_
#define MVCC_INCLUDE_SSTABLE_BLOCK_H_

#include <string>

#include "types.pb.h"

namespace mvcc {

class block {
  public:
    static constexpr int block_size = 1024;

    class block_entry {
      public:
        explicit block_entry(const BlockIndex_BlockIndexEntry &entry);
      public:
        [[nodiscard]] const std::string &key() const noexcept;
        [[nodiscard]] int64_t version() const noexcept;
        [[nodiscard]] uint64_t offset() const noexcept;
        [[nodiscard]] uint64_t length() const noexcept;
        [[nodiscard]] bool is_tombstone() const noexcept;
      private:
        const mvcc::BlockIndex_BlockIndexEntry &entry;
    };

  public:
    explicit block(const void *data, int len = block_size);
    block(const block &) = delete;
    block(block &&) = default;
    block &operator=(const block &) = delete;
    block &operator=(block &&) = default;

  public:
    block_entry first_key() const;
    block_entry at(size_t index) const;
    size_t size() const noexcept;

  private:
    void parse_from(const void *data, int len = block_size);

  private:
    mvcc::BlockIndex blk;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_BLOCK_H_
