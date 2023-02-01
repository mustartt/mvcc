//
// Created by henry on 2023-01-31.
//

#ifndef MVCC_INCLUDE_SSTABLE_SSTABLE_H_
#define MVCC_INCLUDE_SSTABLE_SSTABLE_H_

#include "memtable/memtable.h"
#include "block_reader.h"

namespace mvcc {

class sstable {
  public:
    sstable(const std::string &name, int blk_size, int cache_size = 10);
  public:
    class iterator : public std::iterator<std::forward_iterator_tag, key_value> {
      public:
        iterator(sstable &table, int curr_blk_size, int blk, int idx)
            : table(table), curr_blk_size(curr_blk_size), blk(blk), idx(idx) {};
      public:
        iterator &operator++();
        key_value operator*();
        bool operator==(const iterator &other) const {
            return other.idx == idx && other.blk == blk;
        }
        bool operator!=(const iterator &other) const {
            return !(*this == other);
        }
      private:
        sstable &table;
        int curr_blk_size;
        int blk;
        int idx;
    };

    iterator begin();
    iterator end();

    iterator find(const std::string &key);

  private:
    int find_entry_block(const std::string &key);
    int find_key_in_block(const std::string &key, int blk);

    std::string get_value(uint64_t offset, uint64_t length);
    const block &get_blk(int index);
    int get_block_count() const;

  private:
    block_reader reader;
    std::ifstream data_file;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_SSTABLE_H_
