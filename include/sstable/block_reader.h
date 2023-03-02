//
// Created by henry on 2023-01-31.
//

#ifndef MVCC_INCLUDE_SSTABLE_BLOCK_READER_H_
#define MVCC_INCLUDE_SSTABLE_BLOCK_READER_H_

#include <fstream>
#include <vector>

#include "block_cache.h"

namespace mvcc {

class block_reader {
  public:
    explicit block_reader(const std::string &path, int blk_size, int cache_size = 10);
 public:
    const block& get(int index);
    int size() const;

  private:
    void load_block(int index);

  private:
    block_lfu_cache cache;
    std::ifstream index_file;
    int block_count;
    int block_size;
    std::vector<char> read_buffer;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_BLOCK_READER_H_
