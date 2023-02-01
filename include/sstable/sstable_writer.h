//
// Created by henry on 2023-01-30.
//

#ifndef MVCC_SRC_SSTABLE_SSTABLE_WRITER_H_
#define MVCC_SRC_SSTABLE_SSTABLE_WRITER_H_

#include <fstream>
#include <vector>

#include "types.pb.h"

namespace mvcc {

class sstable_writer {
  public:
    explicit sstable_writer(const std::string &name, int block_size, int generation);
    virtual ~sstable_writer();

  public:
    void write_entry(const std::string &key, int64_t version,
                     const std::string &value, bool is_delete = false);
  private:
    void write_header();
    void flush_block();

  private:
    std::string name;
    int generation_count;

    std::ofstream meta_file;
    std::ofstream data_file;
    std::ofstream index_file;
    mvcc::BlockIndex current_block;
    std::vector<char> buffer;
    int block_size;
    size_t block_count;
};

}

#endif //MVCC_SRC_SSTABLE_SSTABLE_WRITER_H_
