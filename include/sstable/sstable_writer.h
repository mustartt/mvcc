//
// Created by henry on 2023-01-30.
//

#ifndef MVCC_SRC_SSTABLE_SSTABLE_WRITER_H_
#define MVCC_SRC_SSTABLE_SSTABLE_WRITER_H_

#include <fstream>
#include <vector>

#include <boost/filesystem.hpp>

#include "types.pb.h"

namespace mvcc {

class sstable_writer {
  public:
    explicit sstable_writer(std::string name,
                            const boost::filesystem::path &sst_dir,
                            int block_size, int generation, int level);
    virtual ~sstable_writer();
    sstable_writer(const sstable_writer &) = delete;
    sstable_writer(sstable_writer &&) = delete;
    sstable_writer &operator=(sstable_writer &&) = delete;
    sstable_writer &operator=(const sstable_writer &) = delete;

  public:
    void write_entry(const std::string &key, const std::string &value, bool is_delete = false);

    const boost::filesystem::path &get_index_path() const { return index_file_path; }
    const boost::filesystem::path &get_data_path() const { return data_file_path; }

  private:
    void write_header();
    void flush_block();

  private:
    std::string name;
    int generation_count;
    int level;

    boost::filesystem::path data_file_path;
    boost::filesystem::path index_file_path;
    boost::filesystem::path meta_file_path;

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
