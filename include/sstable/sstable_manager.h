//
// Created by henry on 2023-02-07.
//

#ifndef MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_
#define MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_

#include <string>
#include <fstream>
#include <mutex>

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/json/src.hpp>

#include "sstable_writer.h"

namespace mvcc {

class sstable_manager {
  public:
    class sstable_record {
      public:
        sstable_record(std::string name,
                       boost::filesystem::path indexFile,
                       boost::filesystem::path dataFile,
                       int generation, int blockCount, int level);

      public:
        std::string name;
        boost::filesystem::path index_file;
        boost::filesystem::path data_file;

        int generation;
        int block_size;
        int level;
    };
  public:
    sstable_manager(const std::string &sst_dir);

    void initialize_directory();
    void discover_existing_sstables();
    void serialize_manager_state();

    sstable_writer create_sstable(int level = 0);

  private:
    std::mutex mutex;
    boost::uuids::random_generator uuid_gen;

    int default_blk_size = 2048;

    boost::filesystem::path sst_dir;
    int generation_count = 0;

    std::vector<sstable_record> table_records;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_
