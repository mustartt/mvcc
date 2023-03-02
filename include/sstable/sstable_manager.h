//
// Created by henry on 2023-02-07.
//

#ifndef MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_
#define MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_

#include <string>
#include <fstream>
#include <map>
#include <set>

#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "sstable_writer.h"
#include "sstable.h"

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
    explicit sstable_manager(const std::string &sst_dir);

    void initialize_directory();
    void discover_existing_sstables();
    void serialize_manager_state();

    [[nodiscard]] std::pair<std::unique_ptr<sstable_writer>, sstable_record> create_sstable(int level = 0);
    [[nodiscard]] sstable load_sstable(const sstable_record &record);
    [[nodiscard]] sstable load_sstable(const std::string &table);
    [[nodiscard]] std::set<std::string> get_loaded();

  private:
    boost::uuids::random_generator uuid_gen;

    int default_blk_size = 2 * 1024;
    int default_cache_size = 4 * 1024 * 1024;

    boost::filesystem::path sst_dir;
    int generation_count = 0;

    std::map<int, sstable_record, std::greater<>> table_records;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_SSTABLE_MANAGER_H_
