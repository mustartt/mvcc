//
// Created by henry on 2023-02-07.
//

#include "sstable/sstable_manager.h"

#include <utility>

#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>

#include <stdexcept>

#include <boost/log/trivial.hpp>

namespace mvcc {

using namespace boost::filesystem;

sstable_manager::sstable_manager(const std::string &dir)
    : sst_dir(dir) {
    if (!exists(sst_dir) || !is_directory(sst_dir))
        throw std::runtime_error("sstable_manager: " + sst_dir.string() + " is not a directory.");
    initialize_directory();
}

void sstable_manager::initialize_directory() {
    discover_existing_sstables();
}

void sstable_manager::discover_existing_sstables() {
    BOOST_LOG_TRIVIAL(info) << "Starting to automatically discover existing sstables";

    if (!exists(sst_dir) || !is_directory(sst_dir))
        throw std::runtime_error("sstable_manager: " + sst_dir.string() + " is not a directory.");
    for (auto &&dir_entry: directory_iterator(sst_dir)) {
        if (is_directory(dir_entry)) {
            auto path = dir_entry.path();
            path.append("sstable.meta.json");
            if (exists(path) && is_regular_file(path)) {
                BOOST_LOG_TRIVIAL(info) << "Found sstable: " << path.string();

                std::ifstream meta_file(path.string());
                boost::json::object metadata = boost::json::parse(meta_file).as_object();

                auto index_file = boost::filesystem::path(metadata["index_file"].as_string());
                auto data_file = boost::filesystem::path(metadata["data_file"].as_string());
                boost::json::string &table_name = metadata["name"].as_string();

                int gen_count = (int)metadata["generation_count"].as_int64();
                int blk_size = (int)metadata["block_size"].as_int64();
                int level = (int)metadata["level"].as_int64();

                table_records.emplace_back(
                    std::string(table_name.c_str()), index_file, data_file,
                    gen_count, blk_size, level
                );

                if (gen_count >= generation_count)
                    generation_count = gen_count + 1;
            }
        }
    }
}

sstable_writer sstable_manager::create_sstable(int level) {
    std::unique_lock<std::mutex> _(mutex);

    auto table_name = boost::uuids::to_string(uuid_gen());
    BOOST_LOG_TRIVIAL(info) << "Creating sstable " << table_name;

    auto table_directory = sst_dir;
    table_directory.append(table_name);

    BOOST_LOG_TRIVIAL(trace) << "Creating sstable directory: " << table_name;
    create_directory(table_directory);

    sstable_writer writer(table_name, table_directory, default_blk_size, generation_count, level);
    table_records.emplace_back(table_name, writer.get_index_path(), writer.get_data_path(),
                               generation_count, default_blk_size, level);
    generation_count++;
    return writer;
}

void sstable_manager::serialize_manager_state() {

}

sstable_manager::sstable_record::sstable_record(
    std::string name, path indexFile, path dataFile,
    int generation, int blockSize, int level)
    : name(std::move(name)), index_file(std::move(indexFile)),
      data_file(std::move(dataFile)), generation(generation),
      block_size(blockSize), level(level) {}

} // mvcc
