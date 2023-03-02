//
// Created by henry on 2023-02-07.
//

#include "sstable/sstable_manager.h"

#include <utility>

#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>

#include <stdexcept>
#include <boost/json/src.hpp>
#include <boost/log/trivial.hpp>

namespace mvcc {

using namespace boost::filesystem;

sstable_manager::sstable_manager(const std::string &dir)
    : sst_dir(dir) {
    if (!exists(sst_dir) || !is_directory(sst_dir))
        throw std::runtime_error("sstable_manager: " + sst_dir.string() + " is not a directory.");
}

void sstable_manager::initialize_directory() {
}

sstable_manager::sstable_record load_sstable_metadata(const boost::filesystem::path &path) {
    if (!exists(path) || !is_regular_file(path)) {
        throw std::runtime_error("sstable_manager: cannot open " + path.string() + " metadata");
    }
    std::ifstream meta_file(path.string());
    boost::json::object metadata = boost::json::parse(meta_file).as_object();

    auto index_file = boost::filesystem::path(metadata["index_file"].as_string());
    auto data_file = boost::filesystem::path(metadata["data_file"].as_string());
    boost::json::string &table_name = metadata["name"].as_string();

    int gen_count = (int)metadata["generation_count"].as_int64();
    int blk_size = (int)metadata["block_size"].as_int64();
    int level = (int)metadata["level"].as_int64();

    return {std::string(table_name.c_str()), index_file, data_file,
            gen_count, blk_size, level};
}

void sstable_manager::discover_existing_sstables() {
    BOOST_LOG_TRIVIAL(info) << "Starting to automatically discover existing sstables";

    if (!exists(sst_dir) || !is_directory(sst_dir))
        throw std::runtime_error("sstable_manager: " + sst_dir.string() + " is not a directory.");
    for (auto &&dir_entry: directory_iterator(sst_dir)) {
        if (is_directory(dir_entry)) {
            auto path = dir_entry.path();
            path.append("sstable.meta.json");
            auto record = load_sstable_metadata(path);

            if (record.generation >= generation_count)
                generation_count = record.generation + 1;
            table_records.emplace(record.generation, std::move(record));
        }
    }
}

std::pair<std::unique_ptr<sstable_writer>, sstable_manager::sstable_record>
sstable_manager::create_sstable(int level) {
    auto table_name = boost::uuids::to_string(uuid_gen());
    BOOST_LOG_TRIVIAL(info) << "Creating sstable " << table_name;

    auto table_directory = sst_dir;
    table_directory.append(table_name);

    BOOST_LOG_TRIVIAL(trace) << "Creating sstable directory: " << table_name;
    create_directory(table_directory);

    auto writer = std::make_unique<sstable_writer>(table_name, table_directory,
                                                   default_blk_size, generation_count, level);
    sstable_record record(table_name, writer->get_index_path(), writer->get_data_path(),
                          generation_count, default_blk_size, level);
    table_records.emplace(generation_count, record);

    generation_count++;
    return std::make_pair(std::move(writer), record);
}

void sstable_manager::serialize_manager_state() {

}

sstable sstable_manager::load_sstable(const sstable_record &record) {
    return {
        record.name,
        record.index_file.string(), record.data_file.string(),
        record.block_size, default_cache_size / record.block_size
    };
}

std::set<std::string> sstable_manager::get_loaded() {
    std::set<std::string> tables;
    for (const auto &[gen, record]: table_records) {
        tables.insert(record.name);
    }
    return tables;
}

sstable sstable_manager::load_sstable(const std::string &table) {
    auto path = sst_dir;
    path.append(table);
    path.append("sstable.meta.json");

    auto record = load_sstable_metadata(path);
    return load_sstable(record);
}

sstable_manager::sstable_record::sstable_record(
    std::string name, path indexFile, path dataFile,
    int generation, int blockSize, int level)
    : name(std::move(name)), index_file(std::move(indexFile)),
      data_file(std::move(dataFile)), generation(generation),
      block_size(blockSize), level(level) {}

} // mvcc
