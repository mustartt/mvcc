//
// Created by henry on 2023-01-30.
//

#include "sstable/sstable_writer.h"

#include <boost/json.hpp>
#include <utility>

namespace mvcc {

sstable_writer::sstable_writer(std::string name,
                               const boost::filesystem::path &sst_dir,
                               int block_size, int gen_count, int level)
    : name(std::move(name)), generation_count(gen_count), level(level),
      data_file_path(sst_dir), index_file_path(sst_dir), meta_file_path(sst_dir),
      block_size(block_size), block_count(0) {

    buffer.resize(block_size);

    meta_file_path.append("sstable.meta.json");
    data_file_path.append("sstable.data");
    index_file_path.append("sstable.index");

    meta_file = std::ofstream(meta_file_path.string(), std::ios::out | std::ios::trunc);
    data_file = std::ofstream(data_file_path.string(), std::ios::binary | std::ios::out | std::ios::trunc);
    index_file = std::ofstream(index_file_path.string(), std::ios::binary | std::ios::out | std::ios::trunc);
}

sstable_writer::~sstable_writer() {
    flush_block();
    write_header();
}

void sstable_writer::write_entry(const std::string &key, const std::string &value, bool is_delete) {
    BlockIndex_BlockIndexEntry index_entry;
    index_entry.set_key(key);
    if (is_delete) {
        index_entry.set_is_tombstone(true);
    } else {
        index_entry.set_is_tombstone(false);

        auto offset = data_file.tellp();
        index_entry.set_offset(offset);
        index_entry.set_length(value.size());

        data_file.write(value.data(), static_cast<long>(value.size()) + 1);
    }
    if (8 + index_entry.ByteSizeLong() + current_block.ByteSizeLong() > block_size) {
        flush_block();
    }
    auto new_entry = current_block.add_entries();
    new_entry->CopyFrom(index_entry);
}

void sstable_writer::flush_block() {
    size_t curr_size = current_block.ByteSizeLong();
    ++block_count;
    std::fill(buffer.begin(), buffer.end(), 0);
    {
        google::protobuf::io::ArrayOutputStream array_stream(buffer.data(), block_size);
        google::protobuf::io::CodedOutputStream output_stream(&array_stream);

        output_stream.WriteLittleEndian64(curr_size);
        current_block.SerializeToCodedStream(&output_stream);
    }
    index_file.write(buffer.data(), block_size);

    current_block.Clear();
}

void sstable_writer::write_header() {
    boost::json::object obj;

    obj["name"] = name;
    obj["version"] = "0.0.0-dev";
    obj["index_file"] = index_file_path.string();
    obj["data_file"] = data_file_path.string();
    obj["block_count"] = block_count;
    obj["block_size"] = block_size;
    obj["generation_count"] = generation_count;
    obj["level"] = level;

    meta_file << boost::json::serialize(obj) << "\n";
}

}
