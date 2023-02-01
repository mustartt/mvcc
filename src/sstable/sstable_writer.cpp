//
// Created by henry on 2023-01-30.
//

#include "sstable/sstable_writer.h"

mvcc::sstable_writer::sstable_writer(const std::string &name, int block_size)
    : data_file(name + ".data", std::ios::binary | std::ios::out | std::ios::trunc),
      index_file(name + ".index", std::ios::binary | std::ios::out | std::ios::trunc),
      block_size(block_size), block_count(0) {
    buffer.resize(block_size);
}

mvcc::sstable_writer::~sstable_writer() {
    flush_block();
    std::cout << "wrote " << block_count << " blocks" << std::endl;
}

void mvcc::sstable_writer::write_entry(const std::string &key, int64_t version,
                                       const std::string &value, bool is_delete) {
    mvcc::BlockIndex_BlockIndexEntry index_entry;
    index_entry.set_key(key);
    index_entry.set_version(version);
    if (is_delete) {
        index_entry.set_is_tombstone(true);
    } else {
        index_entry.set_is_tombstone(false);

        auto offset = data_file.tellp();
        index_entry.set_offset(offset);
        index_entry.set_length(value.size());

        data_file.write(value.data(), static_cast<long>(value.size()));
    }
    if (8 + index_entry.ByteSizeLong() + current_block.ByteSizeLong() > block_size) {
        flush_block();
    }
    auto new_entry = current_block.add_entries();
    new_entry->CopyFrom(index_entry);
}

void mvcc::sstable_writer::flush_block() {
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
