//
// Created by henry on 2023-01-31.
//

#include "sstable/block.h"

namespace mvcc {

block::block_entry::block_entry(const BlockIndex_BlockIndexEntry &entry)
    : entry(entry) {}

const std::string &block::block_entry::key() const noexcept {
    return entry.key();
}

int64_t block::block_entry::version() const noexcept {
    return entry.version();
}

uint64_t block::block_entry::offset() const noexcept {
    return entry.offset();
}

uint64_t block::block_entry::length() const noexcept {
    return entry.length();
}

bool block::block_entry::is_tombstone() const noexcept {
    return entry.is_tombstone();
}

void block::parse_from(const void *data, int size) {
    blk.ParseFromArray(data, size);
}

block::block_entry block::first_key() const {
    return block_entry(blk.entries().at(0));
}

block::block_entry block::at(size_t index) const {
    return block_entry(blk.entries().at(static_cast<int>(index)));
}

size_t block::size() const noexcept {
    return blk.entries_size();
}

block::block(const void *data, int len) : blk() {
    google::protobuf::io::ArrayInputStream array_stream(data, len);
    google::protobuf::io::CodedInputStream input_stream(&array_stream);

    size_t blk_length;
    input_stream.ReadLittleEndian64(&blk_length);
    blk.ParseFromArray(static_cast<const char *>(data) + sizeof(uint64_t), blk_length);
//    std::cout << "Block Loaded" << std::endl;
}

block::~block() {
//    std::cout << "Block Freed" << std::endl;
}

} // mvcc
