//
// Created by henry on 2023-01-31.
//

#include "sstable/block_reader.h"

namespace mvcc {

block_reader::block_reader(const std::string &path, int blk_size, int cache_size)
    : cache(cache_size),
      index_file(path, std::ios::binary | std::ios::ate),
      block_size(blk_size) {
    if (index_file.tellg() % blk_size != 0)
        throw std::runtime_error("block reader: index not block aligned");
    block_count = (int)(index_file.tellg() / (long)blk_size);
    read_buffer.resize(block_size);
}

const block &block_reader::get(int index) {
    if (!cache.contains(index))
        load_block(index);
    return cache.get(index);
}

int block_reader::size() const {
    return block_count;
}

void block_reader::load_block(int index) {
    if (index >= block_count)
        throw std::runtime_error("block reader: load index out of bound");

    index_file.seekg(index * block_size);
    index_file.read(read_buffer.data(), block_size);

    block block(read_buffer.data(), block_size);
    cache.put(index, std::move(block));
}

} // mvcc
