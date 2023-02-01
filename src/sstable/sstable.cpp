//
// Created by henry on 2023-01-31.
//

#include "sstable/sstable.h"

namespace mvcc {

sstable::sstable(const std::string &name, int blk_size, int cache_size)
    : reader(name, blk_size, cache_size),
      data_file(name + ".data", std::ios::binary) {}

int sstable::find_entry_block(const std::string &key) {
    int left_blk = 0;
    int right_blk = reader.size() - 1;
    int result = 0;

    while (left_blk <= right_blk) {
        int mid_idx = left_blk + (right_blk - left_blk) / 2;
        const auto &mid_blk = reader.get(mid_idx);
        if (mid_blk.first_key().key() <= key) {
            result = mid_idx;
            left_blk = mid_idx + 1;
        } else {
            right_blk = mid_idx - 1;
        }
    }
    return result;
}

int sstable::find_key_in_block(const std::string &key, int blk) {
    const auto &block = reader.get(blk);
    int left = 0;
    int right = (int)block.size() - 1;
    int result = 0;

    while (left <= right) {
        int mid_idx = left + (right - left) / 2;
        const auto &mid_key = block.at(mid_idx).key();
        if (mid_key <= key) {
            result = mid_idx;
            left = mid_idx + 1;
        } else {
            right = mid_idx - 1;
        }
    }

    return result;
}

sstable::iterator sstable::begin() {
    const auto &first_blk = reader.get(0);
    return {*this, static_cast<int>(first_blk.size()), 0, 0};
}

sstable::iterator sstable::end() {
    return {*this, 0, reader.size(), 0};
}

const block &sstable::get_blk(int index) {
    return reader.get(index);
}

int sstable::get_block_count() const {
    return reader.size();
}

std::string sstable::get_value(uint64_t offset, uint64_t length) {
    std::string value;
    value.resize(length);
    data_file.seekg(static_cast<long>(offset));
    data_file.read(value.data(), static_cast<long>(length));
    return value;
}

sstable::iterator sstable::find(const std::string &key) {
    int block_id = find_entry_block(key);
    int blk_index = find_key_in_block(key, block_id);
    return {*this, static_cast<int>(reader.get(block_id).size()), block_id, blk_index};
}

sstable::iterator &sstable::iterator::operator++() {
    ++idx;
    if (idx >= curr_blk_size) {
        ++blk;
        idx = 0;
        if (blk < table.get_block_count())
            curr_blk_size = table.get_blk(blk).size();
    }
    return *this;
}

key_value sstable::iterator::operator*() {
    const auto &entry = table.get_blk(blk).at(idx);
    return {
        entry.key(),
        table.get_value(entry.offset(), entry.length()),
        entry.version(),
        entry.is_tombstone()
    };
}

} // mvcc
