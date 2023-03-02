//
// Created by henry on 2023-01-25.
//

#include "memtable/memtable.h"

#include <chrono>

namespace mvcc {

void memtable::put(const std::string &key, std::string value) {
    std::unique_lock _(rw_lock);
    insert_table[key] = std::move(value);
}

void memtable::del(const std::string &key) {
    std::unique_lock _(rw_lock);
    delete_table.insert(key);
}

memtable::iterator memtable::begin() {
    return {insert_table.cbegin(), delete_table.cbegin(),
            insert_table.cend(), delete_table.cend()};
}

memtable::iterator memtable::end() {
    return {insert_table.cend(), delete_table.cend(),
            insert_table.cend(), delete_table.cend()};
}

memtable::iterator memtable::find(const std::string &key) {
    return {insert_table.lower_bound(key),
            delete_table.lower_bound(key),
            insert_table.cend(), delete_table.cend()};
}

std::shared_lock<std::shared_mutex> memtable::read_lock() {
    return std::shared_lock<std::shared_mutex>(rw_lock);
}

std::unique_lock<std::shared_mutex> memtable::write_lock() {
    return std::unique_lock<std::shared_mutex>(rw_lock);
}

memtable::iterator &memtable::iterator::operator++() {
    if (insert_iter != insert_iter_end && delete_iter != delete_iter_end) {
        const auto &[left_key, value] = *insert_iter;
        const auto &right_key = *delete_iter;
        if (right_key < left_key) {
            ++delete_iter;
        } else {
            ++insert_iter;
        }
    } else if (insert_iter != insert_iter_end) {
        ++insert_iter;
    } else if (delete_iter != delete_iter_end) {
        ++delete_iter;
    } else {
        throw std::runtime_error("memtable::iterator: bad iterator");
    }
    return *this;
}

key_value memtable::iterator::operator*() {
    if (insert_iter != insert_iter_end && delete_iter != delete_iter_end) {
        const auto &[left_key, value] = *insert_iter;
        const auto &right_key = *delete_iter;
        if (right_key < left_key) {
            return {right_key, "", true};
        } else {
            return {left_key, value, false};
        }
    } else if (insert_iter != insert_iter_end) {
        const auto &[left_key, value] = *insert_iter;
        return {left_key, value, false};
    } else if (delete_iter != delete_iter_end) {
        const auto &right_key = *delete_iter;
        return {right_key, "", true};
    } else {
        throw std::runtime_error("memtable::iterator: bad iterator");
    }
}

bool memtable::iterator::operator==(const memtable::iterator &other) const {
    return insert_iter == other.insert_iter
        && delete_iter == other.delete_iter;
}

bool memtable::iterator::operator!=(const memtable::iterator &other) const {
    return !(*this == other);
}

} // mvcc
