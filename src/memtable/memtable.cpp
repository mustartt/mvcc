//
// Created by henry on 2023-01-25.
//

#include "memtable/memtable.h"

#include <chrono>
#include <mutex>
#include <ranges>
#include <iostream>

namespace mvcc {

std::optional<std::string> memtable::get(const std::string &key, memtable::mvcc_timestamp_t timestamp) {
    std::shared_lock _(rw_lock);
    if (timestamp < 0) {
        // uncommitted transaction
        if (!insert_table.contains(key)) return std::nullopt;
        const auto &ins_versions = insert_table[key];
        if (auto insert_value = ins_versions.find(timestamp); insert_value != ins_versions.end()) {
            const auto &[latest_uncommitted_timestamp, value] = *insert_value;
            if (!delete_table.contains(key)) return value;
            const auto &del_versions = delete_table[key];
            if (del_versions.contains(latest_uncommitted_timestamp)) {
                return std::nullopt;
            }
            return value;
        }
        return std::nullopt;
    } else {
        // committed transaction
        if (!insert_table.contains(key)) return std::nullopt;
        const auto &ins_versions = insert_table[key];
        if (auto latest_val = ins_versions.lower_bound(timestamp); latest_val != ins_versions.end()) {
            const auto &[latest_insert_timestamp, value] = *latest_val;
            if (!delete_table.contains(key)) return value;
            const auto &del_versions = delete_table[key];
            if (auto latest_del = del_versions.lower_bound(timestamp); latest_del != del_versions.end()) {
                const auto latest_delete_timestamp = *latest_del;
                if (latest_delete_timestamp >= latest_insert_timestamp) {
                    return std::nullopt;
                } else {
                    return value;
                }
            } else {
                return value;
            }
        }
        return std::nullopt;
    }
}

void memtable::put(const std::string &key, memtable::mvcc_timestamp_t timestamp, std::string value) {
    std::unique_lock _(rw_lock);
    insert_table[key].emplace(timestamp, std::move(value));
}

void memtable::del(const std::string &key, memtable::mvcc_timestamp_t timestamp) {
    std::unique_lock _(rw_lock);
    delete_table[key].insert(timestamp);
}

} // mvcc
