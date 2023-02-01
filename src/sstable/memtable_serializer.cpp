//
// Created by henry on 2023-02-01.
//

#include "sstable/memtable_serializer.h"

namespace mvcc {

void memtable_serializer::serialize() {
    for (auto iter = table.begin(); iter != table.end(); ++iter) {
        auto next = iter;
        auto curr_kv = *iter;
        if (++next != table.end()) {
            auto next_kv = *next;
            bool should_skip = next_kv.is_tombstone
                && next_kv.key == curr_kv.key
                && next_kv.mvcc == curr_kv.mvcc;
            if (should_skip) {
                iter = next;
                continue;
            }
        }
        writer.write_entry(curr_kv.key, curr_kv.mvcc, curr_kv.value, curr_kv.is_tombstone);
    }
}

} // mvcc
