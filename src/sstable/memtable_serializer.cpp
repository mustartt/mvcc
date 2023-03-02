//
// Created by henry on 2023-02-01.
//

#include "sstable/memtable_serializer.h"

namespace mvcc {

void memtable_serializer::serialize() {
    for (auto kv: table) {
        writer.write_entry(kv.key, kv.value, kv.is_tombstone);
    }
}

} // mvcc
