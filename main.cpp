#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <random>
#include <sstream>
#include <boost/timer/timer.hpp>

#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/stream.hpp>
#include <unistd.h>

#include "memtable/memtable.h"
#include "wal.h"
#include "sstable/sstable_writer.h"
#include "sstable/block.h"
#include "sstable/block_reader.h"
#include "sstable/sstable.h"

using namespace mvcc;

void print_kv(const key_value &kv) {
    std::cout << "(" << kv.key
              << " ver: " << kv.mvcc << " "
              << (kv.is_tombstone ? "<null>" : kv.value)
              << ")" << std::endl;
}

int main() {
    memtable table;

    table.del("key:0", 1);
    table.put("key:1", 1, "value:1");
    table.put("key:1", 2, "value:2");
    table.put("key:2", 1, "value:1");
    table.put("key:2", 2, "value:2");
    table.del("key:0", 1);
    table.del("key:1", 1);
    table.put("key:2", 3, "value:3");
    table.put("key:4", 4, "value:4");

    auto iter = table.find("key:0");
    auto end = table.end();

    for (; iter != end; ++iter) {
        auto kv = *iter;
        print_kv(kv);
    }

    return 0;
}
