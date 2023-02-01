#include <iostream>
#include <thread>
#include <vector>
#include <mutex>
#include <sstream>
#include <boost/timer/timer.hpp>
#include <boost/iostreams/stream.hpp>

#include "memtable/memtable.h"
#include "wal.h"
#include "sstable/block_reader.h"
#include "sstable/memtable_serializer.h"

using namespace mvcc;

void print_kv(const key_value &kv) {
    std::cout << "(" << kv.key
              << " ver: " << kv.mvcc << " "
              << (kv.is_tombstone ? "<null>" : kv.value)
              << ")" << std::endl;
}

int main() {
    memtable table;

    table.del("key:1", 1);
    table.del("key:1", 2);
    table.del("key:1", 3);
    table.del("key:1", 4);


    {
        auto _ = table.read_lock();
        auto iter = table.find("key:0");
        auto end = table.end();

        for (; iter != end; ++iter) {
            auto kv = *iter;
            print_kv(kv);
        }
        std::cout << "=====" << std::boolalpha << std::endl;

        memtable_serializer serializer("new_table", 2048, 0, table);
        serializer.serialize();
    }

    return 0;
}
