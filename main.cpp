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
#include "sstable/sstable_manager.h"

using namespace mvcc;

void print_kv(const key_value &kv) {
    std::cout << "(" << kv.key
              << " ver: " << kv.mvcc << " "
              << (kv.is_tombstone ? "<null>" : kv.value)
              << ")" << std::endl;
}

int main() {
    sstable_manager manager("sstables");

    manager.initialize_directory();

//    auto sst = manager.create_sstable(0);

    return 0;
}
