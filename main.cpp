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

int main() {
    using namespace mvcc;

    int blk_size = 1024;

    std::vector<std::string> values;
    for (int i = 0; i < 10000; ++i) {
        values.emplace_back(std::to_string(i));
    }
    std::sort(values.begin(), values.end());

    {
        sstable_writer writer("sstable", blk_size);
        boost::timer::auto_cpu_timer t;

        long mvcc = 0;
        for (const auto &n: values) {
            writer.write_entry("key:" + n, mvcc, "value:" + n);
            ++mvcc;
        }
    }
//    std::random_shuffle(values.begin(), values.end());
    {
        sstable table("sstable", blk_size, 50);

        boost::timer::auto_cpu_timer t;
        for (auto iter = table.find("key:9990"); iter != table.end(); ++iter) {
            std::cout << (*iter).key << " - " << (*iter).value << std::endl;
        }
    }
    return 0;
}
