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

int main() {
    using namespace mvcc;

    int blk_size = 4 * 1024;
    {
        sstable_writer writer("sstable", blk_size);
        boost::timer::auto_cpu_timer t;

        for (int i = 0; i < 100000; ++i) {
            std::string key = "key:";
            std::string val = "val:";
            key.append(std::to_string(i));
            val.append(std::to_string(i));
            writer.write_entry(key, i, val);
        }
    }
    {
        boost::timer::auto_cpu_timer t;

        block_reader reader("sstable", blk_size, 10);
        for (int i = 0; i < reader.size(); ++i) {
            auto &blk = reader.get(i);
            auto entry = blk.first_key();
            std::cout << entry.key() << std::endl;
        }
    }
    return 0;
}
