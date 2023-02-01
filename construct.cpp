#include <iostream>
#include <map>
#include <string>

#include <boost/timer/timer.hpp>
#include <boost/range/combine.hpp>
#include <random>
#include <fstream>
#include <iomanip>

#include "types.pb.h"

std::default_random_engine e;
std::uniform_int_distribution<int> uniform_dist1(0, 61);

const char *base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

std::vector<std::string> gen_sorted_key_sequence(int size, int key_size, int var = 8, bool sorted = true) {
    std::uniform_int_distribution<int> random(0, var / 2);
    std::vector<std::string> keys;
    for (int i = 0; i < size; ++i) {
        std::string key;
        for (int j = 0; j < (key_size - var / 2) + random(e); ++j) {
            key += base64[uniform_dist1(e)];
        }
        keys.emplace_back(std::move(key));
    }

    if (sorted) std::sort(keys.begin(), keys.end(), std::less<>());
    return keys;
}

int main(int argc, char *argv[]) {
    std::cout << "Using Boost "
              << BOOST_VERSION / 100000 << "."  // major version
              << BOOST_VERSION / 100 % 1000 << "."  // minor version
              << BOOST_VERSION % 100                // patch level
              << std::endl;

    std::ofstream index_file("sstable.index", std::ios::binary | std::ios::out | std::ios::trunc);
    std::ofstream data_file("sstable.data", std::ios::binary | std::ios::out | std::ios::trunc);
    std::ofstream plain_file("sstable.plain", std::ios::out | std::ios::trunc);

    int count = 25;
    if (argc > 1) {
        std::string num_keys(argv[1]);
        count = std::stoi(num_keys);
    }

    boost::timer::auto_cpu_timer timer;

    auto keys = gen_sorted_key_sequence(count, 32, 8);
    auto datas = gen_sorted_key_sequence(count, 64, 32, false);

    timer.report();
    timer.stop();

    boost::timer::auto_cpu_timer timer2;

    std::cout << "writing to file" << std::endl;

    uint64_t offset = 0;

    for (const auto kv: boost::combine(keys, datas)) {
        const auto &[key, data] = kv;
        mvcc::KeyValue object;
        object.set_value(data);

        mvcc::IndexEntry index;
        index.set_key(key);
        index.set_version(0);
        index.set_offset(offset);
        index.set_length(object.ByteSizeLong());

        object.SerializeToOstream(&data_file);

        uint32_t entry_size = index.ByteSizeLong();
        index_file.write(reinterpret_cast<const char *>(&entry_size), sizeof(uint32_t));
        index.SerializeToOstream(&index_file);

        plain_file << std::left
                   << std::setw(5) << offset << " | "
                   << std::setw(36) << key << " : "
                   << std::setw(72) << data << "\n";

        offset += object.ByteSizeLong();
    }

    return 0;
}
