#include <iostream>
#include <boost/timer/timer.hpp>

#include "backend/backend.h"
#include "memtable/memtable.h"

using namespace mvcc;

void print_kv(const key_value &kv) {
    std::cout << "(" << kv.key << " "
              << (kv.is_tombstone ? "<null>" : kv.value)
              << ")" << std::endl;
}

int main() {
    backend db("");

    db.write("init", "value");

    boost::timer::auto_cpu_timer timer;

    for (int i = 0; i < 10000; ++i) {
        db.write(std::to_string(i), std::to_string(i));
    }

    return 0;
}
