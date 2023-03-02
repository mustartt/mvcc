#include <iostream>
#include <boost/timer/timer.hpp>

#include "backend/backend.h"
#include "memtable/memtable.h"

using namespace mvcc;

void print_kv(const key_value &kv) {
    std::cout << "(" << kv.key
              << " ver: " << kv.mvcc << " "
              << (kv.is_tombstone ? "<null>" : kv.value)
              << ")" << std::endl;
}

int main() {
    backend db("");

    for (int i = 0; i < 1000; ++i) {
        db.write(std::to_string(i), 1, std::to_string(i));
    }

    return 0;
}
