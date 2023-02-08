//
// Created by henry on 2023-02-07.
//

#ifndef MVCC_INCLUDE_BACKEND_BACKEND_H_
#define MVCC_INCLUDE_BACKEND_BACKEND_H_

#include <semaphore>

#include "memtable/memtable.h"

namespace mvcc {

class backend {

  private:
    std::counting_semaphore<512> client_semaphore{100};

    memtable mtable;
    
};

} // mvcc

#endif //MVCC_INCLUDE_BACKEND_BACKEND_H_
