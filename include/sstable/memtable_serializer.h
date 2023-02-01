//
// Created by henry on 2023-02-01.
//

#ifndef MVCC_INCLUDE_SSTABLE_MEMTABLE_SERIALIZER_H_
#define MVCC_INCLUDE_SSTABLE_MEMTABLE_SERIALIZER_H_

#include "memtable/memtable.h"
#include "sstable_writer.h"

namespace mvcc {

class memtable_serializer {
  public:
    memtable_serializer(const std::string &name, int blk_size,
                        int generation, memtable &table)
        : table(table), writer(name, blk_size, generation) {}

  public:
    void serialize();

  private:
    memtable &table;
    sstable_writer writer;
};

} // mvcc

#endif //MVCC_INCLUDE_SSTABLE_MEMTABLE_SERIALIZER_H_
