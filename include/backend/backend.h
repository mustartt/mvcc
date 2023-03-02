//
// Created by henry on 2023-02-07.
//

#ifndef MVCC_INCLUDE_BACKEND_BACKEND_H_
#define MVCC_INCLUDE_BACKEND_BACKEND_H_

#include <semaphore>
#include <memory>

#include "wal.h"
#include "memtable/memtable.h"
#include "sstable/sstable_manager.h"

namespace mvcc {

class backend {
  public:
    backend(const boost::filesystem::path &database_directory);
    ~backend();
    backend(const backend &) = delete;
    backend(backend &&) = delete;
    backend &operator=(const backend &) = delete;
    backend &operator=(backend &&) = delete;

  public:
    void write(const std::string &key, int64_t mvcc_timestamp, const std::string &value);
    void del(const std::string &key, int64_t mvcc_timestamp);
    void flush_memtable();
    void checkpoint();

  private:
    int wal_segment_id = 0;
  private:
    std::counting_semaphore<512> client_semaphore{100};

    std::mutex sstable_mutex;
    std::set<std::string> loaded_sstables;

    memtable mtable;
    std::unique_ptr<wal_writer> current_checkpoint = nullptr;
    std::unique_ptr<sstable_manager> sst_manager = nullptr;
};

} // mvcc

#endif //MVCC_INCLUDE_BACKEND_BACKEND_H_
