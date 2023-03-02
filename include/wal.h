//
// Created by henry on 2023-01-25.
//

#ifndef MVCC_INCLUDE_WAL_H_
#define MVCC_INCLUDE_WAL_H_

#include <string>
#include <memory>
#include <mutex>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace mvcc {

class wal_writer {
  public:
    using file_handle_t = int;
  public:
    explicit wal_writer(const std::string &filename, uint64_t lsn = 0);
    ~wal_writer() noexcept;
  public:
    std::unique_lock<std::mutex> write_put_log(const std::string &key, const std::string &value);
    std::unique_lock<std::mutex> write_del_log(const std::string &key);
    void write_flush();
    void flush() const;

    [[nodiscard]] uint64_t get_lsn() const { return lsn; }

  private:
    uint64_t get_and_inc_lsn();

  private:
    std::string wal_filename;
    file_handle_t fd;
    uint64_t lsn;

    std::mutex mutex;
    std::vector<char> write_buffer;
};

} // mvcc

#endif //MVCC_INCLUDE_WAL_H_
