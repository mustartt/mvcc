//
// Created by henry on 2023-01-25.
//

#ifndef MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_
#define MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_

#include <shared_mutex>
#include <map>
#include <set>
#include <optional>

namespace mvcc {

class memtable {
  public:
    using mvcc_timestamp_t = int64_t;
    using key_type = std::string;
    using value_type = std::string;

  public:
    std::optional<std::string> get(const std::string &key, mvcc_timestamp_t timestamp);
    void put(const std::string &key, mvcc_timestamp_t timestamp, std::string value);
    void del(const std::string &key, mvcc_timestamp_t timestamp);

//    void range(const std::string &start, const std::string &end, mvcc_timestamp_t timestamp);
  public:

  private:
    std::shared_mutex rw_lock;
    std::map<key_type, std::map<mvcc_timestamp_t, value_type, std::greater<>>> insert_table;
    std::map<key_type, std::set<mvcc_timestamp_t, std::greater<>>> delete_table;
};

} // mvcc

#endif //MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_
