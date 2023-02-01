//
// Created by henry on 2023-01-25.
//

#ifndef MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_
#define MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_

#include <map>
#include <set>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include <iterator>
#include <utility>

namespace mvcc {

struct key_value {
    std::string key;
    std::string value;
    int64_t mvcc;
    bool is_tombstone;
};

class memtable {
  public:
    using mvcc_timestamp_t = int64_t;
    using key_type = std::pair<std::string, mvcc_timestamp_t>;
    using value_type = std::string;

    struct key_comparator {
        bool operator()(const key_type &a, const key_type &b) const {
            if (a.first == b.first) {
                return a.second < b.second;
            } else {
                return a.first < b.first;
            }
        }
    };

    using insert_table_t = std::map<key_type, value_type, key_comparator>;
    using delete_table_t = std::set<key_type, key_comparator>;

  public:
    class iterator : public std::iterator<std::forward_iterator_tag, key_value> {
      public:
        iterator(insert_table_t::const_iterator insert_iter,
                 delete_table_t::const_iterator delete_iter,
                 insert_table_t::const_iterator insert_iter_end,
                 delete_table_t::const_iterator delete_iter_end)
            : insert_iter(insert_iter), delete_iter(delete_iter),
              insert_iter_end(insert_iter_end), delete_iter_end(delete_iter_end) {}

        friend class memtable;
      public:
        iterator &operator++();
        key_value operator*();
        bool operator==(const iterator &other) const;
        bool operator!=(const iterator &other) const;

      private:
        insert_table_t::const_iterator insert_iter;
        delete_table_t::const_iterator delete_iter;
        insert_table_t::const_iterator insert_iter_end;
        delete_table_t::const_iterator delete_iter_end;
    };

  public:
    void put(const std::string &key, mvcc_timestamp_t timestamp, std::string value);
    void del(const std::string &key, mvcc_timestamp_t timestamp);

    [[nodiscard]] iterator begin();
    [[nodiscard]] iterator end();
    [[nodiscard]] iterator find(const std::string &key);

    std::shared_lock<std::shared_mutex> read_lock();
    std::unique_lock<std::shared_mutex> write_lock();

  private:
    std::shared_mutex rw_lock;
    insert_table_t insert_table;
    delete_table_t delete_table;
};

} // mvcc

#endif //MVCC_INCLUDE_MEMTABLE_MEMTABLE_H_
