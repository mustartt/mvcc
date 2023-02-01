//
// Created by henry on 2023-01-31.
//

#ifndef MVCC_INCLUDE_SSTABLE_BLOCK_CACHE_H_
#define MVCC_INCLUDE_SSTABLE_BLOCK_CACHE_H_

#include <unordered_map>
#include <vector>
#include <queue>
#include <list>

#include "block.h"

namespace mvcc {

class block_lfu_cache;
struct lfu_node_compare;

class lfu_node {
  public:
    lfu_node(int k, int f, block &&blk)
        : key(k), value(std::move(blk)), frequency(f) {}

    friend class block_lfu_cache;
    friend lfu_node_compare;
  public:
    const block &blk() const { return value; }
  private:
    int key;
    block value;
    int frequency;
};

struct lfu_node_compare {
    bool operator()(lfu_node *a, lfu_node *b) {
        return a->frequency > b->frequency;
    }
};

class block_lfu_cache {
  public:
    explicit block_lfu_cache(int c) : capacity(c), size(0) {}
    block_lfu_cache(const block_lfu_cache &) = delete;
    block_lfu_cache(block_lfu_cache &&) = default;
    block_lfu_cache &operator=(const block_lfu_cache &) = delete;
    block_lfu_cache &operator=(block_lfu_cache &&) = default;

  public:
    const block &get(int key);
    bool contains(int key) const noexcept;
    void put(int key, block &&value);

  private:
    int capacity;
    int size;
    std::unordered_map<int, std::list<lfu_node>::iterator> list_iter_map;
    std::unordered_map<int, int> freq_map;
    std::list<lfu_node> nodes;
    std::priority_queue<lfu_node *, std::vector<lfu_node *>, lfu_node_compare> pq;
};

}

#endif //MVCC_INCLUDE_SSTABLE_BLOCK_CACHE_H_
