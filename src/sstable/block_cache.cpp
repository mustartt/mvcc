//
// Created by henry on 2023-01-31.
//

#include "sstable/block_cache.h"

namespace mvcc {

const block &block_lfu_cache::get(int key) {
    if (list_iter_map.count(key) == 0)
        throw std::runtime_error("block cache: block does not exist");

    auto it = list_iter_map[key];
    lfu_node node = std::move(*it);

    nodes.erase(it);
    freq_map[node.key]++;
    node.frequency = freq_map[node.key];
    auto node_key = node.key;
    nodes.emplace_front(std::move(node));
    list_iter_map[node_key] = nodes.begin();

    return (*nodes.begin()).blk();
}

bool block_lfu_cache::contains(int key) const noexcept {
    return list_iter_map.count(key);
}

void block_lfu_cache::put(int key, block &&value) {
    if (capacity == 0) return;

    if (list_iter_map.count(key) == 0) {
        if (size == capacity) {
            lfu_node node = std::move(nodes.back());
            nodes.pop_back();
            list_iter_map.erase(node.key);
            freq_map.erase(node.key);
            size--;
        }

        lfu_node node(key, 1, std::move(value));
        nodes.emplace_front(std::move(node));
        list_iter_map[key] = nodes.begin();
        freq_map[key] = 1;
        size++;
    } else {
        auto it = list_iter_map[key];
        nodes.erase(it);
        lfu_node node(key, freq_map[key] + 1, std::move(value));
        nodes.emplace_front(std::move(node));
        list_iter_map[key] = nodes.begin();
        freq_map[key]++;
    }
}
}
