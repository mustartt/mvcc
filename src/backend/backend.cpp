//
// Created by henry on 2023-02-07.
//

#include "backend/backend.h"

namespace mvcc {

backend::backend(const boost::filesystem::path &database_directory)
    : wal_segment_id(0), database_directory(database_directory) {
    auto wal_path = database_directory;
    wal_path.append(std::to_string(wal_segment_id) + ".wal");
    current_checkpoint = std::make_unique<wal_writer>(wal_path.string());

    auto sst_path = database_directory;
    sst_path.append("sstables");
    sst_manager = std::make_unique<sstable_manager>(sst_path.string());

    sst_manager->discover_existing_sstables();
    loaded_sstables = sst_manager->get_loaded();
}

backend::~backend() {
    flush_memtable(false);
}

void backend::write(const std::string &key, const std::string &value) {
    auto lock = current_checkpoint->write_put_log(key, value);
    current_checkpoint->flush();
    mtable.put(key, value);
}

void backend::del(const std::string &key) {
    auto lock = current_checkpoint->write_del_log(key);
    current_checkpoint->flush();
    mtable.del(key);
}

void backend::flush_memtable(bool create_next) {
    std::unique_lock<std::mutex> sst_lock(sstable_mutex);
    auto mtable_lock = mtable.write_lock();
    std::string sst_record_name;
    {
        auto [writer, record] = sst_manager->create_sstable();
        for (const auto kv: mtable) {
            writer->write_entry(kv.key, kv.value, kv.is_tombstone);
        }
        sst_record_name = std::move(record.name);
    }
    loaded_sstables.insert(sst_record_name);
    if (create_next) checkpoint();

    mtable.clear();
}

void backend::checkpoint() {
    ++wal_segment_id;
    auto lsn = current_checkpoint->get_lsn();
    auto wal_path = database_directory;
    wal_path.append(std::to_string(wal_segment_id) + ".wal");
    current_checkpoint = std::make_unique<wal_writer>(wal_path.string(), lsn);
}

} // mvcc
