//
// Created by henry on 2023-01-25.
//

#include "wal.h"

#include <fcntl.h>
#include <unistd.h>
#include <cerrno>
#include <iostream>
#include <cstring>
#include <chrono>

#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <boost/crc.hpp>

#include "types.pb.h"

namespace mvcc {

wal_writer::wal_writer(const std::string &filename) : wal_filename(filename) {
    fd = open(filename.c_str(), O_CREAT | O_TRUNC | O_WRONLY, S_IRWXU);
    if (fd < 0) {
        std::cerr << "Error: " << errno << " - " << std::strerror(errno) << std::endl;
        throw std::runtime_error("Bad File Descriptor");
    }
}

wal_writer::~wal_writer() noexcept {
    flush();
    if (close(fd) != 0) {
        std::cerr << "Error: " << errno << " - " << std::strerror(errno) << std::endl;
    }
}

std::unique_lock<std::mutex> wal_writer::write_put_log(const std::string &key,
                                                       int64_t mvcc_timestamp,
                                                       const std::string &value) {
    std::unique_lock lock(mutex);
    {
        google::protobuf::io::FileOutputStream file_stream(fd);
        google::protobuf::io::CodedOutputStream output_stream(&file_stream);

        mvcc::WalEntry log;
        log.set_timestamp(current_timestamp());
        log.set_type(mvcc::WalEntry_EntryType_Write);
        log.set_key(key);
        log.set_mvcc_timestamp(mvcc_timestamp);
        log.set_value(value);

        size_t size = log.ByteSizeLong();
        write_buffer.resize(size);
        log.SerializeToArray(write_buffer.data(), static_cast<int>(size));

        boost::crc_32_type result;
        result.process_bytes(write_buffer.data(), size);

        output_stream.WriteVarint32(static_cast<int>(size));
        output_stream.WriteLittleEndian32(result.checksum());
        output_stream.WriteRaw(write_buffer.data(), static_cast<int>(size));
    }
    return lock;
}

std::unique_lock<std::mutex> wal_writer::write_del_log(const std::string &key, int64_t mvcc_timestamp) {
    std::unique_lock lock(mutex);
    {
        google::protobuf::io::FileOutputStream file_stream(fd);
        google::protobuf::io::CodedOutputStream output_stream(&file_stream);

        mvcc::WalEntry log;
        log.set_timestamp(current_timestamp());
        log.set_type(mvcc::WalEntry_EntryType_Delete);
        log.set_key(key);
        log.set_mvcc_timestamp(mvcc_timestamp);
        log.set_value("");

        size_t size = log.ByteSizeLong();
        write_buffer.resize(size);
        log.SerializeToArray(write_buffer.data(), static_cast<int>(size));

        boost::crc_32_type result;
        result.process_bytes(write_buffer.data(), size);

        output_stream.WriteVarint32(static_cast<int>(size));
        output_stream.WriteLittleEndian32(result.checksum());
        output_stream.WriteRaw(write_buffer.data(), static_cast<int>(size));
    }
    return lock;
}

void wal_writer::write_flush() {
    google::protobuf::io::FileOutputStream file_stream(fd);
    google::protobuf::io::CodedOutputStream output_stream(&file_stream);

    mvcc::WalEntry log;
    log.set_timestamp(current_timestamp());
    log.set_type(mvcc::WalEntry_EntryType_Flush);
    log.set_mvcc_timestamp(0);

    size_t size = log.ByteSizeLong();
    write_buffer.resize(size);
    log.SerializeToArray(write_buffer.data(), static_cast<int>(size));

    boost::crc_32_type result;
    result.process_bytes(write_buffer.data(), size);

    output_stream.WriteVarint32(static_cast<int>(size));
    output_stream.WriteLittleEndian32(result.checksum());
    output_stream.WriteRaw(write_buffer.data(), static_cast<int>(size));
}

uint64_t wal_writer::current_timestamp() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void wal_writer::flush() const {
    if (fdatasync(fd) != 0) throw std::runtime_error("Failed to Flush");
}

} // mvcc
