cmake_minimum_required(VERSION 3.16)
project(mvcc)

find_package(Boost COMPONENTS filesystem system log timer program_options REQUIRED)
find_package(Protobuf REQUIRED)


set(CMAKE_CXX_STANDARD 20)

set(INCLUDE_DIRECTORY include)

include_directories(${INCLUDE_DIRECTORY})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${Protobuf_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -fsanitize=address -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto/types.proto)

add_library(
        sstable
        src/memtable/memtable.cpp
        include/memtable/memtable.h
        src/wal.cpp include/wal.h
        src/sstable/sstable_writer.cpp
        include/sstable/sstable_writer.h
        src/sstable/block.cpp
        include/sstable/block.h
        src/sstable/block_cache.cpp
        include/sstable/block_cache.h
        src/sstable/block_reader.cpp include/sstable/block_reader.h src/sstable/sstable.cpp include/sstable/sstable.h)

add_executable(mvcc main.cpp ${PROTO_SRCS} ${PROTO_HDRS})

target_compile_definitions(mvcc PUBLIC BOOST_ASIO_HAS_IO_URING BOOST_ASIO_DISABLE_EPOLL)
target_link_libraries(mvcc sstable ${Boost_LIBRARIES} uring ${Protobuf_LIBRARIES})
