# 1. 项目基础设置
cmake_minimum_required(VERSION 3.16)
project(raft-kv LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 2. 设置输出目录
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# 3. 寻找所有第三方依赖 (集中管理)
find_package(Boost REQUIRED COMPONENTS system thread filesystem serialization)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)
find_package(GTest REQUIRED)

find_library(MUDUO_NET_LIBRARY muduo_net)
find_library(MUDUO_BASE_LIBRARY muduo_base)
find_path(MUDUO_INCLUDE_DIR muduo/net/EventLoop.h)
if(NOT MUDUO_NET_LIBRARY OR NOT MUDUO_BASE_LIBRARY OR NOT MUDUO_INCLUDE_DIR)
    message(FATAL_ERROR "Muduo library not found! Please check installation.")
endif()
include_directories(${MUDUO_INCLUDE_DIR})

# 4. Protobuf 代码生成 (针对测试)
protobuf_generate_cpp(FRIEND_PROTO_SRCS FRIEND_PROTO_HDRS "tests/rpc/friend.proto")

# 创建目录结构以匹配include路径
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/raft-kv/rpc)

# 复制生成的friend.pb.h到正确的位置
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/raft-kv/rpc/friend.pb.h
    COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_BINARY_DIR}/friend.pb.h
            ${CMAKE_CURRENT_BINARY_DIR}/raft-kv/rpc/friend.pb.h
    DEPENDS ${FRIEND_PROTO_HDRS}
    COMMENT "Copying friend.pb.h to raft-kv/rpc/ directory"
)

# 5. 包含子目录
add_subdirectory(src)

# 6. 定义所有测试用的可执行文件
add_executable(fiber_test tests/fiber_test.cpp)
add_executable(server tests/rpc/server.cpp ${FRIEND_PROTO_SRCS})
add_executable(client tests/rpc/client.cpp ${FRIEND_PROTO_SRCS})
add_executable(raft_test tests/raft_test.cpp)
add_executable(raft_client tests/raft_client.cpp)

# 确保friend.pb.h被复制到正确位置
add_custom_target(copy_friend_pb_h DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/raft-kv/rpc/friend.pb.h)
add_dependencies(server copy_friend_pb_h)
add_dependencies(client copy_friend_pb_h)

# 7. 设置测试目标的包含路径
target_include_directories(server PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(client PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(raft_test PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}      # 用于找到 .pb.h 文件
    ${CMAKE_SOURCE_DIR}/include     # 用于找到项目的所有头文件
)


# 8. 链接所有目标
target_link_libraries(fiber_test PRIVATE raft-kv_lib)
target_link_libraries(server PRIVATE raft-kv_lib)
target_link_libraries(client PRIVATE raft-kv_lib)
target_link_libraries(raft_test PRIVATE raft-kv_lib)
target_link_libraries(raft_client PRIVATE raft-kv_lib)