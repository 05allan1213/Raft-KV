# 1. Protobuf 代码生成
# 查找 src/rpc 下的
file(GLOB RPC_PROTO_FILES "${CMAKE_CURRENT_SOURCE_DIR}/rpc/*.proto")
# 查找项目根目录下的 raftRpcPro 目录下的
file(GLOB RAFT_CORE_PROTO_FILES "${CMAKE_SOURCE_DIR}/raftRpcPro/*.proto")

# 将它们全部交给 protobuf 处理
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${RPC_PROTO_FILES} ${RAFT_CORE_PROTO_FILES})


# 2. 收集所有模块的源文件
file(GLOB_RECURSE RAFT_KV_SRC_FILES
    "fiber/*.cpp"
    "common/*.cpp"
    "skipList/*.cpp"
    "rpc/*.cpp"
    "raftCore/*.cpp"
    "raftClerk/*.cpp"
)

# 3. 创建核心库 (添加生成的 protobuf 源文件)
add_library(raft-kv_lib ${RAFT_KV_SRC_FILES} ${PROTO_SRCS})

# 4. 为核心库设置头文件路径和依赖
target_include_directories(raft-kv_lib
    PUBLIC
        "${CMAKE_SOURCE_DIR}/include"
        "${CMAKE_CURRENT_BINARY_DIR}"  
    PRIVATE
        ${CMAKE_SOURCE_DIR}/include/raft-kv
        ${CMAKE_SOURCE_DIR}/include/raft-kv/fiber
        ${CMAKE_SOURCE_DIR}/include/raft-kv/common
        ${CMAKE_SOURCE_DIR}/include/raft-kv/skipList
        ${CMAKE_SOURCE_DIR}/include/raft-kv/rpc
        ${CMAKE_SOURCE_DIR}/include/raft-kv/raftCore
        ${CMAKE_SOURCE_DIR}/include/raft-kv/raftClerk
)

target_link_libraries(raft-kv_lib
    PUBLIC
        Boost::system
        Boost::thread
        Boost::filesystem
        Boost::serialization
        ${PROTOBUF_LIBRARIES}
        Threads::Threads
        ${MUDUO_NET_LIBRARY}
        ${MUDUO_BASE_LIBRARY}
)