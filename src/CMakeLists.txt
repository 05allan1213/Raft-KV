# 1. Protobuf 代码生成 
protobuf_generate_cpp(RPC_HEADER_PROTO_SRCS RPC_HEADER_PROTO_HDRS "rpc/rpcheader.proto")

# 2. 收集所有模块的源文件
file(GLOB_RECURSE RAFT_KV_SRC_FILES
    "fiber/*.cpp"
    "common/*.cpp"
    "skipList/*.cpp"
    "rpc/*.cpp"
    "raftCore/*.cpp"
    "raftClerk/*.cpp"
    "raftRpcPro/*.cpp"
)

# 3. 创建核心库
add_library(raft-kv_lib ${RAFT_KV_SRC_FILES} ${RPC_HEADER_PROTO_SRCS})

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