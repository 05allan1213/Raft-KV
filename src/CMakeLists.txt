# 收集所有模块的源文件
file(GLOB_RECURSE FIBER_SRC_FILES "fiber/*.cpp")
file(GLOB_RECURSE COMMON_SRC_FILES "common/*.cpp")
file(GLOB_RECURSE SKIPLIST_SRC_FILES "skipList/*.cpp")
file(GLOB_RECURSE RPC_SRC_FILES "rpc/*.cpp")
file(GLOB_RECURSE RAFTCORE_SRC_FILES "raftCore/*.cpp")
file(GLOB_RECURSE RAFTCLERK_SRC_FILES "raftClerk/*.cpp")
file(GLOB_RECURSE RAFTRPCPRO_SRC_FILES "raftRpcPro/*.cpp")

# 合并所有源文件
set(RAFT_KV_SRC_FILES 
    ${FIBER_SRC_FILES}
    ${COMMON_SRC_FILES}
    ${SKIPLIST_SRC_FILES}
    ${RPC_SRC_FILES}
    ${RAFTCORE_SRC_FILES}
    ${RAFTCLERK_SRC_FILES}
    ${RAFTRPCPRO_SRC_FILES}
)

# 创建动态库
add_library(raft-kv_lib SHARED ${RAFT_KV_SRC_FILES})

# 设置动态库的属性
set_target_properties(raft-kv_lib PROPERTIES
    VERSION 1.0.0
    SOVERSION 1
    OUTPUT_NAME raft-kv
)

# 为动态库添加头文件搜索路径
target_include_directories(raft-kv_lib
    PUBLIC
        ${CMAKE_SOURCE_DIR}/include
        ${Boost_INCLUDE_DIRS}  

    PRIVATE
        ${CMAKE_SOURCE_DIR}/include/raft-kv
        ${CMAKE_SOURCE_DIR}/include/raft-kv/fiber
        ${CMAKE_SOURCE_DIR}/include/raft-kv/common
        ${CMAKE_SOURCE_DIR}/include/raft-kv/skipList
        ${CMAKE_SOURCE_DIR}/include/raft-kv/rpc
        ${CMAKE_SOURCE_DIR}/include/raft-kv/raftCore
        ${CMAKE_SOURCE_DIR}/include/raft-kv/raftClerk
)


# 查找并链接所需的第三方库
find_package(Boost REQUIRED COMPONENTS system thread filesystem)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# 链接第三方库到动态库
target_link_libraries(raft-kv_lib 
    PRIVATE 
        Boost::system
        Boost::thread
        Boost::filesystem
        protobuf::libprotobuf
        Threads::Threads
)

# 如果找到了muduo库，则链接它们
if(MUDUO_NET_LIBRARY AND MUDUO_BASE_LIBRARY)
    target_link_libraries(raft-kv_lib PRIVATE ${MUDUO_NET_LIBRARY} ${MUDUO_BASE_LIBRARY})
endif()