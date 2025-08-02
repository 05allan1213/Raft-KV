cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(raft-kv LANGUAGES CXX)

# 默认构建类型为 Release
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
endif()

# 编译器设置
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 输出目录
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# 寻找依赖包
find_package(Boost REQUIRED COMPONENTS system thread filesystem)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED)

# 启用测试并寻找 GTest
enable_testing()
find_package(GTest REQUIRED)
include(GoogleTest)

# 添加 Boost 的头文件路径
include_directories(${Boost_INCLUDE_DIRS})

# 编译项目核心库
add_subdirectory(src)  

# 主程序目标
add_executable(raft-kv tests/fiber_main.cpp)

# 设置 raft-kv 的头文件路径
target_include_directories(raft-kv PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_SOURCE_DIR}/include/raft-kv
    ${PROJECT_SOURCE_DIR}/include/raft-kv/fiber
    ${PROJECT_SOURCE_DIR}/include/raft-kv/common
    ${PROJECT_SOURCE_DIR}/include/raft-kv/skipList
    ${PROJECT_SOURCE_DIR}/include/raft-kv/rpc
    ${PROJECT_SOURCE_DIR}/include/raft-kv/raftCore
    ${PROJECT_SOURCE_DIR}/include/raft-kv/raftClerk
    ${Boost_INCLUDE_DIRS}  
)

# 链接依赖库
target_link_libraries(raft-kv 
    PRIVATE 
    raft-kv_lib
    Boost::system
    Boost::thread
    Boost::filesystem
    protobuf::libprotobuf
    Threads::Threads
)

# 如果找到了muduo库，则链接它们
if(MUDUO_NET_LIBRARY AND MUDUO_BASE_LIBRARY)
    target_link_libraries(raft-kv PRIVATE ${MUDUO_NET_LIBRARY} ${MUDUO_BASE_LIBRARY})
endif()
