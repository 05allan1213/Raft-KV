# Raft-KV 分布式键值存储系统

基于Raft共识算法的分布式键值存储系统，采用C++实现，集成了高性能协程库、RPC通信框架和完整的Raft算法。

## 核心模块

### Fiber模块 - Monsoon协程库

用户态协程库，提供高性能异步编程支持：

- **Fiber协程类**：实现协程创建、切换、销毁，使用ucontext_t进行上下文切换
- **Scheduler调度器**：N->M协程调度器，支持多线程调度多协程
- **IOManager**：基于epoll的IO事件管理器，集成定时器功能
- **Hook系统**：拦截系统调用实现透明异步化
- **Channel通信**：协程间通信机制

**特性**：轻量级协程切换、高并发支持、异步IO处理、精确定时器

### RPC模块 - 高性能通信框架

基于protobuf的RPC通信框架：

- **RpcProvider**：基于muduo的RPC服务器，支持多服务注册
- **MprpcChannel**：RPC通信通道，支持同步/异步调用，集成协程库
- **MprpcController**：RPC控制器，提供状态管理和错误处理

**协议格式**：`[header_size][service_name][method_name][args_size][args_data]`

**特性**：异步RPC支持、自动重连、错误处理、协程集成

### Raft模块 - 分布式共识算法

完整的Raft共识算法实现：

- **Raft核心类**：实现领导者选举、日志复制、安全性保证
- **ApplyMsg**：Raft与上层应用的通信接口
- **Persister**：状态持久化存储，支持快照机制
- **KvServer**：基于Raft的分布式键值存储服务

**算法特性**：
- 领导者选举：随机超时、任期管理、日志最新性检查
- 日志复制：并行复制、一致性检查、快速回退优化
- 快照机制：状态压缩、增量传输、快速恢复

**特性**：强一致性、高可用性、故障恢复、成员变更

## 模块集成

- **Fiber + RPC**：异步RPC调用，协程化网络处理
- **RPC + Raft**：节点间通信，客户端服务接口
- **Fiber + Raft**：协程化算法处理，Channel通信

## 技术栈

- C++11/17、muduo网络库、Protocol Buffers
- CMake构建、Google Test测试
- 协程 + 事件驱动并发模型

## 项目结构

```
Raft-KV/
├── include/raft-kv/           # 头文件目录
│   ├── fiber/                 # Fiber协程库头文件
│   │   ├── monsoon.h         # 协程库主头文件
│   │   ├── fiber.h           # 协程核心类
│   │   ├── scheduler.h       # 协程调度器
│   │   ├── iomanager.h       # IO管理器
│   │   └── ...
│   ├── rpc/                   # RPC模块头文件
│   │   ├── rpcprovider.h     # RPC服务提供者
│   │   ├── mprpcchannel.h    # RPC通信通道
│   │   └── mprpccontroller.h # RPC控制器
│   ├── raftCore/              # Raft算法头文件
│   │   ├── raft.h            # Raft核心类
│   │   ├── kvServer.h        # KV存储服务器
│   │   ├── ApplyMsg.h        # 应用消息
│   │   └── Persister.h       # 持久化存储
│   └── common/                # 公共头文件
├── src/                       # 源文件目录
│   ├── fiber/                 # Fiber协程库实现
│   ├── rpc/                   # RPC模块实现
│   ├── raftCore/              # Raft算法实现
│   └── common/                # 公共实现
├── raftRpcPro/                # Protobuf协议定义
│   ├── raftRPC.proto         # Raft RPC协议
│   └── kvServerRPC.proto     # KV服务器RPC协议
├── tests/                     # 集成测试
├── unit_tests/                # 单元测试
├── bin/                       # 可执行文件
└── build/                     # 构建目录
```

## 构建和运行

### 环境要求

- **操作系统**：Linux (推荐Ubuntu 18.04+)
- **编译器**：GCC 7.0+ 或 Clang 6.0+
- **依赖库**：
  - muduo网络库
  - protobuf 3.0+
  - boost库
  - Google Test (用于测试)