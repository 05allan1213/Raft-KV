#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include "raftServerRpcUtil.h"
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include <vector>
#include "kvServerRPC.pb.h"
#include "raft-kv/rpc/mprpcconfig.h"

/**
 * @brief Raft客户端类
 *
 * 该类实现了Raft-KV系统的客户端功能，提供：
 * - 与Raft集群的连接管理
 * - 键值对的基本操作（Get、Put、Append）
 * - 自动重试和领导者发现机制
 * - 请求去重和一致性保证
 *
 * 客户端通过RPC与Raft集群中的服务器进行通信，
 * 自动处理领导者选举和故障恢复。
 */
class Clerk
{
private:
  std::vector<std::shared_ptr<raftServerRpcUtil>> m_servers; // 保存所有raft节点的RPC连接，全部初始化为-1表示没有连接上
  std::string m_clientId;                                    // 客户端唯一标识符
  int m_requestId;                                           // 请求ID，用于去重和顺序保证
  int m_recentLeaderId;                                      // 最近已知的领导者ID，只是有可能是领导者

  /**
   * @brief 生成随机UUID
   * @return 随机生成的客户端ID字符串
   *
   * 用于返回随机的clientId，确保客户端的唯一性
   */
  std::string Uuid()
  {
    return std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand()) + std::to_string(rand());
  }

  /**
   * @brief 执行Put或Append操作的内部方法
   * @param key 键
   * @param value 值
   * @param op 操作类型（"Put"或"Append"）
   *
   * 统一的Put/Append操作实现，处理重试和领导者发现
   */
  void PutAppend(std::string key, std::string value, std::string op);

public:
  /**
   * @brief 初始化客户端
   * @param configFileName 配置文件路径
   *
   * 从配置文件读取服务器信息，建立与Raft集群的连接
   */
  void Init(std::string configFileName);

  /**
   * @brief 获取键对应的值
   * @param key 要查询的键
   * @return 键对应的值，如果键不存在返回空字符串
   *
   * 通过Raft共识算法获取键值对，确保强一致性
   */
  std::string Get(std::string key);

  /**
   * @brief 设置键值对
   * @param key 键
   * @param value 值
   *
   * 通过Raft共识算法设置键值对，确保强一致性
   */
  void Put(std::string key, std::string value);

  /**
   * @brief 追加值到现有键
   * @param key 键
   * @param value 要追加的值
   *
   * 将新值追加到指定键的现有值后面，如果键不存在则创建新键值对
   */
  void Append(std::string key, std::string value);

public:
  /**
   * @brief 默认构造函数
   *
   * 初始化客户端的基本状态
   */
  Clerk();
};
