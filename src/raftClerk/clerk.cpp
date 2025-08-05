#include "clerk.h"

#include "raftServerRpcUtil.h"
#include "util.h"
#include <string>
#include <vector>
#include <chrono>
#include <thread>
#include <stdexcept>

/**
 * @brief 获取键对应的值
 *
 * 通过Raft共识算法获取键值对，自动处理领导者选举和故障恢复。
 * 如果键不存在，返回空字符串。
 *
 * @param key 要查询的键
 * @return 键对应的值，如果键不存在返回空字符串
 */
std::string Clerk::Get(std::string key)
{
  // 递增请求ID，确保每个请求的唯一性
  m_requestId++;
  auto requestId = m_requestId;
  int server = m_recentLeaderId;

  // 构造Get请求参数
  raftKVRpcProctoc::GetArgs args;
  args.set_key(key);
  args.set_clientid(m_clientId);
  args.set_requestid(requestId);

  // 改进的重试逻辑，使用指数退避策略，最多重试10次
  int maxRetries = 10;
  int baseDelay = 100;         // 基础延迟100ms
  int consecutiveFailures = 0; // 连续失败次数

  for (int retry = 0; retry < maxRetries; retry++)
  {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);

    DPrintf("[客户端] Get操作尝试 %d/%d，服务器: %d，RPC结果: %s",
            retry + 1, maxRetries, server, ok ? "成功" : "失败");

    if (ok)
    {
      consecutiveFailures = 0; // 重置连续失败计数

      if (reply.err() == OK)
      {
        DPrintf("[客户端] Get操作成功，键: %s，值: %s", key.c_str(), reply.value().c_str());
        m_recentLeaderId = server;
        return reply.value();
      }
      else if (reply.err() == ErrNoKey)
      {
        DPrintf("[客户端] 键不存在: %s", key.c_str());
        m_recentLeaderId = server;
        return ""; // 键不存在，返回空字符串
      }
      else if (reply.err() == ErrWrongLeader)
      {
        DPrintf("[客户端] 服务器%d不是领导者，尝试下一个服务器", server);
        server = (server + 1) % m_servers.size();
        // 对于ErrWrongLeader，使用较短的延迟
        if (retry < maxRetries - 1)
        {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
      }
      else
      {
        DPrintf("[客户端] 服务器返回错误: %s", reply.err().c_str());
        server = (server + 1) % m_servers.size();
        // 对于未知错误，使用中等延迟
        if (retry < maxRetries - 1)
        {
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
      }
    }
    else
    {
      // RPC调用失败，可能是网络问题或服务器未就绪
      consecutiveFailures++;
      DPrintf("[客户端] RPC调用失败 (连续失败 %d 次)，尝试下一个服务器", consecutiveFailures);
      server = (server + 1) % m_servers.size();

      // 对于RPC失败，使用指数退避延迟
      if (retry < maxRetries - 1)
      {
        int delay = std::min(baseDelay * (1 << std::min(consecutiveFailures - 1, 4)), 2000);
        DPrintf("[客户端] 等待 %dms 后重试...", delay);
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
      }
    }
  }

  // 所有重试都失败了
  throw std::runtime_error("Get操作失败：无法连接到Raft集群");
}

/**
 * @brief 执行Put或Append操作的内部方法
 *
 * 统一的Put/Append操作实现，处理重试和领导者发现。
 * 通过Raft共识算法确保操作的强一致性。
 *
 * @param key 键
 * @param value 值
 * @param op 操作类型（"Put"或"Append"）
 */
void Clerk::PutAppend(std::string key, std::string value, std::string op)
{
  // 递增请求ID，确保每个请求的唯一性
  m_requestId++;
  auto requestId = m_requestId;
  auto server = m_recentLeaderId;

  // 改进的重试逻辑，使用指数退避策略，最多重试10次
  int maxRetries = 10;
  int baseDelay = 100;         // 基础延迟100ms
  int consecutiveFailures = 0; // 连续失败次数

  for (int retry = 0; retry < maxRetries; retry++)
  {
    // 构造Put/Append请求参数
    raftKVRpcProctoc::PutAppendArgs args;
    args.set_key(key);
    args.set_value(value);
    args.set_op(op);
    args.set_clientid(m_clientId);
    args.set_requestid(requestId);

    // 发送RPC请求
    raftKVRpcProctoc::PutAppendReply reply;
    bool ok = m_servers[server]->PutAppend(&args, &reply);

    DPrintf("[客户端] %s操作尝试 %d/%d，服务器: %d，RPC结果: %s",
            op.c_str(), retry + 1, maxRetries, server, ok ? "成功" : "失败");

    if (ok)
    {
      consecutiveFailures = 0; // 重置连续失败计数

      if (reply.err() == OK)
      {
        DPrintf("[客户端] %s操作成功完成，键: %s", op.c_str(), key.c_str());
        m_recentLeaderId = server;
        return;
      }
      else if (reply.err() == ErrWrongLeader)
      {
        DPrintf("[客户端] 服务器%d不是领导者，尝试下一个服务器", server);
        server = (server + 1) % m_servers.size();
        // 对于ErrWrongLeader，使用较短的延迟
        if (retry < maxRetries - 1)
        {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
      }
      else
      {
        DPrintf("[客户端] 服务器返回错误: %s", reply.err().c_str());
        server = (server + 1) % m_servers.size();
        // 对于未知错误，使用中等延迟
        if (retry < maxRetries - 1)
        {
          std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
      }
    }
    else
    {
      // RPC调用失败，可能是网络问题或服务器未就绪
      consecutiveFailures++;
      DPrintf("[客户端] RPC调用失败 (连续失败 %d 次)，尝试下一个服务器", consecutiveFailures);
      server = (server + 1) % m_servers.size();

      // 对于RPC失败，使用指数退避延迟
      if (retry < maxRetries - 1)
      {
        int delay = std::min(baseDelay * (1 << std::min(consecutiveFailures - 1, 4)), 2000);
        DPrintf("[客户端] 等待 %dms 后重试...", delay);
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
      }
    }
  }

  // 所有重试都失败了
  throw std::runtime_error(op + "操作失败：无法连接到Raft集群");
}

/**
 * @brief 设置键值对
 * @param key 键
 * @param value 值
 */
void Clerk::Put(std::string key, std::string value) { PutAppend(key, value, "Put"); }

/**
 * @brief 追加值到现有键
 * @param key 键
 * @param value 要追加的值
 */
void Clerk::Append(std::string key, std::string value) { PutAppend(key, value, "Append"); }

/**
 * @brief 初始化客户端
 *
 * 从配置文件读取服务器信息，建立与Raft集群的连接。
 * 解析配置文件中的节点信息，为每个节点创建RPC连接。
 *
 * @param configFileName 配置文件路径
 */
void Clerk::Init(std::string configFileName)
{
  // 获取所有raft节点ip、port，并进行连接
  MprpcConfig config;
  config.LoadConfigFile(configFileName.c_str());
  std::vector<std::pair<std::string, short>> ipPortVt;

  // 解析配置文件中的节点信息
  for (int i = 0; i < INT_MAX - 1; ++i)
  {
    std::string node = "node" + std::to_string(i);

    std::string nodeIp = config.Load(node + "ip");
    std::string nodePortStr = config.Load(node + "port");
    if (nodeIp.empty())
    {
      break;
    }
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str())); // 没有atos方法，可以考虑自己实现
  }

  // 为每个节点创建RPC连接
  for (const auto &item : ipPortVt)
  {
    std::string ip = item.first;
    short port = item.second;
    auto *rpc = new raftServerRpcUtil(ip, port);
    m_servers.push_back(std::shared_ptr<raftServerRpcUtil>(rpc));
  }
}

/**
 * @brief 默认构造函数
 *
 * 初始化客户端的基本状态，包括生成客户端ID、初始化请求ID和领导者ID
 */
Clerk::Clerk() : m_clientId(Uuid()), m_requestId(0), m_recentLeaderId(0) {}
