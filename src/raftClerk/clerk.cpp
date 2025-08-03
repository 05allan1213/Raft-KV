#include "clerk.h"

#include "raftServerRpcUtil.h"
#include "util.h"
#include <string>
#include <vector>

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

  // 循环重试，直到成功获取值或确认键不存在
  while (true)
  {
    raftKVRpcProctoc::GetReply reply;
    bool ok = m_servers[server]->Get(&args, &reply);

    // 如果RPC调用失败或返回错误领导者，尝试下一个服务器
    if (!ok || reply.err() == ErrWrongLeader)
    {
      // 会一直重试，因为requestId没有改变，因此可能会因为RPC的丢失或者其他情况导致重试，
      // kvserver层来保证不重复执行（线性一致性）
      server = (server + 1) % m_servers.size();
      continue;
    }

    // 如果键不存在，返回空字符串
    if (reply.err() == ErrNoKey)
    {
      return "";
    }

    // 如果操作成功，更新领导者ID并返回值
    if (reply.err() == OK)
    {
      m_recentLeaderId = server;
      return reply.value();
    }
  }
  return "";
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

  // 循环重试，直到操作成功
  while (true)
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

    // 如果RPC调用失败或返回错误领导者，尝试下一个服务器
    if (!ok || reply.err() == ErrWrongLeader)
    {
      DPrintf("【Clerk::PutAppend】原以为的leader：{%d}请求失败，向新leader{%d}重试  ，操作：{%s}", server, server + 1,
              op.c_str());
      if (!ok)
      {
        DPrintf("重试原因 ，rpc失敗 ，");
      }
      if (reply.err() == ErrWrongLeader)
      {
        DPrintf("重試原因：非leader");
      }
      server = (server + 1) % m_servers.size(); // 尝试下一个服务器
      continue;
    }

    // 如果操作成功，更新领导者ID并返回
    if (reply.err() == OK)
    {
      // 什么时候reply errno为ok呢？？？
      m_recentLeaderId = server;
      return;
    }
  }
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
    // 2024-01-04 todo：bug fix
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
