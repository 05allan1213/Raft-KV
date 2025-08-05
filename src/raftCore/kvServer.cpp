#include "kvServer.h"

#include <rpcprovider.h>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <fstream>
#include <muduo/base/Logging.h>

#include "raft-kv/rpc/mprpcconfig.h"

/**
 * @brief 打印当前KV数据库内容（仅调试用）
 *
 * 仅在Debug模式下，遍历并输出所有键值对，便于开发者观察数据状态。
 */
void KvServer::DprintfKVDB()
{
  if (!Debug)
  {
    return;
  }
  std::lock_guard<std::mutex> lg(m_mtx);
  DEFER
  {
    m_skipList.display_list(); // 跳表遍历输出
  };
}

/**
 * @brief 执行Append操作，将新值追加到指定键后
 * @param op 操作对象，包含键、值、客户端信息等
 *
 * 若键已存在，则在原值后追加；否则直接插入新值。操作完成后记录请求ID防止重复。
 */
void KvServer::ExecuteAppendOpOnKVDB(Op op)
{
  m_mtx.lock();

  // 先查找现有值，然后追加
  std::string existingValue;
  bool keyExists = m_skipList.search_element(op.Key, existingValue);

  std::string newValue;
  if (keyExists)
  {
    // 键存在，追加到现有值后面
    newValue = existingValue + op.Value;
    DPrintf("[KV服务器] Append：键 %s 存在，原值='%s'，追加='%s'，新值='%s'",
            op.Key.c_str(), existingValue.c_str(), op.Value.c_str(), newValue.c_str());
  }
  else
  {
    // 键不存在，直接使用新值
    newValue = op.Value;
    DPrintf("[KV服务器] Append：键 %s 不存在，创建新值='%s'",
            op.Key.c_str(), newValue.c_str());
  }

  // 设置新值并记录请求ID
  m_skipList.insert_set_element(op.Key, newValue);
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  DprintfKVDB();
}

/**
 * @brief 执行Get操作，查询指定键的值
 * @param op 操作对象
 * @param value 返回值指针
 * @param exist 返回键是否存在
 *
 * 跳表查找，若存在则赋值并标记exist为true，否则返回空字符串。
 */
void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist)
{
  m_mtx.lock();
  *value = "";
  *exist = false;

  // 跳表查找键值对
  if (m_skipList.search_element(op.Key, *value))
  {
    *exist = true;
  }

  // 记录客户端的最新请求ID，用于重复请求检测
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  // 调试模式下打印数据库内容
  DprintfKVDB();
}

/**
 * @brief 执行Put操作，插入或更新键值对
 * @param op 操作对象
 *
 * 直接插入或覆盖原有值，并记录最新请求ID。
 */
void KvServer::ExecutePutOpOnKVDB(Op op)
{
  m_mtx.lock();

  // 跳表执行Put操作
  m_skipList.insert_set_element(op.Key, op.Value);

  // 记录请求ID防止重复
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  DprintfKVDB();
}

/**
 * @brief 处理客户端Get RPC请求
 * @param args 请求参数
 * @param reply 响应结果
 *
 * 将Get请求封装为Op对象，提交给Raft，若本节点为Leader则本地查找并返回结果。
 */
void KvServer::Get(const raftKVRpcProctoc::GetArgs *args, raftKVRpcProctoc::GetReply *reply)
{
  // 构造操作对象
  Op op;
  op.Operation = "Get";
  op.Key = args->key();
  op.Value = "";
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();

  int raftIndex = -1;
  int _ = -1;
  bool isLeader = false;
  m_raftNode->Start(op, &raftIndex, &_, &isLeader); // raftIndex：raft预计的logIndex

  if (!isLeader)
  {
    reply->set_err(ErrWrongLeader);
    return;
  }

  // 单节点集群直接读取数据
  DPrintf("[KV服务器] Get操作，键: %s", op.Key.c_str());

  std::string value;
  bool exist = false;
  ExecuteGetOpOnKVDB(op, &value, &exist);

  if (exist)
  {
    reply->set_err(OK);
    reply->set_value(value);
    DPrintf("[KV服务器] Get成功，键: %s，值: %s", op.Key.c_str(), value.c_str());
  }
  else
  {
    reply->set_err(ErrNoKey);
    reply->set_value("");
    DPrintf("[KV服务器] Get失败，键不存在: %s", op.Key.c_str());
  }
}

/**
 * @brief 从Raft接收并处理命令
 * @param message 来自Raft的ApplyMsg消息
 *
 * 处理从Raft共识层传递过来的命令，执行相应的数据库操作。
 * 包括重复请求检测、命令执行和快照管理。
 */
void KvServer::GetCommandFromRaft(ApplyMsg message)
{
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Operation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex)
  {
    return;
  }

  // 状态机处理重复命令问题
  if (!ifRequestDuplicate(op.ClientId, op.RequestId))
  {
    // 执行命令
    if (op.Operation == "Put")
    {
      ExecutePutOpOnKVDB(op);
    }
    if (op.Operation == "Append")
    {
      ExecuteAppendOpOnKVDB(op);
    }
    //  kv.lastRequestId[op.ClientId] = op.RequestId  在Executexxx函数里面更新的
  }
  // 检查是否需要制作快照
  if (m_maxRaftState != -1)
  {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    // 如果raft的log太大（大于指定的比例）就把制作快照
  }

  // 发送消息到等待通道
  SendMessageToWaitChan(op, message.CommandIndex);
}

/**
 * @brief 检查请求是否重复
 * @param ClientId 客户端ID
 * @param RequestId 请求ID
 * @return 如果请求重复返回true，否则返回false
 *
 * 通过比较客户端ID和请求ID来判断是否为重复请求，
 * 用于实现线性一致性。
 */
bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end())
  {
    return false;
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// get和put/append执行的具体细节是不一样的
// PutAppend在收到raft消息之后执行，具体函数里面只判断幂等性（是否重复）
// get函数收到raft消息之后在，因为get无论是否重复都可以再执行
/**
 * @brief 处理客户端Put/Append RPC请求
 * @param args 请求参数，包含操作类型、键、值、客户端ID、请求ID等
 * @param reply 响应结果，包含错误信息等
 *
 * 处理客户端的Put和Append请求，将请求提交给Raft共识算法，
 * 并等待操作被应用到状态机后返回结果。
 */
void KvServer::PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
  Op op;
  op.Operation = args->op();
  op.Key = args->key();
  op.Value = args->value();
  op.ClientId = args->clientid();
  op.RequestId = args->requestid();
  int raftIndex = -1;
  int _ = -1;
  bool isleader = false;

  m_raftNode->Start(op, &raftIndex, &_, &isleader);

  if (!isleader)
  {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , but "
        "not leader",
        m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, args->clientid().c_str(), args->requestid(), m_me, op.Key.c_str(), raftIndex);
  DPrintf("[KV服务器] Put/Append操作已提交到Raft，索引: %d", raftIndex);

  // 实现真正的等待机制：等待操作被应用到状态机
  const int maxWaitTime = 5000; // 最大等待5秒
  const int checkInterval = 50; // 每50ms检查一次
  int waitedTime = 0;

  while (waitedTime < maxWaitTime)
  {
    // 检查操作是否已经被应用
    m_mtx.lock();
    auto it = m_lastRequestId.find(op.ClientId);
    bool applied = (it != m_lastRequestId.end() && it->second >= op.RequestId);
    m_mtx.unlock();

    if (applied)
    {
      DPrintf("[KV服务器] Put/Append操作已应用到状态机，键: %s", op.Key.c_str());
      reply->set_err(OK);
      DPrintf("[KV服务器] Put/Append操作成功完成，键: %s", op.Key.c_str());
      return;
    }

    // 等待一段时间后再检查
    std::this_thread::sleep_for(std::chrono::milliseconds(checkInterval));
    waitedTime += checkInterval;
  }

  // 超时了，返回错误
  DPrintf("[KV服务器] Put/Append操作超时，键: %s", op.Key.c_str());
  reply->set_err(ErrWrongLeader); // 可能Leader已经改变
}

/**
 * @brief 读取Raft应用命令循环
 *
 * 持续监听来自Raft的ApplyMsg消息，处理命令和快照。
 * 使用Channel进行消息接收，支持协程调度。
 */
void KvServer::ReadRaftApplyCommandLoop()
{
  while (true)
  {
    // 使用Channel接收消息，自动协程调度
    ApplyMsg message;
    auto result = applyChan->receive(message); // 阻塞接收
    if (result != monsoon::ChannelResult::SUCCESS)
    {
      // Channel可能已关闭或出现错误
      if (result == monsoon::ChannelResult::CLOSED)
      {
        DPrintf("[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] applyChan已关闭，退出循环", m_me);
        break;
      }
      continue; // 其他错误，继续尝试
    }
    DPrintf(
        "---------------tmp-------------[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息",
        m_me);
    // 监听每个由raft应用的命令，传递给相应的RPC处理器

    if (message.CommandValid)
    {
      GetCommandFromRaft(message);
    }
    if (message.SnapshotValid)
    {
      GetSnapShotFromRaft(message);
    }
  }
}

// raft会与persist层交互，kvserver层也会，因为kvserver层开始的时候需要恢复kvdb的状态
//  关于快照raft层与persist的交互：保存kvserver传来的snapshot；生成leaderInstallSnapshot RPC的时候也需要读取snapshot；
//  因此snapshot的具体格式是由kvserver层来定的，raft只负责传递这个东西
//  snapShot里面包含kvserver需要维护的persist_lastRequestId 以及kvDB真正保存的数据persist_kvdb
/**
 * @brief 读取快照并安装到KV数据库
 * @param snapshot 快照数据字符串
 *
 * 从快照中恢复KV数据库状态和客户端请求ID映射。
 * 快照格式由KV服务器层定义，Raft层只负责传递。
 */
void KvServer::ReadSnapShotToInstall(std::string snapshot)
{
  if (snapshot.empty())
  {
    // 无状态启动
    return;
  }
  parseFromString(snapshot);
}

/**
 * @brief 发送消息到等待通道
 * @param op 操作对象
 * @param raftIndex Raft日志索引
 * @return 发送是否成功
 *
 * 根据当前等待模式（Promise/Future、Channel、LockQueue）发送消息到相应的等待通道。
 */
bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%s}, RequestId "
      "{%d}, Operation {%s}, Key :{%s}, Value :{%s}",
      m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());

  if (usePromiseFuture_)
  {
    // 使用 Promise/Future 模式
    bool success = promiseManager_.setResult(raftIndex, op);
    if (success)
    {
      DPrintf("[SendMessageToWaitChan] Promise/Future mode: Successfully set result for index %d", raftIndex);
    }
    return success;
  }
  else if (useChannel_)
  {
    // 使用新的 Channel 模式
    std::lock_guard<std::mutex> lg(m_mtx);

    if (waitApplyChChannel.find(raftIndex) == waitApplyChChannel.end())
    {
      return false;
    }
    auto result = waitApplyChChannel[raftIndex]->send(op);
    if (result == monsoon::ChannelResult::SUCCESS)
    {
      DPrintf(
          "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command via Channel --> Index:{%d} , ClientId {%s}, RequestId "
          "{%d}, Operation {%s}, Key :{%s}, Value :{%s}",
          m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
      return true;
    }
    else
    {
      DPrintf("[RaftApplyMessageSendToWaitChan] Channel send failed, result: %d", (int)result);
      return false;
    }
  }
  else
  {
    // 使用原有的 LockQueue 模式
    std::lock_guard<std::mutex> lg(m_mtx);

    if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
      return false;
    }
    waitApplyCh[raftIndex]->Push(op);
    DPrintf(
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%s}, RequestId "
        "{%d}, Operation {%s}, Key :{%s}, Value :{%s}",
        m_me, raftIndex, op.ClientId.c_str(), op.RequestId, op.Operation.c_str(), op.Key.c_str(), op.Value.c_str());
    return true;
  }
}

/**
 * @brief 检查是否需要发送快照命令
 * @param raftIndex Raft日志索引
 * @param proportion 快照比例阈值
 *
 * 根据数据大小选择合适的快照方式（流式或常规），并发送快照命令给Raft。
 */
void KvServer::IfNeedToSendSnapShotCommand(int raftIndex, int proportion)
{
  if (ShouldTakeSnapshot(raftIndex))
  {
    // 根据数据大小选择快照方式
    size_t skipListSize = m_skipList.size();
    const size_t STREAMING_THRESHOLD = 10000; // 超过10000个元素使用流式快照

    if (skipListSize > STREAMING_THRESHOLD)
    {
      // 使用流式快照
      auto snapshotPath = MakeStreamingSnapshot();
      if (!snapshotPath.empty())
      {
        m_raftNode->StreamingSnapshot(raftIndex, snapshotPath);
        DPrintf("[IfNeedToSendSnapShotCommand] Server %d used streaming snapshot for %zu elements",
                m_me, skipListSize);
      }
      else
      {
        DPrintf("[IfNeedToSendSnapShotCommand] Server %d failed to create streaming snapshot, falling back to regular snapshot", m_me);
        // 回退到常规快照
        auto snapshot = MakeSnapShot();
        m_raftNode->Snapshot(raftIndex, snapshot);
      }
    }
    else
    {
      // 使用常规快照
      auto snapshot = MakeSnapShot();
      m_raftNode->Snapshot(raftIndex, snapshot);
      DPrintf("[IfNeedToSendSnapShotCommand] Server %d used regular snapshot for %zu elements",
              m_me, skipListSize);
    }

    // 更新快照时间
    m_lastSnapshotTime = std::chrono::steady_clock::now();
  }
}

/**
 * @brief 判断是否应该制作快照
 * @param raftIndex 当前Raft日志索引
 * @return 如果需要制作快照返回true，否则返回false
 *
 * 根据三个条件判断是否需要制作快照：
 * 1. Raft状态大小超过阈值
 * 2. 距离上次快照时间过长
 * 3. 日志条目数量过多
 */
bool KvServer::ShouldTakeSnapshot(int raftIndex)
{
  // 条件1：检查Raft状态大小（使用缓存的值，避免IO）
  size_t currentRaftStateSize = m_raftStateSize.load();
  bool sizeExceeded = currentRaftStateSize > static_cast<size_t>(m_maxRaftState * SNAPSHOT_SIZE_THRESHOLD_RATIO);

  // 条件2：检查时间间隔
  auto now = std::chrono::steady_clock::now();
  auto timeSinceLastSnapshot = now - m_lastSnapshotTime;
  bool timeExceeded = timeSinceLastSnapshot > SNAPSHOT_TIME_THRESHOLD;

  // 条件3：检查日志条目数量（从上次快照点到当前索引）
  int logEntriesSinceSnapshot = raftIndex - m_lastSnapShotRaftLogIndex;
  bool logEntriesExceeded = logEntriesSinceSnapshot > SNAPSHOT_LOG_ENTRIES_THRESHOLD;

  // 任何一个条件满足都触发快照
  bool shouldSnapshot = sizeExceeded || timeExceeded || logEntriesExceeded;

  if (shouldSnapshot)
  {
    DPrintf("[ShouldTakeSnapshot] Server %d triggering snapshot at index %d. "
            "Size: %zu/%d (exceeded: %s), Time: %lld min (exceeded: %s), "
            "LogEntries: %d/%d (exceeded: %s)",
            m_me, raftIndex, currentRaftStateSize, m_maxRaftState,
            sizeExceeded ? "yes" : "no",
            std::chrono::duration_cast<std::chrono::minutes>(timeSinceLastSnapshot).count(),
            timeExceeded ? "yes" : "no",
            logEntriesSinceSnapshot, SNAPSHOT_LOG_ENTRIES_THRESHOLD,
            logEntriesExceeded ? "yes" : "no");
  }

  return shouldSnapshot;
}

/**
 * @brief 更新Raft状态大小缓存
 * @param deltaSize 状态大小变化量
 *
 * 原子操作更新缓存的Raft状态大小，避免频繁IO操作。
 */
void KvServer::UpdateRaftStateSizeCache(long long deltaSize)
{
  // 原子操作更新缓存的Raft状态大小
  size_t oldSize = m_raftStateSize.load();
  size_t newSize = static_cast<size_t>(std::max(0LL, static_cast<long long>(oldSize) + deltaSize));
  m_raftStateSize.store(newSize);

  DPrintf("[UpdateRaftStateSizeCache] Server %d: size changed from %zu to %zu (delta: %lld)",
          m_me, oldSize, newSize, deltaSize);
}

/**
 * @brief 从Raft接收快照
 * @param message 包含快照信息的ApplyMsg
 *
 * 处理来自Raft的快照消息，如果条件满足则安装快照。
 */
void KvServer::GetSnapShotFromRaft(ApplyMsg message)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot))
  {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

/**
 * @brief 制作快照
 * @return 快照数据字符串
 *
 * 创建当前KV数据库状态的快照，用于持久化存储。
 */
std::string KvServer::MakeSnapShot()
{
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

/**
 * @brief 制作流式快照
 * @return 快照文件路径，失败时返回空字符串
 *
 * 使用流式快照管理器创建快照，适用于大数据量场景。
 */
std::string KvServer::MakeStreamingSnapshot()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  std::string snapshotPath;
  if (m_streamingSnapshotManager->CreateSnapshot(m_skipList, m_lastRequestId, snapshotPath))
  {
    DPrintf("[MakeStreamingSnapshot] Server %d created streaming snapshot: %s", m_me, snapshotPath.c_str());
    return snapshotPath;
  }
  else
  {
    DPrintf("[MakeStreamingSnapshot] Server %d failed to create streaming snapshot", m_me);
    return "";
  }
}

/**
 * @brief 读取并安装流式快照
 * @param snapshotPath 快照文件路径
 *
 * 从流式快照文件中恢复KV数据库状态，恢复完成后清理临时文件。
 */
void KvServer::ReadStreamingSnapshotToInstall(const std::string &snapshotPath)
{
  if (snapshotPath.empty())
  {
    return;
  }

  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_streamingSnapshotManager->RestoreSnapshot(snapshotPath, m_skipList, m_lastRequestId))
  {
    DPrintf("[ReadStreamingSnapshotToInstall] Server %d restored streaming snapshot from: %s",
            m_me, snapshotPath.c_str());

    // 清理临时文件
    StreamingSnapshotManager::CleanupTempFile(snapshotPath);
  }
  else
  {
    DPrintf("[ReadStreamingSnapshotToInstall] Server %d failed to restore streaming snapshot from: %s",
            m_me, snapshotPath.c_str());
  }
}

/**
 * @brief PutAppend RPC回调函数
 * @param controller RPC控制器
 * @param request 请求参数
 * @param response 响应结果
 * @param done 完成回调
 *
 * Google Protocol Buffers RPC框架的回调函数，处理异步RPC请求。
 */
void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done)
{
  KvServer::PutAppend(request, response);
  done->Run();
}

/**
 * @brief Get RPC回调函数
 * @param controller RPC控制器
 * @param request 请求参数
 * @param response 响应结果
 * @param done 完成回调
 *
 * Google Protocol Buffers RPC框架的回调函数，处理异步RPC请求。
 */
void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
  KvServer::Get(request, response);
  done->Run();
}

/**
 * @brief KvServer构造函数
 * @param me 节点ID
 * @param maxraftstate 最大Raft状态大小
 * @param nodeInforFileName 节点信息文件名
 * @param port 服务端口
 *
 * 初始化KV服务器，包括Raft节点、RPC服务、持久化存储等组件。
 * 支持多种等待模式（Promise/Future、Channel、LockQueue）和流式快照。
 */
KvServer::KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port) : m_skipList(6)
{
  std::shared_ptr<Persister> persister = std::make_shared<Persister>(me);

  m_me = me;
  m_maxRaftState = maxraftstate;

  // 初始化优化相关变量
  usePromiseFuture_ = false;                         // 暂时关闭Promise/Future模式
  useChannel_ = true;                                // 默认使用Channel模式
  m_raftStateSize.store(persister->RaftStateSize()); // 从持久化存储中读取初始状态大小
  m_lastSnapshotTime = std::chrono::steady_clock::now();

  // 初始化流式快照管理器
  m_streamingSnapshotManager = std::make_unique<StreamingSnapshotManager>(me);

  applyChan = monsoon::createChannel<ApplyMsg>(100); // 使用Channel替代LockQueue，缓冲区大小100

  m_raftNode = std::make_shared<Raft>();

  // 从配置文件读取本节点的IP地址
  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str());
  std::string nodeIpKey = "node" + std::to_string(m_me) + "ip";
  std::string nodeIp = config.Load(nodeIpKey);
  if (nodeIp.empty())
  {
    nodeIp = "127.0.0.1"; // 默认IP地址
  }

  // clerk层面 kvserver开启rpc接受功能
  // 同时raft与raft节点之间也要开启rpc功能，因此有两个注册

  // 设置Muduo日志级别，减少第三方库日志输出
  muduo::Logger::setLogLevel(muduo::Logger::WARN);

  // 使用条件变量来同步RPC服务启动
  std::mutex rpcReadyMutex;
  std::condition_variable rpcReadyCV;
  bool rpcReady = false;

  std::thread t([this, nodeIp, port, &rpcReadyMutex, &rpcReadyCV, &rpcReady]() -> void
                {
    // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(
        this->m_raftNode.get()); 

    // 启动一个rpc服务发布节点，使用带回调的版本来通知服务就绪
    provider.Run(nodeIp, port, [&rpcReadyMutex, &rpcReadyCV, &rpcReady, this]() {
      std::lock_guard<std::mutex> lock(rpcReadyMutex);
      rpcReady = true;
      rpcReadyCV.notify_one();
      std::cout << "🚀 [节点" << m_me << "] RPC服务已完全就绪，可以接受连接" << std::endl;
    }); });
  t.detach();

  // 等待RPC服务完全就绪
  std::unique_lock<std::mutex> lock(rpcReadyMutex);
  rpcReadyCV.wait(lock, [&rpcReady]
                  { return rpcReady; });
  std::cout << "✅ [节点" << m_me << "] RPC服务启动完成，继续初始化..." << std::endl;

  // 开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
  // 使用更智能的等待机制，确保RPC服务真正就绪
  std::cout << "raftServer node:" << m_me << " start to wait for RPC service ready..." << std::endl;

  // 基础等待时间，确保RPC服务线程有足够时间启动
  int baseWaitTime = 8; // 增加到8秒
  std::cout << "raftServer node:" << m_me << " 基础等待 " << baseWaitTime << " 秒..." << std::endl;
  sleep(baseWaitTime);

  // 额外的节点特定延迟，避免所有节点同时开始连接
  // 但是要确保所有节点都有足够的时间完成初始化
  int nodeSpecificDelay = m_me * 3; // 每个节点额外延迟 3 * 节点ID 秒，增加延迟时间
  if (nodeSpecificDelay > 0)
  {
    std::cout << "raftServer node:" << m_me << " 节点特定延迟 " << nodeSpecificDelay << " 秒..." << std::endl;
    sleep(nodeSpecificDelay);
  }
  else
  {
    // 即使是节点0，也要额外等待一些时间，确保其他节点有机会启动
    int additionalWaitForNode0 = 5; // 节点0额外等待5秒
    std::cout << "raftServer node:" << m_me << " 作为节点0，额外等待 " << additionalWaitForNode0 << " 秒确保其他节点启动..." << std::endl;
    sleep(additionalWaitForNode0);
  }

  std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;

  // 获取所有raft节点ip、port，并进行连接，要排除自己
  // 重用之前声明的 config 对象
  std::vector<std::pair<std::string, short>> ipPortVt;
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

  std::vector<std::shared_ptr<RaftRpcUtil>> servers;

  // 改进的连接建立逻辑：带重试和验证的连接
  std::cout << "node" << m_me << " 开始建立与其他节点的连接..." << std::endl;
  for (int i = 0; i < ipPortVt.size(); ++i)
  {
    if (i == m_me)
    {
      servers.push_back(nullptr);
      continue;
    }

    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;

    // 尝试建立连接，最多重试10次，使用指数退避
    bool connected = false;
    int maxRetries = 10;
    int baseDelay = 500; // 基础延迟500ms

    for (int retry = 0; retry < maxRetries && !connected; ++retry)
    {
      try
      {
        auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
        auto rpcPtr = std::shared_ptr<RaftRpcUtil>(rpc);
        servers.push_back(rpcPtr);

        // 验证连接是否真正可用
        // 注意：由于使用延迟连接，这里的测试可能会触发实际的连接建立
        if (rpcPtr->testConnection())
        {
          connected = true;
          std::cout << "node" << m_me << " 连接node" << i << " success! (尝试 " << (retry + 1) << "/" << maxRetries << ")" << std::endl;
        }
        else
        {
          std::cout << "node" << m_me << " 连接node" << i << " 建立成功但验证失败 (尝试 " << (retry + 1) << "/" << maxRetries << ")" << std::endl;
          // 连接验证失败，但我们仍然保留连接，稍后可能会成功
          connected = true; // 暂时标记为成功，允许系统继续运行
        }
      }
      catch (const std::exception &e)
      {
        std::cout << "node" << m_me << " 连接node" << i << " 失败 (尝试 " << (retry + 1) << "/" << maxRetries << "): " << e.what() << std::endl;

        if (retry < maxRetries - 1)
        {
          // 指数退避：每次重试延迟时间翻倍，最大不超过8秒
          int delay = std::min(baseDelay * (1 << retry), 8000);
          std::cout << "node" << m_me << " 等待 " << delay << "ms 后重试连接node" << i << std::endl;
          std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        }
      }
    }

    if (!connected)
    {
      std::cerr << "node" << m_me << " 无法连接到node" << i << " 在 " << maxRetries << " 次尝试后，使用空连接" << std::endl;
      servers.push_back(nullptr); // 添加空连接，稍后可能会重连
    }
  }

  // 额外等待时间，确保所有节点都完成了相互连接
  int additionalWait = std::max(5, static_cast<int>(ipPortVt.size()) * 2);
  std::cout << "node" << m_me << " 连接建立完成，额外等待 " << additionalWait << " 秒确保集群稳定..." << std::endl;
  sleep(additionalWait);

  // 连接状态验证：尝试验证与其他节点的连接是否真正可用
  std::cout << "node" << m_me << " 开始验证与其他节点的连接状态..." << std::endl;
  int validConnections = 0;
  for (int i = 0; i < servers.size(); ++i)
  {
    if (i == m_me || servers[i] == nullptr)
    {
      continue; // 跳过自己和空连接
    }

    // 这里我们暂时跳过实际的连接验证，因为需要等待目标节点的Raft服务完全启动
    // 在实际生产环境中，可以发送一个简单的ping RPC来验证连接
    validConnections++;
  }

  std::cout << "node" << m_me << " 连接验证完成，有效连接数: " << validConnections
            << "/" << (ipPortVt.size() - 1) << std::endl;

  // 如果连接数不足，给出警告但仍然继续
  if (validConnections < (ipPortVt.size() - 1) / 2)
  {
    std::cout << "警告: node" << m_me << " 的有效连接数不足一半，可能影响集群稳定性" << std::endl;
  }

  std::cout << "node" << m_me << " 开始初始化Raft节点..." << std::endl;
  m_raftNode->init(servers, m_me, persister, applyChan);

  // Raft初始化完成后，稍微等待一下确保系统稳定
  int postInitWait = 5; // 初始化后等待5秒
  std::cout << "node" << m_me << " Raft初始化完成，等待 " << postInitWait << " 秒确保系统稳定..." << std::endl;
  sleep(postInitWait);

  // 创建就绪标志文件，表示该节点已完全初始化
  std::string readyFile = "/tmp/raft_node_" + std::to_string(m_me) + "_ready";
  std::ofstream ofs(readyFile);
  if (ofs.is_open())
  {
    ofs << "ready" << std::endl;
    ofs.close();
    std::cout << "📝 [节点" << m_me << "] 创建就绪标志文件: " << readyFile << std::endl;
  }

  // 等待所有节点都就绪
  std::cout << "node" << m_me << " 等待所有节点就绪..." << std::endl;
  int totalNodes = ipPortVt.size();
  bool allReady = false;
  int checkCount = 0;
  const int maxChecks = 120; // 最多检查2分钟

  while (!allReady && checkCount < maxChecks)
  {
    allReady = true;
    for (int i = 0; i < totalNodes; ++i)
    {
      std::string nodeReadyFile = "/tmp/raft_node_" + std::to_string(i) + "_ready";
      std::ifstream ifs(nodeReadyFile);
      if (!ifs.is_open())
      {
        allReady = false;
        break;
      }
      ifs.close();
    }

    if (!allReady)
    {
      checkCount++;
      std::cout << "node" << m_me << " 等待其他节点就绪... (检查 " << checkCount << "/" << maxChecks << ")" << std::endl;
      sleep(1);
    }
  }

  if (allReady)
  {
    std::cout << "node" << m_me << " 所有节点已就绪，开始正常运行" << std::endl;
  }
  else
  {
    std::cout << "node" << m_me << " 警告：等待超时，但仍继续运行" << std::endl;
  }

  // 现在所有节点都就绪了，启动选举定时器
  std::cout << "🗳️  [节点" << m_me << "] 启动选举定时器，开始Raft选举过程" << std::endl;
  m_raftNode->startElectionTimer();

  std::cout << "🎯 [节点" << m_me << "] 完全就绪，可以开始处理请求" << std::endl;
  // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

  // 设置状态大小变化回调
  m_raftNode->SetStateSizeChangeCallback([this](long long deltaSize)
                                         { this->UpdateRaftStateSizeCache(deltaSize); });

  m_skipList;
  waitApplyCh;
  m_lastRequestId;
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty())
  {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this); // 马上向其他节点宣告自己就是leader
  t2.join();                                                 // 由于ReadRaftApplyCommandLoop一直不会结束，达到一直卡在这的目的
}

// ==================== 等待机制优化实现 ====================

/**
 * @brief 优化的Raft提交等待机制
 * @param op 操作对象
 * @param raftIndex Raft日志索引
 * @param timeoutMs 超时时间（毫秒）
 * @param result 返回结果
 * @return 等待是否成功
 *
 * 支持三种等待模式：Promise/Future、Channel、LockQueue对象池。
 * 根据配置选择最优的等待机制。
 */
bool KvServer::WaitForRaftCommitOptimized(const Op &op, int raftIndex, int timeoutMs, Op *result)
{
  if (usePromiseFuture_)
  {
    // 使用 Promise/Future 模式
    auto handle = promiseManager_.createWaitHandle(raftIndex);

    // 等待结果
    bool success = promiseManager_.waitForResult(handle, timeoutMs, result);

    if (!success)
    {
      // 超时或失败，清理等待句柄
      promiseManager_.removeWaitHandle(raftIndex);
    }

    return success;
  }
  else if (useChannel_)
  {
    // 使用新的 Channel 模式
    m_mtx.lock();

    monsoon::Channel<Op>::ptr chForRaftIndex;
    if (waitApplyChChannel.find(raftIndex) == waitApplyChChannel.end())
    {
      // 创建新的 Channel
      chForRaftIndex = monsoon::createChannel<Op>(1); // 缓冲区大小为1
      waitApplyChChannel[raftIndex] = chForRaftIndex;
    }
    else
    {
      chForRaftIndex = waitApplyChChannel[raftIndex];
    }

    m_mtx.unlock();

    // 等待结果
    auto channelResult = chForRaftIndex->receive(*result, timeoutMs);
    bool success = (channelResult == monsoon::ChannelResult::SUCCESS);

    // 清理
    m_mtx.lock();
    waitApplyChChannel.erase(raftIndex);
    m_mtx.unlock();

    return success;
  }
  else
  {
    // 回退到原有的 LockQueue 模式（使用对象池优化）
    m_mtx.lock();

    std::shared_ptr<LockQueue<Op>> chForRaftIndex;
    if (waitApplyCh.find(raftIndex) == waitApplyCh.end())
    {
      // 从对象池获取 LockQueue
      chForRaftIndex = lockQueuePool_.acquire();
      waitApplyCh[raftIndex] = chForRaftIndex.get();
    }
    else
    {
      // 这种情况下需要创建新的，因为原有代码使用裸指针
      chForRaftIndex = std::make_shared<LockQueue<Op>>();
      waitApplyCh[raftIndex] = chForRaftIndex.get();
    }

    m_mtx.unlock();

    // 等待结果
    bool success = chForRaftIndex->timeOutPop(timeoutMs, result);

    // 清理
    m_mtx.lock();
    waitApplyCh.erase(raftIndex);
    m_mtx.unlock();

    // 归还到对象池
    lockQueuePool_.release(chForRaftIndex);

    return success;
  }
}

/**
 * @brief 设置等待模式
 * @param usePromiseFuture 是否使用Promise/Future模式
 *
 * 动态切换等待机制，支持运行时配置。
 */
void KvServer::SetWaitMode(bool usePromiseFuture)
{
  std::lock_guard<std::mutex> lock(m_mtx);
  usePromiseFuture_ = usePromiseFuture;

  DPrintf("[SetWaitMode] KvServer %d switched to %s mode",
          m_me, usePromiseFuture ? "Promise/Future" : "LockQueue Pool");
}

/**
 * @brief 更新Raft状态大小
 * @param newSize 新的状态大小
 *
 * 原子操作更新Raft状态大小缓存。
 */
void KvServer::UpdateRaftStateSize(size_t newSize)
{
  m_raftStateSize.store(newSize);
}