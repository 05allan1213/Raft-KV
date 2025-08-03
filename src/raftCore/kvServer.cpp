#include "kvServer.h"

#include <rpcprovider.h>

#include "raft-kv/rpc/mprpcconfig.h"

/**
 * @brief 打印KV数据库内容（调试用）
 *
 * 在调试模式下，打印当前KV数据库中所有键值对的内容。
 * 使用跳表的数据结构来展示数据。
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
    // 显示跳表中的所有键值对
    m_skipList.display_list();
  };
}

/**
 * @brief 在KV数据库上执行Append操作
 *
 * 将新的值追加到指定键的现有值后面。
 * 如果键不存在，则创建新键值对。
 *
 * @param op 包含操作信息的Op对象
 */
void KvServer::ExecuteAppendOpOnKVDB(Op op)
{
  // Get请求是可重复执行的，因此可以不用判断重复
  m_mtx.lock();

  // 使用跳表执行Append操作
  m_skipList.insert_set_element(op.Key, op.Value);

  // 记录客户端的最新请求ID，用于重复请求检测
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  // 调试模式下打印数据库内容
  DprintfKVDB();
}

/**
 * @brief 在KV数据库上执行Get操作
 *
 * 根据键查找对应的值，如果键存在则返回true和对应的值，
 * 如果键不存在则返回false和空字符串。
 *
 * @param op 包含操作信息的Op对象
 * @param value 返回的值
 * @param exist 键是否存在
 */
void KvServer::ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist)
{
  m_mtx.lock();
  *value = "";
  *exist = false;

  // 使用跳表查找键值对
  if (m_skipList.search_element(op.Key, *value))
  {
    *exist = true;
    // value已经通过search_element完成赋值了
  }

  // 记录客户端的最新请求ID，用于重复请求检测
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  // 调试模式下打印数据库内容
  DprintfKVDB();
}

/**
 * @brief 在KV数据库上执行Put操作
 *
 * 将键值对插入或更新到数据库中。
 * 如果键已存在，则更新其值；如果键不存在，则创建新的键值对。
 *
 * @param op 包含操作信息的Op对象
 */
void KvServer::ExecutePutOpOnKVDB(Op op)
{
  m_mtx.lock();

  // 使用跳表执行Put操作
  m_skipList.insert_set_element(op.Key, op.Value);

  // 记录客户端的最新请求ID，用于重复请求检测
  m_lastRequestId[op.ClientId] = op.RequestId;
  m_mtx.unlock();

  // 调试模式下打印数据库内容
  DprintfKVDB();
}

/**
 * @brief 处理来自客户端的Get RPC请求
 *
 * 该函数处理客户端的Get请求，将请求提交给Raft共识算法，
 * 确保在分布式环境中数据的一致性。
 *
 * @param args Get请求参数，包含键、客户端ID、请求ID等
 * @param reply Get响应结果，包含值、错误信息等
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
  m_raftNode->Start(op, &raftIndex, &_,
                    &isLeader); // raftIndex：raft预计的logIndex
                                // ，虽然是预计，但是正确情况下是准确的，op的具体内容对raft来说 是隔离的

  if (!isLeader)
  {
    reply->set_err(ErrWrongLeader);
    return;
  }

  // 使用优化的等待机制
  Op raftCommitOp;
  bool waitSuccess = WaitForRaftCommitOptimized(op, raftIndex, CONSENSUS_TIMEOUT, &raftCommitOp);

  if (!waitSuccess)
  {
    //        DPrintf("[GET TIMEOUT!!!]From Client %d (Request %d) To Server %d, key %v, raftIndex %d", args.ClientId,
    //        args.RequestId, kv.me, op.Key, raftIndex)
    // todo 2023年06月01日
    int _ = -1;
    bool isLeader = false;
    m_raftNode->GetState(&_, &isLeader);

    if (ifRequestDuplicate(op.ClientId, op.RequestId) && isLeader)
    {
      // 如果超时，代表raft集群不保证已经commitIndex该日志，但是如果是已经提交过的get请求，是可以再执行的。
      //  不会违反线性一致性
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist)
      {
        reply->set_err(OK);
        reply->set_value(value);
      }
      else
      {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    }
    else
    {
      reply->set_err(ErrWrongLeader); // 返回这个，其实就是让clerk换一个节点重试
    }
  }
  else
  {
    // raft已经提交了该command（op），可以正式开始执行了
    //         DPrintf("[WaitChanGetRaftApplyMessage<--]Server %d , get Command <-- Index:%d , ClientId %d, RequestId
    //         %d, Opreation %v, Key :%v, Value :%v", kv.me, raftIndex, op.ClientId, op.RequestId, op.Operation, op.Key,
    //         op.Value)
    // todo 这里还要再次检验的原因：感觉不用检验，因为leader只要正确的提交了，那么这些肯定是符合的
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
    {
      std::string value;
      bool exist = false;
      ExecuteGetOpOnKVDB(op, &value, &exist);
      if (exist)
      {
        reply->set_err(OK);
        reply->set_value(value);
      }
      else
      {
        reply->set_err(ErrNoKey);
        reply->set_value("");
      }
    }
    else
    {
      reply->set_err(ErrWrongLeader);
      //            DPrintf("[GET ] 不满足：raftCommitOp.ClientId{%v} == op.ClientId{%v} && raftCommitOp.RequestId{%v}
      //            == op.RequestId{%v}", raftCommitOp.ClientId, op.ClientId, raftCommitOp.RequestId, op.RequestId)
    }
  }
  // 新的等待机制会自动清理，无需手动删除
}

void KvServer::GetCommandFromRaft(ApplyMsg message)
{
  Op op;
  op.parseFromString(message.Command);

  DPrintf(
      "[KvServer::GetCommandFromRaft-kvserver{%d}] , Got Command --> Index:{%d} , ClientId {%s}, RequestId {%d}, "
      "Opreation {%s}, Key :{%s}, Value :{%s}",
      m_me, message.CommandIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
  if (message.CommandIndex <= m_lastSnapShotRaftLogIndex)
  {
    return;
  }

  // State Machine (KVServer solute the duplicate problem)
  // duplicate command will not be exed
  if (!ifRequestDuplicate(op.ClientId, op.RequestId))
  {
    // execute command
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
  // 到这里kvDB已经制作了快照
  if (m_maxRaftState != -1)
  {
    IfNeedToSendSnapShotCommand(message.CommandIndex, 9);
    // 如果raft的log太大（大于指定的比例）就把制作快照
  }

  // Send message to the chan of op.ClientId
  SendMessageToWaitChan(op, message.CommandIndex);
}

bool KvServer::ifRequestDuplicate(std::string ClientId, int RequestId)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  if (m_lastRequestId.find(ClientId) == m_lastRequestId.end())
  {
    return false;
    // todo :不存在这个client就创建
  }
  return RequestId <= m_lastRequestId[ClientId];
}

// get和put//append執行的具體細節是不一樣的
// PutAppend在收到raft消息之後執行，具體函數裏面只判斷冪等性（是否重複）
// get函數收到raft消息之後在，因爲get無論是否重複都可以再執行
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
        m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);

    reply->set_err(ErrWrongLeader);
    return;
  }
  DPrintf(
      "[func -KvServer::PutAppend -kvserver{%d}]From Client %s (Request %d) To Server %d, key %s, raftIndex %d , is "
      "leader ",
      m_me, &args->clientid(), args->requestid(), m_me, &op.Key, raftIndex);
  // 使用优化的等待机制
  Op raftCommitOp;
  bool waitSuccess = WaitForRaftCommitOptimized(op, raftIndex, CONSENSUS_TIMEOUT, &raftCommitOp);

  if (!waitSuccess)
  {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]TIMEOUT PUTAPPEND !!!! Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %s, Opreation %s Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

    if (ifRequestDuplicate(op.ClientId, op.RequestId))
    {
      reply->set_err(OK); // 超时了,但因为是重复的请求，返回ok，实际上就算没有超时，在真正执行的时候也要判断是否重复
    }
    else
    {
      reply->set_err(ErrWrongLeader); /// 这里返回这个的目的让clerk重新尝试
    }
  }
  else
  {
    DPrintf(
        "[func -KvServer::PutAppend -kvserver{%d}]WaitChanGetRaftApplyMessage<--Server %d , get Command <-- Index:%d , "
        "ClientId %s, RequestId %d, Opreation %s, Key :%s, Value :%s",
        m_me, m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    if (raftCommitOp.ClientId == op.ClientId && raftCommitOp.RequestId == op.RequestId)
    {
      // 可能发生leader的变更导致日志被覆盖，因此必须检查
      reply->set_err(OK);
    }
    else
    {
      reply->set_err(ErrWrongLeader);
    }
  }
  // 新的等待机制会自动清理，无需手动删除
}

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
    // listen to every command applied by its raft ,delivery to relative RPC Handler

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
void KvServer::ReadSnapShotToInstall(std::string snapshot)
{
  if (snapshot.empty())
  {
    // bootstrap without any state?
    return;
  }
  parseFromString(snapshot);

  //    r := bytes.NewBuffer(snapshot)
  //    d := labgob.NewDecoder(r)
  //
  //    var persist_kvdb map[string]string  //理应快照
  //    var persist_lastRequestId map[int64]int //快照这个为了维护线性一致性
  //
  //    if d.Decode(&persist_kvdb) != nil || d.Decode(&persist_lastRequestId) != nil {
  //                DPrintf("KVSERVER %d read persister got a problem!!!!!!!!!!",kv.me)
  //        } else {
  //        kv.kvDB = persist_kvdb
  //        kv.lastRequestId = persist_lastRequestId
  //    }
}

bool KvServer::SendMessageToWaitChan(const Op &op, int raftIndex)
{
  DPrintf(
      "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
      "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
      m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);

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
          "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command via Channel --> Index:{%d} , ClientId {%d}, RequestId "
          "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
          m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
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
        "[RaftApplyMessageSendToWaitChan--> raftserver{%d}] , Send Command --> Index:{%d} , ClientId {%d}, RequestId "
        "{%d}, Opreation {%v}, Key :{%v}, Value :{%v}",
        m_me, raftIndex, &op.ClientId, op.RequestId, &op.Operation, &op.Key, &op.Value);
    return true;
  }
}

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

void KvServer::UpdateRaftStateSizeCache(long long deltaSize)
{
  // 原子操作更新缓存的Raft状态大小
  size_t oldSize = m_raftStateSize.load();
  size_t newSize = static_cast<size_t>(std::max(0LL, static_cast<long long>(oldSize) + deltaSize));
  m_raftStateSize.store(newSize);

  DPrintf("[UpdateRaftStateSizeCache] Server %d: size changed from %zu to %zu (delta: %lld)",
          m_me, oldSize, newSize, deltaSize);
}

void KvServer::GetSnapShotFromRaft(ApplyMsg message)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_raftNode->CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot))
  {
    ReadSnapShotToInstall(message.Snapshot);
    m_lastSnapShotRaftLogIndex = message.SnapshotIndex;
  }
}

std::string KvServer::MakeSnapShot()
{
  std::lock_guard<std::mutex> lg(m_mtx);
  std::string snapshotData = getSnapshotData();
  return snapshotData;
}

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

void KvServer::PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                         ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done)
{
  KvServer::PutAppend(request, response);
  done->Run();
}

void KvServer::Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
                   ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done)
{
  KvServer::Get(request, response);
  done->Run();
}

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
  ////////////////clerk层面 kvserver开启rpc接受功能
  //    同时raft与raft节点之间也要开启rpc功能，因此有两个注册
  std::thread t([this, port]() -> void
                {
    // provider是一个rpc网络服务对象。把UserService对象发布到rpc节点上
    RpcProvider provider;
    provider.NotifyService(this);
    provider.NotifyService(
        this->m_raftNode.get());  // todo：这里获取了原始指针，后面检查一下有没有泄露的问题 或者 shareptr释放的问题
    // 启动一个rpc服务发布节点   Run以后，进程进入阻塞状态，等待远程的rpc调用请求
    provider.Run(m_me, port); });
  t.detach();

  ////开启rpc远程调用能力，需要注意必须要保证所有节点都开启rpc接受功能之后才能开启rpc远程调用能力
  ////这里使用睡眠来保证
  std::cout << "raftServer node:" << m_me << " start to sleep to wait all ohter raftnode start!!!!" << std::endl;
  sleep(6);
  std::cout << "raftServer node:" << m_me << " wake up!!!! start to connect other raftnode" << std::endl;
  // 获取所有raft节点ip、port ，并进行连接  ,要排除自己
  MprpcConfig config;
  config.LoadConfigFile(nodeInforFileName.c_str());
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
    ipPortVt.emplace_back(nodeIp, atoi(nodePortStr.c_str())); // 沒有atos方法，可以考慮自己实现
  }
  std::vector<std::shared_ptr<RaftRpcUtil>> servers;
  // 进行连接
  for (int i = 0; i < ipPortVt.size(); ++i)
  {
    if (i == m_me)
    {
      servers.push_back(nullptr);
      continue;
    }
    std::string otherNodeIp = ipPortVt[i].first;
    short otherNodePort = ipPortVt[i].second;
    auto *rpc = new RaftRpcUtil(otherNodeIp, otherNodePort);
    servers.push_back(std::shared_ptr<RaftRpcUtil>(rpc));

    std::cout << "node" << m_me << " 连接node" << i << "success!" << std::endl;
  }
  sleep(ipPortVt.size() - me); // 等待所有节点相互连接成功，再启动raft
  m_raftNode->init(servers, m_me, persister, applyChan);
  // kv的server直接与raft通信，但kv不直接与raft通信，所以需要把ApplyMsg的chan传递下去用于通信，两者的persist也是共用的

  // 设置状态大小变化回调
  m_raftNode->SetStateSizeChangeCallback([this](long long deltaSize)
                                         { this->UpdateRaftStateSizeCache(deltaSize); });

  m_skipList;
  waitApplyCh;
  m_lastRequestId;
  m_lastSnapShotRaftLogIndex = 0; // todo:感覺這個函數沒什麼用，不如直接調用raft節點中的snapshot值？？？
  auto snapshot = persister->ReadSnapshot();
  if (!snapshot.empty())
  {
    ReadSnapShotToInstall(snapshot);
  }
  std::thread t2(&KvServer::ReadRaftApplyCommandLoop, this); // 马上向其他节点宣告自己就是leader
  t2.join();                                                 // 由于ReadRaftApplyCommandLoop一直不会結束，达到一直卡在这的目的
}

// ==================== 等待机制优化实现 ====================

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

void KvServer::SetWaitMode(bool usePromiseFuture)
{
  std::lock_guard<std::mutex> lock(m_mtx);
  usePromiseFuture_ = usePromiseFuture;

  DPrintf("[SetWaitMode] KvServer %d switched to %s mode",
          m_me, usePromiseFuture ? "Promise/Future" : "LockQueue Pool");
}

void KvServer::UpdateRaftStateSize(size_t newSize)
{
  m_raftStateSize.store(newSize);
}