#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include <iomanip>
#include "config.h"
#include "util.h"

/**
 * @brief 处理日志追加RPC请求
 *
 * 实现Raft算法中的AppendEntries RPC处理逻辑，负责：
 * 1. 验证领导者任期的有效性
 * 2. 检查日志一致性约束
 * 3. 追加新的日志条目
 * 4. 更新提交索引
 *
 * @param args 包含领导者任期、前置日志信息、新日志条目等的请求参数
 * @param reply 返回处理结果，包括成功状态、当前任期、索引更新建议等
 */
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply)
{
  // 设置网络状态为正常，表示能够接收到RPC请求
  reply->set_appstate(AppNormal);

  // 任期检查：拒绝来自过期领导者的请求
  if (args->term() < m_currentTerm)
  {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100); // 特殊值，提示领导者及时更新
    DPrintf("[AppendEntries-节点%d] 拒绝过期领导者%d的请求，任期%d < 当前任期%d\n",
            m_me, args->leaderid(), args->term(), m_currentTerm);
    return; // 不重置选举定时器，因为这是过期消息
  }
  // 确保在函数结束时持久化状态
  DEFER { persist(); };
  if (args->term() > m_currentTerm)
  {
    // 执行"三变"：状态、任期、投票记录

    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1; // 重置投票记录，允许在新任期中投票

    // 如果本来就是Follower，那么其term变化，相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
  }
  myAssert(args->term() == m_currentTerm, "任期不一致错误");

  // 确保当前节点为跟随者状态（处理候选者收到同任期领导者消息的情况）
  m_status = Follower;

  // 重置选举超时定时器，因为收到了有效的领导者消息
  m_lastResetElectionTime = now();

  // 日志一致性检查：前置日志索引超出当前日志范围
  if (args->prevlogindex() > getLastLogIndex())
  {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    return;
  }
  // 前置日志索引在快照范围内
  else if (args->prevlogindex() < m_lastSnapshotIncludeIndex)
  {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(m_lastSnapshotIncludeIndex + 1);
  }
  // 检查前置日志是否匹配
  if (matchLog(args->prevlogindex(), args->prevlogterm()))
  {

    // 不能直接截断，必须一个一个检查，因为发送来的log可能是之前的，直接截断可能导致“取回”已经在follower日志中的条目
    // 那意思是不是可能会有一段发来的AE中的logs中前半是匹配的，后半是不匹配的，这种应该：1.follower如何处理？ 2.如何给leader回复 3. leader如何处理

    for (int i = 0; i < args->entries_size(); i++)
    {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex())
      {
        // 新日志条目，直接追加到日志末尾
        m_logs.push_back(log);
      }
      else
      {
        // 检查现有位置的日志条目是否需要更新
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command())
        {
          // 同索引同任期但命令不同，违反Raft一致性原则
          myAssert(false, format("[AppendEntries-节点%d] 日志一致性错误: 索引%d任期%d处命令不一致 "
                                 "本地命令%d vs 领导者%d命令%d",
                                 m_me, log.logindex(), log.logterm(),
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(),
                                 args->leaderid(), log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm())
        {
          // 任期不匹配，用新条目覆盖
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[AppendEntries-节点%d] 日志长度验证失败: lastIndex=%d, prevIndex=%d, entries=%d",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));

    if (args->leadercommit() > m_commitIndex)
    {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      // 注意：提交索引不能超过最后日志索引
    }

    // 确保提交索引不超过最后日志索引
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[AppendEntries-节点%d] 提交索引%d超过最后日志索引%d",
                    m_me, m_commitIndex, getLastLogIndex()));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    return;
  }
  else
  {
    // 日志不匹配，执行快速回退优化
    // 寻找冲突任期的第一个条目，减少RPC往返次数
    reply->set_updatenextindex(args->prevlogindex());

    for (int index = args->prevlogindex(); index >= m_lastSnapshotIncludeIndex; --index)
    {
      if (getLogTermFromLogIndex(index) != getLogTermFromLogIndex(args->prevlogindex()))
      {
        reply->set_updatenextindex(index + 1);
        break;
      }
    }
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    return;
  }
}

/**
 * @brief 日志应用定时器
 *
 * 运行在独立线程中的定时器，负责将已提交但未应用的日志条目应用到状态机。
 * 该机制确保了Raft集群中各节点状态机的一致性。
 *
 * 主要功能：
 * - 定期检查提交索引与应用索引的差异
 * - 按顺序应用日志条目到状态机
 * - 处理配置变更类型的日志条目
 * - 通过应用通道向上层服务发送应用消息
 */
void Raft::applierTicker()
{
  while (true)
  {
    m_mtx.lock();

    // 定期输出状态信息用于调试
    static int debugCount = 0;
    if (++debugCount % 50 == 1 || (m_status.load() == Leader && m_commitIndex > m_lastApplied))
    {
      DPrintf("🔄 [节点%d] 应用器状态: lastApplied=%d, commitIndex=%d, status=%d",
              m_me, m_lastApplied, m_commitIndex, (int)m_status.load());
    }

    // 获取待应用的日志消息列表
    auto applyMsgs = getApplyLogs();
    if (!applyMsgs.empty())
    {
      DPrintf("📋 [节点%d] 准备应用日志: lastApplied=%d, commitIndex=%d, 消息数=%d",
              m_me, m_lastApplied, m_commitIndex, applyMsgs.size());
    }
    m_mtx.unlock();

    // 向上层服务发送应用消息
    if (!applyMsgs.empty())
    {
      DPrintf("📤 [节点%d] 向KV服务器发送 %d 条应用消息", m_me, applyMsgs.size());
    }

    // 通过应用通道发送消息到上层服务
    for (auto &message : applyMsgs)
    {
      auto result = applyChan->send(message);
      if (result != monsoon::ChannelResult::SUCCESS)
      {
        DPrintf("[Raft::applier] 发送ApplyMsg失败，结果: %d", (int)result);
      }
    }

    // 休眠一段时间后继续检查
    sleepNMilliseconds(ApplyInterval);
  }
}

bool Raft::CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot)
{
  return true;
}

/**
 * @brief 执行领导者选举
 *
 * 当选举超时触发时，节点转为候选者状态并发起新一轮选举。
 * 该函数实现了Raft算法中的选举机制，包括：
 * - 增加当前任期号
 * - 转换为候选者状态
 * - 为自己投票
 * - 向其他节点发送投票请求
 * - 处理单节点集群的特殊情况
 */
void Raft::doElection()
{
  std::lock_guard<std::mutex> g(m_mtx);

  // 只有非领导者节点才能发起选举
  if (m_status != Leader)
  {
    DPrintf("🗳️  [节点%d] 选举定时器到期，开始新一轮选举", m_me);

    // 转换为候选者状态并开始新一轮选举
    m_status = Candidate;
    m_currentTerm += 1; // 增加任期号
    m_votedFor = m_me;  // 为自己投票
    persist();          // 持久化状态变更

    // 初始化投票计数器（包含自己的一票）
    std::shared_ptr<int> votedNum = std::make_shared<int>(1);

    // 重置选举定时器
    m_lastResetElectionTime = now();

    // 特殊处理：单节点集群直接成为领导者
    if (m_peers.size() == 1)
    {
      m_status = Leader;
      DPrintf("[doElection-节点%d] 单节点集群，直接成为领导者，任期:%d", m_me, m_currentTerm);

      // 初始化领导者状态
      int lastLogIndex = getLastLogIndex();
      for (int i = 0; i < m_nextIndex.size(); i++)
      {
        m_nextIndex[i] = lastLogIndex + 1;
        m_matchIndex[i] = 0;
      }

      // 开始发送心跳
      if (m_ioManager)
      {
        m_ioManager->scheduler([this]()
                               { this->doHeartBeat(); });
      }
      else
      {
        std::thread t(&Raft::doHeartBeat, this);
        t.detach();
      }
      return; // 直接返回，不需要发送RequestVote
    }

    //	发布RequestVote RPC
    for (int i = 0; i < m_peers.size(); i++)
    {
      if (i == m_me)
      {
        continue;
      }
      int lastLogIndex = -1, lastLogTerm = -1;
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm); // 获取最后一个日志条目的任期和索引

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 异步发送投票请求，优先使用协程提高性能
      if (m_ioManager)
      {
        // 使用协程异步发送RequestVote RPC
        m_ioManager->scheduler([this, i, requestVoteArgs, requestVoteReply, votedNum]()
                               { this->sendRequestVote(i, requestVoteArgs, requestVoteReply, votedNum); });
      }
      else
      {
        // 回退到线程模式（向后兼容）
        std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                      votedNum);
        t.detach();
      }
    }
  }
}

/**
 * @brief 发送心跳消息
 *
 * 领导者定期调用此函数向所有跟随者发送心跳消息（空的AppendEntries RPC）。
 * 心跳机制的主要作用：
 * - 维持领导者地位，防止跟随者发起新选举
 * - 同步日志条目到跟随者节点
 * - 更新跟随者的提交索引
 * - 处理日志复制和快照发送
 *
 * 只有领导者节点才会执行心跳发送逻辑。
 */
void Raft::doHeartBeat()
{
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader)
  {
    // 控制心跳日志输出频率，避免日志过多
    static int heartbeatCount = 0;
    if (++heartbeatCount <= 3 || heartbeatCount % 50 == 0)
    {
      DPrintf("💗 [节点%d-Leader] 发送心跳 #%d", m_me, heartbeatCount);
    }

    // 统计成功响应的节点数量
    auto appendNums = std::make_shared<int>(1);

    // 向所有跟随者节点发送心跳或日志复制请求
    for (int i = 0; i < m_peers.size(); i++)
    {
      if (i == m_me)
      {
        continue;
      }
      myAssert(m_nextIndex[i] >= 1, format("nextIndex[%d] = %d", i, m_nextIndex[i]));
      // 判断是发送快照还是发送日志条目
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex)
      {
        // 跟随者日志落后太多，发送快照进行同步
        if (m_ioManager)
        {
          m_ioManager->scheduler([this, i]()
                                 { this->leaderSendSnapShot(i); });
        }
        else
        {
          std::thread t(&Raft::leaderSendSnapShot, this, i);
          t.detach();
        }
        continue;
      }
      // 构造AppendEntries RPC参数
      int preLogIndex = -1;
      int PrevLogTerm = -1;
      getPrevLogInfo(i, &preLogIndex, &PrevLogTerm);
      std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> appendEntriesArgs =
          std::make_shared<raftRpcProctoc::AppendEntriesArgs>();
      appendEntriesArgs->set_term(m_currentTerm);
      appendEntriesArgs->set_leaderid(m_me);
      appendEntriesArgs->set_prevlogindex(preLogIndex);
      appendEntriesArgs->set_prevlogterm(PrevLogTerm);
      appendEntriesArgs->clear_entries();
      appendEntriesArgs->set_leadercommit(m_commitIndex);
      if (preLogIndex != m_lastSnapshotIncludeIndex)
      {
        for (int j = getSlicesIndexFromLogIndex(preLogIndex) + 1; j < m_logs.size(); ++j)
        {
          raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = m_logs[j]; // 复制日志条目到RPC消息中
        }
      }
      else
      {
        for (const auto &item : m_logs)
        {
          raftRpcProctoc::LogEntry *sendEntryPtr = appendEntriesArgs->add_entries();
          *sendEntryPtr = item; //=是可以点进去的，可以点进去看下protobuf如何重写这个的
        }
      }
      int lastLogIndex = getLastLogIndex();
      // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
      myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == lastLogIndex,
               format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%d} != lastLogIndex{%d}",
                      appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), lastLogIndex));
      // 构造返回值
      const std::shared_ptr<raftRpcProctoc::AppendEntriesReply> appendEntriesReply =
          std::make_shared<raftRpcProctoc::AppendEntriesReply>();
      appendEntriesReply->set_appstate(Disconnected);

      // 使用协程替代线程发送AppendEntries
      if (m_ioManager)
      {
        m_ioManager->scheduler([this, i, appendEntriesArgs, appendEntriesReply, appendNums]()
                               { this->sendAppendEntries(i, appendEntriesArgs, appendEntriesReply, appendNums); });
      }
      else
      {
        std::thread t(&Raft::sendAppendEntries, this, i, appendEntriesArgs, appendEntriesReply,
                      appendNums);
        t.detach();
      }
    }

    // 更新提交索引（特别重要：确保单节点集群的日志能被提交）
    leaderUpdateCommitIndex();

    m_lastResetHearBeatTime = now(); // leader发送心跳，就不是随机时间了
  }
}

/**
 * @brief 选举超时检查器
 *
 * 运行在独立线程中的定时器，负责监控选举超时并触发新的选举。
 * 该函数实现了Raft算法中的选举超时机制：
 * - 对于领导者：进入休眠状态，避免CPU空转
 * - 对于跟随者和候选者：计算合适的睡眠时间
 * - 当选举超时到期时：调用doElection()发起新选举
 *
 * 使用随机化的选举超时时间来避免选举冲突。
 */
void Raft::electionTimeOutTicker()
{
  while (true)
  {
    // 领导者节点休眠，避免CPU空转和协程阻塞
    while (m_status == Leader)
    {
      usleep(HeartBeatTimeout); // 使用心跳间隔作为休眠时间
    }
    // 计算合适的睡眠时间
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }

    // 如果需要睡眠时间大于1毫秒，则进行睡眠
    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
    {
      auto start = std::chrono::steady_clock::now();
      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      auto end = std::chrono::steady_clock::now();

      // 输出睡眠时间统计信息（调试用）
      std::chrono::duration<double, std::milli> duration = end - start;
      std::cout << "\033[1;35m 选举定时器睡眠时间: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()
                << "ms (实际: " << duration.count() << "ms)\033[0m" << std::endl;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0)
    {
      // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    }
    doElection();
  }
}

/**
 * @brief 获取待应用的日志条目
 *
 * 从已提交但未应用的日志中提取消息，准备发送给上层状态机。
 * 该函数确保日志按顺序应用，维护状态机的一致性。
 *
 * 处理两种类型的日志条目：
 * - 普通命令日志：直接转换为ApplyMsg
 * - 配置变更日志：先应用配置变更，再转换为ApplyMsg
 *
 * @return 待应用的消息列表，按日志索引顺序排列
 */
std::vector<ApplyMsg> Raft::getApplyLogs()
{
  std::vector<ApplyMsg> applyMsgs;
  myAssert(m_commitIndex <= getLastLogIndex(), format("[func-getApplyLogs-rf{%d}] commitIndex{%d} >getLastLogIndex{%d}",
                                                      m_me, m_commitIndex, getLastLogIndex()));

  while (m_lastApplied < m_commitIndex)
  {
    m_lastApplied++;
    myAssert(m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex() == m_lastApplied,
             format("rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogIndex{%d} != rf.lastApplied{%d} ",
                    m_logs[getSlicesIndexFromLogIndex(m_lastApplied)].logindex(), m_lastApplied));
    const auto &logEntry = m_logs[getSlicesIndexFromLogIndex(m_lastApplied)];

    // 检查是否为配置变更日志
    if (logEntry.isconfigchange())
    {
      // 应用配置变更
      applyConfigChange(logEntry.configchange());

      // 配置变更日志也需要通知上层应用
      ApplyMsg applyMsg;
      applyMsg.CommandValid = true;
      applyMsg.SnapshotValid = false;
      applyMsg.Command = logEntry.command();
      applyMsg.CommandIndex = m_lastApplied;
      applyMsgs.emplace_back(applyMsg);

      DPrintf("[getApplyLogs] Applied config change at index %d", m_lastApplied);
    }
    else
    {
      // 普通日志条目
      ApplyMsg applyMsg;
      applyMsg.CommandValid = true;
      applyMsg.SnapshotValid = false;
      applyMsg.Command = logEntry.command();
      applyMsg.CommandIndex = m_lastApplied;
      applyMsgs.emplace_back(applyMsg);
    }
  }
  return applyMsgs;
}

/**
 * @brief 获取新命令应该分配的日志索引
 *
 * 计算下一个新日志条目应该使用的逻辑索引。
 * 如果日志为空，则使用快照索引+1；否则使用最后日志索引+1。
 *
 * @return 新命令的日志索引
 */
int Raft::getNewCommandIndex()
{
  auto lastLogIndex = getLastLogIndex();
  return lastLogIndex + 1;
}

// getPrevLogInfo
// leader调用，传入：服务器index，传出：发送的AE的preLogIndex和PrevLogTerm
void Raft::getPrevLogInfo(int server, int *preIndex, int *preTerm)
{
  // logs长度为0返回0,0，不是0就根据nextIndex数组的数值返回
  if (m_nextIndex[server] == m_lastSnapshotIncludeIndex + 1)
  {
    // 要发送的日志是第一个日志，因此直接返回m_lastSnapshotIncludeIndex和m_lastSnapshotIncludeTerm
    *preIndex = m_lastSnapshotIncludeIndex;
    *preTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  auto nextIndex = m_nextIndex[server];
  *preIndex = nextIndex - 1;
  *preTerm = m_logs[getSlicesIndexFromLogIndex(*preIndex)].logterm();
}

/**
 * @brief 获取当前节点状态
 *
 * 返回当前节点的任期号和领导者状态。
 * 这是一个线程安全的只读操作，使用读写锁优化并发性能。
 *
 * @param term 输出参数，返回当前任期号
 * @param isLeader 输出参数，返回是否为领导者
 */
void Raft::GetState(int *term, bool *isLeader)
{
  // 使用读锁，因为这是读多写少的场景
  monsoon::RWMutex::ReadLock lock(m_stateMutex);

  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

/**
 * @brief 处理快照安装RPC请求
 *
 * 当跟随者的日志落后太多时，领导者会发送快照来快速同步状态。
 * 该函数处理快照安装请求，包括：
 * - 验证领导者任期的有效性
 * - 检查快照是否比当前快照更新
 * - 截断过时的日志条目
 * - 更新快照相关的状态信息
 * - 将快照数据发送给上层应用
 *
 * @param args 快照安装请求参数，包含快照数据和元信息
 * @param reply 快照安装响应结果
 */
void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply)
{
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm)
  {
    reply->set_term(m_currentTerm);
    return;
  }
  if (args->term() > m_currentTerm)
  {
    // 后面两种情况都要接收日志
    m_currentTerm = args->term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
  }
  m_status = Follower;
  m_lastResetElectionTime = now();
  // outdated snapshot
  if (args->lastsnapshotincludeindex() <= m_lastSnapshotIncludeIndex)
  {
    return;
  }
  // 截断日志，修改commitIndex和lastApplied
  // 截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
  // 但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
  auto lastLogIndex = getLastLogIndex();

  if (lastLogIndex > args->lastsnapshotincludeindex())
  {
    m_logs.erase(m_logs.begin(), m_logs.begin() + getSlicesIndexFromLogIndex(args->lastsnapshotincludeindex()) + 1);
  }
  else
  {
    m_logs.clear();
  }
  m_commitIndex = std::max(m_commitIndex, args->lastsnapshotincludeindex());
  m_lastApplied = std::max(m_lastApplied, args->lastsnapshotincludeindex());
  m_lastSnapshotIncludeIndex = args->lastsnapshotincludeindex();
  m_lastSnapshotIncludeTerm = args->lastsnapshotincludeterm();

  reply->set_term(m_currentTerm);
  ApplyMsg msg;
  msg.SnapshotValid = true;
  msg.Snapshot = args->data();
  msg.SnapshotTerm = args->lastsnapshotincludeterm();
  msg.SnapshotIndex = args->lastsnapshotincludeindex();

  // 异步发送快照到上层应用
  std::thread t(&Raft::pushMsgToKvServer, this, msg);
  t.detach();
  // 持久化快照和状态
  m_persister->Save(persistData(), args->data());
}

/**
 * @brief 向上层服务发送应用消息
 *
 * 通过应用通道将日志条目或快照发送给上层的KV服务器。
 * 该函数通常在独立线程中调用，避免阻塞Raft核心逻辑。
 *
 * @param msg 要发送的应用消息（日志条目或快照）
 */
void Raft::pushMsgToKvServer(ApplyMsg msg)
{
  auto result = applyChan->send(msg);
  if (result != monsoon::ChannelResult::SUCCESS)
  {
    DPrintf("[pushMsgToKvServer] 发送ApplyMsg失败，结果: %d", (int)result);
  }
}

/**
 * @brief 领导者心跳定时器
 *
 * 运行在独立线程中的定时器，负责领导者的心跳发送调度。
 * 该函数的主要功能：
 * - 等待节点成为领导者
 * - 按照心跳间隔定期调用doHeartBeat()
 * - 维持领导者地位，防止跟随者超时
 * - 确保日志复制的及时性
 *
 * 只有领导者节点才会执行心跳发送，非领导者节点会进入等待状态。
 */
void Raft::leaderHearBeatTicker()
{
  while (true)
  {
    // 等待成为领导者，非领导者节点休眠以避免无效操作
    int waitCount = 0;
    while (m_status.load() != Leader)
    {
      if (++waitCount % 10 == 1)
      {
        DPrintf("⏳ [节点%d] 心跳定时器等待成为Leader (当前状态:%d)", m_me, (int)m_status.load());
      }
      usleep(1000 * HeartBeatTimeout);
    }

    // 成为领导者后开始心跳调度
    DPrintf("👑 [节点%d] 心跳定时器检测到成为Leader，开始发送心跳", m_me);
    static std::atomic<int32_t> atomicCount = 0;

    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      std::lock_guard<std::mutex> lock(m_mtx);
      wakeTime = now();
      suitableSleepTime = std::chrono::milliseconds(HeartBeatTimeout) + m_lastResetHearBeatTime - wakeTime;
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
    {
      // 减少心跳日志的输出频率，但在开始时多输出一些
      if (atomicCount <= 3 || atomicCount % 20 == 0)
      {
        std::cout << "💓 [节点" << m_me << "-Leader] 心跳定时器 #" << atomicCount
                  << " 睡眠: " << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()
                  << "ms" << std::endl;
      }
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 只在开始时和每20次心跳时输出实际睡眠时间
      if (atomicCount <= 3 || atomicCount % 20 == 0)
      {
        std::cout << "⏰ [节点" << m_me << "-Leader] 心跳定时器 #" << atomicCount
                  << " 实际睡眠: " << std::fixed << std::setprecision(1) << duration.count()
                  << "ms" << std::endl;
      }
      ++atomicCount;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetHearBeatTime - wakeTime).count() > 0)
    {
      // 睡眠的这段时间有重置定时器，没有超时，再次睡眠
      continue;
    }
    doHeartBeat();
  }
}

void Raft::leaderSendSnapShot(int server)
{
  m_mtx.lock();
  raftRpcProctoc::InstallSnapshotRequest args;
  args.set_leaderid(m_me);
  args.set_term(m_currentTerm);
  args.set_lastsnapshotincludeindex(m_lastSnapshotIncludeIndex);
  args.set_lastsnapshotincludeterm(m_lastSnapshotIncludeTerm);
  args.set_data(m_persister->ReadSnapshot());

  raftRpcProctoc::InstallSnapshotResponse reply;
  m_mtx.unlock();
  bool ok = m_peers[server]->InstallSnapshot(&args, &reply);
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (!ok)
  {
    return;
  }
  if (m_status != Leader || m_currentTerm != args.term())
  {
    return; // 中间释放过锁，可能状态已经改变了
  }
  //	无论什么时候都要判断term
  if (reply.term() > m_currentTerm)
  {
    // 三变
    m_currentTerm = reply.term();
    m_votedFor = -1;
    m_status = Follower;
    persist();
    m_lastResetElectionTime = now();
    return;
  }
  m_matchIndex[server] = args.lastsnapshotincludeindex();
  m_nextIndex[server] = m_matchIndex[server] + 1;
}

/**
 * @brief 领导者更新提交索引
 *
 * 根据多数节点的日志复制情况更新提交索引。只有当前任期的日志
 * 被多数节点复制后才能提交，这保证了Raft的安全性。
 *
 * 特殊处理单节点集群的情况，直接提交所有日志。
 */
void Raft::leaderUpdateCommitIndex()
{
  // 特殊处理：单节点集群直接提交所有日志
  if (m_peers.size() == 1)
  {
    int lastIndex = getLastLogIndex();
    if (lastIndex > m_commitIndex)
    {
      m_commitIndex = lastIndex;
      DPrintf("[单节点集群] 直接提交所有日志，commitIndex更新为: %d", m_commitIndex);
    }
    return;
  }

  // 多节点集群：寻找可以安全提交的最高索引
  m_commitIndex = m_lastSnapshotIncludeIndex;
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--)
  {
    int sum = 0;
    // 统计已复制该索引的节点数量
    for (int i = 0; i < m_peers.size(); i++)
    {
      if (i == m_me)
      {
        sum += 1; // 领导者自己总是拥有所有日志
        continue;
      }
      if (m_matchIndex[i] >= index)
      {
        sum += 1; // 该跟随者已复制到此索引
      }
    }

    // 只有当前任期的日志被多数节点复制后才能提交
    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm)
    {
      m_commitIndex = index;
      break;
    }
  }
}

/**
 * @brief 检查日志条目是否匹配
 *
 * 验证指定索引位置的日志条目任期是否与给定任期匹配。
 * 这是Raft算法中日志一致性检查的核心函数。
 *
 * @param logIndex 要检查的日志索引（必须在有效范围内）
 * @param logTerm 期望的日志任期
 * @return true表示匹配，false表示不匹配
 *
 * @pre logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex()
 */
bool Raft::matchLog(int logIndex, int logTerm)
{
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
}

/**
 * @brief 持久化Raft状态
 *
 * 将当前的Raft状态（任期、投票记录、日志等）持久化到存储中。
 * 这是Raft算法正确性的关键要求，确保节点重启后能恢复状态。
 *
 * 持久化的数据包括：
 * - 当前任期号
 * - 投票记录
 * - 日志条目
 * - 快照相关信息
 */
void Raft::persist()
{
  auto data = persistData();

  // 获取持久化前的状态大小
  long long oldSize = m_persister->RaftStateSize();

  m_persister->SaveRaftState(data);

  // 计算状态大小变化并通知回调
  if (m_stateSizeChangeCallback)
  {
    long long newSize = m_persister->RaftStateSize();
    long long deltaSize = newSize - oldSize;
    if (deltaSize != 0)
    {
      m_stateSizeChangeCallback(deltaSize);
    }
  }
}

void Raft::startElectionTimer()
{
  if (m_ioManager)
  {
    std::cout << "node" << m_me << " 启动选举定时器" << std::endl;
    m_ioManager->scheduler([this]() -> void
                           { this->electionTimeOutTicker(); });
  }
}

void Raft::SetStateSizeChangeCallback(std::function<void(long long)> callback)
{
  m_stateSizeChangeCallback = callback;
}

/**
 * @brief 处理投票请求RPC
 *
 * 实现Raft算法中的RequestVote RPC处理逻辑。当候选者发起选举时，
 * 其他节点通过此函数决定是否投票给该候选者。
 *
 * 投票决策基于以下条件：
 * - 候选者任期不能小于当前任期
 * - 当前任期内尚未投票或已投票给该候选者
 * - 候选者的日志至少与当前节点一样新
 *
 * @param args 投票请求参数，包含候选者任期、ID、最后日志信息
 * @param reply 投票响应结果，包含是否投票、当前任期等信息
 */
void Raft::RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 处理连接测试请求
  if (args->term() == -1 && args->candidateid() == -1)
  {
    // 这是一个连接测试请求，直接返回成功
    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(false); // 不投票，只是确认连接
    DPrintf("[RequestVote-节点%d] 收到连接测试请求", m_me);
    return; // 不需要持久化
  }

  // 添加调试信息
  DPrintf("🗳️ [节点%d] 收到来自节点%d的投票请求 (任期:%d)", m_me, args->candidateid(), args->term());

  // 投票决策需要考虑任期和日志新旧程度
  bool needPersist = false;

  // 处理候选者任期过时的情况
  if (args->term() < m_currentTerm)
  {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    DPrintf("❌ [节点%d] 拒绝投票给节点%d：任期过时 (%d < %d)", m_me, args->candidateid(), args->term(), m_currentTerm);
    // 状态未改变，无需持久化
    return;
  }
  // 发现更高任期：更新状态并转为跟随者
  if (args->term() > m_currentTerm)
  {
    DPrintf("📈 [节点%d] 更新任期：%d -> %d，变成Follower", m_me, m_currentTerm, args->term());
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;    // 重置投票记录
    needPersist = true; // 状态改变，需要持久化
  }
  myAssert(args->term() == m_currentTerm,
           format("[RequestVote-节点%d] 任期一致性检查失败", m_me));

  // 现在任期相同，需要检查日志新旧程度
  int lastLogTerm = getLastLogTerm();
  // 只有在未投票且候选者日志足够新的情况下才投票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm()))
  {
    if (args->lastlogterm() < lastLogTerm)
    {
    }

    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    DPrintf("❌ [节点%d] 拒绝投票给节点%d：日志不够新", m_me, args->candidateid());

    if (needPersist)
    {
      persist();
    }
    return;
  }

  //     当因为网络质量不好导致的请求丢失重发就有可能！！！！
  if (m_votedFor != -1 && m_votedFor != args->candidateid())
  {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Voted);
    reply->set_votegranted(false);
    DPrintf("❌ [节点%d] 拒绝投票给节点%d：已经投票给节点%d", m_me, args->candidateid(), m_votedFor);

    if (needPersist)
    {
      persist();
    }
    return;
  }
  else
  {
    m_votedFor = args->candidateid();
    m_lastResetElectionTime = now(); // 投票后重置选举定时器
    needPersist = true;              // 投票状态改变，需要持久化

    reply->set_term(m_currentTerm);
    reply->set_votestate(Normal);
    reply->set_votegranted(true);
    DPrintf("✅ [节点%d] 投票给节点%d (任期:%d)", m_me, args->candidateid(), m_currentTerm);

    if (needPersist)
    {
      persist();
    }
    return;
  }
}

/**
 * @brief 检查候选者日志是否足够新
 *
 * 根据Raft算法的日志匹配原则，判断候选者的日志是否至少与当前节点一样新。
 * 日志新旧程度的判断标准：
 * 1. 任期更高的日志更新
 * 2. 任期相同时，索引更高的日志更新
 *
 * @param index 候选者最后日志条目的索引
 * @param term 候选者最后日志条目的任期
 * @return true表示候选者日志足够新，可以投票
 */
bool Raft::UpToDate(int index, int term)
{
  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

/**
 * @brief 获取最后一个日志条目的索引和任期
 *
 * 如果日志为空，则返回快照中包含的最后一个日志条目的信息。
 *
 * @param lastLogIndex 输出参数，返回最后日志条目的索引
 * @param lastLogTerm 输出参数，返回最后日志条目的任期
 */
void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
  if (m_logs.empty())
  {
    // 日志为空，使用快照信息
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  else
  {
    // 返回最后一个日志条目的信息
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 * @brief 获取最后一个日志条目的索引
 *
 * 返回日志中最后一个条目的逻辑索引。如果日志为空，
 * 则返回快照中包含的最后一个日志索引。
 *
 * @return 最后一个日志条目的逻辑索引
 * @note 这是逻辑索引，不是在m_logs数组中的物理索引
 */
int Raft::getLastLogIndex()
{
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

/**
 * @brief 获取最后一个日志条目的任期号
 *
 * 返回日志中最后一个条目的任期。如果日志为空，
 * 则返回快照中包含的最后一个日志任期。
 *
 * @return 最后一个日志条目的任期号
 */
int Raft::getLastLogTerm()
{
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 * @brief 根据日志索引获取对应的任期号
 *
 * 支持快照压缩后的日志任期查询。如果索引正好是快照的最后一个索引，
 * 则返回快照的任期；否则从日志数组中查找。
 *
 * @param logIndex 要查询的逻辑日志索引
 * @return 对应的任期号
 */
int Raft::getLogTermFromLogIndex(int logIndex)
{
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[getLogTermFromLogIndex-节点%d] 索引%d < 快照索引%d", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[getLogTermFromLogIndex-节点%d] 索引%d > 最后索引%d",
                                            m_me, logIndex, lastLogIndex));

  if (logIndex == m_lastSnapshotIncludeIndex)
  {
    return m_lastSnapshotIncludeTerm;
  }
  else
  {
    return m_logs[getSlicesIndexFromLogIndex(logIndex)].logterm();
  }
}

/**
 * @brief 获取Raft状态数据的大小
 *
 * 返回当前持久化的Raft状态数据的字节大小。
 * 用于监控和调试目的。
 *
 * @return 状态数据的字节大小
 */
int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

/**
 * @brief 将逻辑日志索引转换为数组物理索引
 *
 * 由于日志压缩的存在，逻辑索引和数组索引不一致。
 * 该函数将逻辑日志索引转换为m_logs数组中的实际位置。
 *
 * @param logIndex 逻辑日志索引
 * @return 在m_logs数组中的物理索引
 *
 * @pre logIndex > m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex()
 */
int Raft::getSlicesIndexFromLogIndex(int logIndex)
{
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[getSlicesIndexFromLogIndex-节点%d] 索引%d <= 快照索引%d", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[getSlicesIndexFromLogIndex-节点%d] 索引%d > 最后索引%d",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

/**
 * @brief 发送投票请求RPC
 *
 * 向指定节点发送RequestVote RPC请求，并处理响应结果。
 * 该函数在选举过程中被异步调用，用于收集其他节点的投票。
 *
 * @param server 目标节点索引
 * @param args 投票请求参数
 * @param reply 投票响应结果
 * @param votedNum 共享的投票计数器
 * @return true表示RPC调用成功，false表示网络通信失败
 */
bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
{
  auto start = now();
  DPrintf("[RequestVote-节点%d] 向节点%d发送投票请求", m_me, server);
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[RequestVote-节点%d] 向节点%d发送完毕，耗时%ld ms", m_me, server,
          std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count());

  if (!ok)
  {
  }
  // 处理投票响应，检查任期变化
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm)
  {
    // 发现更高任期，立即转为跟随者
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  }
  else if (reply->term() < m_currentTerm)
  {
    // 过期响应，忽略
    return true;
  }
  myAssert(reply->term() == m_currentTerm, "投票响应任期不一致");

  if (!reply->votegranted())
  {
    // 未获得投票，直接返回
    return true;
  }

  // 获得投票，增加计数器
  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1)
  {
    // 获得多数票，成为领导者
    *votedNum = 0;
    if (m_status == Leader)
    {
      // 同一任期内不应该有两个领导者
      myAssert(false,
               format("[RequestVote-节点%d] 任期%d重复当选领导者错误", m_me, m_currentTerm));
    }
    // 首次成为领导者，初始化相关状态
    m_status = Leader;

    DPrintf("🎉 [节点%d] 选举成功！成为Leader (任期:%d, 最后日志索引:%d)", m_me, m_currentTerm, getLastLogIndex());
    DPrintf("👑 [节点%d] 状态已设置为Leader (m_status=%d)", m_me, (int)m_status);
    DPrintf("🔍 [节点%d] Leader状态检查: lastApplied=%d, commitIndex=%d", m_me, m_lastApplied, m_commitIndex);

    int lastLogIndex = getLastLogIndex();
    // 初始化领导者状态：nextIndex和matchIndex数组
    for (int i = 0; i < m_nextIndex.size(); i++)
    {
      m_nextIndex[i] = lastLogIndex + 1; // 初始化为最后日志索引+1
      m_matchIndex[i] = 0;               // 初始化为0，表示尚未匹配任何日志
    }
    // 立即发送心跳宣告领导者地位
    if (m_ioManager)
    {
      m_ioManager->scheduler([this]()
                             { this->doHeartBeat(); });
    }
    else
    {
      std::thread t(&Raft::doHeartBeat, this);
      t.detach();
    }

    persist();
  }
  return true;
}

/**
 * @brief 发送日志追加RPC请求
 *
 * 向指定节点发送AppendEntries RPC，用于日志复制或心跳。
 * 该函数处理RPC响应并更新相关状态。
 *
 * @param server 目标节点索引
 * @param args 日志追加请求参数
 * @param reply 日志追加响应结果
 * @param appendNums 共享的成功响应计数器
 * @return true表示网络通信成功，false表示网络故障
 */
bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums)
{
  // ok表示网络通信是否成功，不是RPC逻辑结果
  DPrintf("[sendAppendEntries-节点%d] 向节点%d发送AE RPC，条目数:%d", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok)
  {
    DPrintf("[sendAppendEntries-节点%d] 向节点%d发送AE RPC失败", m_me, server);
    return ok;
  }
  DPrintf("[sendAppendEntries-节点%d] 向节点%d发送AE RPC成功", m_me, server);
  if (reply->appstate() == Disconnected)
  {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 处理AppendEntries响应，检查任期变化
  if (reply->term() > m_currentTerm)
  {
    // 发现更高任期，立即转为跟随者
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  }
  else if (reply->term() < m_currentTerm)
  {
    // 过期响应，忽略
    DPrintf("[sendAppendEntries-节点%d] 节点%d任期%d < 当前任期%d，忽略响应",
            m_me, server, reply->term(), m_currentTerm);
    return ok;
  }

  if (m_status != Leader)
  {
    // 非领导者节点不处理AppendEntries响应
    return ok;
  }

  myAssert(reply->term() == m_currentTerm,
           format("响应任期%d != 当前任期%d", reply->term(), m_currentTerm));
  if (!reply->success())
  {
    // 日志不匹配，需要回退nextIndex
    if (reply->updatenextindex() != -100)
    {
      // 使用跟随者建议的nextIndex进行快速回退
      DPrintf("[sendAppendEntries-节点%d] 日志不匹配，回退nextIndex[%d]到%d",
              m_me, server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex(); // 失败时不更新matchIndex
    }
  }
  else
  {
    // 日志复制成功
    *appendNums = *appendNums + 1;
    DPrintf("[sendAppendEntries-节点%d] 节点%d复制成功，当前成功数%d", m_me, server, *appendNums);

    // 更新matchIndex和nextIndex
    // 使用max确保matchIndex只能增长，避免重复消息导致的问题
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("nextIndex[%d]=%d > lastLogIndex+1=%d，日志数组长度=%d",
                    server, m_nextIndex[server], lastLogIndex + 1, m_logs.size()));
    if (*appendNums >= 1 + m_peers.size() / 2)
    {
      // 获得多数节点确认，可以提交日志

      *appendNums = 0;
      // 更新领导者的提交索引
      // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”

      leaderUpdateCommitIndex();
      if (args->entries_size() > 0)
      {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm)
      {
        DPrintf("[sendAppendEntries-节点%d] 当前任期日志成功提交，更新commitIndex: %d -> %d",
                m_me, m_commitIndex, args->prevlogindex() + args->entries_size());

        int oldCommitIndex = m_commitIndex;
        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());

        if (m_commitIndex > oldCommitIndex)
        {
          DPrintf("📈 [节点%d-Leader] commitIndex更新: %d -> %d, lastApplied=%d",
                  m_me, oldCommitIndex, m_commitIndex, m_lastApplied);
        }
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[sendAppendEntries-节点%d] 提交索引验证: lastLogIndex=%d, commitIndex=%d",
                      m_me, lastLogIndex, m_commitIndex));
    }
  }
  return ok;
}

/**
 * @brief AppendEntries RPC的protobuf接口包装
 *
 * 这是protobuf生成的RPC接口的实现，负责调用实际的处理函数并完成RPC调用。
 *
 * @param controller RPC控制器（未使用）
 * @param request 日志追加请求参数
 * @param response 日志追加响应结果
 * @param done RPC完成回调
 */
void Raft::AppendEntries(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::AppendEntriesArgs *request,
                         ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done)
{
  AppendEntries1(request, response);
  done->Run();
}

/**
 * @brief InstallSnapshot RPC的protobuf接口包装
 *
 * 这是protobuf生成的RPC接口的实现，负责调用实际的处理函数并完成RPC调用。
 *
 * @param controller RPC控制器（未使用）
 * @param request 快照安装请求参数
 * @param response 快照安装响应结果
 * @param done RPC完成回调
 */
void Raft::InstallSnapshot(google::protobuf::RpcController *controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest *request,
                           ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done)
{
  InstallSnapshot(request, response);
  done->Run();
}

/**
 * @brief RequestVote RPC的protobuf接口包装
 *
 * 这是protobuf生成的RPC接口的实现，负责调用实际的处理函数并完成RPC调用。
 *
 * @param controller RPC控制器（未使用）
 * @param request 投票请求参数
 * @param response 投票响应结果
 * @param done RPC完成回调
 */
void Raft::RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                       ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done)
{
  RequestVote(request, response);
  done->Run();
}

/**
 * @brief 处理集群配置变更请求
 *
 * 处理集群成员的添加或删除请求。只有领导者可以处理配置变更，
 * 配置变更通过特殊的日志条目进行复制和提交。
 *
 * @param controller RPC控制器（未使用）
 * @param request 配置变更请求参数
 * @param response 配置变更响应结果
 * @param done RPC完成回调
 */
void Raft::ChangeConfig(google::protobuf::RpcController *controller,
                        const ::raftRpcProctoc::ChangeConfigArgs *request,
                        ::raftRpcProctoc::ChangeConfigReply *response, ::google::protobuf::Closure *done)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  // 只有领导者可以处理成员变更请求
  if (m_status != Leader)
  {
    response->set_success(false);
    response->set_isleader(false);
    response->set_error("非领导者节点");
    done->Run();
    return;
  }

  response->set_isleader(true);

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      request->type(), request->nodeid(), request->address());

  // 将配置变更作为日志条目添加到日志中
  m_logs.emplace_back(configEntry);

  // 立即开始复制配置变更日志到其他节点
  if (m_ioManager)
  {
    m_ioManager->scheduler([this]()
                           { this->doHeartBeat(); });
  }
  else
  {
    std::thread t(&Raft::doHeartBeat, this);
    t.detach();
  }

  response->set_success(true);
  response->set_error("");

  DPrintf("[配置变更-节点%d] 处理配置变更: 类型=%d, 节点ID=%s, 地址=%s",
          m_me, request->type(), request->nodeid().c_str(), request->address().c_str());

  done->Run();
}

/**
 * @brief 开始处理客户端命令
 *
 * 接收来自客户端的命令，如果当前节点是领导者，则将命令添加到日志中。
 * 这是Raft对外提供服务的主要接口。
 *
 * @param command 客户端提交的命令
 * @param newLogIndex 输出参数，返回新日志条目的索引
 * @param newLogTerm 输出参数，返回新日志条目的任期
 * @param isLeader 输出参数，返回当前节点是否为领导者
 */
void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
  std::lock_guard<std::mutex> lg1(m_mtx);
  if (m_status != Leader)
  {
    DPrintf("[Start-节点%d] 非领导者节点，拒绝处理客户端命令", m_me);
    *newLogIndex = -1;
    *newLogTerm = -1;
    *isLeader = false;
    return;
  }

  raftRpcProctoc::LogEntry newLogEntry;
  newLogEntry.set_command(command.asString());
  newLogEntry.set_logterm(m_currentTerm);
  newLogEntry.set_logindex(getNewCommandIndex());
  m_logs.emplace_back(newLogEntry);

  int lastLogIndex = getLastLogIndex();

  // 领导者通过心跳机制将新日志同步到跟随者节点
  DPrintf("[Start-节点%d] 添加新日志条目: index=%d, command=%s", m_me, lastLogIndex, command.asString().c_str());
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

/**
 * @brief 初始化Raft节点
 *
 * 创建并初始化一个Raft服务器实例。该函数设置节点的基本状态，
 * 启动必要的后台线程，并从持久化存储中恢复状态。
 *
 * @param peers 集群中所有节点的RPC客户端列表
 * @param me 当前节点在peers数组中的索引
 * @param persister 持久化存储接口
 * @param applyCh 向上层应用发送消息的通道
 *
 * @note 该函数必须快速返回，长时间运行的任务在后台线程中执行
 */
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                monsoon::Channel<ApplyMsg>::ptr applyCh)
{
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  // 初始化基本状态
  m_mtx.lock();

  // 设置应用通道
  this->applyChan = applyCh;
  m_currentTerm = 0;
  m_status = Follower;
  m_commitIndex = 0;
  m_lastApplied = 0;
  m_logs.clear();
  for (int i = 0; i < m_peers.size(); i++)
  {
    m_matchIndex.push_back(0);
    m_nextIndex.push_back(0);
  }
  m_votedFor = -1;

  m_lastSnapshotIncludeIndex = 0;
  m_lastSnapshotIncludeTerm = 0;

  // 为了避免选举时序冲突，给每个节点添加不同的初始延迟
  // 节点ID越大，延迟越长，这样可以错开选举时间
  auto baseTime = now();
  auto additionalDelay = std::chrono::milliseconds(me * 200); // 每个节点延迟200ms * 节点ID
  m_lastResetElectionTime = baseTime + additionalDelay;
  m_lastResetHearBeatTime = baseTime;

  // 从持久化存储中恢复状态
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0)
  {
    m_lastApplied = m_lastSnapshotIncludeIndex;
  }

  DPrintf("[初始化] 节点%d启动完成，任期%d，快照索引%d，快照任期%d", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();

  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // 启动后台定时器协程
  // 心跳定时器：负责领导者发送心跳
  m_ioManager->scheduler([this]() -> void
                         { this->leaderHearBeatTicker(); });
  // 选举定时器通过startElectionTimer()方法手动启动
  std::thread t3(&Raft::applierTicker, this);
  t3.detach();
}

/**
 * @brief 序列化Raft状态数据
 *
 * 将需要持久化的Raft状态序列化为字符串格式，包括：
 * - 当前任期号
 * - 投票记录
 * - 日志条目
 * - 快照相关信息
 *
 * @return 序列化后的状态数据字符串
 */
std::string Raft::persistData()
{
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;

  // 序列化日志条目
  for (auto &item : m_logs)
  {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

/**
 * @brief 从持久化数据恢复Raft状态
 *
 * 反序列化持久化的状态数据，恢复节点的关键状态信息。
 * 如果数据为空，则保持默认初始状态。
 *
 * @param data 序列化的状态数据字符串
 */
void Raft::readPersist(std::string data)
{
  if (data.empty())
  {
    return;
  }

  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  // 恢复基本状态
  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;

  // 恢复日志条目
  m_logs.clear();
  for (auto &item : boostPersistRaftNode.m_logs)
  {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

/**
 * @brief 创建快照并压缩日志
 *
 * 将指定索引之前的日志条目压缩为快照，释放内存空间。
 * 只有已提交的日志条目才能被压缩为快照。
 *
 * @param index 快照包含的最后一个日志条目索引
 * @param snapshot 快照数据
 */
void Raft::Snapshot(int index, std::string snapshot)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 检查快照索引的有效性
  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
  {
    DPrintf("[Snapshot-节点%d] 拒绝快照请求，索引%d无效（当前快照索引%d，提交索引%d）",
            m_me, index, m_lastSnapshotIncludeIndex, m_commitIndex);
    return;
  }

  auto lastLogIndex = getLastLogIndex(); // 用于验证日志截断的正确性

  // 计算快照后保留的日志条目
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;

  // 保留快照索引之后的所有日志条目
  for (int i = index + 1; i <= getLastLogIndex(); i++)
  {
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  m_persister->Save(persistData(), snapshot);

  DPrintf("[快照-节点%d] 创建快照完成: 索引=%d, 任期=%d, 剩余日志数=%d",
          m_me, index, m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("日志验证失败: 日志数%d + 快照索引%d != 最后索引%d",
                  m_logs.size(), m_lastSnapshotIncludeIndex, lastLogIndex));
}

/**
 * @brief 创建流式快照并压缩日志
 *
 * 类似于Snapshot函数，但使用文件路径而不是内存数据。
 * 适用于大型快照的流式传输场景。
 *
 * @param index 快照包含的最后一个日志条目索引
 * @param snapshotFilePath 快照文件路径
 */
void Raft::StreamingSnapshot(int index, const std::string &snapshotFilePath)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 检查快照索引的有效性
  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
  {
    DPrintf("[StreamingSnapshot-节点%d] 拒绝快照请求，索引%d无效（当前快照索引%d）",
            m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }

  auto lastLogIndex = getLastLogIndex(); // 用于验证日志截断的正确性

  // 计算快照后保留的日志条目
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // 保留快照索引之后的所有日志条目
  for (int i = index + 1; i <= getLastLogIndex(); i++)
  {
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // 保存流式快照
  if (m_persister->SaveStreamingSnapshot(snapshotFilePath))
  {
    // 持久化Raft状态
    m_persister->SaveRaftState(persistData());

    DPrintf("[流式快照-节点%d] 创建完成: 索引=%d, 任期=%d, 剩余日志数=%d",
            m_me, index, m_lastSnapshotIncludeTerm, m_logs.size());
  }
  else
  {
    DPrintf("[流式快照-节点%d] 保存快照文件失败", m_me);
  }

  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("日志验证失败: 日志数%d + 快照索引%d != 最后索引%d",
                  m_logs.size(), m_lastSnapshotIncludeIndex, lastLogIndex));
}

// ==================== 集群成员变更相关方法实现 ====================

/**
 * @brief 添加新节点到集群
 *
 * 只有领导者可以执行节点添加操作。该函数创建配置变更日志条目，
 * 通过Raft共识机制将新节点信息复制到集群中的所有节点。
 *
 * @param nodeId 新节点的唯一标识符
 * @param address 新节点的网络地址
 * @return true表示成功发起添加操作，false表示失败
 */
bool Raft::AddNode(const std::string &nodeId, const std::string &address)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  if (m_status != Leader)
  {
    DPrintf("[添加节点] 节点%d非领导者，无法添加节点%s", m_me, nodeId.c_str());
    return false;
  }

  // 检查节点是否已存在
  if (m_nodeAddresses.find(nodeId) != m_nodeAddresses.end())
  {
    DPrintf("[添加节点] 节点%s已存在", nodeId.c_str());
    return false;
  }

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      raftRpcProctoc::ADD_NODE, nodeId, address);

  // 添加到日志中
  m_logs.emplace_back(configEntry);

  DPrintf("[添加节点] 领导者%d已创建添加节点%s的配置变更日志，地址%s",
          m_me, nodeId.c_str(), address.c_str());

  return true;
}

/**
 * @brief 从集群中移除节点
 *
 * 只有领导者可以执行节点移除操作。该函数创建配置变更日志条目，
 * 通过Raft共识机制将节点移除信息复制到集群中的所有节点。
 *
 * @param nodeId 要移除的节点标识符
 * @return true表示成功发起移除操作，false表示失败
 */
bool Raft::RemoveNode(const std::string &nodeId)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  if (m_status != Leader)
  {
    DPrintf("[移除节点] 节点%d非领导者，无法移除节点%s", m_me, nodeId.c_str());
    return false;
  }

  // 检查节点是否存在
  if (m_nodeAddresses.find(nodeId) == m_nodeAddresses.end())
  {
    DPrintf("[移除节点] 节点%s不存在", nodeId.c_str());
    return false;
  }

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      raftRpcProctoc::REMOVE_NODE, nodeId, "");

  // 添加到日志中
  m_logs.emplace_back(configEntry);

  DPrintf("[移除节点] 领导者%d已创建移除节点%s的配置变更日志",
          m_me, nodeId.c_str());

  return true;
}

/**
 * @brief 创建配置变更日志条目
 *
 * 构造一个特殊的日志条目来记录集群配置变更操作。
 * 该日志条目会通过正常的Raft复制机制传播到所有节点。
 *
 * @param type 配置变更类型（添加或移除节点）
 * @param nodeId 目标节点的标识符
 * @param address 节点地址（移除操作时可为空）
 * @return 配置变更日志条目
 */
raftRpcProctoc::LogEntry Raft::createConfigChangeEntry(raftRpcProctoc::ConfigChangeType type,
                                                       const std::string &nodeId,
                                                       const std::string &address)
{
  raftRpcProctoc::LogEntry entry;
  entry.set_logterm(m_currentTerm);
  entry.set_logindex(getNewCommandIndex());
  entry.set_isconfigchange(true);

  // 设置配置变更内容
  raftRpcProctoc::ConfigChange *configChange = entry.mutable_configchange();
  configChange->set_type(type);
  configChange->set_nodeid(nodeId);
  configChange->set_address(address);

  // 将配置变更序列化为命令
  std::string configData;
  configChange->SerializeToString(&configData);
  entry.set_command(configData);

  return entry;
}

/**
 * @brief 应用配置变更到本地状态
 *
 * 当配置变更日志条目被提交后，调用此函数实际执行配置变更操作。
 * 包括更新节点列表、RPC连接、索引映射等。
 *
 * @param configChange 配置变更信息
 */
void Raft::applyConfigChange(const raftRpcProctoc::ConfigChange &configChange)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  const std::string &nodeId = configChange.nodeid();
  const std::string &address = configChange.address();

  if (configChange.type() == raftRpcProctoc::ADD_NODE)
  {
    // 添加新节点到集群
    if (m_nodeAddresses.find(nodeId) == m_nodeAddresses.end())
    {
      m_nodeAddresses[nodeId] = address;

      // 创建新的RPC连接
      auto newPeer = std::make_shared<RaftRpcUtil>(address.substr(0, address.find(':')),
                                                   std::stoi(address.substr(address.find(':') + 1)));
      m_peers.push_back(newPeer);

      // 更新索引映射关系
      int newIndex = m_peers.size() - 1;
      m_indexToNodeId[newIndex] = nodeId;
      m_nodeIdToIndex[nodeId] = newIndex;

      // 为新节点初始化日志复制状态
      m_nextIndex.push_back(getLastLogIndex() + 1);
      m_matchIndex.push_back(0);

      DPrintf("[应用配置变更] 添加节点%s，地址%s，集群大小:%d",
              nodeId.c_str(), address.c_str(), m_peers.size());
    }
  }
  else if (configChange.type() == raftRpcProctoc::REMOVE_NODE)
  {
    // 从集群中移除节点
    auto it = m_nodeIdToIndex.find(nodeId);
    if (it != m_nodeIdToIndex.end())
    {
      int removeIndex = it->second;

      // 移除RPC连接
      m_peers.erase(m_peers.begin() + removeIndex);

      // 清理节点映射关系
      m_nodeAddresses.erase(nodeId);
      m_indexToNodeId.erase(removeIndex);
      m_nodeIdToIndex.erase(nodeId);

      // 移除对应的日志复制状态
      m_nextIndex.erase(m_nextIndex.begin() + removeIndex);
      m_matchIndex.erase(m_matchIndex.begin() + removeIndex);

      // 更新其他节点的索引映射（因为数组元素被移除，后续索引需要前移）
      for (auto &pair : m_nodeIdToIndex)
      {
        if (pair.second > removeIndex)
        {
          pair.second--;
          m_indexToNodeId[pair.second] = pair.first;
        }
      }
      m_indexToNodeId.erase(m_indexToNodeId.upper_bound(removeIndex), m_indexToNodeId.end());

      DPrintf("[应用配置变更] 移除节点%s，集群大小:%d",
              nodeId.c_str(), m_peers.size());

      // 如果移除的是当前节点自己，需要停止服务
      if (nodeId == std::to_string(m_me))
      {
        DPrintf("[应用配置变更] 当前节点%d被移除，准备关闭服务", m_me);
        // 这里可以设置标志来优雅关闭服务
      }
    }
  }
}