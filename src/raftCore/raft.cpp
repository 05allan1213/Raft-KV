#include "raft.h"
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <memory>
#include <iomanip>
#include "config.h"
#include "util.h"

/**
 * @brief 处理AppendEntries RPC请求的核心函数
 *
 * 这是Raft算法中最重要的函数之一，负责处理来自领导者的日志复制请求。
 * 该函数实现了Raft论文中AppendEntries RPC的完整逻辑。
 *
 * @param args RPC请求参数，包含任期、领导者ID、前一个日志信息等
 * @param reply RPC响应结果，包含是否成功、当前任期等信息
 */
void Raft::AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply)
{
  // 使用细化的锁策略：先检查term（只需要读锁），然后根据需要获取写锁
  // 这样可以提高并发性能，特别是在大量AppendEntries请求的情况下
  reply->set_appstate(AppNormal); // 能接收到代表网络是正常的

  // 第一步：检查任期号，这是Raft算法的核心安全保证
  // 不同的人收到AppendEntries的反应是不同的，要注意无论什么时候收到rpc请求和响应都要检查term

  if (args->term() < m_currentTerm)
  {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(-100); // 论文中：让领导人可以及时更新自己
    DPrintf("[func-AppendEntries-rf{%d}] 拒绝了 因为Leader{%d}的term{%v}< rf{%d}.term{%d}\n", m_me, args->leaderid(),
            args->term(), m_me, m_currentTerm);
    return; // 注意从过期的领导人收到消息不要重设定时器
  }
  //    Defer ec1([this]() -> void { this->persist(); });
  //    //由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
  DEFER { persist(); }; // 由于这个局部变量创建在锁之后，因此执行persist的时候应该也是拿到锁的.
  if (args->term() > m_currentTerm)
  {
    // 三变 ,防止遗漏，无论什么时候都是三变
    // DPrintf("[func-AppendEntries-rf{%v} ] 变成follower且更新term 因为Leader{%v}的term{%v}> rf{%v}.term{%v}\n", rf.me,
    // args.LeaderId, args.Term, rf.me, rf.currentTerm)
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1; // 这里设置成-1有意义，如果突然宕机然后上线理论上是可以投票的
    // 这里可不返回，应该改成让改节点尝试接收日志
    // 如果是领导人和candidate突然转到Follower好像也不用其他操作
    // 如果本来就是Follower，那么其term变化，相当于“不言自明”的换了追随的对象，因为原来的leader的term更小，是不会再接收其消息了
  }
  myAssert(args->term() == m_currentTerm, "assert {args.Term == rf.currentTerm} fail");
  // 如果发生网络分区，那么candidate可能会收到同一个term的leader的消息，要转变为Follower，为了和上面，因此直接写
  m_status = Follower; // 这里是有必要的，因为如果candidate收到同一个term的leader的AE，需要变成follower
  // term相等
  m_lastResetElectionTime = now();
  //  DPrintf("[	AppendEntries-func-rf(%v)		] 重置了选举超时定时器\n", rf.me);

  // 不能无脑的从prevlogIndex开始阶段日志，因为rpc可能会延迟，导致发过来的log是很久之前的

  //	那么就比较日志，日志有3种情况
  if (args->prevlogindex() > getLastLogIndex())
  {
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(getLastLogIndex() + 1);
    //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为日志太新,args.PrevLogIndex{%v} >
    //  lastLogIndex{%v}，返回值：{%v}\n", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), reply)
    return;
  }
  else if (args->prevlogindex() < m_lastSnapshotIncludeIndex)
  {
    // 如果prevlogIndex还没有更上快照
    reply->set_success(false);
    reply->set_term(m_currentTerm);
    reply->set_updatenextindex(
        m_lastSnapshotIncludeIndex +
        1); // todo 如果想直接弄到最新好像不对，因为是从后慢慢往前匹配的，这里不匹配说明后面的都不匹配
    //  DPrintf("[func-AppendEntries-rf{%v}] 拒绝了节点{%v}，因为log太老，返回值：{%v}\n", rf.me, args.LeaderId, reply)
    //  return
  }
  //	本机日志有那么长，冲突(same index,different term),截断日志
  // 注意：这里目前当args.PrevLogIndex == rf.lastSnapshotIncludeIndex与不等的时候要分开考虑，可以看看能不能优化这块
  if (matchLog(args->prevlogindex(), args->prevlogterm()))
  {
    //	todo：	整理logs
    // ，不能直接截断，必须一个一个检查，因为发送来的log可能是之前的，直接截断可能导致“取回”已经在follower日志中的条目
    // 那意思是不是可能会有一段发来的AE中的logs中前半是匹配的，后半是不匹配的，这种应该：1.follower如何处理？ 2.如何给leader回复
    // 3. leader如何处理

    for (int i = 0; i < args->entries_size(); i++)
    {
      auto log = args->entries(i);
      if (log.logindex() > getLastLogIndex())
      {
        // 超过就直接添加日志
        m_logs.push_back(log);
      }
      else
      {
        // 没超过就比较是否匹配，不匹配再更新，而不是直接截断
        //  todo ： 这里可以改进为比较对应logIndex位置的term是否相等，term相等就代表匹配
        //   todo：这个地方放出来会出问题,按理说index相同，term相同，log也应该相同才对
        //  rf.logs[entry.Index-firstIndex].Term ?= entry.Term

        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() == log.logterm() &&
            m_logs[getSlicesIndexFromLogIndex(log.logindex())].command() != log.command())
        {
          // 相同位置的log ，其logTerm相等，但是命令却不相同，不符合raft的前向匹配，异常了！
          myAssert(false, format("[func-AppendEntries-rf{%d}] 两节点logIndex{%d}和term{%d}相同，但是其command{%d:%d}   "
                                 " {%d:%d}却不同！！\n",
                                 m_me, log.logindex(), log.logterm(), m_me,
                                 m_logs[getSlicesIndexFromLogIndex(log.logindex())].command(), args->leaderid(),
                                 log.command()));
        }
        if (m_logs[getSlicesIndexFromLogIndex(log.logindex())].logterm() != log.logterm())
        {
          // 不匹配就更新
          m_logs[getSlicesIndexFromLogIndex(log.logindex())] = log;
        }
      }
    }

    // 错误写法like：  rf.shrinkLogsToIndex(args.PrevLogIndex)
    // rf.logs = append(rf.logs, args.Entries...)
    // 因为可能会收到过期的log！！！ 因此这里是大于等于
    myAssert(
        getLastLogIndex() >= args->prevlogindex() + args->entries_size(),
        format("[func-AppendEntries1-rf{%d}]rf.getLastLogIndex(){%d} != args.PrevLogIndex{%d}+len(args.Entries){%d}",
               m_me, getLastLogIndex(), args->prevlogindex(), args->entries_size()));
    // if len(args.Entries) > 0 {
    //	fmt.Printf("[func-AppendEntries  rf:{%v}] ] : args.term:%v, rf.term:%v  ,rf.logs的长度：%v\n", rf.me, args.Term,
    // rf.currentTerm, len(rf.logs))
    // }
    if (args->leadercommit() > m_commitIndex)
    {
      m_commitIndex = std::min(args->leadercommit(), getLastLogIndex());
      // 这个地方不能无脑跟上getLastLogIndex()，因为可能存在args->leadercommit()落后于 getLastLogIndex()的情况
    }

    // 领导会一次发送完所有的日志
    myAssert(getLastLogIndex() >= m_commitIndex,
             format("[func-AppendEntries1-rf{%d}]  rf.getLastLogIndex{%d} < rf.commitIndex{%d}", m_me,
                    getLastLogIndex(), m_commitIndex));
    reply->set_success(true);
    reply->set_term(m_currentTerm);

    //        DPrintf("[func-AppendEntries-rf{%v}] 接收了来自节点{%v}的log，当前lastLogIndex{%v}，返回值：{%v}\n",
    //        rf.me,
    //                args.LeaderId, rf.getLastLogIndex(), reply)

    return;
  }
  else
  {
    // 优化
    // PrevLogIndex 长度合适，但是不匹配，因此往前寻找 矛盾的term的第一个元素
    // 为什么该term的日志都是矛盾的呢？也不一定都是矛盾的，只是这么优化减少rpc而已
    // ？什么时候term会矛盾呢？很多情况，比如leader接收了日志之后马上就崩溃等等
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
    // 对UpdateNextIndex待优化  todo  找到符合的term的最后一个
    //        DPrintf("[func-AppendEntries-rf{%v}]
    //        拒绝了节点{%v}，因为prevLodIndex{%v}的args.term{%v}不匹配当前节点的logterm{%v}，返回值：{%v}\n",
    //                rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
    //                rf.logs[rf.getSlicesIndexFromLogIndex(args.PrevLogIndex)].LogTerm, reply)
    //        DPrintf("[func-AppendEntries-rf{%v}] 返回值: reply.UpdateNextIndex从{%v}优化到{%v}，优化了{%v}\n", rf.me,
    //                args.PrevLogIndex, reply.UpdateNextIndex, args.PrevLogIndex - reply.UpdateNextIndex) //
    //                很多都是优化了0
    return;
  }

  // fmt.Printf("[func-AppendEntries,rf{%v}]:len(rf.logs):%v, rf.commitIndex:%v\n", rf.me, len(rf.logs), rf.commitIndex)
}

/**
 * @brief 应用日志到状态机的定时器函数
 *
 * 该函数运行在一个独立的线程中，定期检查并应用已提交但未应用的日志条目到状态机。
 * 这是Raft算法中确保状态机一致性的关键机制。
 */
void Raft::applierTicker()
{
  while (true)
  {
    m_mtx.lock();

    // 添加调试信息，显示applierTicker的状态
    static int debugCount = 0;
    if (++debugCount % 50 == 1 || (m_status.load() == Leader && m_commitIndex > m_lastApplied))
    { // 每50次输出一次状态，或者Leader有待应用的日志时
      DPrintf("🔄 [节点%d] applierTicker状态: lastApplied=%d, commitIndex=%d, status=%d",
              m_me, m_lastApplied, m_commitIndex, (int)m_status.load());
    }

    auto applyMsgs = getApplyLogs();
    // 只在有日志需要应用时才输出调试信息
    if (!applyMsgs.empty())
    {
      DPrintf("📋 [节点%d] 应用日志: lastApplied=%d, commitIndex=%d, 消息数=%d",
              m_me, m_lastApplied, m_commitIndex, applyMsgs.size());
    }
    m_mtx.unlock();

    // 使用匿名函数是因为传递管道的时候不用拿锁
    // 好像必须拿锁，因为不拿锁的话如果调用多次applyLog函数，可能会导致应用的顺序不一样
    if (!applyMsgs.empty())
    {
      DPrintf("📤 [节点%d] 向KV服务器发送 %d 条应用消息", m_me, applyMsgs.size());
    }

    // 将待应用的日志消息推送到应用通道
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

void Raft::doElection()
{
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader)
  {
    // fmt.Printf("[       ticker-func-rf(%v)              ] is a Leader,wait the  lock\n", rf.me)
  }
  // fmt.Printf("[       ticker-func-rf(%v)              ] get the  lock\n", rf.me)

  if (m_status != Leader)
  {
    DPrintf("🗳️  [节点%d] 选举定时器到期，开始新一轮选举", m_me);
    // 当选举的时候定时器超时就必须重新选举，不然没有选票就会一直卡主
    // 重竞选超时，term也会增加的
    m_status = Candidate;
    /// 开始新一轮的选举
    m_currentTerm += 1;
    m_votedFor = m_me; // 即是自己给自己投，也避免candidate给同辈的candidate投
    persist();
    std::shared_ptr<int> votedNum = std::make_shared<int>(1); // 使用 make_shared 函数初始化 !! 亮点
    //	重新设置定时器
    m_lastResetElectionTime = now();

    // 特殊处理：单节点集群直接成为领导者
    if (m_peers.size() == 1)
    {
      m_status = Leader;
      DPrintf("[func-Raft::doElection rf{%d}] 单节点集群，直接成为领导者，term:{%d}\n", m_me, m_currentTerm);

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
      getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm); // 获取最后一个log的term和下标

      std::shared_ptr<raftRpcProctoc::RequestVoteArgs> requestVoteArgs =
          std::make_shared<raftRpcProctoc::RequestVoteArgs>();
      requestVoteArgs->set_term(m_currentTerm);
      requestVoteArgs->set_candidateid(m_me);
      requestVoteArgs->set_lastlogindex(lastLogIndex);
      requestVoteArgs->set_lastlogterm(lastLogTerm);
      auto requestVoteReply = std::make_shared<raftRpcProctoc::RequestVoteReply>();

      // 使用协程替代线程，提高并发性能
      if (m_ioManager)
      {
        // 使用协程异步发送RequestVote RPC
        m_ioManager->scheduler([this, i, requestVoteArgs, requestVoteReply, votedNum]()
                               { this->sendRequestVote(i, requestVoteArgs, requestVoteReply, votedNum); });
      }
      else
      {
        // 回退到原有的线程模式（向后兼容）
        std::thread t(&Raft::sendRequestVote, this, i, requestVoteArgs, requestVoteReply,
                      votedNum);
        t.detach();
      }
    }
  }
}

void Raft::doHeartBeat()
{
  std::lock_guard<std::mutex> g(m_mtx);

  if (m_status == Leader)
  {
    // 减少心跳日志输出频率，但在开始时多输出一些
    static int heartbeatCount = 0;
    if (++heartbeatCount <= 5 || heartbeatCount % 10 == 0)
    {
      DPrintf("💗 [节点%d-Leader] 发送心跳 #%d", m_me, heartbeatCount);
    }
    auto appendNums = std::make_shared<int>(1); // 正确返回的节点的数量

    // 对Follower（除了自己外的所有节点发送AE）
    //  todo 这里肯定是要修改的，最好使用一个单独的goruntime来负责管理发送log，因为后面的log发送涉及优化之类的
    // 最少要单独写一个函数来管理，而不是在这一坨
    for (int i = 0; i < m_peers.size(); i++)
    {
      if (i == m_me)
      {
        continue;
      }
      // 注释掉频繁的心跳日志
      // DPrintf("💗 [节点%d-Leader] 向节点%d发送心跳", m_me, i);
      myAssert(m_nextIndex[i] >= 1, format("rf.nextIndex[%d] = {%d}", i, m_nextIndex[i]));
      // 日志压缩加入后要判断是发送快照还是发送AE
      if (m_nextIndex[i] <= m_lastSnapshotIncludeIndex)
      {
        //                        DPrintf("[func-ticker()-rf{%v}]rf.nextIndex[%v] {%v} <=
        //                        rf.lastSnapshotIncludeIndex{%v},so leaderSendSnapShot", rf.me, i, rf.nextIndex[i],
        //                        rf.lastSnapshotIncludeIndex)
        // 使用协程替代线程发送快照
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
      // 构造发送值
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
          *sendEntryPtr = m_logs[j]; //=是可以点进去的，可以点进去看下protobuf如何重写这个的
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

void Raft::electionTimeOutTicker()
{
  // Check if a Leader election should be started.
  while (true)
  {
    /**
     * 如果不睡眠，那么对于leader，这个函数会一直空转，浪费cpu。且加入协程之后，空转会导致其他协程无法运行，对于时间敏感的AE，会导致心跳无法正常发送导致异常
     */
    while (m_status == Leader)
    {
      usleep(
          HeartBeatTimeout); // 定时时间没有严谨设置，因为HeartBeatTimeout比选举超时一般小一个数量级，因此就设置为HeartBeatTimeout了
    }
    std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
    std::chrono::system_clock::time_point wakeTime{};
    {
      m_mtx.lock();
      wakeTime = now();
      suitableSleepTime = getRandomizedElectionTimeout() + m_lastResetElectionTime - wakeTime;
      m_mtx.unlock();
    }

    if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1)
    {
      // 获取当前时间点
      auto start = std::chrono::steady_clock::now();

      usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
      // std::this_thread::sleep_for(suitableSleepTime);

      // 获取函数运行结束后的时间点
      auto end = std::chrono::steady_clock::now();

      // 计算时间差并输出结果（单位为毫秒）
      std::chrono::duration<double, std::milli> duration = end - start;

      // 使用ANSI控制序列将输出颜色修改为紫色
      std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                << std::endl;
      std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                << std::endl;
    }

    if (std::chrono::duration<double, std::milli>(m_lastResetElectionTime - wakeTime).count() > 0)
    {
      // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠
      continue;
    }
    doElection();
  }
}

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
    //        DPrintf("[	applyLog func-rf{%v}	] apply Log,logIndex:%v  ，logTerm：{%v},command：{%v}\n",
    //        rf.me, rf.lastApplied, rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].LogTerm,
    //        rf.logs[rf.getSlicesIndexFromLogIndex(rf.lastApplied)].Command)
  }
  return applyMsgs;
}

// 获取新命令应该分配的Index
int Raft::getNewCommandIndex()
{
  //	如果len(logs)==0,就为快照的index+1，否则为log最后一个日志+1
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

// GetState return currentTerm and whether this server
// believes it is the Leader.
void Raft::GetState(int *term, bool *isLeader)
{
  // 使用读锁，因为这是读多写少的场景
  monsoon::RWMutex::ReadLock lock(m_stateMutex);

  // Your code here (2A).
  *term = m_currentTerm;
  *isLeader = (m_status == Leader);
}

void Raft::InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                           raftRpcProctoc::InstallSnapshotResponse *reply)
{
  m_mtx.lock();
  DEFER { m_mtx.unlock(); };
  if (args->term() < m_currentTerm)
  {
    reply->set_term(m_currentTerm);
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.term{%v}<rf{%v}.term{%v} ", rf.me, args.LeaderId,
    //        args.Term, rf.me, rf.currentTerm)

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
    //        DPrintf("[func-InstallSnapshot-rf{%v}] leader{%v}.LastSnapShotIncludeIndex{%v} <=
    //        rf{%v}.lastSnapshotIncludeIndex{%v} ", rf.me, args.LeaderId, args.LastSnapShotIncludeIndex, rf.me,
    //        rf.lastSnapshotIncludeIndex)
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

  std::thread t(&Raft::pushMsgToKvServer, this, msg); // 创建新线程并执行b函数，并传递参数
  t.detach();
  // 看下这里能不能再优化
  //     DPrintf("[func-InstallSnapshot-rf{%v}] receive snapshot from {%v} ,LastSnapShotIncludeIndex ={%v} ", rf.me,
  //     args.LeaderId, args.LastSnapShotIncludeIndex)
  // 持久化
  m_persister->Save(persistData(), args->data());
}

void Raft::pushMsgToKvServer(ApplyMsg msg)
{
  auto result = applyChan->send(msg);
  if (result != monsoon::ChannelResult::SUCCESS)
  {
    DPrintf("[Raft::pushMsgToKvServer] 发送ApplyMsg失败，结果: %d", (int)result);
  }
}

void Raft::leaderHearBeatTicker()
{
  while (true)
  {
    // 不是leader的话就没有必要进行后续操作，况且还要拿锁，很影响性能，目前是睡眠，后面再优化优化
    int waitCount = 0;
    while (m_status.load() != Leader)
    {
      // 添加调试信息，确认心跳定时器的状态
      if (++waitCount % 10 == 1)
      { // 每10次输出一次，避免日志过多
        DPrintf("⏳ [节点%d] 心跳定时器等待成为Leader (当前状态:%d)", m_me, (int)m_status.load());
      }
      usleep(1000 * HeartBeatTimeout);
      // std::this_thread::sleep_for(std::chrono::milliseconds(HeartBeatTimeout));
    }

    // 成为Leader后输出确认信息
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
      // std::this_thread::sleep_for(suitableSleepTime);

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
    // DPrintf("[func-Raft::doHeartBeat()-Leader: {%d}] Leader的心跳定时器触发了\n", m_me);
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

  // 多节点集群的原有逻辑
  m_commitIndex = m_lastSnapshotIncludeIndex;
  for (int index = getLastLogIndex(); index >= m_lastSnapshotIncludeIndex + 1; index--)
  {
    int sum = 0;
    for (int i = 0; i < m_peers.size(); i++)
    {
      if (i == m_me)
      {
        sum += 1;
        continue;
      }
      if (m_matchIndex[i] >= index)
      {
        sum += 1;
      }
    }

    if (sum >= m_peers.size() / 2 + 1 && getLogTermFromLogIndex(index) == m_currentTerm)
    {
      m_commitIndex = index;
      break;
    }
  }
}

// 进来前要保证logIndex是存在的，即≥rf.lastSnapshotIncludeIndex	，而且小于等于rf.getLastLogIndex()
bool Raft::matchLog(int logIndex, int logTerm)
{
  myAssert(logIndex >= m_lastSnapshotIncludeIndex && logIndex <= getLastLogIndex(),
           format("不满足：logIndex{%d}>=rf.lastSnapshotIncludeIndex{%d}&&logIndex{%d}<=rf.getLastLogIndex{%d}",
                  logIndex, m_lastSnapshotIncludeIndex, logIndex, getLastLogIndex()));
  return logTerm == getLogTermFromLogIndex(logIndex);
  // if logIndex == rf.lastSnapshotIncludeIndex {
  // 	return logTerm == rf.lastSnapshotIncludeTerm
  // } else {
  // 	return logTerm == rf.logs[rf.getSlicesIndexFromLogIndex(logIndex)].LogTerm
  // }
}

void Raft::persist()
{
  // Your code here (2C).
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

  // fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm,
  // rf.votedFor, rf.logs) fmt.Printf("%v\n", string(data))
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
    DPrintf("[func-RequestVote rf{%d}] 收到连接测试请求", m_me);
    return; // 不需要持久化
  }

  // 添加调试信息
  DPrintf("🗳️ [节点%d] 收到来自节点%d的投票请求 (任期:%d)", m_me, args->candidateid(), args->term());

  // Your code here (2A, 2B).
  // 注意：persist()必须在锁保护下调用，因为persistData()需要访问成员变量
  bool needPersist = false;
  // 对args的term的三种情况分别进行处理，大于小于等于自己的term都是不同的处理
  //  reason: 出现网络分区，该竞选者已经OutOfDate(过时）
  if (args->term() < m_currentTerm)
  {
    reply->set_term(m_currentTerm);
    reply->set_votestate(Expire);
    reply->set_votegranted(false);
    DPrintf("❌ [节点%d] 拒绝投票给节点%d：任期过时 (%d < %d)", m_me, args->candidateid(), args->term(), m_currentTerm);
    // 不需要持久化，因为状态没有改变
    return;
  }
  // fig2:右下角，如果任何时候rpc请求或者响应的term大于自己的term，更新term，并变成follower
  if (args->term() > m_currentTerm)
  {
    DPrintf("📈 [节点%d] 更新任期：%d -> %d，变成Follower", m_me, m_currentTerm, args->term());
    m_status = Follower;
    m_currentTerm = args->term();
    m_votedFor = -1;
    needPersist = true; // 状态改变，需要持久化

    //	重置定时器：收到leader的ae，开始选举，透出票
    // 这时候更新了term之后，votedFor也要置为-1
  }
  myAssert(args->term() == m_currentTerm,
           format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", m_me));
  //	现在节点任期都是相同的(任期小的也已经更新到新的args的term了)，还需要检查log的term和index是不是匹配的了

  int lastLogTerm = getLastLogTerm();
  // 只有没投票，且candidate的日志的新的程度 ≥ 接受者的日志新的程度 才会授票
  if (!UpToDate(args->lastlogindex(), args->lastlogterm()))
  {
    // args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
    // 日志太旧了
    if (args->lastlogterm() < lastLogTerm)
    {
      //                    DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //                    candidate_lastlog_term{%v} < lastlog_term{%v}\n", rf.me, args.CandidateId, args.LastLogTerm,
      //                    lastLogTerm)
    }
    else
    {
      //            DPrintf("[	    func-RequestVote-rf(%v)		] : refuse voted rf[%v] ,because
      //            candidate_log_index{%v} < log_index{%v}\n", rf.me, args.CandidateId, args.LastLogIndex,
      //            rf.getLastLogIndex())
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
  // todo ： 啥时候会出现rf.votedFor == args.CandidateId ，就算candidate选举超时再选举，其term也是不一样的呀
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
    m_lastResetElectionTime = now(); // 认为必须要在投出票的时候才重置定时器，
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

bool Raft::UpToDate(int index, int term)
{
  // lastEntry := rf.log[len(rf.log)-1]

  int lastIndex = -1;
  int lastTerm = -1;
  getLastLogIndexAndTerm(&lastIndex, &lastTerm);
  return term > lastTerm || (term == lastTerm && index >= lastIndex);
}

void Raft::getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm)
{
  if (m_logs.empty())
  {
    *lastLogIndex = m_lastSnapshotIncludeIndex;
    *lastLogTerm = m_lastSnapshotIncludeTerm;
    return;
  }
  else
  {
    *lastLogIndex = m_logs[m_logs.size() - 1].logindex();
    *lastLogTerm = m_logs[m_logs.size() - 1].logterm();
    return;
  }
}
/**
 *
 * @return 最新的log的logindex，即log的逻辑index。区别于log在m_logs中的物理index
 * 可见：getLastLogIndexAndTerm()
 */
int Raft::getLastLogIndex()
{
  int lastLogIndex = -1;
  int _ = -1;
  getLastLogIndexAndTerm(&lastLogIndex, &_);
  return lastLogIndex;
}

int Raft::getLastLogTerm()
{
  int _ = -1;
  int lastLogTerm = -1;
  getLastLogIndexAndTerm(&_, &lastLogTerm);
  return lastLogTerm;
}

/**
 *
 * @param logIndex log的逻辑index。注意区别于m_logs的物理index
 * @return
 */
int Raft::getLogTermFromLogIndex(int logIndex)
{
  myAssert(logIndex >= m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} < rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));

  int lastLogIndex = getLastLogIndex();

  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
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

int Raft::GetRaftStateSize() { return m_persister->RaftStateSize(); }

// 找到index对应的真实下标位置！！！
// 限制，输入的logIndex必须保存在当前的logs里面（不包含snapshot）
int Raft::getSlicesIndexFromLogIndex(int logIndex)
{
  myAssert(logIndex > m_lastSnapshotIncludeIndex,
           format("[func-getSlicesIndexFromLogIndex-rf{%d}]  index{%d} <= rf.lastSnapshotIncludeIndex{%d}", m_me,
                  logIndex, m_lastSnapshotIncludeIndex));
  int lastLogIndex = getLastLogIndex();
  myAssert(logIndex <= lastLogIndex, format("[func-getSlicesIndexFromLogIndex-rf{%d}]  logIndex{%d} > lastLogIndex{%d}",
                                            m_me, logIndex, lastLogIndex));
  int SliceIndex = logIndex - m_lastSnapshotIncludeIndex - 1;
  return SliceIndex;
}

bool Raft::sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                           std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum)
{
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  //  ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //  todo
  auto start = now();
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 开始", m_me, server);
  bool ok = m_peers[server]->RequestVote(args.get(), reply.get());
  DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 完毕，耗时:{%ld} ms", m_me, server,
          std::chrono::duration_cast<std::chrono::milliseconds>(now() - start).count());

  if (!ok)
  {
    return ok; // 不知道为什么不加这个的话如果服务器宕机会出现问题的，通不过2B  todo
  }
  // for !ok {
  //
  //	//ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
  //	//if ok {
  //	//	break
  //	//}
  // } //这里是发送出去了，但是不能保证他一定到达
  // 对回应进行处理，要记得无论什么时候收到回复就要检查term
  std::lock_guard<std::mutex> lg(m_mtx);
  if (reply->term() > m_currentTerm)
  {
    m_status = Follower; // 三变：身份，term，和投票
    m_currentTerm = reply->term();
    m_votedFor = -1;
    persist();
    return true;
  }
  else if (reply->term() < m_currentTerm)
  {
    return true;
  }
  myAssert(reply->term() == m_currentTerm, "assert {reply.Term==rf.currentTerm} fail");

  // todo：这里没有按博客写
  if (!reply->votegranted())
  {
    return true;
  }

  *votedNum = *votedNum + 1;
  if (*votedNum >= m_peers.size() / 2 + 1)
  {
    // 变成leader
    *votedNum = 0;
    if (m_status == Leader)
    {
      // 如果已经是leader了，那么是就是了，不会进行下一步处理了k
      myAssert(false,
               format("[func-sendRequestVote-rf{%d}]  term:{%d} 同一个term当两次领导，error", m_me, m_currentTerm));
    }
    //	第一次变成leader，初始化状态和nextIndex、matchIndex
    m_status = Leader;

    DPrintf("🎉 [节点%d] 选举成功！成为Leader (任期:%d, 最后日志索引:%d)", m_me, m_currentTerm, getLastLogIndex());
    DPrintf("👑 [节点%d] 状态已设置为Leader (m_status=%d)", m_me, (int)m_status);
    DPrintf("🔍 [节点%d] Leader状态检查: lastApplied=%d, commitIndex=%d", m_me, m_lastApplied, m_commitIndex);

    int lastLogIndex = getLastLogIndex();
    for (int i = 0; i < m_nextIndex.size(); i++)
    {
      m_nextIndex[i] = lastLogIndex + 1; // 有效下标从1开始，因此要+1
      m_matchIndex[i] = 0;               // 每换一个领导都是从0开始，见fig2
    }
    // 马上向其他节点宣告自己就是leader - 使用协程替代线程
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

bool Raft::sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                             std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> appendNums)
{
  // 这个ok是网络是否正常通信的ok，而不是requestVote rpc是否投票的rpc
  //  如果网络不通的话肯定是没有返回的，不用一直重试
  //  todo： paper中5.3节第一段末尾提到，如果append失败应该不断的retries ,直到这个log成功的被store
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc开始 ， args->entries_size():{%d}", m_me,
          server, args->entries_size());
  bool ok = m_peers[server]->AppendEntries(args.get(), reply.get());

  if (!ok)
  {
    DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失敗", m_me, server);
    return ok;
  }
  DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", m_me, server);
  if (reply->appstate() == Disconnected)
  {
    return ok;
  }
  std::lock_guard<std::mutex> lg1(m_mtx);

  // 对reply进行处理
  //  对于rpc通信，无论什么时候都要检查term
  if (reply->term() > m_currentTerm)
  {
    m_status = Follower;
    m_currentTerm = reply->term();
    m_votedFor = -1;
    return ok;
  }
  else if (reply->term() < m_currentTerm)
  {
    DPrintf("[func -sendAppendEntries  rf{%d}]  节点：{%d}的term{%d}<rf{%d}的term{%d}\n", m_me, server, reply->term(),
            m_me, m_currentTerm);
    return ok;
  }

  if (m_status != Leader)
  {
    // 如果不是leader，那么就不要对返回的情况进行处理了
    return ok;
  }
  // term相等

  myAssert(reply->term() == m_currentTerm,
           format("reply.Term{%d} != rf.currentTerm{%d}   ", reply->term(), m_currentTerm));
  if (!reply->success())
  {
    // 日志不匹配，正常来说就是index要往前-1，既然能到这里，第一个日志（idnex =
    //  1）发送后肯定是匹配的，因此不用考虑变成负数 因为真正的环境不会知道是服务器宕机还是发生网络分区了
    if (reply->updatenextindex() != -100)
    {
      // todo:待总结，就算term匹配，失败的时候nextIndex也不是照单全收的，因为如果发生rpc延迟，leader的term可能从不符合term要求
      // 变得符合term要求
      // 但是不能直接赋值reply.UpdateNextIndex
      DPrintf("[func -sendAppendEntries  rf{%d}]  返回的日志term相等，但是不匹配，回缩nextIndex[%d]：{%d}\n", m_me,
              server, reply->updatenextindex());
      m_nextIndex[server] = reply->updatenextindex(); // 失败是不更新mathIndex的
    }
    //	怎么越写越感觉rf.nextIndex数组是冗余的呢，看下论文fig2，其实不是冗余的
  }
  else
  {
    *appendNums = *appendNums + 1;
    DPrintf("---------------------------tmp------------------------- 节点{%d}返回true,当前*appendNums{%d}", server,
            *appendNums);
    // rf.matchIndex[server] = len(args.Entries) //只要返回一个响应就对其matchIndex应该对其做出反应，
    // 但是这么修改是有问题的，如果对某个消息发送了多遍（心跳时就会再发送），那么一条消息会导致n次上涨
    m_matchIndex[server] = std::max(m_matchIndex[server], args->prevlogindex() + args->entries_size());
    m_nextIndex[server] = m_matchIndex[server] + 1;
    int lastLogIndex = getLastLogIndex();

    myAssert(m_nextIndex[server] <= lastLogIndex + 1,
             format("error msg:rf.nextIndex[%d] > lastLogIndex+1, len(rf.logs) = %d   lastLogIndex{%d} = %d", server,
                    m_logs.size(), server, lastLogIndex));
    if (*appendNums >= 1 + m_peers.size() / 2)
    {
      // 可以commit了
      // 两种方法保证幂等性，1.赋值为0 	2.上面≥改为==

      *appendNums = 0;
      // todo https://578223592-laughing-halibut-wxvpggvw69qh99q4.github.dev/ 不断遍历来统计rf.commitIndex
      // 改了好久！！！！！
      // leader只有在当前term有日志提交的时候才更新commitIndex，因为raft无法保证之前term的Index是否提交
      // 只有当前term有日志提交，之前term的log才可以被提交，只有这样才能保证“领导人完备性{当选领导人的节点拥有之前被提交的所有log，当然也可能有一些没有被提交的}”
      // 修复：启用领导者提交索引更新
      leaderUpdateCommitIndex();
      if (args->entries_size() > 0)
      {
        DPrintf("args->entries(args->entries_size()-1).logterm(){%d}   m_currentTerm{%d}",
                args->entries(args->entries_size() - 1).logterm(), m_currentTerm);
      }
      if (args->entries_size() > 0 && args->entries(args->entries_size() - 1).logterm() == m_currentTerm)
      {
        DPrintf(
            "---------------------------tmp------------------------- 当前term有log成功提交，更新leader的m_commitIndex "
            "from{%d} to{%d}",
            m_commitIndex, args->prevlogindex() + args->entries_size());

        int oldCommitIndex = m_commitIndex;
        m_commitIndex = std::max(m_commitIndex, args->prevlogindex() + args->entries_size());

        if (m_commitIndex > oldCommitIndex)
        {
          DPrintf("📈 [节点%d-Leader] commitIndex更新: %d -> %d, lastApplied=%d",
                  m_me, oldCommitIndex, m_commitIndex, m_lastApplied);
        }
      }
      myAssert(m_commitIndex <= lastLogIndex,
               format("[func-sendAppendEntries,rf{%d}] lastLogIndex:%d  rf.commitIndex:%d\n", m_me, lastLogIndex,
                      m_commitIndex));
      // fmt.Printf("[func-sendAppendEntries,rf{%v}] len(rf.logs):%v  rf.commitIndex:%v\n", rf.me, len(rf.logs),
      // rf.commitIndex)
    }
  }
  return ok;
}

void Raft::AppendEntries(google::protobuf::RpcController *controller,
                         const ::raftRpcProctoc::AppendEntriesArgs *request,
                         ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done)
{
  AppendEntries1(request, response);
  done->Run();
}

void Raft::InstallSnapshot(google::protobuf::RpcController *controller,
                           const ::raftRpcProctoc::InstallSnapshotRequest *request,
                           ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done)
{
  InstallSnapshot(request, response);

  done->Run();
}

void Raft::RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                       ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done)
{
  RequestVote(request, response);
  done->Run();
}

void Raft::ChangeConfig(google::protobuf::RpcController *controller,
                        const ::raftRpcProctoc::ChangeConfigArgs *request,
                        ::raftRpcProctoc::ChangeConfigReply *response, ::google::protobuf::Closure *done)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  // 只有Leader可以处理成员变更请求
  if (m_status != Leader)
  {
    response->set_success(false);
    response->set_isleader(false);
    response->set_error("Not leader");
    done->Run();
    return;
  }

  response->set_isleader(true);

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      request->type(), request->nodeid(), request->address());

  // 将配置变更作为日志条目添加到日志中
  m_logs.emplace_back(configEntry);

  // 立即开始复制这个配置变更日志
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

  DPrintf("[ChangeConfig] Leader %d processed config change: type=%d, nodeId=%s, address=%s",
          m_me, request->type(), request->nodeid().c_str(), request->address().c_str());

  done->Run();
}

void Raft::Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader)
{
  std::lock_guard<std::mutex> lg1(m_mtx);
  //    m_mtx.lock();
  //    Defer ec1([this]()->void {
  //       m_mtx.unlock();
  //    });
  if (m_status != Leader)
  {
    DPrintf("[func-Start-rf{%d}]  is not leader");
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

  // leader应该不停的向各个Follower发送AE来维护心跳和保持日志同步，目前的做法是新的命令来了不会直接执行，而是等待leader的心跳触发
  DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s", m_me, lastLogIndex, command.asString().c_str());
  // rf.timer.Reset(10) //接收到命令后马上给follower发送,改成这样不知为何会出现问题，待修正 todo
  persist();
  *newLogIndex = newLogEntry.logindex();
  *newLogTerm = newLogEntry.logterm();
  *isLeader = true;
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
void Raft::init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
                monsoon::Channel<ApplyMsg>::ptr applyCh)
{
  m_peers = peers;
  m_persister = persister;
  m_me = me;
  // Your initialization code here (2A, 2B, 2C).
  m_mtx.lock();

  // applier
  this->applyChan = applyCh;
  //    rf.ApplyMsgQueue = make(chan ApplyMsg)
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

  // initialize from state persisted before a crash
  readPersist(m_persister->ReadRaftState());
  if (m_lastSnapshotIncludeIndex > 0)
  {
    m_lastApplied = m_lastSnapshotIncludeIndex;
    // rf.commitIndex = rf.lastSnapshotIncludeIndex   todo ：崩溃恢复为何不能读取commitIndex
  }

  DPrintf("[Init&ReInit] Sever %d, term %d, lastSnapshotIncludeIndex {%d} , lastSnapshotIncludeTerm {%d}", m_me,
          m_currentTerm, m_lastSnapshotIncludeIndex, m_lastSnapshotIncludeTerm);

  m_mtx.unlock();

  m_ioManager = std::make_unique<monsoon::IOManager>(FIBER_THREAD_NUM, FIBER_USE_CALLER_THREAD);

  // start ticker fiber to start elections
  // 启动循环定时器，但不立即启动选举定时器
  // todo:原来是启动了三个线程，现在是直接使用了协程，三个函数中leaderHearBeatTicker
  // 、electionTimeOutTicker执行时间是恒定的，applierTicker时间受到数据库响应延迟和两次apply之间请求数量的影响，这个随着数据量增多可能不太合理，最好其还是启用一个线程。
  m_ioManager->scheduler([this]() -> void
                         { this->leaderHearBeatTicker(); });
  // 注意：选举定时器将通过 startElectionTimer() 方法手动启动
  // m_ioManager->scheduler([this]() -> void
  //                        { this->electionTimeOutTicker(); });

  std::thread t3(&Raft::applierTicker, this);
  t3.detach();

  // std::thread t(&Raft::leaderHearBeatTicker, this);
  // t.detach();
  //
  // std::thread t2(&Raft::electionTimeOutTicker, this);
  // t2.detach();
  //
  // std::thread t3(&Raft::applierTicker, this);
  // t3.detach();
}

std::string Raft::persistData()
{
  BoostPersistRaftNode boostPersistRaftNode;
  boostPersistRaftNode.m_currentTerm = m_currentTerm;
  boostPersistRaftNode.m_votedFor = m_votedFor;
  boostPersistRaftNode.m_lastSnapshotIncludeIndex = m_lastSnapshotIncludeIndex;
  boostPersistRaftNode.m_lastSnapshotIncludeTerm = m_lastSnapshotIncludeTerm;
  for (auto &item : m_logs)
  {
    boostPersistRaftNode.m_logs.push_back(item.SerializeAsString());
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << boostPersistRaftNode;
  return ss.str();
}

void Raft::readPersist(std::string data)
{
  if (data.empty())
  {
    return;
  }
  std::stringstream iss(data);
  boost::archive::text_iarchive ia(iss);
  // read class state from archive
  BoostPersistRaftNode boostPersistRaftNode;
  ia >> boostPersistRaftNode;

  m_currentTerm = boostPersistRaftNode.m_currentTerm;
  m_votedFor = boostPersistRaftNode.m_votedFor;
  m_lastSnapshotIncludeIndex = boostPersistRaftNode.m_lastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = boostPersistRaftNode.m_lastSnapshotIncludeTerm;
  m_logs.clear();
  for (auto &item : boostPersistRaftNode.m_logs)
  {
    raftRpcProctoc::LogEntry logEntry;
    logEntry.ParseFromString(item);
    m_logs.emplace_back(logEntry);
  }
}

void Raft::Snapshot(int index, std::string snapshot)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
  {
    DPrintf(
        "[func-Snapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex(); // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  // 制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++)
  {
    // 注意有=，因为要拿到最后一个日志
    trunckedLogs.push_back(m_logs[getSlicesIndexFromLogIndex(i)]);
  }
  m_lastSnapshotIncludeIndex = newLastSnapshotIncludeIndex;
  m_lastSnapshotIncludeTerm = newLastSnapshotIncludeTerm;
  m_logs = trunckedLogs;
  m_commitIndex = std::max(m_commitIndex, index);
  m_lastApplied = std::max(m_lastApplied, index);

  // rf.lastApplied = index //lastApplied 和 commit应不应该改变呢？？？ 为什么  不应该改变吧
  m_persister->Save(persistData(), snapshot);

  DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
          m_lastSnapshotIncludeTerm, m_logs.size());
  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}

void Raft::StreamingSnapshot(int index, const std::string &snapshotFilePath)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  if (m_lastSnapshotIncludeIndex >= index || index > m_commitIndex)
  {
    DPrintf(
        "[func-StreamingSnapshot-rf{%d}] rejects replacing log with snapshotIndex %d as current snapshotIndex %d is larger or "
        "smaller ",
        m_me, index, m_lastSnapshotIncludeIndex);
    return;
  }
  auto lastLogIndex = getLastLogIndex(); // 为了检查snapshot前后日志是否一样，防止多截取或者少截取日志

  // 制造完此快照后剩余的所有日志
  int newLastSnapshotIncludeIndex = index;
  int newLastSnapshotIncludeTerm = m_logs[getSlicesIndexFromLogIndex(index)].logterm();
  std::vector<raftRpcProctoc::LogEntry> trunckedLogs;
  // todo :这种写法有点笨，待改进，而且有内存泄漏的风险
  for (int i = index + 1; i <= getLastLogIndex(); i++)
  {
    // 注意有=，因为要拿到最后一个日志
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

    DPrintf("[StreamingSnapshot]Server %d streaming snapshot index {%d}, term {%d}, loglen {%d}", m_me, index,
            m_lastSnapshotIncludeTerm, m_logs.size());
  }
  else
  {
    DPrintf("[StreamingSnapshot]Server %d failed to save streaming snapshot", m_me);
  }

  myAssert(m_logs.size() + m_lastSnapshotIncludeIndex == lastLogIndex,
           format("len(rf.logs){%d} + rf.lastSnapshotIncludeIndex{%d} != lastLogjInde{%d}", m_logs.size(),
                  m_lastSnapshotIncludeIndex, lastLogIndex));
}

// ==================== 成员变更相关方法实现 ====================

bool Raft::AddNode(const std::string &nodeId, const std::string &address)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  if (m_status != Leader)
  {
    DPrintf("[AddNode] Node %d is not leader, cannot add node %s", m_me, nodeId.c_str());
    return false;
  }

  // 检查节点是否已存在
  if (m_nodeAddresses.find(nodeId) != m_nodeAddresses.end())
  {
    DPrintf("[AddNode] Node %s already exists", nodeId.c_str());
    return false;
  }

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      raftRpcProctoc::ADD_NODE, nodeId, address);

  // 添加到日志中
  m_logs.emplace_back(configEntry);

  DPrintf("[AddNode] Leader %d added config change log for adding node %s at %s",
          m_me, nodeId.c_str(), address.c_str());

  return true;
}

bool Raft::RemoveNode(const std::string &nodeId)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  if (m_status != Leader)
  {
    DPrintf("[RemoveNode] Node %d is not leader, cannot remove node %s", m_me, nodeId.c_str());
    return false;
  }

  // 检查节点是否存在
  if (m_nodeAddresses.find(nodeId) == m_nodeAddresses.end())
  {
    DPrintf("[RemoveNode] Node %s does not exist", nodeId.c_str());
    return false;
  }

  // 创建配置变更日志条目
  raftRpcProctoc::LogEntry configEntry = createConfigChangeEntry(
      raftRpcProctoc::REMOVE_NODE, nodeId, "");

  // 添加到日志中
  m_logs.emplace_back(configEntry);

  DPrintf("[RemoveNode] Leader %d added config change log for removing node %s",
          m_me, nodeId.c_str());

  return true;
}

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

void Raft::applyConfigChange(const raftRpcProctoc::ConfigChange &configChange)
{
  std::lock_guard<std::mutex> lock(m_mtx);

  const std::string &nodeId = configChange.nodeid();
  const std::string &address = configChange.address();

  if (configChange.type() == raftRpcProctoc::ADD_NODE)
  {
    // 添加节点
    if (m_nodeAddresses.find(nodeId) == m_nodeAddresses.end())
    {
      m_nodeAddresses[nodeId] = address;

      // 创建新的 RPC 连接
      auto newPeer = std::make_shared<RaftRpcUtil>(address.substr(0, address.find(':')),
                                                   std::stoi(address.substr(address.find(':') + 1)));
      m_peers.push_back(newPeer);

      // 更新索引映射
      int newIndex = m_peers.size() - 1;
      m_indexToNodeId[newIndex] = nodeId;
      m_nodeIdToIndex[nodeId] = newIndex;

      // 为新节点初始化 nextIndex 和 matchIndex
      m_nextIndex.push_back(getLastLogIndex() + 1);
      m_matchIndex.push_back(0);

      DPrintf("[applyConfigChange] Added node %s at %s, new cluster size: %d",
              nodeId.c_str(), address.c_str(), m_peers.size());
    }
  }
  else if (configChange.type() == raftRpcProctoc::REMOVE_NODE)
  {
    // 移除节点
    auto it = m_nodeIdToIndex.find(nodeId);
    if (it != m_nodeIdToIndex.end())
    {
      int removeIndex = it->second;

      // 移除 RPC 连接
      m_peers.erase(m_peers.begin() + removeIndex);

      // 移除索引映射
      m_nodeAddresses.erase(nodeId);
      m_indexToNodeId.erase(removeIndex);
      m_nodeIdToIndex.erase(nodeId);

      // 移除对应的 nextIndex 和 matchIndex
      m_nextIndex.erase(m_nextIndex.begin() + removeIndex);
      m_matchIndex.erase(m_matchIndex.begin() + removeIndex);

      // 更新其他节点的索引映射
      for (auto &pair : m_nodeIdToIndex)
      {
        if (pair.second > removeIndex)
        {
          pair.second--;
          m_indexToNodeId[pair.second] = pair.first;
        }
      }
      m_indexToNodeId.erase(m_indexToNodeId.upper_bound(removeIndex), m_indexToNodeId.end());

      DPrintf("[applyConfigChange] Removed node %s, new cluster size: %d",
              nodeId.c_str(), m_peers.size());

      // 如果移除的是当前节点自己，需要停止服务
      if (nodeId == std::to_string(m_me))
      {
        DPrintf("[applyConfigChange] Current node %d is being removed, shutting down", m_me);
        // 这里可以设置一个标志来优雅关闭
      }
    }
  }
}