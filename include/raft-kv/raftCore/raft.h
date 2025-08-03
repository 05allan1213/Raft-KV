#pragma once

#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "ApplyMsg.h"
#include "Persister.h"
#include "boost/any.hpp"
#include "boost/serialization/serialization.hpp"
#include "raft-kv/common/config.h"
#include "raft-kv/fiber/monsoon.h"
#include "raft-kv/raftCore/raftRpcUtil.h"
#include "raft-kv/common/util.h"

/**
 * @brief 网络状态常量定义
 * @note 网络状态表示，实际生产中可以在rpc中删除该字段，主要用于网络分区时的调试
 */
constexpr int Disconnected = 0; // 网络异常时为disconnected，网络正常时为AppNormal，防止matchIndex[]数组异常减小
constexpr int AppNormal = 1;    // 网络正常状态

/**
 * @brief 投票状态常量定义
 */
constexpr int Killed = 0; // 节点已终止
constexpr int Voted = 1;  // 本轮已经投过票了
constexpr int Expire = 2; // 投票（消息、竞选者）过期
constexpr int Normal = 3; // 正常状态

/**
 * @brief Raft共识算法实现类
 *
 * 该类实现了Raft共识算法的核心功能，包括：
 * - 领导者选举
 * - 日志复制
 * - 安全性保证
 * - 成员变更
 * - 快照机制
 *
 * Raft算法通过选举机制确保集群中只有一个领导者，
 * 领导者负责处理所有客户端请求并复制日志到其他节点。
 */
class Raft : public raftRpcProctoc::raftRpc
{
private:
  std::mutex m_mtx;                                  // 互斥锁，保护共享数据
  std::vector<std::shared_ptr<RaftRpcUtil>> m_peers; // 集群中所有节点的RPC客户端
  std::shared_ptr<Persister> m_persister;            // 持久化存储对象
  int m_me;                                          // 当前节点的ID
  int m_currentTerm;                                 // 当前任期号
  int m_votedFor;                                    // 当前任期内投票给哪个候选者，-1表示未投票
  std::vector<raftRpcProctoc::LogEntry> m_logs;      // 日志条目数组，包含状态机要执行的指令集和任期号
  int m_commitIndex;                                 // 已知已提交的最高日志条目的索引
  int m_lastApplied;                                 // 已经应用到状态机的最高日志条目的索引

  // 这两个状态由服务器维护，易失性数据
  std::vector<int> m_nextIndex;  // 对于每个服务器，发送到该服务器的下一个日志条目的索引
  std::vector<int> m_matchIndex; // 对于每个服务器，已知的已在该服务器上复制的最高日志条目的索引

  /**
   * @brief 节点状态枚举
   */
  enum Status
  {
    Follower,  // 跟随者状态
    Candidate, // 候选者状态
    Leader     // 领导者状态
  };

  Status m_status; // 当前节点状态

  std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // 客户端从这里取日志，client与raft通信的接口

  // 选举超时相关
  std::chrono::_V2::system_clock::time_point m_lastResetElectionTime; // 上次重置选举超时的时间点
  std::chrono::_V2::system_clock::time_point m_lastResetHearBeatTime; // 上次重置心跳超时的时间点

  // 快照相关
  int m_lastSnapshotIncludeIndex; // 快照中包含的最后一个日志的索引
  int m_lastSnapshotIncludeTerm;  // 快照中包含的最后一个日志的任期

  // 协程管理器
  std::unique_ptr<monsoon::IOManager> m_ioManager = nullptr; // IO管理器，用于协程调度

public:
  /**
   * @brief 处理AppendEntries RPC请求
   * @param args RPC请求参数
   * @param reply RPC响应结果
   *
   * 处理来自领导者的日志复制请求，包括：
   * - 验证任期号
   * - 检查日志一致性
   * - 追加或覆盖日志条目
   * - 更新提交索引
   */
  void AppendEntries1(const raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *reply);

  /**
   * @brief 应用日志到状态机的定时器
   *
   * 定期检查并应用已提交但未应用的日志条目到状态机
   */
  void applierTicker();

  /**
   * @brief 条件安装快照
   * @param lastIncludedTerm 快照中最后一个日志条目的任期
   * @param lastIncludedIndex 快照中最后一个日志条目的索引
   * @param snapshot 快照数据
   * @return 是否成功安装快照
   */
  bool CondInstallSnapshot(int lastIncludedTerm, int lastIncludedIndex, std::string snapshot);

  /**
   * @brief 执行领导者选举
   *
   * 当节点成为候选者时，向其他节点发送投票请求
   */
  void doElection();

  /**
   * @brief 发起心跳，只有leader才需要发起心跳
   *
   * 领导者定期向其他节点发送心跳消息，维持领导地位
   */
  void doHeartBeat();

  /**
   * @brief 选举超时检查器
   *
   * 每隔一段时间检查睡眠时间内有没有重置定时器，
   * 没有则说明超时了，如果有则设置合适睡眠时间
   */
  void electionTimeOutTicker();

  /**
   * @brief 获取待应用的日志列表
   * @return 待应用的日志消息列表
   */
  std::vector<ApplyMsg> getApplyLogs();

  /**
   * @brief 获取新的命令索引
   * @return 新命令的索引号
   */
  int getNewCommandIndex();

  /**
   * @brief 获取指定服务器的前一个日志信息
   * @param server 服务器ID
   * @param preIndex 前一个日志索引
   * @param preTerm 前一个日志任期
   */
  void getPrevLogInfo(int server, int *preIndex, int *preTerm);

  /**
   * @brief 获取当前状态
   * @param term 当前任期
   * @param isLeader 是否为领导者
   */
  void GetState(int *term, bool *isLeader);

  /**
   * @brief 安装快照
   * @param args 快照安装请求参数
   * @param reply 快照安装响应结果
   */
  void InstallSnapshot(const raftRpcProctoc::InstallSnapshotRequest *args,
                       raftRpcProctoc::InstallSnapshotResponse *reply);

  /**
   * @brief 领导者心跳定时器
   *
   * 领导者定期发送心跳消息的定时器
   */
  void leaderHearBeatTicker();

  /**
   * @brief 领导者发送快照给指定服务器
   * @param server 目标服务器ID
   */
  void leaderSendSnapShot(int server);

  /**
   * @brief 领导者更新提交索引
   *
   * 领导者根据多数派节点的日志复制情况更新提交索引
   */
  void leaderUpdateCommitIndex();

  /**
   * @brief 检查日志是否匹配
   * @param logIndex 日志索引
   * @param logTerm 日志任期
   * @return 是否匹配
   */
  bool matchLog(int logIndex, int logTerm);

  /**
   * @brief 持久化当前状态
   *
   * 将当前状态保存到持久化存储中
   */
  void persist();

  /**
   * @brief 处理投票请求
   * @param args 投票请求参数
   * @param reply 投票响应结果
   */
  void RequestVote(const raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *reply);

  /**
   * @brief 检查日志是否最新
   * @param index 日志索引
   * @param term 日志任期
   * @return 是否最新
   */
  bool UpToDate(int index, int term);

  /**
   * @brief 获取最后一个日志的索引
   * @return 最后一个日志的索引
   */
  int getLastLogIndex();

  /**
   * @brief 获取最后一个日志的任期
   * @return 最后一个日志的任期
   */
  int getLastLogTerm();

  /**
   * @brief 获取最后一个日志的索引和任期
   * @param lastLogIndex 最后一个日志索引
   * @param lastLogTerm 最后一个日志任期
   */
  void getLastLogIndexAndTerm(int *lastLogIndex, int *lastLogTerm);

  /**
   * @brief 根据日志索引获取日志任期
   * @param logIndex 日志索引
   * @return 日志任期
   */
  int getLogTermFromLogIndex(int logIndex);

  /**
   * @brief 获取Raft状态大小
   * @return Raft状态的大小
   */
  int GetRaftStateSize();

  /**
   * @brief 根据日志索引获取切片索引
   * @param logIndex 日志索引
   * @return 切片索引
   */
  int getSlicesIndexFromLogIndex(int logIndex);

  /**
   * @brief 发送投票请求
   * @param server 目标服务器ID
   * @param args 投票请求参数
   * @param reply 投票响应结果
   * @param votedNum 投票数量
   * @return 是否发送成功
   */
  bool sendRequestVote(int server, std::shared_ptr<raftRpcProctoc::RequestVoteArgs> args,
                       std::shared_ptr<raftRpcProctoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);

  /**
   * @brief 发送日志复制请求
   * @param server 目标服务器ID
   * @param args 日志复制请求参数
   * @param reply 日志复制响应结果
   * @param appendNums 追加数量
   * @return 是否发送成功
   */
  bool sendAppendEntries(int server, std::shared_ptr<raftRpcProctoc::AppendEntriesArgs> args,
                         std::shared_ptr<raftRpcProctoc::AppendEntriesReply> reply, std::shared_ptr<int> appendNums);

  /**
   * @brief 向KV服务器推送消息
   * @param msg 要推送的消息
   *
   * 不拿锁执行，可以单独创建一个线程执行，但是为了统一使用std::thread，
   * 避免使用pthread_create，因此专门写一个函数来执行
   */
  void pushMsgToKvServer(ApplyMsg msg);

  /**
   * @brief 从持久化存储读取状态
   * @param data 持久化数据
   */
  void readPersist(std::string data);

  /**
   * @brief 获取持久化数据
   * @return 持久化数据字符串
   */
  std::string persistData();

  /**
   * @brief 开始处理客户端命令
   * @param command 客户端命令
   * @param newLogIndex 新日志索引
   * @param newLogTerm 新日志任期
   * @param isLeader 是否为领导者
   */
  void Start(Op command, int *newLogIndex, int *newLogTerm, bool *isLeader);

  /**
   * @brief 创建快照
   * @param index 快照包含的最后一个日志索引
   * @param snapshot 快照数据
   *
   * 服务层主动发起请求raft保存snapshot里面的数据，
   * index用来表示snapshot快照执行到了哪条命令
   */
  void Snapshot(int index, std::string snapshot);

public:
  // 重写基类方法，因为rpc远程调用真正调用的是这个方法
  // 序列化，反序列化等操作rpc框架都已经做完了，因此这里只需要获取值然后真正调用本地方法即可。

  /**
   * @brief RPC接口：处理AppendEntries请求
   */
  void AppendEntries(google::protobuf::RpcController *controller, const ::raftRpcProctoc::AppendEntriesArgs *request,
                     ::raftRpcProctoc::AppendEntriesReply *response, ::google::protobuf::Closure *done) override;

  /**
   * @brief RPC接口：处理InstallSnapshot请求
   */
  void InstallSnapshot(google::protobuf::RpcController *controller,
                       const ::raftRpcProctoc::InstallSnapshotRequest *request,
                       ::raftRpcProctoc::InstallSnapshotResponse *response, ::google::protobuf::Closure *done) override;

  /**
   * @brief RPC接口：处理RequestVote请求
   */
  void RequestVote(google::protobuf::RpcController *controller, const ::raftRpcProctoc::RequestVoteArgs *request,
                   ::raftRpcProctoc::RequestVoteReply *response, ::google::protobuf::Closure *done) override;

public:
  /**
   * @brief 初始化Raft节点
   * @param peers 集群中的所有节点
   * @param me 当前节点ID
   * @param persister 持久化存储对象
   * @param applyCh 应用通道
   */
  void init(std::vector<std::shared_ptr<RaftRpcUtil>> peers, int me, std::shared_ptr<Persister> persister,
            std::shared_ptr<LockQueue<ApplyMsg>> applyCh);

private:
  /**
   * @brief 用于持久化的Raft节点数据类
   *
   * 该类用于序列化和反序列化Raft节点的持久化数据
   */
  class BoostPersistRaftNode
  {
  public:
    friend class boost::serialization::access;

    /**
     * @brief 序列化/反序列化函数
     * @param ar 归档对象
     * @param version 版本号
     */
    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
      ar & m_currentTerm;
      ar & m_votedFor;
      ar & m_lastSnapshotIncludeIndex;
      ar & m_lastSnapshotIncludeTerm;
      ar & m_logs;
    }

    int m_currentTerm;                         // 当前任期
    int m_votedFor;                            // 投票给谁
    int m_lastSnapshotIncludeIndex;            // 快照包含的最后一个日志索引
    int m_lastSnapshotIncludeTerm;             // 快照包含的最后一个日志任期
    std::vector<std::string> m_logs;           // 日志条目
    std::unordered_map<std::string, int> umap; // 映射表
  };
};
