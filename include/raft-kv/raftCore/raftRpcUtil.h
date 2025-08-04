#pragma once

#include "raftRPC.pb.h"
#include <functional>
#include <memory>

/// @brief 维护当前节点对其他某一个结点的所有rpc发送通信的功能
// 对于一个raft节点来说，对于任意其他的节点都要维护一个rpc连接，即MprpcChannel
class RaftRpcUtil
{
private:
  raftRpcProctoc::raftRpc_Stub *stub_;

public:
  // 同步RPC调用方法（保持向后兼容）
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response);
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);

  // 异步RPC调用方法（用于协程环境）
  using AppendEntriesCallback = std::function<void(bool success, raftRpcProctoc::AppendEntriesReply *response)>;
  using RequestVoteCallback = std::function<void(bool success, raftRpcProctoc::RequestVoteReply *response)>;
  using InstallSnapshotCallback = std::function<void(bool success, raftRpcProctoc::InstallSnapshotResponse *response)>;

  void AppendEntriesAsync(raftRpcProctoc::AppendEntriesArgs *args,
                          raftRpcProctoc::AppendEntriesReply *response,
                          AppendEntriesCallback callback);
  void RequestVoteAsync(raftRpcProctoc::RequestVoteArgs *args,
                        raftRpcProctoc::RequestVoteReply *response,
                        RequestVoteCallback callback);
  void InstallSnapshotAsync(raftRpcProctoc::InstallSnapshotRequest *args,
                            raftRpcProctoc::InstallSnapshotResponse *response,
                            InstallSnapshotCallback callback);

  /**
   * @brief 测试RPC连接是否可用
   * @return true 如果连接可用，false 否则
   */
  bool testConnection();

  /**
   * @param ip  远端ip
   * @param port  远端端口
   */
  RaftRpcUtil(std::string ip, short port);
  ~RaftRpcUtil();
};
