#pragma once

#include "raftRPC.pb.h"
#include <functional>
#include <memory>

/**
 * @brief Raft RPC通信工具类
 *
 * 维护当前节点对其他某一个节点的所有RPC发送通信功能。
 * 对于一个raft节点来说，对于任意其他的节点都要维护一个rpc连接，即MprpcChannel。
 * 提供同步和异步两种RPC调用方式，支持协程环境。
 */
class RaftRpcUtil
{
private:
  raftRpcProctoc::raftRpc_Stub *stub_; // RPC存根对象，用于发送RPC请求

public:
  // 同步RPC调用方法（保持向后兼容）
  /**
   * @brief 发送AppendEntries RPC请求
   * @param args 请求参数，包含日志条目和领导者信息
   * @param response 响应结果，包含跟随者的确认信息
   * @return 请求是否成功发送
   */
  bool AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response);

  /**
   * @brief 发送InstallSnapshot RPC请求
   * @param args 请求参数，包含快照数据和元信息
   * @param response 响应结果，包含安装确认信息
   * @return 请求是否成功发送
   */
  bool InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args, raftRpcProctoc::InstallSnapshotResponse *response);

  /**
   * @brief 发送RequestVote RPC请求
   * @param args 请求参数，包含候选者信息和日志状态
   * @param response 响应结果，包含投票决定
   * @return 请求是否成功发送
   */
  bool RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response);

  // 异步RPC调用方法（用于协程环境）
  using AppendEntriesCallback = std::function<void(bool success, raftRpcProctoc::AppendEntriesReply *response)>;
  using RequestVoteCallback = std::function<void(bool success, raftRpcProctoc::RequestVoteReply *response)>;
  using InstallSnapshotCallback = std::function<void(bool success, raftRpcProctoc::InstallSnapshotResponse *response)>;

  /**
   * @brief 异步发送AppendEntries RPC请求
   * @param args 请求参数
   * @param response 响应结果
   * @param callback 回调函数，处理异步响应
   *
   * 优先使用真正的异步接口，如果不可用则回退到协程或线程模式
   */
  void AppendEntriesAsync(raftRpcProctoc::AppendEntriesArgs *args,
                          raftRpcProctoc::AppendEntriesReply *response,
                          AppendEntriesCallback callback);

  /**
   * @brief 异步发送RequestVote RPC请求
   * @param args 请求参数
   * @param response 响应结果
   * @param callback 回调函数，处理异步响应
   *
   * 优先使用真正的异步接口，如果不可用则回退到协程或线程模式
   */
  void RequestVoteAsync(raftRpcProctoc::RequestVoteArgs *args,
                        raftRpcProctoc::RequestVoteReply *response,
                        RequestVoteCallback callback);

  /**
   * @brief 异步发送InstallSnapshot RPC请求
   * @param args 请求参数
   * @param response 响应结果
   * @param callback 回调函数，处理异步响应
   *
   * 优先使用真正的异步接口，如果不可用则回退到协程或线程模式
   */
  void InstallSnapshotAsync(raftRpcProctoc::InstallSnapshotRequest *args,
                            raftRpcProctoc::InstallSnapshotResponse *response,
                            InstallSnapshotCallback callback);

  /**
   * @brief 测试RPC连接是否可用
   * @return true 如果连接可用，false 否则
   *
   * 通过发送测试请求来检查RPC连接状态
   */
  bool testConnection();

  /**
   * @brief 构造函数
   * @param ip 远端节点IP地址
   * @param port 远端节点端口
   *
   * 创建RPC工具对象，使用延迟连接避免在构造时立即连接
   */
  RaftRpcUtil(std::string ip, short port);

  /**
   * @brief 析构函数
   *
   * 清理RPC存根对象，释放资源
   */
  ~RaftRpcUtil();
};
