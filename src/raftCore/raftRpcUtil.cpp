#include "raftRpcUtil.h"

#include "raft-kv/rpc/mprpcchannel.h"
#include "raft-kv/rpc/mprpccontroller.h"
#include "raft-kv/fiber/monsoon.h"
#include <thread>

/**
 * @brief 发送AppendEntries RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @return 请求是否成功
 *
 * 同步发送AppendEntries RPC请求，用于领导者向跟随者发送日志条目
 */
bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response)
{
  MprpcController controller;
  stub_->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}

/**
 * @brief 发送InstallSnapshot RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @return 请求是否成功
 *
 * 同步发送InstallSnapshot RPC请求，用于领导者向跟随者发送快照数据
 */
bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response)
{
  MprpcController controller;
  stub_->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}

/**
 * @brief 发送RequestVote RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @return 请求是否成功
 *
 * 同步发送RequestVote RPC请求，用于候选者向其他节点请求投票
 */
bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response)
{
  MprpcController controller;
  stub_->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

/**
 * @brief 异步发送AppendEntries RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @param callback 回调函数
 *
 * 异步发送AppendEntries RPC请求，优先使用真正的异步接口，
 * 如果不可用则回退到协程或线程模式
 */
void RaftRpcUtil::AppendEntriesAsync(raftRpcProctoc::AppendEntriesArgs *args,
                                     raftRpcProctoc::AppendEntriesReply *response,
                                     AppendEntriesCallback callback)
{
  // 尝试获取底层的MprpcChannel以使用真正的异步接口
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口，避免阻塞
    MprpcController controller;

    // 获取RPC服务的方法描述符
    const google::protobuf::ServiceDescriptor *service_desc =
        raftRpcProctoc::raftRpc::descriptor();
    const google::protobuf::MethodDescriptor *method_desc =
        service_desc->FindMethodByName("AppendEntries");

    // 调用异步接口，通过lambda回调处理结果
    channel->CallMethodAsync(method_desc, &controller, args, response,
                             [callback](bool success, google::protobuf::Message *msg)
                             {
                               auto *reply = dynamic_cast<raftRpcProctoc::AppendEntriesReply *>(msg);
                               callback(success, reply);
                             });
  }
  else
  {
    // 回退到协程模式，使用IOManager调度
    auto ioManager = monsoon::IOManager::GetThis();
    if (ioManager)
    {
      ioManager->scheduler([this, args, response, callback]()
                           {
        bool success = this->AppendEntries(args, response);
        callback(success, response); });
    }
    else
    {
      // 如果没有IOManager，回退到线程模式
      std::thread([this, args, response, callback]()
                  {
        bool success = this->AppendEntries(args, response);
        callback(success, response); })
          .detach();
    }
  }
}

/**
 * @brief 异步发送RequestVote RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @param callback 回调函数，处理异步响应
 */
void RaftRpcUtil::RequestVoteAsync(raftRpcProctoc::RequestVoteArgs *args,
                                   raftRpcProctoc::RequestVoteReply *response,
                                   RequestVoteCallback callback)
{
  // 尝试获取底层的MprpcChannel以使用真正的异步接口
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口
    MprpcController controller;

    // 获取RPC服务的方法描述符
    const google::protobuf::ServiceDescriptor *service_desc =
        raftRpcProctoc::raftRpc::descriptor();
    const google::protobuf::MethodDescriptor *method_desc =
        service_desc->FindMethodByName("RequestVote");

    // 调用异步接口
    channel->CallMethodAsync(method_desc, &controller, args, response,
                             [callback](bool success, google::protobuf::Message *msg)
                             {
                               auto *reply = dynamic_cast<raftRpcProctoc::RequestVoteReply *>(msg);
                               callback(success, reply);
                             });
  }
  else
  {
    // 回退到协程模式
    auto ioManager = monsoon::IOManager::GetThis();
    if (ioManager)
    {
      ioManager->scheduler([this, args, response, callback]()
                           {
        bool success = this->RequestVote(args, response);
        callback(success, response); });
    }
    else
    {
      // 回退到线程模式
      std::thread([this, args, response, callback]()
                  {
        bool success = this->RequestVote(args, response);
        callback(success, response); })
          .detach();
    }
  }
}

/**
 * @brief 异步发送InstallSnapshot RPC请求
 * @param args 请求参数
 * @param response 响应结果
 * @param callback 回调函数，处理异步响应
 */
void RaftRpcUtil::InstallSnapshotAsync(raftRpcProctoc::InstallSnapshotRequest *args,
                                       raftRpcProctoc::InstallSnapshotResponse *response,
                                       InstallSnapshotCallback callback)
{
  // 尝试获取底层的MprpcChannel以使用真正的异步接口
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口
    MprpcController controller;

    // 获取RPC服务的方法描述符
    const google::protobuf::ServiceDescriptor *service_desc =
        raftRpcProctoc::raftRpc::descriptor();
    const google::protobuf::MethodDescriptor *method_desc =
        service_desc->FindMethodByName("InstallSnapshot");

    // 调用异步接口
    channel->CallMethodAsync(method_desc, &controller, args, response,
                             [callback](bool success, google::protobuf::Message *msg)
                             {
                               auto *reply = dynamic_cast<raftRpcProctoc::InstallSnapshotResponse *>(msg);
                               callback(success, reply);
                             });
  }
  else
  {
    // 回退到协程模式
    auto ioManager = monsoon::IOManager::GetThis();
    if (ioManager)
    {
      ioManager->scheduler([this, args, response, callback]()
                           {
        bool success = this->InstallSnapshot(args, response);
        callback(success, response); });
    }
    else
    {
      // 回退到线程模式
      std::thread([this, args, response, callback]()
                  {
        bool success = this->InstallSnapshot(args, response);
        callback(success, response); })
          .detach();
    }
  }
}

/**
 * @brief 构造函数
 * @param ip 目标节点IP地址
 * @param port 目标节点端口
 *
 * 创建RPC工具对象，使用延迟连接避免在构造时立即连接
 * 这样可以避免在其他节点RPC服务未就绪时的连接失败
 */
RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
  // 创建RPC存根，使用延迟连接模式
  // 延迟连接可以避免在构造时立即连接，防止其他节点RPC服务未就绪时的连接失败
  stub_ = new raftRpcProctoc::raftRpc_Stub(new MprpcChannel(ip, port, false));
}

/**
 * @brief 测试连接状态
 * @return 连接是否可用
 *
 * 通过发送一个测试请求来检查RPC连接是否可用
 * @return 连接是否可用
 */
bool RaftRpcUtil::testConnection()
{
  try
  {
    // 创建一个简单的测试请求，使用无效参数作为测试标识
    raftRpcProctoc::RequestVoteArgs testArgs;
    testArgs.set_term(-1); // 使用无效term作为测试标识
    testArgs.set_candidateid(-1);
    testArgs.set_lastlogindex(-1);
    testArgs.set_lastlogterm(-1);

    raftRpcProctoc::RequestVoteReply testReply;

    // 尝试发送测试请求
    // 注意：stub_->RequestVote 返回 void，我们通过是否抛出异常来判断连接状态
    stub_->RequestVote(nullptr, &testArgs, &testReply, nullptr);

    // 如果没有抛出异常，说明连接基本可用
    return true;
  }
  catch (const std::exception &e)
  {
    return false; // 连接不可用
  }
}

/**
 * @brief 析构函数
 *
 * 清理RPC存根对象
 */
RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
