#include "raftRpcUtil.h"

#include "raft-kv/rpc/mprpcchannel.h"
#include "raft-kv/rpc/mprpccontroller.h"
#include "raft-kv/fiber/monsoon.h"
#include <thread>

bool RaftRpcUtil::AppendEntries(raftRpcProctoc::AppendEntriesArgs *args, raftRpcProctoc::AppendEntriesReply *response)
{
  MprpcController controller;
  stub_->AppendEntries(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::InstallSnapshot(raftRpcProctoc::InstallSnapshotRequest *args,
                                  raftRpcProctoc::InstallSnapshotResponse *response)
{
  MprpcController controller;
  stub_->InstallSnapshot(&controller, args, response, nullptr);
  return !controller.Failed();
}

bool RaftRpcUtil::RequestVote(raftRpcProctoc::RequestVoteArgs *args, raftRpcProctoc::RequestVoteReply *response)
{
  MprpcController controller;
  stub_->RequestVote(&controller, args, response, nullptr);
  return !controller.Failed();
}

// 异步RPC实现 - 使用真正的异步接口
void RaftRpcUtil::AppendEntriesAsync(raftRpcProctoc::AppendEntriesArgs *args,
                                     raftRpcProctoc::AppendEntriesReply *response,
                                     AppendEntriesCallback callback)
{
  // 获取底层的MprpcChannel
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口
    MprpcController controller;

    // 获取方法描述符
    const google::protobuf::ServiceDescriptor *service_desc =
        raftRpcProctoc::raftRpc::descriptor();
    const google::protobuf::MethodDescriptor *method_desc =
        service_desc->FindMethodByName("AppendEntries");

    // 调用异步接口
    channel->CallMethodAsync(method_desc, &controller, args, response,
                             [callback](bool success, google::protobuf::Message *msg)
                             {
                               auto *reply = dynamic_cast<raftRpcProctoc::AppendEntriesReply *>(msg);
                               callback(success, reply);
                             });
  }
  else
  {
    // 回退到原有的协程模式
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

void RaftRpcUtil::RequestVoteAsync(raftRpcProctoc::RequestVoteArgs *args,
                                   raftRpcProctoc::RequestVoteReply *response,
                                   RequestVoteCallback callback)
{
  // 获取底层的MprpcChannel
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口
    MprpcController controller;

    // 获取方法描述符
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
    // 回退到原有的协程模式
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
      std::thread([this, args, response, callback]()
                  {
        bool success = this->RequestVote(args, response);
        callback(success, response); })
          .detach();
    }
  }
}

void RaftRpcUtil::InstallSnapshotAsync(raftRpcProctoc::InstallSnapshotRequest *args,
                                       raftRpcProctoc::InstallSnapshotResponse *response,
                                       InstallSnapshotCallback callback)
{
  // 获取底层的MprpcChannel
  MprpcChannel *channel = dynamic_cast<MprpcChannel *>(stub_->channel());
  if (channel)
  {
    // 使用真正的异步接口
    MprpcController controller;

    // 获取方法描述符
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
    // 回退到原有的协程模式
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
      std::thread([this, args, response, callback]()
                  {
        bool success = this->InstallSnapshot(args, response);
        callback(success, response); })
          .detach();
    }
  }
}

// 先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动

RaftRpcUtil::RaftRpcUtil(std::string ip, short port)
{
  //*********************************************  */
  // 发送rpc设置 - 使用延迟连接，避免在构造时立即连接
  // 这样可以避免在其他节点RPC服务未就绪时的连接失败
  stub_ = new raftRpcProctoc::raftRpc_Stub(new MprpcChannel(ip, port, false));
}

bool RaftRpcUtil::testConnection()
{
  try
  {
    // 创建一个简单的测试请求
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

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
