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
  // 发送rpc设置
  stub_ = new raftRpcProctoc::raftRpc_Stub(new MprpcChannel(ip, port, true));
}

RaftRpcUtil::~RaftRpcUtil() { delete stub_; }
