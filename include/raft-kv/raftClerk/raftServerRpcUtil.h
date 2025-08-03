#pragma once

#include <iostream>
#include "kvServerRPC.pb.h"
#include "raft-kv/rpc/mprpcchannel.h"
#include "raft-kv/rpc/mprpccontroller.h"
#include "raft-kv/rpc/rpcprovider.h"

/**
 * @brief Raft服务器RPC工具类
 *
 * 维护当前节点对其他某一个节点的所有RPC通信，包括接收其他节点的RPC和发送。
 * 对于一个节点来说，对于任意其他的节点都要维护一个RPC连接。
 *
 * 该类封装了与特定Raft服务器的RPC通信功能，提供：
 * - Get操作的RPC调用
 * - Put/Append操作的RPC调用
 * - 连接管理和错误处理
 */
class raftServerRpcUtil
{
private:
  raftKVRpcProctoc::kvServerRpc_Stub *stub; // RPC存根，用于与远程服务器通信

public:
  /**
   * @brief 执行Get操作的RPC调用
   * @param GetArgs Get请求参数
   * @param reply Get响应结果
   * @return 是否调用成功
   *
   * 主动调用其他节点的Get方法，可以按照MIT 6.824来调用，
   * 但是别的节点调用自己的好像就不行了，要继承protoc提供的service类才行
   */
  bool Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply);

  /**
   * @brief 执行Put/Append操作的RPC调用
   * @param args Put/Append请求参数
   * @param reply Put/Append响应结果
   * @return 是否调用成功
   *
   * 主动调用其他节点的Put/Append方法
   */
  bool PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  /**
   * @brief 构造函数
   * @param ip 目标服务器的IP地址
   * @param port 目标服务器的端口号
   *
   * 建立与指定服务器的RPC连接
   */
  raftServerRpcUtil(std::string ip, short port);

  /**
   * @brief 析构函数
   *
   * 清理RPC连接资源
   */
  ~raftServerRpcUtil();
};
