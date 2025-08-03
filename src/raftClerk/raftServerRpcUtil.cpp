#include "raftServerRpcUtil.h"

/**
 * @brief 构造函数
 *
 * 建立与指定Raft服务器的RPC连接。
 * 先开启服务器，再尝试连接其他的节点，中间给一个间隔时间，等待其他的rpc服务器节点启动。
 *
 * @param ip 目标服务器的IP地址
 * @param port 目标服务器的端口号
 */
raftServerRpcUtil::raftServerRpcUtil(std::string ip, short port)
{
  // 创建RPC存根，用于与远程服务器通信
  // 发送rpc设置
  stub = new raftKVRpcProctoc::kvServerRpc_Stub(new MprpcChannel(ip, port, false));
}

/**
 * @brief 析构函数
 *
 * 清理RPC连接资源
 */
raftServerRpcUtil::~raftServerRpcUtil() { delete stub; }

/**
 * @brief 执行Get操作的RPC调用
 *
 * 向远程Raft服务器发送Get请求，获取指定键的值。
 *
 * @param GetArgs Get请求参数
 * @param reply Get响应结果
 * @return 是否调用成功
 */
bool raftServerRpcUtil::Get(raftKVRpcProctoc::GetArgs *GetArgs, raftKVRpcProctoc::GetReply *reply)
{
  MprpcController controller;
  stub->Get(&controller, GetArgs, reply, nullptr);
  return !controller.Failed();
}

/**
 * @brief 执行Put/Append操作的RPC调用
 *
 * 向远程Raft服务器发送Put或Append请求，设置或追加键值对。
 *
 * @param args Put/Append请求参数
 * @param reply Put/Append响应结果
 * @return 是否调用成功
 */
bool raftServerRpcUtil::PutAppend(raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply)
{
  MprpcController controller;
  stub->PutAppend(&controller, args, reply, nullptr);
  if (controller.Failed())
  {
    std::cout << controller.ErrorText() << endl;
  }
  return !controller.Failed();
}
