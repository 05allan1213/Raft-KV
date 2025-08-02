#pragma once

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/service.h>
#include <algorithm>
#include <functional>
#include <iostream>
#include <map>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>
using namespace std;

/**
 * @brief RPC通道类，继承自google::protobuf::RpcChannel
 *
 * 真正负责发送和接受的前后处理工作，如消息的组织方式，向哪个节点发送等等。
 * 实现了protobuf RPC框架的通道接口，负责RPC调用的网络传输部分。
 */
class MprpcChannel : public google::protobuf::RpcChannel
{
public:
  /**
   * @brief RPC方法调用接口
   * @param method 方法描述符
   * @param controller RPC控制器
   * @param request 请求消息
   * @param response 响应消息
   * @param done 完成回调
   *
   * 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据数据序列化和网络发送那一步
   */
  void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                  const google::protobuf::Message *request, google::protobuf::Message *response,
                  google::protobuf::Closure *done) override;

  /**
   * @brief 构造函数
   * @param ip 服务器IP地址
   * @param port 服务器端口
   * @param connectNow 是否立即连接
   */
  MprpcChannel(string ip, short port, bool connectNow);

private:
  int m_clientFd;         // 客户端socket文件描述符
  const std::string m_ip; // 保存ip和端口，如果断了可以尝试重连
  const uint16_t m_port;  // 服务器端口号

  /**
   * @brief 连接IP和端口，并设置m_clientFd
   * @param ip IP地址，本机字节序
   * @param port 端口，本机字节序
   * @param errMsg 错误信息输出参数
   * @return 成功返回true，否则返回false
   */
  bool newConnect(const char *ip, uint16_t port, string *errMsg);
};
