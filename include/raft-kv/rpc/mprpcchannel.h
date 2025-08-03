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
#include <future>
#include <memory>
#include <atomic>
#include <mutex>
#include <chrono>
using namespace std;

// 前向声明
namespace monsoon
{
  class IOManager;
}

/**
 * @brief 异步RPC请求上下文
 */
struct AsyncRpcContext
{
  uint64_t requestId;                                                                // 请求ID
  std::function<void(bool, google::protobuf::Message *)> callback;                   // 回调函数
  std::shared_ptr<std::promise<std::unique_ptr<google::protobuf::Message>>> promise; // Promise对象
  std::unique_ptr<google::protobuf::Message> response;                               // 响应消息
  std::chrono::steady_clock::time_point startTime;                                   // 请求开始时间

  AsyncRpcContext(uint64_t id) : requestId(id), startTime(std::chrono::steady_clock::now()) {}
};

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
   * @brief RPC方法调用接口（同步版本）
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
   * @brief 异步RPC方法调用接口（回调风格）
   * @param method 方法描述符
   * @param controller RPC控制器
   * @param request 请求消息
   * @param response 响应消息
   * @param callback 完成回调函数
   */
  void CallMethodAsync(const google::protobuf::MethodDescriptor *method,
                       google::protobuf::RpcController *controller,
                       const google::protobuf::Message *request,
                       google::protobuf::Message *response,
                       std::function<void(bool, google::protobuf::Message *)> callback);

  /**
   * @brief 异步RPC方法调用接口（Future风格）
   * @param method 方法描述符
   * @param controller RPC控制器
   * @param request 请求消息
   * @param response 响应消息模板（用于创建实际响应对象）
   * @return Future对象，可用于获取响应结果
   */
  std::future<std::unique_ptr<google::protobuf::Message>> CallMethodAsync(
      const google::protobuf::MethodDescriptor *method,
      google::protobuf::RpcController *controller,
      const google::protobuf::Message *request,
      const google::protobuf::Message *response);

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

  // 异步RPC相关成员
  std::atomic<uint64_t> m_nextRequestId{1};                                         // 下一个请求ID
  std::unordered_map<uint64_t, std::shared_ptr<AsyncRpcContext>> m_pendingRequests; // 待处理的异步请求
  std::mutex m_pendingMutex;                                                        // 保护待处理请求的互斥锁
  monsoon::IOManager *m_ioManager;                                                  // IOManager指针
  bool m_isAsyncMode;                                                               // 是否处于异步模式

  /**
   * @brief 连接IP和端口，并设置m_clientFd
   * @param ip IP地址，本机字节序
   * @param port 端口，本机字节序
   * @param errMsg 错误信息输出参数
   * @return 成功返回true，否则返回false
   */
  bool newConnect(const char *ip, uint16_t port, string *errMsg);

  /**
   * @brief 异步发送RPC请求的核心实现
   * @param method 方法描述符
   * @param controller RPC控制器
   * @param request 请求消息
   * @param context 异步请求上下文
   * @return 是否成功发送
   */
  bool sendAsyncRequest(const google::protobuf::MethodDescriptor *method,
                        google::protobuf::RpcController *controller,
                        const google::protobuf::Message *request,
                        std::shared_ptr<AsyncRpcContext> context);

  /**
   * @brief 处理异步响应的回调函数
   * 当socket可读时，IOManager会调用此函数
   */
  void handleAsyncResponse();

  /**
   * @brief 生成请求ID
   * @return 唯一的请求ID
   */
  uint64_t generateRequestId() { return m_nextRequestId.fetch_add(1); }
};
