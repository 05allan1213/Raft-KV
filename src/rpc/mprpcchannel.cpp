#include "mprpcchannel.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <string>
#include "mprpccontroller.h"
#include "rpcheader.pb.h"
#include "util.h"
#include "raft-kv/fiber/iomanager.h"

/**
 * @brief RPC方法调用接口实现
 * @param method 方法描述符
 * @param controller RPC控制器
 * @param request 请求消息
 * @param response 响应消息
 * @param done 完成回调
 *
 * 所有通过stub代理对象调用的rpc方法，都会走到这里了，
 * 统一通过rpcChannel来调用方法，统一做rpc方法调用的数据序列化和网络发送
 *
 * 数据格式：
 * header_size + service_name method_name args_size + args
 */
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller, const google::protobuf::Message *request,
                              google::protobuf::Message *response, google::protobuf::Closure *done)
{
  // 检查连接状态，如果未连接则尝试建立连接
  if (m_clientFd == -1)
  {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt)
    {
      DPrintf("[func-MprpcChannel::CallMethod]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
      controller->SetFailed(errMsg);
      return;
    }
    else
    {
      DPrintf("[func-MprpcChannel::CallMethod]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
    }
  }

  // 获取服务和方法信息
  const google::protobuf::ServiceDescriptor *sd = method->service();
  std::string service_name = sd->name();    // 服务名称
  std::string method_name = method->name(); // 方法名称

  // 序列化请求参数
  uint32_t args_size{}; // 参数大小
  std::string args_str; // 序列化后的参数字符串
  if (request->SerializeToString(&args_str))
  {
    args_size = args_str.size();
  }
  else
  {
    controller->SetFailed("serialize request error!");
    return;
  }

  // 构建RPC头部信息
  mprpc::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);

  // 序列化RPC头部
  std::string rpc_header_str;
  if (!rpcHeader.SerializeToString(&rpc_header_str))
  {
    controller->SetFailed("serialize rpc header error!");
    return;
  }

  // 使用protobuf的CodedOutputStream来构建发送的数据流
  std::string send_rpc_str; // 用来存储最终发送的数据
  {
    // 创建一个StringOutputStream用于写入send_rpc_str
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    // 先写入header的长度（变长编码）
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    // 不需要手动写入header_size，因为上面的WriteVarint32已经包含了header的长度信息
    // 然后写入rpc_header本身
    coded_output.WriteString(rpc_header_str);
  }

  // 最后，将请求参数附加到send_rpc_str后面
  send_rpc_str += args_str;

  // 发送rpc请求
  // 失败会重试连接再发送，重试连接失败会直接return
  while (-1 == send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0))
  {
    char errtxt[512] = {0};
    sprintf(errtxt, "send error! errno:%d", errno);
    std::cout << "尝试重新连接，对方ip：" << m_ip << " 对方端口" << m_port << std::endl;
    close(m_clientFd); // 关闭当前连接
    m_clientFd = -1;   // 重置连接状态
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg); // 尝试重新连接
    if (!rt)
    {
      controller->SetFailed(errMsg);
      return;
    }
  }

  /*
  从时间节点来说，这里将请求发送过去之后rpc服务的提供者就会开始处理，返回的时候就代表着已经返回响应了
   */

  // 接收rpc请求的响应值
  char recv_buf[1024] = {0}; // 接收缓冲区
  int recv_size = 0;
  if (-1 == (recv_size = recv(m_clientFd, recv_buf, 1024, 0)))
  {
    close(m_clientFd);
    m_clientFd = -1;
    char errtxt[512] = {0};
    sprintf(errtxt, "recv error! errno:%d", errno);
    controller->SetFailed(errtxt);
    return;
  }

  // 反序列化rpc调用的响应数据
  // std::string response_str(recv_buf, 0, recv_size); //
  // bug：出现问题，recv_buf中遇到\0后面的数据就存不下来了，导致反序列化失败 if
  // (!response->ParseFromString(response_str))
  if (!response->ParseFromArray(recv_buf, recv_size))
  {
    char errtxt[1050] = {0};
    sprintf(errtxt, "parse error! response_str:%s", recv_buf);
    controller->SetFailed(errtxt);
    return;
  }
}

/**
 * @brief 连接IP和端口，并设置m_clientFd
 * @param ip IP地址，本机字节序
 * @param port 端口，本机字节序
 * @param errMsg 错误信息输出参数
 * @return 成功返回true，否则返回false
 *
 * 创建TCP连接并设置socket文件描述符
 */
bool MprpcChannel::newConnect(const char *ip, uint16_t port, string *errMsg)
{
  // 创建TCP socket
  int clientfd = socket(AF_INET, SOCK_STREAM, 0);
  if (-1 == clientfd)
  {
    char errtxt[512] = {0};
    sprintf(errtxt, "create socket error! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }

  // 设置服务器地址结构
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;            // IPv4
  server_addr.sin_port = htons(port);          // 端口号，转换为网络字节序
  server_addr.sin_addr.s_addr = inet_addr(ip); // IP地址，转换为网络字节序

  // 连接rpc服务节点
  if (-1 == connect(clientfd, (struct sockaddr *)&server_addr, sizeof(server_addr)))
  {
    close(clientfd); // 连接失败，关闭socket
    char errtxt[512] = {0};
    sprintf(errtxt, "connect fail! errno:%d", errno);
    m_clientFd = -1;
    *errMsg = errtxt;
    return false;
  }
  m_clientFd = clientfd; // 连接成功，保存socket文件描述符
  return true;
}

/**
 * @brief 构造函数
 * @param ip 服务器IP地址
 * @param port 服务器端口
 * @param connectNow 是否立即连接
 *
 * 使用tcp编程，完成rpc方法的远程调用，使用的是短连接，因此每次都要重新连接上去，待改成长连接。
 * 没有连接或者连接已经断开，那么就要重新连接呢,会一直不断地重试
 */
MprpcChannel::MprpcChannel(string ip, short port, bool connectNow)
    : m_ip(ip), m_port(port), m_clientFd(-1), m_ioManager(nullptr), m_isAsyncMode(false)
{
  // 尝试获取当前的IOManager
  m_ioManager = monsoon::IOManager::GetThis();
  if (m_ioManager)
  {
    m_isAsyncMode = true;
    DPrintf("[MprpcChannel] 检测到IOManager，启用异步模式");
  }

  // 读取配置文件rpcserver的信息
  // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
  // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
  // rpc调用方想调用service_name的method_name服务，需要查询zk上该服务所在的host信息
  //  /UserServiceRpc/Login
  if (!connectNow)
  {
    return; // 可以允许延迟连接
  }

  // 尝试建立连接，最多重试3次
  std::string errMsg;
  auto rt = newConnect(ip.c_str(), port, &errMsg);
  int tryCount = 3;
  while (!rt && tryCount--)
  {
    std::cout << errMsg << std::endl;
    rt = newConnect(ip.c_str(), port, &errMsg);
  }
}

/**
 * @brief 异步RPC方法调用接口（回调风格）
 */
void MprpcChannel::CallMethodAsync(const google::protobuf::MethodDescriptor *method,
                                   google::protobuf::RpcController *controller,
                                   const google::protobuf::Message *request,
                                   google::protobuf::Message *response,
                                   std::function<void(bool, google::protobuf::Message *)> callback)
{
  if (!m_isAsyncMode || !m_ioManager)
  {
    // 如果不在异步模式下，回退到同步调用
    DPrintf("[MprpcChannel::CallMethodAsync] 不在异步模式，回退到同步调用");
    CallMethod(method, controller, request, response, nullptr);
    callback(!controller->Failed(), response);
    return;
  }

  // 创建异步请求上下文
  auto context = std::make_shared<AsyncRpcContext>(generateRequestId());
  context->callback = callback;
  context->response.reset(response->New());

  // 发送异步请求
  if (!sendAsyncRequest(method, controller, request, context))
  {
    // 发送失败，直接调用回调
    controller->SetFailed("Failed to send async request");
    callback(false, nullptr);
    return;
  }

  // 将请求添加到待处理列表
  {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    m_pendingRequests[context->requestId] = context;
  }
}

/**
 * @brief 异步RPC方法调用接口（Future风格）
 */
std::future<std::unique_ptr<google::protobuf::Message>> MprpcChannel::CallMethodAsync(
    const google::protobuf::MethodDescriptor *method,
    google::protobuf::RpcController *controller,
    const google::protobuf::Message *request,
    const google::protobuf::Message *response)
{
  auto promise = std::make_shared<std::promise<std::unique_ptr<google::protobuf::Message>>>();
  auto future = promise->get_future();

  if (!m_isAsyncMode || !m_ioManager)
  {
    // 如果不在异步模式下，回退到同步调用
    DPrintf("[MprpcChannel::CallMethodAsync] 不在异步模式，回退到同步调用");
    auto responsePtr = std::unique_ptr<google::protobuf::Message>(response->New());
    CallMethod(method, controller, request, responsePtr.get(), nullptr);

    if (controller->Failed())
    {
      promise->set_value(nullptr);
    }
    else
    {
      promise->set_value(std::move(responsePtr));
    }
    return future;
  }

  // 创建异步请求上下文
  auto context = std::make_shared<AsyncRpcContext>(generateRequestId());
  context->promise = promise;
  context->response.reset(response->New());

  // 发送异步请求
  if (!sendAsyncRequest(method, controller, request, context))
  {
    // 发送失败，设置promise为nullptr
    promise->set_value(nullptr);
    return future;
  }

  // 将请求添加到待处理列表
  {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    m_pendingRequests[context->requestId] = context;
  }

  return future;
}

/**
 * @brief 异步发送RPC请求的核心实现
 */
bool MprpcChannel::sendAsyncRequest(const google::protobuf::MethodDescriptor *method,
                                    google::protobuf::RpcController *controller,
                                    const google::protobuf::Message *request,
                                    std::shared_ptr<AsyncRpcContext> context)
{
  // 检查连接状态，如果未连接则尝试建立连接
  if (m_clientFd == -1)
  {
    std::string errMsg;
    bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
    if (!rt)
    {
      DPrintf("[func-MprpcChannel::sendAsyncRequest]重连接ip：{%s} port{%d}失败", m_ip.c_str(), m_port);
      controller->SetFailed(errMsg);
      return false;
    }
    else
    {
      DPrintf("[func-MprpcChannel::sendAsyncRequest]连接ip：{%s} port{%d}成功", m_ip.c_str(), m_port);
    }
  }

  // 获取服务和方法信息
  const google::protobuf::ServiceDescriptor *sd = method->service();
  std::string service_name = sd->name();    // 服务名称
  std::string method_name = method->name(); // 方法名称

  // 序列化请求参数
  uint32_t args_size{}; // 参数大小
  std::string args_str; // 序列化后的参数字符串
  if (request->SerializeToString(&args_str))
  {
    args_size = args_str.size();
  }
  else
  {
    controller->SetFailed("serialize request error!");
    return false;
  }

  // 定义rpc的请求header
  mprpc::RpcHeader rpcHeader;
  rpcHeader.set_service_name(service_name);
  rpcHeader.set_method_name(method_name);
  rpcHeader.set_args_size(args_size);
  rpcHeader.set_request_id(context->requestId); // 设置请求ID

  uint32_t header_size{};
  std::string rpc_header_str;
  if (rpcHeader.SerializeToString(&rpc_header_str))
  {
    header_size = rpc_header_str.size();
  }
  else
  {
    controller->SetFailed("serialize rpc header error!");
    return false;
  }

  // 使用protobuf的CodedOutputStream来构建发送的数据流
  std::string send_rpc_str; // 用来存储最终发送的数据
  {
    // 创建一个StringOutputStream用于写入send_rpc_str
    google::protobuf::io::StringOutputStream string_output(&send_rpc_str);
    google::protobuf::io::CodedOutputStream coded_output(&string_output);

    // 先写入header的长度（变长编码）
    coded_output.WriteVarint32(static_cast<uint32_t>(rpc_header_str.size()));

    // 然后写入rpc_header本身
    coded_output.WriteString(rpc_header_str);
  }

  // 最后，将请求参数附加到send_rpc_str后面
  send_rpc_str += args_str;

  // 发送rpc请求（非阻塞）
  ssize_t sent = send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), MSG_DONTWAIT);
  if (sent == -1)
  {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
      // 发送缓冲区满，需要等待
      DPrintf("[MprpcChannel::sendAsyncRequest] 发送缓冲区满，需要等待");
      // TODO: 这里可以注册WRITE事件等待发送完成
      return false;
    }
    else
    {
      // 其他错误，尝试重连
      DPrintf("[MprpcChannel::sendAsyncRequest] 发送失败，尝试重连");
      close(m_clientFd);
      m_clientFd = -1;
      std::string errMsg;
      bool rt = newConnect(m_ip.c_str(), m_port, &errMsg);
      if (!rt)
      {
        controller->SetFailed(errMsg);
        return false;
      }
      // 重连成功后重新发送
      sent = send(m_clientFd, send_rpc_str.c_str(), send_rpc_str.size(), 0);
      if (sent == -1)
      {
        controller->SetFailed("send error after reconnect");
        return false;
      }
    }
  }

  // 注册READ事件，等待响应
  if (m_ioManager->addEvent(m_clientFd, monsoon::READ,
                            [this]()
                            { this->handleAsyncResponse(); }) != 0)
  {
    DPrintf("[MprpcChannel::sendAsyncRequest] 注册READ事件失败");
    return false;
  }

  DPrintf("[MprpcChannel::sendAsyncRequest] 异步请求发送成功，requestId: %lu", context->requestId);
  return true;
}

/**
 * @brief 处理异步响应的回调函数
 */
void MprpcChannel::handleAsyncResponse()
{
  // 接收响应数据
  char recv_buf[4096] = {0}; // 增大接收缓冲区
  int recv_size = 0;
  if (-1 == (recv_size = recv(m_clientFd, recv_buf, sizeof(recv_buf), MSG_DONTWAIT)))
  {
    if (errno == EAGAIN || errno == EWOULDBLOCK)
    {
      // 没有数据可读，重新注册READ事件
      m_ioManager->addEvent(m_clientFd, monsoon::READ,
                            [this]()
                            { this->handleAsyncResponse(); });
      return;
    }
    else
    {
      // 连接错误，处理所有待处理的请求
      DPrintf("[MprpcChannel::handleAsyncResponse] recv error! errno:%d", errno);
      close(m_clientFd);
      m_clientFd = -1;

      // 通知所有待处理的请求失败
      std::lock_guard<std::mutex> lock(m_pendingMutex);
      for (auto &pair : m_pendingRequests)
      {
        auto &context = pair.second;
        if (context->callback)
        {
          context->callback(false, nullptr);
        }
        if (context->promise)
        {
          context->promise->set_value(nullptr);
        }
      }
      m_pendingRequests.clear();
      return;
    }
  }

  if (recv_size == 0)
  {
    // 连接被对方关闭
    DPrintf("[MprpcChannel::handleAsyncResponse] 连接被对方关闭");
    close(m_clientFd);
    m_clientFd = -1;

    // 通知所有待处理的请求失败
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    for (auto &pair : m_pendingRequests)
    {
      auto &context = pair.second;
      if (context->callback)
      {
        context->callback(false, nullptr);
      }
      if (context->promise)
      {
        context->promise->set_value(nullptr);
      }
    }
    m_pendingRequests.clear();
    return;
  }

  // 解析响应数据
  // 简化版本：假设一次recv就能收到完整的响应
  // 实际应用中可能需要处理分包情况

  // 这里需要根据协议解析出requestId
  // 由于当前协议没有在响应中包含requestId，我们假设响应是按顺序返回的
  // 在实际实现中，应该修改协议在响应中包含requestId

  std::shared_ptr<AsyncRpcContext> context;
  {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    if (!m_pendingRequests.empty())
    {
      // 简化处理：取第一个待处理的请求
      auto it = m_pendingRequests.begin();
      context = it->second;
      m_pendingRequests.erase(it);
    }
  }

  if (!context)
  {
    DPrintf("[MprpcChannel::handleAsyncResponse] 没有找到对应的请求上下文");
    // 重新注册READ事件，继续等待其他响应
    m_ioManager->addEvent(m_clientFd, monsoon::READ,
                          [this]()
                          { this->handleAsyncResponse(); });
    return;
  }

  // 反序列化响应数据
  if (!context->response->ParseFromArray(recv_buf, recv_size))
  {
    DPrintf("[MprpcChannel::handleAsyncResponse] 解析响应失败");
    // 通知请求失败
    if (context->callback)
    {
      context->callback(false, nullptr);
    }
    if (context->promise)
    {
      context->promise->set_value(nullptr);
    }
  }
  else
  {
    // 解析成功，通知请求完成
    DPrintf("[MprpcChannel::handleAsyncResponse] 异步请求完成，requestId: %lu", context->requestId);
    if (context->callback)
    {
      context->callback(true, context->response.get());
    }
    if (context->promise)
    {
      context->promise->set_value(std::move(context->response));
    }
  }

  // 检查是否还有待处理的请求，如果有则继续注册READ事件
  {
    std::lock_guard<std::mutex> lock(m_pendingMutex);
    if (!m_pendingRequests.empty())
    {
      m_ioManager->addEvent(m_clientFd, monsoon::READ,
                            [this]()
                            { this->handleAsyncResponse(); });
    }
  }
}