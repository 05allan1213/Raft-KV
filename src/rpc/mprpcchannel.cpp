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
  RPC::RpcHeader rpcHeader;
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
MprpcChannel::MprpcChannel(string ip, short port, bool connectNow) : m_ip(ip), m_port(port), m_clientFd(-1)
{
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