#pragma once
#include <google/protobuf/descriptor.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>
#include <muduo/net/TcpConnection.h>
#include <muduo/net/TcpServer.h>
#include <functional>
#include <string>
#include <unordered_map>
#include "google/protobuf/service.h"

/**
 * @brief RPC服务提供者类
 *
 * 框架提供的专门发布rpc服务的网络对象类，负责管理RPC服务的注册、启动和请求处理。
 * 使用muduo网络库作为底层网络框架，支持多服务注册和动态服务发现。
 *
 * @todo 现在rpc客户端变成了长连接，因此rpc服务器这边最好提供一个定时器，用以断开很久没有请求的连接。
 * @todo 为了配合这个，那么rpc客户端那边每次发送之前也需要真正的
 */
class RpcProvider
{
public:
  /**
   * @brief 注册RPC服务
   * @param service 要注册的服务对象指针
   *
   * 这里是框架提供给外部使用的，可以发布rpc方法的函数接口
   */
  void NotifyService(google::protobuf::Service *service);

  /**
   * @brief 启动RPC服务节点
   * @param nodeIndex 节点索引
   * @param port 服务端口
   *
   * 启动rpc服务节点，开始提供rpc远程网络调用服务
   */
  void Run(int nodeIndex, short port);

private:
  muduo::net::EventLoop m_eventLoop;                     // 组合EventLoop，用于事件循环
  std::shared_ptr<muduo::net::TcpServer> m_muduo_server; // muduo TCP服务器

  /**
   * @brief 服务信息结构体
   *
   * 存储单个服务的相关信息，包括服务对象和服务方法映射
   */
  struct ServiceInfo
  {
    google::protobuf::Service *m_service;                                                    // 保存服务对象
    std::unordered_map<std::string, const google::protobuf::MethodDescriptor *> m_methodMap; // 保存服务方法
  };
  // 存储注册成功的服务对象和其服务方法的所有信息
  std::unordered_map<std::string, ServiceInfo> m_serviceMap;

  /**
   * @brief 新的socket连接回调
   * @param conn TCP连接指针
   */
  void OnConnection(const muduo::net::TcpConnectionPtr &);

  /**
   * @brief 已建立连接用户的读写事件回调
   * @param conn TCP连接指针
   * @param buffer 数据缓冲区
   * @param receiveTime 接收时间戳
   */
  void OnMessage(const muduo::net::TcpConnectionPtr &, muduo::net::Buffer *, muduo::Timestamp);

  /**
   * @brief Closure的回调操作，用于序列化rpc的响应和网络发送
   * @param conn TCP连接指针
   * @param response RPC响应消息
   */
  void SendRpcResponse(const muduo::net::TcpConnectionPtr &, google::protobuf::Message *);

public:
  /**
   * @brief 析构函数
   */
  ~RpcProvider();
};