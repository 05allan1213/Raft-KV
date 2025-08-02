#pragma once
#include <google/protobuf/service.h>
#include <string>

/**
 * @brief RPC控制器类，继承自google::protobuf::RpcController
 *
 * 实现了protobuf RPC框架的控制器接口，用于管理RPC调用的状态和错误信息。
 * 提供RPC调用的生命周期管理和错误处理功能。
 */
class MprpcController : public google::protobuf::RpcController
{
public:
  /**
   * @brief 构造函数
   */
  MprpcController();

  /**
   * @brief 重置控制器状态
   *
   * 清除所有错误信息和状态，将控制器恢复到初始状态
   */
  void Reset();

  /**
   * @brief 检查RPC调用是否失败
   * @return 如果RPC调用失败返回true，否则返回false
   */
  bool Failed() const;

  /**
   * @brief 获取错误信息文本
   * @return 错误信息字符串
   */
  std::string ErrorText() const;

  /**
   * @brief 设置RPC调用失败状态
   * @param reason 失败原因
   */
  void SetFailed(const std::string &reason);

  /**
   * @brief 开始取消操作
   *
   * 目前未实现具体的功能
   */
  void StartCancel();

  /**
   * @brief 检查是否已取消
   * @return 如果已取消返回true，否则返回false
   *
   * 目前未实现具体的功能
   */
  bool IsCanceled() const;

  /**
   * @brief 设置取消回调
   * @param callback 取消时的回调函数
   *
   * 目前未实现具体的功能
   */
  void NotifyOnCancel(google::protobuf::Closure *callback);

private:
  bool m_failed;         // RPC方法执行过程中的状态
  std::string m_errText; // RPC方法执行过程中的错误信息
};