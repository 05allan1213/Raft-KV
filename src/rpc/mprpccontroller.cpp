#include "mprpccontroller.h"

/**
 * @brief 构造函数
 *
 * 初始化RPC控制器，设置初始状态为未失败，错误信息为空
 */
MprpcController::MprpcController()
{
  m_failed = false; // 初始状态：未失败
  m_errText = "";   // 初始错误信息：空字符串
}

/**
 * @brief 重置控制器状态
 *
 * 清除所有错误信息和状态，将控制器恢复到初始状态
 */
void MprpcController::Reset()
{
  m_failed = false; // 重置失败状态
  m_errText = "";   // 清空错误信息
}

/**
 * @brief 检查RPC调用是否失败
 * @return 如果RPC调用失败返回true，否则返回false
 */
bool MprpcController::Failed() const
{
  return m_failed;
}

/**
 * @brief 获取错误信息文本
 * @return 错误信息字符串
 */
std::string MprpcController::ErrorText() const
{
  return m_errText;
}

/**
 * @brief 设置RPC调用失败状态
 * @param reason 失败原因
 *
 * 将控制器状态设置为失败，并记录失败原因
 */
void MprpcController::SetFailed(const std::string &reason)
{
  m_failed = true;    // 设置失败状态
  m_errText = reason; // 记录失败原因
}

/**
 * @brief 开始取消操作
 *
 * 目前未实现具体的功能，预留接口用于后续扩展
 */
void MprpcController::StartCancel() {}

/**
 * @brief 检查是否已取消
 * @return 如果已取消返回true，否则返回false
 *
 * 目前未实现具体的功能，总是返回false
 */
bool MprpcController::IsCanceled() const
{
  return false;
}

/**
 * @brief 设置取消回调
 * @param callback 取消时的回调函数
 *
 * 目前未实现具体的功能，预留接口用于后续扩展
 */
void MprpcController::NotifyOnCancel(google::protobuf::Closure *callback) {}