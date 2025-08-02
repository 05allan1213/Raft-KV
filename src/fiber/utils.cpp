#include "utils.h"

namespace monsoon
{
  /**
   * @brief 获取当前线程ID
   * @return 当前线程的ID
   */
  pid_t GetThreadId() { return syscall(SYS_gettid); } // 使用系统调用获取线程ID

  /**
   * @brief 获取当前协程ID
   * @return 当前协程的ID
   * @details TODO: 需要实现协程ID获取逻辑
   */
  u_int32_t GetFiberId()
  {
    // TODO: 实现协程ID获取逻辑
    return 0; // 暂时返回0
  }
} // namespace monsoon