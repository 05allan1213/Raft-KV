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
   */
  u_int32_t GetFiberId()
  {
    return 0; // 暂时返回0
  }
}