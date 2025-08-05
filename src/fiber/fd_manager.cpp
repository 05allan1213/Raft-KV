#include "fd_manager.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "hook.h"

namespace monsoon
{

  /**
   * @brief 文件描述符上下文构造函数
   * @param fd 文件描述符
   */
  FdCtx::FdCtx(int fd)
      : m_isInit(false),       // 初始化标志
        m_isSocket(false),     // 是否为socket
        m_sysNonblock(false),  // 系统非阻塞标志
        m_userNonblock(false), // 用户非阻塞标志
        m_isClosed(false),     // 是否已关闭
        m_fd(fd),              // 文件描述符
        m_recvTimeout(-1),     // 接收超时时间
        m_sendTimeout(-1)      // 发送超时时间
  {
    init(); // 初始化文件描述符上下文
  }

  /**
   * @brief 文件描述符上下文析构函数
   */
  FdCtx::~FdCtx() {}

  /**
   * @brief 初始化文件描述符上下文
   * @return true表示初始化成功，false表示初始化失败
   */
  bool FdCtx::init()
  {
    if (m_isInit)
    {
      return true; // 已经初始化过，直接返回
    }
    m_recvTimeout = -1; // 设置接收超时为-1（无超时）
    m_sendTimeout = -1; // 设置发送超时为-1（无超时）

    // 获取文件状态信息
    struct stat fd_stat;
    if (-1 == fstat(m_fd, &fd_stat)) // 获取文件状态失败
    {
      m_isInit = false;   // 初始化失败
      m_isSocket = false; // 不是socket
    }
    else
    {
      m_isInit = true; // 初始化成功
      // 判断是否是socket
      m_isSocket = S_ISSOCK(fd_stat.st_mode); // 检查文件类型是否为socket
    }

    // 对socket设置非阻塞
    if (m_isSocket)
    {
      int flags = fcntl_f(m_fd, F_GETFL, 0); // 获取文件描述符标志
      if (!(flags & O_NONBLOCK))             // 如果不是非阻塞
      {
        fcntl_f(m_fd, F_SETFL, flags | O_NONBLOCK); // 设置为非阻塞
      }
      m_sysNonblock = true; // 设置系统非阻塞标志
    }
    else
    {
      m_sysNonblock = false; // 不是socket，系统非阻塞标志为false
    }

    m_userNonblock = false; // 用户非阻塞标志初始化为false
    m_isClosed = false;     // 关闭标志初始化为false
    return m_isInit;        // 返回初始化结果
  }

  /**
   * @brief 设置超时时间
   * @param type 超时类型（SO_RCVTIMEO为接收超时，SO_SNDTIMEO为发送超时）
   * @param v 超时时间（毫秒）
   */
  void FdCtx::setTimeout(int type, uint64_t v)
  {
    if (type == SO_RCVTIMEO)
    {
      m_recvTimeout = v; // 设置接收超时
    }
    else
    {
      m_sendTimeout = v; // 设置发送超时
    }
  }

  /**
   * @brief 获取超时时间
   * @param type 超时类型
   * @return 超时时间（毫秒）
   */
  uint64_t FdCtx::getTimeout(int type)
  {
    if (type == SO_RCVTIMEO)
    {
      return m_recvTimeout; // 返回接收超时
    }
    else
    {
      return m_sendTimeout; // 返回发送超时
    }
  }

  /**
   * @brief 文件描述符管理器构造函数
   */
  FdManager::FdManager() { m_datas.resize(64); } // 初始化文件描述符数组大小为64

  /**
   * @brief 获取文件描述符上下文
   * @param fd 文件描述符
   * @param auto_create 是否自动创建
   * @return 文件描述符上下文指针
   */
  FdCtx::ptr FdManager::get(int fd, bool auto_create)
  {
    if (fd == -1)
    {
      return nullptr; // 无效文件描述符
    }
    RWMutexType::ReadLock lock(m_mutex); // 加读锁
    if ((int)m_datas.size() <= fd)
    {
      if (auto_create == false)
      {
        return nullptr;
      }
    }
    else
    {
      if (m_datas[fd] || !auto_create)
      {
        return m_datas[fd];
      }
    }
    lock.unlock();

    RWMutexType::WriteLock lock2(m_mutex);
    FdCtx::ptr ctx(new FdCtx(fd));
    if (fd >= (int)m_datas.size())
    {
      m_datas.resize(fd * 1.5);
    }
    m_datas[fd] = ctx;
    return ctx;
  }

  void FdManager::del(int fd)
  {
    RWMutexType::WriteLock lock(m_mutex);
    if ((int)m_datas.size() <= fd)
    {
      return;
    }
    m_datas[fd].reset();
  }
}