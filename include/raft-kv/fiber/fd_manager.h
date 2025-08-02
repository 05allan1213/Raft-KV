#pragma once

#include <memory>
#include <vector>
#include "mutex.h"
#include "singleton.h"
#include "thread.h"

namespace monsoon
{
  /**
   * @brief 文件句柄上下文类
   * @details 管理文件句柄类型，阻塞，关闭，读写超时等属性
   */
  class FdCtx : public std::enable_shared_from_this<FdCtx>
  {
  public:
    typedef std::shared_ptr<FdCtx> ptr;

    /**
     * @brief 构造函数
     * @param fd 文件描述符
     */
    FdCtx(int fd);

    /**
     * @brief 析构函数
     */
    ~FdCtx();

    /**
     * @brief 检查是否完成初始化
     * @return true表示已初始化，false表示未初始化
     */
    bool isInit() const { return m_isInit; }

    /**
     * @brief 检查是否是socket
     * @return true表示是socket，false表示不是socket
     */
    bool isSocket() const { return m_isSocket; }

    /**
     * @brief 检查是否已经关闭
     * @return true表示已关闭，false表示未关闭
     */
    bool isClose() const { return m_isClosed; }

    /**
     * @brief 设置用户主动设置的非阻塞标志
     * @param v 非阻塞标志
     */
    void setUserNonblock(bool v) { m_userNonblock = v; }

    /**
     * @brief 获取用户是否主动设置了非阻塞
     * @return true表示用户设置了非阻塞，false表示用户未设置
     */
    bool getUserNonblock() const { return m_userNonblock; }

    /**
     * @brief 设置系统非阻塞标志
     * @param v 非阻塞标志
     */
    void setSysNonblock(bool v) { m_sysNonblock = v; }

    /**
     * @brief 获取系统是否非阻塞
     * @return true表示系统设置了非阻塞，false表示系统未设置
     */
    bool getSysNonblock() const { return m_sysNonblock; }

    /**
     * @brief 设置超时时间
     * @param type 超时类型（读/写）
     * @param v 超时时间（毫秒）
     */
    void setTimeout(int type, uint64_t v);

    /**
     * @brief 获取超时时间
     * @param type 超时类型（读/写）
     * @return 超时时间（毫秒）
     */
    uint64_t getTimeout(int type);

  private:
    /**
     * @brief 初始化文件句柄上下文
     * @return true表示初始化成功，false表示初始化失败
     */
    bool init();

  private:
    bool m_isInit : 1;       // 是否初始化
    bool m_isSocket : 1;     // 是否socket
    bool m_sysNonblock : 1;  // 是否hook非阻塞
    bool m_userNonblock : 1; // 是否用户主动设置非阻塞
    bool m_isClosed : 1;     // 是否关闭
    int m_fd;                // 文件句柄
    uint64_t m_recvTimeout;  // 读超时时间毫秒
    uint64_t m_sendTimeout;  // 写超时时间毫秒
  };

  /**
   * @brief 文件句柄管理类
   * @details 管理所有文件句柄的上下文，提供获取、创建、删除等功能
   */
  class FdManager
  {
  public:
    typedef RWMutex RWMutexType;

    /**
     * @brief 构造函数
     */
    FdManager();

    /**
     * @brief 获取/创建文件句柄上下文
     * @param fd 文件描述符
     * @param auto_create 是否自动创建
     * @return 文件句柄上下文指针
     */
    FdCtx::ptr get(int fd, bool auto_create = false);

    /**
     * @brief 删除文件句柄上下文
     * @param fd 文件描述符
     */
    void del(int fd);

  private:
    RWMutexType m_mutex;             // 读写锁
    std::vector<FdCtx::ptr> m_datas; // 文件句柄集合
  };

  /**
   * @brief 文件句柄单例类型定义
   * @details 使用单例模式管理全局的文件句柄管理器
   */
  typedef Singleton<FdManager> FdMgr;

} // namespace monsoon