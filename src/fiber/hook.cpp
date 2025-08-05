#include "hook.h"
#include <dlfcn.h>
#include <cstdarg>
#include <string>
#include "fd_manager.h"
#include "fiber.h"
#include "iomanager.h"
namespace monsoon
{
  // 当前线程是否启用hook
  static thread_local bool t_hook_enable = false;
  static int g_tcp_connect_timeout = 5000; // TCP连接超时时间（毫秒）

  /**
   * @brief 定义需要hook的系统调用函数宏
   * @details 包含sleep、socket、IO等系统调用
   */
#define HOOK_FUN(XX) \
  XX(sleep)          \
  XX(usleep)         \
  XX(nanosleep)      \
  XX(socket)         \
  XX(connect)        \
  XX(accept)         \
  XX(read)           \
  XX(readv)          \
  XX(recv)           \
  XX(recvfrom)       \
  XX(recvmsg)        \
  XX(write)          \
  XX(writev)         \
  XX(send)           \
  XX(sendto)         \
  XX(sendmsg)        \
  XX(close)          \
  XX(fcntl)          \
  XX(ioctl)          \
  XX(getsockopt)     \
  XX(setsockopt)

  /**
   * @brief 初始化hook系统
   * @details 获取所有需要hook的系统调用的原始函数地址
   */
  void hook_init()
  {
    static bool is_inited = false;
    if (is_inited)
    {
      return; // 已经初始化过，直接返回
    }
    // dlsym:Dynamic Linking Library.返回指定符号的地址
#define XX(name) name##_f = (name##_fun)dlsym(RTLD_NEXT, #name); // 获取原始函数地址
    HOOK_FUN(XX);
#undef XX
  }

  /**
   * @brief hook初始化器
   * @details hook_init放在静态对象中，则在main函数执行之前就会获取各个符号地址并保存到全局变量中
   */
  static uint64_t s_connect_timeout = -1;
  struct _HOOKIniter
  {
    _HOOKIniter()
    {
      hook_init();                               // 初始化hook系统
      s_connect_timeout = g_tcp_connect_timeout; // 设置连接超时时间
    }
  };
  static _HOOKIniter s_hook_initer; // 全局静态对象，程序启动时自动初始化

  /**
   * @brief 检查当前线程是否启用hook
   * @return true表示启用hook，false表示不启用
   */
  bool is_hook_enable() { return t_hook_enable; }

  /**
   * @brief 设置当前线程的hook状态
   * @param flag true表示启用hook，false表示不启用
   */
  void set_hook_enable(const bool flag) { t_hook_enable = flag; }

  /**
   * @brief 定时器信息结构体
   * @details 用于跟踪定时器的取消状态
   */
  struct timer_info
  {
    int cnacelled = 0; // 取消标志，0表示未取消，非0表示已取消
  };

  /**
   * @brief 通用IO操作模板函数
   * @param fd 文件描述符
   * @param fun 原始IO函数
   * @param hook_fun_name hook函数名称（用于调试）
   * @param event 事件类型（READ/WRITE）
   * @param timeout_so 超时选项类型
   * @param args 可变参数
   * @return 操作结果
   * @details 将阻塞的IO操作转换为非阻塞的协程调度
   */
  template <typename OriginFun, typename... Args>
  static ssize_t do_io(int fd, OriginFun fun, const char *hook_fun_name, uint32_t event, int timeout_so, Args &&...args)
  {
    if (!t_hook_enable)
    {
      return fun(fd, std::forward<Args>(args)...); // 未启用hook，直接调用原始函数
    }
    // 为当前文件描述符创建上下文ctx
    FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd);
    if (!ctx)
    {
      return fun(fd, std::forward<Args>(args)...); // 获取上下文失败，直接调用原始函数
    }
    // 文件已经关闭
    if (ctx->isClose())
    {
      errno = EBADF; // 设置错误码为文件描述符无效
      return -1;
    }

    if (!ctx->isSocket() || ctx->getUserNonblock())
    {
      return fun(fd, std::forward<Args>(args)...); // 不是socket或用户设置了非阻塞，直接调用原始函数
    }
    // 获取对应type的fd超时时间
    uint64_t to = ctx->getTimeout(timeout_so);
    std::shared_ptr<timer_info> tinfo(new timer_info); // 创建定时器信息

  retry:
    ssize_t n = fun(fd, std::forward<Args>(args)...); // 尝试执行原始IO操作
    while (n == -1 && errno == EINTR)
    {
      // 读取操作被信号中断，继续尝试
      n = fun(fd, std::forward<Args>(args)...);
    }
    if (n == -1 && errno == EAGAIN)
    {
      // 数据未就绪，需要等待
      IOManager *iom = IOManager::GetThis();  // 获取IO管理器
      Timer::ptr timer;                       // 定时器指针
      std::weak_ptr<timer_info> winfo(tinfo); // 弱引用，避免循环引用

      if (to != (uint64_t)-1)
      {
        // 设置超时定时器
        timer = iom->addConditionTimer(
            to,
            [winfo, fd, iom, event]()
            {
              auto t = winfo.lock(); // 尝试获取强引用
              if (!t || t->cnacelled)
              {
                return; // 定时器已被取消
              }
              t->cnacelled = ETIMEDOUT;             // 设置超时标志
              iom->cancelEvent(fd, (Event)(event)); // 取消IO事件
            },
            winfo);
      }

      int rt = iom->addEvent(fd, (Event)(event)); // 添加IO事件到epoll
      if (rt)
      {
        // 添加事件失败
        std::cout << hook_fun_name << " addEvent(" << fd << ", " << event << ")";
        if (timer)
        {
          timer->cancel(); // 取消定时器
        }
        return -1;
      }
      else
      {
        Fiber::GetThis()->yield(); // 让出协程执行权，等待IO事件
        if (timer)
        {
          timer->cancel(); // 取消定时器
        }
        if (tinfo->cnacelled)
        {
          errno = tinfo->cnacelled; // 设置错误码
          return -1;
        }
        goto retry; // 重新尝试IO操作
      }
    }

    return n; // 返回操作结果
  }

  /**
   * @brief C语言接口，用于hook系统调用
   */
  extern "C"
  {
#define XX(name) name##_fun name##_f = nullptr; // 声明原始函数指针
    HOOK_FUN(XX);
#undef XX

    /**
     * @brief hook sleep函数
     * @param seconds 睡眠的秒数
     * @return 0表示成功
     * @details 将阻塞的sleep转换为非阻塞的协程调度
     */
    unsigned int sleep(unsigned int seconds)
    {
      if (!t_hook_enable)
      {
        // 不允许hook,则直接使用系统调用
        return sleep_f(seconds);
      }
      // 允许hook,则直接让当前协程退出，seconds秒后再重启（by定时器）
      Fiber::ptr fiber = Fiber::GetThis();   // 获取当前协程
      IOManager *iom = IOManager::GetThis(); // 获取IO管理器
      // 添加定时器，seconds秒后重新调度当前协程
      iom->addTimer(seconds * 1000,
                    std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
      Fiber::GetThis()->yield(); // 让出协程执行权
      return 0;
    }
    /**
     * @brief hook usleep函数
     * @param usec 睡眠的微秒数
     * @return 0表示成功
     * @details 将阻塞的usleep转换为非阻塞的协程调度
     */
    int usleep(useconds_t usec)
    {
      if (!t_hook_enable)
      {
        // 不允许hook,则直接使用系统调用
        auto ret = usleep_f(usec);
        return 0;
      }
      // 允许hook,则直接让当前协程退出，usec微秒后再重启（by定时器）
      Fiber::ptr fiber = Fiber::GetThis();   // 获取当前协程
      IOManager *iom = IOManager::GetThis(); // 获取IO管理器
      // 添加定时器，usec微秒后重新调度当前协程
      iom->addTimer(usec / 1000,
                    std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
      Fiber::GetThis()->yield(); // 让出协程执行权
      return 0;
    }

    /**
     * @brief hook nanosleep函数
     * @param req 请求的睡眠时间
     * @param rem 剩余的睡眠时间
     * @return 0表示成功
     * @details 将阻塞的nanosleep转换为非阻塞的协程调度
     */
    int nanosleep(const struct timespec *req, struct timespec *rem)
    {
      if (!t_hook_enable)
      {
        // 不允许hook,则直接使用系统调用
        return nanosleep_f(req, rem);
      }
      // 允许hook,则直接让当前协程退出，指定时间后再重启（by定时器）
      Fiber::ptr fiber = Fiber::GetThis();   // 获取当前协程
      IOManager *iom = IOManager::GetThis(); // 获取IO管理器
      // 计算总毫秒数
      int timeout_s = req->tv_sec * 1000 + req->tv_nsec / 1000 / 1000;
      // 添加定时器，指定时间后重新调度当前协程
      iom->addTimer(timeout_s,
                    std::bind((void(Scheduler::*)(Fiber::ptr, int thread)) & IOManager::scheduler, iom, fiber, -1));
      Fiber::GetThis()->yield(); // 让出协程执行权
      return 0;
    }

    /**
     * @brief hook socket函数
     * @param domain 协议域
     * @param type 套接字类型
     * @param protocol 协议类型
     * @return 文件描述符
     * @details 创建socket并加入文件描述符管理器
     */
    int socket(int domain, int type, int protocol)
    {
      if (!t_hook_enable)
      {
        return socket_f(domain, type, protocol); // 未启用hook，直接调用原始函数
      }
      int fd = socket_f(domain, type, protocol); // 创建socket
      if (fd == -1)
      {
        return fd; // 创建失败
      }
      // 将fd加入Fdmanager中，自动管理文件描述符上下文
      FdMgr::GetInstance()->get(fd, true);
      return fd;
    }

    /**
     * @brief 带超时的connect函数
     * @param fd 文件描述符
     * @param addr 目标地址
     * @param addrlen 地址长度
     * @param timeout_ms 超时时间（毫秒）
     * @return 0表示成功，-1表示失败
     * @details 将阻塞的connect转换为非阻塞的协程调度，支持超时控制
     */
    int connect_with_timeout(int fd, const struct sockaddr *addr, socklen_t addrlen, uint64_t timeout_ms)
    {
      if (!t_hook_enable)
      {
        return connect_f(fd, addr, addrlen); // 未启用hook，直接调用原始函数
      }
      FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd); // 获取文件描述符上下文
      if (!ctx || ctx->isClose())
      {
        errno = EBADF; // 文件描述符无效
        return -1;
      }

      if (!ctx->isSocket())
      {
        return connect_f(fd, addr, addrlen); // 不是socket，直接调用原始函数
      }

      // fd是否被显式设置为非阻塞模式
      if (ctx->getUserNonblock())
      {
        return connect_f(fd, addr, addrlen); // 用户设置了非阻塞，直接调用原始函数
      }

      // 系统调用connect(fd为非阻塞)
      int n = connect_f(fd, addr, addrlen);
      if (n == 0)
      {
        return 0; // 连接立即成功
      }
      else if (n != -1 || errno != EINPROGRESS)
      {
        return n; // 连接立即失败
      }
      // 返回EINPROGRESS:正在进行，但是尚未完成
      IOManager *iom = IOManager::GetThis();             // 获取IO管理器
      Timer::ptr timer;                                  // 定时器指针
      std::shared_ptr<timer_info> tinfo(new timer_info); // 定时器信息
      std::weak_ptr<timer_info> winfo(tinfo);            // 弱引用

      // 保证超时参数有效
      if (timeout_ms != (uint64_t)-1)
      {
        // 添加条件定时器
        timer = iom->addConditionTimer(
            timeout_ms,
            [winfo, fd, iom]()
            {
              auto t = winfo.lock(); // 尝试获取强引用
              if (!t || t->cnacelled)
              {
                return; // 定时器已被取消
              }
              // 定时时间到达，设置超时标志，触发一次WRITE事件
              t->cnacelled = ETIMEDOUT;
              iom->cancelEvent(fd, WRITE); // 取消WRITE事件
            },
            winfo);
      }

      // 添加WRITE事件，并yield,等待WRITE事件触发再往下执行
      int rt = iom->addEvent(fd, WRITE);
      if (rt == 0)
      {
        Fiber::GetThis()->yield(); // 让出协程执行权，等待连接完成
        // 等待超时or套接字可写，协程返回
        if (timer)
        {
          timer->cancel(); // 取消定时器
        }
        // 超时返回，通过超时标志设置errno并返回-1
        if (tinfo->cnacelled)
        {
          errno = tinfo->cnacelled;
          return -1;
        }
      }
      else
      {
        // addEvent error
        if (timer)
        {
          timer->cancel(); // 取消定时器
        }
        std::cout << "connect addEvent(" << fd << ", WRITE) error" << std::endl;
      }

      int error = 0;
      socklen_t len = sizeof(int);
      // 获取套接字的错误状态
      if (-1 == getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len))
      {
        return -1; // 获取错误状态失败
      }
      if (!error)
      {
        return 0; // 连接成功
      }
      else
      {
        errno = error; // 设置错误码
        return -1;
      }
    }

    /**
     * @brief hook connect函数
     * @param sockfd 套接字文件描述符
     * @param addr 目标地址
     * @param addrlen 地址长度
     * @return 0表示成功，-1表示失败
     * @details 使用默认超时时间的connect函数
     */
    int connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen)
    {
      return monsoon::connect_with_timeout(sockfd, addr, addrlen, s_connect_timeout);
    }

    /**
     * @brief hook accept函数
     * @param s 监听套接字
     * @param addr 客户端地址
     * @param addrlen 地址长度
     * @return 新的文件描述符
     * @details 接受连接并管理新的文件描述符
     */
    int accept(int s, struct sockaddr *addr, socklen_t *addrlen)
    {
      int fd = do_io(s, accept_f, "accept", READ, SO_RCVTIMEO, addr, addrlen); // 使用通用IO函数
      if (fd >= 0)
      {
        FdMgr::GetInstance()->get(fd, true); // 将新文件描述符加入管理器
      }
      return fd;
    }

    /**
     * @brief hook read函数
     * @param fd 文件描述符
     * @param buf 缓冲区
     * @param count 读取字节数
     * @return 实际读取的字节数
     * @details 将阻塞的read转换为非阻塞的协程调度
     */
    ssize_t read(int fd, void *buf, size_t count) { return do_io(fd, read_f, "read", READ, SO_RCVTIMEO, buf, count); }

    /**
     * @brief hook readv函数
     * @param fd 文件描述符
     * @param iov IO向量数组
     * @param iovcnt 向量数量
     * @return 实际读取的字节数
     * @details 将阻塞的readv转换为非阻塞的协程调度
     */
    ssize_t readv(int fd, const struct iovec *iov, int iovcnt)
    {
      return do_io(fd, readv_f, "readv", READ, SO_RCVTIMEO, iov, iovcnt);
    }

    /**
     * @brief hook recv函数
     * @param sockfd 套接字文件描述符
     * @param buf 缓冲区
     * @param len 接收长度
     * @param flags 标志位
     * @return 实际接收的字节数
     * @details 将阻塞的recv转换为非阻塞的协程调度
     */
    ssize_t recv(int sockfd, void *buf, size_t len, int flags)
    {
      return do_io(sockfd, recv_f, "recv", READ, SO_RCVTIMEO, buf, len, flags);
    }

    /**
     * @brief hook recvfrom函数
     * @param sockfd 套接字文件描述符
     * @param buf 缓冲区
     * @param len 接收长度
     * @param flags 标志位
     * @param src_addr 源地址
     * @param addrlen 地址长度
     * @return 实际接收的字节数
     * @details 将阻塞的recvfrom转换为非阻塞的协程调度
     */
    ssize_t recvfrom(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr, socklen_t *addrlen)
    {
      return do_io(sockfd, recvfrom_f, "recvfrom", READ, SO_RCVTIMEO, buf, len, flags, src_addr, addrlen);
    }

    /**
     * @brief hook recvmsg函数
     * @param sockfd 套接字文件描述符
     * @param msg 消息结构
     * @param flags 标志位
     * @return 实际接收的字节数
     * @details 将阻塞的recvmsg转换为非阻塞的协程调度
     */
    ssize_t recvmsg(int sockfd, struct msghdr *msg, int flags)
    {
      return do_io(sockfd, recvmsg_f, "recvmsg", READ, SO_RCVTIMEO, msg, flags);
    }

    /**
     * @brief hook write函数
     * @param fd 文件描述符
     * @param buf 缓冲区
     * @param count 写入字节数
     * @return 实际写入的字节数
     * @details 将阻塞的write转换为非阻塞的协程调度
     */
    ssize_t write(int fd, const void *buf, size_t count)
    {
      return do_io(fd, write_f, "write", WRITE, SO_SNDTIMEO, buf, count);
    }

    /**
     * @brief hook writev函数
     * @param fd 文件描述符
     * @param iov IO向量数组
     * @param iovcnt 向量数量
     * @return 实际写入的字节数
     * @details 将阻塞的writev转换为非阻塞的协程调度
     */
    ssize_t writev(int fd, const struct iovec *iov, int iovcnt)
    {
      return do_io(fd, writev_f, "writev", WRITE, SO_SNDTIMEO, iov, iovcnt);
    }

    /**
     * @brief hook send函数
     * @param s 套接字文件描述符
     * @param msg 消息
     * @param len 发送长度
     * @param flags 标志位
     * @return 实际发送的字节数
     * @details 将阻塞的send转换为非阻塞的协程调度
     */
    ssize_t send(int s, const void *msg, size_t len, int flags)
    {
      return do_io(s, send_f, "send", WRITE, SO_SNDTIMEO, msg, len, flags);
    }

    /**
     * @brief hook sendto函数
     * @param s 套接字文件描述符
     * @param msg 消息
     * @param len 发送长度
     * @param flags 标志位
     * @param to 目标地址
     * @param tolen 地址长度
     * @return 实际发送的字节数
     * @details 将阻塞的sendto转换为非阻塞的协程调度
     */
    ssize_t sendto(int s, const void *msg, size_t len, int flags, const struct sockaddr *to, socklen_t tolen)
    {
      return do_io(s, sendto_f, "sendto", WRITE, SO_SNDTIMEO, msg, len, flags, to, tolen);
    }

    /**
     * @brief hook sendmsg函数
     * @param s 套接字文件描述符
     * @param msg 消息结构
     * @param flags 标志位
     * @return 实际发送的字节数
     * @details 将阻塞的sendmsg转换为非阻塞的协程调度
     */
    ssize_t sendmsg(int s, const struct msghdr *msg, int flags)
    {
      return do_io(s, sendmsg_f, "sendmsg", WRITE, SO_SNDTIMEO, msg, flags);
    }

    /**
     * @brief hook close函数
     * @param fd 文件描述符
     * @return 0表示成功，-1表示失败
     * @details 关闭文件描述符并清理相关资源
     */
    int close(int fd)
    {
      if (!t_hook_enable)
      {
        return close_f(fd); // 未启用hook，直接调用原始函数
      }

      FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd); // 获取文件描述符上下文
      if (ctx)
      {
        auto iom = IOManager::GetThis(); // 获取IO管理器
        if (iom)
        {
          iom->cancelAll(fd); // 取消该文件描述符的所有IO事件
        }
        FdMgr::GetInstance()->del(fd); // 从管理器中删除文件描述符
      }
      return close_f(fd); // 调用原始close函数
    }
    /**
     * @brief hook fcntl函数
     * @param fd 文件描述符
     * @param cmd 命令
     * @param ... 可变参数
     * @return 操作结果
     * @details 处理文件描述符控制命令，特别是非阻塞标志的设置和获取
     */
    int fcntl(int fd, int cmd, ... /* arg */)
    {
      va_list va;
      va_start(va, cmd); // 初始化可变参数列表
      switch (cmd)
      {
      case F_SETFL:
      {
        // 设置文件描述符标志
        int arg = va_arg(va, int); // 获取标志参数
        va_end(va);
        FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd); // 获取文件描述符上下文
        if (!ctx || ctx->isClose() || !ctx->isSocket())
        {
          return fcntl_f(fd, cmd, arg); // 不是socket或已关闭，直接调用原始函数
        }
        ctx->setUserNonblock(arg & O_NONBLOCK); // 设置用户非阻塞标志
        if (ctx->getSysNonblock())
        {
          arg |= O_NONBLOCK; // 系统设置了非阻塞，保持非阻塞标志
        }
        else
        {
          arg &= ~O_NONBLOCK; // 系统未设置非阻塞，清除非阻塞标志
        }
        return fcntl_f(fd, cmd, arg); // 调用原始函数
      }
      break;
      case F_GETFL:
      {
        // 获取文件描述符标志
        va_end(va);
        int arg = fcntl_f(fd, cmd);                     // 获取原始标志
        FdCtx::ptr ctx = FdMgr::GetInstance()->get(fd); // 获取文件描述符上下文
        if (!ctx || ctx->isClose() || !ctx->isSocket())
        {
          return arg; // 不是socket或已关闭，直接返回
        }
        if (ctx->getUserNonblock())
        {
          return arg | O_NONBLOCK; // 用户设置了非阻塞，返回非阻塞标志
        }
        else
        {
          return arg & ~O_NONBLOCK; // 用户未设置非阻塞，清除非阻塞标志
        }
      }
      break;
      case F_DUPFD:
      case F_DUPFD_CLOEXEC:
      case F_SETFD:
      case F_SETOWN:
      case F_SETSIG:
      case F_SETLEASE:
      case F_NOTIFY:
#ifdef F_SETPIPE_SZ
      case F_SETPIPE_SZ:
#endif
      {
        // 其他设置命令，直接传递参数
        int arg = va_arg(va, int);
        va_end(va);
        return fcntl_f(fd, cmd, arg);
      }
      break;
      case F_GETFD:
      case F_GETOWN:
      case F_GETSIG:
      case F_GETLEASE:
#ifdef F_GETPIPE_SZ
      case F_GETPIPE_SZ:
#endif
      {
        // 其他获取命令，不需要参数
        va_end(va);
        return fcntl_f(fd, cmd);
      }
      break;
      case F_SETLK:
      case F_SETLKW:
      case F_GETLK:
      {
        // 文件锁相关命令
        struct flock *arg = va_arg(va, struct flock *);
        va_end(va);
        return fcntl_f(fd, cmd, arg);
      }
      break;
      case F_GETOWN_EX:
      case F_SETOWN_EX:
      {
        // 扩展的所有者命令
        struct f_owner_exlock *arg = va_arg(va, struct f_owner_exlock *);
        va_end(va);
        return fcntl_f(fd, cmd, arg);
      }
      break;
      default:
        // 其他命令，直接调用原始函数
        va_end(va);
        return fcntl_f(fd, cmd);
      }
    }

    /**
     * @brief hook ioctl函数
     * @param d 文件描述符
     * @param request 请求命令
     * @param ... 可变参数
     * @return 操作结果
     * @details 处理设备控制命令，特别是非阻塞标志的设置
     */
    int ioctl(int d, unsigned long int request, ...)
    {
      va_list va;
      va_start(va, request);          // 初始化可变参数列表
      void *arg = va_arg(va, void *); // 获取参数
      va_end(va);

      if (FIONBIO == request)
      {
        // 设置非阻塞标志
        bool user_nonblock = !!*(int *)arg;            // 转换为布尔值
        FdCtx::ptr ctx = FdMgr::GetInstance()->get(d); // 获取文件描述符上下文
        if (!ctx || ctx->isClose() || !ctx->isSocket())
        {
          return ioctl_f(d, request, arg); // 不是socket或已关闭，直接调用原始函数
        }
        ctx->setUserNonblock(user_nonblock); // 设置用户非阻塞标志
      }
      return ioctl_f(d, request, arg); // 调用原始函数
    }

    /**
     * @brief hook getsockopt函数
     * @param sockfd 套接字文件描述符
     * @param level 选项级别
     * @param optname 选项名称
     * @param optval 选项值
     * @param optlen 选项长度
     * @return 0表示成功，-1表示失败
     * @details 获取套接字选项，直接调用原始函数
     */
    int getsockopt(int sockfd, int level, int optname, void *optval, socklen_t *optlen)
    {
      return getsockopt_f(sockfd, level, optname, optval, optlen); // 直接调用原始函数
    }

    /**
     * @brief hook setsockopt函数
     * @param sockfd 套接字文件描述符
     * @param level 选项级别
     * @param optname 选项名称
     * @param optval 选项值
     * @param optlen 选项长度
     * @return 0表示成功，-1表示失败
     * @details 设置套接字选项，特别处理超时选项
     */
    int setsockopt(int sockfd, int level, int optname, const void *optval, socklen_t optlen)
    {
      if (!t_hook_enable)
      {
        return setsockopt_f(sockfd, level, optname, optval, optlen); // 未启用hook，直接调用原始函数
      }
      if (level == SOL_SOCKET)
      {
        if (optname == SO_RCVTIMEO || optname == SO_SNDTIMEO)
        {
          // 设置接收或发送超时
          FdCtx::ptr ctx = FdMgr::GetInstance()->get(sockfd); // 获取文件描述符上下文
          if (ctx)
          {
            const timeval *v = (const timeval *)optval; // 转换为timeval结构
            // 将秒和微秒转换为毫秒
            ctx->setTimeout(optname, v->tv_sec * 1000 + v->tv_usec / 1000);
          }
        }
      }
      return setsockopt_f(sockfd, level, optname, optval, optlen); // 调用原始函数
    }
  }
}