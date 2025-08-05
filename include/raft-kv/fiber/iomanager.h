#pragma once

#include "fcntl.h"
#include "scheduler.h"
#include "string.h"
#include "sys/epoll.h"
#include "timer.h"

namespace monsoon
{
  /**
   * @brief IO事件类型枚举
   */
  enum Event
  {
    NONE = 0x0,  // 无事件
    READ = 0x1,  // 读事件
    WRITE = 0x4, // 写事件
  };

  /**
   * @brief 事件上下文结构体
   * @details 存储与IO事件相关的调度器和协程信息
   */
  struct EventContext
  {
    Scheduler *scheduler = nullptr; // 调度器指针
    Fiber::ptr fiber;               // 协程指针
    std::function<void()> cb;       // 回调函数
  };

  /**
   * @brief 文件描述符上下文类
   * @details 管理单个文件描述符的IO事件
   */
  class FdContext
  {
    friend class IOManager;

  public:
    /**
     * @brief 获取事件上下文
     * @param event 事件类型
     * @return 对应事件的上下文引用
     */
    EventContext &getEveContext(Event event);

    /**
     * @brief 重置事件上下文
     * @param ctx 要重置的事件上下文
     */
    void resetEveContext(EventContext &ctx);

    /**
     * @brief 触发事件
     * @param event 要触发的事件类型
     */
    void triggerEvent(Event event);

  private:
    EventContext read;   // 读事件上下文
    EventContext write;  // 写事件上下文
    int fd = 0;          // 文件描述符
    Event events = NONE; // 当前注册的事件
    Mutex mutex;         // 互斥锁
  };

  /**
   * @brief IO管理器类
   * @details 继承自调度器和定时器管理器，实现IO事件驱动的协程调度
   *          使用epoll实现高效的IO多路复用
   */
  class IOManager : public Scheduler, public TimerManager
  {
  public:
    typedef std::shared_ptr<IOManager> ptr;

    /**
     * @brief 构造函数
     * @param threads 工作线程数量
     * @param use_caller 是否使用调用者线程作为工作线程
     * @param name IO管理器名称
     */
    IOManager(size_t threads = 1, bool use_caller = true, const std::string &name = "IOManager");

    /**
     * @brief 析构函数
     */
    ~IOManager();

    /**
     * @brief 添加IO事件
     * @param fd 文件描述符
     * @param event 事件类型
     * @param cb 事件触发时的回调函数
     * @return 0表示成功，-1表示失败
     */
    int addEvent(int fd, Event event, std::function<void()> cb = nullptr);

    /**
     * @brief 删除IO事件
     * @param fd 文件描述符
     * @param event 事件类型
     * @return true表示成功，false表示失败
     */
    bool delEvent(int fd, Event event);

    /**
     * @brief 取消IO事件
     * @param fd 文件描述符
     * @param event 事件类型
     * @return true表示成功，false表示失败
     */
    bool cancelEvent(int fd, Event event);

    /**
     * @brief 取消所有IO事件
     * @param fd 文件描述符
     * @return true表示成功，false表示失败
     */
    bool cancelAll(int fd);

    /**
     * @brief 获取当前线程的IO管理器
     * @return 当前线程的IO管理器指针
     */
    static IOManager *GetThis();

  protected:
    /**
     * @brief 通知调度器有任务要调度
     * @details 重写父类方法，通过管道通知epoll
     */
    void tickle() override;

    /**
     * @brief 判断是否可以停止
     * @return true表示可以停止，false表示不能停止
     */
    bool stopping() override;

    /**
     * @brief idle协程
     * @details 重写父类方法，实现epoll等待
     */
    void idle() override;

    /**
     * @brief 判断是否可以停止，同时获取最近一个定时超时时间
     * @param timeout 输出参数，最近定时器的超时时间
     * @return true表示可以停止，false表示不能停止
     */
    bool stopping(uint64_t &timeout);

    /**
     * @brief 定时器插入到前端时的回调
     * @details 重写父类方法，通知epoll更新超时时间
     */
    void OnTimerInsertedAtFront() override;

    /**
     * @brief 调整上下文数组大小
     * @param size 新的数组大小
     */
    void contextResize(size_t size);

  private:
    int epfd_ = 0;                              // epoll文件描述符
    int tickleFds_[2];                          // 用于通知的管道文件描述符
    std::atomic<size_t> pendingEventCnt_ = {0}; // 正在等待执行的IO事件数量
    RWMutex mutex_;                             // 读写锁
    std::vector<FdContext *> fdContexts_;       // 文件描述符上下文数组
  };
}