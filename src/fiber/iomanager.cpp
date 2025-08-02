#include "iomanager.h"

namespace monsoon
{
  /**
   * @brief 获取事件上下文
   * @param event 事件类型
   * @return 对应事件的上下文引用
   */
  EventContext &FdContext::getEveContext(Event event)
  {
    switch (event)
    {
    case READ:
      return read; // 返回读事件上下文
    case WRITE:
      return write; // 返回写事件上下文
    default:
      CondPanic(false, "getContext error: unknow event"); // 未知事件类型
    }
    throw std::invalid_argument("getContext invalid event");
  }

  /**
   * @brief 重置事件上下文
   * @param ctx 要重置的事件上下文
   */
  void FdContext::resetEveContext(EventContext &ctx)
  {
    ctx.scheduler = nullptr; // 清空调度器指针
    ctx.fiber.reset();       // 清空协程指针
    ctx.cb = nullptr;        // 清空回调函数
  }

  /**
   * @brief 触发事件（只是将对应的fiber or cb 加入scheduler tasklist）
   * @param event 要触发的事件类型
   */
  void FdContext::triggerEvent(Event event)
  {
    CondPanic(events & event, "event hasn't been registed"); // 检查事件是否已注册
    events = (Event)(events & ~event);                       // 清除已触发的事件
    EventContext &ctx = getEveContext(event);                // 获取事件上下文
    if (ctx.cb)
    {
      ctx.scheduler->scheduler(ctx.cb); // 调度回调函数
    }
    else
    {
      ctx.scheduler->scheduler(ctx.fiber); // 调度协程
    }
    resetEveContext(ctx); // 重置事件上下文
    return;
  }

  /**
   * @brief IO管理器构造函数
   * @param threads 工作线程数量
   * @param use_caller 是否使用调用者线程
   * @param name 管理器名称
   */
  IOManager::IOManager(size_t threads, bool use_caller, const std::string &name) : Scheduler(threads, use_caller, name)
  {
    epfd_ = epoll_create(5000);        // 创建epoll实例
    int ret = pipe(tickleFds_);        // 创建管道用于通知
    CondPanic(ret == 0, "pipe error"); // 检查管道创建是否成功

    // 注册pipe读句柄的可读事件，用于tickle调度协程
    epoll_event event{};
    memset(&event, 0, sizeof(epoll_event)); // 清空事件结构
    event.events = EPOLLIN | EPOLLET;       // 设置读事件和边缘触发
    event.data.fd = tickleFds_[0];          // 设置文件描述符
    // 边缘触发，设置非阻塞
    ret = fcntl(tickleFds_[0], F_SETFL, O_NONBLOCK); // 设置非阻塞
    CondPanic(ret == 0, "set fd nonblock error");    // 检查设置是否成功
    // 注册管道读描述符
    ret = epoll_ctl(epfd_, EPOLL_CTL_ADD, tickleFds_[0], &event); // 添加到epoll
    CondPanic(ret == 0, "epoll_ctl error");                       // 检查添加是否成功

    contextResize(32); // 初始化上下文数组大小

    // 启动scheduler，开始进行协程调度
    start();
  }

  /**
   * @brief IO管理器析构函数
   */
  IOManager::~IOManager()
  {
    stop();               // 停止调度器
    close(epfd_);         // 关闭epoll文件描述符
    close(tickleFds_[0]); // 关闭管道读端
    close(tickleFds_[1]); // 关闭管道写端

    // 释放所有文件描述符上下文
    for (size_t i = 0; i < fdContexts_.size(); i++)
    {
      if (fdContexts_[i])
      {
        delete fdContexts_[i]; // 删除上下文
      }
    }
  }

  /**
   * @brief 添加IO事件
   * @param fd 文件描述符
   * @param event 事件类型
   * @param cb 回调函数
   * @return 0表示成功，-1表示失败
   */
  int IOManager::addEvent(int fd, Event event, std::function<void()> cb)
  {
    FdContext *fd_ctx = nullptr;
    RWMutex::ReadLock lock(mutex_); // 加读锁
    // TODO：可以使用map代替
    // 找到fd对应的fdContext,没有则创建
    if ((int)fdContexts_.size() > fd)
    {
      fd_ctx = fdContexts_[fd]; // 获取已存在的上下文
      lock.unlock();            // 释放读锁
    }
    else
    {
      lock.unlock();                    // 释放读锁
      RWMutex::WriteLock lock2(mutex_); // 加写锁
      contextResize(fd * 1.5);          // 调整上下文数组大小
      fd_ctx = fdContexts_[fd];         // 获取上下文
    }

    // 同一个fd不允许注册重复事件
    Mutex::Lock ctxLock(fd_ctx->mutex);
    CondPanic(!(fd_ctx->events & event), "addevent error, fd = " + fd);

    int op = fd_ctx->events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    epoll_event epevent;
    epevent.events = EPOLLET | fd_ctx->events | event;
    epevent.data.ptr = fd_ctx;

    int ret = epoll_ctl(epfd_, op, fd, &epevent);
    if (ret)
    {
      std::cout << "addevent: epoll ctl error" << std::endl;
      return -1;
    }
    // 待执行IO事件数量
    ++pendingEventCnt_;

    // 赋值fd对应的event事件的EventContext
    fd_ctx->events = (Event)(fd_ctx->events | event);
    EventContext &event_ctx = fd_ctx->getEveContext(event);
    CondPanic(!event_ctx.scheduler && !event_ctx.fiber && !event_ctx.cb, "event_ctx is nullptr");

    event_ctx.scheduler = Scheduler::GetThis();
    if (cb)
    {
      // 设置了回调函数
      event_ctx.cb.swap(cb);
    }
    else
    {
      // 未设置回调函数，则将当前协程设置为回调任务
      event_ctx.fiber = Fiber::GetThis();
      CondPanic(event_ctx.fiber->getState() == Fiber::RUNNING, "state=" + event_ctx.fiber->getState());
    }
    std::cout << "add event success,fd = " << fd << std::endl;
    return 0;
  }
  // 删除事件 (删除前不会主动触发事件)
  bool IOManager::delEvent(int fd, Event event)
  {
    RWMutex::ReadLock lock(mutex_);
    if ((int)fdContexts_.size() <= fd)
    {
      // 找不到当前事件，返回
      return false;
    }
    FdContext *fd_ctx = fdContexts_[fd];
    lock.unlock();

    Mutex::Lock ctxLock(fd_ctx->mutex);
    if (!(fd_ctx->events & event))
    {
      return false;
    }
    // 清理指定事件
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;
    // 注册删除事件
    int ret = epoll_ctl(epfd_, op, fd, &epevent);
    if (ret)
    {
      std::cout << "delevent: epoll_ctl error" << std::endl;
      return false;
    }
    --pendingEventCnt_;
    fd_ctx->events = new_events;
    EventContext &event_ctx = fd_ctx->getEveContext(event);
    fd_ctx->resetEveContext(event_ctx);
    return true;
  }

  // 取消事件 （取消前会主动触发事件）
  bool IOManager::cancelEvent(int fd, Event event)
  {
    RWMutex::ReadLock lock(mutex_);
    if ((int)fdContexts_.size() <= fd)
    {
      // 找不到当前事件，返回
      return false;
    }
    FdContext *fd_ctx = fdContexts_[fd];
    lock.unlock();

    Mutex::Lock ctxLock(fd_ctx->mutex);
    if (!(fd_ctx->events & event))
    {
      return false;
    }
    // 清理指定事件
    Event new_events = (Event)(fd_ctx->events & ~event);
    int op = new_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events = EPOLLET | new_events;
    epevent.data.ptr = fd_ctx;
    // 注册删除事件
    int ret = epoll_ctl(epfd_, op, fd, &epevent);
    if (ret)
    {
      std::cout << "delevent: epoll_ctl error" << std::endl;
      return false;
    }
    // 删除之前，触发以此事件
    fd_ctx->triggerEvent(event);
    --pendingEventCnt_;
    return true;
  }
  // 取消fd所有事件
  bool IOManager::cancelAll(int fd)
  {
    RWMutex::ReadLock lock(mutex_);
    if ((int)fdContexts_.size() <= fd)
    {
      // 找不到当前事件，返回
      return false;
    }
    FdContext *fd_ctx = fdContexts_[fd];
    lock.unlock();

    Mutex::Lock ctxLock(fd_ctx->mutex);
    if (!fd_ctx->events)
    {
      return false;
    }

    int op = EPOLL_CTL_DEL;
    epoll_event epevent;
    epevent.events = 0;
    epevent.data.ptr = fd_ctx;
    // 注册删除事件
    int ret = epoll_ctl(epfd_, op, fd, &epevent);
    if (ret)
    {
      std::cout << "delevent: epoll_ctl error" << std::endl;
      return false;
    }
    // 触发全部已注册事件
    if (fd_ctx->events & READ)
    {
      fd_ctx->triggerEvent(READ);
      --pendingEventCnt_;
    }
    if (fd_ctx->events & WRITE)
    {
      fd_ctx->triggerEvent(WRITE);
      --pendingEventCnt_;
    }
    CondPanic(fd_ctx->events == 0, "fd not totally clear");
    return true;
  }
  IOManager *IOManager::GetThis() { return dynamic_cast<IOManager *>(Scheduler::GetThis()); }

  // 通知调度器有任务到来
  void IOManager::tickle()
  {
    if (!isHasIdleThreads())
    {
      // 此时没有空闲的调度线程
      return;
    }
    // 写pipe管道，使得idle协程凑够epoll_wait退出，开始调度任务
    int rt = write(tickleFds_[1], "T", 1);
    CondPanic(rt == 1, "write pipe error");
  }

  // 调度器无任务则阻塞在idle线程上
  // 当有新事件触发，则退出idle状态，则执行回调函数
  // 当有新的调度任务，则退出idle状态，并执行对应任务
  void IOManager::idle()
  {
    // 以此最多检测256个就绪事件
    const uint64_t MAX_EVENTS = 256;
    epoll_event *events = new epoll_event[MAX_EVENTS]();
    std::shared_ptr<epoll_event> shared_events(events, [](epoll_event *ptr)
                                               { delete[] ptr; });

    while (true)
    {
      // std::cout << "[IOManager] idle begin..." << std::endl;
      //  获取下一个定时器超时时间，同时判断调度器是否已经stop
      uint64_t next_timeout = 0;
      if (stopping(next_timeout))
      {
        std::cout << "name=" << getName() << "idle stopping exit";
        break;
      }

      // 阻塞等待，等待事件发生 或者 定时器超时
      int ret = 0;
      do
      {
        static const int MAX_TIMEOUT = 5000;

        if (next_timeout != ~0ull)
        {
          next_timeout = std::min((int)next_timeout, MAX_TIMEOUT);
        }
        else
        {
          next_timeout = MAX_TIMEOUT;
        }
        // 阻塞等待事件就绪
        ret = epoll_wait(epfd_, events, MAX_EVENTS, (int)next_timeout);
        // std::cout << "wait..." << std::endl;
        if (ret < 0)
        {
          if (errno == EINTR)
          {
            // 系统调用被信号中断
            continue;
          }
          std::cout << "epoll_wait [" << epfd_ << "] errno,err: " << errno << std::endl;
          break;
        }
        else
        {
          break;
        }
      } while (true);

      // 收集所有超时定时器，执行回调函数
      std::vector<std::function<void()>> cbs;
      listExpiredCb(cbs);
      if (!cbs.empty())
      {
        for (const auto &cb : cbs)
        {
          scheduler(cb);
        }
        cbs.clear();
      }

      for (int i = 0; i < ret; i++)
      {
        epoll_event &event = events[i];
        if (event.data.fd == tickleFds_[0])
        {
          // pipe管道内数据无意义，只是tickle意义,读完即可
          uint8_t dummy[256];
          // TODO：ET下阻塞读取可能有问题
          while (read(tickleFds_[0], dummy, sizeof(dummy)) > 0)
            ;
          continue;
        }

        //  通过epoll_event的私有指针获取FdContext
        FdContext *fd_ctx = (FdContext *)event.data.ptr;
        Mutex::Lock lock(fd_ctx->mutex);

        // 错误事件 or 挂起事件(对端关闭)
        if (event.events & (EPOLLERR | EPOLLHUP))
        {
          std::cout << "error events" << std::endl;
          event.events |= (EPOLLIN | EPOLLOUT) & fd_ctx->events;
        }
        // 实际发生的事件类型
        int real_events = NONE;
        if (event.events & EPOLLIN)
        {
          real_events |= READ;
        }
        if (event.events & EPOLLOUT)
        {
          real_events |= WRITE;
        }
        if ((fd_ctx->events & real_events) == NONE)
        {
          // 触发的事件类型与注册的事件类型无交集
          continue;
        }
        // 剔除已经发生的事件，将剩余的事件重新加入epoll_wait
        // issue: 在处理 EPOLLERR 或 EPOLLHUP 事件时，可能需要重新注
        // 册 EPOLLIN 或 EPOLLOUT 事件，以确保后续的 IO 可以正常进行
        int left_events = (fd_ctx->events & ~real_events);
        int op = left_events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
        event.events = EPOLLET | left_events;

        int ret2 = epoll_ctl(epfd_, op, fd_ctx->fd, &event);
        if (ret2)
        {
          std::cout << "epoll_wait [" << epfd_ << "] errno,err: " << errno << std::endl;
          continue;
        }
        // 处理已就绪事件 （加入scheduler tasklist,未调度执行）
        if (real_events & READ)
        {
          fd_ctx->triggerEvent(READ);
          --pendingEventCnt_;
        }
        if (real_events & WRITE)
        {
          fd_ctx->triggerEvent(WRITE);
          --pendingEventCnt_;
        }
      }
      // 处理结束，idle协程yield,此时调度协程可以执行run去tasklist中
      // 检测，拿取新任务去调度
      Fiber::ptr cur = Fiber::GetThis();
      auto raw_ptr = cur.get();
      cur.reset();
      // std::cout << "[IOManager] idle yield..." << std::endl;
      raw_ptr->yield();
    }
  }

  bool IOManager::stopping()
  {
    uint64_t timeout = 0;
    return stopping(timeout);
  }

  bool IOManager::stopping(uint64_t &timeout)
  {
    // 所有待调度的Io事件执行结束后，才允许退出
    timeout = getNextTimer();
    return timeout == ~0ull && pendingEventCnt_ == 0 && Scheduler::stopping();
  }

  void IOManager::contextResize(size_t size)
  {
    fdContexts_.resize(size);
    for (size_t i = 0; i < fdContexts_.size(); i++)
    {
      if (!fdContexts_[i])
      {
        fdContexts_[i] = new FdContext;
        fdContexts_[i]->fd = i;
      }
    }
  }
  void IOManager::OnTimerInsertedAtFront() { tickle(); }

} // namespace monsoon
