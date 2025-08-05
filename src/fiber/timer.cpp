#include "timer.h"
#include "utils.h"

namespace monsoon
{
  /**
   * @brief 定时器比较器
   * @param lhs 左操作数
   * @param rhs 右操作数
   * @return true表示lhs应该排在rhs前面
   *
   * 实现最小堆的比较逻辑，确保最早执行的定时器在堆顶
   */
  bool Timer::Comparator::operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const
  {
    if (!lhs && !rhs)
    {
      return false; // 两个都为空，不交换
    }
    if (!lhs)
    {
      return true; // lhs为空，排在前面（空指针优先）
    }
    if (!rhs)
    {
      return false; // rhs为空，不交换
    }
    if (lhs->next_ < rhs->next_)
    {
      return true; // lhs执行时间早，排在前面
    }
    if (rhs->next_ < lhs->next_)
    {
      return false; // rhs执行时间早，不交换
    }
    return lhs.get() < rhs.get(); // 执行时间相同，按指针地址排序（保证稳定性）
  }

  /**
   * @brief 定时器构造函数
   * @param ms 超时时间（毫秒）
   * @param cb 回调函数
   * @param recuring 是否为循环定时器
   * @param manager 定时器管理器
   *
   * 创建定时器并计算下次执行时间
   */
  Timer::Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager)
      : recurring_(recuring), ms_(ms), cb_(cb), manager_(manager)
  {
    next_ = GetElapsedMS() + ms_; // 计算下次执行时间（当前时间 + 超时时间）
  }

  /**
   * @brief 定时器构造函数（用于比较器）
   * @param next 下次执行时间
   *
   * 创建一个临时的定时器对象，仅用于在定时器集合中进行查找和比较
   */
  Timer::Timer(uint64_t next) : next_(next) {}

  /**
   * @brief 取消定时器
   * @return true表示成功取消，false表示取消失败
   *
   * 清空回调函数并从管理器中移除定时器
   */
  bool Timer::cancel()
  {
    RWMutex::WriteLock lock(manager_->mutex_); // 加写锁保护定时器集合
    if (cb_)
    {
      cb_ = nullptr;                                        // 清空回调函数，防止重复执行
      auto it = manager_->timers_.find(shared_from_this()); // 查找定时器在集合中的位置
      manager_->timers_.erase(it);                          // 从管理器中删除定时器
      return true;
    }
    return false;
  }

  /**
   * @brief 刷新定时器
   * @return true表示成功刷新，false表示刷新失败
   *
   * 重新计算定时器的下次执行时间，相当于重新启动定时器
   */
  bool Timer::refresh()
  {
    RWMutex::RWMutex::WriteLock lock(manager_->mutex_); // 加写锁保护定时器集合
    if (!cb_)
    {
      return false; // 没有回调函数，刷新失败
    }
    auto it = manager_->timers_.find(shared_from_this()); // 查找定时器在集合中的位置
    if (it == manager_->timers_.end())
    {
      return false; // 定时器不存在于管理器中
    }
    manager_->timers_.erase(it);                  // 从管理器中删除
    next_ = GetElapsedMS() + ms_;                 // 重新计算下次执行时间
    manager_->timers_.insert(shared_from_this()); // 重新插入管理器（会自动调整堆结构）
    return true;
  }

  /**
   * @brief 重置定时器，重新设置定时器触发时间
   * @param ms 新的超时时间（毫秒）
   * @param from_now 是否从当前时间开始计算
   * @return true表示成功重置，false表示重置失败
   * @details from_now = true: 下次触发时间从当前时刻开始计算
   *          from_now = false: 下次触发时间从上一次开始计算
   */
  bool Timer::reset(uint64_t ms, bool from_now)
  {
    if (ms == ms_ && !from_now)
    {
      return true; // 参数相同且不从当前时间开始，无需重置
    }
    RWMutex::WriteLock lock(manager_->mutex_); // 加写锁保护定时器集合
    if (!cb_)
    {
      return true; // 没有回调函数，重置成功
    }
    auto it = manager_->timers_.find(shared_from_this()); // 查找定时器在集合中的位置
    if (it == manager_->timers_.end())
    {
      return false; // 定时器不存在于管理器中
    }
    manager_->timers_.erase(it); // 从管理器中删除
    uint64_t start = 0;
    if (from_now)
    {
      start = GetElapsedMS(); // 从当前时间开始计算
    }
    else
    {
      start = next_ - ms_; // 从上一次开始时间计算（保持相对时间间隔）
    }
    ms_ = ms;                                     // 设置新的超时时间
    next_ = start + ms_;                          // 计算新的下次执行时间
    manager_->addTimer(shared_from_this(), lock); // 重新添加到管理器
    return true;
  }

  /**
   * @brief 定时器管理器构造函数
   *
   * 初始化管理器，记录当前时间作为基准时间
   */
  TimerManager::TimerManager() { previouseTime_ = GetElapsedMS(); }

  /**
   * @brief 定时器管理器析构函数
   */
  TimerManager::~TimerManager() {}

  /**
   * @brief 添加定时器
   * @param ms 超时时间（毫秒）
   * @param cb 回调函数
   * @param recurring 是否为循环定时器
   * @return 定时器指针
   *
   * 创建定时器并添加到管理器中
   */
  Timer::ptr TimerManager::addTimer(uint64_t ms, std::function<void()> cb, bool recurring)
  {
    Timer::ptr timer(new Timer(ms, cb, recurring, this)); // 创建定时器
    RWMutex::WriteLock lock(mutex_);                      // 加写锁
    addTimer(timer, lock);                                // 添加到管理器
    return timer;
  }

  /**
   * @brief 定时器回调包装函数
   * @param weak_cond 弱引用条件
   * @param cb 原始回调函数
   *
   * 检查条件是否仍然有效，如果有效则执行回调
   * 用于实现条件定时器，当条件失效时自动取消定时器
   */
  static void OnTimer(std::weak_ptr<void> weak_cond, std::function<void()> cb)
  {
    std::shared_ptr<void> tmp = weak_cond.lock(); // 尝试获取强引用
    if (tmp)
    {
      cb(); // 条件仍然有效，执行回调
    }
    // 如果条件已失效，不执行回调（自动取消定时器）
  }

  /**
   * @brief 添加条件定时器
   * @param ms 超时时间（毫秒）
   * @param cb 回调函数
   * @param weak_cond 弱引用条件
   * @param recurring 是否为循环定时器
   * @return 定时器指针
   *
   * 创建条件定时器，当条件失效时自动取消定时器
   */
  Timer::ptr TimerManager::addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                                             bool recurring)
  {
    return addTimer(ms, std::bind(&OnTimer, weak_cond, cb), recurring);
  }

  /**
   * @brief 获取下一个定时器的等待时间
   * @return 距离下一个定时器触发的时间（毫秒），如果没有定时器返回~0ull
   *
   * 用于优化事件循环的等待时间，避免不必要的唤醒
   */
  uint64_t TimerManager::getNextTimer()
  {
    RWMutex::ReadLock lock(mutex_); // 加读锁
    tickled_ = false;               // 重置唤醒标志
    if (timers_.empty())
    {
      return ~0ull; // 没有定时器，返回最大值
    }
    const Timer::ptr &next = *timers_.begin(); // 获取堆顶定时器（最早执行的）
    uint64_t now_ms = GetElapsedMS();
    if (now_ms >= next->next_)
    {
      return 0; // 定时器已过期，立即执行
    }
    else
    {
      return next->next_ - now_ms; // 返回等待时间
    }
  }

  /**
   * @brief 获取所有过期的定时器回调
   * @param cbs 输出参数，存储过期的回调函数
   *
   * 核心算法：批量处理过期的定时器，提高效率
   */
  void TimerManager::listExpiredCb(std::vector<std::function<void()>> &cbs)
  {
    uint64_t now_ms = GetElapsedMS(); // 获取当前时间
    std::vector<Timer::ptr> expired;  // 存储过期的定时器
    {
      RWMutex::ReadLock lock(mutex_); // 快速检查是否有定时器
      if (timers_.empty())
      {
        return;
      }
    }
    RWMutex::WriteLock lock(mutex_); // 加写锁进行批量操作
    if (timers_.empty())
    {
      return;
    }

    // 检测时钟回绕（系统时间被调整）
    bool rollover = false;
    if (detectClockRolllover(now_ms))
    {
      rollover = true;
    }

    // 检查是否有过期的定时器
    if (!rollover && ((*timers_.begin())->next_ > now_ms))
    {
      return; // 没有过期的定时器
    }

    // 创建临时定时器用于查找
    Timer::ptr now_timer(new Timer(now_ms));
    auto it = rollover ? timers_.end() : timers_.lower_bound(now_timer);

    // 处理相同执行时间的定时器（边界情况）
    while (it != timers_.end() && (*it)->next_ == now_ms)
    {
      ++it;
    }

    // 批量提取过期的定时器
    expired.insert(expired.begin(), timers_.begin(), it);
    timers_.erase(timers_.begin(), it);

    // 处理过期的定时器
    cbs.reserve(expired.size()); // 预分配空间，避免频繁重新分配
    for (auto &timer : expired)
    {
      cbs.push_back(timer->cb_); // 收集回调函数
      if (timer->recurring_)
      {
        // 循环定时器：重新计算下次执行时间并重新加入堆
        timer->next_ = now_ms + timer->ms_;
        timers_.insert(timer);
      }
      else
      {
        // 一次性定时器：清空回调函数
        timer->cb_ = nullptr;
      }
    }
  }

  /**
   * @brief 添加定时器到管理器
   * @param val 定时器指针
   * @param lock 写锁引用
   *
   * 内部方法，用于将定时器添加到集合中
   */
  void TimerManager::addTimer(Timer::ptr val, RWMutex::WriteLock &lock)
  {
    auto it = timers_.insert(val).first;                  // 插入定时器并获取迭代器
    bool at_front = (it == timers_.begin()) && !tickled_; // 检查是否插入到堆顶
    if (at_front)
    {
      tickled_ = true; // 设置唤醒标志
    }
    lock.unlock(); // 释放锁
    if (at_front)
    {
      OnTimerInsertedAtFront(); // 通知插入到堆顶（可能需要调整事件循环的等待时间）
    }
  }

  /**
   * @brief 检测时钟回绕
   * @param now_ms 当前时间
   * @return true表示检测到时钟回绕
   *
   * 检测系统时间是否被调整（向前或向后），用于处理时间异常情况
   */
  bool TimerManager::detectClockRolllover(uint64_t now_ms)
  {
    bool rollover = false;
    // 如果当前时间比上次记录的时间早1小时以上，认为发生了时钟回绕
    if (now_ms < previouseTime_ && now_ms < (previouseTime_ - 60 * 60 * 1000))
    {
      rollover = true;
    }
    previouseTime_ = now_ms; // 更新上次记录的时间
    return rollover;
  }

  /**
   * @brief 检查是否有定时器
   * @return true表示有定时器，false表示没有定时器
   */
  bool TimerManager::hasTimer()
  {
    RWMutex::ReadLock lock(mutex_); // 加读锁
    return !timers_.empty();
  }

}