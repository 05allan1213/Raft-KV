#include "timer.h"
#include "utils.h"

namespace monsoon
{
  /**
   * @brief 定时器比较器
   * @param lhs 左操作数
   * @param rhs 右操作数
   * @return true表示lhs应该排在rhs前面
   */
  bool Timer::Comparator::operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const
  {
    if (!lhs && !rhs)
    {
      return false; // 两个都为空，不交换
    }
    if (!lhs)
    {
      return true; // lhs为空，排在前面
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
    return lhs.get() < rhs.get(); // 执行时间相同，按指针地址排序
  }

  /**
   * @brief 定时器构造函数
   * @param ms 超时时间（毫秒）
   * @param cb 回调函数
   * @param recuring 是否为循环定时器
   * @param manager 定时器管理器
   */
  Timer::Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager)
      : recurring_(recuring), ms_(ms), cb_(cb), manager_(manager)
  {
    next_ = GetElapsedMS() + ms_; // 计算下次执行时间
  }

  /**
   * @brief 定时器构造函数（用于比较器）
   * @param next 下次执行时间
   */
  Timer::Timer(uint64_t next) : next_(next) {}

  /**
   * @brief 取消定时器
   * @return true表示成功取消，false表示取消失败
   */
  bool Timer::cancel()
  {
    RWMutex::WriteLock lock(manager_->mutex_); // 加写锁
    if (cb_)
    {
      cb_ = nullptr;                                        // 清空回调函数
      auto it = manager_->timers_.find(shared_from_this()); // 查找定时器
      manager_->timers_.erase(it);                          // 从管理器中删除
      return true;
    }
    return false;
  }

  /**
   * @brief 刷新定时器
   * @return true表示成功刷新，false表示刷新失败
   */
  bool Timer::refresh()
  {
    RWMutex::RWMutex::WriteLock lock(manager_->mutex_); // 加写锁
    if (!cb_)
    {
      return false; // 没有回调函数，刷新失败
    }
    auto it = manager_->timers_.find(shared_from_this()); // 查找定时器
    if (it == manager_->timers_.end())
    {
      return false; // 定时器不存在
    }
    manager_->timers_.erase(it);                  // 从管理器中删除
    next_ = GetElapsedMS() + ms_;                 // 重新计算下次执行时间
    manager_->timers_.insert(shared_from_this()); // 重新插入管理器
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
    RWMutex::WriteLock lock(manager_->mutex_); // 加写锁
    if (!cb_)
    {
      return true; // 没有回调函数，重置成功
    }
    auto it = manager_->timers_.find(shared_from_this()); // 查找定时器
    if (it == manager_->timers_.end())
    {
      return false; // 定时器不存在
    }
    manager_->timers_.erase(it); // 从管理器中删除
    uint64_t start = 0;
    if (from_now)
    {
      start = GetElapsedMS(); // 从当前时间开始
    }
    else
    {
      start = next_ - ms_; // 从上一次开始时间计算
    }
    ms_ = ms;                                     // 设置新的超时时间
    next_ = start + ms_;                          // 计算新的下次执行时间
    manager_->addTimer(shared_from_this(), lock); // 重新添加到管理器
    return true;
  }

  TimerManager::TimerManager() { previouseTime_ = GetElapsedMS(); }

  TimerManager::~TimerManager() {}

  Timer::ptr TimerManager::addTimer(uint64_t ms, std::function<void()> cb, bool recurring)
  {
    Timer::ptr timer(new Timer(ms, cb, recurring, this));
    RWMutex::WriteLock lock(mutex_);
    addTimer(timer, lock);
    return timer;
  }

  static void OnTimer(std::weak_ptr<void> weak_cond, std::function<void()> cb)
  {
    std::shared_ptr<void> tmp = weak_cond.lock();
    if (tmp)
    {
      cb();
    }
  }

  Timer::ptr TimerManager::addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                                             bool recurring)
  {
    return addTimer(ms, std::bind(&OnTimer, weak_cond, cb), recurring);
  }

  uint64_t TimerManager::getNextTimer()
  {
    RWMutex::ReadLock lock(mutex_);
    tickled_ = false;
    if (timers_.empty())
    {
      return ~0ull;
    }
    const Timer::ptr &next = *timers_.begin();
    uint64_t now_ms = GetElapsedMS();
    if (now_ms >= next->next_)
    {
      return 0;
    }
    else
    {
      return next->next_ - now_ms;
    }
  }

  void TimerManager::listExpiredCb(std::vector<std::function<void()>> &cbs)
  {
    uint64_t now_ms = GetElapsedMS();
    std::vector<Timer::ptr> expired;
    {
      RWMutex::ReadLock lock(mutex_);
      if (timers_.empty())
      {
        return;
      }
    }
    RWMutex::WriteLock lock(mutex_);
    if (timers_.empty())
    {
      return;
    }
    bool rollover = false;
    if (detectClockRolllover(now_ms))
    {
      rollover = true;
    }
    if (!rollover && ((*timers_.begin())->next_ > now_ms))
    {
      return;
    }

    Timer::ptr now_timer(new Timer(now_ms));
    auto it = rollover ? timers_.end() : timers_.lower_bound(now_timer);
    while (it != timers_.end() && (*it)->next_ == now_ms)
    {
      ++it;
    }
    expired.insert(expired.begin(), timers_.begin(), it);
    timers_.erase(timers_.begin(), it);

    cbs.reserve(expired.size());
    for (auto &timer : expired)
    {
      cbs.push_back(timer->cb_);
      if (timer->recurring_)
      {
        // 循环计时，重新加入堆中
        timer->next_ = now_ms + timer->ms_;
        timers_.insert(timer);
      }
      else
      {
        timer->cb_ = nullptr;
      }
    }
  }

  void TimerManager::addTimer(Timer::ptr val, RWMutex::WriteLock &lock)
  {
    auto it = timers_.insert(val).first;
    bool at_front = (it == timers_.begin()) && !tickled_;
    if (at_front)
    {
      tickled_ = true;
    }
    lock.unlock();
    if (at_front)
    {
      OnTimerInsertedAtFront();
    }
  }

  bool TimerManager::detectClockRolllover(uint64_t now_ms)
  {
    bool rollover = false;
    if (now_ms < previouseTime_ && now_ms < (previouseTime_ - 60 * 60 * 1000))
    {
      rollover = true;
    }
    previouseTime_ = now_ms;
    return rollover;
  }

  bool TimerManager::hasTimer()
  {
    RWMutex::ReadLock lock(mutex_);
    return !timers_.empty();
  }

} // namespace monsoon