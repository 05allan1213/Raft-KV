#pragma once

#include <memory>
#include <set>
#include <vector>
#include "mutex.h"

namespace monsoon
{
  class TimerManager;

  /**
   * @brief 定时器类
   * @details 表示一个定时器，可以是单次或循环定时器
   */
  class Timer : public std::enable_shared_from_this<Timer>
  {
    friend class TimerManager;

  public:
    typedef std::shared_ptr<Timer> ptr;

    /**
     * @brief 取消定时器
     * @return true表示成功取消，false表示取消失败
     */
    bool cancel();

    /**
     * @brief 刷新定时器
     * @return true表示成功刷新，false表示刷新失败
     */
    bool refresh();

    /**
     * @brief 重置定时器
     * @param ms 新的超时时间（毫秒）
     * @param from_now 是否从当前时间开始计算
     * @return true表示成功重置，false表示重置失败
     */
    bool reset(uint64_t ms, bool from_now);

  private:
    /**
     * @brief 私有构造函数，由TimerManager调用
     * @param ms 超时时间（毫秒）
     * @param cb 定时器触发时的回调函数
     * @param recuring 是否为循环定时器
     * @param manager 定时器管理器指针
     */
    Timer(uint64_t ms, std::function<void()> cb, bool recuring, TimerManager *manager);

    /**
     * @brief 私有构造函数，用于比较器
     * @param next 下次执行时间
     */
    Timer(uint64_t next);

    bool recurring_ = false;          // 是否是循环定时器
    uint64_t ms_ = 0;                 // 执行周期（毫秒）
    uint64_t next_ = 0;               // 精确的执行时间
    std::function<void()> cb_;        // 回调函数
    TimerManager *manager_ = nullptr; // 管理器指针

  private:
    /**
     * @brief 定时器比较器
     * @details 用于在set中按执行时间排序
     */
    struct Comparator
    {
      /**
       * @brief 比较两个定时器
       * @param lhs 左操作数
       * @param rhs 右操作数
       * @return true表示lhs应该排在rhs前面
       */
      bool operator()(const Timer::ptr &lhs, const Timer::ptr &rhs) const;
    };
  };

  /**
   * @brief 定时器管理器类
   * @details 管理多个定时器，提供定时器的添加、删除、查询等功能
   */
  class TimerManager
  {
    friend class Timer;

  public:
    /**
     * @brief 构造函数
     */
    TimerManager();

    /**
     * @brief 析构函数
     */
    virtual ~TimerManager();

    /**
     * @brief 添加定时器
     * @param ms 超时时间（毫秒）
     * @param cb 定时器触发时的回调函数
     * @param recuring 是否为循环定时器
     * @return 定时器指针
     */
    Timer::ptr addTimer(uint64_t ms, std::function<void()> cb, bool recuring = false);

    /**
     * @brief 添加条件定时器
     * @param ms 超时时间（毫秒）
     * @param cb 定时器触发时的回调函数
     * @param weak_cond 弱引用条件，当条件失效时定时器自动取消
     * @param recurring 是否为循环定时器
     * @return 定时器指针
     */
    Timer::ptr addConditionTimer(uint64_t ms, std::function<void()> cb, std::weak_ptr<void> weak_cond,
                                 bool recurring = false);

    /**
     * @brief 获取到最近一个定时器的时间间隔（毫秒）
     * @return 最近定时器的超时时间，如果没有定时器返回~0ull
     */
    uint64_t getNextTimer();

    /**
     * @brief 获取需要执行的定时器的回调函数列表
     * @param cbs 输出参数，存储需要执行的回调函数
     */
    void listExpiredCb(std::vector<std::function<void()>> &cbs);

    /**
     * @brief 检查是否有定时器
     * @return true表示有定时器，false表示没有定时器
     */
    bool hasTimer();

  protected:
    /**
     * @brief 当有新的定时器插入到定时器首部时执行该函数
     * @details 虚函数，子类可以重写此函数实现自定义逻辑
     */
    virtual void OnTimerInsertedAtFront() = 0;

    /**
     * @brief 将定时器添加到管理器
     * @param val 定时器指针
     * @param lock 写锁引用
     */
    void addTimer(Timer::ptr val, RWMutex::WriteLock &lock);

  private:
    /**
     * @brief 检测服务器时间是否被调后了
     * @param now_ms 当前时间（毫秒）
     * @return true表示时间被调后了，false表示时间正常
     */
    bool detectClockRolllover(uint64_t now_ms);

    RWMutex mutex_;                                  // 读写锁
    std::set<Timer::ptr, Timer::Comparator> timers_; // 定时器集合
    bool tickled_ = false;                           // 是否触发OnTimerInsertedAtFront
    uint64_t previouseTime_ = 0;                     // 上次执行时间
  };
} // namespace monsoon