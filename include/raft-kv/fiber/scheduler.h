#pragma once

#include <atomic>
#include <boost/type_index.hpp>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <vector>
#include "fiber.h"
#include "mutex.h"
#include "thread.h"
#include "utils.h"

namespace monsoon
{
  /**
   * @brief 调度任务类
   * @details 封装协程或函数，用于调度器管理
   */
  class SchedulerTask
  {
  public:
    friend class Scheduler;

    /**
     * @brief 默认构造函数
     */
    SchedulerTask() { thread_ = -1; }

    /**
     * @brief 构造函数，使用协程和指定线程
     * @param f 协程指针
     * @param t 指定执行的线程ID
     */
    SchedulerTask(Fiber::ptr f, int t) : fiber_(f), thread_(t) {}

    /**
     * @brief 构造函数，使用协程指针的引用和指定线程
     * @param f 协程指针的引用
     * @param t 指定执行的线程ID
     */
    SchedulerTask(Fiber::ptr *f, int t)
    {
      fiber_.swap(*f);
      thread_ = t;
    }

    /**
     * @brief 构造函数，使用函数对象和指定线程
     * @param f 要执行的函数对象
     * @param t 指定执行的线程ID
     */
    SchedulerTask(std::function<void()> f, int t)
    {
      cb_ = f;
      thread_ = t;
    }

    /**
     * @brief 清空任务
     * @details 重置所有成员变量
     */
    void reset()
    {
      fiber_ = nullptr;
      cb_ = nullptr;
      thread_ = -1;
    }

  private:
    Fiber::ptr fiber_;         // 协程指针
    std::function<void()> cb_; // 回调函数
    int thread_;               // 指定执行的线程ID
  };

  /**
   * @brief N->M协程调度器
   * @details 实现N个线程调度M个协程的调度器
   *          支持协程和函数对象的调度
   */
  class Scheduler
  {
  public:
    typedef std::shared_ptr<Scheduler> ptr;

    /**
     * @brief 构造函数
     * @param threads 工作线程数量
     * @param use_caller 是否使用调用者线程作为工作线程
     * @param name 调度器名称
     */
    Scheduler(size_t threads = 1, bool use_caller = true, const std::string &name = "Scheduler");

    /**
     * @brief 析构函数
     */
    virtual ~Scheduler();

    /**
     * @brief 获取调度器名称
     * @return 调度器的名称
     */
    const std::string &getName() const { return name_; }

    /**
     * @brief 获取当前线程调度器
     * @return 当前线程的调度器指针
     */
    static Scheduler *GetThis();

    /**
     * @brief 获取当前线程的调度器协程
     * @return 当前线程的主协程指针
     */
    static Fiber *GetMainFiber();

    /**
     * @brief 添加调度任务
     * @tparam TaskType 任务类型，可以是协程对象或函数指针
     * @param task 任务
     * @param thread 指定执行函数的线程，-1为不指定
     */
    template <class TaskType>
    void scheduler(TaskType task, int thread = -1)
    {
      bool isNeedTickle = false;
      {
        Mutex::Lock lock(mutex_);
        isNeedTickle = schedulerNoLock(task, thread);
      }

      if (isNeedTickle)
      {
        tickle(); // 唤醒idle协程
      }
    }

    /**
     * @brief 启动调度器
     * @details 启动所有工作线程，开始调度任务
     */
    void start();

    /**
     * @brief 停止调度器,等待所有任务结束
     * @details 停止所有工作线程，等待任务完成
     */
    void stop();

  protected:
    /**
     * @brief 通知调度器任务到达
     * @details 虚函数，子类可以重写此函数实现自定义的通知机制
     */
    virtual void tickle();

    /**
     * @brief 协程调度函数
     * @details 默认会启用hook，处理协程的调度逻辑
     */
    void run();

    /**
     * @brief 无任务时执行idle协程
     * @details 虚函数，子类可以重写此函数实现自定义的idle逻辑
     */
    virtual void idle();

    /**
     * @brief 返回是否可以停止
     * @return true表示可以停止，false表示不能停止
     */
    virtual bool stopping();

    /**
     * @brief 设置当前线程调度器
     * @details 将当前线程与调度器关联
     */
    void setThis();

    /**
     * @brief 返回是否有空闲进程
     * @return true表示有空闲线程，false表示没有空闲线程
     */
    bool isHasIdleThreads() { return idleThreadCnt_ > 0; }

  private:
    /**
     * @brief 无锁下，添加调度任务
     * @tparam TaskType 任务类型
     * @param t 任务对象
     * @param thread 指定线程ID
     * @return 是否需要唤醒idle协程
     */
    template <class TaskType>
    bool schedulerNoLock(TaskType t, int thread)
    {
      bool isNeedTickle = tasks_.empty();
      SchedulerTask task(t, thread);
      if (task.fiber_ || task.cb_)
      {
        tasks_.push_back(task);
      }
      return isNeedTickle;
    }

    std::string name_;                          // 调度器名称
    Mutex mutex_;                               // 互斥锁
    std::vector<Thread::ptr> threadPool_;       // 线程池
    std::list<SchedulerTask> tasks_;            // 任务队列
    std::vector<int> threadIds_;                // 线程池id数组
    size_t threadCnt_ = 0;                      // 工作线程数量（不包含use_caller的主线程）
    std::atomic<size_t> activeThreadCnt_ = {0}; // 活跃线程数
    std::atomic<size_t> idleThreadCnt_ = {0};   // IDLE线程数
    bool isUseCaller_;                          // 是否是use caller
    Fiber::ptr rootFiber_;                      // use caller= true,调度器所在线程的调度协程
    int rootThread_ = 0;                        // use caller = true,调度器协程所在线程的id
    bool isStopped_ = false;                    // 是否已停止
  };
} // namespace monsoon