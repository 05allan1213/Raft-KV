#pragma once

#include <stdio.h>
#include <ucontext.h>
#include <unistd.h>
#include <functional>
#include <iostream>
#include <memory>
#include "utils.h"

namespace monsoon
{
  /**
   * @brief 协程类
   * @details 实现用户态协程，支持协程的创建、切换、销毁等操作
   *          协程是轻量级的线程，可以在用户态进行上下文切换
   */
  class Fiber : public std::enable_shared_from_this<Fiber>
  {
  public:
    typedef std::shared_ptr<Fiber> ptr;

    /**
     * @brief 协程状态枚举
     */
    enum State
    {
      READY,   // 就绪态，刚创建后者yield后状态
      RUNNING, // 运行态，resume之后的状态
      TERM,    // 结束态，协程的回调函数执行完之后的状态
    };

  private:
    /**
     * @brief 私有构造函数，用于创建主协程
     * @details 初始化当前线程的协程功能，构造线程主协程对象
     */
    Fiber();

  public:
    /**
     * @brief 构造子协程
     * @param cb 协程执行的回调函数
     * @param stackSz 协程栈大小，0表示使用默认大小
     * @param run_in_scheduler 是否在调度器中运行
     */
    Fiber(std::function<void()> cb, size_t stackSz = 0, bool run_in_scheduler = true);

    /**
     * @brief 析构函数
     */
    ~Fiber();

    /**
     * @brief 重置协程状态，复用栈空间
     * @param cb 新的回调函数
     */
    void reset(std::function<void()> cb);

    /**
     * @brief 切换协程到运行态
     * @details 将当前协程切换到运行状态，开始执行协程函数
     */
    void resume();

    /**
     * @brief 让出协程执行权
     * @details 当前协程主动让出CPU，切换到其他协程执行
     */
    void yield();

    /**
     * @brief 获取协程Id
     * @return 协程的唯一标识符
     */
    uint64_t getId() const { return id_; }

    /**
     * @brief 获取协程状态
     * @return 当前协程的状态
     */
    State getState() const { return state_; }

    /**
     * @brief 设置当前正在运行的协程
     * @param f 要设置为当前协程的指针
     */
    static void SetThis(Fiber *f);

    /**
     * @brief 获取当前线程中的执行协程
     * @details 如果当前线程没有创建协程，则创建第一个协程，且该协程为当前线程的主协程
     * @return 当前协程的智能指针
     */
    static Fiber::ptr GetThis();

    /**
     * @brief 获取协程总数
     * @return 当前系统中协程的总数
     */
    static uint64_t TotalFiberNum();

    /**
     * @brief 协程回调函数
     * @details 协程执行的主要入口函数
     */
    static void MainFunc();

    /**
     * @brief 获取当前协程Id
     * @return 当前协程的ID
     */
    static uint64_t GetCurFiberID();

  private:
    uint64_t id_ = 0;          // 协程ID
    uint32_t stackSize_ = 0;   // 协程栈大小
    State state_ = READY;      // 协程状态
    ucontext_t ctx_;           // 协程上下文
    void *stack_ptr = nullptr; // 协程栈地址
    std::function<void()> cb_; // 协程回调函数
    bool isRunInScheduler_;    // 本协程是否参与调度器调度
  };
}
