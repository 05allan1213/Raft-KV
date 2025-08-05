#include "fiber.h"
#include <assert.h>
#include <atomic>
#include "scheduler.h"
#include "utils.h"

namespace monsoon
{
  const bool DEBUG = true;
  // 当前线程正在运行的协程
  static thread_local Fiber *cur_fiber = nullptr;
  // 当前线程的主协程
  static thread_local Fiber::ptr cur_thread_fiber = nullptr;
  // 用于生成协程Id
  static std::atomic<uint64_t> cur_fiber_id{0};
  // 统计当前协程数
  static std::atomic<uint64_t> fiber_count{0};
  // 协议栈默认大小 128k
  static int g_fiber_stack_size = 128 * 1024;

  /**
   * @brief 栈分配器类
   * @details 负责协程栈空间的分配和释放
   */
  class StackAllocator
  {
  public:
    /**
     * @brief 分配栈空间
     * @param size 栈大小
     * @return 栈空间指针
     */
    static void *Alloc(size_t size) { return malloc(size); }

    /**
     * @brief 释放栈空间
     * @param vp 栈空间指针
     * @param size 栈大小
     */
    static void Delete(void *vp, size_t size) { return free(vp); }
  };

  /**
   * @brief 主协程构造函数
   * @details 只用于创建主协程，初始化当前线程的协程环境
   */
  Fiber::Fiber()
  {
    SetThis(this);                                         // 设置当前协程为this
    state_ = RUNNING;                                      // 设置状态为运行态
    CondPanic(getcontext(&ctx_) == 0, "getcontext error"); // 获取当前上下文
    ++fiber_count;                                         // 增加协程计数
    id_ = cur_fiber_id++;                                  // 分配协程ID
    std::cout << "[fiber] create fiber , id = " << id_ << std::endl;
  }

  /**
   * @brief 设置当前协程
   * @param f 要设置为当前协程的指针
   */
  void Fiber::SetThis(Fiber *f) { cur_fiber = f; }

  /**
   * @brief 获取当前执行协程，不存在则创建主协程
   * @return 当前协程的智能指针
   */
  Fiber::ptr Fiber::GetThis()
  {
    if (cur_fiber)
    {
      return cur_fiber->shared_from_this();
    }
    // 创建主协程并初始化
    Fiber::ptr main_fiber(new Fiber);
    CondPanic(cur_fiber == main_fiber.get(), "cur_fiber need to be main_fiber");
    cur_thread_fiber = main_fiber; // 保存主协程指针
    return cur_fiber->shared_from_this();
  }

  /**
   * @brief 子协程构造函数
   * @param cb 协程执行的回调函数
   * @param stacksize 栈大小
   * @param run_inscheduler 是否在调度器中运行
   */
  Fiber::Fiber(std::function<void()> cb, size_t stacksize, bool run_inscheduler)
      : id_(cur_fiber_id++), cb_(cb), isRunInScheduler_(run_inscheduler)
  {
    ++fiber_count;                                               // 增加协程计数
    stackSize_ = stacksize > 0 ? stacksize : g_fiber_stack_size; // 设置栈大小
    stack_ptr = StackAllocator::Alloc(stackSize_);               // 分配栈空间
    CondPanic(getcontext(&ctx_) == 0, "getcontext error");       // 获取当前上下文

    // 初始化协程上下文
    ctx_.uc_link = nullptr;                  // 协程结束后不链接到其他协程
    ctx_.uc_stack.ss_sp = stack_ptr;         // 设置栈指针
    ctx_.uc_stack.ss_size = stackSize_;      // 设置栈大小
    makecontext(&ctx_, &Fiber::MainFunc, 0); // 设置协程入口函数
  }

  /**
   * @brief 切换当前协程到执行态,并保存主协程的上下文
   * @details 将当前协程切换到运行状态，开始执行协程函数
   */
  void Fiber::resume()
  {
    CondPanic(state_ != TERM && state_ != RUNNING, "state error"); // 检查状态
    SetThis(this);                                                 // 设置当前协程为this
    state_ = RUNNING;                                              // 设置状态为运行态

    if (isRunInScheduler_)
    {
      // 当前协程参与调度器调度，则与调度器主协程进行swap
      CondPanic(0 == swapcontext(&(Scheduler::GetMainFiber()->ctx_), &ctx_),
                "isRunInScheduler_ = true,swapcontext error");
    }
    else
    {
      // 切换主协程到当前协程，并保存主协程上下文到子协程ctx_
      CondPanic(0 == swapcontext(&(cur_thread_fiber->ctx_), &ctx_), "isRunInScheduler_ = false,swapcontext error");
    }
  }

  /**
   * @brief 当前协程让出执行权
   * @details 协程执行完成之后会自动yield,回到主协程，此时状态为TERM
   */
  void Fiber::yield()
  {
    CondPanic(state_ == TERM || state_ == RUNNING, "state error"); // 检查状态
    SetThis(cur_thread_fiber.get());                               // 设置当前协程为主协程
    if (state_ != TERM)
    {
      state_ = READY; // 如果不是结束状态，设置为就绪状态
    }
    if (isRunInScheduler_)
    {
      // 如果参与调度器调度，切换回调度器主协程
      CondPanic(0 == swapcontext(&ctx_, &(Scheduler::GetMainFiber()->ctx_)),
                "isRunInScheduler_ = true,swapcontext error");
    }
    else
    {
      // 切换当前协程到主协程，并保存子协程的上下文到主协程ctx_
      CondPanic(0 == swapcontext(&ctx_, &(cur_thread_fiber->ctx_)), "swapcontext failed");
    }
  }

  /**
   * @brief 协程入口函数
   * @details 协程开始执行时调用的函数，执行完成后自动yield
   */
  void Fiber::MainFunc()
  {
    Fiber::ptr cur = GetThis(); // 获取当前协程
    CondPanic(cur != nullptr, "cur is nullptr");

    cur->cb_();         // 执行协程回调函数
    cur->cb_ = nullptr; // 清空回调函数
    cur->state_ = TERM; // 设置状态为结束态

    // 手动使得cur_fiber引用计数减1
    auto raw_ptr = cur.get(); // 保存原始指针
    cur.reset();              // 释放智能指针

    // 协程结束，自动yield,回到主协程
    // 访问原始指针原因：reset后cur已经被释放
    raw_ptr->yield();
  }

  /**
   * @brief 协程重置（复用已经结束的协程，复用其栈空间，创建新协程）
   * @param cb 新的回调函数
   * @details 暂时不允许Ready状态下的重置
   */
  void Fiber::reset(std::function<void()> cb)
  {
    CondPanic(stack_ptr, "stack is nullptr");               // 检查栈指针
    CondPanic(state_ == TERM, "state isn't TERM");          // 检查状态
    cb_ = cb;                                               // 设置新的回调函数
    CondPanic(0 == getcontext(&ctx_), "getcontext failed"); // 获取当前上下文

    // 重新初始化协程上下文
    ctx_.uc_link = nullptr;                  // 协程结束后不链接到其他协程
    ctx_.uc_stack.ss_sp = stack_ptr;         // 设置栈指针
    ctx_.uc_stack.ss_size = stackSize_;      // 设置栈大小
    makecontext(&ctx_, &Fiber::MainFunc, 0); // 设置协程入口函数
    state_ = READY;                          // 设置状态为就绪态
  }

  /**
   * @brief 获取当前协程ID
   * @return 当前协程的ID
   */
  uint64_t Fiber::GetCurFiberID()
  {
    if (cur_fiber)
    {
      return cur_fiber->getId();
    }
    return 0;
  }

  /**
   * @brief 获取协程总数
   * @return 当前系统中协程的总数
   */
  uint64_t Fiber::TotalFiberNum()
  {
    return fiber_count;
  }

  /**
   * @brief 析构函数
   * @details 释放协程资源，包括栈空间
   */
  Fiber::~Fiber()
  {
    --fiber_count; // 减少协程计数
    if (stack_ptr)
    {
      // 有栈空间，说明是子协程
      CondPanic(state_ == TERM, "fiber state should be term"); // 检查状态
      StackAllocator::Delete(stack_ptr, stackSize_);           // 释放栈空间
    }
    else
    {
      // 没有栈空间，说明是线程的主协程
      CondPanic(!cb_, "main fiber no callback");                          // 主协程不应该有回调函数
      CondPanic(state_ == RUNNING, "main fiber state should be running"); // 主协程状态应该是运行态

      Fiber *cur = cur_fiber; // 获取当前协程指针
      if (cur == this)
      {
        SetThis(nullptr);
      }
    }
  }

}