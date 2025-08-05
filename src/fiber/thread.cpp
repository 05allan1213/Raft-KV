#include "thread.h"
#include "utils.h"

namespace monsoon
{
  // 指向当前线程
  static thread_local Thread *cur_thread = nullptr;
  static thread_local std::string cur_thread_name = "UNKNOW";

  /**
   * @brief 线程构造函数
   * @param cb 线程执行的回调函数
   * @param name 线程名称
   */
  Thread::Thread(std::function<void()> cb, const std::string &name = "UNKNOW") : cb_(cb), name_(name)
  {
    if (name.empty())
    {
      name_ = "UNKNOW"; // 如果名称为空，设置为默认名称
    }

    int rt = pthread_create(&thread_, nullptr, &Thread::run, this); // 创建POSIX线程
    if (rt)
    {
      std::cout << "pthread_create error,name:" << name_ << std::endl;
      throw std::logic_error("pthread_create"); // 创建失败抛出异常
    }
  }

  /**
   * @brief 线程执行函数
   * @param arg 线程参数（Thread指针）
   * @return 线程返回值
   */
  void *Thread::run(void *arg)
  {
    Thread *thread = (Thread *)arg;       // 转换为Thread指针
    cur_thread = thread;                  // 设置当前线程指针
    cur_thread_name = thread->name_;      // 设置当前线程名称
    thread->id_ = monsoon::GetThreadId(); // 获取线程ID

    // 给线程命名（Linux系统调用）
    pthread_setname_np(pthread_self(), thread->name_.substr(0, 15).c_str());

    std::function<void()> cb;
    cb.swap(thread->cb_); // 交换回调函数，避免拷贝

    // 启动回调函数
    cb(); // 执行回调函数
    return 0;
  }

  /**
   * @brief 线程析构函数
   */
  Thread::~Thread()
  {
    if (thread_)
    {
      pthread_detach(thread_); // 分离线程，让系统自动回收资源
    }
  }

  /**
   * @brief 等待线程结束
   */
  void Thread::join()
  {
    if (thread_)
    {
      int rt = pthread_join(thread_, nullptr); // 等待线程结束
      if (rt)
      {
        std::cout << "pthread_join error,name:" << name_ << std::endl;
        throw std::logic_error("pthread_join"); // 等待失败抛出异常
      }
      thread_ = 0; // 清空线程句柄
    }
  }

  /**
   * @brief 获取当前线程对象
   * @return 当前线程指针
   */
  Thread *Thread::GetThis() { return cur_thread; }

  /**
   * @brief 获取当前线程名称
   * @return 当前线程名称
   */
  const std::string &Thread::GetName() { return cur_thread_name; }

  /**
   * @brief 设置当前线程名称
   * @param name 要设置的线程名称
   */
  void Thread::SetName(const std::string &name)
  {
    if (name.empty())
    {
      return; // 名称为空，直接返回
    }
    if (cur_thread)
    {
      cur_thread->name_ = name; // 设置当前线程对象的名称
    }
    cur_thread_name = name; // 设置当前线程名称
  }

}
