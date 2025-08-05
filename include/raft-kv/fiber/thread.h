#pragma once

#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <functional>
#include <iostream>
#include <memory>
#include <thread>

namespace monsoon
{
  /**
   * @brief 线程类
   * @details 封装POSIX线程，提供线程的创建、管理、销毁等功能
   */
  class Thread
  {
  public:
    typedef std::shared_ptr<Thread> ptr;

    /**
     * @brief 构造函数
     * @param cb 线程执行的回调函数
     * @param name 线程名称
     */
    Thread(std::function<void()> cb, const std::string &name);

    /**
     * @brief 析构函数
     */
    ~Thread();

    /**
     * @brief 获取线程ID
     * @return 线程的进程ID
     */
    pid_t getId() const { return id_; }

    /**
     * @brief 获取线程名称
     * @return 线程的名称
     */
    const std::string &getName() const { return name_; }

    /**
     * @brief 等待线程结束
     * @details 阻塞当前线程，直到目标线程执行完毕
     */
    void join();

    /**
     * @brief 获取当前线程对象
     * @return 当前线程的指针
     */
    static Thread *GetThis();

    /**
     * @brief 获取当前线程名称
     * @return 当前线程的名称
     */
    static const std::string &GetName();

    /**
     * @brief 设置当前线程名称
     * @param name 要设置的线程名称
     */
    static void SetName(const std::string &name);

  private:
    /**
     * @brief 禁用拷贝构造函数
     */
    Thread(const Thread &) = delete;

    /**
     * @brief 禁用移动构造函数
     */
    Thread(const Thread &&) = delete;

    /**
     * @brief 禁用赋值操作符
     */
    Thread operator=(const Thread &) = delete;

    /**
     * @brief 线程执行函数
     * @param args 线程参数
     * @return 线程返回值
     */
    static void *run(void *args);

  private:
    pid_t id_;                 // 线程ID
    pthread_t thread_;         // POSIX线程句柄
    std::function<void()> cb_; // 线程回调函数
    std::string name_;         // 线程名称
  };
}