#pragma once

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <thread>

#include "noncopyable.h"
#include "utils.h"

namespace monsoon
{
  /**
   * @brief 信号量类
   * @details 基于POSIX信号量实现的信号量封装
   */
  class Semaphore : Nonecopyable
  {
  public:
    /**
     * @brief 构造函数
     * @param count 信号量的初始值
     */
    Semaphore(uint32_t count = 0);

    /**
     * @brief 析构函数
     */
    ~Semaphore();

    /**
     * @brief 等待信号量
     * @details 如果信号量值大于0，则减1并返回；否则阻塞等待
     */
    void wait();

    /**
     * @brief 通知信号量
     * @details 将信号量值加1，唤醒等待的线程
     */
    void notify();

  private:
    sem_t semaphore_; // POSIX信号量
  };

  /**
   * @brief 局部锁类模板
   * @tparam T 锁类型
   * @details RAII风格的锁封装，自动管理锁的获取和释放
   */
  template <class T>
  struct ScopedLockImpl
  {
  public:
    /**
     * @brief 构造函数，自动获取锁
     * @param mutex 要锁定的互斥量引用
     */
    ScopedLockImpl(T &mutex) : m_(mutex)
    {
      m_.lock();
      isLocked_ = true;
    }

    /**
     * @brief 手动获取锁
     * @details 如果锁未被持有，则获取锁
     */
    void lock()
    {
      if (!isLocked_)
      {
        m_.lock();
        isLocked_ = true;
      }
    }

    /**
     * @brief 手动释放锁
     * @details 如果锁被持有，则释放锁
     */
    void unlock()
    {
      if (isLocked_)
      {
        m_.unlock();
        isLocked_ = false;
      }
    }

    /**
     * @brief 析构函数，自动释放锁
     */
    ~ScopedLockImpl()
    {
      unlock();
    }

  private:
    T &m_;          // 互斥量引用
    bool isLocked_; // 是否已经上锁
  };

  /**
   * @brief 读锁局部类模板
   * @tparam T 读写锁类型
   * @details RAII风格的读锁封装
   */
  template <class T>
  struct ReadScopedLockImpl
  {
  public:
    /**
     * @brief 构造函数，自动获取读锁
     * @param mutex 要锁定的读写锁引用
     */
    ReadScopedLockImpl(T &mutex) : mutex_(mutex)
    {
      mutex_.rdlock();
      isLocked_ = true;
    }

    /**
     * @brief 析构函数，自动释放锁
     */
    ~ReadScopedLockImpl() { unlock(); }

    /**
     * @brief 手动获取读锁
     */
    void lock()
    {
      if (!isLocked_)
      {
        mutex_.rdlock();
        isLocked_ = true;
      }
    }

    /**
     * @brief 手动释放锁
     */
    void unlock()
    {
      if (isLocked_)
      {
        mutex_.unlock();
        isLocked_ = false;
      }
    }

  private:
    T &mutex_;      // 读写锁引用
    bool isLocked_; // 是否已上锁
  };

  /**
   * @brief 写锁局部类模板
   * @tparam T 读写锁类型
   * @details RAII风格的写锁封装
   */
  template <class T>
  struct WriteScopedLockImpl
  {
  public:
    /**
     * @brief 构造函数，自动获取写锁
     * @param mutex 要锁定的读写锁引用
     */
    WriteScopedLockImpl(T &mutex) : mutex_(mutex)
    {
      mutex_.wrlock();
      isLocked_ = true;
    }

    /**
     * @brief 析构函数，自动释放锁
     */
    ~WriteScopedLockImpl() { unlock(); }

    /**
     * @brief 手动获取写锁
     */
    void lock()
    {
      if (!isLocked_)
      {
        mutex_.wrlock();
        isLocked_ = true;
      }
    }

    /**
     * @brief 手动释放锁
     */
    void unlock()
    {
      if (isLocked_)
      {
        mutex_.unlock();
        isLocked_ = false;
      }
    }

  private:
    T &mutex_;      // 读写锁引用
    bool isLocked_; // 是否已上锁
  };

  /**
   * @brief 互斥锁类
   * @details 基于POSIX互斥锁实现的互斥锁封装
   */
  class Mutex : Nonecopyable
  {
  public:
    typedef ScopedLockImpl<Mutex> Lock; // 局部锁类型定义

    /**
     * @brief 构造函数，初始化互斥锁
     */
    Mutex() { CondPanic(0 == pthread_mutex_init(&m_, nullptr), "lock init success"); }

    /**
     * @brief 获取锁
     */
    void lock() { CondPanic(0 == pthread_mutex_lock(&m_), "lock error"); }

    /**
     * @brief 释放锁
     */
    void unlock() { CondPanic(0 == pthread_mutex_unlock(&m_), "unlock error"); }

    /**
     * @brief 析构函数，销毁互斥锁
     */
    ~Mutex() { CondPanic(0 == pthread_mutex_destroy(&m_), "destroy lock error"); }

  private:
    pthread_mutex_t m_; // POSIX互斥锁
  };

  /**
   * @brief 读写锁类
   * @details 基于POSIX读写锁实现的读写锁封装
   */
  class RWMutex : Nonecopyable
  {
  public:
    typedef ReadScopedLockImpl<RWMutex> ReadLock;   // 局部读锁类型定义
    typedef WriteScopedLockImpl<RWMutex> WriteLock; // 局部写锁类型定义

    /**
     * @brief 构造函数，初始化读写锁
     */
    RWMutex() { pthread_rwlock_init(&m_, nullptr); }

    /**
     * @brief 析构函数，销毁读写锁
     */
    ~RWMutex() { pthread_rwlock_destroy(&m_); }

    /**
     * @brief 获取读锁
     */
    void rdlock() { pthread_rwlock_rdlock(&m_); }

    /**
     * @brief 获取写锁
     */
    void wrlock() { pthread_rwlock_wrlock(&m_); }

    /**
     * @brief 释放锁
     */
    void unlock() { pthread_rwlock_unlock(&m_); }

  private:
    pthread_rwlock_t m_; // POSIX读写锁
  };
} // namespace monsoon