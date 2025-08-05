#include "fiber/channel.h"
#include "fiber/iomanager.h"
#include "fiber/scheduler.h"
#include "raftCore/ApplyMsg.h"
#include "common/util.h"
#include <algorithm>
#include <random>
#include <thread>

namespace monsoon
{
  // 显式实例化常用类型
  template class Channel<int>;
  template class Channel<std::string>;
  template class Channel<void *>;
  template class Channel<ApplyMsg>;
  template class Channel<Op>;

  /**
   * @brief Channel构造函数
   * @tparam T 通道数据类型
   * @param capacity 通道容量
   * @param scheduler 调度器指针
   *
   * 创建一个指定容量的通道，用于协程间通信
   */
  template <typename T>
  Channel<T>::Channel(size_t capacity, Scheduler *scheduler)
      : capacity_(capacity), scheduler_(scheduler)
  {
    // 如果没有指定调度器，尝试获取当前的IOManager
    if (!scheduler_)
    {
      scheduler_ = IOManager::GetThis();
    }
  }

  /**
   * @brief Channel析构函数
   * @tparam T 通道数据类型
   *
   * 析构时自动关闭通道
   */
  template <typename T>
  Channel<T>::~Channel()
  {
    close();
  }

  /**
   * @brief 发送数据（阻塞方式）
   * @tparam T 通道数据类型
   * @param data 要发送的数据
   * @param timeoutMs 超时时间（毫秒）
   * @return 发送结果
   *
   * 阻塞发送数据到通道，支持超时控制
   */
  template <typename T>
  ChannelResult Channel<T>::send(const T &data, int timeoutMs)
  {
    return sendImpl(data, timeoutMs, true);
  }

  /**
   * @brief 发送数据（移动语义，阻塞方式）
   * @tparam T 通道数据类型
   * @param data 要发送的数据（右值引用）
   * @param timeoutMs 超时时间（毫秒）
   * @return 发送结果
   *
   * 使用移动语义发送数据，避免不必要的拷贝
   */
  template <typename T>
  ChannelResult Channel<T>::send(T &&data, int timeoutMs)
  {
    return sendImpl(std::move(data), timeoutMs, true);
  }

  /**
   * @brief 接收数据（阻塞方式）
   * @tparam T 通道数据类型
   * @param data 接收数据的引用
   * @param timeoutMs 超时时间（毫秒）
   * @return 接收结果
   *
   * 阻塞接收通道中的数据，支持超时控制
   */
  template <typename T>
  ChannelResult Channel<T>::receive(T &data, int timeoutMs)
  {
    return receiveImpl(data, timeoutMs, true);
  }

  /**
   * @brief 尝试发送数据（非阻塞）
   * @tparam T 通道数据类型
   * @param data 要发送的数据
   * @return 发送结果
   *
   * 非阻塞发送，如果通道满则立即返回失败
   */
  template <typename T>
  ChannelResult Channel<T>::trySend(const T &data)
  {
    return sendImpl(data, 0, false);
  }

  /**
   * @brief 尝试发送数据（移动语义，非阻塞）
   * @tparam T 通道数据类型
   * @param data 要发送的数据（右值引用）
   * @return 发送结果
   *
   * 使用移动语义的非阻塞发送
   */
  template <typename T>
  ChannelResult Channel<T>::trySend(T &&data)
  {
    return sendImpl(std::move(data), 0, false);
  }

  /**
   * @brief 尝试接收数据（非阻塞）
   * @tparam T 通道数据类型
   * @param data 接收数据的引用
   * @return 接收结果
   *
   * 非阻塞接收，如果通道空则立即返回失败
   */
  template <typename T>
  ChannelResult Channel<T>::tryReceive(T &data)
  {
    return receiveImpl(data, 0, false);
  }

  /**
   * @brief 发送数据的核心实现
   * @tparam T 通道数据类型
   * @tparam U 数据类型（支持完美转发）
   * @param data 要发送的数据
   * @param timeoutMs 超时时间（毫秒）
   * @param blocking 是否阻塞
   * @return 发送结果
   *
   * 实现了发送数据的核心逻辑，包括：
   * 1. 直接传递给等待的接收者
   * 2. 放入缓冲区
   * 3. 阻塞等待
   * 4. 超时处理
   */
  template <typename T>
  template <typename U>
  ChannelResult Channel<T>::sendImpl(U &&data, int timeoutMs, bool blocking)
  {
    // 计算截止时间，用于超时控制
    auto deadline = (timeoutMs > 0)
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs)
                        : std::chrono::steady_clock::time_point::max();

    Mutex::Lock lock(mutex_);

    // 检查Channel是否已关闭，关闭的通道不能发送数据
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 优化路径1：如果有等待接收的协程，直接传递数据（零拷贝优化）
    if (!receiveWaiters_.empty())
    {
      auto waitInfo = receiveWaiters_.front();
      receiveWaiters_.pop_front();

      // 直接将数据传递给等待的协程，避免中间缓冲区
      if (waitInfo.dataPtr)
      {
        *waitInfo.dataPtr = std::forward<U>(data);
        waitInfo.isCompleted = true;
      }

      // 唤醒等待的协程，让其继续执行
      if (scheduler_ && waitInfo.fiber)
      {
        scheduler_->scheduler(waitInfo.fiber);
      }

      return ChannelResult::SUCCESS;
    }

    // 优化路径2：如果缓冲区未满，直接放入缓冲区
    if (buffer_.size() < capacity_)
    {
      buffer_.push(std::forward<U>(data));
      return ChannelResult::SUCCESS;
    }

    // 缓冲区已满，非阻塞模式直接返回
    if (!blocking)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 阻塞模式：需要等待接收者
    auto currentFiber = Fiber::GetThis();
    if (!currentFiber || !scheduler_)
    {
      // 如果不在协程环境中，无法进行阻塞等待
      return ChannelResult::WOULD_BLOCK;
    }

    // 将当前协程加入发送等待队列，保存数据指针以便后续直接传递
    sendWaiters_.emplace_back(currentFiber, deadline, &data);

    // 释放锁并让出协程，等待被唤醒
    lock.unlock();
    currentFiber->yield();

    // 协程被唤醒后重新获取锁
    lock.lock();

    // 检查是否超时或Channel已关闭
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 再次尝试发送（可能在等待期间缓冲区有了空间）
    if (buffer_.size() < capacity_)
    {
      buffer_.push(std::forward<U>(data));
      return ChannelResult::SUCCESS;
    }

    return ChannelResult::TIMEOUT;
  }

  /**
   * @brief 接收数据的核心实现
   * @tparam T 通道数据类型
   * @param data 接收数据的引用
   * @param timeoutMs 超时时间（毫秒）
   * @param blocking 是否阻塞
   * @return 接收结果
   *
   * 实现了接收数据的核心逻辑，包括：
   * 1. 从缓冲区获取数据
   * 2. 从等待的发送者获取数据
   * 3. 阻塞等待
   * 4. 超时处理
   */
  template <typename T>
  ChannelResult Channel<T>::receiveImpl(T &data, int timeoutMs, bool blocking)
  {
    // 计算截止时间，用于超时控制
    auto deadline = (timeoutMs > 0)
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs)
                        : std::chrono::steady_clock::time_point::max();

    Mutex::Lock lock(mutex_);

    // 优化路径1：如果缓冲区有数据，直接取出
    if (!buffer_.empty())
    {
      data = std::move(buffer_.front());
      buffer_.pop();

      // 唤醒等待发送的协程，让它们有机会发送更多数据
      if (!sendWaiters_.empty())
      {
        auto waitInfo = sendWaiters_.front();
        sendWaiters_.pop_front();

        // 如果等待发送的协程有数据，将其放入缓冲区
        if (waitInfo.sendDataPtr && buffer_.size() < capacity_)
        {
          buffer_.push(*waitInfo.sendDataPtr);
          waitInfo.isCompleted = true;
        }

        // 唤醒等待的协程
        if (scheduler_ && waitInfo.fiber)
        {
          scheduler_->scheduler(waitInfo.fiber);
        }
      }

      return ChannelResult::SUCCESS;
    }

    // 优化路径2：如果有等待发送的协程，直接从其获取数据（零拷贝优化）
    if (!sendWaiters_.empty())
    {
      auto waitInfo = sendWaiters_.front();
      sendWaiters_.pop_front();

      if (waitInfo.sendDataPtr)
      {
        data = *waitInfo.sendDataPtr;
        waitInfo.isCompleted = true;

        // 唤醒等待的协程
        if (scheduler_ && waitInfo.fiber)
        {
          scheduler_->scheduler(waitInfo.fiber);
        }

        return ChannelResult::SUCCESS;
      }
    }

    // 如果Channel已关闭且缓冲区为空，无法接收数据
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 缓冲区为空，非阻塞模式直接返回
    if (!blocking)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 阻塞模式：需要等待发送者
    auto currentFiber = Fiber::GetThis();
    if (!currentFiber || !scheduler_)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 将当前协程加入接收等待队列，传递数据指针以便发送者直接写入
    receiveWaiters_.emplace_back(currentFiber, deadline, &data);

    // 释放锁并让出协程，等待被唤醒
    lock.unlock();
    currentFiber->yield();

    // 协程被唤醒后重新获取锁
    lock.lock();

    // 再次尝试接收（可能在等待期间缓冲区有了数据）
    if (!buffer_.empty())
    {
      data = std::move(buffer_.front());
      buffer_.pop();
      return ChannelResult::SUCCESS;
    }

    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    return ChannelResult::TIMEOUT;
  }

  /**
   * @brief 关闭通道
   * @tparam T 通道数据类型
   *
   * 关闭通道并唤醒所有等待的协程
   */
  template <typename T>
  void Channel<T>::close()
  {
    Mutex::Lock lock(mutex_);
    closed_.store(true);

    // 唤醒所有等待的协程，让它们感知到通道已关闭
    wakeupWaiters(sendWaiters_);
    wakeupWaiters(receiveWaiters_);
  }

  /**
   * @brief 检查通道是否已关闭
   * @tparam T 通道数据类型
   * @return 如果通道已关闭返回true
   */
  template <typename T>
  bool Channel<T>::isClosed() const
  {
    return closed_.load();
  }

  /**
   * @brief 获取通道当前大小
   * @tparam T 通道数据类型
   * @return 通道中当前元素数量
   */
  template <typename T>
  size_t Channel<T>::size() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.size();
  }

  /**
   * @brief 检查通道是否为空
   * @tparam T 通道数据类型
   * @return 如果通道为空返回true
   */
  template <typename T>
  bool Channel<T>::empty() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.empty();
  }

  /**
   * @brief 检查通道是否已满
   * @tparam T 通道数据类型
   * @return 如果通道已满返回true
   */
  template <typename T>
  bool Channel<T>::full() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.size() >= capacity_;
  }

  /**
   * @brief 唤醒等待队列中的所有协程
   * @tparam T 通道数据类型
   * @param waitQueue 等待队列
   *
   * 在通道关闭时唤醒所有等待的协程
   */
  template <typename T>
  void Channel<T>::wakeupWaiters(std::deque<FiberWaitInfo<T>> &waitQueue)
  {
    if (!scheduler_)
      return;

    // 遍历所有等待的协程并唤醒它们
    while (!waitQueue.empty())
    {
      auto waitInfo = waitQueue.front();
      waitQueue.pop_front();

      if (waitInfo.fiber)
      {
        scheduler_->scheduler(waitInfo.fiber);
      }
    }
  }

  /**
   * @brief 清理超时的等待者
   * @tparam T 通道数据类型
   * @param waitQueue 等待队列
   *
   * 移除已超时的等待协程，防止内存泄漏
   */
  template <typename T>
  void Channel<T>::cleanupTimeoutWaiters(std::deque<FiberWaitInfo<T>> &waitQueue)
  {
    auto now = std::chrono::steady_clock::now();

    // 使用STL算法移除超时的等待者
    waitQueue.erase(
        std::remove_if(waitQueue.begin(), waitQueue.end(),
                       [now](const FiberWaitInfo<T> &info)
                       {
                         return now > info.deadline;
                       }),
        waitQueue.end());
  }

  /**
   * @brief 获取当前调度器
   * @tparam T 通道数据类型
   * @return 当前调度器指针
   */
  template <typename T>
  Scheduler *Channel<T>::getCurrentScheduler()
  {
    if (scheduler_)
      return scheduler_;
    return IOManager::GetThis();
  }

  /**
   * @brief 获取发送等待者数量
   * @tparam T 通道数据类型
   * @return 当前等待发送的协程数量
   */
  template <typename T>
  size_t Channel<T>::getSendWaitersCount() const
  {
    Mutex::Lock lock(mutex_);
    return sendWaiters_.size();
  }

  /**
   * @brief 获取接收等待者数量
   * @tparam T 通道数据类型
   * @return 当前等待接收的协程数量
   */
  template <typename T>
  size_t Channel<T>::getReceiveWaitersCount() const
  {
    Mutex::Lock lock(mutex_);
    return receiveWaiters_.size();
  }

  // ChannelManager实现 - 单例模式管理所有通道
  ChannelManager::ptr ChannelManager::instance_ = nullptr;
  Mutex ChannelManager::instanceMutex_;

  /**
   * @brief ChannelManager构造函数
   * @param ioManager IO管理器指针
   *
   * 创建通道管理器，用于统一管理通道资源
   */
  ChannelManager::ChannelManager(IOManager *ioManager)
      : ioManager_(ioManager)
  {
    if (!ioManager_)
    {
      ioManager_ = IOManager::GetThis();
    }
  }

  /**
   * @brief ChannelManager析构函数
   */
  ChannelManager::~ChannelManager() {}

  /**
   * @brief 获取ChannelManager单例
   * @return ChannelManager单例指针
   *
   * 使用双重检查锁定模式确保线程安全
   */
  ChannelManager::ptr ChannelManager::getInstance()
  {
    if (!instance_)
    {
      Mutex::Lock lock(instanceMutex_);
      if (!instance_)
      {
        instance_ = std::make_shared<ChannelManager>();
      }
    }
    return instance_;
  }

  // ChannelSelector实现 - 多路选择功能

  /**
   * @brief 添加默认case到选择器
   * @param callback 默认case的回调函数
   * @return case的索引
   *
   * 当所有其他case都无法执行时，执行默认case
   */
  int ChannelSelector::addDefaultCase(std::function<void()> callback)
  {
    SelectCase selectCase;
    selectCase.type = OperationType::DEFAULT;
    selectCase.callback = [callback](bool)
    { if (callback) callback(); };
    selectCase.dataDeleter = nullptr;
    cases_.push_back(selectCase);
    return cases_.size() - 1;
  }

  /**
   * @brief 执行多路选择操作
   * @param timeoutMs 超时时间（毫秒）
   * @return 选择结果
   *
   * 实现类似Go语言select的功能，随机选择一个可执行的case
   */
  ChannelSelector::SelectResult ChannelSelector::select(int timeoutMs)
  {
    SelectResult result;
    result.caseIndex = -1;
    result.success = false;

    // 首先尝试非阻塞操作，避免不必要的等待
    int readyCase = tryExecuteNonBlocking();
    if (readyCase >= 0)
    {
      result.caseIndex = readyCase;
      result.type = cases_[readyCase].type;
      result.success = true;
      if (cases_[readyCase].callback)
      {
        cases_[readyCase].callback(true);
      }
      return result;
    }

    // 如果有默认case且没有其他操作可执行，执行默认case
    for (size_t i = 0; i < cases_.size(); ++i)
    {
      if (cases_[i].type == OperationType::DEFAULT)
      {
        result.caseIndex = i;
        result.type = OperationType::DEFAULT;
        result.success = true;
        if (cases_[i].callback)
        {
          cases_[i].callback(true);
        }
        return result;
      }
    }

    // 如果没有默认case，需要阻塞等待
    // 这里简化实现，使用轮询方式（实际可以使用epoll等更高效的方式）
    auto deadline = (timeoutMs > 0)
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs)
                        : std::chrono::steady_clock::time_point::max();

    while (std::chrono::steady_clock::now() < deadline)
    {
      readyCase = tryExecuteNonBlocking();
      if (readyCase >= 0)
      {
        result.caseIndex = readyCase;
        result.type = cases_[readyCase].type;
        result.success = true;
        if (cases_[readyCase].callback)
        {
          cases_[readyCase].callback(true);
        }
        return result;
      }

      // 短暂休眠避免CPU占用过高，同时保持响应性
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // 超时返回
    result.errorMessage = "Select operation timed out";
    return result;
  }

  /**
   * @brief 尝试执行非阻塞操作
   * @return 可执行的case索引，如果没有可执行的case返回-1
   *
   * 随机化case顺序以避免饥饿问题
   */
  int ChannelSelector::tryExecuteNonBlocking()
  {
    // 收集所有非默认case的索引
    std::vector<size_t> indices;
    for (size_t i = 0; i < cases_.size(); ++i)
    {
      if (cases_[i].type != OperationType::DEFAULT)
      {
        indices.push_back(i);
      }
    }

    // 随机化case顺序，避免某些case总是优先执行导致的饥饿问题
    if (!indices.empty())
    {
      auto now = std::chrono::steady_clock::now();
      size_t seed = now.time_since_epoch().count();
      std::shuffle(indices.begin(), indices.end(), std::default_random_engine(seed));
    }

    // 按随机顺序尝试执行每个case
    for (size_t idx : indices)
    {
      const auto &selectCase = cases_[idx];

      // 使用tryOperation函数指针执行操作
      if (selectCase.tryOperation && selectCase.tryOperation())
      {
        return static_cast<int>(idx);
      }
    }

    return -1; // 没有可执行的操作
  }

  /**
   * @brief 清理选择器
   *
   * 清理所有case并释放相关资源
   */
  void ChannelSelector::clear()
  {
    for (auto &selectCase : cases_)
    {
      cleanupCase(selectCase);
    }
    cases_.clear();
  }

  /**
   * @brief ChannelSelector析构函数
   */
  ChannelSelector::~ChannelSelector()
  {
    clear();
  }

  /**
   * @brief 清理单个case
   * @param selectCase 要清理的case
   *
   * 释放case相关的数据和资源
   */
  void ChannelSelector::cleanupCase(SelectCase &selectCase)
  {
    if (selectCase.dataDeleter && selectCase.data)
    {
      selectCase.dataDeleter(selectCase.data);
      selectCase.data = nullptr;
    }
  }

}
