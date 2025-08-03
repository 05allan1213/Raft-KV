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

  template <typename T>
  Channel<T>::~Channel()
  {
    close();
  }

  template <typename T>
  ChannelResult Channel<T>::send(const T &data, int timeoutMs)
  {
    return sendImpl(data, timeoutMs, true);
  }

  template <typename T>
  ChannelResult Channel<T>::send(T &&data, int timeoutMs)
  {
    return sendImpl(std::move(data), timeoutMs, true);
  }

  template <typename T>
  ChannelResult Channel<T>::receive(T &data, int timeoutMs)
  {
    return receiveImpl(data, timeoutMs, true);
  }

  template <typename T>
  ChannelResult Channel<T>::trySend(const T &data)
  {
    return sendImpl(data, 0, false);
  }

  template <typename T>
  ChannelResult Channel<T>::trySend(T &&data)
  {
    return sendImpl(std::move(data), 0, false);
  }

  template <typename T>
  ChannelResult Channel<T>::tryReceive(T &data)
  {
    return receiveImpl(data, 0, false);
  }

  template <typename T>
  template <typename U>
  ChannelResult Channel<T>::sendImpl(U &&data, int timeoutMs, bool blocking)
  {
    auto deadline = (timeoutMs > 0)
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs)
                        : std::chrono::steady_clock::time_point::max();

    Mutex::Lock lock(mutex_);

    // 检查Channel是否已关闭
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 如果有等待接收的协程，直接传递数据
    if (!receiveWaiters_.empty())
    {
      auto waitInfo = receiveWaiters_.front();
      receiveWaiters_.pop_front();

      // 直接将数据传递给等待的协程
      if (waitInfo.dataPtr)
      {
        *waitInfo.dataPtr = std::forward<U>(data);
        waitInfo.isCompleted = true;
      }

      // 唤醒等待的协程
      if (scheduler_ && waitInfo.fiber)
      {
        scheduler_->scheduler(waitInfo.fiber);
      }

      return ChannelResult::SUCCESS;
    }

    // 如果缓冲区未满，直接放入缓冲区
    if (buffer_.size() < capacity_)
    {
      buffer_.push(std::forward<U>(data));
      return ChannelResult::SUCCESS;
    }

    // 缓冲区已满
    if (!blocking)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 阻塞等待
    auto currentFiber = Fiber::GetThis();
    if (!currentFiber || !scheduler_)
    {
      // 如果不在协程环境中，返回错误
      return ChannelResult::WOULD_BLOCK;
    }

    // 将当前协程加入等待队列，保存数据指针
    sendWaiters_.emplace_back(currentFiber, deadline, &data);

    // 释放锁并让出协程
    lock.unlock();
    currentFiber->yield();

    // 协程被唤醒后重新获取锁
    lock.lock();

    // 检查是否超时或Channel已关闭
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 再次尝试发送
    if (buffer_.size() < capacity_)
    {
      buffer_.push(std::forward<U>(data));
      return ChannelResult::SUCCESS;
    }

    return ChannelResult::TIMEOUT;
  }

  template <typename T>
  ChannelResult Channel<T>::receiveImpl(T &data, int timeoutMs, bool blocking)
  {
    auto deadline = (timeoutMs > 0)
                        ? std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs)
                        : std::chrono::steady_clock::time_point::max();

    Mutex::Lock lock(mutex_);

    // 如果缓冲区有数据，直接取出
    if (!buffer_.empty())
    {
      data = std::move(buffer_.front());
      buffer_.pop();

      // 唤醒等待发送的协程
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

    // 如果有等待发送的协程，直接从其获取数据
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

    // 如果Channel已关闭且缓冲区为空
    if (closed_.load())
    {
      return ChannelResult::CLOSED;
    }

    // 缓冲区为空
    if (!blocking)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 阻塞等待
    auto currentFiber = Fiber::GetThis();
    if (!currentFiber || !scheduler_)
    {
      return ChannelResult::WOULD_BLOCK;
    }

    // 将当前协程加入等待队列，传递数据指针
    receiveWaiters_.emplace_back(currentFiber, deadline, &data);

    // 释放锁并让出协程
    lock.unlock();
    currentFiber->yield();

    // 协程被唤醒后重新获取锁
    lock.lock();

    // 检查操作是否已完成（数据已被直接传递）
    // 这里需要一个机制来检查数据是否已被设置

    // 再次尝试接收
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

  template <typename T>
  void Channel<T>::close()
  {
    Mutex::Lock lock(mutex_);
    closed_.store(true);

    // 唤醒所有等待的协程
    wakeupWaiters(sendWaiters_);
    wakeupWaiters(receiveWaiters_);
  }

  template <typename T>
  bool Channel<T>::isClosed() const
  {
    return closed_.load();
  }

  template <typename T>
  size_t Channel<T>::size() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.size();
  }

  template <typename T>
  bool Channel<T>::empty() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.empty();
  }

  template <typename T>
  bool Channel<T>::full() const
  {
    Mutex::Lock lock(mutex_);
    return buffer_.size() >= capacity_;
  }

  template <typename T>
  void Channel<T>::wakeupWaiters(std::deque<FiberWaitInfo<T>> &waitQueue)
  {
    if (!scheduler_)
      return;

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

  template <typename T>
  void Channel<T>::cleanupTimeoutWaiters(std::deque<FiberWaitInfo<T>> &waitQueue)
  {
    auto now = std::chrono::steady_clock::now();

    waitQueue.erase(
        std::remove_if(waitQueue.begin(), waitQueue.end(),
                       [now](const FiberWaitInfo<T> &info)
                       {
                         return now > info.deadline;
                       }),
        waitQueue.end());
  }

  template <typename T>
  Scheduler *Channel<T>::getCurrentScheduler()
  {
    if (scheduler_)
      return scheduler_;
    return IOManager::GetThis();
  }

  template <typename T>
  size_t Channel<T>::getSendWaitersCount() const
  {
    Mutex::Lock lock(mutex_);
    return sendWaiters_.size();
  }

  template <typename T>
  size_t Channel<T>::getReceiveWaitersCount() const
  {
    Mutex::Lock lock(mutex_);
    return receiveWaiters_.size();
  }

  // ChannelManager实现
  ChannelManager::ptr ChannelManager::instance_ = nullptr;
  Mutex ChannelManager::instanceMutex_;

  ChannelManager::ChannelManager(IOManager *ioManager)
      : ioManager_(ioManager)
  {
    if (!ioManager_)
    {
      ioManager_ = IOManager::GetThis();
    }
  }

  ChannelManager::~ChannelManager() {}

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

  // ChannelSelector实现
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

  ChannelSelector::SelectResult ChannelSelector::select(int timeoutMs)
  {
    SelectResult result;
    result.caseIndex = -1;
    result.success = false;

    // 首先尝试非阻塞操作
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
    // 这里简化实现，使用轮询方式
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

      // 短暂休眠避免CPU占用过高
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    // 超时
    result.errorMessage = "Select operation timed out";
    return result;
  }

  int ChannelSelector::tryExecuteNonBlocking()
  {
    // 随机化case顺序，避免饥饿
    std::vector<size_t> indices;
    for (size_t i = 0; i < cases_.size(); ++i)
    {
      if (cases_[i].type != OperationType::DEFAULT)
      {
        indices.push_back(i);
      }
    }

    // 简单的随机化
    if (!indices.empty())
    {
      auto now = std::chrono::steady_clock::now();
      size_t seed = now.time_since_epoch().count();
      std::shuffle(indices.begin(), indices.end(), std::default_random_engine(seed));
    }

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

  void ChannelSelector::clear()
  {
    for (auto &selectCase : cases_)
    {
      cleanupCase(selectCase);
    }
    cases_.clear();
  }

  ChannelSelector::~ChannelSelector()
  {
    clear();
  }

  void ChannelSelector::cleanupCase(SelectCase &selectCase)
  {
    if (selectCase.dataDeleter && selectCase.data)
    {
      selectCase.dataDeleter(selectCase.data);
      selectCase.data = nullptr;
    }
  }

} // namespace monsoon
