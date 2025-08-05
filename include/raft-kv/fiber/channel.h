#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <type_traits>

#include "fiber.h"
#include "mutex.h"
#include "noncopyable.h"

namespace monsoon
{
  // 前向声明
  class IOManager;
  class Scheduler;

  /**
   * @brief Channel操作结果枚举
   */
  enum class ChannelResult
  {
    SUCCESS,    // 操作成功
    TIMEOUT,    // 操作超时
    CLOSED,     // Channel已关闭
    WOULD_BLOCK // 非阻塞操作会阻塞
  };

  /**
   * @brief 协程等待信息结构
   */
  template <typename T>
  struct FiberWaitInfo
  {
    Fiber::ptr fiber;                               // 等待的协程
    std::chrono::steady_clock::time_point deadline; // 超时时间点
    T *dataPtr = nullptr;                           // 用于接收数据的指针（仅接收等待使用）
    const T *sendDataPtr = nullptr;                 // 用于发送数据的指针（仅发送等待使用）
    bool isTimeout = false;                         // 是否超时
    bool isCompleted = false;                       // 操作是否完成

    FiberWaitInfo(Fiber::ptr f, std::chrono::steady_clock::time_point d)
        : fiber(f), deadline(d) {}

    FiberWaitInfo(Fiber::ptr f, std::chrono::steady_clock::time_point d, T *data)
        : fiber(f), deadline(d), dataPtr(data) {}

    FiberWaitInfo(Fiber::ptr f, std::chrono::steady_clock::time_point d, const T *sendData)
        : fiber(f), deadline(d), sendDataPtr(sendData) {}
  };

  /**
   * @brief 协程安全的Channel模板类
   * @tparam T 传输的数据类型
   * @details 实现类似Go语言的Channel，支持协程间的安全通信
   *          支持有缓冲和无缓冲模式，自动协程调度
   */
  template <typename T>
  class Channel : public std::enable_shared_from_this<Channel<T>>, Nonecopyable
  {
  public:
    typedef std::shared_ptr<Channel<T>> ptr;

    /**
     * @brief 构造函数
     * @param capacity 缓冲区容量，0表示无缓冲Channel
     * @param scheduler 关联的调度器，nullptr表示使用当前IOManager
     */
    explicit Channel(size_t capacity = 0, Scheduler *scheduler = nullptr);

    /**
     * @brief 析构函数
     */
    ~Channel();

    /**
     * @brief 发送数据（阻塞模式）
     * @param data 要发送的数据
     * @param timeoutMs 超时时间（毫秒），-1表示永不超时
     * @return 操作结果
     */
    ChannelResult send(const T &data, int timeoutMs = -1);

    /**
     * @brief 发送数据（移动语义）
     * @param data 要发送的数据
     * @param timeoutMs 超时时间（毫秒），-1表示永不超时
     * @return 操作结果
     */
    ChannelResult send(T &&data, int timeoutMs = -1);

    /**
     * @brief 接收数据（阻塞模式）
     * @param data 用于存储接收数据的引用
     * @param timeoutMs 超时时间（毫秒），-1表示永不超时
     * @return 操作结果
     */
    ChannelResult receive(T &data, int timeoutMs = -1);

    /**
     * @brief 尝试发送数据（非阻塞模式）
     * @param data 要发送的数据
     * @return 操作结果
     */
    ChannelResult trySend(const T &data);

    /**
     * @brief 尝试发送数据（非阻塞模式，移动语义）
     * @param data 要发送的数据
     * @return 操作结果
     */
    ChannelResult trySend(T &&data);

    /**
     * @brief 尝试接收数据（非阻塞模式）
     * @param data 用于存储接收数据的引用
     * @return 操作结果
     */
    ChannelResult tryReceive(T &data);

    /**
     * @brief 关闭Channel
     * @details 关闭后不能再发送数据，但可以继续接收已缓冲的数据
     */
    void close();

    /**
     * @brief 检查Channel是否已关闭
     * @return true表示已关闭
     */
    bool isClosed() const;

    /**
     * @brief 获取当前缓冲区大小
     * @return 缓冲区中的元素数量
     */
    size_t size() const;

    /**
     * @brief 获取缓冲区容量
     * @return 缓冲区容量
     */
    size_t capacity() const { return capacity_; }

    /**
     * @brief 检查缓冲区是否为空
     * @return true表示为空
     */
    bool empty() const;

    /**
     * @brief 检查缓冲区是否已满
     * @return true表示已满
     */
    bool full() const;

    /**
     * @brief 设置关联的调度器
     * @param scheduler 调度器指针
     */
    void setScheduler(Scheduler *scheduler) { scheduler_ = scheduler; }

    /**
     * @brief 获取等待发送的协程数量
     * @return 等待发送的协程数量
     */
    size_t getSendWaitersCount() const;

    /**
     * @brief 获取等待接收的协程数量
     * @return 等待接收的协程数量
     */
    size_t getReceiveWaitersCount() const;

  private:
    /**
     * @brief 内部发送实现
     * @param data 数据
     * @param timeoutMs 超时时间
     * @param blocking 是否阻塞
     * @return 操作结果
     */
    template <typename U>
    ChannelResult sendImpl(U &&data, int timeoutMs, bool blocking);

    /**
     * @brief 内部接收实现
     * @param data 数据引用
     * @param timeoutMs 超时时间
     * @param blocking 是否阻塞
     * @return 操作结果
     */
    ChannelResult receiveImpl(T &data, int timeoutMs, bool blocking);

    /**
     * @brief 唤醒等待的协程
     * @param waitQueue 等待队列
     */
    void wakeupWaiters(std::deque<FiberWaitInfo<T>> &waitQueue);

    /**
     * @brief 清理超时的等待协程
     * @param waitQueue 等待队列
     */
    void cleanupTimeoutWaiters(std::deque<FiberWaitInfo<T>> &waitQueue);

    /**
     * @brief 获取当前调度器
     * @return 调度器指针
     */
    Scheduler *getCurrentScheduler();

  private:
    const size_t capacity_; // 缓冲区容量
    std::queue<T> buffer_;  // 数据缓冲区
    mutable Mutex mutex_;   // 保护内部状态的互斥锁

    std::deque<FiberWaitInfo<T>> sendWaiters_;    // 等待发送的协程队列
    std::deque<FiberWaitInfo<T>> receiveWaiters_; // 等待接收的协程队列

    std::atomic<bool> closed_{false}; // Channel是否已关闭
    Scheduler *scheduler_;            // 关联的调度器
  };

  /**
   * @brief Channel管理器类
   * @details 管理Channel的生命周期，提供与IOManager的深度集成
   */
  class ChannelManager : Nonecopyable
  {
  public:
    typedef std::shared_ptr<ChannelManager> ptr;

    /**
     * @brief 构造函数
     * @param ioManager 关联的IOManager
     */
    explicit ChannelManager(IOManager *ioManager = nullptr);

    /**
     * @brief 析构函数
     */
    ~ChannelManager();

    /**
     * @brief 创建Channel
     * @tparam T 数据类型
     * @param capacity 缓冲区容量
     * @return Channel智能指针
     */
    template <typename T>
    typename Channel<T>::ptr createChannel(size_t capacity = 0)
    {
      auto channel = std::make_shared<Channel<T>>(capacity, ioManager_);
      return channel;
    }

    /**
     * @brief 设置关联的IOManager
     * @param ioManager IOManager指针
     */
    void setIOManager(IOManager *ioManager) { ioManager_ = ioManager; }

    /**
     * @brief 获取关联的IOManager
     * @return IOManager指针
     */
    IOManager *getIOManager() const { return ioManager_; }

    /**
     * @brief 获取单例实例
     * @return ChannelManager单例
     */
    static ChannelManager::ptr getInstance();

    /**
     * @brief 创建一对连接的Channel（类似管道）
     * @tparam T 数据类型
     * @param capacity 缓冲区容量
     * @return 一对Channel，first用于发送，second用于接收
     */
    template <typename T>
    std::pair<typename Channel<T>::ptr, typename Channel<T>::ptr> createChannelPair(size_t capacity = 0)
    {
      auto ch1 = createChannel<T>(capacity);
      auto ch2 = createChannel<T>(capacity);
      return std::make_pair(ch1, ch2);
    }

    /**
     * @brief 创建广播Channel
     * @tparam T 数据类型
     * @param capacity 缓冲区容量
     * @param subscriberCount 订阅者数量
     * @return 广播Channel和订阅者Channel列表
     */
    template <typename T>
    std::pair<typename Channel<T>::ptr, std::vector<typename Channel<T>::ptr>>
    createBroadcastChannel(size_t capacity = 0, size_t subscriberCount = 1)
    {
      auto broadcaster = createChannel<T>(capacity);
      std::vector<typename Channel<T>::ptr> subscribers;

      for (size_t i = 0; i < subscriberCount; ++i)
      {
        subscribers.push_back(createChannel<T>(capacity));
      }

      return std::make_pair(broadcaster, subscribers);
    }

  private:
    IOManager *ioManager_;                // 关联的IOManager
    static ChannelManager::ptr instance_; // 单例实例
    static Mutex instanceMutex_;          // 保护单例的互斥锁
  };

  /**
   * @brief Channel工厂函数
   * @tparam T 数据类型
   * @param capacity 缓冲区容量
   * @param scheduler 调度器
   * @return Channel智能指针
   */
  template <typename T>
  typename Channel<T>::ptr makeChannel(size_t capacity = 0, Scheduler *scheduler = nullptr)
  {
    return std::make_shared<Channel<T>>(capacity, scheduler);
  }

  /**
   * @brief 便捷的Channel创建函数（使用ChannelManager）
   * @tparam T 数据类型
   * @param capacity 缓冲区容量
   * @return Channel智能指针
   */
  template <typename T>
  typename Channel<T>::ptr createChannel(size_t capacity = 0)
  {
    auto manager = ChannelManager::getInstance();
    return manager->createChannel<T>(capacity);
  }

  /**
   * @brief Channel选择器，类似Go的select语句
   * @details 允许在多个Channel操作中选择一个可执行的操作
   */
  class ChannelSelector : Nonecopyable
  {
  public:
    /**
     * @brief 选择操作类型
     */
    enum class OperationType
    {
      SEND,    // 发送操作
      RECEIVE, // 接收操作
      DEFAULT  // 默认操作（当所有Channel操作都会阻塞时执行）
    };

    /**
     * @brief 选择操作结果
     */
    struct SelectResult
    {
      int caseIndex = -1;       // 执行的case索引
      OperationType type;       // 操作类型
      bool success = false;     // 操作是否成功
      std::string errorMessage; // 错误信息
    };

    /**
     * @brief 构造函数
     */
    ChannelSelector() = default;

    /**
     * @brief 添加发送case
     * @tparam T 数据类型
     * @param channel Channel指针
     * @param data 要发送的数据
     * @param callback 操作完成后的回调函数
     * @return case索引
     */
    template <typename T>
    int addSendCase(typename Channel<T>::ptr channel, const T &data,
                    std::function<void(bool)> callback = nullptr)
    {
      SelectCase selectCase;
      selectCase.type = OperationType::SEND;
      selectCase.channel = channel.get();
      selectCase.data = new T(data); // 复制数据
      selectCase.callback = [callback](bool success)
      {
        if (callback)
          callback(success);
      };
      selectCase.dataDeleter = [](void *ptr)
      {
        delete static_cast<T *>(ptr);
      };
      selectCase.tryOperation = [channel, data]() -> bool
      {
        auto result = channel->trySend(data);
        return result == ChannelResult::SUCCESS;
      };

      cases_.push_back(selectCase);
      return cases_.size() - 1;
    }

    /**
     * @brief 添加接收case
     * @tparam T 数据类型
     * @param channel Channel指针
     * @param dataPtr 用于存储接收数据的指针
     * @param callback 接收到数据后的回调函数
     * @return case索引
     */
    template <typename T>
    int addReceiveCase(typename Channel<T>::ptr channel, T *dataPtr,
                       std::function<void(const T &, bool)> callback = nullptr)
    {
      SelectCase selectCase;
      selectCase.type = OperationType::RECEIVE;
      selectCase.channel = channel.get();
      selectCase.data = dataPtr; // 存储接收数据的指针
      selectCase.callback = [callback, dataPtr](bool success)
      {
        if (callback && success)
          callback(*dataPtr, success);
      };
      selectCase.dataDeleter = nullptr; // 接收case不需要删除数据
      selectCase.tryOperation = [channel, dataPtr]() -> bool
      {
        auto result = channel->tryReceive(*dataPtr);
        return result == ChannelResult::SUCCESS;
      };

      cases_.push_back(selectCase);
      return cases_.size() - 1;
    }

    /**
     * @brief 添加默认case
     * @param callback 默认操作的回调函数
     * @return case索引
     */
    int addDefaultCase(std::function<void()> callback = nullptr);

    /**
     * @brief 执行选择操作
     * @param timeoutMs 超时时间（毫秒）
     * @return 选择结果
     */
    SelectResult select(int timeoutMs = -1);

    /**
     * @brief 清空所有case
     */
    void clear();

    /**
     * @brief 析构函数，清理资源
     */
    ~ChannelSelector();

  private:
    struct SelectCase
    {
      OperationType type;
      void *channel = nullptr;                 // Channel指针
      void *data = nullptr;                    // 数据指针
      std::function<void(bool)> callback;      // 回调函数
      std::function<void(void *)> dataDeleter; // 数据删除器
      std::function<bool()> tryOperation;      // 尝试执行操作的函数
    };

    std::vector<SelectCase> cases_; // 所有的case

    /**
     * @brief 尝试执行非阻塞操作
     * @return 成功执行的case索引，-1表示没有可执行的case
     */
    int tryExecuteNonBlocking();

    /**
     * @brief 清理case资源
     * @param selectCase 要清理的case
     */
    void cleanupCase(SelectCase &selectCase);
  };

}
