#pragma once

#include "channel.h"
#include "iomanager.h"
#include "raft-kv/common/util.h"
#include <memory>
#include <functional>
#include <thread>
#include <chrono>

namespace monsoon
{
  /**
   * @brief LockQueue到Channel的迁移助手类
   * @details 提供平滑迁移的工具和兼容层
   */
  class MigrationHelper
  {
  public:
    /**
     * @brief LockQueue包装器，提供Channel接口
     * @tparam T 数据类型
     */
    template <typename T>
    class LockQueueWrapper
    {
    public:
      explicit LockQueueWrapper(std::shared_ptr<LockQueue<T>> lockQueue)
          : lockQueue_(lockQueue) {}

      /**
       * @brief 发送数据（兼容Channel接口）
       */
      ChannelResult send(const T &data, int timeoutMs = -1)
      {
        (void)timeoutMs; // LockQueue不支持发送超时
        lockQueue_->Push(data);
        return ChannelResult::SUCCESS;
      }

      /**
       * @brief 接收数据（兼容Channel接口）
       */
      ChannelResult receive(T &data, int timeoutMs = -1)
      {
        if (timeoutMs <= 0)
        {
          data = lockQueue_->Pop();
          return ChannelResult::SUCCESS;
        }
        else
        {
          bool success = lockQueue_->timeOutPop(timeoutMs, &data);
          return success ? ChannelResult::SUCCESS : ChannelResult::TIMEOUT;
        }
      }

      /**
       * @brief 非阻塞发送
       */
      ChannelResult trySend(const T &data)
      {
        return send(data);
      }

      /**
       * @brief 非阻塞接收
       */
      ChannelResult tryReceive(T &data)
      {
        // LockQueue没有非阻塞接收，使用很短的超时
        return receive(data, 1);
      }

      /**
       * @brief 关闭（LockQueue不支持）
       */
      void close() {}

      /**
       * @brief 检查是否关闭
       */
      bool isClosed() const { return false; }

    private:
      std::shared_ptr<LockQueue<T>> lockQueue_;
    };

    /**
     * @brief 创建LockQueue包装器
     * @tparam T 数据类型
     * @return LockQueue包装器
     */
    template <typename T>
    static std::shared_ptr<LockQueueWrapper<T>> wrapLockQueue(std::shared_ptr<LockQueue<T>> lockQueue)
    {
      return std::make_shared<LockQueueWrapper<T>>(lockQueue);
    }

    /**
     * @brief 从LockQueue迁移到Channel
     * @tparam T 数据类型
     * @param lockQueue 原LockQueue
     * @param capacity Channel缓冲区大小
     * @return 新的Channel
     */
    template <typename T>
    static typename Channel<T>::ptr migrateFromLockQueue(
        std::shared_ptr<LockQueue<T>> lockQueue,
        size_t capacity = 10)
    {
      auto channel = createChannel<T>(capacity);

      // 如果LockQueue中有数据，迁移到Channel
      // 注意：这是一个简化实现，实际使用时需要考虑线程安全
      try
      {
        while (true)
        {
          T data;
          bool hasData = lockQueue->timeOutPop(1, &data); // 1ms超时
          if (!hasData)
            break;

          auto result = channel->trySend(data);
          if (result != ChannelResult::SUCCESS)
          {
            // 如果Channel满了，停止迁移
            break;
          }
        }
      }
      catch (...)
      {
        // 忽略异常，继续返回Channel
      }

      return channel;
    }

    /**
     * @brief 性能基准测试
     * @tparam T 数据类型
     * @param messageCount 消息数量
     * @param capacity Channel缓冲区大小
     * @return 性能测试结果
     */
    template <typename T>
    struct BenchmarkResult
    {
      long lockQueueTimeUs; // LockQueue耗时（微秒）
      long channelTimeUs;   // Channel耗时（微秒）
      double improvement;   // 性能改进百分比（正数表示Channel更快）
    };

    template <typename T>
    static BenchmarkResult<T> benchmark(int messageCount, size_t capacity = 10)
    {
      BenchmarkResult<T> result;

      // 测试LockQueue
      auto start = std::chrono::high_resolution_clock::now();
      {
        auto lockQueue = std::make_shared<LockQueue<T>>();

        std::thread producer([lockQueue, messageCount]()
                             {
          for (int i = 0; i < messageCount; ++i) {
            T data{};
            lockQueue->Push(data);
          } });

        std::thread consumer([lockQueue, messageCount]()
                             {
          for (int i = 0; i < messageCount; ++i) {
            T data = lockQueue->Pop();
            (void)data;
          } });

        producer.join();
        consumer.join();
      }
      result.lockQueueTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::high_resolution_clock::now() - start)
                                   .count();

      // 测试Channel
      start = std::chrono::high_resolution_clock::now();
      {
        auto ioManager = std::make_unique<IOManager>(2, false, "Benchmark");
        auto channel = createChannel<T>(capacity);

        ioManager->scheduler([channel, messageCount]()
                             {
          for (int i = 0; i < messageCount; ++i) {
            T data{};
            channel->send(data);
          } });

        ioManager->scheduler([channel, messageCount]()
                             {
          for (int i = 0; i < messageCount; ++i) {
            T data;
            channel->receive(data);
          } });

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ioManager->stop();
      }
      result.channelTimeUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                 std::chrono::high_resolution_clock::now() - start)
                                 .count();

      // 计算性能改进
      result.improvement = ((double)(result.lockQueueTimeUs - result.channelTimeUs) / result.lockQueueTimeUs) * 100.0;

      return result;
    }
  };

  /**
   * @brief 迁移配置类
   */
  class MigrationConfig
  {
  public:
    bool enableChannel = true;          // 是否启用Channel
    bool enableLockQueue = false;       // 是否启用LockQueue（向后兼容）
    bool enablePromiseFuture = false;   // 是否启用Promise/Future
    size_t defaultChannelCapacity = 10; // 默认Channel缓冲区大小
    int defaultTimeoutMs = 5000;        // 默认超时时间（毫秒）

    /**
     * @brief 从环境变量加载配置
     */
    void loadFromEnvironment()
    {
      const char *env = std::getenv("RAFT_KV_USE_CHANNEL");
      if (env && std::string(env) == "false")
      {
        enableChannel = false;
        enableLockQueue = true;
      }

      env = std::getenv("RAFT_KV_CHANNEL_CAPACITY");
      if (env)
      {
        defaultChannelCapacity = std::stoul(env);
      }

      env = std::getenv("RAFT_KV_TIMEOUT_MS");
      if (env)
      {
        defaultTimeoutMs = std::stoi(env);
      }
    }

    /**
     * @brief 打印配置信息
     */
    void printConfig() const
    {
      std::cout << "=== 迁移配置 ===" << std::endl;
      std::cout << "启用Channel: " << (enableChannel ? "是" : "否") << std::endl;
      std::cout << "启用LockQueue: " << (enableLockQueue ? "是" : "否") << std::endl;
      std::cout << "启用Promise/Future: " << (enablePromiseFuture ? "是" : "否") << std::endl;
      std::cout << "默认Channel容量: " << defaultChannelCapacity << std::endl;
      std::cout << "默认超时时间: " << defaultTimeoutMs << "ms" << std::endl;
    }
  };

  /**
   * @brief 全局迁移配置实例
   */
  extern MigrationConfig g_migrationConfig;

} // namespace monsoon
