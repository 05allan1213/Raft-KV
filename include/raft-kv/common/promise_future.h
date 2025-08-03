#pragma once

#include <future>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <condition_variable>
#include <vector>
#include <queue>

// 前向声明
template <typename T>
class LockQueue;

/**
 * @brief Promise/Future 机制，用于替代 LockQueue 的高性能等待机制
 *
 * 这个类提供了一个高效的等待机制，避免了频繁的内存分配和释放。
 * 相比于为每个请求创建 LockQueue，这种方式使用 promise/future 对，
 * 减少了内存碎片和锁竞争。
 */
template <typename T>
class PromiseFutureManager
{
public:
    /**
     * @brief 等待结果的句柄
     */
    struct WaitHandle
    {
        std::shared_ptr<std::promise<T>> promise;
        std::shared_future<T> future;

        WaitHandle() : promise(std::make_shared<std::promise<T>>())
        {
            future = promise->get_future().share();
        }
    };

    /**
     * @brief 创建一个等待句柄
     * @param index 等待的索引（通常是 raftIndex）
     * @return 等待句柄
     */
    std::shared_ptr<WaitHandle> createWaitHandle(int index)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto handle = std::make_shared<WaitHandle>();
        waitHandles_[index] = handle;

        return handle;
    }

    /**
     * @brief 等待结果（带超时）
     * @param handle 等待句柄
     * @param timeoutMs 超时时间（毫秒）
     * @param result 结果输出参数
     * @return 是否成功获取结果
     */
    bool waitForResult(std::shared_ptr<WaitHandle> handle, int timeoutMs, T *result)
    {
        if (!handle || !result)
        {
            return false;
        }

        auto status = handle->future.wait_for(std::chrono::milliseconds(timeoutMs));

        if (status == std::future_status::ready)
        {
            try
            {
                *result = handle->future.get();
                return true;
            }
            catch (const std::exception &e)
            {
                // Promise 被设置为异常状态
                return false;
            }
        }

        return false; // 超时
    }

    /**
     * @brief 设置结果并唤醒等待者
     * @param index 索引
     * @param result 结果
     * @return 是否成功设置
     */
    bool setResult(int index, const T &result)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = waitHandles_.find(index);
        if (it == waitHandles_.end())
        {
            return false; // 没有等待者
        }

        auto handle = it->second.lock(); // 尝试获取 weak_ptr
        if (handle)
        {
            try
            {
                handle->promise->set_value(result);
            }
            catch (const std::exception &e)
            {
                // Promise 可能已经被设置过了
                return false;
            }
        }

        waitHandles_.erase(it);
        return true;
    }

    /**
     * @brief 移除等待句柄（用于清理）
     * @param index 索引
     */
    void removeWaitHandle(int index)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        waitHandles_.erase(index);
    }

    /**
     * @brief 获取当前等待者数量（用于监控）
     * @return 等待者数量
     */
    size_t getWaitingCount() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return waitHandles_.size();
    }

private:
    mutable std::mutex mutex_;
    // 使用 weak_ptr 避免循环引用，允许句柄在超时后自动清理
    std::unordered_map<int, std::weak_ptr<WaitHandle>> waitHandles_;
};

/**
 * @brief 对象池模板类，用于复用对象减少内存分配
 */
template <typename T>
class ObjectPool
{
public:
    /**
     * @brief 获取一个对象
     * @return 对象的智能指针
     */
    std::shared_ptr<T> acquire()
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!pool_.empty())
        {
            auto obj = pool_.back();
            pool_.pop_back();
            return obj;
        }

        // 池中没有对象，创建新的
        return std::make_shared<T>();
    }

    /**
     * @brief 归还对象到池中
     * @param obj 要归还的对象
     */
    void release(std::shared_ptr<T> obj)
    {
        if (!obj)
            return;

        std::lock_guard<std::mutex> lock(mutex_);

        // 限制池的大小，避免内存无限增长
        if (pool_.size() < maxPoolSize_)
        {
            // 重置对象状态（如果需要的话）
            resetObject(obj);
            pool_.push_back(obj);
        }
        // 如果池已满，对象会自动销毁
    }

    /**
     * @brief 设置池的最大大小
     * @param maxSize 最大大小
     */
    void setMaxPoolSize(size_t maxSize)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        maxPoolSize_ = maxSize;
    }

    /**
     * @brief 获取池中对象数量
     * @return 对象数量
     */
    size_t getPoolSize() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return pool_.size();
    }

private:
    /**
     * @brief 重置对象状态（子类可以重写）
     * @param obj 要重置的对象
     */
    virtual void resetObject(std::shared_ptr<T> obj)
    {
        // 默认不做任何操作
        // 子类可以重写这个方法来重置对象状态
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::shared_ptr<T>> pool_;
    size_t maxPoolSize_ = 100; // 默认最大池大小
};

/**
 * @brief LockQueue 的对象池特化版本
 */
template <typename T>
class LockQueuePool : public ObjectPool<LockQueue<T>>
{
private:
    void resetObject(std::shared_ptr<LockQueue<T>> obj) override
    {
        // 清空队列中的所有元素
        T dummy;
        while (obj->timeOutPop(0, &dummy))
        {
            // 继续清空
        }
    }
};
