#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <thread>
#include "config.h"

/**
 * @brief 延迟执行类，用于RAII资源管理
 * @tparam F 函数类型
 *
 * 该类在构造时保存一个函数，在析构时自动执行该函数。
 * 常用于确保资源在作用域结束时被正确释放。
 */
template <class F>
class DeferClass
{
public:
  /**
   * @brief 构造函数，接受右值引用函数
   * @param f 要延迟执行的函数
   */
  DeferClass(F &&f) : m_func(std::forward<F>(f)) {}

  /**
   * @brief 构造函数，接受左值引用函数
   * @param f 要延迟执行的函数
   */
  DeferClass(const F &f) : m_func(f) {}

  /**
   * @brief 析构函数，自动执行保存的函数
   */
  ~DeferClass() { m_func(); }

  DeferClass(const DeferClass &e) = delete;            // 禁用拷贝构造
  DeferClass &operator=(const DeferClass &e) = delete; // 禁用赋值操作

private:
  F m_func; // 存储要延迟执行的函数
};

#define _CONCAT(a, b) a##b
#define _MAKE_DEFER_(line) DeferClass _CONCAT(defer_placeholder, line) = [&]()

#undef DEFER
#define DEFER _MAKE_DEFER_(__LINE__)

/**
 * @brief 调试打印函数
 * @param format 格式化字符串
 * @param ... 可变参数
 *
 * 当Debug模式开启时，将格式化输出打印到控制台
 */
void DPrintf(const char *format, ...);

/**
 * @brief 断言函数
 * @param condition 断言条件
 * @param message 断言失败时的错误信息
 *
 * 如果条件为false，则输出错误信息并退出程序
 */
void myAssert(bool condition, std::string message = "Assertion failed!");

/**
 * @brief 字符串格式化函数
 * @tparam Args 可变参数类型
 * @param format_str 格式化字符串
 * @param args 可变参数
 * @return 格式化后的字符串
 *
 * 使用snprintf进行安全的字符串格式化
 */
template <typename... Args>
std::string format(const char *format_str, Args... args)
{
  // 当没有参数时，直接返回格式字符串
  if constexpr (sizeof...(args) == 0)
  {
    return std::string(format_str);
  }
  else
  {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1; // "\0"
    if (size_s <= 0)
    {
      throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1); // remove '\0'
  }
}

/**
 * @brief 获取当前时间点
 * @return 当前系统时间点
 */
std::chrono::_V2::system_clock::time_point now();

/**
 * @brief 获取随机化的选举超时时间
 * @return 随机化的选举超时时间（毫秒）
 *
 * 返回一个在minRandomizedElectionTime到maxRandomizedElectionTime之间的随机值
 */
std::chrono::milliseconds getRandomizedElectionTimeout();

/**
 * @brief 睡眠指定毫秒数
 * @param N 要睡眠的毫秒数
 */
void sleepNMilliseconds(int N);

/**
 * @brief 线程安全的锁队列模板类
 * @tparam T 队列元素类型
 *
 * 实现了生产者-消费者模式的线程安全队列，支持阻塞读取和超时读取。
 * 多个生产者线程可以安全地向队列写入数据，一个消费者线程可以安全地从队列读取数据。
 */
template <typename T>
class LockQueue
{
public:
  /**
   * @brief 向队列推送数据
   * @param data 要推送的数据
   *
   * 线程安全地向队列添加数据，并通知等待的消费者线程
   */
  void Push(const T &data)
  {
    std::lock_guard<std::mutex> lock(m_mutex); // 使用lock_gurad，即RAII的思想保证锁正确释放
    m_queue.push(data);
    m_condvariable.notify_one();
  }

  /**
   * @brief 从队列弹出数据（阻塞方式）
   * @return 队列中的数据
   *
   * 如果队列为空，线程将阻塞等待直到有数据可用
   */
  T Pop()
  {
    std::unique_lock<std::mutex> lock(m_mutex);
    while (m_queue.empty())
    {
      // 日志队列为空，线程进入wait状态
      m_condvariable.wait(lock); // 这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持
    }
    T data = m_queue.front();
    m_queue.pop();
    return data;
  }

  /**
   * @brief 从队列弹出数据（超时方式）
   * @param timeout 超时时间（毫秒）
   * @param ResData 用于存储结果的指针
   * @return 是否成功获取数据
   *
   * 在指定时间内尝试从队列获取数据，如果超时则返回false
   */
  bool timeOutPop(int timeout, T *ResData) // 添加一个超时时间参数，默认为 50 毫秒
  {
    std::unique_lock<std::mutex> lock(m_mutex);

    // 获取当前时间点，并计算出超时时刻
    auto now = std::chrono::system_clock::now();
    auto timeout_time = now + std::chrono::milliseconds(timeout);

    // 在超时之前，不断检查队列是否为空
    while (m_queue.empty())
    {
      // 如果已经超时了，就返回一个空对象
      if (m_condvariable.wait_until(lock, timeout_time) == std::cv_status::timeout)
      {
        return false;
      }
      else
      {
        continue;
      }
    }

    T data = m_queue.front();
    m_queue.pop();
    *ResData = data;
    return true;
  }

private:
  std::queue<T> m_queue;                  // 底层队列容器
  std::mutex m_mutex;                     // 互斥锁，保护队列访问
  std::condition_variable m_condvariable; // 条件变量，用于线程同步
};
// 两个对锁的管理用到了RAII的思想，防止中途出现问题而导致资源无法释放的问题！！！
// std::lock_guard 和 std::unique_lock 都是 C++11 中用来管理互斥锁的工具类，它们都封装了 RAII（Resource Acquisition Is
// Initialization）技术，使得互斥锁在需要时自动加锁，在不需要时自动解锁，从而避免了很多手动加锁和解锁的繁琐操作。
// std::lock_guard 是一个模板类，它的模板参数是一个互斥量类型。当创建一个 std::lock_guard
// 对象时，它会自动地对传入的互斥量进行加锁操作，并在该对象被销毁时对互斥量进行自动解锁操作。std::lock_guard
// 不能手动释放锁，因为其所提供的锁的生命周期与其绑定对象的生命周期一致。 std::unique_lock
// 也是一个模板类，同样的，其模板参数也是互斥量类型。不同的是，std::unique_lock 提供了更灵活的锁管理功能。可以通过
// lock()、unlock()、try_lock() 等方法手动控制锁的状态。当然，std::unique_lock 也支持 RAII
// 技术，即在对象被销毁时会自动解锁。另外， std::unique_lock 还支持超时等待和可中断等待的操作。

/**
 * @brief KV操作类，用于在KV存储和Raft之间传递命令
 *
 * 该类封装了KV存储操作（Get、Put、Append），包含操作类型、键、值、客户端ID和请求ID等信息。
 * 支持序列化和反序列化，用于RPC通信。
 */
class Op
{
public:
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  std::string Operation; // "Get" "Put" "Append" - 操作类型
  std::string Key;       // 键名
  std::string Value;     // 值
  std::string ClientId;  // 客户端号码
  int RequestId;         // 客户端号码请求的Request的序列号，为了保证线性一致性
                         //  IfDuplicate bool // Duplicate command can't be applied twice , but only for PUT and APPEND

public:
  // todo
  // 为了协调raftRPC中的command只设置成了string,这个的限制就是正常字符中不能包含|
  // 当然后期可以换成更高级的序列化方法，比如protobuf
  /**
   * @brief 将Op对象序列化为字符串
   * @return 序列化后的字符串
   *
   * 使用boost序列化库将Op对象转换为字符串，用于RPC传输
   */
  std::string asString() const
  {
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);

    // write class instance to archive
    oa << *this;
    // close archive

    return ss.str();
  }

  /**
   * @brief 从字符串反序列化Op对象
   * @param str 要解析的字符串
   * @return 是否解析成功
   *
   * 使用boost序列化库从字符串恢复Op对象
   */
  bool parseFromString(std::string str)
  {
    std::stringstream iss(str);
    boost::archive::text_iarchive ia(iss);
    // read class state from archive
    ia >> *this;
    return true; // todo : 解析失敗如何處理，要看一下boost庫了
  }

public:
  /**
   * @brief 输出流操作符重载
   * @param os 输出流
   * @param obj Op对象
   * @return 输出流引用
   */
  friend std::ostream &operator<<(std::ostream &os, const Op &obj)
  {
    os << "[MyClass:Operation{" + obj.Operation + "},Key{" + obj.Key + "},Value{" + obj.Value + "},ClientId{" +
              obj.ClientId + "},RequestId{" + std::to_string(obj.RequestId) + "}"; // 在这里实现自定义的输出格式
    return os;
  }

private:
  friend class boost::serialization::access;
  /**
   * @brief boost序列化函数
   * @param ar 归档对象
   * @param version 版本号
   */
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)
  {
    ar & Operation;
    ar & Key;
    ar & Value;
    ar & ClientId;
    ar & RequestId;
  }
};

///////////////////////////////////////////////kvserver reply err to clerk

const std::string OK = "OK";                         // 操作成功
const std::string ErrNoKey = "ErrNoKey";             // 键不存在错误
const std::string ErrWrongLeader = "ErrWrongLeader"; // 错误的领导者错误

////////////////////////////////////获取可用端口

/**
 * @brief 检查端口是否可用
 * @param usPort 要检查的端口号
 * @return 如果端口可用返回true，否则返回false
 */
bool isReleasePort(unsigned short usPort);

/**
 * @brief 获取一个可用的端口
 * @param port 输入输出参数，输入起始端口，输出找到的可用端口
 * @return 如果找到可用端口返回true，否则返回false
 */
bool getReleasePort(short &port);
