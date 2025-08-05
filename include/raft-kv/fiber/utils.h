#pragma once

#include <assert.h>
#include <cxxabi.h>
#include <execinfo.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <sstream>
#include <vector>

namespace monsoon
{
  /**
   * @brief 获取当前线程ID
   * @return 当前线程的ID
   */
  pid_t GetThreadId();

  /**
   * @brief 获取当前协程ID
   * @return 当前协程的ID
   */
  u_int32_t GetFiberId();

  /**
   * @brief 获取当前启动的毫秒数
   * @details 系统从启动到当前时刻的毫秒数
   * @return 系统启动后的毫秒数
   */
  static uint64_t GetElapsedMS()
  {
    struct timespec ts = {0};
    clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
    return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
  }

  /**
   * @brief 将原始函数名解析为可读函数名
   * @param str 原始函数名字符串
   * @return 解析后的可读函数名
   */
  static std::string demangle(const char *str)
  {
    size_t size = 0;
    int status = 0;
    std::string rt;
    rt.resize(256);
    if (1 == sscanf(str, "%*[^(]%*[^_]%255[^)+]", &rt[0]))
    {
      // 解析函数
      char *v = abi::__cxa_demangle(&rt[0], nullptr, &size, &status);
      if (v)
      {
        std::string result(v);
        free(v);
        return result;
      }
    }
    // 解析失败，返回原始函数名
    if (1 == sscanf(str, "%255s", &rt[0]))
    {
      return rt;
    }
    return str;
  }

  /**
   * @brief 获取当前线程的调用栈信息
   * @param bt 输出参数，存储调用栈信息
   * @param size 调用栈大小
   * @param skip 跳过的栈帧数量
   */
  static void Backtrace(std::vector<std::string> &bt, int size, int skip)
  {
    // 分配用于存储调用栈信息的数组
    void **array = (void **)malloc((sizeof(void *) * size));
    size_t s = ::backtrace(array, size);

    // 获取调用栈信息
    char **strings = backtrace_symbols(array, s);
    if (strings == NULL)
    {
      std::cout << "backtrace_synbols error" << std::endl;
      return;
    }
    // 解析每一个调用栈的信息，并将解析后的函数名添加到bt中
    for (size_t i = skip; i < s; ++i)
    {
      bt.push_back(demangle(strings[i]));
    }

    free(strings);
    free(array);
  }

  /**
   * @brief 将调用栈信息转换为字符串
   * @param size 调用栈大小
   * @param skip 跳过的栈帧数量
   * @param prefix 每行前缀
   * @return 调用栈字符串
   */
  static std::string BacktraceToString(int size, int skip, const std::string &prefix)
  {
    std::vector<std::string> bt;
    Backtrace(bt, size, skip);
    std::stringstream ss;
    for (size_t i = 0; i < bt.size(); ++i)
    {
      ss << prefix << bt[i] << std::endl;
    }
    return ss.str();
  }

  /**
   * @brief 断言处理函数
   * @param condition 断言条件
   * @param err 错误信息
   * @details 如果条件为false，则打印错误信息和调用栈，然后触发assert
   */
  static void CondPanic(bool condition, std::string err)
  {
    if (!condition)
    {
      std::cout << "[assert by] (" << __FILE__ << ":" << __LINE__ << "),err: " << err << std::endl;
      std::cout << "[backtrace]\n"
                << BacktraceToString(6, 3, "") << std::endl;
      assert(condition);
    }
  }
}