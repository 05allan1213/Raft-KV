#include "util.h"
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <iomanip>

/**
 * @brief 断言函数实现
 * @param condition 断言条件
 * @param message 断言失败时的错误信息
 *
 * 如果条件为false，则输出错误信息并退出程序
 */
void myAssert(bool condition, std::string message)
{
  if (!condition)
  {
    std::cerr << "Error: " << message << std::endl;
    std::exit(EXIT_FAILURE); // 断言失败时直接退出程序
  }
}

/**
 * @brief 获取当前时间点
 * @return 当前系统时间点
 *
 * 使用高精度时钟获取当前时间，用于性能测量和时间计算
 */
std::chrono::_V2::system_clock::time_point now()
{
  return std::chrono::high_resolution_clock::now();
}

/**
 * @brief 获取随机化的选举超时时间
 * @return 随机化的选举超时时间（毫秒）
 *
 * 返回一个在minRandomizedElectionTime到maxRandomizedElectionTime之间的随机值
 * 这是Raft算法中的重要机制，用于避免选举冲突
 */
std::chrono::milliseconds getRandomizedElectionTimeout()
{
  std::random_device rd;                                                                         // 获取随机种子
  std::mt19937 rng(rd());                                                                        // 使用Mersenne Twister随机数生成器
  std::uniform_int_distribution<int> dist(minRandomizedElectionTime, maxRandomizedElectionTime); // 创建均匀分布

  return std::chrono::milliseconds(dist(rng)); // 生成随机超时时间
}

/**
 * @brief 睡眠指定毫秒数
 * @param N 要睡眠的毫秒数
 *
 * 使用当前线程睡眠指定的毫秒数
 */
void sleepNMilliseconds(int N)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(N));
};

/**
 * @brief 获取一个可用的端口
 * @param port 输入输出参数，输入起始端口，输出找到的可用端口
 * @return 如果找到可用端口返回true，否则返回false
 *
 * 从指定端口开始，逐个检查端口是否可用，最多尝试30次
 */
bool getReleasePort(short &port)
{
  short num = 0; // 尝试次数计数器
  while (!isReleasePort(port) && num < 30)
  {         // 最多尝试30次
    ++port; // 尝试下一个端口
    ++num;
  }
  if (num >= 30)
  {            // 如果尝试了30次都没找到可用端口
    port = -1; // 设置端口为无效值
    return false;
  }
  return true; // 找到可用端口
}

/**
 * @brief 检查端口是否可用
 * @param usPort 要检查的端口号
 * @return 如果端口可用返回true，否则返回false
 *
 * 通过尝试绑定socket来检查端口是否被占用
 */
bool isReleasePort(unsigned short usPort)
{
  // 创建TCP socket
  int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  sockaddr_in addr;                              // 创建地址结构体
  addr.sin_family = AF_INET;                     // 设置为IPv4
  addr.sin_port = htons(usPort);                 // 设置端口号，转换为网络字节序
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); // 设置为本地回环地址

  // 尝试绑定socket到指定端口
  int ret = ::bind(s, (sockaddr *)&addr, sizeof(addr));
  if (ret != 0)
  {           // 如果绑定失败，说明端口被占用
    close(s); // 关闭socket
    return false;
  }
  close(s);    // 关闭socket
  return true; // 绑定成功，说明端口可用
}

/**
 * @brief 调试打印函数实现
 * @param format 格式化字符串
 * @param ... 可变参数
 *
 * 当Debug模式开启时，将格式化输出打印到控制台
 * 输出包含时间戳，便于调试和日志记录
 */
void DPrintf(const char *format, ...)
{
  if (Debug)
  { // 只有在调试模式下才输出
    // 获取当前的日期，然后取日志信息，写入相应的日志文件当中 a+
    time_t now = time(nullptr);  // 获取当前时间
    tm *nowtm = localtime(&now); // 转换为本地时间结构

    va_list args;           // 声明可变参数列表
    va_start(args, format); // 初始化可变参数

    // 打印时间戳：年-月-日 时:分:秒
    std::printf("[%04d-%02d-%02d %02d:%02d:%02d] ",
                nowtm->tm_year + 1900, // 年份（需要加1900）
                nowtm->tm_mon + 1,     // 月份（需要加1，因为tm_mon从0开始）
                nowtm->tm_mday,        // 日期
                nowtm->tm_hour,        // 小时
                nowtm->tm_min,         // 分钟
                nowtm->tm_sec);        // 秒

    std::vprintf(format, args); // 打印格式化的消息内容
    std::printf("\n");          // 换行
    va_end(args);               // 结束可变参数处理
  }
}
