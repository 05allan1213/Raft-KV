/**
 * @file raft_client.cpp
 * @brief Raft-KV客户端测试程序
 *
 * 演示Raft-KV分布式键值存储的核心功能：
 * - Put操作：存储键值对
 * - Get操作：读取键值对
 * - Append操作：追加值
 */

#include <iostream>
#include <string>
#include <fstream>
#include <chrono>
#include <thread>
#include "raft-kv/raftClerk/clerk.h"

// 统计信息
int g_total_operations = 0;
int g_successful_operations = 0;
int g_failed_operations = 0;

/**
 * @brief 等待指定时间
 */
void wait_seconds(int seconds, const std::string &message = "")
{
    if (!message.empty())
    {
        std::cout << "[等待] " << message << " (" << seconds << "秒)" << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
}

/**
 * @brief 执行Put操作
 */
bool test_put(Clerk &clerk, const std::string &key, const std::string &value)
{
    try
    {
        std::cout << "[1] Put操作" << std::endl;
        std::cout << "    Put('" << key << "', '" << value << "')" << std::endl;

        clerk.Put(key, value);

        std::cout << "    [成功] Put操作完成" << std::endl;
        g_successful_operations++;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cout << "    [失败] Put操作失败：" << e.what() << std::endl;
        g_failed_operations++;
        return false;
    }
}

/**
 * @brief 执行Get操作
 */
bool test_get(Clerk &clerk, const std::string &key)
{
    try
    {
        std::cout << "\n[2] Get操作" << std::endl;
        std::cout << "    Get('" << key << "')" << std::endl;

        std::string value = clerk.Get(key);

        std::cout << "    [成功] 获取到值：'" << value << "'" << std::endl;
        g_successful_operations++;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cout << "    [失败] Get操作失败：" << e.what() << std::endl;
        g_failed_operations++;
        return false;
    }
}

/**
 * @brief 执行Append操作
 */
bool test_append(Clerk &clerk, const std::string &key, const std::string &value)
{
    try
    {
        std::cout << "\n[3] Append操作" << std::endl;
        std::cout << "    Append('" << key << "', '" << value << "')" << std::endl;

        clerk.Append(key, value);

        std::cout << "    [成功] Append操作完成" << std::endl;
        g_successful_operations++;
        return true;
    }
    catch (const std::exception &e)
    {
        std::cout << "    [失败] Append操作失败：" << e.what() << std::endl;
        g_failed_operations++;
        return false;
    }
}

/**
 * @brief 主函数
 */
int main()
{
    std::cout << "=== Raft-KV客户端测试 ===" << std::endl;

    try
    {
        // 生成配置文件
        std::cout << "[配置] 生成配置文件..." << std::endl;
        std::ofstream config_file("test.conf");
        if (!config_file.is_open())
        {
            std::cerr << "[错误] 无法创建配置文件" << std::endl;
            return 1;
        }

        // 与服务端使用相同的端口（单节点）
        std::vector<int> ports = {20000};
        for (size_t i = 0; i < ports.size(); i++)
        {
            config_file << "node" << i << "ip=127.0.1.1" << std::endl;
            config_file << "node" << i << "port=" << ports[i] << std::endl;
        }
        config_file.close();

        // 连接到Raft集群
        std::cout << "[客户端] 正在连接到Raft集群..." << std::endl;
        Clerk clerk;
        clerk.Init("test.conf");
        std::cout << "[客户端] 连接成功！" << std::endl;

        std::cout << "\n=== Raft-KV基本操作演示 ===" << std::endl;

        // 等待一下确保服务端准备好
        wait_seconds(3, "等待服务端准备就绪");

        // 测试Put操作
        g_total_operations++;
        test_put(clerk, "user", "alice");

        wait_seconds(3, "等待Put操作完成");

        // 测试Get操作
        g_total_operations++;
        test_get(clerk, "user");

        wait_seconds(3, "等待Get操作完成");

        // 测试Append操作
        g_total_operations++;
        test_append(clerk, "user", "_admin");

        wait_seconds(3, "等待Append操作完成");

        // 再次Get验证Append结果
        g_total_operations++;
        test_get(clerk, "user");

        // 显示测试结果
        std::cout << "\n=== 测试结果 ===" << std::endl;
        std::cout << "总操作: " << g_total_operations << std::endl;
        std::cout << "成功: " << g_successful_operations << std::endl;
        std::cout << "失败: " << g_failed_operations << std::endl;

        if (g_failed_operations == 0)
        {
            std::cout << "🎉 所有测试通过！Raft-KV系统工作正常！" << std::endl;
        }
        else
        {
            std::cout << "⚠️  有 " << g_failed_operations << " 个操作失败" << std::endl;
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "[错误] " << e.what() << std::endl;
        return 1;
    }

    std::cout << "=== 测试完成 ===" << std::endl;
    return 0;
}
