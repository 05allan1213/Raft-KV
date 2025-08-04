/**
 * @file client.cpp
 * @brief RPC客户端全面功能测试
 *
 * 该文件对RPC客户端进行全面测试，包括：
 * - MprpcChannel客户端通道功能（连接管理、同步/异步调用）
 * - MprpcController控制器功能（状态管理、错误处理）
 * - 长连接管理和重连机制
 * - 并发请求测试
 * - 错误处理和超时测试
 * - 性能基准测试
 *
 * @author Raft-KV Team
 * @date 2024
 */

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <future>
#include <random>
#include <future>
#include "raft-kv/rpc/mprpcchannel.h"
#include "raft-kv/rpc/mprpccontroller.h"
#include "raft-kv/rpc/friend.pb.h"

// 全局统计计数器
std::atomic<int> g_total_requests{0};
std::atomic<int> g_success_requests{0};
std::atomic<int> g_failed_requests{0};
std::atomic<int> g_timeout_requests{0};

// ==================== 1. 基本RPC调用测试 ====================

/**
 * @brief 执行单次RPC调用测试
 */
bool test_single_rpc_call(const std::string &ip, short port, uint32_t userid, const std::string &test_name)
{
    std::cout << "[" << test_name << "] 开始测试用户ID: " << userid << std::endl;

    try
    {
        // 1. 创建RPC通道
        MprpcChannel channel(ip, port, true);

        // 2. 创建服务存根
        fixbug::FriendServiceRpc_Stub stub(&channel);

        // 3. 准备请求和响应
        fixbug::GetFriendsListRequest request;
        request.set_userid(userid);
        fixbug::GetFriendsListResponse response;

        // 4. 创建控制器
        MprpcController controller;

        auto start_time = std::chrono::high_resolution_clock::now();

        // 5. 发起同步RPC调用
        stub.GetFriendsList(&controller, &request, &response, nullptr);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        g_total_requests++;

        // 6. 检查结果
        if (controller.Failed())
        {
            std::cerr << "[" << test_name << "] RPC调用失败: " << controller.ErrorText() << std::endl;
            g_failed_requests++;
            return false;
        }
        else
        {
            if (response.result().errcode() == 0)
            {
                std::cout << "[" << test_name << "] RPC调用成功！耗时: " << duration.count() << "ms" << std::endl;
                std::cout << "[" << test_name << "] 好友列表 (共" << response.friends_size() << "个):" << std::endl;
                for (int i = 0; i < response.friends_size(); ++i)
                {
                    std::cout << "  - " << response.friends(i) << std::endl;
                }
                g_success_requests++;
                return true;
            }
            else
            {
                std::cerr << "[" << test_name << "] 业务逻辑错误: " << response.result().errmsg() << std::endl;
                g_failed_requests++;
                return false;
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "[" << test_name << "] 异常: " << e.what() << std::endl;
        g_failed_requests++;
        return false;
    }
}

// ==================== 2. 并发测试 ====================

/**
 * @brief 并发RPC调用测试
 */
void test_concurrent_rpc_calls(const std::string &ip, short port, int thread_count, int requests_per_thread)
{
    std::cout << "\n=== 开始并发RPC调用测试 ===" << std::endl;
    std::cout << "线程数: " << thread_count << ", 每线程请求数: " << requests_per_thread << std::endl;

    std::vector<std::thread> threads;
    auto start_time = std::chrono::high_resolution_clock::now();

    // 创建多个线程进行并发测试
    for (int i = 0; i < thread_count; i++)
    {
        threads.emplace_back([ip, port, requests_per_thread, i]()
                             {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(10000, 99999);

            for (int j = 0; j < requests_per_thread; j++) {
                uint32_t userid = dis(gen);
                std::string test_name = "并发测试_线程" + std::to_string(i) + "_请求" + std::to_string(j);
                test_single_rpc_call(ip, port, userid, test_name);

                // 随机延迟，模拟真实场景
                std::this_thread::sleep_for(std::chrono::milliseconds(10 + (j % 50)));
            } });
    }

    // 等待所有线程完成
    for (auto &thread : threads)
    {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "=== 并发测试完成 ===" << std::endl;
    std::cout << "总耗时: " << duration.count() << "ms" << std::endl;
    std::cout << "平均QPS: " << (thread_count * requests_per_thread * 1000.0 / duration.count()) << std::endl;
}

// ==================== 3. 错误处理和边界测试 ====================

/**
 * @brief 测试各种错误情况
 */
void test_error_handling(const std::string &ip, short port)
{
    std::cout << "\n=== 开始错误处理测试 ===" << std::endl;

    // 测试1: 无效用户ID
    std::cout << "\n[错误测试1] 测试无效用户ID (0)" << std::endl;
    test_single_rpc_call(ip, port, 0, "无效用户ID测试");

    // 测试2: 不存在的用户ID
    std::cout << "\n[错误测试2] 测试不存在的用户ID (99999)" << std::endl;
    test_single_rpc_call(ip, port, 99999, "不存在用户测试");

    // 测试3: 连接错误的端口
    std::cout << "\n[错误测试3] 测试连接错误端口" << std::endl;
    test_single_rpc_call(ip, 9999, 12345, "错误端口测试");

    // 测试4: 连接错误的IP (跳过，避免长时间阻塞)
    std::cout << "\n[错误测试4] 跳过错误IP测试（避免长时间阻塞）" << std::endl;

    std::cout << "=== 错误处理测试完成 ===" << std::endl;
}

// ==================== 4. 长连接和重连测试 ====================

/**
 * @brief 测试长连接复用
 */
void test_connection_reuse(const std::string &ip, short port)
{
    std::cout << "\n=== 开始长连接复用测试 ===" << std::endl;

    try
    {
        // 创建一个长连接
        MprpcChannel channel(ip, port, true);
        fixbug::FriendServiceRpc_Stub stub(&channel);

        // 使用同一个连接发送多个请求
        std::vector<uint32_t> test_users = {12345, 67890, 11111, 22222};

        for (int i = 0; i < test_users.size(); i++)
        {
            std::cout << "\n[长连接测试] 第" << (i + 1) << "次请求，用户ID: " << test_users[i] << std::endl;

            fixbug::GetFriendsListRequest request;
            request.set_userid(test_users[i]);
            fixbug::GetFriendsListResponse response;
            MprpcController controller;

            auto start_time = std::chrono::high_resolution_clock::now();
            stub.GetFriendsList(&controller, &request, &response, nullptr);
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

            if (!controller.Failed() && response.result().errcode() == 0)
            {
                std::cout << "[长连接测试] 请求成功，耗时: " << duration.count() << "ms，好友数: " << response.friends_size() << std::endl;
            }
            else
            {
                std::cout << "[长连接测试] 请求失败" << std::endl;
            }

            // 短暂延迟
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "[长连接测试] 异常: " << e.what() << std::endl;
    }

    std::cout << "=== 长连接复用测试完成 ===" << std::endl;
}

// ==================== 5. 性能基准测试 ====================

/**
 * @brief 性能基准测试
 */
void test_performance_benchmark(const std::string &ip, short port)
{
    std::cout << "\n=== 开始性能基准测试 ===" << std::endl;

    const int WARMUP_REQUESTS = 50;
    const int BENCHMARK_REQUESTS = 1000;

    // 预热阶段
    std::cout << "[性能测试] 预热阶段，发送 " << WARMUP_REQUESTS << " 个请求..." << std::endl;
    for (int i = 0; i < WARMUP_REQUESTS; i++)
    {
        test_single_rpc_call(ip, port, 12345, "预热");
    }

    // 重置计数器
    g_total_requests = 0;
    g_success_requests = 0;
    g_failed_requests = 0;

    // 基准测试
    std::cout << "[性能测试] 基准测试阶段，发送 " << BENCHMARK_REQUESTS << " 个请求..." << std::endl;
    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    const int THREAD_COUNT = 10;
    const int REQUESTS_PER_THREAD = BENCHMARK_REQUESTS / THREAD_COUNT;

    for (int i = 0; i < THREAD_COUNT; i++)
    {
        threads.emplace_back([ip, port, REQUESTS_PER_THREAD, i]()
                             {
            for (int j = 0; j < REQUESTS_PER_THREAD; j++) {
                test_single_rpc_call(ip, port, 12345, "基准测试");
            } });
    }

    for (auto &thread : threads)
    {
        thread.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n=== 性能基准测试结果 ===" << std::endl;
    std::cout << "总请求数: " << g_total_requests.load() << std::endl;
    std::cout << "成功请求数: " << g_success_requests.load() << std::endl;
    std::cout << "失败请求数: " << g_failed_requests.load() << std::endl;
    std::cout << "成功率: " << (100.0 * g_success_requests.load() / g_total_requests.load()) << "%" << std::endl;
    std::cout << "总耗时: " << duration.count() << "ms" << std::endl;
    std::cout << "平均延迟: " << (duration.count() / (double)g_total_requests.load()) << "ms" << std::endl;
    std::cout << "QPS: " << (g_total_requests.load() * 1000.0 / duration.count()) << std::endl;
    std::cout << "=== 性能基准测试完成 ===" << std::endl;
}

// ==================== 6. 主函数 ====================

int main(int argc, char **argv)
{
    std::cout << "========================================" << std::endl;
    std::cout << "    RPC客户端全面功能测试开始" << std::endl;
    std::cout << "========================================" << std::endl;

    std::string ip = "127.0.1.1";
    short port = 7788;

    // 解析命令行参数
    if (argc >= 2)
    {
        ip = argv[1];
    }
    if (argc >= 3)
    {
        port = std::atoi(argv[2]);
    }

    std::cout << "[客户端] 目标服务器: " << ip << ":" << port << std::endl;
    std::cout << "[客户端] 使用方法: " << argv[0] << " [ip] [port]" << std::endl;

    try
    {
        // 1. 基本功能测试
        std::cout << "\n========== 1. 基本RPC调用测试 ==========" << std::endl;

        // 测试已知用户
        test_single_rpc_call(ip, port, 12345, "基本测试1");
        test_single_rpc_call(ip, port, 67890, "基本测试2");
        test_single_rpc_call(ip, port, 11111, "基本测试3");
        test_single_rpc_call(ip, port, 22222, "基本测试4");

        // 2. 错误处理测试
        std::cout << "\n========== 2. 错误处理测试 ==========" << std::endl;
        test_error_handling(ip, port);

        // 3. 长连接测试
        std::cout << "\n========== 3. 长连接复用测试 ==========" << std::endl;
        test_connection_reuse(ip, port);

        // 4. 并发测试
        std::cout << "\n========== 4. 并发测试 ==========" << std::endl;
        test_concurrent_rpc_calls(ip, port, 5, 10); // 5个线程，每个线程10个请求

        // 5. 性能基准测试
        std::cout << "\n========== 5. 性能基准测试 ==========" << std::endl;
        test_performance_benchmark(ip, port);

        // 最终统计
        std::cout << "\n========================================" << std::endl;
        std::cout << "    所有测试完成！" << std::endl;
        std::cout << "    最终统计:" << std::endl;
        std::cout << "    - 总请求数: " << g_total_requests.load() << std::endl;
        std::cout << "    - 成功请求数: " << g_success_requests.load() << std::endl;
        std::cout << "    - 失败请求数: " << g_failed_requests.load() << std::endl;
        std::cout << "    - 超时请求数: " << g_timeout_requests.load() << std::endl;
        if (g_total_requests.load() > 0)
        {
            std::cout << "    - 总体成功率: " << (100.0 * g_success_requests.load() / g_total_requests.load()) << "%" << std::endl;
        }
        std::cout << "========================================" << std::endl;
    }
    catch (const std::exception &e)
    {
        std::cerr << "[客户端] 测试过程中发生异常: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}