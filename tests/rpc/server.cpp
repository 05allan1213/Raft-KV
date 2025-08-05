/**
 * @file server.cpp
 * @brief RPC服务器全面功能测试
 *
 * 该文件对RPC模块进行全面测试，包括：
 * - RpcProvider服务提供者功能（服务注册、启动、请求处理）
 * - 多服务注册和管理
 * - 长连接管理
 * - 错误处理机制
 * - 并发请求处理
 * - 性能监控
 */

#include <iostream>
#include <string>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>
#include <map>
#include <memory>
#include "raft-kv/rpc/rpcprovider.h"
#include "raft-kv/rpc/friend.pb.h"

// 全局统计计数器
std::atomic<int> g_request_count{0};
std::atomic<int> g_error_count{0};
std::atomic<int> g_success_count{0};

// ==================== 1. 好友服务实现 ====================

/**
 * @brief 好友服务类，提供好友相关的RPC服务
 */
class FriendService : public fixbug::FriendServiceRpc
{
public:
    FriendService()
    {
        std::cout << "[服务器] 好友服务已初始化" << std::endl;

        // 初始化一些测试数据
        user_friends_[12345] = {"张三", "李四", "王五", "赵六"};
        user_friends_[67890] = {"小明", "小红", "小刚"};
        user_friends_[11111] = {"Alice", "Bob", "Charlie", "David", "Eve"};
        user_friends_[22222] = {}; // 空好友列表
    }

    // 实际的本地业务方法
    std::vector<std::string> GetFriendsList(uint32_t userid)
    {
        std::cout << "[好友服务] 处理用户 " << userid << " 的好友列表请求" << std::endl;

        auto it = user_friends_.find(userid);
        if (it != user_friends_.end())
        {
            std::cout << "[好友服务] 找到用户 " << userid << "，好友数量: " << it->second.size() << std::endl;
            return it->second;
        }
        else
        {
            std::cout << "[好友服务] 用户 " << userid << " 不存在，返回空列表" << std::endl;
            return {};
        }
    }

    // 重写 Protobuf 生成的虚函数，用于响应 RPC 请求
    void GetFriendsList(::google::protobuf::RpcController *controller,
                        const ::fixbug::GetFriendsListRequest *request,
                        ::fixbug::GetFriendsListResponse *response,
                        ::google::protobuf::Closure *done) override
    {
        g_request_count++;
        auto start_time = std::chrono::high_resolution_clock::now();

        std::cout << "[好友服务] 收到RPC请求，请求ID: " << g_request_count.load() << std::endl;

        try
        {
            // 1. 从 request 中获取参数
            uint32_t userid = request->userid();

            // 模拟参数验证
            if (userid == 0)
            {
                std::cout << "[好友服务] 参数错误：用户ID不能为0" << std::endl;
                response->mutable_result()->set_errcode(1001);
                response->mutable_result()->set_errmsg("用户ID不能为0");
                g_error_count++;
            }
            else
            {
                // 2. 调用本地业务
                std::vector<std::string> friendsList = GetFriendsList(userid);

                // 3. 填充 response
                response->mutable_result()->set_errcode(0);
                response->mutable_result()->set_errmsg("成功");
                for (const std::string &name : friendsList)
                {
                    response->add_friends(name);
                }

                g_success_count++;
                std::cout << "[好友服务] 请求处理成功，返回 " << friendsList.size() << " 个好友" << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "[好友服务] 处理请求时发生异常: " << e.what() << std::endl;
            response->mutable_result()->set_errcode(1002);
            response->mutable_result()->set_errmsg("服务器内部错误");
            g_error_count++;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        std::cout << "[好友服务] 请求处理耗时: " << duration.count() << " 微秒" << std::endl;

        // 4. 执行回调，完成 RPC 响应
        if (done)
        {
            done->Run();
        }
    }

private:
    // 用户好友数据存储
    std::map<uint32_t, std::vector<std::string>> user_friends_;
};

// ==================== 2. 计算服务实现（模拟多服务） ====================

/**
 * @brief 计算服务类，提供数学计算相关的RPC服务
 * 注意：这里为了演示多服务，我们复用friend.proto的消息格式
 */
class CalculatorService : public fixbug::FriendServiceRpc
{
public:
    CalculatorService()
    {
        std::cout << "[服务器] 计算服务已初始化" << std::endl;
    }

    // 重写方法，将其用作计算服务
    void GetFriendsList(::google::protobuf::RpcController *controller,
                        const ::fixbug::GetFriendsListRequest *request,
                        ::fixbug::GetFriendsListResponse *response,
                        ::google::protobuf::Closure *done) override
    {
        g_request_count++;
        auto start_time = std::chrono::high_resolution_clock::now();

        std::cout << "[计算服务] 收到计算请求，请求ID: " << g_request_count.load() << std::endl;

        try
        {
            uint32_t number = request->userid(); // 复用userid字段作为计算输入

            // 模拟一些计算操作
            std::vector<std::string> results;
            results.push_back("平方: " + std::to_string(number * number));
            results.push_back("立方: " + std::to_string(number * number * number));
            results.push_back("阶乘: " + std::to_string(factorial(number % 10))); // 限制在10以内

            // 模拟计算耗时
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            response->mutable_result()->set_errcode(0);
            response->mutable_result()->set_errmsg("计算成功");
            for (const std::string &result : results)
            {
                response->add_friends(result); // 复用friends字段返回计算结果
            }

            g_success_count++;
            std::cout << "[计算服务] 计算完成，输入: " << number << std::endl;
        }
        catch (const std::exception &e)
        {
            std::cout << "[计算服务] 计算时发生异常: " << e.what() << std::endl;
            response->mutable_result()->set_errcode(2001);
            response->mutable_result()->set_errmsg("计算错误");
            g_error_count++;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        std::cout << "[计算服务] 请求处理耗时: " << duration.count() << " 微秒" << std::endl;

        if (done)
        {
            done->Run();
        }
    }

private:
    uint64_t factorial(int n)
    {
        if (n <= 1)
            return 1;
        uint64_t result = 1;
        for (int i = 2; i <= n; i++)
        {
            result *= i;
        }
        return result;
    }
};

// ==================== 3. 统计监控功能 ====================

void print_statistics()
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(10));

        std::cout << "\n========== 服务器统计信息 ==========" << std::endl;
        std::cout << "总请求数: " << g_request_count.load() << std::endl;
        std::cout << "成功请求数: " << g_success_count.load() << std::endl;
        std::cout << "错误请求数: " << g_error_count.load() << std::endl;
        std::cout << "成功率: " << (g_request_count.load() > 0 ? (100.0 * g_success_count.load() / g_request_count.load()) : 0.0)
                  << "%" << std::endl;
        std::cout << "================================\n"
                  << std::endl;
    }
}

int main(int argc, char **argv)
{
    std::cout << "========================================" << std::endl;
    std::cout << "    RPC服务器全面功能测试开始" << std::endl;
    std::cout << "========================================" << std::endl;

    try
    {
        // 1. 创建 RpcProvider
        RpcProvider provider;
        std::cout << "[服务器] RpcProvider已创建" << std::endl;

        // 2. 注册多个服务
        auto friend_service = std::make_unique<FriendService>();
        auto calculator_service = std::make_unique<CalculatorService>();

        provider.NotifyService(friend_service.get());
        std::cout << "[服务器] 好友服务已注册" << std::endl;

        // 注意：由于我们复用了相同的proto定义，这里不能同时注册两个相同类型的服务
        // 在实际应用中，应该为不同服务定义不同的proto文件
        // provider.NotifyService(calculator_service.get());

        std::cout << "[服务器] 所有服务注册完成" << std::endl;

        // 3. 启动统计监控线程
        std::thread stats_thread(print_statistics);
        stats_thread.detach();
        std::cout << "[服务器] 统计监控线程已启动" << std::endl;

        // 4. 启动服务器
        std::cout << "[服务器] 准备启动RPC服务器..." << std::endl;
        std::cout << "[服务器] 监听地址: 127.0.1.1:7788" << std::endl;
        std::cout << "[服务器] 支持的服务:" << std::endl;
        std::cout << "  - FriendService::GetFriendsList (好友列表查询)" << std::endl;
        std::cout << "[服务器] 服务器启动中，按Ctrl+C停止..." << std::endl;

        // provider.Run(nodeIndex, port)
        provider.Run(1, 7788);
    }
    catch (const std::exception &e)
    {
        std::cerr << "[服务器] 启动失败: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}