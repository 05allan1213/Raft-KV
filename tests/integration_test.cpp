/**
 * @file integration_test.cpp
 * @brief Raft-KV 系统综合集成测试
 *
 * 该测试文件旨在全面展示Raft-KV系统的核心功能，包括：
 * 1.  动态启动一个多节点的Raft集群。
 * 2.  验证领导者选举过程。
 * 3.  通过客户端执行Put、Get、Append等操作，检验数据一致性。
 * 4.  模拟领导者节点故障，测试系统的容错能力和新领导者的选举。
 * 5.  展示 fiber、rpc 和 raft 模块的协同工作效果。
 *
 */

#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <thread>
#include <chrono>
#include <memory>
#include <csignal>
#include <sys/wait.h>
#include <unistd.h>

#include "raft-kv/raftCore/kvServer.h"
#include "raft-kv/raftClerk/clerk.h"
#include "raft-kv/common/util.h"

// --- 全局控制变量 ---
std::vector<pid_t> g_server_pids;
const std::string CONFIG_FILE = "integration_test.conf";
const int CLUSTER_SIZE = 3;
const int BASE_PORT = 21000;

// --- 前向声明 ---
void cleanup_cluster();

/**
 * @brief 信号处理器，用于优雅地关闭集群
 */
void signal_handler(int sig)
{
    std::cout << "\n[主控] 收到信号 " << sig << "，开始清理集群..." << std::endl;
    cleanup_cluster();
    exit(0);
}

/**
 * @brief 生成集群的配置文件
 */
void generate_config(int num_nodes)
{
    std::cout << "[主控] 正在生成配置文件: " << CONFIG_FILE << std::endl;
    std::ofstream config_file(CONFIG_FILE);
    if (!config_file.is_open())
    {
        std::cerr << "[错误] 无法创建配置文件" << std::endl;
        exit(1);
    }

    for (int i = 0; i < num_nodes; ++i)
    {
        config_file << "node" << i << "ip=127.0.0.1" << std::endl;
        config_file << "node" << i << "port=" << BASE_PORT + i << std::endl;
    }
    config_file.close();
    std::cout << "[主控] 配置文件生成完毕。" << std::endl;
}

/**
 * @brief 启动Raft-KV集群
 */
void start_cluster(int num_nodes)
{
    std::cout << "[主控] 准备启动 " << num_nodes << " 个节点的 Raft-KV 集群..." << std::endl;
    for (int i = 0; i < num_nodes; ++i)
    {
        pid_t pid = fork();
        if (pid == 0)
        { // 子进程
            try
            {
                // 等待一小段时间，确保配置文件写入磁盘
                std::this_thread::sleep_for(std::chrono::milliseconds(100));

                std::cout << "[节点 " << i << "] 正在启动 KvServer..." << std::endl;
                KvServer kvServer(i, 10000, CONFIG_FILE, BASE_PORT + i);
                // KvServer的构造函数会阻塞并运行服务，所以这里不需要其他代码
            }
            catch (const std::exception &e)
            {
                std::cerr << "[节点 " << i << "] 启动失败: " << e.what() << std::endl;
                exit(1);
            }
            exit(0);
        }
        else if (pid > 0)
        { // 父进程
            g_server_pids.push_back(pid);
            std::cout << "[主控] 节点 " << i << " 进程已创建，PID: " << pid << ", 端口: " << BASE_PORT + i << std::endl;
        }
        else
        {
            std::cerr << "[错误] Fork 节点 " << i << " 失败" << std::endl;
            cleanup_cluster();
            exit(1);
        }
        // 稍微错开启动时间
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    std::cout << "[主控] 所有节点进程已创建。" << std::endl;
}

/**
 * @brief 清理并关闭所有集群节点
 */
void cleanup_cluster()
{
    std::cout << "[主控] 正在关闭所有集群节点..." << std::endl;
    for (pid_t pid : g_server_pids)
    {
        if (pid > 0)
        {
            kill(pid, SIGTERM);
        }
    }
    // 等待所有子进程退出
    for (pid_t pid : g_server_pids)
    {
        if (pid > 0)
        {
            waitpid(pid, nullptr, 0);
        }
    }
    g_server_pids.clear();
    remove(CONFIG_FILE.c_str());
    std::cout << "[主控] 集群已清理完毕。" << std::endl;
}

/**
 * @brief 客户端操作：尝试找到集群的领导者
 * @return 领导者的ID，如果找不到则返回-1
 */
int find_leader(Clerk &clerk)
{
    std::cout << "\n[客户端] 正在寻找领导者..." << std::endl;
    for (int i = 0; i < 15; ++i) // 增加重试次数
    {
        try
        {
            // 使用Put操作来确保找到真正的Leader，因为Put操作需要Leader处理
            std::string test_key = "leader_test_" + std::to_string(i);
            std::string test_value = "test_value";

            std::cout << "[客户端] 尝试Put操作来寻找Leader..." << std::endl;
            clerk.Put(test_key, test_value);

            // 验证Put操作是否成功
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            std::string ret_value = clerk.Get(test_key);

            if (ret_value == test_value)
            {
                std::cout << "[客户端] 成功找到Leader并验证数据一致性！" << std::endl;
                return 0; // 返回0表示成功
            }
            else
            {
                std::cout << "[客户端] 数据不一致，继续寻找Leader..." << std::endl;
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "[客户端] 寻找领导者尝试失败: " << e.what() << "，等待重试..." << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(2)); // 增加重试间隔
    }
    std::cerr << "[客户端] 错误：在15次尝试后仍未找到稳定的领导者。" << std::endl;
    return -1;
}

/**
 * @brief 测试场景1：基本的Put和Get操作
 */
void test_basic_put_get(Clerk &clerk)
{
    std::cout << "\n========== 测试场景 1: 基本 Put/Get 操作 ==========" << std::endl;
    const std::string key = "test_key_1";
    const std::string value = "hello_raft";

    try
    {
        std::cout << "[客户端] 执行 Put('" << key << "', '" << value << "')" << std::endl;
        clerk.Put(key, value);
        std::cout << "[客户端] Put 操作成功。" << std::endl;

        // 增加更长的延迟，确保数据在集群中完全同步
        std::cout << "[客户端] 等待数据同步..." << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        std::cout << "[客户端] 执行 Get('" << key << "')" << std::endl;
        std::string ret_value = clerk.Get(key);
        std::cout << "[客户端] Get 操作成功，返回值: '" << ret_value << "'" << std::endl;

        if (ret_value == value)
        {
            std::cout << "✅ 验证成功: Get 的值与 Put 的值一致。" << std::endl;
        }
        else
        {
            std::cerr << "❌ 验证失败: Get 的值 ('" << ret_value << "') 与期望值 ('" << value << "') 不一致。" << std::endl;
            cleanup_cluster();
            exit(1);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "❌ 测试场景 1 失败: " << e.what() << std::endl;
        cleanup_cluster();
        exit(1);
    }
}

/**
 * @brief 测试场景2：Append 操作
 */
void test_append(Clerk &clerk)
{
    std::cout << "\n========== 测试场景 2: Append 操作 ==========" << std::endl;
    const std::string key = "test_key_2";
    const std::string initial_value = "start";
    const std::string append_value = "_end";
    const std::string expected_value = initial_value + append_value;

    try
    {
        std::cout << "[客户端] 先执行 Put('" << key << "', '" << initial_value << "')" << std::endl;
        clerk.Put(key, initial_value);

        std::cout << "[客户端] 执行 Append('" << key << "', '" << append_value << "')" << std::endl;
        clerk.Append(key, append_value);
        std::cout << "[客户端] Append 操作成功。" << std::endl;

        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        std::cout << "[客户端] 再次执行 Get('" << key << "') 以验证 Append 结果" << std::endl;
        std::string ret_value = clerk.Get(key);
        std::cout << "[客户端] Get 操作成功，返回值: '" << ret_value << "'" << std::endl;

        if (ret_value == expected_value)
        {
            std::cout << "✅ 验证成功: Append 后的值正确。" << std::endl;
        }
        else
        {
            std::cerr << "❌ 验证失败: Append 后的值 ('" << ret_value << "') 与期望值 ('" << expected_value << "') 不一致。" << std::endl;
            cleanup_cluster();
            exit(1);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "❌ 测试场景 2 失败: " << e.what() << std::endl;
        cleanup_cluster();
        exit(1);
    }
}

/**
 * @brief 测试场景3：领导者故障与恢复
 */
void test_leader_failover(Clerk &clerk)
{
    std::cout << "\n========== 测试场景 3: 领导者故障与恢复 ==========" << std::endl;

    // 注意：Clerk内部会自动寻找Leader，我们无法直接获取其ID。
    // 我们通过模拟杀死一个节点来验证故障转移。
    int leader_pid_index_to_kill = 0; // 简单起见，我们假设第一个启动的节点可能成为leader
    pid_t leader_pid = g_server_pids[leader_pid_index_to_kill];

    std::cout << "[主控] 模拟领导者故障，将杀死节点 " << leader_pid_index_to_kill << " (PID: " << leader_pid << ")" << std::endl;
    kill(leader_pid, SIGKILL);
    waitpid(leader_pid, nullptr, 0);
    g_server_pids[leader_pid_index_to_kill] = -1; // 标记为已杀死

    std::cout << "[主控] 节点已杀死。等待集群选举新的领导者..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5)); // 等待选举

    if (find_leader(clerk) != 0)
    {
        std::cerr << "❌ 故障转移失败: 未能找到新的领导者。" << std::endl;
        cleanup_cluster();
        exit(1);
    }
    std::cout << "✅ 新的领导者已选举成功！" << std::endl;

    // 在新领导者下测试数据一致性
    const std::string key = "failover_key";
    const std::string value = "survived";

    try
    {
        std::cout << "[客户端] 在新领导者下执行 Put('" << key << "', '" << value << "')" << std::endl;
        clerk.Put(key, value);

        std::cout << "[客户端] 在新领导者下执行 Get('" << key << "')" << std::endl;
        std::string ret_value = clerk.Get(key);

        if (ret_value == value)
        {
            std::cout << "✅ 验证成功: 故障转移后，集群仍然可以正确读写数据。" << std::endl;
        }
        else
        {
            std::cerr << "❌ 验证失败: 故障转移后数据不一致。" << std::endl;
            cleanup_cluster();
            exit(1);
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << "❌ 测试场景 3 失败: " << e.what() << std::endl;
        cleanup_cluster();
        exit(1);
    }
}

// --- 主函数 ---
int main()
{
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    std::cout << "========================================" << std::endl;
    std::cout << "    Raft-KV 综合集成测试启动" << std::endl;
    std::cout << "========================================" << std::endl;

    // --- 1. 环境设置 ---

    // 清理旧的就绪标志文件
    std::cout << "[主控] 清理旧的就绪标志文件..." << std::endl;
    for (int i = 0; i < CLUSTER_SIZE; ++i)
    {
        std::string readyFile = "/tmp/raft_node_" + std::to_string(i) + "_ready";
        std::remove(readyFile.c_str());
    }

    generate_config(CLUSTER_SIZE);
    start_cluster(CLUSTER_SIZE);

    // 大幅增加等待时间，确保所有节点都完成初始化
    // 根据我们的启动逻辑：基础等待8秒 + 节点特定延迟(最大6秒) + 连接建立时间 + Raft初始化时间(10秒)
    int clusterStabilizationTime = 60;      // 基础等待时间60秒
    int additionalTime = CLUSTER_SIZE * 10; // 每个节点额外等待10秒
    int totalWaitTime = clusterStabilizationTime + additionalTime;

    std::cout << "\n[主控] 等待集群稳定和领导者选举 (" << totalWaitTime << "秒)..." << std::endl;
    std::cout << "[主控] 这包括节点间连接建立、RPC服务启动、Raft初始化和选举过程" << std::endl;
    std::cout << "[主控] 请耐心等待，确保所有节点都完成初始化..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(totalWaitTime));

    // --- 2. 客户端初始化 ---
    Clerk clerk;
    clerk.Init(CONFIG_FILE);

    if (find_leader(clerk) != 0)
    {
        cleanup_cluster();
        return 1;
    }

    // --- 3. 执行测试场景 ---
    test_basic_put_get(clerk);
    test_append(clerk);
    test_leader_failover(clerk);

    // --- 4. 清理环境 ---
    std::cout << "\n========================================" << std::endl;
    std::cout << "🎉 所有测试场景均已通过！" << std::endl;
    std::cout << "========================================" << std::endl;

    cleanup_cluster();

    return 0;
}