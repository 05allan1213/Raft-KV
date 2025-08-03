/**
 * @file raft_test.cpp
 * @brief Raft集群启动器
 * 
 * 该文件实现了一个Raft集群启动器，可以启动指定数量的Raft节点，
 * 用于测试和演示Raft共识算法的功能。
 * 
 * @author Raft-KV Team
 * @date 2024
 */

#include <iostream>
#include <vector>
#include <string>
#include <fstream>
#include <unistd.h>
#include <random>
#include "raft-kv/raftCore/kvServer.h"

/**
 * @brief 显示程序使用说明
 * 
 * 当用户输入参数不正确时，显示正确的命令行参数格式。
 */
void show_usage()
{
    std::cerr << "Usage: ./raft_cluster_launcher -n <number_of_nodes> -f <config_file_name>" << std::endl;
}

/**
 * @brief 主函数 - Raft集群启动器
 * 
 * 该函数解析命令行参数，启动指定数量的Raft节点，
 * 每个节点运行在独立的进程中。
 * 
 * @param argc 命令行参数数量
 * @param argv 命令行参数数组
 * @return 程序退出码
 */
int main(int argc, char **argv)
{
    // 检查命令行参数数量
    if (argc < 5)
    {
        show_usage();
        return 1;
    }

    int nodeNum = 0;
    std::string configFileName;
    int opt;
    
    // 解析命令行参数
    while ((opt = getopt(argc, argv, "n:f:")) != -1)
    {
        switch (opt)
        {
        case 'n':
            nodeNum = std::atoi(optarg);
            break;
        case 'f':
            configFileName = optarg;
            break;
        default:
            show_usage();
            return 1;
        }
    }

    // 验证参数有效性
    if (nodeNum <= 0 || configFileName.empty())
    {
        show_usage();
        return 1;
    }

    // 清空配置文件，准备写入新的节点信息
    std::ofstream ofs(configFileName, std::ios::trunc);
    ofs.close();

    // 随机选择一个起始端口，避免端口冲突
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(10000, 30000);
    short basePort = distrib(gen);

    std::cout << "--- Starting a " << nodeNum << "-node Raft cluster ---" << std::endl;
    std::cout << "Config file: " << configFileName << ", Base port: " << basePort << std::endl;

    // 启动指定数量的Raft节点
    for (int i = 0; i < nodeNum; ++i)
    {
        pid_t pid = fork();
        if (pid == 0)
        { 
            // 子进程：启动单个Raft节点
            short port = basePort + i;
            std::cout << "[Launcher] Starting Node " << i << " on port " << port << " (PID: " << getpid() << ")" << std::endl;
            
            // 每个子进程创建一个KvServer实例并永久运行
            KvServer kv(i, -1, configFileName, port);
            pause(); // 阻塞子进程，防止其退出
            exit(0);
        }
        else if (pid < 0)
        {
            std::cerr << "Failed to fork process for node " << i << std::endl;
            // 在此可能需要杀死已经创建的子进程
            return 1;
        }
        
        // 父进程继续循环创建下一个节点
        sleep(1); // 稍微延迟，确保端口写入配置文件
    }

    std::cout << "--- All cluster nodes launched. Parent process will wait. (PID: " << getpid() << ") ---" << std::endl;
    // 父进程也阻塞，以便观察或手动终止
    pause();

    return 0;
}