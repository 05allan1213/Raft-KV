#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <memory>
#include <fstream>
#include <unistd.h> // for pause()

// 包含所有需要的头文件
#include "raft-kv/raftCore/kvServer.h"
#include "raft-kv/common/util.h"
#include "kvServerRPC.pb.h" // 包含 Get/Put 的 RPC 定义

// 函数：找到 Leader 节点
// 修正：调用公有的 isLeader() 方法
int findLeader(const std::vector<std::shared_ptr<KvServer>> &cluster)
{
    for (int i = 0; i < cluster.size(); ++i)
    {
        if (cluster[i]->isLeader())
        {
            return i;
        }
    }
    return -1;
}

int main()
{
    // 1. 设置集群
    const int clusterSize = 3;
    const int maxRaftState = -1;
    const std::string configFile = "test.conf";
    short basePort = 8000;

    // 清理并准备配置文件
    std::ofstream conf_file_writer(configFile, std::ios::trunc);
    conf_file_writer.close();

    std::vector<std::shared_ptr<KvServer>> cluster;

    std::cout << "--- Creating a cluster of " << clusterSize << " nodes ---" << std::endl;

    // 2. 创建所有节点
    for (int i = 0; i < clusterSize; ++i)
    {
        auto kv = std::make_shared<KvServer>(i, maxRaftState, configFile, basePort + i);
        cluster.push_back(kv);
        // KvServer 在构造时已启动内部线程，无需在此处创建
    }

    std::cout << "--- Waiting for leader election ---" << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    // 3. 检查 Leader
    int leaderId = findLeader(cluster);
    if (leaderId == -1)
    {
        std::cerr << "[Error] No leader elected after 5 seconds!" << std::endl;
    }
    else
    {
        std::cout << "[Success] Node " << leaderId << " is the leader." << std::endl;
    }

    // 4. 测试数据复制
    if (leaderId != -1)
    {
        std::cout << "--- Testing data replication (Put) ---" << std::endl;

        raftKVRpcProctoc::PutAppendArgs putArgs;
        raftKVRpcProctoc::PutAppendReply putReply;
        putArgs.set_key("name");
        putArgs.set_value("raft-kv");
        putArgs.set_op("Put");
        putArgs.set_clientid("test-client");
        putArgs.set_requestid(1);

        // 修正：对于本地同步调用，最后一个参数（Closure）可以传 nullptr
        cluster[leaderId]->PutAppend(nullptr, &putArgs, &putReply, nullptr);

        // 修正：通过 reply.err() 判断是否成功
        if (putReply.err() == OK)
        {
            std::cout << "--- Command proposed to leader, waiting for replication ---" << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2)); // 等待日志复制

            std::cout << "--- Verifying data on all nodes (Get) ---" << std::endl;
            bool all_synced = true;
            for (int i = 0; i < clusterSize; ++i)
            {
                raftKVRpcProctoc::GetArgs getArgs;
                raftKVRpcProctoc::GetReply getReply;
                getArgs.set_key("name");
                getArgs.set_clientid("test-client-verify");
                getArgs.set_requestid(i + 1);

                // 修正：最后一个参数传 nullptr
                cluster[i]->Get(nullptr, &getArgs, &getReply, nullptr);

                if (getReply.err() == OK && getReply.value() == "raft-kv")
                {
                    std::cout << "Node " << i << " synced. Key: name, Value: " << getReply.value() << std::endl;
                }
                else
                {
                    std::cerr << "[Error] Node " << i << " did not sync or Get failed! Reply: " << getReply.err() << std::endl;
                    all_synced = false;
                }
            }

            if (all_synced)
            {
                std::cout << "[Success] Data replicated to all nodes." << std::endl;
            }
        }
        else
        {
            std::cerr << "[Error] Failed to propose command to leader. Reason: " << putReply.err() << std::endl;
        }
    }

    // 主线程暂停，防止程序直接退出
    std::cout << "--- Test running, press Ctrl+C to exit ---" << std::endl;
    pause();

    return 0;
}