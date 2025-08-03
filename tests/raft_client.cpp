#include <iostream>
#include <string>
#include <signal.h>
#include "raft-kv/raftClerk/clerk.h"
#include "raft-kv/common/util.h"

int main()
{
    signal(SIGPIPE, SIG_IGN);

    Clerk client;
    const char *configFile = "test.conf"; // 客户端也需要配置文件来找到节点

    std::cout << "[Client] Initializing with config: " << configFile << std::endl;
    client.Init(configFile);

    std::cout << "\n--- Test 1: Put a value ---" << std::endl;
    std::string key = "carl";
    std::string value = "code-follower";
    std::cout << "[Client] Putting: Key='" << key << "', Value='" << value << "'" << std::endl;
    client.Put(key, value);
    std::cout << "[Client] Put operation sent." << std::endl;

    // 等待Raft日志同步
    sleep(2);

    std::cout << "\n--- Test 2: Get the value ---" << std::endl;
    std::cout << "[Client] Getting: Key='" << key << "'" << std::endl;
    std::string retrieved_value = client.Get(key);
    std::cout << "[Client] Got: Value='" << retrieved_value << "'" << std::endl;

    if (retrieved_value == value)
    {
        std::cout << "\n[SUCCESS] The retrieved value matches the put value!" << std::endl;
    }
    else
    {
        std::cerr << "\n[FAILURE] The retrieved value does NOT match!" << std::endl;
    }

    std::cout << "\n--- Test 3: Append a value ---" << std::endl;
    std::string append_value = "-handsome";
    std::cout << "[Client] Appending: Key='" << key << "', Value='" << append_value << "'" << std::endl;
    client.Append(key, append_value);
    std::cout << "[Client] Append operation sent." << std::endl;

    sleep(2);

    std::cout << "\n--- Test 4: Get the appended value ---" << std::endl;
    std::cout << "[Client] Getting: Key='" << key << "'" << std::endl;
    retrieved_value = client.Get(key);
    std::cout << "[Client] Got: Value='" << retrieved_value << "'" << std::endl;

    if (retrieved_value == (value + append_value))
    {
        std::cout << "\n[SUCCESS] The appended value is correct!" << std::endl;
    }
    else
    {
        std::cerr << "\n[FAILURE] The appended value is incorrect!" << std::endl;
    }

    return 0;
}