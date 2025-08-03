#include <iostream>
#include "raft-kv/rpc/mprpcchannel.h"
#include "raft-kv/rpc/mprpccontroller.h"
#include "raft-kv/rpc/friend.pb.h"

int main(int argc, char **argv)
{
    std::string ip = "127.0.1.1";
    short port = 7788;

    // 1. 创建一个 RPC 通道
    MprpcChannel channel(ip, port, true);

    // 2. 使用通道创建一个 Stub (服务存根)
    fixbug::FriendServiceRpc_Stub stub(&channel);

    // 3. 准备请求和响应对象
    fixbug::GetFriendsListRequest request;
    request.set_userid(12345);
    fixbug::GetFriendsListResponse response;

    // 4. 创建控制器
    MprpcController controller;

    std::cout << "[Client] Sending RPC request to " << ip << ":" << port << std::endl;

    // 5. 发起同步 RPC 调用
    stub.GetFriendsList(&controller, &request, &response, nullptr);

    // 6. 检查结果
    if (controller.Failed())
    {
        std::cerr << "[Client] RPC call failed: " << controller.ErrorText() << std::endl;
    }
    else
    {
        if (response.result().errcode() == 0)
        {
            std::cout << "[Client] RPC call success!" << std::endl;
            std::cout << "Friends list:" << std::endl;
            for (int i = 0; i < response.friends_size(); ++i)
            {
                std::cout << "  - " << response.friends(i) << std::endl;
            }
        }
        else
        {
            std::cerr << "[Client] Business logic error: " << response.result().errmsg() << std::endl;
        }
    }

    return 0;
}