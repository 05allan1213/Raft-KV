#include <iostream>
#include "raft-kv/rpc/mprpcchannel.h"
#include "echo.pb.h"
#include <google/protobuf/service.h>

int main()
{
    // 创建一个 RPC 通道，并指定要连接的服务器 IP 和端口
    // 提供了三个参数：IP, 端口, 是否立即连接（通常为 true）
    MprpcChannel channel("127.0.1.1", 8888, true);

    // 使用通道创建一个 stub (服务存根)
    echo::EchoService_Stub stub(&channel);

    // 准备请求参数
    echo::EchoRequest request;
    request.set_message("Hello, RPC!");

    // 准备接收响应
    echo::EchoResponse response;

    std::cout << "Sending RPC request..." << std::endl;

    // 发起 RPC 调用，同步等待结果
    stub.Echo(nullptr, &request, &response, nullptr);

    // 检查结果
    if (response.response().empty())
    {
        std::cout << "RPC call failed!" << std::endl;
    }
    else
    {
        std::cout << "Client received: " << response.response() << std::endl;
    }

    return 0;
}