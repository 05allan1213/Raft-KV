#include <iostream>
#include "raft-kv/rpc/rpcprovider.h"
#include "echo.pb.h"
#include <google/protobuf/service.h>

// 继承自 protobuf 生成的服务类
class EchoServiceImpl : public echo::EchoService
{
public:
    // 重写 Echo 方法
    virtual void Echo(google::protobuf::RpcController *controller,
                      const echo::EchoRequest *request,
                      echo::EchoResponse *response,
                      google::protobuf::Closure *done)
    {

        std::string msg = request->message();
        std::cout << "Server received: " << msg << std::endl;

        // 简单地将收到的消息加上 "echo: " 前缀返回
        response->set_response("echo from server: " + msg);

        // done->Run() 用于通知 RPC 框架，方法执行完毕
        if (done)
        {
            done->Run();
        }
    }
};

int main()
{
    // 创建一个 RpcProvider
    RpcProvider provider;

    // 注册我们自己实现的服务
    provider.NotifyService(new EchoServiceImpl());

    // 启动服务，监听在所有网络接口的 8888 端口上
    // 提供了两个参数：节点索引（这里用 0 作为示例）和端口号
    std::cout << "Starting RPC server on port 8888..." << std::endl;
    provider.Run(0, 8888);

    return 0;
}