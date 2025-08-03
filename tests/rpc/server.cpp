#include <iostream>
#include <string>
#include <vector>
#include "raft-kv/rpc/rpcprovider.h"
#include "raft-kv/rpc/friend.pb.h"

// 服务类，继承自 Protobuf 生成的 Service 类
class FriendService : public fixbug::FriendServiceRpc
{
public:
    // 实际的本地业务方法
    std::vector<std::string> GetFriendsList(uint32_t userid)
    {
        std::cout << "[Server] Executing local method GetFriendsList for userid: " << userid << std::endl;
        std::vector<std::string> friends;
        friends.push_back("gao yang");
        friends.push_back("liu hong");
        friends.push_back("wang shuo");
        return friends;
    }

    // 重写 Protobuf 生成的虚函数，用于响应 RPC 请求
    void GetFriendsList(::google::protobuf::RpcController *controller,
                        const ::fixbug::GetFriendsListRequest *request,
                        ::fixbug::GetFriendsListResponse *response,
                        ::google::protobuf::Closure *done) override
    {

        // 1. 从 request 中获取参数
        uint32_t userid = request->userid();

        // 2. 调用本地业务
        std::vector<std::string> friendsList = GetFriendsList(userid);

        // 3. 填充 response
        response->mutable_result()->set_errcode(0);
        response->mutable_result()->set_errmsg("");
        for (const std::string &name : friendsList)
        {
            response->add_friends(name);
        }

        // 4. 执行回调，完成 RPC 响应
        if (done)
        {
            done->Run();
        }
    }
};

int main(int argc, char **argv)
{
    // 1. 创建 RpcProvider
    RpcProvider provider;

    // 2. 注册服务
    provider.NotifyService(new FriendService());

    // 3. 启动服务，监听在 127.0.1.1:7788
    // provider.Run(nodeIndex, port)
    provider.Run(1, 7788);

    return 0;
}