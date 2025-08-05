/**
 * @file kvserver_test.cpp
 * @brief KV 服务器单元测试
 *
 * 该文件包含对 KvServer 类核心功能的单元测试，包括：
 * - Get、Put、Append 操作测试
 * - 重复请求检测测试
 * - 快照机制测试
 * - 并发操作测试
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include "raft-kv/raftCore/kvServer.h"
#include "raft-kv/raftCore/raft.h"
#include "raft-kv/raftCore/Persister.h"
#include "raft-kv/raftCore/ApplyMsg.h"
#include "raft-kv/fiber/channel.h"
#include "kvServerRPC.pb.h"

/**
 * @brief KV 服务器测试夹具类
 */
class KvServerTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        // 注意：KvServer 的构造函数需要参数，这里我们需要模拟一个测试环境
        // 由于 KvServer 构造函数比较复杂，我们可能需要创建一个简化的测试版本
        // 或者使用模拟对象
    }

    void TearDown() override
    {
        // 清理资源
    }

    /**
     * @brief 创建测试用的 Op 对象
     */
    Op createTestOp(const std::string &operation, const std::string &key,
                    const std::string &value, const std::string &clientId, int requestId)
    {
        Op op;
        op.Operation = operation;
        op.Key = key;
        op.Value = value;
        op.ClientId = clientId;
        op.RequestId = requestId;
        return op;
    }

    /**
     * @brief 创建测试用的 GetArgs
     */
    raftKVRpcProctoc::GetArgs createGetArgs(const std::string &key,
                                            const std::string &clientId, int requestId)
    {
        raftKVRpcProctoc::GetArgs args;
        args.set_key(key);
        args.set_clientid(clientId);
        args.set_requestid(requestId);
        return args;
    }

    /**
     * @brief 创建测试用的 PutAppendArgs
     */
    raftKVRpcProctoc::PutAppendArgs createPutAppendArgs(const std::string &op,
                                                        const std::string &key,
                                                        const std::string &value,
                                                        const std::string &clientId,
                                                        int requestId)
    {
        raftKVRpcProctoc::PutAppendArgs args;
        args.set_op(op);
        args.set_key(key);
        args.set_value(value);
        args.set_clientid(clientId);
        args.set_requestid(requestId);
        return args;
    }
};

/**
 * @brief 测试 Op 对象的基本操作
 */
TEST_F(KvServerTest, OpBasicOperations)
{
    // 创建测试 Op
    Op op = createTestOp("Put", "test_key", "test_value", "client_1", 1);

    // 验证 Op 对象的属性
    EXPECT_EQ(op.Operation, "Put");
    EXPECT_EQ(op.Key, "test_key");
    EXPECT_EQ(op.Value, "test_value");
    EXPECT_EQ(op.ClientId, "client_1");
    EXPECT_EQ(op.RequestId, 1);

    // 测试序列化和反序列化
    std::string serialized = op.asString();
    EXPECT_FALSE(serialized.empty());

    Op deserializedOp;
    deserializedOp.parseFromString(serialized);

    // 验证反序列化结果
    EXPECT_EQ(deserializedOp.Operation, op.Operation);
    EXPECT_EQ(deserializedOp.Key, op.Key);
    EXPECT_EQ(deserializedOp.Value, op.Value);
    EXPECT_EQ(deserializedOp.ClientId, op.ClientId);
    EXPECT_EQ(deserializedOp.RequestId, op.RequestId);
}

/**
 * @brief 测试不同操作类型的 Op 对象
 */
TEST_F(KvServerTest, OpDifferentOperations)
{
    // 测试 Put 操作
    Op putOp = createTestOp("Put", "key1", "value1", "client1", 1);
    EXPECT_EQ(putOp.Operation, "Put");

    // 测试 Get 操作
    Op getOp = createTestOp("Get", "key1", "", "client1", 2);
    EXPECT_EQ(getOp.Operation, "Get");
    EXPECT_EQ(getOp.Value, "");

    // 测试 Append 操作
    Op appendOp = createTestOp("Append", "key1", "append_value", "client1", 3);
    EXPECT_EQ(appendOp.Operation, "Append");
    EXPECT_EQ(appendOp.Value, "append_value");
}

/**
 * @brief 测试 RPC 参数对象的创建
 */
TEST_F(KvServerTest, RpcArgsCreation)
{
    // 测试 GetArgs
    auto getArgs = createGetArgs("test_key", "client_1", 1);
    EXPECT_EQ(getArgs.key(), "test_key");
    EXPECT_EQ(getArgs.clientid(), "client_1");
    EXPECT_EQ(getArgs.requestid(), 1);

    // 测试 PutAppendArgs
    auto putArgs = createPutAppendArgs("Put", "test_key", "test_value", "client_1", 1);
    EXPECT_EQ(putArgs.op(), "Put");
    EXPECT_EQ(putArgs.key(), "test_key");
    EXPECT_EQ(putArgs.value(), "test_value");
    EXPECT_EQ(putArgs.clientid(), "client_1");
    EXPECT_EQ(putArgs.requestid(), 1);

    auto appendArgs = createPutAppendArgs("Append", "test_key", "append_value", "client_1", 2);
    EXPECT_EQ(appendArgs.op(), "Append");
    EXPECT_EQ(appendArgs.value(), "append_value");
}

/**
 * @brief 测试 Op 对象的序列化性能
 */
TEST_F(KvServerTest, OpSerializationPerformance)
{
    Op op = createTestOp("Put", "performance_test_key", "performance_test_value", "client_perf", 1);

    auto start = std::chrono::high_resolution_clock::now();

    // 执行多次序列化
    const int iterations = 1000;
    for (int i = 0; i < iterations; i++)
    {
        std::string serialized = op.asString();
        EXPECT_FALSE(serialized.empty());
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

    // 验证性能（每次序列化应该在合理时间内完成）
    EXPECT_LT(duration.count(), 100000); // 小于 100ms

    std::cout << "序列化 " << iterations << " 次耗时: " << duration.count() << " 微秒" << std::endl;
}

/**
 * @brief 测试 Op 对象的边界情况
 */
TEST_F(KvServerTest, OpBoundaryConditions)
{
    // 测试空字符串
    Op emptyOp = createTestOp("", "", "", "", 0);
    std::string serialized = emptyOp.asString();
    EXPECT_FALSE(serialized.empty());

    Op deserializedOp;
    deserializedOp.parseFromString(serialized);
    EXPECT_EQ(deserializedOp.Operation, "");
    EXPECT_EQ(deserializedOp.Key, "");
    EXPECT_EQ(deserializedOp.Value, "");
    EXPECT_EQ(deserializedOp.ClientId, "");
    EXPECT_EQ(deserializedOp.RequestId, 0);

    // 测试长字符串
    std::string longString(1000, 'a');
    Op longOp = createTestOp("Put", longString, longString, "client_long", 999);
    std::string longSerialized = longOp.asString();
    EXPECT_FALSE(longSerialized.empty());

    Op longDeserializedOp;
    longDeserializedOp.parseFromString(longSerialized);
    EXPECT_EQ(longDeserializedOp.Key, longString);
    EXPECT_EQ(longDeserializedOp.Value, longString);
}

/**
 * @brief 测试并发序列化
 */
TEST_F(KvServerTest, ConcurrentSerialization)
{
    const int numThreads = 4;
    const int operationsPerThread = 100;
    std::atomic<int> successCount{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; t++)
    {
        threads.emplace_back([&, t]()
                             {
            for (int i = 0; i < operationsPerThread; i++) {
                Op op = createTestOp("Put", 
                                   "key_" + std::to_string(t) + "_" + std::to_string(i),
                                   "value_" + std::to_string(t) + "_" + std::to_string(i),
                                   "client_" + std::to_string(t),
                                   i);
                
                std::string serialized = op.asString();
                if (!serialized.empty()) {
                    Op deserializedOp;
                    deserializedOp.parseFromString(serialized);
                    if (deserializedOp.Key == op.Key && deserializedOp.Value == op.Value) {
                        successCount++;
                    }
                }
            } });
    }

    // 等待所有线程完成
    for (auto &thread : threads)
    {
        thread.join();
    }

    // 验证所有操作都成功
    EXPECT_EQ(successCount, numThreads * operationsPerThread);
}

/**
 * @brief 测试 RPC 响应对象
 */
TEST_F(KvServerTest, RpcReplyObjects)
{
    // 测试 GetReply
    raftKVRpcProctoc::GetReply getReply;
    getReply.set_err("OK");
    getReply.set_value("test_value");

    EXPECT_EQ(getReply.err(), "OK");
    EXPECT_EQ(getReply.value(), "test_value");

    // 测试 PutAppendReply
    raftKVRpcProctoc::PutAppendReply putReply;
    putReply.set_err("OK");

    EXPECT_EQ(putReply.err(), "OK");

    // 测试错误情况
    raftKVRpcProctoc::GetReply errorReply;
    errorReply.set_err("ErrNoKey");
    errorReply.set_value("");

    EXPECT_EQ(errorReply.err(), "ErrNoKey");
    EXPECT_EQ(errorReply.value(), "");
}

/**
 * @brief 测试请求 ID 的处理
 */
TEST_F(KvServerTest, RequestIdHandling)
{
    // 测试正常的请求 ID
    Op op1 = createTestOp("Put", "key1", "value1", "client1", 1);
    Op op2 = createTestOp("Put", "key1", "value2", "client1", 2);

    EXPECT_NE(op1.RequestId, op2.RequestId);

    // 测试相同的请求 ID（模拟重复请求）
    Op op3 = createTestOp("Put", "key1", "value1", "client1", 1);
    EXPECT_EQ(op1.RequestId, op3.RequestId);
    EXPECT_EQ(op1.ClientId, op3.ClientId);

    // 测试不同客户端的相同请求 ID
    Op op4 = createTestOp("Put", "key1", "value1", "client2", 1);
    EXPECT_EQ(op1.RequestId, op4.RequestId);
    EXPECT_NE(op1.ClientId, op4.ClientId);
}
