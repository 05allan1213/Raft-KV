#pragma once

#include <boost/any.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/foreach.hpp>
#include <boost/serialization/export.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/unordered_map.hpp>
#include <boost/serialization/vector.hpp>
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <memory>
#include "kvServerRPC.pb.h"
#include "raft-kv/raftCore/raft.h"
#include "raft-kv/skipList/skipList.h"

/**
 * @brief KV存储服务器类
 *
 * 该类实现了基于Raft共识算法的键值存储服务器，提供：
 * - 键值对的存储和检索
 * - 基于Raft的强一致性保证
 * - 快照机制支持
 * - 重复请求检测
 *
 * 服务器通过Raft算法确保数据的一致性和可用性，
 * 支持分布式部署和故障恢复。
 */
class KvServer : raftKVRpcProctoc::kvServerRpc
{
public:
  /**
   * @brief 删除默认构造函数
   */
  KvServer() = delete;

  /**
   * @brief 构造函数
   * @param me 服务器ID
   * @param maxraftstate 最大Raft状态大小
   * @param nodeInforFileName 节点信息文件名
   * @param port 服务端口
   */
  KvServer(int me, int maxraftstate, std::string nodeInforFileName, short port);

  /**
   * @brief 启动KV服务器
   *
   * 初始化并启动KV服务器，包括Raft节点和RPC服务
   */
  void StartKVServer();

  /**
   * @brief 打印KV数据库内容
   *
   * 用于调试，打印当前KV数据库中的所有键值对
   */
  void DprintfKVDB();

  /**
   * @brief 在KV数据库上执行Append操作
   * @param op 操作对象
   */
  void ExecuteAppendOpOnKVDB(Op op);

  /**
   * @brief 在KV数据库上执行Get操作
   * @param op 操作对象
   * @param value 返回的值
   * @param exist 键是否存在
   */
  void ExecuteGetOpOnKVDB(Op op, std::string *value, bool *exist);

  /**
   * @brief 在KV数据库上执行Put操作
   * @param op 操作对象
   */
  void ExecutePutOpOnKVDB(Op op);

  /**
   * @brief 处理Get请求
   * @param args Get请求参数
   * @param reply Get响应结果
   *
   * 将GetArgs改为rpc调用的，因为是远程客户端，即服务器宕机对客户端来说是无感的
   */
  void Get(const raftKVRpcProctoc::GetArgs *args,
           raftKVRpcProctoc::GetReply *reply);

  /**
   * @brief 从raft节点中获取消息
   * @param message 应用消息
   *
   * 注意：不要误以为是执行【GET】命令，这是从Raft获取应用消息
   */
  void GetCommandFromRaft(ApplyMsg message);

  /**
   * @brief 检查请求是否重复
   * @param ClientId 客户端ID
   * @param RequestId 请求ID
   * @return 是否为重复请求
   */
  bool ifRequestDuplicate(std::string ClientId, int RequestId);

  /**
   * @brief 检查当前节点是否为领导者
   * @return 是否为领导者
   */
  bool isLeader();

  /**
   * @brief 处理Put/Append请求
   * @param args Put/Append请求参数
   * @param reply Put/Append响应结果
   *
   * clerk使用RPC远程调用
   */
  void PutAppend(const raftKVRpcProctoc::PutAppendArgs *args, raftKVRpcProctoc::PutAppendReply *reply);

  /**
   * @brief 持续等待raft传来的applyCh
   *
   * 该函数会一直运行，监听来自Raft的应用消息
   */
  void ReadRaftApplyCommandLoop();

  /**
   * @brief 读取快照并安装
   * @param snapshot 快照数据
   */
  void ReadSnapShotToInstall(std::string snapshot);

  /**
   * @brief 发送消息到等待通道
   * @param op 操作对象
   * @param raftIndex Raft索引
   * @return 是否发送成功
   */
  bool SendMessageToWaitChan(const Op &op, int raftIndex);

  /**
   * @brief 检查是否需要制作快照
   * @param raftIndex Raft索引
   * @param proportion 比例
   *
   * 检查是否需要制作快照，需要的话就向raft发送制作快照命令
   */
  void IfNeedToSendSnapShotCommand(int raftIndex, int proportion);

  /**
   * @brief 处理来自kv.rf.applyCh的快照
   * @param message 应用消息
   */
  void GetSnapShotFromRaft(ApplyMsg message);

  /**
   * @brief 制作快照
   * @return 快照数据字符串
   */
  std::string MakeSnapShot();

public: // for rpc
  /**
   * @brief RPC接口：处理PutAppend请求
   */
  void PutAppend(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::PutAppendArgs *request,
                 ::raftKVRpcProctoc::PutAppendReply *response, ::google::protobuf::Closure *done) override;

  /**
   * @brief RPC接口：处理Get请求
   */
  void Get(google::protobuf::RpcController *controller, const ::raftKVRpcProctoc::GetArgs *request,
           ::raftKVRpcProctoc::GetReply *response, ::google::protobuf::Closure *done) override;

private:
  friend class boost::serialization::access;

  /**
   * @brief 序列化/反序列化函数
   * @param ar 归档对象
   * @param version 版本号
   *
   * 这里面写需要序列化和反序列化的字段
   */
  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)
  {
    ar & m_serializedKVData;
    ar & m_lastRequestId;
  }

  /**
   * @brief 获取快照数据
   * @return 序列化的快照数据
   */
  std::string getSnapshotData()
  {
    m_serializedKVData = m_skipList.dump_file();
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << *this;
    m_serializedKVData.clear();
    return ss.str();
  }

  /**
   * @brief 从字符串解析数据
   * @param str 序列化的字符串
   */
  void parseFromString(const std::string &str)
  {
    std::stringstream ss(str);
    boost::archive::text_iarchive ia(ss);
    ia >> *this;
    m_skipList.load_file(m_serializedKVData);
    m_serializedKVData.clear();
  }

private:
  std::mutex m_mtx;                               // 互斥锁，保护共享数据
  int m_me;                                       // 服务器ID
  std::shared_ptr<Raft> m_raftNode;               // Raft节点
  std::shared_ptr<LockQueue<ApplyMsg>> applyChan; // kvServer和raft节点的通信管道
  int m_maxRaftState;                             // 快照阈值，如果日志增长超过这个大小就制作快照

  // 数据存储相关
  std::string m_serializedKVData;                      // 序列化后的kv数据，理论上可以不用，但是目前没有找到特别好的替代方法
  SkipList<std::string, std::string> m_skipList;       // 跳表，用于存储键值对
  std::unordered_map<std::string, std::string> m_kvDB; // 哈希表，用于存储键值对

  std::unordered_map<int, LockQueue<Op> *> waitApplyCh; // index(raft) -> chan，等待应用的操作通道映射

  std::unordered_map<std::string, int> m_lastRequestId; // clientid -> requestID，一个kV服务器可能连接多个client

  // 快照相关
  int m_lastSnapShotRaftLogIndex; // 最后一个快照点的Raft日志索引
};
