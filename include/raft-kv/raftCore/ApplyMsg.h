#pragma once
#include <string>

/**
 * @brief 应用消息类
 * 
 * 该类用于在Raft节点和上层应用（如KV服务器）之间传递消息，
 * 包括命令消息和快照消息两种类型。
 * 
 * 当Raft节点提交一个日志条目时，会创建一个ApplyMsg对象，
 * 并通过applyCh通道发送给上层应用进行处理。
 */
class ApplyMsg
{
public:
  bool CommandValid; // 命令是否有效
  std::string Command; // 命令内容
  int CommandIndex; // 命令索引
  bool SnapshotValid; // 快照是否有效
  std::string Snapshot; // 快照数据
  int SnapshotTerm; // 快照任期
  int SnapshotIndex; // 快照索引

public:
  /**
   * @brief 默认构造函数
   * 
   * 两个valid最开始要赋予false！
   * 初始化所有成员变量为默认值
   */
  ApplyMsg()
      : CommandValid(false),
        Command(),
        CommandIndex(-1),
        SnapshotValid(false),
        SnapshotTerm(-1),
        SnapshotIndex(-1) {

        };
};
