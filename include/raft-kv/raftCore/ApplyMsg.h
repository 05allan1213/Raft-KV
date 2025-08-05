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
 * 上层应用根据消息类型执行相应的操作。
 */
class ApplyMsg
{
public:
  bool CommandValid;    // 命令是否有效，标识这是一个命令消息
  std::string Command;  // 命令内容，序列化的命令数据
  int CommandIndex;     // 命令索引，对应日志条目的索引
  bool SnapshotValid;   // 快照是否有效，标识这是一个快照消息
  std::string Snapshot; // 快照数据，序列化的快照内容
  int SnapshotTerm;     // 快照任期，快照创建时的任期号
  int SnapshotIndex;    // 快照索引，快照对应的日志索引

public:
  /**
   * @brief 默认构造函数
   *
   * 两个valid最开始要赋予false！
   * 初始化所有成员变量为默认值，确保消息状态的一致性
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
