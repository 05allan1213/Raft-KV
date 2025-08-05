#pragma once
#include <fstream>
#include <mutex>

/**
 * @brief 持久化存储类
 *
 * 该类负责Raft节点的状态持久化，包括：
 * - Raft状态数据的保存和读取
 * - 快照数据的保存和读取
 * - 文件流的管理
 *
 * 通过持久化机制，Raft节点可以在重启后恢复之前的状态，
 * 确保系统的可靠性和一致性。
 */
class Persister
{
private:
  std::mutex m_mtx;        // 互斥锁，保护共享数据
  std::string m_raftState; // Raft状态数据
  std::string m_snapshot;  // 快照数据

  const std::string m_raftStateFileName;         // Raft状态文件名
  const std::string m_snapshotFileName;          // 快照文件名
  const std::string m_streamingSnapshotFileName; // 流式快照文件名

  std::ofstream m_raftStateOutStream; // 保存Raft状态的输出流
  std::ofstream m_snapshotOutStream;  // 保存快照的输出流

  long long m_raftStateSize; // 保存Raft状态大小，避免每次都读取文件来获取具体的大小

public:
  /**
   * @brief 保存Raft状态和快照
   * @param raftstate Raft状态数据，包含日志、任期等信息
   * @param snapshot 快照数据，包含状态机的快照
   *
   * 将Raft状态数据和快照数据同时保存到本地文件中
   */
  void Save(std::string raftstate, std::string snapshot);

  /**
   * @brief 读取快照数据
   * @return 快照数据字符串
   *
   * 从快照文件中读取之前保存的快照数据
   */
  std::string ReadSnapshot();

  /**
   * @brief 保存Raft状态
   * @param data Raft状态数据
   *
   * 将Raft状态数据保存到本地文件，并更新状态大小
   */
  void SaveRaftState(const std::string &data);

  /**
   * @brief 获取Raft状态大小
   * @return Raft状态的大小（字节数）
   *
   * 返回当前保存的状态数据的大小
   */
  long long RaftStateSize();

  /**
   * @brief 读取Raft状态
   * @return Raft状态数据字符串
   *
   * 从状态文件中读取之前保存的Raft状态数据
   */
  std::string ReadRaftState();

  /**
   * @brief 保存流式快照
   * @param snapshotFilePath 快照文件路径
   * @return 是否保存成功
   *
   * 将临时快照文件复制到持久化位置
   */
  bool SaveStreamingSnapshot(const std::string &snapshotFilePath);

  /**
   * @brief 读取流式快照
   * @return 快照文件路径，失败时返回空字符串
   *
   * 检查流式快照文件是否存在并返回文件路径
   */
  std::string ReadStreamingSnapshot();

  /**
   * @brief 构造函数
   * @param me 节点ID，用于生成唯一的文件名
   *
   * 初始化持久化器，创建对应的文件并打开输出流
   */
  explicit Persister(int me);

  /**
   * @brief 析构函数
   *
   * 清理资源，关闭文件流
   */
  ~Persister();

private:
  /**
   * @brief 清理Raft状态
   *
   * 重置状态大小并清空状态文件内容
   */
  void clearRaftState();

  /**
   * @brief 清理快照
   *
   * 清空快照文件内容，为新的快照数据做准备
   */
  void clearSnapshot();

  /**
   * @brief 清理Raft状态和快照
   *
   * 同时清空状态文件和快照文件，用于完全重置持久化数据
   */
  void clearRaftStateAndSnapshot();
};
