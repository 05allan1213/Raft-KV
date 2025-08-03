#pragma once

#include <fstream>
#include <memory>
#include <string>
#include <functional>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/unordered_map.hpp>
#include "raft-kv/skipList/skipList.h"

/**
 * @brief 流式快照写入器
 * 
 * 该类负责将跳表数据流式地写入到临时文件中，避免一次性将所有数据加载到内存。
 * 支持大数据量的快照生成，内存使用量保持在常数级别。
 */
class StreamingSnapshotWriter
{
public:
  /**
   * @brief 构造函数
   * @param tempFilePath 临时文件路径
   */
  explicit StreamingSnapshotWriter(const std::string& tempFilePath);

  /**
   * @brief 析构函数
   * 
   * 自动清理临时文件和资源
   */
  ~StreamingSnapshotWriter();

  /**
   * @brief 开始写入快照
   * @return 是否成功开始
   */
  bool BeginSnapshot();

  /**
   * @brief 写入键值对
   * @param key 键
   * @param value 值
   * @return 是否写入成功
   */
  bool WriteKeyValue(const std::string& key, const std::string& value);

  /**
   * @brief 写入客户端请求ID映射
   * @param lastRequestId 客户端请求ID映射
   * @return 是否写入成功
   */
  bool WriteLastRequestId(const std::unordered_map<std::string, int>& lastRequestId);

  /**
   * @brief 完成快照写入
   * @return 是否成功完成
   */
  bool EndSnapshot();

  /**
   * @brief 获取临时文件路径
   * @return 临时文件路径
   */
  const std::string& GetTempFilePath() const { return m_tempFilePath; }

  /**
   * @brief 获取写入的数据大小
   * @return 数据大小（字节）
   */
  size_t GetDataSize() const { return m_dataSize; }

private:
  std::string m_tempFilePath;           // 临时文件路径
  std::unique_ptr<std::ofstream> m_file; // 文件输出流
  std::unique_ptr<boost::archive::text_oarchive> m_archive; // boost序列化归档
  size_t m_dataSize;                    // 已写入的数据大小
  bool m_isOpen;                        // 是否已打开
  
  /**
   * @brief 清理资源
   */
  void cleanup();
};

/**
 * @brief 流式快照读取器
 * 
 * 该类负责从快照文件中流式地读取数据，避免一次性将所有数据加载到内存。
 */
class StreamingSnapshotReader
{
public:
  /**
   * @brief 构造函数
   * @param snapshotFilePath 快照文件路径
   */
  explicit StreamingSnapshotReader(const std::string& snapshotFilePath);

  /**
   * @brief 析构函数
   */
  ~StreamingSnapshotReader();

  /**
   * @brief 开始读取快照
   * @return 是否成功开始
   */
  bool BeginSnapshot();

  /**
   * @brief 读取键值对
   * @param key 输出参数：键
   * @param value 输出参数：值
   * @return 是否成功读取（false表示已到文件末尾）
   */
  bool ReadKeyValue(std::string& key, std::string& value);

  /**
   * @brief 读取客户端请求ID映射
   * @param lastRequestId 输出参数：客户端请求ID映射
   * @return 是否读取成功
   */
  bool ReadLastRequestId(std::unordered_map<std::string, int>& lastRequestId);

  /**
   * @brief 完成快照读取
   * @return 是否成功完成
   */
  bool EndSnapshot();

private:
  std::string m_snapshotFilePath;       // 快照文件路径
  std::unique_ptr<std::ifstream> m_file; // 文件输入流
  std::unique_ptr<boost::archive::text_iarchive> m_archive; // boost序列化归档
  bool m_isOpen;                        // 是否已打开
  
  /**
   * @brief 清理资源
   */
  void cleanup();
};

/**
 * @brief 流式快照管理器
 * 
 * 该类提供高级接口，用于管理流式快照的创建和恢复。
 */
class StreamingSnapshotManager
{
public:
  /**
   * @brief 构造函数
   * @param nodeId 节点ID，用于生成唯一的临时文件名
   */
  explicit StreamingSnapshotManager(int nodeId);

  /**
   * @brief 创建跳表的流式快照
   * @param skipList 跳表引用
   * @param lastRequestId 客户端请求ID映射
   * @param snapshotPath 输出参数：生成的快照文件路径
   * @return 是否创建成功
   */
  bool CreateSnapshot(const SkipList<std::string, std::string>& skipList,
                     const std::unordered_map<std::string, int>& lastRequestId,
                     std::string& snapshotPath);

  /**
   * @brief 从快照恢复跳表
   * @param snapshotPath 快照文件路径
   * @param skipList 跳表引用
   * @param lastRequestId 客户端请求ID映射
   * @return 是否恢复成功
   */
  bool RestoreSnapshot(const std::string& snapshotPath,
                      SkipList<std::string, std::string>& skipList,
                      std::unordered_map<std::string, int>& lastRequestId);

  /**
   * @brief 清理临时文件
   * @param filePath 文件路径
   */
  static void CleanupTempFile(const std::string& filePath);

private:
  int m_nodeId;                         // 节点ID
  
  /**
   * @brief 生成临时文件路径
   * @return 临时文件路径
   */
  std::string generateTempFilePath();
};
