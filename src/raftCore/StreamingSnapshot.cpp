#include "raft-kv/raftCore/StreamingSnapshot.h"
#include "util.h"
#include <filesystem>
#include <sstream>

// ==================== StreamingSnapshotWriter 实现 ====================

/**
 * @brief 构造函数
 * @param tempFilePath 临时文件路径，用于写入快照数据
 */
StreamingSnapshotWriter::StreamingSnapshotWriter(const std::string &tempFilePath)
    : m_tempFilePath(tempFilePath), m_dataSize(0), m_isOpen(false)
{
}

/**
 * @brief 析构函数
 * 清理资源，关闭文件
 */
StreamingSnapshotWriter::~StreamingSnapshotWriter()
{
  cleanup();
}

/**
 * @brief 开始快照写入
 * @return 是否成功开始写入
 *
 * 创建文件并初始化boost归档对象，准备写入快照数据
 */
bool StreamingSnapshotWriter::BeginSnapshot()
{
  if (m_isOpen)
  {
    DPrintf("[StreamingSnapshotWriter::BeginSnapshot] Already opened");
    return false;
  }

  try
  {
    // 创建目录（如果不存在）
    std::filesystem::path filePath(m_tempFilePath);
    if (filePath.has_parent_path() && !filePath.parent_path().empty())
    {
      std::filesystem::create_directories(filePath.parent_path());
    }

    // 打开文件，使用二进制模式和截断模式
    m_file = std::make_unique<std::ofstream>(m_tempFilePath, std::ios::binary | std::ios::trunc);
    if (!m_file->is_open())
    {
      DPrintf("[StreamingSnapshotWriter::BeginSnapshot] Failed to open file: %s", m_tempFilePath.c_str());
      return false;
    }

    // 创建boost文本归档对象，用于序列化数据
    m_archive = std::make_unique<boost::archive::text_oarchive>(*m_file);
    m_isOpen = true;
    m_dataSize = 0;

    DPrintf("[StreamingSnapshotWriter::BeginSnapshot] Started writing to: %s", m_tempFilePath.c_str());
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotWriter::BeginSnapshot] Exception: %s", e.what());
    cleanup();
    return false;
  }
}

/**
 * @brief 写入键值对
 * @param key 键
 * @param value 值
 * @return 是否成功写入
 *
 * 将单个键值对写入快照文件，使用标记区分不同类型的数据
 */
bool StreamingSnapshotWriter::WriteKeyValue(const std::string &key, const std::string &value)
{
  if (!m_isOpen || !m_archive)
  {
    DPrintf("[StreamingSnapshotWriter::WriteKeyValue] Not opened");
    return false;
  }

  try
  {
    // 写入键值对标记，用于区分不同类型的数据
    std::string marker = "KV";
    *m_archive << marker;
    *m_archive << key;
    *m_archive << value;

    // 更新数据大小统计
    m_dataSize += key.size() + value.size() + marker.size();
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotWriter::WriteKeyValue] Exception: %s", e.what());
    return false;
  }
}

/**
 * @brief 写入最后请求ID映射
 * @param lastRequestId 客户端ID到请求ID的映射
 * @return 是否成功写入
 *
 * 将客户端请求ID映射写入快照文件，用于恢复客户端状态
 */
bool StreamingSnapshotWriter::WriteLastRequestId(const std::unordered_map<std::string, int> &lastRequestId)
{
  if (!m_isOpen || !m_archive)
  {
    DPrintf("[StreamingSnapshotWriter::WriteLastRequestId] Not opened");
    return false;
  }

  try
  {
    // 写入请求ID映射标记
    std::string marker = "REQ";
    *m_archive << marker;
    *m_archive << lastRequestId;

    // 估算数据大小
    for (const auto &pair : lastRequestId)
    {
      m_dataSize += pair.first.size() + sizeof(pair.second);
    }
    m_dataSize += marker.size();

    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotWriter::WriteLastRequestId] Exception: %s", e.what());
    return false;
  }
}

/**
 * @brief 结束快照写入
 * @return 是否成功结束
 *
 * 写入结束标记并关闭文件，完成快照写入过程
 */
bool StreamingSnapshotWriter::EndSnapshot()
{
  if (!m_isOpen)
  {
    return true; // 已经关闭
  }

  try
  {
    // 写入结束标记，标识快照数据结束
    std::string endMarker = "END";
    *m_archive << endMarker;

    // 关闭归档和文件
    m_archive.reset();
    m_file->close();
    m_file.reset();

    m_isOpen = false;

    DPrintf("[StreamingSnapshotWriter::EndSnapshot] Finished writing %zu bytes to: %s",
            m_dataSize, m_tempFilePath.c_str());
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotWriter::EndSnapshot] Exception: %s", e.what());
    cleanup();
    return false;
  }
}

/**
 * @brief 清理资源
 *
 * 关闭文件流和归档对象，释放资源
 */
void StreamingSnapshotWriter::cleanup()
{
  if (m_archive)
  {
    m_archive.reset();
  }
  if (m_file && m_file->is_open())
  {
    m_file->close();
  }
  m_file.reset();
  m_isOpen = false;
}

// ==================== StreamingSnapshotReader 实现 ====================

/**
 * @brief 构造函数
 * @param snapshotFilePath 快照文件路径
 */
StreamingSnapshotReader::StreamingSnapshotReader(const std::string &snapshotFilePath)
    : m_snapshotFilePath(snapshotFilePath), m_isOpen(false)
{
}

/**
 * @brief 析构函数
 * 清理资源，关闭文件
 */
StreamingSnapshotReader::~StreamingSnapshotReader()
{
  cleanup();
}

/**
 * @brief 开始快照读取
 * @return 是否成功开始读取
 *
 * 打开文件并初始化boost归档对象，准备读取快照数据
 */
bool StreamingSnapshotReader::BeginSnapshot()
{
  if (m_isOpen)
  {
    DPrintf("[StreamingSnapshotReader::BeginSnapshot] Already opened");
    return false;
  }

  try
  {
    // 检查文件是否存在
    if (!std::filesystem::exists(m_snapshotFilePath))
    {
      DPrintf("[StreamingSnapshotReader::BeginSnapshot] File not found: %s", m_snapshotFilePath.c_str());
      return false;
    }

    // 打开文件，使用二进制模式
    m_file = std::make_unique<std::ifstream>(m_snapshotFilePath, std::ios::binary);
    if (!m_file->is_open())
    {
      DPrintf("[StreamingSnapshotReader::BeginSnapshot] Failed to open file: %s", m_snapshotFilePath.c_str());
      return false;
    }

    // 创建boost文本归档对象，用于反序列化数据
    m_archive = std::make_unique<boost::archive::text_iarchive>(*m_file);
    m_isOpen = true;

    DPrintf("[StreamingSnapshotReader::BeginSnapshot] Started reading from: %s", m_snapshotFilePath.c_str());
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotReader::BeginSnapshot] Exception: %s", e.what());
    cleanup();
    return false;
  }
}

/**
 * @brief 读取键值对
 * @param key 输出参数，读取到的键
 * @param value 输出参数，读取到的值
 * @return 是否成功读取到键值对
 *
 * 从快照文件中读取单个键值对，根据标记区分数据类型
 */
bool StreamingSnapshotReader::ReadKeyValue(std::string &key, std::string &value)
{
  if (!m_isOpen || !m_archive)
  {
    return false;
  }

  try
  {
    std::string marker;
    *m_archive >> marker;

    if (marker == "KV")
    {
      // 读取键值对数据
      *m_archive >> key;
      *m_archive >> value;
      return true;
    }
    else if (marker == "REQ" || marker == "END")
    {
      // 遇到其他标记，需要回退（但boost archive不支持回退，所以这里需要特殊处理）
      // 这里我们返回false，调用者需要调用相应的读取方法
      return false;
    }

    return false;
  }
  catch (const std::exception &e)
  {
    // 可能是文件结束或其他错误
    return false;
  }
}

/**
 * @brief 读取最后请求ID映射
 * @param lastRequestId 输出参数，读取到的请求ID映射
 * @return 是否成功读取
 *
 * 从快照文件中读取客户端请求ID映射，用于恢复客户端状态
 */
bool StreamingSnapshotReader::ReadLastRequestId(std::unordered_map<std::string, int> &lastRequestId)
{
  if (!m_isOpen || !m_archive)
  {
    return false;
  }

  try
  {
    std::string marker;
    *m_archive >> marker;

    if (marker == "REQ")
    {
      // 读取请求ID映射数据
      *m_archive >> lastRequestId;
      return true;
    }

    return false;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotReader::ReadLastRequestId] Exception: %s", e.what());
    return false;
  }
}

/**
 * @brief 结束快照读取
 * @return 是否成功结束
 *
 * 关闭文件并清理资源，完成快照读取过程
 */
bool StreamingSnapshotReader::EndSnapshot()
{
  if (!m_isOpen)
  {
    return true;
  }

  cleanup();
  DPrintf("[StreamingSnapshotReader::EndSnapshot] Finished reading from: %s", m_snapshotFilePath.c_str());
  return true;
}

/**
 * @brief 清理资源
 *
 * 关闭文件流和归档对象，释放资源
 */
void StreamingSnapshotReader::cleanup()
{
  if (m_archive)
  {
    m_archive.reset();
  }
  if (m_file && m_file->is_open())
  {
    m_file->close();
  }
  m_file.reset();
  m_isOpen = false;
}

// ==================== StreamingSnapshotManager 实现 ====================

/**
 * @brief 构造函数
 * @param nodeId 节点ID，用于生成唯一的临时文件路径
 */
StreamingSnapshotManager::StreamingSnapshotManager(int nodeId)
    : m_nodeId(nodeId)
{
}

/**
 * @brief 创建快照
 * @param skipList 跳表数据
 * @param lastRequestId 客户端请求ID映射
 * @param snapshotPath 输出参数，生成的快照文件路径
 * @return 是否成功创建快照
 *
 * 遍历跳表数据并创建流式快照，避免一次性加载所有数据到内存
 */
bool StreamingSnapshotManager::CreateSnapshot(const SkipList<std::string, std::string> &skipList,
                                              const std::unordered_map<std::string, int> &lastRequestId,
                                              std::string &snapshotPath)
{
  // 生成临时文件路径
  std::string tempPath = generateTempFilePath();

  // 创建流式写入器
  StreamingSnapshotWriter writer(tempPath);

  if (!writer.BeginSnapshot())
  {
    DPrintf("[StreamingSnapshotManager::CreateSnapshot] Failed to begin snapshot");
    return false;
  }

  // 遍历跳表并写入键值对
  bool traverseSuccess = true;
  skipList.traverse([&writer, &traverseSuccess](const std::string &key, const std::string &value)
                    {
    if (!writer.WriteKeyValue(key, value)) {
      traverseSuccess = false;
    } });

  if (!traverseSuccess)
  {
    DPrintf("[StreamingSnapshotManager::CreateSnapshot] Failed to write key-value pairs");
    return false;
  }

  // 写入客户端请求ID映射
  if (!writer.WriteLastRequestId(lastRequestId))
  {
    DPrintf("[StreamingSnapshotManager::CreateSnapshot] Failed to write last request ID");
    return false;
  }

  if (!writer.EndSnapshot())
  {
    DPrintf("[StreamingSnapshotManager::CreateSnapshot] Failed to end snapshot");
    return false;
  }

  snapshotPath = tempPath;
  DPrintf("[StreamingSnapshotManager::CreateSnapshot] Created snapshot: %s (%zu bytes)",
          snapshotPath.c_str(), writer.GetDataSize());

  return true;
}

/**
 * @brief 恢复快照
 * @param snapshotPath 快照文件路径
 * @param skipList 输出参数，恢复的跳表数据
 * @param lastRequestId 输出参数，恢复的请求ID映射
 * @return 是否成功恢复快照
 *
 * 从快照文件中恢复数据到跳表和请求ID映射，重建状态机状态
 */
bool StreamingSnapshotManager::RestoreSnapshot(const std::string &snapshotPath,
                                               SkipList<std::string, std::string> &skipList,
                                               std::unordered_map<std::string, int> &lastRequestId)
{
  StreamingSnapshotReader reader(snapshotPath);

  if (!reader.BeginSnapshot())
  {
    DPrintf("[StreamingSnapshotManager::RestoreSnapshot] Failed to begin reading snapshot");
    return false;
  }

  // 清空现有数据，为恢复做准备
  skipList.clear_all();
  lastRequestId.clear();

  // 读取键值对并插入到跳表中
  std::string key, value;
  while (reader.ReadKeyValue(key, value))
  {
    skipList.insert_element(key, value);
  }

  // 读取客户端请求ID映射
  if (!reader.ReadLastRequestId(lastRequestId))
  {
    DPrintf("[StreamingSnapshotManager::RestoreSnapshot] Failed to read last request ID");
    // 这不是致命错误，继续执行
  }

  reader.EndSnapshot();

  DPrintf("[StreamingSnapshotManager::RestoreSnapshot] Restored snapshot from: %s", snapshotPath.c_str());
  return true;
}

/**
 * @brief 清理临时文件
 * @param filePath 要删除的文件路径
 *
 * 删除指定的临时快照文件，释放磁盘空间
 */
void StreamingSnapshotManager::CleanupTempFile(const std::string &filePath)
{
  try
  {
    if (std::filesystem::exists(filePath))
    {
      std::filesystem::remove(filePath);
      DPrintf("[StreamingSnapshotManager::CleanupTempFile] Removed temp file: %s", filePath.c_str());
    }
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotManager::CleanupTempFile] Failed to remove %s: %s", filePath.c_str(), e.what());
  }
}

/**
 * @brief 生成临时文件路径
 * @return 生成的临时文件路径
 *
 * 根据节点ID和时间戳生成唯一的临时文件路径，避免文件名冲突
 */
std::string StreamingSnapshotManager::generateTempFilePath()
{
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  std::stringstream ss;
  ss << "/tmp/temp_snapshot_node" << m_nodeId << "_" << timestamp << ".dat";
  return ss.str();
}
