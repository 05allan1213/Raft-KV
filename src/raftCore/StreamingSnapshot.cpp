#include "raft-kv/raftCore/StreamingSnapshot.h"
#include "util.h"
#include <filesystem>
#include <sstream>

// ==================== StreamingSnapshotWriter 实现 ====================

StreamingSnapshotWriter::StreamingSnapshotWriter(const std::string &tempFilePath)
    : m_tempFilePath(tempFilePath), m_dataSize(0), m_isOpen(false)
{
}

StreamingSnapshotWriter::~StreamingSnapshotWriter()
{
  cleanup();
}

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

    // 打开文件
    m_file = std::make_unique<std::ofstream>(m_tempFilePath, std::ios::binary | std::ios::trunc);
    if (!m_file->is_open())
    {
      DPrintf("[StreamingSnapshotWriter::BeginSnapshot] Failed to open file: %s", m_tempFilePath.c_str());
      return false;
    }

    // 创建boost归档
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

bool StreamingSnapshotWriter::WriteKeyValue(const std::string &key, const std::string &value)
{
  if (!m_isOpen || !m_archive)
  {
    DPrintf("[StreamingSnapshotWriter::WriteKeyValue] Not opened");
    return false;
  }

  try
  {
    // 写入键值对标记
    std::string marker = "KV";
    *m_archive << marker;
    *m_archive << key;
    *m_archive << value;

    m_dataSize += key.size() + value.size() + marker.size();
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[StreamingSnapshotWriter::WriteKeyValue] Exception: %s", e.what());
    return false;
  }
}

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

bool StreamingSnapshotWriter::EndSnapshot()
{
  if (!m_isOpen)
  {
    return true; // 已经关闭
  }

  try
  {
    // 写入结束标记
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

StreamingSnapshotReader::StreamingSnapshotReader(const std::string &snapshotFilePath)
    : m_snapshotFilePath(snapshotFilePath), m_isOpen(false)
{
}

StreamingSnapshotReader::~StreamingSnapshotReader()
{
  cleanup();
}

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

    // 打开文件
    m_file = std::make_unique<std::ifstream>(m_snapshotFilePath, std::ios::binary);
    if (!m_file->is_open())
    {
      DPrintf("[StreamingSnapshotReader::BeginSnapshot] Failed to open file: %s", m_snapshotFilePath.c_str());
      return false;
    }

    // 创建boost归档
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

StreamingSnapshotManager::StreamingSnapshotManager(int nodeId)
    : m_nodeId(nodeId)
{
}

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

  // 清空现有数据
  skipList.clear_all();
  lastRequestId.clear();

  // 读取键值对
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

std::string StreamingSnapshotManager::generateTempFilePath()
{
  auto now = std::chrono::system_clock::now();
  auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();

  std::stringstream ss;
  ss << "/tmp/temp_snapshot_node" << m_nodeId << "_" << timestamp << ".dat";
  return ss.str();
}
