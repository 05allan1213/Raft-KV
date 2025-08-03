#include "Persister.h"
#include "util.h"

/**
 * @brief 保存Raft状态和快照到本地文件
 *
 * 将Raft状态数据和快照数据同时保存到本地文件中。
 * 注意：会涉及反复打开文件的操作，没有考虑如果文件出现问题会怎么办？
 *
 * @param raftstate Raft状态数据
 * @param snapshot 快照数据
 */
void Persister::Save(const std::string raftstate, const std::string snapshot)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  clearRaftStateAndSnapshot();

  // 将raftstate和snapshot写入本地文件
  m_raftStateOutStream << raftstate;
  m_snapshotOutStream << snapshot;
}

/**
 * @brief 从本地文件读取快照数据
 *
 * 从快照文件中读取之前保存的快照数据。
 * 如果文件不存在或读取失败，返回空字符串。
 *
 * @return 快照数据字符串
 */
std::string Persister::ReadSnapshot()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 如果输出流已打开，先关闭
  if (m_snapshotOutStream.is_open())
  {
    m_snapshotOutStream.close();
  }

  DEFER
  {
    m_snapshotOutStream.open(m_snapshotFileName); // 默认是追加模式
  };

  // 读取快照文件
  std::fstream ifs(m_snapshotFileName, std::ios_base::in);
  if (!ifs.good())
  {
    return "";
  }
  std::string snapshot;
  ifs >> snapshot;
  ifs.close();
  return snapshot;
}

void Persister::SaveRaftState(const std::string &data)
{
  std::lock_guard<std::mutex> lg(m_mtx);
  // 将raftstate和snapshot写入本地文件
  clearRaftState();
  m_raftStateOutStream << data;
  m_raftStateOutStream.flush(); // 确保数据写入磁盘

  // 更新状态大小为当前数据的大小（而不是累加）
  m_raftStateSize = data.size();
}

long long Persister::RaftStateSize()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

std::string Persister::ReadRaftState()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  std::fstream ifs(m_raftStateFileName, std::ios_base::in);
  if (!ifs.good())
  {
    return "";
  }
  std::string snapshot;
  ifs >> snapshot;
  ifs.close();
  return snapshot;
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_streamingSnapshotFileName("streamingSnapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0)
{
  /**
   * 检查文件状态并清空文件
   */
  bool fileOpenFlag = true;
  std::fstream file(m_raftStateFileName, std::ios::out | std::ios::trunc);
  if (file.is_open())
  {
    file.close();
  }
  else
  {
    fileOpenFlag = false;
  }
  file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::trunc);
  if (file.is_open())
  {
    file.close();
  }
  else
  {
    fileOpenFlag = false;
  }
  if (!fileOpenFlag)
  {
    DPrintf("[func-Persister::Persister] file open error");
  }
  /**
   * 绑定流
   */
  m_raftStateOutStream.open(m_raftStateFileName);
  m_snapshotOutStream.open(m_snapshotFileName);
}

Persister::~Persister()
{
  if (m_raftStateOutStream.is_open())
  {
    m_raftStateOutStream.close();
  }
  if (m_snapshotOutStream.is_open())
  {
    m_snapshotOutStream.close();
  }
}

void Persister::clearRaftState()
{
  m_raftStateSize = 0;
  // 关闭文件流
  if (m_raftStateOutStream.is_open())
  {
    m_raftStateOutStream.close();
  }
  // 重新打开文件流并清空文件内容
  m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearSnapshot()
{
  if (m_snapshotOutStream.is_open())
  {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot()
{
  clearRaftState();
  clearSnapshot();
}

bool Persister::SaveStreamingSnapshot(const std::string &snapshotFilePath)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  try
  {
    // 将临时快照文件复制到持久化位置
    std::ifstream src(snapshotFilePath, std::ios::binary);
    if (!src.is_open())
    {
      DPrintf("[Persister::SaveStreamingSnapshot] Failed to open source file: %s", snapshotFilePath.c_str());
      return false;
    }

    std::ofstream dst(m_streamingSnapshotFileName, std::ios::binary | std::ios::trunc);
    if (!dst.is_open())
    {
      DPrintf("[Persister::SaveStreamingSnapshot] Failed to open destination file: %s",
              m_streamingSnapshotFileName.c_str());
      return false;
    }

    // 复制文件内容
    dst << src.rdbuf();

    src.close();
    dst.close();

    DPrintf("[Persister::SaveStreamingSnapshot] Saved streaming snapshot to: %s",
            m_streamingSnapshotFileName.c_str());
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[Persister::SaveStreamingSnapshot] Exception: %s", e.what());
    return false;
  }
}

std::string Persister::ReadStreamingSnapshot()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 检查文件是否存在
  std::ifstream file(m_streamingSnapshotFileName);
  if (!file.good())
  {
    DPrintf("[Persister::ReadStreamingSnapshot] File not found: %s", m_streamingSnapshotFileName.c_str());
    return "";
  }
  file.close();

  DPrintf("[Persister::ReadStreamingSnapshot] Found streaming snapshot: %s",
          m_streamingSnapshotFileName.c_str());
  return m_streamingSnapshotFileName;
}
