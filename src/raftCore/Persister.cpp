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

/**
 * @brief 保存Raft状态数据
 * @param data 要保存的状态数据
 *
 * 将Raft状态数据保存到本地文件，并更新状态大小
 */
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

/**
 * @brief 获取Raft状态数据大小
 * @return 状态数据的大小（字节数）
 */
long long Persister::RaftStateSize()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  return m_raftStateSize;
}

/**
 * @brief 读取Raft状态数据
 * @return 状态数据字符串，如果文件不存在或读取失败返回空字符串
 */
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

/**
 * @brief 构造函数
 * @param me 节点ID
 *
 * 初始化持久化器，创建对应的文件并打开输出流
 */
Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersist" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshotPersist" + std::to_string(me) + ".txt"),
      m_streamingSnapshotFileName("streamingSnapshotPersist" + std::to_string(me) + ".txt"),
      m_raftStateSize(0)
{
  // 检查文件状态并清空文件
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
    DPrintf("[func-Persister::Persister] 文件打开失败");
  }
  // 绑定流
  m_raftStateOutStream.open(m_raftStateFileName);
  m_snapshotOutStream.open(m_snapshotFileName);
}

/**
 * @brief 析构函数
 *
 * 关闭所有打开的文件流
 */
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

/**
 * @brief 清空Raft状态数据
 *
 * 重置状态大小并清空状态文件内容
 */
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

/**
 * @brief 清空快照数据
 *
 * 清空快照文件内容
 */
void Persister::clearSnapshot()
{
  if (m_snapshotOutStream.is_open())
  {
    m_snapshotOutStream.close();
  }
  m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

/**
 * @brief 清空Raft状态和快照数据
 *
 * 同时清空状态文件和快照文件
 */
void Persister::clearRaftStateAndSnapshot()
{
  clearRaftState();
  clearSnapshot();
}

/**
 * @brief 保存流式快照
 * @param snapshotFilePath 临时快照文件路径
 * @return 保存是否成功
 *
 * 将临时快照文件复制到持久化位置
 */
bool Persister::SaveStreamingSnapshot(const std::string &snapshotFilePath)
{
  std::lock_guard<std::mutex> lg(m_mtx);

  try
  {
    // 将临时快照文件复制到持久化位置
    std::ifstream src(snapshotFilePath, std::ios::binary);
    if (!src.is_open())
    {
      DPrintf("[Persister::SaveStreamingSnapshot] 无法打开源文件: %s", snapshotFilePath.c_str());
      return false;
    }

    std::ofstream dst(m_streamingSnapshotFileName, std::ios::binary | std::ios::trunc);
    if (!dst.is_open())
    {
      DPrintf("[Persister::SaveStreamingSnapshot] 无法打开目标文件: %s",
              m_streamingSnapshotFileName.c_str());
      return false;
    }

    // 复制文件内容
    dst << src.rdbuf();

    src.close();
    dst.close();

    DPrintf("[Persister::SaveStreamingSnapshot] 流式快照已保存到: %s",
            m_streamingSnapshotFileName.c_str());
    return true;
  }
  catch (const std::exception &e)
  {
    DPrintf("[Persister::SaveStreamingSnapshot] 异常: %s", e.what());
    return false;
  }
}

/**
 * @brief 读取流式快照
 * @return 快照文件路径，如果文件不存在返回空字符串
 *
 * 检查流式快照文件是否存在并返回文件路径
 */
std::string Persister::ReadStreamingSnapshot()
{
  std::lock_guard<std::mutex> lg(m_mtx);

  // 检查文件是否存在
  std::ifstream file(m_streamingSnapshotFileName);
  if (!file.good())
  {
    DPrintf("[Persister::ReadStreamingSnapshot] 文件未找到: %s", m_streamingSnapshotFileName.c_str());
    return "";
  }
  file.close();

  DPrintf("[Persister::ReadStreamingSnapshot] 找到流式快照: %s",
          m_streamingSnapshotFileName.c_str());
  return m_streamingSnapshotFileName;
}
