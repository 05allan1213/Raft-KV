#pragma once

#include <string>
#include <unordered_map>

/**
 * @brief RPC配置管理类
 *
 * 框架读取配置文件类，负责解析和管理RPC相关的配置信息。
 * 支持配置文件的加载、解析和查询功能。
 *
 * 配置文件格式示例：
 * rpcserverip   rpcserverport    zookeeperip   zookeeperport
 */
class MprpcConfig
{
public:
  /**
   * @brief 负责解析加载配置文件
   * @param config_file 配置文件路径
   *
   * 从指定路径加载配置文件，解析其中的键值对并存储到内存中
   */
  void LoadConfigFile(const char *config_file);

  /**
   * @brief 查询配置项信息
   * @param key 配置项键名
   * @return 配置项的值，如果不存在则返回空字符串
   */
  std::string Load(const std::string &key);

private:
  std::unordered_map<std::string, std::string> m_configMap; // 存储配置键值对的映射表

  /**
   * @brief 去掉字符串前后的空格
   * @param src_buf 要处理的字符串，会被修改
   */
  void Trim(std::string &src_buf);
};