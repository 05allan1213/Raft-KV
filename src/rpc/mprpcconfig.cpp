#include "mprpcconfig.h"

#include <iostream>
#include <string>

/**
 * @brief 负责解析加载配置文件
 * @param config_file 配置文件路径
 *
 * 从指定路径读取配置文件，解析其中的键值对并存储到内存中。
 * 支持注释行（以#开头）和空行的处理。
 */
void MprpcConfig::LoadConfigFile(const char *config_file)
{
  // 打开配置文件
  FILE *pf = fopen(config_file, "r");
  if (nullptr == pf)
  {
    std::cout << config_file << " is note exist!" << std::endl;
    exit(EXIT_FAILURE); // 文件不存在时退出程序
  }

  // 逐行读取配置文件内容
  // 1.注释   2.正确的配置项 =    3.去掉开头的多余的空格
  while (!feof(pf))
  {
    char buf[512] = {0}; // 缓冲区，用于存储每行内容
    fgets(buf, 512, pf); // 读取一行内容

    // 去掉字符串前面多余的空格
    std::string read_buf(buf);
    Trim(read_buf);

    // 判断#的注释行或空行，跳过这些行
    if (read_buf[0] == '#' || read_buf.empty())
    {
      continue;
    }

    // 解析配置项，查找等号分隔符
    int idx = read_buf.find('=');
    if (idx == -1)
    {
      // 配置项不合法，没有等号分隔符
      continue;
    }

    // 提取键和值
    std::string key;
    std::string value;
    key = read_buf.substr(0, idx); // 提取等号前的部分作为键
    Trim(key);                     // 去除键的前后空格

    // rpcserverip=127.0.0.1\n
    int endidx = read_buf.find('\n', idx);              // 查找换行符位置
    value = read_buf.substr(idx + 1, endidx - idx - 1); // 提取等号后的部分作为值
    Trim(value);                                        // 去除值的前后空格

    // 将键值对存储到配置映射表中
    m_configMap.insert({key, value});
  }

  fclose(pf); // 关闭文件
}

/**
 * @brief 查询配置项信息
 * @param key 配置项键名
 * @return 配置项的值，如果不存在则返回空字符串
 *
 * 从内存中的配置映射表中查找指定键对应的值
 */
std::string MprpcConfig::Load(const std::string &key)
{
  auto it = m_configMap.find(key); // 在映射表中查找键
  if (it == m_configMap.end())
  {
    return ""; // 如果没找到，返回空字符串
  }
  return it->second; // 返回找到的值
}

/**
 * @brief 去掉字符串前后的空格
 * @param src_buf 要处理的字符串，会被修改
 *
 * 去除字符串开头和结尾的所有空格字符
 */
void MprpcConfig::Trim(std::string &src_buf)
{
  // 查找第一个非空格字符的位置
  int idx = src_buf.find_first_not_of(' ');
  if (idx != -1)
  {
    // 说明字符串前面有空格，截取从第一个非空格字符开始的部分
    src_buf = src_buf.substr(idx, src_buf.size() - idx);
  }

  // 去掉字符串后面多余的空格
  idx = src_buf.find_last_not_of(' ');
  if (idx != -1)
  {
    // 说明字符串后面有空格，截取到最后一个非空格字符
    src_buf = src_buf.substr(0, idx + 1);
  }
}