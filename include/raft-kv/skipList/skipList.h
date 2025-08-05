#pragma once

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>

#define STORE_FILE "store/dumpFile"

static std::string delimiter = ":"; // 键值对分隔符

/**
 * @brief 跳表节点类模板
 *
 * 跳表中的每个节点都包含一个键值对，以及指向不同层级下一个节点的指针数组。
 *
 * @tparam K 键的类型
 * @tparam V 值的类型
 */
template <typename K, typename V>
class Node
{
public:
  Node() {}

  Node(K k, V v, int);

  ~Node();

  K get_key() const;

  V get_value() const;

  void set_value(V);

  // 线性数组，用于保存指向不同层级下一个节点的指针
  Node<K, V> **forward;

  int node_level;

private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level)
{
  this->key = k;
  this->value = v;
  this->node_level = level;

  // level + 1，因为数组索引从 0 到 level
  this->forward = new Node<K, V> *[level + 1];

  // 用 0(NULL) 填充 forward 数组
  memset(this->forward, 0, sizeof(Node<K, V> *) * (level + 1));
};

template <typename K, typename V>
Node<K, V>::~Node()
{
  delete[] forward;
};

template <typename K, typename V>
K Node<K, V>::get_key() const
{
  return key;
};

template <typename K, typename V>
V Node<K, V>::get_value() const
{
  return value;
};
template <typename K, typename V>
void Node<K, V>::set_value(V value)
{
  this->value = value;
};

/**
 * @brief 跳表转储类模板
 * @tparam K 键的类型
 * @tparam V 值的类型
 *
 * 用于序列化和反序列化跳表数据
 */
template <typename K, typename V>
class SkipListDump
{
public:
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive &ar, const unsigned int version)
  {
    ar & keyDumpVt_;
    ar & valDumpVt_;
  }
  std::vector<K> keyDumpVt_; // 键的转储向量
  std::vector<V> valDumpVt_; // 值的转储向量

public:
  void insert(const Node<K, V> &node);
};

/**
 * @brief 跳表类模板
 * @tparam K 键的类型
 * @tparam V 值的类型
 *
 * 实现了跳表的核心功能，包括插入、删除、查找、遍历等操作
 */
template <typename K, typename V>
class SkipList
{
public:
  SkipList(int);
  ~SkipList();
  int get_random_level();
  Node<K, V> *create_node(K, V, int);
  int insert_element(K, V);
  void display_list();
  bool search_element(K, V &value);
  void delete_element(K);
  void insert_set_element(K &, V &);
  std::string dump_file();
  void load_file(const std::string &dumpStr);
  // 递归删除节点
  void clear(Node<K, V> *);
  int size();

  // 流式遍历方法
  template <typename Func>
  void traverse(Func func) const;

  // 清空跳表
  void clear_all();

private:
  void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
  bool is_valid_string(const std::string &str);

private:
  int _max_level;             // 跳表的最大层级
  int _skip_list_level;       // 跳表的当前层级
  Node<K, V> *_header;        // 指向头节点的指针
  std::ofstream _file_writer; // 文件写入器
  std::ifstream _file_reader; // 文件读取器
  int _element_count;         // 跳表当前元素数量
  std::mutex _mtx;            // 临界区互斥锁
};

// 创建新节点
template <typename K, typename V>
Node<K, V> *SkipList<K, V>::create_node(const K k, const V v, int level)
{
  Node<K, V> *n = new Node<K, V>(k, v, level);
  return n;
}

// 在跳表中插入给定的键和值
// 返回 1 表示元素已存在
// 返回 0 表示插入成功
/*
                           +------------+
                           |  插入 50   |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |                      插入 +----+
level 3         1+-------->10+---------------> | 50 |          70       100
                                               |    |
                                               |    |
level 2         1          10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 1         1    4     10         30       | 50 |          70       100
                                               |    |
                                               |    |
level 0         1    4   9 10         30   40  | 50 |  60      70       100
                                               +----+

*/
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value)
{
  _mtx.lock();
  Node<K, V> *current = this->_header;

  // 创建更新数组并初始化
  // update 是数组，存放稍后需要操作的 node->forward[i] 的节点
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  // 从跳表的最高层级开始
  for (int i = _skip_list_level; i >= 0; i--)
  {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
    {
      current = current->forward[i];
    }
    update[i] = current;
  }

  // 到达第 0 层，forward 指针指向右侧节点，这是要插入键的期望位置
  current = current->forward[0];

  // 如果当前节点的键等于搜索的键，我们就找到了它
  if (current != NULL && current->get_key() == key)
  {
    std::cout << "键: " << key << ", 已存在" << std::endl;
    _mtx.unlock();
    return 1;
  }

  // 如果 current 为 NULL，说明我们已经到达了该层的末尾
  // 如果 current 的键不等于 key，说明我们必须在 update[0] 和 current 节点之间插入节点
  if (current == NULL || current->get_key() != key)
  {
    // 为节点生成一个随机层级
    int random_level = get_random_level();

    // 如果随机层级大于跳表的当前层级，用指向头部的指针初始化 update 值
    if (random_level > _skip_list_level)
    {
      for (int i = _skip_list_level + 1; i < random_level + 1; i++)
      {
        update[i] = _header;
      }
      _skip_list_level = random_level;
    }

    // 用生成的随机层级创建新节点
    Node<K, V> *inserted_node = create_node(key, value, random_level);

    // 插入节点
    for (int i = 0; i <= random_level; i++)
    {
      inserted_node->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = inserted_node;
    }
    std::cout << "成功插入键:" << key << ", 值:" << value << std::endl;
    _element_count++;
  }
  _mtx.unlock();
  return 0;
}

// 显示跳表
template <typename K, typename V>
void SkipList<K, V>::display_list()
{
  std::cout << "\n*****跳表*****"
            << "\n";
  for (int i = 0; i <= _skip_list_level; i++)
  {
    Node<K, V> *node = this->_header->forward[i];
    std::cout << "层级 " << i << ": ";
    while (node != NULL)
    {
      std::cout << node->get_key() << ":" << node->get_value() << ";";
      node = node->forward[i];
    }
    std::cout << std::endl;
  }
}

// 将内存中的数据转储到文件
template <typename K, typename V>
std::string SkipList<K, V>::dump_file()
{
  Node<K, V> *node = this->_header->forward[0];
  SkipListDump<K, V> dumper;
  while (node != nullptr)
  {
    dumper.insert(*node);
    node = node->forward[0];
  }
  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dumper;
  return ss.str();
}

// 从磁盘加载数据
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr)
{
  if (dumpStr.empty())
  {
    return;
  }
  SkipListDump<K, V> dumper;
  std::stringstream iss(dumpStr);
  boost::archive::text_iarchive ia(iss);
  ia >> dumper;
  for (int i = 0; i < dumper.keyDumpVt_.size(); ++i)
  {
    insert_element(dumper.keyDumpVt_[i], dumper.keyDumpVt_[i]);
  }
}

// 获取当前跳表大小
template <typename K, typename V>
int SkipList<K, V>::size()
{
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value)
{
  if (!is_valid_string(str))
  {
    return;
  }
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str)
{
  if (str.empty())
  {
    return false;
  }
  if (str.find(delimiter) == std::string::npos)
  {
    return false;
  }
  return true;
}

// 从跳表中删除元素
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key)
{
  _mtx.lock();
  Node<K, V> *current = this->_header;
  Node<K, V> *update[_max_level + 1];
  memset(update, 0, sizeof(Node<K, V> *) * (_max_level + 1));

  // 从跳表的最高层级开始
  for (int i = _skip_list_level; i >= 0; i--)
  {
    while (current->forward[i] != NULL && current->forward[i]->get_key() < key)
    {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != NULL && current->get_key() == key)
  {
    // 从最低层级开始，删除每一层的当前节点
    for (int i = 0; i <= _skip_list_level; i++)
    {
      // 如果在第 i 层，下一个节点不是目标节点，跳出循环
      if (update[i]->forward[i] != current)
        break;

      update[i]->forward[i] = current->forward[i];
    }

    // 移除没有元素的层级
    while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
    {
      _skip_list_level--;
    }

    std::cout << "成功删除键 " << key << std::endl;
    delete current;
    _element_count--;
  }
  _mtx.unlock();
  return;
}

/**
 * @brief 作用与insert_element相同类似
 * @param key 键
 * @param value 值
 *
 * insert_element是插入新元素，
 * insert_set_element是插入元素，如果元素存在则改变其值
 */
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value)
{
  V oldValue;
  if (search_element(key, oldValue))
  {
    delete_element(key);
  }
  insert_element(key, value);
}

// 在跳表中搜索元素
/*
                           +------------+
                           |  选择 60   |
                           +------------+
level 4     +-->1+                                                      100
                 |
                 |
level 3         1+-------->10+------------------>50+           70       100
                                                   |
                                                   |
level 2         1          10         30         50|           70       100
                                                   |
                                                   |
level 1         1    4     10         30         50|           70       100
                                                   |
                                                   |
level 0         1    4   9 10         30   40    50+-->60      70       100
*/
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value)
{
  std::cout << "search_element-----------------" << std::endl;
  Node<K, V> *current = _header;

  // 从跳表的最高层级开始
  for (int i = _skip_list_level; i >= 0; i--)
  {
    while (current->forward[i] && current->forward[i]->get_key() < key)
    {
      current = current->forward[i];
    }
  }

  // 到达第 0 层，前进指针到右侧节点，这是我们搜索的节点
  current = current->forward[0];

  // 如果当前节点的键等于搜索的键，我们就找到了它
  if (current and current->get_key() == key)
  {
    value = current->get_value();
    std::cout << "找到键: " << key << ", 值: " << current->get_value() << std::endl;
    return true;
  }

  std::cout << "未找到键:" << key << std::endl;
  return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node)
{
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

// 构造跳表
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level)
{
  this->_max_level = max_level;
  this->_skip_list_level = 0;
  this->_element_count = 0;

  // 创建头节点并将键和值初始化为 null
  K k;
  V v;
  this->_header = new Node<K, V>(k, v, _max_level);
};

template <typename K, typename V>
SkipList<K, V>::~SkipList()
{
  if (_file_writer.is_open())
  {
    _file_writer.close();
  }
  if (_file_reader.is_open())
  {
    _file_reader.close();
  }

  // 递归删除跳表链条
  if (_header->forward[0] != nullptr)
  {
    clear(_header->forward[0]);
  }
  delete (_header);
}
template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *cur)
{
  if (cur->forward[0] != nullptr)
  {
    clear(cur->forward[0]);
  }
  delete (cur);
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level()
{
  int k = 1;
  while (rand() % 2)
  {
    k++;
  }
  k = (k < _max_level) ? k : _max_level;
  return k;
};

// 流式遍历方法实现
template <typename K, typename V>
template <typename Func>
void SkipList<K, V>::traverse(Func func) const
{
  Node<K, V> *current = _header->forward[0];
  while (current != nullptr)
  {
    func(current->get_key(), current->get_value());
    current = current->forward[0];
  }
}

// 清空跳表实现
template <typename K, typename V>
void SkipList<K, V>::clear_all()
{
  _mtx.lock();
  if (_header->forward[0] != nullptr)
  {
    clear(_header->forward[0]);
    // 重置头节点的所有指针
    for (int i = 0; i <= _max_level; i++)
    {
      _header->forward[i] = nullptr;
    }
  }
  _skip_list_level = 0;
  _element_count = 0;
  _mtx.unlock();
}
