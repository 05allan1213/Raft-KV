#pragma once

namespace monsoon
{
  /**
   * @brief 不可拷贝类
   * @details 继承此类的对象不能被拷贝构造和拷贝赋值
   *          用于防止对象被意外拷贝，确保资源的唯一性
   */
  class Nonecopyable
  {
  public:
    /**
     * @brief 默认构造函数
     */
    Nonecopyable() = default;

    /**
     * @brief 默认析构函数
     */
    ~Nonecopyable() = default;

    /**
     * @brief 禁用拷贝构造函数
     */
    Nonecopyable(const Nonecopyable &) = delete;

    /**
     * @brief 禁用拷贝赋值操作符
     */
    Nonecopyable operator=(const Nonecopyable) = delete;
  };
} // namespace monsoon