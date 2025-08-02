#pragma once

#include <memory>

namespace monsoon
{
  namespace
  {
    /**
     * @brief 获取实例的辅助函数（裸指针版本）
     * @tparam T 类型
     * @tparam X 为了创造多个实例对应的Tag
     * @tparam N 同一个Tag创造多个实例索引
     * @return 实例的引用
     */
    template <class T, class X, int N>
    T &GetInstanceX()
    {
      static T v;
      return v;
    }

    /**
     * @brief 获取实例的辅助函数（智能指针版本）
     * @tparam T 类型
     * @tparam X 为了创造多个实例对应的Tag
     * @tparam N 同一个Tag创造多个实例索引
     * @return 实例的智能指针
     */
    template <class T, class X, int N>
    std::shared_ptr<T> GetInstancePtr()
    {
      static std::shared_ptr<T> v(new T);
      return v;
    }
  } // namespace

  /**
   * @brief 单例模式封装类
   * @tparam T 类型
   * @tparam X 为了创造多个实例对应的Tag
   * @tparam N 同一个Tag创造多个实例索引
   * @details 使用模板实现单例模式，支持通过Tag和索引创建多个不同的单例实例
   */
  template <class T, class X = void, int N = 0>
  class Singleton
  {
  public:
    /**
     * @brief 返回单例裸指针
     * @return 单例实例的指针
     */
    static T *GetInstance()
    {
      static T v;
      return &v;
    }
  };

  /**
   * @brief 单例模式智能指针封装类
   * @tparam T 类型
   * @tparam X 为了创造多个实例对应的Tag
   * @tparam N 同一个Tag创造多个实例索引
   * @details 使用模板实现单例模式，支持通过Tag和索引创建多个不同的单例实例
   */
  template <class T, class X = void, int N = 0>
  class SingletonPtr
  {
  public:
    /**
     * @brief 返回单例智能指针
     * @return 单例实例的智能指针
     */
    static std::shared_ptr<T> GetInstance()
    {
      static std::shared_ptr<T> v(new T);
      return v;
    }
  };

} // namespace monsoon