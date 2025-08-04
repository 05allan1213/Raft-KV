/**
 * @file main.cpp
 * @brief Google Test 单元测试主入口文件
 * 
 * 该文件是所有单元测试的入口点，使用 Google Test 框架
 * 运行所有的单元测试用例。
 * 
 * @author Raft-KV Team
 * @date 2024
 */

#include <gtest/gtest.h>
#include <iostream>

/**
 * @brief 单元测试主函数
 * 
 * 初始化 Google Test 框架并运行所有测试用例
 * 
 * @param argc 命令行参数数量
 * @param argv 命令行参数数组
 * @return 测试结果，0表示所有测试通过
 */
int main(int argc, char **argv) {
    // 初始化 Google Test
    ::testing::InitGoogleTest(&argc, argv);
    
    std::cout << "=== Raft-KV 单元测试开始 ===" << std::endl;
    
    // 运行所有测试
    int result = RUN_ALL_TESTS();
    
    std::cout << "=== Raft-KV 单元测试结束 ===" << std::endl;
    
    return result;
}
