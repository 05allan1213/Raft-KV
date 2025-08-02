#include <iostream>
#include <vector>
#include <unistd.h>

#include "raft-kv/fiber/iomanager.h"
#include "raft-kv/fiber/fiber.h"

using namespace monsoon;

/**
 * @brief 测试场景1：基本的协程调度
 * @details 创建两个协程，它们会交替执行并主动让出CPU
 *          演示协程的基本调度机制
 */
void test_fiber_execution()
{
    std::cout << "[INFO] test_fiber_execution begin" << std::endl;

    // 协程1：循环打印信息，每次执行后主动让出CPU
    Fiber::ptr fiber1 = std::make_shared<Fiber>([]()
                                                {
        for (int i = 0; i < 5; ++i) {
            std::cout << "[INFO] Fiber 1 is running, iteration: " << i << std::endl;
            Fiber::GetThis()->yield(); // 主动让出执行权，切换到其他协程
        }
        std::cout << "[INFO] Fiber 1 finished" << std::endl; });

    // 协程2：循环打印信息，每次执行后主动让出CPU
    Fiber::ptr fiber2 = std::make_shared<Fiber>([]()
                                                {
        for (int i = 0; i < 5; ++i) {
            std::cout << "[INFO] Fiber 2 is running, iteration: " << i << std::endl;
            Fiber::GetThis()->yield(); // 主动让出执行权，切换到其他协程
        }
        std::cout << "[INFO] Fiber 2 finished" << std::endl; });

    // 将这两个协程加入调度器，开始调度
    IOManager::GetThis()->scheduler(fiber1);
    IOManager::GetThis()->scheduler(fiber2);

    std::cout << "[INFO] test_fiber_execution end, fibers scheduled" << std::endl;
}

/**
 * @brief 测试场景2：I/O hook 和 sleep
 * @details 测试系统调用hook功能，sleep会被hook为非阻塞操作
 *          演示协程如何将阻塞的系统调用转换为非阻塞操作
 */
void test_io_hook()
{
    std::cout << "[INFO] test_io_hook begin" << std::endl;

    // 创建一个协程，其中包含sleep调用
    IOManager::GetThis()->scheduler([]()
                                    {
        std::cout << "[INFO] Fiber with sleep is starting..." << std::endl;
        sleep(2); // 这个sleep会被hook，变为非阻塞的定时器事件
        std::cout << "[INFO] Fiber with sleep resumed after 2 seconds" << std::endl; });

    std::cout << "[INFO] test_io_hook end, sleep fiber scheduled" << std::endl;
}

/**
 * @brief 测试场景3：IOManager的定时器功能
 * @details 测试定时器功能，创建一个3秒后执行的一次性定时器
 *          演示定时器与协程调度的结合
 */
void test_timer()
{
    std::cout << "[INFO] test_timer begin" << std::endl;

    // 添加一个3秒后执行的一次性定时器
    IOManager::GetThis()->addTimer(3000, []()
                                   { std::cout << "[INFO] This is a one-shot timer event after 3000ms!" << std::endl; }, false); // false 表示是一次性事件

    std::cout << "[INFO] test_timer end, timer scheduled" << std::endl;
}

/**
 * @brief 主函数
 * @details 创建IO管理器，调度各种测试任务，演示协程库的功能
 */
int main()
{
    std::cout << "[INFO] Main function started" << std::endl;

    // 创建一个包含2个工作线程的IOManager
    // IOManager继承自Scheduler和TimerManager，提供IO事件驱动的协程调度
    IOManager iom(2);

    // 向IOManager的任务队列中添加我们的测试函数
    // 这些函数会在IO管理器的线程池中执行
    iom.scheduler(test_fiber_execution); // 测试基本协程调度
    iom.scheduler(test_io_hook);         // 测试IO hook功能
    iom.scheduler(test_timer);           // 测试定时器功能

    std::cout << "[INFO] Main function is about to finish, IOManager will take over and then stop." << std::endl;
    // 当 main 函数结束，iom 对象的析构函数会被调用
    // 析构函数会调用 stop()，等待所有任务完成
    return 0;
}