#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include "raft-kv/fiber/monsoon.h"

// --- 线程测试部分 ---
void test_thread_func()
{
    std::cout << "[Thread Test] name: " << monsoon::Thread::GetThis()->GetName()
              << ", id: " << monsoon::GetThreadId() << std::endl;
}

void run_thread_test()
{
    std::cout << "\n--- Running Thread Test ---" << std::endl;
    std::vector<monsoon::Thread::ptr> tpool;
    for (int i = 0; i < 3; i++)
    {
        monsoon::Thread::ptr t(new monsoon::Thread(&test_thread_func, "thread_" + std::to_string(i)));
        tpool.push_back(t);
    }
    for (auto &t : tpool)
    {
        t->join();
    }
    std::cout << "--- Thread Test Finished ---\n"
              << std::endl;
}

// --- 调度器测试部分 ---
void fiber_task(const std::string &name)
{
    std::cout << "[Scheduler Test] Fiber " << name
              << " running on thread: " << monsoon::GetThreadId() << std::endl;
}

void run_scheduler_test()
{
    std::cout << "--- Running Scheduler Test ---" << std::endl;
    monsoon::Scheduler sc(2, true, "TestScheduler"); // 2个线程，并使用主线程
    sc.start();

    sc.scheduler([]
                 { fiber_task("A"); });
    sc.scheduler([]
                 { fiber_task("B"); });

    sleep(1); // 等待任务执行
    sc.stop();
    std::cout << "--- Scheduler Test Finished ---\n"
              << std::endl;
}

// --- HOOK 和 IOManager 测试部分 ---
int sock = 0;

void run_hook_and_iomanager_test()
{
    std::cout << "--- Running Hook & IOManager Test ---" << std::endl;
    monsoon::IOManager iom(2); // 2个IO线程

    // 1. 测试 sleep hook
    iom.scheduler([]
                  {
        std::cout << "[Hook Test] Fiber is going to sleep for 2 seconds..." << std::endl;
        sleep(2); // 这个 sleep 会被 hook，变为非阻塞的定时器事件
        std::cout << "[Hook Test] Fiber woke up after 2 seconds." << std::endl; });

    // 2. 测试 socket hook 和 IOManager 事件
    iom.scheduler([]
                  {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons(80);
        inet_pton(AF_INET, "36.152.44.96", &addr.sin_addr.s_addr); // 这是一个百度IP

        std::cout << "[IO Test] Begin connecting..." << std::endl;
        int rt = connect(sock, (const sockaddr *)&addr, sizeof(addr));
        if (rt != 0 && errno != EINPROGRESS) {
             std::cout << "[IO Test] Connect failed, rt=" << rt << ", errno=" << errno << ", errstr=" << strerror(errno) << std::endl;
             return;
        }
        std::cout << "[IO Test] Connect returned " << rt << ", errno=" << errno << ". Waiting for socket to be writable." << std::endl;


        const char data[] = "GET / HTTP/1.0\r\n\r\n";
        rt = send(sock, data, sizeof(data), 0);
        if (rt <= 0) {
            std::cout << "[IO Test] Send failed." << std::endl;
            close(sock);
            return;
        }
        std::cout << "[IO Test] Send success, " << rt << " bytes sent." << std::endl;


        std::string buff;
        buff.resize(4096);
        rt = recv(sock, &buff[0], buff.size(), 0);
        if (rt <= 0) {
            std::cout << "[IO Test] Recv failed." << std::endl;
            close(sock);
            return;
        }

        buff.resize(rt);
        std::cout << "[IO Test] Recv success, " << rt << " bytes received:\n" << buff << std::endl;
        close(sock); });

    // IOManager 会在析构时调用 stop()，等待所有任务完成
}

int main()
{
    // 依次执行各项测试
    run_thread_test();
    run_scheduler_test();
    run_hook_and_iomanager_test(); // IOManager的stop会阻塞main函数直到其任务完成

    return 0;
}