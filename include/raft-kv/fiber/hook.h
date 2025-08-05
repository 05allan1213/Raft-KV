#pragma once

#include <fcntl.h>
#include <stdint.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "hook.h"

namespace monsoon
{
    /**
     * @brief 检查当前线程是否启用hook
     * @return true表示启用hook，false表示未启用hook
     */
    bool is_hook_enable();

    /**
     * @brief 设置当前线程的hook状态
     * @param flag true表示启用hook，false表示禁用hook
     */
    void set_hook_enable(bool flag);
}

extern "C"
{
    // sleep相关函数指针
    typedef unsigned int (*sleep_fun)(unsigned int seconds);
    extern sleep_fun sleep_f; // sleep函数指针

    typedef int (*usleep_fun)(useconds_t usec);
    extern usleep_fun usleep_f; // usleep函数指针

    typedef int (*nanosleep_fun)(const struct timespec *req, struct timespec *rem);
    extern nanosleep_fun nanosleep_f; // nanosleep函数指针

    // socket相关函数指针
    typedef int (*socket_fun)(int domain, int type, int protocol);
    extern socket_fun socket_f; // socket函数指针

    typedef int (*connect_fun)(int sockfd, const struct sockaddr *addr, socklen_t addrlen);
    extern connect_fun connect_f; // connect函数指针

    typedef int (*accept_fun)(int s, struct sockaddr *addr, socklen_t *addrlen);
    extern accept_fun accept_f; // accept函数指针

    // read相关函数指针
    typedef ssize_t (*read_fun)(int fd, void *buf, size_t count);
    extern read_fun read_f; // read函数指针

    typedef ssize_t (*readv_fun)(int fd, const struct iovec *iov, int iovcnt);
    extern readv_fun readv_f; // readv函数指针

    typedef ssize_t (*recv_fun)(int sockfd, void *buf, size_t len, int flags);
    extern recv_fun recv_f; // recv函数指针

    typedef ssize_t (*recvfrom_fun)(int sockfd, void *buf, size_t len, int flags, struct sockaddr *src_addr,
                                    socklen_t *addrlen);
    extern recvfrom_fun recvfrom_f; // recvfrom函数指针

    typedef ssize_t (*recvmsg_fun)(int sockfd, struct msghdr *msg, int flags);
    extern recvmsg_fun recvmsg_f; // recvmsg函数指针

    // write相关函数指针
    typedef ssize_t (*write_fun)(int fd, const void *buf, size_t count);
    extern write_fun write_f; // write函数指针

    typedef ssize_t (*writev_fun)(int fd, const struct iovec *iov, int iovcnt);
    extern writev_fun writev_f; // writev函数指针

    typedef ssize_t (*send_fun)(int s, const void *msg, size_t len, int flags);
    extern send_fun send_f; // send函数指针

    typedef ssize_t (*sendto_fun)(int s, const void *msg, size_t len, int flags, const struct sockaddr *to,
                                  socklen_t tolen);
    extern sendto_fun sendto_f; // sendto函数指针

    typedef ssize_t (*sendmsg_fun)(int s, const struct msghdr *msg, int flags);
    extern sendmsg_fun sendmsg_f; // sendmsg函数指针

    typedef int (*close_fun)(int fd);
    extern close_fun close_f; // close函数指针

    // 其他系统调用函数指针
    typedef int (*fcntl_fun)(int fd, int cmd, ... /* arg */);
    extern fcntl_fun fcntl_f; // fcntl函数指针

    typedef int (*ioctl_fun)(int d, unsigned long int request, ...);
    extern ioctl_fun ioctl_f; // ioctl函数指针

    typedef int (*getsockopt_fun)(int sockfd, int level, int optname, void *optval, socklen_t *optlen);
    extern getsockopt_fun getsockopt_f; // getsockopt函数指针

    typedef int (*setsockopt_fun)(int sockfd, int level, int optname, const void *optval, socklen_t optlen);
    extern setsockopt_fun setsockopt_f; // setsockopt函数指针

    /**
     * @brief 带超时的connect函数
     * @param fd 文件描述符
     * @param addr 目标地址
     * @param addrlen 地址长度
     * @param timeout_ms 超时时间（毫秒）
     * @return 0表示成功，-1表示失败
     */
    extern int connect_with_timeout(int fd, const struct sockaddr *addr, socklen_t addrlen, uint64_t timeout_ms);
}