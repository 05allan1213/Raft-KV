#pragma once

/**
 * @brief 全局配置文件
 *
 * 该文件包含了Raft-KV系统的所有配置常量，
 * 包括调试设置、超时时间、协程配置等。
 */

const bool Debug = true; // 调试模式开关

const int debugMul = 1;                      // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 100 * debugMul; // 心跳时间一般要比选举超时小一个数量级（调整为100ms，让日志输出更慢）
const int ApplyInterval = 500 * debugMul;    // 应用日志的时间间隔（调整为500ms，让日志输出更慢）

const int minRandomizedElectionTime = 300 * debugMul; // 最小随机化选举超时时间（毫秒）
const int maxRandomizedElectionTime = 500 * debugMul; // 最大随机化选举超时时间（毫秒）

const int CONSENSUS_TIMEOUT = 500 * debugMul; // 共识超时时间（毫秒）

/**
 * @brief 协程相关设置
 */
const int FIBER_THREAD_NUM = 1;             // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false; // 是否使用caller_thread执行调度任务
