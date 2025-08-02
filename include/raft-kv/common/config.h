#pragma once

const bool Debug = true; // 调试模式开关

const int debugMul = 1;                     // 时间单位：time.Millisecond，不同网络环境rpc速度不同，因此需要乘以一个系数
const int HeartBeatTimeout = 25 * debugMul; // 心跳时间一般要比选举超时小一个数量级
const int ApplyInterval = 10 * debugMul;    // 应用日志的时间间隔

const int minRandomizedElectionTime = 300 * debugMul; // 最小随机化选举超时时间（毫秒）
const int maxRandomizedElectionTime = 500 * debugMul; // 最大随机化选举超时时间（毫秒）

const int CONSENSUS_TIMEOUT = 500 * debugMul; // 共识超时时间（毫秒）

// 协程相关设置

const int FIBER_THREAD_NUM = 1;             // 协程库中线程池大小
const bool FIBER_USE_CALLER_THREAD = false; // 是否使用caller_thread执行调度任务
