#include "fiber/migration_helper.h"
#include "fiber/iomanager.h"
#include "raftCore/ApplyMsg.h"
#include <iostream>
#include <thread>
#include <chrono>

namespace monsoon
{
  // 全局迁移配置实例
  MigrationConfig g_migrationConfig;

  // 显式实例化常用类型的模板
  template class MigrationHelper::LockQueueWrapper<int>;
  template class MigrationHelper::LockQueueWrapper<std::string>;
  template class MigrationHelper::LockQueueWrapper<ApplyMsg>;
  template class MigrationHelper::LockQueueWrapper<Op>;

}
