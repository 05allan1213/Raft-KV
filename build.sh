#!/bin/bash

# Raft-KV 项目清理和构建脚本
# 功能：清理所有生成文件，重新构建项目

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

# 获取脚本所在目录（项目根目录）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

print_info "Raft-KV 项目清理和构建脚本"
print_info "项目根目录: $SCRIPT_DIR"
echo

# ================================
# 第一步：清理 bin/ 目录
# ================================
print_step "1. 清理 bin/ 目录"
if [ -d "bin" ]; then
    print_info "删除 bin/ 目录及其内容..."
    rm -rf bin/
    print_success "bin/ 目录已删除"
else
    print_warning "bin/ 目录不存在，跳过"
fi
echo

# ================================
# 第二步：清理 build/ 目录
# ================================
print_step "2. 清理 build/ 目录"
if [ -d "build" ]; then
    print_info "删除 build/ 目录及其内容..."
    rm -rf build/
    print_success "build/ 目录已删除"
else
    print_warning "build/ 目录不存在，跳过"
fi
echo

# ================================
# 第三步：清理配置文件
# ================================
print_step "3. 清理配置文件"
config_files=(
    "*.conf"
)

for pattern in "${config_files[@]}"; do
    files=$(find . -maxdepth 1 -name "$pattern" 2>/dev/null || true)
    if [ -n "$files" ]; then
        print_info "删除配置文件: $pattern"
        rm -f $pattern
        print_success "已删除: $pattern"
    else
        print_warning "未找到: $pattern"
    fi
done
echo

# ================================
# 第四步：清理 Raft 持久化文件
# ================================
print_step "4. 清理 Raft 持久化文件"
persist_files=(
    "raftstatePersist0.txt"
    "raftstatePersist1.txt" 
    "raftstatePersist2.txt"
    "snapshotPersist0.txt"
    "snapshotPersist1.txt"
    "snapshotPersist2.txt"
)

for file in "${persist_files[@]}"; do
    if [ -f "$file" ]; then
        print_info "删除持久化文件: $file"
        rm -f "$file"
        print_success "已删除: $file"
    else
        print_warning "文件不存在: $file"
    fi
done
echo

# ================================
# 第五步：清理其他临时文件
# ================================
print_step "5. 清理其他临时文件"
temp_patterns=(
    "*.log"
    "*.tmp"
    "*~"
    ".DS_Store"
    "core.*"
    "/tmp/raft_node_*_ready"
)

for pattern in "${temp_patterns[@]}"; do
    if [[ "$pattern" == "/tmp/raft_node_*_ready" ]]; then
        # 特殊处理 /tmp 目录下的文件
        files=$(find /tmp -name "raft_node_*_ready" 2>/dev/null || true)
        if [ -n "$files" ]; then
            print_info "删除临时同步文件: /tmp/raft_node_*_ready"
            rm -f /tmp/raft_node_*_ready
            print_success "已删除临时同步文件"
        fi
    else
        files=$(find . -maxdepth 1 -name "$pattern" 2>/dev/null || true)
        if [ -n "$files" ]; then
            print_info "删除临时文件: $pattern"
            rm -f $pattern
            print_success "已删除: $pattern"
        fi
    fi
done
echo

# ================================
# 第六步：创建必要目录
# ================================
print_step "6. 创建必要目录"
directories=(
    "build"
    "bin"
)

for dir in "${directories[@]}"; do
    if [ ! -d "$dir" ]; then
        print_info "创建目录: $dir"
        mkdir -p "$dir"
        print_success "已创建: $dir"
    else
        print_warning "目录已存在: $dir"
    fi
done
echo

# ================================
# 第七步：CMake 配置
# ================================
print_step "7. CMake 配置"
cd build
print_info "运行 cmake 配置..."

if cmake .. -DCMAKE_BUILD_TYPE=Release; then
    print_success "CMake 配置成功"
else
    print_error "CMake 配置失败"
    exit 1
fi
echo

# ================================
# 第八步：编译项目
# ================================
print_step "8. 编译项目"
print_info "开始编译项目..."

# 获取 CPU 核心数用于并行编译
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
print_info "使用 $CORES 个核心进行并行编译"

if make -j$CORES; then
    print_success "项目编译成功"
else
    print_error "项目编译失败"
    exit 1
fi
echo

# ================================
# 第九步：验证构建结果
# ================================
print_step "9. 验证构建结果"
cd "$SCRIPT_DIR"

expected_binaries=(
    "bin/integration_test"
    "bin/server"
    "bin/client"
    "bin/raft_test"
    "bin/unit_tests"
)

all_exist=true
for binary in "${expected_binaries[@]}"; do
    if [ -f "$binary" ]; then
        print_success "✓ $binary"
    else
        print_error "✗ $binary (缺失)"
        all_exist=false
    fi
done

echo
if [ "$all_exist" = true ]; then
    print_success "🎉 所有二进制文件构建成功！"
else
    print_error "❌ 部分二进制文件缺失"
    exit 1
fi

# ================================
# 完成
# ================================
echo
print_success "🚀 清理和构建完成！"
print_info "项目已准备就绪，可以运行测试："
echo -e "  ${CYAN}./bin/integration_test${NC}  - 运行集成测试"
echo -e "  ${CYAN}./bin/unit_tests${NC}       - 运行单元测试"
echo -e "  ${CYAN}./bin/server${NC}           - 启动服务器"
echo -e "  ${CYAN}./bin/client${NC}           - 启动客户端"
echo
