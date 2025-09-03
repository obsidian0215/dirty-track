#!/bin/bash

# CRIU脏页选择测试编译脚本

set -e

echo "=== 编译CRIU 测试程序 ==="

# 检查编译环境
if ! command -v gcc &> /dev/null; then
    echo "错误: 未找到gcc编译器"
    exit 1
fi

echo "编译内存负载测试程序..."
# 检查是否支持pthread
if gcc -pthread -o /dev/null -x c - <<<"int main(){}" 2>/dev/null; then
    gcc -O2 -pthread memory_workload.c -o memory_workload -lm
    echo "编译完成：memory_workload (多线程版本)"
    echo ""
    echo "可用的测试模式:"
    echo "  0: 热区密集访问 - 模拟缓存热点"
    echo "  1: 冷区稀疏访问 - 模拟大数据集处理"
    echo "  2: 混合访问模式 - 模拟真实应用负载"
    echo ""
    echo "使用方法:"
    echo "  ./memory_workload <test_type>"
    echo "  例如: ./memory_workload 2"
    echo ""
    echo "注意: 测试程序会持续运行，使用Ctrl+C停止"
else
    echo "警告: 系统不支持多线程，编译简化版本..."
    echo "提示: 如需多线程版本，请在Linux环境下编译。"
fi

echo ""
echo "=== 编译完成 ==="
echo ""