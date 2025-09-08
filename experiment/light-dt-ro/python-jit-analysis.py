#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Python JIT 和动态代码分析 - 针对 dirty-track 监控
分析 Python 解释器的代码生成和内存操作特性
"""

import mmap
import os
import sys
import time
import psutil
import ctypes
import struct
import subprocess
import tempfile
from typing import Optional

def create_code_only_segment():
    """创建只读代码段，模拟传统可执行文件"""
    pagesize = 4096

    # 分配内存页
    addr = ctypes.c_void_p()
    size = ctypes.c_size_t(pagesize)

    # 使用 mmap 创建内存映射
    libc = ctypes.CDLL('c', use_errno=True)
    libc.mmap.restype = ctypes.c_void_p

    # 创建私有匿名映射，初始为读写
    mapping = libc.mmap(ctypes.c_void_p(0), size, 3,  # PROT_READ | PROT_WRITE
                       0x22,  # MAP_PRIVATE | MAP_ANONYMOUS
                       -1, 0)

    if mapping == -1:
        print("Failed to create memory mapping")
        return None

    print(f"Created RW mapping at address: 0x{mapping:016x}")

    # 写入模拟代码数据
    code_data = b'\x55\x48\x89\xe5\xb8\x2a\x00\x00\x00\x5d\xc3'  # 简单函数：返回42
    ctypes.memmove(mapping, code_data, len(code_data))

    print(f"Wrote {len(code_data)} bytes of simulated code")

    # 改变权限为只读 + 执行
    result = libc.mprotect(mapping, size, 5)  # PROT_READ | PROT_EXEC
    if result == -1:
        errno = ctypes.get_errno()
        print(f"mprotect failed with errno {errno}")
        return None

    print("Changed permissions to read-only + execute")
    return mapping, pagesize

def demonstrate_python_code_objects():
    """演示 Python 代码对象的特性"""
    print("\n=== Python 代码对象分析 ===")

    def sample_function():
        return 42

    # 获取代码对象
    code = sample_function.__code__

    print(f"Function code object: {code}")
    print(f"Code object type: {type(code)}")
    print(f"Code object address: {hex(id(code))}")
    print(f"Bytecode size: {len(code.co_code)} bytes")
    print(f"Constants: {code.co_consts}")
    print(f"Names: {code.co_names}")

    # Python 代码对象是可修改的，但通常不会被修改
    print("Note: Python code objects are mutable but rarely modified at runtime")
    print("Main memory operations are object allocation and data mutation")

    return code

def demonstrate_dynamic_codetype_modification():
    """演示使用 ctypes 的动态代码生成和修改"""
    print("\n=== 使用 ctypes 的动态代码修改 ===")

    # 创建模拟的机器码
    # x86-64: mov rax, 42; ret
    machine_code = b'\x48\xc7\xc0\x2a\x00\x00\x00\xc3'

    # 分配可执行内存
    pagesize = os.sysconf('SC_PAGESIZE') if hasattr(os, 'sysconf') else 4096

    libc = ctypes.CDLL('c', use_errno=True)

    # 分配 RWX 内存（JIT 典型做法）
    code_addr = libc.mmap(ctypes.c_void_p(0), pagesize,
                         7,  # PROT_READ | PROT_WRITE | PROT_EXEC
                         0x22,  # MAP_PRIVATE | MAP_ANONYMOUS
                         -1, 0)

    if code_addr == -1:
        print("Failed to allocate executable memory")
        return None

    print(f"Allocated RWX memory at: 0x{code_addr:016x}")

    # 写入初始代码
    ctypes.memmove(code_addr, machine_code, len(machine_code))
    print(f"Initial code written: mov rax, 42; ret")

    # 定义函数指针
    func_type = ctypes.CFUNCTYPE(ctypes.c_int64)
    func = func_type(code_addr)

    # 执行初始代码
    result = func()
    print(f"Initial execution result: {result}")

    # 动态修改代码 (JIT 优化模拟)
    new_machine_code = b'\x48\xc7\xc0\xe8\x03\x00\x00\xc3'  # mov rax, 1000; ret
    ctypes.memmove(code_addr, new_machine_code, len(new_machine_code))
    print("Modified code to: mov rax, 1000; ret (JIT optimization simulation)")

    # 执行修改后的代码
    result = func()
    print(f"Modified execution result: {result}")

    # 清理
    libc.munmap(code_addr, pagesize)
    print("Cleaned up executable memory")

    return True

def create_dirty_track_test_example():
    """创建dirty-track测试示例"""
    print("\n=== 推荐的 dirty-track 测试流程 ===")

    steps = [
        "1. 启动 dirty-track 对当前进程的监控",
        "2. 执行 dynamic_codetype_modification() 函数",
        "3. 检查 dirty-track 生成的 dirty-map 文件",
        "4. 分析哪些内存写入操作被捕获",
        "5. 验证 RWX 内存分配和修改的跟踪覆盖率"
    ]

    for step in steps:
        print(f"   {step}")

    print("\n预期结果：")
    print("   - 代码写入阶段的所有操作被记录")
    print("   - 动态修改阶段被检测")
    print("   - 内存地址和写入次数在 dirty-map 中")

def main():
    print("Python JIT 和动态代码分析工具")
    print("=" * 50)

    if len(sys.argv) > 1 and sys.argv[1] == "--actual-test":
        print("运行实际的 dirty-track 测试...")
        # 这里可以集成实际的 dirty-track 测试
        demonstrate_dynamic_codetype_modification()
        create_dirty_track_test_example()
        return

    # 信息性分析
    demonstrate_python_code_objects()

    if hasattr(ctypes.CDLL('c', use_errno=True), 'mmap'):
        demonstrate_dynamic_codetype_modification()
    else:
        print("\n注意：完整的动态代码修改测试需要 Linux 环境")

    create_dirty_track_test_example()

    print("\n" + "=" * 50)
    print("CONCLUSION:")
    print("=" * 50)
    print("Python:")
    print("   • 不直接修改传统代码段")
    print("   • 内存操作主要在堆上")
    print("   • dirty-track 更容易捕获对象和数据变更")
    print("\nJIT:")
    print("   • 创建 RWX 内存段用于机器码")
    print("   • 动态生成和修改代码")
    print("   • 所有写入操作会被 dirty-track 检测")
    print("\nDirty-track监控重点：")
    print("   • 不是权限变化本身")
    print("   • 是权限变化后的实际写入行为")

if __name__ == "__main__":
    main()