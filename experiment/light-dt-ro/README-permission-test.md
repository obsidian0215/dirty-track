# Dirty-Track 权限变化测试工具

这个目录包含用于验证 dirty-track 内核模块对内存权限变化监控能力的工具。

## 背景

dirty-track 内核模块通过跟踪 soft-dirty 标志位来监测页面的写操作。当页面被写入时，Linux 内核会自动设置页表中的 soft-dirty 标志位。

我们想验证：
1. 只读代码段的权限变化是否能被 dirty-track 被动检测
2. 通过 `mprotect()` 修改权限后，当进程真正写入时，dirty-track 是否能捕获

## 测试原理

1. **只读映射阶段**：创建只读内存映射，尝试写入（预期失败）
2. **权限变化阶段**：使用 `mprotect()` 将只读映射改为读写映射
3. **写入测试阶段**：成功写入映射，生成 soft-dirty 事件
4. **再次修改阶段**：将映射改回只读

通过观察 dirty-track 生成的 dirty-map 文件，可以验证：
- 权限变化本身是否触发 dirty-track 事件
- 实际写入操作是否被正确捕获
- 跟踪期间的完整性

## 工具说明

### C 版本 (`permission-test.c`) - 推荐使用

完整的 C 实现，支持所有 Linux mprotect 操作：

```bash
# 编译
make clean && make

# 运行测试
./permission-test
```

### Python 版本 (`permission-test.py`) - 基础验证

简化版，仅演示基本概念：

```bash
# 运行
python3 permission-test.py
```

## 预期测试流程

1. **前提条件**：
   - 确保 dirty-track 内核模块已加载
   - `/dev/dirty-track` 设备文件存在
   - 临时目录 `/tmp/dirty-maps` 可写

2. **运行测试**：
   ```bash
   cd experiment/light-dt-ro/
   make
   ./permission-test
   ```

3. **验证结果**：
   ```bash
   # 检查 dirty-map 输出
   ls -la /tmp/dirty-maps/permission-test/

   # 分析结果
   ../../dirtymap/user-read-dirtymap /tmp/dirty-maps/permission-test/*.dirtymap
   ```

## 预期观察

### 成功情况：
- 程序正常运行，无错误
- dirty-map 文件被创建
- dirty-map 包含写入操作的记录

### 测试输出示例：
```
=== DIRTY-TRACK PERMISSION CHANGE TEST ===
PID: 12345
Set dirty-map path to: /tmp/dirty-maps/permission-test
Started tracking PID: 12345

--- PHASE 1: INITIAL READONLY MAPPING ---
Created readonly mapping at 0x7f8bcbfe7000, size 4096
Testing write to readonly mapping (expect failure): Write failed as expected (permission denied)

--- PHASE 2: PERMISSION CHANGE ---
Changed permission to read-write at 0x7f8bcbfe7000

--- PHASE 3: WRITE AFTER PERMISSION CHANGE ---
Testing write to read-write mapping (expect success): Write succeeded after permission change

--- PHASE 4: CHANGE BACK TO READONLY ---
Changed permission to readonly at 0x7f8bcbfe7000
Now mapping is readonly again

--- PHASE 5: CLEANUP ---
Test completed!
Check dirty-map files in: /tmp/dirty-maps/permission-test
Use ../../dirtymap/user-read-dirtymap to analyze results
```

### dirty-map 分析：
使用 `user-read-dirtymap` 分析结果：
```bash
Track duration: 5000000 ns
Page address: 0x7f8bcbfe7000, Write count: 1
Page address: 0x7f8bcbfe7004, Write count: 1
...
```

## 技术细节

### 权限变化和 dirty-track

dirty-track 通过以下机制工作：

1. **Soft-dirty 初始化**：在追踪开始时，内核遍历进程的所有页表，清空 soft-dirty 标志位
2. **写 protec 激活**：通过 `pte_wrprotect()` 设置页面为写保护
3. **页错误处理**：当进程写入时，触发页错误，内核记录 soft-dirty，dirty-track 更新 dirty-map
4. **定时清理**：定期再次清空 soft-dirty 标志，以便检测新写入

### 权限变化影响

- **只读 → 读写**：权限允许写入，实际写入时产生页错误，dirty-track 检测
- **读写 → 只读**：内核需要设置写保护，可能影响后续写入
- **权限变化本身**：不直接产生脏页，除非涉及页面复制或迁移

### 验证要点

1. **写入操作检测**：权限变化后的第一次写入是否触发 soft-dirty
2. **多区域跟踪**：验证在权限变化发生前后，不同时间段的页面是否都被正确跟踪
3. **保持完整性**：确保权限变化过程中不丢失已有的脏页信息

## 故障排除

### 常见问题

1. **设备不存在**：
   ```bash
   ls -la /dev/dirty-track
   # 如果不存在，需要加载内核模块
   sudo insmod ../light-dt/dirty-track.ko
   ```

2. **权限不足**：
   ```bash
   # 添加到dialout组或使用sudo运行
   sudo ./permission-test
   ```

3. **临时目录问题**：
   ```bash
   mkdir -p /tmp/dirty-maps
   chmod 755 /tmp/dirty-maps
   ```

4. **Python 版本兼容性**：
   - 使用 Python 3.6+
   - 在 Windows 环境只用于概念验证，完整测试请用 C 版本

## 动态代码修改场景分析

### Python 程序的代码段

Python 解释器与代码段修改的关系：

1. **字节码存储**：
   - Python 字节码存储在堆内存，不是传统的代码段
   - `PyCodeObject` 结构体可修改，但通常不需要

2. **自修改代码**：
   - Python 很少自修改代码段
   - 主要的内存操作是对象创建和数据变更

3. **JIT 相关**：
   - CPython 没有 JIT，可写内存中的对象变更更容易被 dirty-track 捕获

### JIT 编译器的代码段修改

JIT 编译器典型的内存操作模式：

1. **RWX 内存分配**：
   ```c
   mmap(NULL, size, PROT_READ | PROT_WRITE | PROT_EXEC,
        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
   ```

2. **编译阶段**：
   - 写入新生成的机器码到内存
   - 这是可写的操作，会触发 soft-dirty 标志

3. **执行优化**：
   - 执行时发现热点代码路径
   - 生成优化的机器码
   - 再次写入内存，更新代码段

### 测试动态代码场景

使用 `jit-code-modification-test.c` 程序测试：

```bash
# 编译
make jit-test

# 运行
./jit-code-modification-test
```

这个测试模拟：
- 创建可执行内存段（JIT 风格）
- 写入初始机器码
- 动态修改代码（运行时优化）
- 权限调整（写入→执行转换）

### dirty-track 对动态代码的监控

**预期效果**：
- ✅ **代码生成阶段**：所有写入操作都被捕获
- ✅ **权限变化**：`mprotect()` 后的写入被检测
- ✅ **运行时优化**：动态更新的代码写入被监控

**关键发现**：
- JIT 经常使用的 RWX 内存天然可写，写入随时被检测
- 权限变化后的写入操作才能触发软脏页事件
- 不是权限变化本身，而是**写入行为**被 dirty-track 捕获

## 扩展建议

- **多次权限变化**：测试频繁的权限切换
- **大页面测试**：使用透明大页（THP）
- **文件映射测试**：对文件映射而不是匿名映射测试
- **多线程测试**：在多线程环境中验证
- **JIT 优化序列**：测试复杂的方法内联和去优化
- **跨页面代码更新**：验证多页面代码段的修改检测

## 结论

这个测试工具验证了 dirty-track 内核模块对权限变更的监控能力，并提供了完整的测试环境用于：
- 概念验证
- 回归测试
- 性能分析
- 边界情况测试