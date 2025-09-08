# Dirty-Track Testing Framework

## 项目概述

这是一个基于 dirty-track 内核模块的综合性测试框架，用于比较不同内存监控方案的效果。该框架包含内核和用户空间的脏页检测工具，以及专门针对 JIT 编译器的测试用例。

## 测试工具概览

### C 语言程序 (需要编译)
- **dirty-track-ro** - C 语言版本的内核脏页跟踪对比测试
- **user-read-dirtymap** - 读取和分析内核生成 .dirtymap 二进制文件
- **user-read-timestamp** - 解析时间戳数据
- **user-read-warmlist** - 分析热页列表统计信息

### Python 脚本 (开箱即用)
- **dirty-track-comp.py** - Python 综合比较测试工具
- **python-jit-emu.py** - JIT 代码修改模拟测试

## 系统要求

### 最低系统配置
- **操作系统**: Linux (内核版本 4.0+)
- **编译器**: GCC 4.8+
- **Python**: Python 3.6+
- **权限**: 需要 root 权限加载内核模块

### Python 依赖包
```bash
pip3 install psutil numpy
```

### 外部依赖
- **dirty-track.ko**: 内核模块 (在 ../light-dt/ 目录中)
- **soft-dirty**: 用户空间脏页检测工具 (在 ../soft-dirty/ 目录中)

## 快速开始

### 1. 一键构建
```bash
cd experiment/dirty-track
make all
```

### 2. 加载内核模块
```bash
cd ../light-dt
sudo make install
lsmod | grep dirty_track  # 确认模块已加载
ls -la /dev/dirty-track   # 确认设备节点存在
```

### 3. 运行全面测试
```bash
cd ../experiment/dirty-track
make test-comprehensive
```

### 4. 查看结果对比
```bash
make compare-modes
```

## 详细使用指南

### 单个工具的用法

#### 1. 内核脏页跟踪测试 (C 版本)
```bash
./dirty-track-ro
```
- 功能: 执行权限变化和 JIT 风格代码修改测试
- 输出: 直接在终端显示测试结果
- 依赖: 需要加载 dirty-track 内核模块

#### 2. 读取脏页映射文件
```bash
./user-read-dirtymap /tmp/dirty-maps/file.dirmap
```
- 功能: 解析和显示内核生成的 .dirmap 二进制文件内容
- 输出: 页地址和对应的写入次数统计

#### 3. Python 综合比较工具
```bash
python3 dirty-track-comp.py [内存大小MB] [周期数]
python3 dirty-track-comp.py 50 20    # 50MB内存，20个周期
```
- 功能: 先执行 dirty-track 内核测试，再执行 soft-dirty 用户空间测试
- 参数:
  - 内存大小: 每个内存映射的大小 (默认 25MB)
  - 周期数: 每个监控模式下的测试周期 (默认 15)
- 输出: 创建时间戳目录，生成 .dirmap 和 .txt 文件

#### 4. JIT 代码修改模拟
```bash
python3 python-jit-emu.py              # 信息性分析和指南
python3 python-jit-emu.py --actual-test # 实际的内存分配和修改测试
```

### 构建选项

#### 构建全部内容
```bash
make all              # 构建所有工具并检查依赖
make c-targets        # 只构建 C 程序
make python-check     # 检查 Python 环境
make dependencies     # 检查和构建外部依赖
```

#### 不同规模的测试
```bash
make test-tiny        # 10MB, 5周期 - 快速验证
make test-small       # 25MB, 10周期 - 标准测试
make test-medium      # 50MB, 20周期 - 中等规模
make test-large       # 100MB, 30周期 - 大规模测试
```

### 高级用法

#### 内核模块管理
```bash
# 加载模块
sudo modprobe dirty-track

# 卸载模块
sudo rmmod dirty-track

# 查看模块状态
lsmod | grep dirty_track
ls -la /dev/dirty-track
```

#### 结果分析
测试完成后，所有的输出文件都会保存在 `/tmp/dirty-memory-test/` 目录下:

```
/tmp/dirty-memory-test/
├── test-dirty-track-20240908_140000/
│   ├── page_cache.dirmap          # 内核脏页跟踪结果
│   └── metadata.txt               # 相关的元数据
└── test-soft-dirty-20240908_140000/
    ├── soft-dirty-进程ID-场景名.txt  # 用户空间结果
    └── metadata.txt
```

#### 清理操作
```bash
make clean                 # 删除编译的可执行文件
make clean-results         # 删除测试结果目录
make clean-kernel          # 卸载内核模块
make clean-all            # 清理所有内容
```

## 监控方案对比

| 监控方案 | 位置 | 精确度 | 性能开销 | 适用场景 |
|---------|-----|-------|--------|----------|
| dirty-track | 内核 | 高 | 低 | 生产环境监控 |
| soft-dirty | 用户空间 | 中 | 中 | 容器迁移、调试 |

## 故障排除

### 常见问题

1. **Permission denied** 或 **Device not found**
   ```bash
   # 解决方法: 重新加载内核模块
   cd ../light-dt
   sudo make clean
   sudo make install
   ```

2. **Kernel module not found**
   ```bash
   # 确保内核源码编译时包含了 dirty-track 模块
   cd ../light-dt
   lsmod | grep dirty_track
   ```

3. **Python package missing**
   ```bash
   pip3 install psutil numpy
   ```

4. **权限变化不被监控**
   - 权限变化本身不会被监控
   - 实际的内存写入操作才会触发监控

### 调试命令

```bash
# 查看系统日志
dmesg | grep dirty-track

# 检查设备文件权限
ls -la /dev/dirty-track

# 查看正在运行的进程
ps aux | grep soft-dirty
```

## 示例输出

### 成功测试输出示例
```
DIRTY-TRACK VS SOFT-DIRTY COMPREHENSIVE MEMORY TEST
======================================================================
Process PID: 12345
Memory size per test: 25MB
Cycles per mode: 15
Testing both kernel (dirty-track) and userspace (soft-dirty) monitoring

========================= TESTING KERNEL MODE ========================
Mode: kernel
Total mappings created: 234
Total bytes written: 3,141,592,384 (2.93 GB)
Active mappings: 5
Average write rate: 145.67 MB/sec

========================= TESTING SOFT-DIRTY MODE ====================
Mode: soft-dirty
Total mappings created: 231
Total bytes written: 3,123,456,789 (2.91 GB)
Active mappings: 5
Average write rate: 141.12 MB/sec

COMPREHENSIVE COMPARISON COMPLETED!
Results saved in: /tmp/dirty-memory-test/compare-20240908_140000
```

## 开发和贡献

### 项目结构
```
experiment/dirty-track/
├── Makefile                    # 构建系统
├── README.md                  # 本文档
├── dirty-track-ro.c           # C 测试程序
├── dirty-track-comp.py        # Python 主要测试工具
├── python-jit-emu.py          # JIT 测试和分析
├── user-read-dirtymap.c       # 二进制文件解析器
├── user-read-timestamp.c      # 时间戳数据解析器
└── user-read-warmlist.c       # 热页统计解析器
```

### 扩展测试框架

欢迎为该框架添加新的测试用例。请遵循以下原则:

1. **测试隔离**: 每个测试应该独立运行，不相互影响
2. **输出标准化**: 使用统一的目录和文件格式
3. **错误处理**: 提供有意义的错误信息和恢复机制
4. **文档完善**: 新功能应该在 README 中有详细说明

## 许可证

本项目遵循 AGENTS 项目的许可证条款。

## 联系和支持

如果您发现问题或需要帮助，请检查:

1. 该 README 文档的故障排除部分
2. 内核模块和外部工具的构建说明
3. 系统日志中相关的错误信息

最后的更新: 2024-09-08