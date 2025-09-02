# OCI 容器迁移实验指南

本指南描述了如何验证有状态服务的容器迁移兼容性，及如何在迁移实验中使用mig-scripts工具集。

## 支持的有状态服务

### Redis
- 镜像: `docker.io/library/redis:latest`
- 端口: 6379 (默认)
- 特点: 内存键值存储，持久化状态

### InfluxDB
- 镜像: `docker.io/library/influxdb:latest`
- 端口: 8086 (默认)
- 特点: 时序数据库，海量时间序列数据

### Elasticsearch
- 镜像: `docker.io/library/elasticsearch:latest`
- 端口: 9200 (默认)
- 特点: 分布式搜索和分析引擎

### 内存工作负载 (测试用途)
- mem-workload1: 多线程内存访问模式模拟
- mem-workload2: 脏页测试程序

## 服务启动流程

### 1. 构建 OCI Bundle

```bash
# Redis 构建
cd experiment/migration/redis
./build.sh

# InfluxDB 构建
cd experiment/migration/influxdb
./build.sh

# Elasticsearch 构建
cd experiment/migration/elasticsearch
./build.sh

# 内存负载编译
cd experiment/migration/mem-workload1
./build.sh
```

构建后会在各服务目录下生成：
- redis/ - Redis 的 OCI bundle
- influxdb/ - InfluxDB 的 OCI bundle
- elasticsearch/ - Elasticsearch 的 OCI bundle
- mem-workload1/ - 编译后的二进制

### 2. 准备容器

```bash
# 进入容器目录
cd /runc/containers

# 创建容器目录和 OCI bundle 备份
mkdir -p redis redis.bak influxdb influxdb.bak elasticsearch elasticsearch.bak

# 复制 OCI bundle 并启动容器
cp -r experiment/migration/redis/* /runc/containers/redis/
cp -r experiment/migration/influxdb/* /runc/containers/influxdb/
cp -r experiment/migration/elasticsearch/* /runc/containers/elasticsearch/

# 初始化控制台套接字 (多数量可选)
recvtty -m single /runc/containers/redis/console.sock &
recvtty -m single /runc/containers/influxdb/console.sock &
recvtty -m single /runc/containers/elasticsearch/console.sock &
```

### 3. 启动容器

```bash
# Redis
runc run --console-socket /runc/containers/redis/console.sock -d -b /runc/containers/redis redis

# InfluxDB (可能需要额外配置)
runc run --console-socket /runc/containers/influxdb/console.sock -d -b /runc/containers/influxdb influxdb

# Elasticsearch (可能需要内存配置)
runc run --console-socket /runc/containers/elasticsearch/console.sock -d -b /runc/containers/elasticsearch elasticsearch
```

内存工作负载无需 OCI bundle，直接使用 chk_restore.py 迁移二进制进程。

## 迁移实验流程

### 使用 chk_restore.py (单一节点迁移)

chk_restore.py 支持不同迁移技术，在同一节点上进行完整的容器迁移测试。

#### 参数说明：
- `container_name`: 容器名（对应 OCI bundle 目录名）
- `-pre`: 启用预拷贝迁移
- `-post`: 启用后拷贝迁移
- `-dm`: 使用脏页映射（需要内核模块支持）
- `-i max_iterations`: 最大预拷贝迭代次数
- `-tc time_constraint`: 时间限制 (ms)

#### 示例：

```bash
# Redis 冷迁移
python3 chk_restore.py redis

# Redis 预拷贝迁移
python3 chk_restore.py redis -pre -i 5

# Redis 混合迁移（预+后拷贝）
python3 chk_restore.py redis -pre -post -dm -i 5

# InfluxDB 预拷贝迁移
python3 chk_restore.py influxdb -pre -dm

# Elasticsearch 预拷贝迁移
python3 chk_restore.py elasticsearch -pre -dm

# 内存工作负载迁移
# 先运行内存工作负载进程，然后获取其 PID
./mem-workload1/memory_workload 0  # 启动工作负载
PID=$(pgrep memory_workload)
python3 chk_restore.py mem-workload1 -pre -dm --container-pid $PID
```

### 使用 mig-scripts (分布式迁移)

mig-scripts 套件支持源节点和目标节点的分布式迁移。

#### 参数说明：
- `-c container`: 容器名
- `-t tool`: 测试工具（如 ycsb）
- `-s source_ip`: 源节点 IP
- `-d dest_ip`: 目标节点 IP
- `--client-ip`: 客户端节点 IP (可选)
- `--virtual-ip`: 虚拟 IP (可选)

#### 环境准备：
1. 在源和目标节点上安装并配置 CRIU/runc
2. 启动内核脏页跟踪模块（如果使用 dirty-map）
3. 配置网络以允许节点间通信

#### 示例：
```bash
# 启动目标节点监听
python3 mig-scripts/destination.py  # 在目标节点运行

# 源节点执行迁移 (Redis)
python3 mig-scripts/start.py \
  -c redis \
  -t ycsb \
  -s 192.168.1.10 \
  -d 192.168.1.20

# 源节点执行迁移 (InfluxDB)
python3 mig-scripts/start.py \
  -c influxdb \
  -s 192.168.1.10 \
  -d 192.168.1.20

# 源节点执行迁移 (Elasticsearch)
python3 mig-scripts/start.py \
  -c elasticsearch \
  -s 192.168.1.10 \
  -d 192.168.1.20
```

## 基准测试执行

### Redis 基准测试

```bash
# 使用内置 redis-benchmark
cd /runc/containers/redis  # 需要映射端口或直接访问
redis-benchmark -h localhost -p 6379 -c 50 -n 10000 -t SET,GET

# 或自定义脚本
./experiment/migration/redis/benchmark.sh \
  --host localhost \
  --port 6379 \
  --clients 50 \
  --requests 10000
```

### InfluxDB 基准测试

```bash
# 使用 Python 基准测试脚本
python3 ./experiment/migration/influxdb/benchmark.py \
  --threads 10 \
  --operations 1000 \
  --url http://localhost:8086 \
  --token my-token \
  --org my-org \
  --bucket benchmark \
  --test-mode write
```

### Elasticsearch 基准测试

```bash
# 使用 Python 基准测试脚本
python3 ./experiment/migration/elasticsearch/benchmark.py \
  --threads 10 \
  --operations 1000 \
  --es-host localhost \
  --es-port 9200 \
  --index-name benchmark-test \
  --test-mode index
```

### 负载测试

```bash
# Redis 负载
./experiment/migration/redis/load.sh

# InfluxDB 负载 (Python 脚本)
python3 ./experiment/migration/influxdb/load.py \
  --points 1000 \
  --batch_size 100 \
  --org my-org \
  --bucket load-test
```

## 与 mig-scripts 的集成

### chk_restore.py 集成：
- 直接使用容器名作为参数
- 支持脏页映射优化迁移
- 提供迁移时间和大小统计

### source.py/destination.py 集成：
- source.py 在源节点准备和启动迁移
- destination.py 在目标节点接收和恢复
- 支持多迁移阶段（预拷贝、後拷贝）
- 自动处理网络传输和状态同步

### start.py 自动化：
- 自动化准备源和目标环境
- 支持多种迁移类型（预拷贝、后拷贝、混合）
- 集成了负载和基准测试工具
- 生成实验统计数据

## 关键配置

### 内核模块（dirty-map 支持）
```bash
# 编译 dirty-track 内核模块
cd light-dt
make
sudo make install

# 加载模块
sudo insmod light-dt/dirty-track.ko

# 启动服务 recvtty 和 ’runc run’ 前确保控制台套接字配置正确
```

### 网络配置（多节点实验）
- 确保源和目标节点间端口 12345+ 可用
- 配置防火墙允许迁移通信
- 使用 nc 进行数据传输验证

### 资源配置
- Elasticsearch 需要至少 2GB 内存
- CRIU 可能需要额外 tmpfs 空间
- rsyslog/spigot 为进程使用提供控制台访问

## 故障排除

### 常见问题：
1. **criu/binfmt_misc** 缺失：确保 CRIU 和 binfmt 支持已安装
2. **容器控制台套接字错误**：确认 recvtty 正在运行
3. **权限问题**：使用 sudo 运行管理操作
4. **网络问题**：检查防火墙和端口配置
5. **内核模块**：验证 dirty-track.ko 已加载并配置设备节点

### 调试：
- 检查容器状态：`runc ps [container_name]`
- 查看 CRIU 日志：`criu -d -v4 -o criu.log [...]`
- 验证 OCI bundle：`runc spec --rootless`，然后检查生成的文件结构

这个指南提供了完整的工作流程，从构建和准备到迁移和测试不同有状态服务的容器。