# OCI 容器迁移实验指南

本指南描述了如何验证有状态服务的容器迁移兼容性，及如何在迁移实验中使用最新的bench脚本工具集。

## 支持的有状态服务

### Redis
- 镜像: `docker.io/library/redis:latest`
- 端口: 6379 (默认)
- 特点: 内存键值存储，持久化状态，支持多种数据结构

### InfluxDB
- 镜像: `docker.io/library/influxdb:latest`
- 端口: 8181 (默认)
- 特点: 时序数据库，海量时间序列数据

### Elasticsearch
- 镜像: `docker.io/library/elasticsearch:latest`
- 端口: 9200 (默认)
- 特点: 分布式搜索和分析引擎

### 高级基准测试工作负载
- **Car Telematics (bench_cartelem.py)**: 高级车联网遥测基准测试
  - 生成车辆实时数据：GPS位置、速度、燃油水平、发动机温度、诊断码
  - 支持多种驾驶模式：城市驾驶、高速公路、停止启动
  - 数据规模扩展：可配置负载大小，支持多种分布模式
  - Redis支持：使用Stream数据结构写入车辆遥测数据
  - InfluxDB支持：时序数据存储，优化查询性能

- **Sensor Aggregation (bench_sensoragg.py)**: 传感器聚合基准测试
  - 多传感器类型支持：温度、湿度、压力、振动传感器
  - 真实传感器模拟：漂移、电池电量消耗、校准状态
  - 数据规模扩展：每设备配置传感器数量，扩展到目标负载大小
  - 查询模式：范围查询、计数查询、最值查询
  - Redis存储：使用有序集合(ZSET)进行时间序列聚合

- **Video Cache (bench_video_cache.py)**: 视频分析缓存基准测试
  - 模拟视频推理结果缓存：帧ID、检测对象、置信度分数
  - 缓存策略：SET操作配合TTL和回退写入
  - GET/SET操作混合，模拟读写负载
  - Redis缓存：作为视频分析服务的主缓存层

### 传统基准测试
- **Redis Benchmark (redis-benchmark)**: Redis内置基准测试工具
- **Elasticsearch Benchmark (bench.py)**: ES索引和搜索性能测试
- **内存工作负载 (simple-mem)**: 多线程内存访问模式模拟

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
# 启动目标节点监听（在目标节点运行）
python3 mig-scripts/destination.py

# 源节点执行迁移（Redis, 使用专用 orchestrator）
python3 mig-scripts/redis_test.py --scene video --runs 1 --source-ip 192.168.1.10 --dest-ip 192.168.1.20

# 源节点执行迁移（InfluxDB）
python3 mig-scripts/influxdb_test.py --scene video --runs 1 --source-ip 192.168.1.10 --dest-ip 192.168.1.20

# 源节点执行迁移（Elasticsearch, YCSB 风格）
python3 mig-scripts/elasticsearch-ycsb.py --runs 1 --source-ip 192.168.1.10 --dest-ip 192.168.1.20

# 说明：原先的 `start.py` 已归档（见 `mig-scripts/archived/`）；建议使用上述更专用的测试入口。
```

## 基准测试执行

### 高级基准测试

#### 车联网遥测基准测试 (bench_cartelem.py)

Redis版本 - 车辆数据写入：
```bash
# 城市驾驶模式测试
python3 ./experiment/migration/redis/bench_cartelem.py \
  --redis-host 127.0.0.1 \
  --threads 8 \
  --duration 30 \
  --vehicle-pattern highway \
  --payload-size 2KB \
  --connect-timeout 5 \
  --stream vehicle:telemetry

# 高速路驾驶模式（高负载）
python3 ./experiment/migration/redis/bench_cartelem.py \
  --redis-host 127.0.0.1 \
  --threads 16 \
  --duration 60 \
  --vehicle-pattern highway \
  --payload-size 4KB \
  --rps 1000 \
  --size-distribution normal
```

InfluxDB版本 - 车辆时序数据：
```bash
# 车队管理平台测试
python3 ./experiment/migration/influxdb/bench_cartelem.py \
  --influx-url http://localhost:8181 \
  --token my-token \
  --org my-org \
  --bucket vehicle-data \
  --threads 4 \
  --duration 60 \
  --vehicle-pattern normal_city \
  --payload-size 1KB \
  --rps 100 \
  --read-pct 10 \
  --size-distribution uniform

# 数据中心聚合监控
python3 ./experiment/migration/influxdb/bench_cartelem.py \
  --influx-url http://localhost:8181 \
  --token my-token \
  --org my-org \
  --bucket datacenter-telemetry \
  --threads 8 \
  --duration 90 \
  --vehicle-pattern highway \
  --payload-size 3KB \
  --rps 500
```

参数说明：
- `--vehicle-pattern`: 驾驶模式 [normal_city/highway/stop_go]
- `--payload-size`: 目标负载大小，带单位（例如 256B, 16KB, 1MB），默认单位为 KB
- `--size-distribution`: 数据大小分布模式 [uniform/normal/zipf]
- `--rps`/ `--max-requests-per-second`: 每秒最大请求数限制
- `--threads`: 并发工作线程数
- `--duration`: 测试持续时间(秒)

#### 传感器聚合基准测试 (bench_sensoragg.py)

Redis聚合测试：
```bash
# 多传感器IoT平台测试
python3 ./experiment/migration/redis/bench_sensoragg.py \
  --redis-host 127.0.0.1 \
  --threads 8 \
  --duration 30 \
  --read-pct 20 \
  --sensors-per-device 5 \
  --sensor-types temperature,humidity,pressure,vibration \
  --rps 1000

# 大规模工业物联网监控
python3 ./experiment/migration/redis/bench_sensoragg.py \
  --redis-host 127.0.0.1 \
  --threads 16 \
  --duration 60 \
  --read-pct 15 \
  --sensors-per-device 20 \
  --sensor-types temperature,vibration,humidity,pressure \
  --payload-size 2KB \
  --environmental-noise 0.1
```

参数说明：
- `--sensors-per-device`: 每设备传感器数量
- `--sensor-types`: 传感器类型列表
- `--environmental-noise`: 环境噪音水平
- `--read-pct`: 读操作百分比

#### 视频缓存基准测试 (bench_video_cache.py)

Redis缓存负载：
```bash
# 智能视频监控平台测试
python3 ./experiment/migration/redis/bench_video_cache.py \
  --redis-host 127.0.0.1 \
  --threads 12 \
  --duration 45 \
  --write-pct 80 \
  --ttl 300 \
  --camera-count 20 \
  --inference-model yolov5_medium \
  --fallback-rate 5

# 高并发零售客流分析
python3 ./experiment/migration/redis/bench_video_cache.py \
  --redis-host 127.0.0.1 \
  --threads 16 \
  --duration 30 \
  --write-pct 75 \
  --ttl 180 \
  --camera-count 50 \
  --payload-size 4KB \
  --rps 2000
```

### 传统基准测试

#### Redis 内置基准测试
```bash
# 进入容器目录
cd /runc/containers/redis
redis-benchmark -h localhost -p 6379 -c 50 -n 10000 -t SET,GET
```

#### Elasticsearch 基准测试
```bash
python3 ./experiment/migration/elasticsearch/bench.py \
  --threads 10 \
  --operations 1000 \
  --es-host localhost \
  --es-port 9200 \
  --index-name benchmark-test \
  --test-mode index \
  --field-count 5
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

## 高级脚本功能和配置

### 数据规模扩展
所有bench_*.py脚本都支持：
- **Payload Size Control**: 通过`--payload-size`指定目标负载大小（带单位，例如 16KB 或 1MB）
- **Size Distribution**: 支持uniform/normal/zipf分布模式，模拟真实数据模式
- **Sensor/Attribute Scaling**: 可配置每设备传感器数量，数据结构扩展

### 真实性模拟
- **车辆行为模式**: normal_city/highway/stop_go，模拟不同驾驶场景
- **传感器漂移**: 温度/时间相关的传感器漂移模拟
- **电池消耗**: 电量随时间缓慢下降的现实模拟
- **地理位置**: 基于真实城市范围的GPS坐标生成

### 连接和性能优化
- **连接池管理**: Redis连接池避免频繁连接开销
- **超时配置**: connect_timeout, socket_timeout, pool_timeout
- **速率控制**: 通过`--rps`参数限制每秒请求数，模拟生产环境负载
- **批量操作**: Elasticsearch支持批量索引优化

### 监控和统计
- **实时监控**: 每秒输出TPS(事务每秒)、平均延迟、P95延迟
- **延迟分布**: 详细的延迟分位数统计(平均值/P50/P90/P99/最大值)
- **操作统计**: 成功/失败操作计数和成功率
- **内存监控**: Redis操作内存使用情况报告

### 混合读写负载
- **Read/Write Mix**: 通过`--read-pct`配置读写操作比例
- **查询模式**: 范围查询/计数查询/最值查询/文本搜索
- **缓存策略**: GET/SET混合，TTL管理，write-through策略

## 最佳实践

### 基准测试执行建议

#### 性能测试配置
- **并发线程数**: 根据CPU核心数设置，通常threads = CPU_cores * 2
- **测试时长**: 建议30-60秒获取稳定的性能数据
- **负载大小**: 从1KB开始逐步增加，观察性能拐点

#### 实时监控要点
- 关注**P95延迟**而非平均延迟，更能反映用户体验
- TPS波动小于10%时认为达到稳定状态
- 记录峰值内存使用和CPU饱和度

#### 迁移实验设置
- **预热阶段**: 运行基准测试10-30秒让系统达到稳定状态
- **迁移窗口**: 在低峰期执行，避免业务高峰期
- **监测周期**: 每5秒监测一次关键指标

### 示例用例

#### IoT时序数据迁移
```bash
# 源节点预热
python3 ./experiment/migration/influxdb/bench_cartelem.py \
  --influx-url http://localhost:8181 \
  --threads 16 \
  --duration 30 \
  --vehicle-pattern highway \
  --rps 100

# 执行迁移
python3 chk_restore.py influxdb -pre -dm -i 3

# 目标节点恢复
python3 ./experiment/migration/influxdb/bench_cartelem.py \
  --influx-url http://target-node:8181 \
  --threads 16 \
  --duration 30 \
  --vehicle-pattern highway
```

#### 实时缓存服务迁移
```bash
# 传感器数据写入负载
python3 ./experiment/migration/redis/bench_sensoragg.py \
  --redis-host localhost \
  --threads 12 \
  --duration 45 \
  --read-pct 25 \
  --rps 200

# 执行热迁移
python3 chk_restore.py redis -pre -dm -tc 500

# 服务切换验证
python3 ./experiment/migration/redis/bench_sensoragg.py \
  --redis-host target-host \
  --threads 12 \
  --duration 30
```

#### 大数据分析迁移
```bash
# Elasticsearch负载测试
python3 ./experiment/migration/elasticsearch/bench.py \
  --threads 20 \
  --operations 2000 \
  --test-mode mixed

# 执行混合迁移
python3 chk_restore.py elasticsearch -pre -post -dm -i 5

# 索引重建验证
python3 ./experiment/migration/elasticsearch/bench.py \
  --test-mode index \
  --threads 20
```

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