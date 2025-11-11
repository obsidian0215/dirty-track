# InfluxDB 基准测试脚本

该目录包含用于InfluxDB的时间序列数据基准测试脚本，适用于物联网、传感器监控等时间序列应用场景。


## 🚀 核心特性

### 数据大小控制 (Data Size Control)
防止数据库无限增长，支持通过retention policy间接管理：
```bash
# 设置1小时retention，自动控制数据库大小
python3 bench_cartelem.py --retention-policy 1h --rps 1000

# 验证bucket配置
INFO: Checking bucket 'sensor-data' retention policy...
INFO: Bucket configured with 1h retention policy
```

⚠️ InfluxDB脚本不支持直接设置最大数据库大小
- 📉 **不直接支持**: 无`--target-db-size-mb`参数
- 📋 **间接控制**: 通过设置更长的retention policy来增加数据库大小
- 🔄 **替代方案**: 使用`--retention-policy 168h`（7天）或更长来间接控制大小

## 依赖安装

```bash
pip install influxdb-client
```

## 数据过期验证方法

### 脚本执行时的验证日志
```bash
# 脚本运行时会显示数据过期配置信息
INFO: Checking bucket 'vehicle-data' retention policy...
INFO: Bucket configured with 1h retention policy
INFO: Desired retention policy: 1h - matches current configuration

# 如果配置不匹配会显示警告
WARNING: Bucket 'sensor-data' current retention: 3600s, desired: 7200s
INFO: Consider updating bucket retention policy manually
```

### InfluxDB CLI验证过期设置

1. **连接到InfluxDB CLI**：
```bash
influx v2 query -t YOUR_TOKEN --org YOUR_ORG
```

2. **查看所有buckets的retention policy**：
```bash
buckets()
  |> rename(columns: {name: "Bucket_Name", retentionPeriod: "Retention_Hours"})
  |> map(fn: (r) => ({r.Bucket_Name, Retention_Hours: int(v: r.Retention_Hours) / 3600000000000}))
```

3. **查看特定bucket的详细信息**：
```bash
buckets()
  |> filter(fn: (r) => r.name == "vehicle-data")
  |> rename(columns: {retentionPeriod: "Retention_Nanoseconds"})
```

4. **监控数据删除**：
```bash
# 查询bucket中的数据量随时间变化
from(bucket: "vehicle-data")
  |> range(start: -24h)
  |> count()
```

### 直接API验证过期设置

```bash
# 使用curl验证bucket配置
curl -X GET "http://localhost:8086/api/v2/buckets" \
  -H "Authorization: Token YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  | jq '.buckets[] | {name, retentionRules}'
```

### 测试数据过期效果

1. **写入测试数据**：
```bash
python3 bench_cartelem.py --influx-url http://localhost:8181 \
  --token my-token --org my-org --bucket test-retention \
  --retention-policy 5m --threads 2 --duration 30
```

2. **让retention policy生效**：
```bash
# 等待5分钟后检查数据
influx v2 query -t YOUR_TOKEN --org YOUR_ORG \
  -q "from(bucket: \"test-retention\") |> range(start: -10m) |> count()"
```

3. **检查数据是否按预期清理**：
```bash
# 如果retention policy生效，5分钟后的数据应该已被清理
# 查询结果应该显示逐渐减少的记录数
```

## 脚本概览

### bench_cartelem.py
**车联网遥测数据InfluxDB基准测试**

模拟车联网数据写入InfluxDB时间序列存储，包含车辆GPS、速度、燃油、诊断等真实遥测数据。

#### 基本用法
```bash
python3 bench_cartelem.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket vehicle-data --threads 8 --duration 30
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8181)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: vehicle-data)

##### 数据规模扩展参数
- `--payload-size`: 目标负载大小，带单位（例如 256B, 16KB, 1MB） (默认: 1KB)
- `--size-distribution`: 数据大小分布 (uniform/normal/zipf) (默认: uniform)

##### 数据类型真实性参数
- `--vehicle-pattern`: 驾驶模式 (normal_city/highway/stop_go) (默认: normal_city)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--read-pct`: 读操作百分比 (默认: 10)

##### 速率控制参数
- `--rps` 或 `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

#### 使用示例
```bash
# 车队管理平台测试
python3 bench_cartelem.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket fleet-health --threads 12 --duration 45 \
    --vehicle-pattern highway --payload-size 3KB

# 城市物流车辆监控测试
python3 bench_cartelem.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket logistics-monitor --threads 8 --duration 30 \
    --vehicle-pattern stop_go --payload-size 2KB --rps 200

# 高速交通数据分析
python3 bench_cartelem.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket traffic-analytics --threads 16 --duration 60 \
    --vehicle-pattern highway --payload-size 4KB --read-pct 5
```

---

### bench_sensoragg.py
**传感器聚合数据InfluxDB基准测试**

模拟IoT传感器数据写入InfluxDB，支持多传感器类型和环境噪音模拟。

#### 基本用法
```bash
python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket sensor-data --threads 8 --duration 30 --read-pct 10
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8181)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: sensor-data)

##### 数据规模扩展参数
- `--payload-size`: 目标负载大小，带单位（例如 256B, 16KB, 1MB） (默认: 1KB)
- `--sensors-per-device`: 每个设备传感器数量 (默认: 5)

##### 数据类型真实性参数
- `--sensor-types`: 传感器类型列表 (temperature,humidity,pressure,vibration) (默认: temperature,humidity,pressure,vibration)
- `--environmental-noise`: 环境噪音水平 (默认: 0.05)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--read-pct`: 读操作百分比 (默认: 10)

##### 速率控制参数
- `--rps` 或 `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

##### 数据生命周期管理参数
- `--retention-policy`: bucket保留策略 (默认: 1h，即1小时)
  - 支持格式: 1h, 24h, 7d, 30d 等

##### 数据生命周期管理参数
- `--retention-policy`: bucket保留策略 (默认: 1h，即1小时)
  - 支持格式: 1h, 24h, 7d, 30d 等
  - ⚠️ **重要**: 脚本会自动与InfluxDB交互，验证和配置bucket的retention policy
  - 🔄 **自动处理**: 检查现有bucket配置，如果与设置不匹配则提醒更新
  - ⚠️ **重要**: 脚本会自动与InfluxDB交互，验证和配置bucket的retention policy
  - 🔄 **自动处理**: 检查现有bucket配置，如果与设置不匹配则提醒更新

#### 使用示例
```bash
# 多传感器物联网平台测试
python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket iot-sensors --threads 16 --duration 60 \
    --sensors-per-device 12 --rps 500

# 工业环境传感器测试（高噪声）
python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket industrial-mon --threads 8 --duration 30 \
    --sensor-types temperature,vibration,pressure --environmental-noise 0.15

# 大规模物联网数据采集测试
python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket large-scale-iot --threads 32 --duration 45 \
    --sensors-per-device 20 --payload-size 5KB --read-pct 5
```

---

### bench_video_cache.py
**视频分析缓存数据InfluxDB基准测试**

将视频分析推理结果存储到InfluxDB的时间序列数据库，用于AI视频分析平台的数据存储。

#### 基本用法
```bash
python3 bench_video_cache.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket video-analytics --threads 8 --duration 30
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8181)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: video-analytics)

##### 数据规模扩展参数
- `--payload-size`: 目标负载大小，带单位（例如 256B, 16KB, 1MB） (默认: 2KB)

##### 数据类型真实性参数
- `--camera-count`: 摄像头数量 (默认: 10)
- `--inference-model`: 推理模型类型 (yolov5_small/medium/ssd_mobile) (默认: yolov5_medium)
- `--objects-per-frame`: 每帧检测物体数量 (默认: 3)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)

#### 使用示例
```bash
# 智能城市视频监控平台测试
python3 bench_video_cache.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket smart-city-video --threads 20 --duration 45 \
    --camera-count 100 --objects-per-frame 8 --inference-model yolov5_medium

# 交通监控系统测试
python3 bench_video_cache.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket traffic-monitoring --threads 12 --duration 30 \
    --camera-count 50 --inference-model ssd_mobile

# 零售客流分析平台测试
python3 bench_video_cache.py --influx-url http://localhost:8181 --token my-token \
    --org my-org --bucket retail-analytics --threads 8 --duration 60 \
    --objects-per-frame 6 --payload-size 3KB
```

---

## 输出统计

InfluxDB脚本提供与Redis一致的性能指标和格式：

### 实时监控输出格式
```
Thread 1-5: ops=5000, total_time=2.45s, avg_latency=0.49ms, throughput=2040.82 ops/s
```

### 最终统计摘要
- **总体统计**: 总操作数、总时间、成功率
- **延迟分布**: 平均延迟、P50、P95等百分位延迟
- **吞吐量**: 每秒操作数

---

## 重要特性

### 时间序列优化
- 使用纳秒精度时间戳
- 支持Flux查询语言的高级查询
- 标签(tag)和字段(field)的分离设计优化存储效率

### 并发写入
- 异步写入API支持
- 批量写入优化
- 连接池管理

### 查询能力
- 范围查询支持
- 聚合操作支持
- 多度量联合查询

---

## 适用场景

- **物联网数据平台**: 海量传感器数据存储和实时分析
- **工业监控系统**: 生产设备状态监控和异常检测
- **车联网平台**: 车辆遥测数据存储和车队管理
- **视频分析平台**: AI推理结果时间序列存储
- **智能城市建设**: 城市基础设施监测数据
- **金融市场数据**: 高频交易数据存储和分析

---

## 配置建议

### 生产环境配置
```bash
# InfluxDB配置文件建议
[influxdb]
# 数据存储路径
data-dir = "/var/lib/influxdb/data"

# WAL路径
wal-dir = "/var/lib/influxdb/wal"

# HTTP绑定
http-bind-address = "0.0.0.0:8181"

# 缓存大小
cache-max-memory-size = "256m"

# 并发写入限制
max-concurrent-compactions = 4
```

### 系统要求
- **CPU**: 至少4核，推荐8核以上
- **内存**: 最低4GB，推荐8GB以上
- **存储**: SSD，IOPS > 1000
- **网络**: 千兆网卡

---

## 故障排除

### 常见错误处理

1. **连接超时**
   ```bash
   # 检查InfluxDB服务状态
   systemctl status influxdb

   # 查看日志
   journalctl -u influxdb -f
   ```

2. **认证失败**
   - 检查token是否正确
   - 验证组织和bucket权限

3. **写入性能低**
   - 调整批量写入大小
   - 检查磁盘I/O情况
   - 增加缓存大小

4. **内存不足**
   - 降低并发线程数
   - 减少测试持续时间
   - 增加系统内存

---

## 与迁移测试结合

这些InfluxDB基准测试脚本可与`chk_restore.py`容器迁移测试结合使用，评估迁移期间的性能影响：

### 数据过期验证测试
```bash
# 测试数据过期配置并运行迁移基准测试
python3 ./experiment/migration/influxdb/bench_sensoragg.py \
    --influx-url http://localhost:8181 \
    --token my-token \
    --org my-org \
    --bucket migration-test \
    --threads 8 \
    --duration 300 \
    --rps 200 \
    --retention-policy 10m

# 预期输出日志：
# INFO: Checking bucket 'migration-test' retention policy...
# INFO: Bucket configured with 10m retention policy
# INFO: Desired retention policy: 10m - matches current configuration
```

### 单一节点迁移测试
```bash
# 🔄 使用retention policy控制数据大小（间接控制）
python3 ./experiment/migration/influxdb/bench_sensoragg.py \
    --influx-url http://localhost:8181 \
    --token my-token \
    --org my-org \
    --bucket migration-test \
    --threads 8 \
    --duration 300 \
    --rps 200 \
    --retention-policy 30m &  # 通过较短的retention实现大小控制

# 💡 大数据库测试：设置更长的retention来增加数据量
python3 ./experiment/migration/influxdb/bench_cartelem.py \
    --retention-policy 24h \  # 24小时retention，间接增加最大大小
    --threads 16 \
    --rps 1000
```

### 分布式迁移测试
```bash
# 源节点运行负载测试
python3 ./experiment/migration/influxdb/bench_cartelem.py \
    --influx-url http://source-node:8181 \
    --threads 12 \
    --duration 180 \
    --vehicle-pattern highway \
    --payload-size 2KB

# 迁移完成后在目标节点验证
python3 ./experiment/migration/influxdb/bench_cartelem.py \
    --influx-url http://dest-node:8181 \
    --threads 12 \
    --duration 60 \
    --vehicle-pattern highway
```

然后运行迁移测试或mig-scripts，从而评估迁移对InfluxDB性能的影响。