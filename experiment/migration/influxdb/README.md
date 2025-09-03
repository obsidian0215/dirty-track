# InfluxDB 基准测试脚本

该目录包含用于InfluxDB的时间序列数据基准测试脚本，适用于物联网、传感器监控等时间序列应用场景。

## 依赖安装

```bash
pip install influxdb-client
```

## 脚本概览

### benchmark.py
**通用InfluxDB基准测试**

基本的InfluxDB写入和查询性能测试，支持并发写入和范围查询。

#### 基本用法
```bash
python3 benchmark.py --threads 10 --operations 1000 --url http://localhost:8086 \
    --token my-token --org my-org --bucket benchmark --test-mode write
```

#### 参数配置

##### InfluxDB连接参数
- `--url`: InfluxDB服务器URL (默认: http://localhost:8086)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: benchmark)

##### 负载参数
- `--threads`: 并发线程数 (默认: 10)
- `--operations`: 每个线程操作数 (默认: 1000)
- `--field-count`: 每点字段数 (默认: 5)
- `--tag-count`: 每点标签数 (默认: 3)
- `--test-mode`: 测试模式 (write/read/mixed) (默认: write)

#### 使用示例
```bash
# 高并发写入测试
python3 benchmark.py --threads 20 --operations 5000 --url http://localhost:8086 \
    --bucket high-throughput-test --test-mode write

# 混合读写测试
python3 benchmark.py --threads 8 --operations 1000 --url http://localhost:8086 \
    --bucket mixed-workload --test-mode mixed

# 大量字段数据测试
python3 benchmark.py --threads 4 --operations 1000 --url http://localhost:8086 \
    --field-count 20 --tag-count 5
```

---

### bench_sensor_influx.py
**传感器数据专项基准测试**

针对传感器数据的InfluxDB测试，支持多传感器类型、环境噪音模拟等真实性特征。

#### 基本用法
```bash
python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket sensor-data --threads 8 --duration 30 --read-pct 10
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8086)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: sensor-data)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 1)
- `--sensors-per-device`: 每个设备传感器数量 (默认: 5)

##### 数据类型真实性参数
- `--sensor-types`: 传感器类型列表 (默认: temperature,humidity,pressure,vibration)
- `--environmental-noise`: 环境噪音水平 (默认: 0.05)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--read-pct`: 读操作百分比 (默认: 10)

#### 使用示例
```bash
# 多传感器类型物联网平台测试
python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket iot-sensors --threads 16 --duration 60 \
    --sensors-per-device 12 --sensor-types temperature,humidity,pressure,vibration \
    --payload-size-kb 2

# 工业环境传感器测试（高噪声）
python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket industrial-mon --threads 8 --duration 30 \
    --sensor-types temperature,vibration,pressure --environmental-noise 0.15

# 大规模物联网数据采集测试
python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket large-scale-iot --threads 32 --duration 45 \
    --sensors-per-device 20 --payload-size-kb 5 --read-pct 5
```

---

### bench_vehicle_influx.py
**车联网遥测数据InfluxDB测试**

将car telematics数据适配到InfluxDB的车辆遥测基准测试。

#### 基本用法
```bash
python3 bench_vehicle_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket vehicle-telemetry --threads 8 --duration 30
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8086)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: vehicle-telemetry)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 2)

##### 数据类型真实性参数
- `--vehicle-pattern`: 驾驶模式 (normal_city/highway/stop_go) (默认: normal_city)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)

#### 使用示例
```bash
# 车队管理平台测试
python3 bench_vehicle_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket fleet-health --threads 12 --duration 45 \
    --vehicle-pattern highway --payload-size-kb 3

# 城市物流车辆监控测试
python3 bench_vehicle_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket logistics-monitor --threads 8 --duration 30 \
    --vehicle-pattern stop_go --payload-size-kb 2

# 高速交通数据分析
python3 bench_vehicle_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket traffic-analytics --threads 16 --duration 60 \
    --vehicle-pattern highway --payload-size-kb 4
```

---

### bench_video_influx.py
**视频分析数据InfluxDB测试**

将视频流分析数据适配到InfluxDB的时间序列存储。

#### 基本用法
```bash
python3 bench_video_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket video-analytics --threads 8 --duration 30
```

#### 参数配置

##### InfluxDB连接参数
- `--influx-url`: InfluxDB服务器URL (默认: http://localhost:8086)
- `--token`: 认证令牌 (默认: my-super-secret-auth-token)
- `--org`: 组织名称 (默认: my-org)
- `--bucket`: 数据桶名称 (默认: video-analytics)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 2)
- `--objects-per-frame`: 每帧物体数量 (默认: 3)

##### 数据类型真实性参数
- `--camera-count`: 摄像头数量 (默认: 10)
- `--inference-model`: 推理模型类型 (yolov5_small/medium/ssd_mobile)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)

#### 使用示例
```bash
# 智能城市视频监控平台测试
python3 bench_video_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket smart-city-video --threads 20 --duration 45 \
    --camera-count 100 --objects-per-frame 8 --inference-model yolov5_medium

# 交通监控系统测试
python3 bench_video_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket traffic-monitoring --threads 12 --duration 30 \
    --camera-count 50 --inference-model ssd_mobile

# 零售客流分析平台测试
python3 bench_video_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket retail-analytics --threads 8 --duration 60 \
    --objects-per-frame 6 --payload-size-kb 3
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
http-bind-address = "0.0.0.0:8086"

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

```bash
# 在迁移测试期间运行基准测试
python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token \
    --org my-org --bucket migration-test --threads 8 --duration 300 &
```

然后运行迁移测试，从而评估迁移对InfluxDB性能的影响。