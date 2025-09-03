# Redis 基准测试脚本

该目录包含多个Redis基准测试脚本，用于模拟不同应用场景下的负载测试。

## 依赖安装

```bash
pip install redis
```

## 🚀 新功能特性

### ✅ **消息速率管理**
支持精确消息速率控制，支持设置每秒最大请求数(RPS)。

```bash
# 限制为100 RPS
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 30 --rps 100

# 高速流量控制
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30 --rps 500

# 精确性能评估
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 4 --duration 30 --rps 200 --read-pct 25
```

**特性：**
- **精确定量**: 从无限制到精确RPS控制
- **自适应等待**: 基于操作耗时动态调整间隔
- **延迟友好**: 保证网络性能的同时控制速率
- **资源保护**: 防止系统过载和拒绝服务攻击

#### 消息速率控制机制
```python
# 精确RPS控制公式
min_interval_per_request = 1.0 / max_requests_per_second

# 动态等待计算
wait_time = max(0, min_interval_per_request - operation_duration)

# 自适应调整
if wait_time > 0.01:
    time.sleep(wait_time)
else:
    time.sleep(0.01)  # 保留最小间隔
```

### 📊 **实时期期性监控输出**
实现在测试过程中每秒显示性能统计，已修复时间计算问题。

#### 实时监控格式
```bash
[1.0s] TPS: 1250.3, Avg Lat: 2.45ms, P95: 4.12ms
[2.0s] TPS: 1180.5, Avg Lat: 2.51ms, P95: 4.23ms
[3.0s] TPS: 1320.7, Avg Lat: 2.38ms, P95: 3.98ms
...

Total ops: 37500 success=37500 fail=0 ops/sec=1250.00
```

#### 监控指标说明
- **TPS**: 每秒事务数(Transaction Per Second)
- **Avg Lat**: 平均延迟时间(毫秒)
- **P95**: 95百分位延迟，表示95%的请求延迟低于此值

### 🕒 **时序数据过期时限**
Redis缓存应用支持TTL时间过期管理，支持时序数据自动过期清理：

```bash
# 缓存TTL设置 (默认60秒)
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --ttl 300 --threads 8
```

**时序策略：**
- **Redis缓存**: TTL过期，自动清理过期数据
- **传感数据**: 有状态数据，持续更新的时间序列存储
- **视频分析**: 中期限缓存(TTL 60s-300s)
- **车联网数据**: 实时流数据，无TTL限制

---

## 脚本概览

### bench_cartelem.py
**车联网遥测数据基准测试**

模拟车联网数据写入Redis Stream，包含真实车辆物理特性模拟。

#### 基本用法
```bash
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30
```

#### 参数配置

##### 基础Redis连接参数
- `--redis-host`: Redis主机地址 (默认: 127.0.0.1)
- `--redis-port`: Redis端口 (默认: 6379)
- `--stream`: Redis Stream名称 (默认: vehicle:telemetry)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 1)
- `--size-distribution`: 负载大小分布 (uniform/normal/zipf) (默认: uniform)

##### 数据类型真实性参数
- `--vehicle-pattern`: 车辆驾驶模式
  - `normal_city`: 城市正常驾驶 (默认)
  - `highway`: 高速公路驾驶
  - `stop_go`: 停止-启动模式

##### 连接超时配置
- `--connect-timeout`: 连接超时秒数 (默认: 5)
- `--socket-timeout`: Socket读取超时 (默认: 5)
- `--pool-timeout`: 连接池等待超时 (默认: 10)
- `--pool-size`: 连接池最大尺寸 (默认: threads*10)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)

##### 消息速率控制参数
- `--rps` / `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

#### 使用示例
```bash
# 城市驾驶模式，高负载测试
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --vehicle-pattern normal_city --payload-size-kb 5 \
    --connect-timeout 2 --pool-size 100

# 高速公路驾驶模式，中等负载
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 60 \
    --vehicle-pattern highway --payload-size-kb 2

# 停止-启动模式，大规模测试
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 16 --duration 60 \
    --vehicle-pattern stop_go --payload-size-kb 10 \
    --size-distribution normal --pool-size 200
```

---

### bench_sensoragg.py
**传感器聚合数据基准测试**

模拟多传感器数据写入Redis Sorted Set，支持温度、湿度、压力、振动等多种传感器类型。

#### 基本用法
```bash
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 8 --duration 30 --read-pct 10
```

#### 参数配置

##### 基础Redis连接参数
- `--redis-host`: Redis主机地址 (默认: 127.0.0.1)
- `--redis-port`: Redis端口 (默认: 6379)
- `--set-key`: Redis Sorted Set键名 (默认: sensors:ts)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 1)
- `--sensors-per-device`: 每个设备传感器数量 (默认: 5)

##### 数据类型真实性参数
- `--sensor-types`: 传感器类型列表 (默认: temperature,humidity,pressure,vibration)
- `--environmental-noise`: 环境噪音水平 (默认: 0.05)

##### 连接超时配置
- `--connect-timeout`: 连接超时秒数 (默认: 5)
- `--socket-timeout`: Socket读取超时 (默认: 5)
- `--pool-timeout`: 连接池等待超时 (默认: 10)
- `--pool-size`: 连接池最大尺寸 (默认: threads*10)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--read-pct`: 读操作百分比 (默认: 10)

##### 消息速率控制参数
- `--rps` / `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

#### 使用示例
```bash
# 多传感器类型测试，包括振动传感器
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --read-pct 10 --sensors-per-device 10 \
    --sensor-types temperature,humidity,pressure,vibration

# 高噪声环境模拟，大负载测试
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 12 --duration 60 \
    --payload-size-kb 2 --sensors-per-device 15 \
    --environmental-noise 0.1 --pool-size 150

# 混合读写测试，偏重读取
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 4 --duration 60 \
    --read-pct 30 --sensor-types temperature,humidity
```

---

### bench_video_cache.py
**视频缓存基准测试**

模拟AI推理结果缓存写入Redis，主要针对视频分析应用场景。

#### 基本用法
```bash
python3 bench_video_cache.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --write-pct 80 --ttl 60 --fallback-rate 5
```

#### 参数配置

##### 基础Redis连接参数
- `--redis-host`: Redis主机地址 (默认: 127.0.0.1)
- `--redis-port`: Redis端口 (默认: 6379)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 2)
- `--objects-per-frame`: 每帧物体数量 (默认: 3)

##### 数据类型真实性参数
- `--camera-count`: 摄像头数量 (默认: 10)
- `--inference-model`: 推理模型类型 (yolov5_small/medium/ssd_mobile)

##### 缓存参数
- `--ttl`: 缓存TTL秒数 (默认: 60)
- `--persist-hash`: 持久化Hash键名 (默认: video_inference_persist)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--write-pct`: 写操作百分比 (默认: 80)
- `--fallback-rate`: 回退到持久化百分比 (默认: 5)
- `--do-get-pct`: 写后立即GET百分比 (默认: 0)

##### 消息速率控制参数
- `--rps` / `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

#### 使用示例
```bash
# 高并发视频缓存测试
python3 bench_video_cache.py --redis-host 127.0.0.1 --threads 16 --duration 30 \
    --write-pct 85 --ttl 300 --fallback-rate 10 \
    --camera-count 20 --objects-per-frame 5 \
    --inference-model yolov5_medium

# 混合读写缓存测试
python3 bench_video_cache.py --redis-host 127.0.0.1 --threads 8 --duration 60 \
    --write-pct 70 --do-get-pct 20 --ttl 120 \
    --payload-size-kb 5

# 轻量级推理模型测试
python3 bench_video_cache.py --redis-host 127.0.0.1 --threads 4 --duration 30 \
    --inference-model yolov5_small --objects-per-frame 2
```

---

### bench_video_cache_enhanced.py (推荐)
**增强版视频缓存基准测试**

完整的视频缓存基准测试工具，包含数据规模扩展、摄像头地理分布、多类别物体检测等高级特性。

#### 基本用法
```bash
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --inference-model yolov5_medium --payload-size-kb 2
```

#### 参数配置

##### 基础Redis连接参数
- `--redis-host`: Redis主机地址 (默认: 127.0.0.1)
- `--redis-port`: Redis端口 (默认: 6379)

##### 数据规模扩展参数
- `--payload-size-kb`: 目标负载大小(KB) (默认: 2)
- `--objects-per-frame`: 每帧物体数量 (默认: 3)

##### 数据类型真实性参数
- `--camera-count`: 摄像头数量 (默认: 10)
- `--inference-model`: 推理模型类型 (默认: yolov5_medium)
  - `yolov5_small`: 小型模型
  - `yolov5_medium`: 中型模型
  - `ssd_mobile`: 移动端模型

##### 连接超时配置
- `--connect-timeout`: 连接超时秒数 (默认: 5)
- `--socket-timeout`: Socket读取超时 (默认: 5)
- `--pool-timeout`: 连接池等待超时 (默认: 10)
- `--pool-size`: 连接池最大尺寸 (默认: 16)

##### 缓存参数
- `--ttl`: 缓存TTL秒数 (默认: 60)
- `--persist-hash`: 持久化Hash键名 (默认: video_inference_persist)

##### 负载参数
- `--threads`: 工作线程数 (默认: 4)
- `--duration`: 测试时长(秒) (默认: 10)
- `--write-pct`: 写操作百分比 (默认: 80)
- `--fallback-rate`: 回退到持久化百分比 (默认: 5)
- `--do-get-pct`: 写后立即GET百分比 (默认: 0)

##### 消息速率控制参数
- `--rps` / `--max-requests-per-second`: 每秒最大请求数 (默认: 无限制)

#### 使用示例
```bash
# 完整视频分析测试，大规模部署
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --threads 16 --duration 60 \
    --inference-model yolov5_medium --payload-size-kb 5 \
    --camera-count 50 --objects-per-frame 8 \
    --pool-size 200

# 高精度推理模型测试
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --threads 8 --duration 45 \
    --inference-model yolov5_medium --objects-per-frame 5 \
    --payload-size-kb 3 --ttl 300

# 移动端优化的轻量测试
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --threads 4 --duration 30 \
    --inference-model ssd_mobile --objects-per-frame 2 \
    --camera-count 5 --ttl 120

# 吞吐量优化配置
python3 bench_video_cache_enhanced.py --redis-host 127.0.0.1 --threads 32 --duration 30 \
    --connect-timeout 3 --socket-timeout 3 --pool-size 400 \
    --payload-size-kb 1 --objects-per-frame 3
```

---

## 输出统计

所有脚本都会提供以下性能指标：

### 实时监控输出格式
```
[30.0s] TPS: 1250.3, Avg Lat: 2.45ms, P95: 4.12ms
```

### 最终统计摘要
- **总体统计**: 总操作数、成功数、失败数、每秒操作数
- **延迟分布**: 平均延迟、P50、P90、P95、P99、最大延迟

### 统计字段说明
- `TPS`: Transactions Per Second (每秒事务数)
- `Lat`: Latency (延迟)
- `P50/P90/P95/P99`: 百分位延迟

---

## 注意事项

1. **连接配置**: 根据Redis服务器性能调整连接池大小和超时设置
2. **负载适配**: 根据系统资源和测试需求调整线程数
3. **数据真实性**: 使用真实性参数获得更准确的性能评估
4. **监控频率**: 脚本会每秒输出一次性能统计
5. **内存使用**: 大负载测试注意系统内存使用情况

---

## 适用场景

- **车联网应用**: 评估车辆遥测数据处理能力
- **物联网监控**: 测试传感器数据聚合性能
- **视频分析**: 评估AI推理结果缓存效率
- **实时缓存**: 测试高并发缓存读写性能
- **迁移测试**: 在容器迁移期间评估Redis性能影响