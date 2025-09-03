# Redis 高级基准测试

该目录包含用于Redis的三个主要工作负载基准测试脚本，每个脚本都支持完整的数据规模扩展、真实性模拟和性能监控功能。

## 🚀 核心功能特性

### Rate Limiting (速率控制)
支持精确的请求速率限制，从无限制到精确RPS控制：
```bash
# 限制为200 RPS
python3 bench_sensoragg.py --redis-host 127.0.0.1 --rps 200 --threads 8 --duration 30
```

### Real-time Monitoring (实时监控)
每秒输出TPS、平均延迟、P95延迟：
```bash
[1.0s] TPS: 1250.3, Avg Lat: 2.45ms, P95: 4.12ms
[2.0s] TPS: 1180.5, Avg Lat: 2.51ms, P95: 4.23ms
```

### Connection Pooling (连接池)
自动连接池管理，性能优化：
```bash
# 大负载测试连接池配置
python3 bench_cartelem.py --pool-size 100 --connect-timeout 3 --socket-timeout 3
```

### TTL Management (过期管理)
支持不同场景的TTL策略：
- **缓存模式**: 短期TTL (60-300秒)
- **时序数据**: 长期存储，无TTL
- **传感器数据**: 中期缓存，根据刷新频率

## 📝 脚本总览

### 1. bench_cartelem.py - 车联网遥测基准测试
模拟车辆实时数据写入Redis Stream，包含GPS、速度、燃油水平、诊断码等。

**核心特性：**
- 支持三种驾驶模式：城市驾驶、高速公路、停止启动
- 车辆状态持续跟踪和物理模拟
- 数据规模可扩展到目标负载大小

**快速开始：**
```bash
# 城市驾驶模式测试
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --vehicle-pattern normal_city --payload-size-kb 5
```

### 2. bench_sensoragg.py - 传感器聚合基准测试
模拟IoT传感器数据写入Redis Sorted Set，支持多传感器类型和环境噪音。

**核心特性：**
- 多传感器类型支持：温度、湿度、压力、振动
- 传感器漂移和校准模拟
- 可扩展的传感器数量配置

**快速开始：**
```bash
# 多传感器工业环境测试
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 8 --duration 30 \
    --sensors-per-device 10 --sensor-types temperature,humidity,pressure,vibration \
    --read-pct 10
```

### 3. bench_video_cache.py - 视频缓存基准测试
模拟视频分析推理结果缓存写入Redis，支持TTL和缓存策略。

**核心特性：**
- 物体检测结果缓存管理
- 支持回退到持久化Hash存储
- 混合读写操作模拟

**快速开始：**
```bash
# 高并发视频缓存测试
python3 bench_video_cache.py --redis-host 127.0.0.1 --threads 16 --duration 30 \
    --write-pct 80 --ttl 300 --camera-count 20
```

## 📊 统一配置参数

### 🔥 所有脚本通用参数

| 参数 | 说明 | 默认值 | 示例 |
|------|------|--------|------|
| `--redis-host` | Redis服务器地址 | 127.0.0.1 | --redis-host 192.168.1.100 |
| `--redis-port` | Redis服务器端口 | 6379 | `----redis-port 6380` |
| `--threads` | 工作线程数 | 4 | `--threads 16` |
| `--duration` | 测试时长(秒) | 10 | `--duration 300` |
| `--payload-size-kb` | 目标负载大小(KB) | 1 | `--payload-size-kb 5` |
| `--size-distribution` | 数据大小分布 | uniform | `--size-distribution normal` |
| `--rps`/`--max-requests-per-second` | 请求速率限制(每秒) | 无限制 | `--rps 1000` |
| `--connect-timeout` | 连接超时(秒) | 5 | `--connect-timeout 3` |
| `--socket-timeout` | Socekt超时(秒) | 5 | `--socket-timeout 3` |
| `--pool-timeout` | 池等待超时(秒) | 10 | `--pool-timeout 15` |
| `--pool-size` | 连接池大小 | threads*10 | `--pool-size 100` |

### 🚀 高级用法示例

#### 高并发性能测试
```bash
# 最大化吞吐量测试
for script in bench_cartelem.py bench_sensoragg.py bench_video_cache.py; do
    python3 ./experiment/migration/redis/$script \
        --redis-host 127.0.0.1 \
        --threads 32 \
        --duration 60 \
        --pool-size 400 \
        --connect-timeout 3 \
        --socket-timeout 3 \
        --rps 5000
done
```

#### 连接池优化测试
```bash
# 密集连接负载测试
python3 ./experiment/migration/redis/bench_cartelem.py \
    --redis-host 127.0.0.1 \
    --threads 16 \
    --duration 60 \
    --pool-size 200 \
    --connect-timeout 2 \
    --socket-timeout 3 \
    --pool-timeout 10
```

#### 速率控制测试
```bash
# 精确控制每秒请求数
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 60 \
    --rps 500 --payload-size-kb 2
```

#### 大规模数据测试
```bash
# 扩展到大规模数据负载
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 16 --duration 300 \
    --sensors-per-device 50 --payload-size-kb 10 --read-pct 20
```

## 🎯 性能输出格式

### 实时监控
```
[1.0s] TPS: 1250.3, Avg Lat: 2.45ms, P95: 4.12ms
[2.0s] TPS: 1180.5, Avg Lat: 2.51ms, P95: 4.23ms
...
```

### 测试摘要
```
Total ops: 37500 success=37500 fail=0 ops/sec=1250.00
Latency ms - avg=2.47 p50=2.12 p90=3.45 p99=4.23 max=5.67
```

## 📋 依赖安装

```bash
pip install redis
```

## 🏗️ 架构特性

- **数据真实性**: 物理约束模拟、状态持续跟踪
- **扩展性**: 从单设备到大规模IoT部署
- **性能监控**: 实时TPS和延迟统计
- **连接优化**: 自动连接池和超时管理
- **资源保护**: RPS限制防止系统过载

## 💡 最佳实践

1. **根据CPU核心数设置线程数**: threads ≈ CPU_cores * 2
2. **监控内存使用**: 大负载测试注意系统内存
3. **网络优化**: 高并发时使用连接池参数
4. **真实性优先**: 生产评估时使用真实性参数
5. **速率控制**: 负载测试时设置合理的RPS限制
6. **分布模式选择**: normal模式适合现实世界负载，uniform适合理论分析
7. **实时监控**: 关注P95延迟而非平均延迟，更能反映用户体验