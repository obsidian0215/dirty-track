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
优先使用TTL机制，stream_maxlen作为可选辅助：

- **TTL（推荐）**: 基于时间的精确控制，支持自适应调整
- **自适应TTL**: 设置 `--target-db-size-mb` 启用智能大小控制
- **固定TTL**: 设置 `--ttl` 使用固定时间，默认3600秒
- **Stream Maxlen**: 可选的长度限制，仅对bench_cartelem.py有效

### 🆕 自适应TTL
- ✨ **动态Payload计算**: 实时跟踪并更新平均payload大小以提高计算精度
- ⚡ **更高响应速度**: TTL调整频率为10秒
- 🎯 **智能调整算法**: 根据大小差异程度采用不同的调整步长（小差值稳步调整，大差值快速调整）

```bash
# 1. 自适应大小控制（推荐）
--target-db-size-mb 100

# 2. 固定TTL控制
--ttl 7200

# 3. 结合长度限制（可选）
--stream-maxlen 10000
```

```bash
# 自适应数据库大小控制 (推荐)
python3 bench_cartelem.py --target-db-size-mb 100 --rps 500

# 固定TTL模式：自定义TTL时间
python3 bench_sensoragg.py --rps 200 --ttl 7200  # 2小时TTL

# 默认TTL模式：使用3600秒(1小时)TTL
python3 bench_sensoragg.py --rps 200
```

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
    --vehicle-pattern normal_city --payload-size 5KB
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
| `--redis-port` | Redis服务器端口 | 6379 | `--redis-port 6380` |
| `--threads` | 工作线程数 | 4 | `--threads 16` |
| `--duration` | 测试时长(秒) | 10 | `--duration 300` |
| `--payload-size` | 目标负载大小 (支持单位B/KB/MB, 如 "5KB") | 1KB | `--payload-size 5KB` |
| `--size-distribution` | 数据大小分布 | uniform | `--size-distribution normal` |
| `--rps`/`--max-requests-per-second` | 请求速率限制(每秒) | 无限制 | `--rps 1000` |
| `--target-db-size-mb` | 目标数据库大小(MB) | 自适应 | `--target-db-size-mb 100` |
| `--ttl` | TTL秒数(非自适应时) | 3600 | `--ttl 7200` |
| `--stream-maxlen` | 可选Stream最大长度(仅bench_cartelem.py) | None | `--stream-maxlen 5000` |
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
    --rps 500 --payload-size 2KB
```

#### 自适应数据库大小控制
```bash
# 🍃 动态大小控制（推荐，已优化算法）
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 300 \
    --target-db-size-mb 1000 --rps 2000  # 现在具备更好的精度和响应速度

# 🔧 固定TTL控制
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 300 \
    --ttl 1800 --rps 1000  # 30分钟固定TTL

# 📊 大数据库测试
python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 16 --duration 600 \
    --target-db-size-mb 5000 --rps 5000  # 适应高负载场景

# 🔍 调试模式（查看详细TTL调整日志）
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 4 --duration 180 \
    --target-db-size-mb 200 --rps 500  # 观察日志输出
```

#### 大规模数据测试
```bash
# 扩展到大规模数据负载
python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 16 --duration 300 \
    --sensors-per-device 50 --payload-size 10KB --read-pct 20
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
6. **数据库大小控制**: 迁移测试时使用 `--target-db-size-mb` 防止无限增长
7. **分布模式选择**: normal模式适合现实世界负载，uniform适合理论分析
8. **实时监控**: 关注P95延迟而非平均延迟，更能反映用户体验
9. **TTL策略选择**: 自适应模式适合动态负载，固定TTL适合稳定负载
10. **TTL配置建议**: 高并发场景推荐自适应模式，稳定负载适合固定TTL

### 🆕 数据库大小控制优化指南

#### 使用建议
```bash
# 1. 小型数据库 (推荐自适应模式)
python3 bench_cartelem.py --target-db-size-mb 100 --rps 200 --duration 300

# 2. 中型数据库 (稳定负载)
python3 bench_cartelem.py --ttl 3600 --rps 500 --duration 300

# 3. 大型数据库 (高并发场景)
python3 bench_cartelem.py --target-db-size-mb 5000 --rps 2000 --threads 32 --duration 600

# 4. 调试和监控
python3 bench_sensoragg.py --target-db-size-mb 200 --rps 300 --duration 180
```

#### 验证方法
- 📈 观察日志中的"Adaptive TTL adjusted"消息
- 📊 实时监控Redis内存使用情况 (INFO memory)
- ⚙️ 查看payload大小统计输出
- 📋 确认TTL调整频率 (每10秒一次)