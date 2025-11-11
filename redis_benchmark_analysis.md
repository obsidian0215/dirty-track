# Redis 负载脚本分析报告

本文档详细分析了 `experiment/migration/redis/` 目录下的三个基准测试脚本的功能和负载构成。

## 1. bench_cartelem.py - 车联网基准测试

### 功能描述
- **用途**: 模拟车联网（Car Telematics）场景的数据写入负载
- **主要操作**: Redis Stream 的 XADD 操作，用于实时写入车辆遥测数据
- **类名**: `CarTelematicsBench`
- **默认流名**: `vehicle:telemetry`

### 负载构成
- **操作类型**: 纯写入负载（write-only）
- **数据格式**: JSON 对象，包含：
  - `vehicle_id`: 车辆ID (veh-XXXX)
  - `timestamp`: 时间戳 (毫秒)
  - `lat`: 纬度 (float，范围约31.0±0.1中国)
  - `lon`: 经度 (float，范围约121.0±0.1中国)
  - `speed_kmh`: 车速 (float，0-120 km/h)
- **并发特性**: 支持多线程（默认4线程），每个线程独立生成和写入数据
- **持续时间**: 可配置运行时长
- **性能指标**: 收集操作延迟（ms）、QPS、成功/失败统计
- **延迟分布**: 记录平均值、P50/P90/P99/最大延迟

### 关键代码示例

**数据生成方法**:
```python
def _make_payload(self):
    import time
    payload = {
        "vehicle_id": f"veh-{random.randint(1000,9999)}",
        "timestamp": int(time.time()*1000),
        "lat": round(31.0 + random.random()*0.1, 6),
        "lon": round(121.0 + random.random()*0.1, 6),
        "speed_kmh": round(random.random()*120, 2)
    }
    return payload
```

**写入存储过程**:
```python
try:
    r.xadd(self.stream_name, {"data": json.dumps(payload)})
    lat = (time.perf_counter() - start) * 1000.0
    self.success += 1
except Exception as e:
    logger.debug("xadd failed: %s", e)
    self.fail += 1
```

### 典型用法
```bash
python3 bench_cartelem.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30
```

## 2. bench_sensoragg.py - 传感器聚合基准测试

### 功能描述
- **用途**: 模拟物联网传感器数据聚合场景，支持时间序列写入和窗口查询
- **主要操作**: ZADD 写入 + ZRANGEBYSCORE 读取
- **类名**: `SensorAggBench`
- **默认键名**: `sensors:ts`（Sorted Set）

### 负载构成
- **操作类型**: 混合负载（write-dominated）
- **写入操作**: ZADD 到 Sorted Set
- **数据格式**: JSON 对象，包含：
  - `sensor_id`: 传感器ID (sen-1~50)
  - `timestamp`: 时间戳 (毫秒)
  - `value`: 传感器值 (float，0-100)
- **读取操作**: ZRANGEBYSCORE 查询最近1分钟数据（权重读取概率）
- **读写比例**: 可配置 read_pct 参数（默认10%）
- **并发特性**: 多线程并发，各线程独立执行
- **性能指标**: 与cartelem类似，包括延迟分布和QPS

### 关键代码示例

**数据生成和写入**:
```python
def _make_reading(self):
    return {
        "sensor_id": f"sen-{random.randint(1,50)}",
        "timestamp": int(time.time()*1000),
        "value": round(random.random()*100.0, 3)
    }

# 写入操作
rcd = self._make_reading()
r.zadd(self.set_key, {json.dumps(rcd): float(rcd["timestamp"])})
```

**读取操作（窗口查询）**:
```python
# 按概率决定读写
do_read = random.randint(1,100) <= read_pct
if do_read:
    now = int(time.time()*1000)
    results = r.zrangebyscore(self.set_key, now-60000, now)
```

### 典型用法
```bash
python3 bench_sensoragg.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30 --read-pct 10
```

## 3. bench_video_cache.py - 视频缓存基准测试

### 功能描述
- **用途**: 模拟视频分析推理结果缓存场景
- **主要操作**: SET + optional GET + fallback HSET
- **类名**: `VideoCacheRedisBench`
- **缓存TTL**: 默认60秒
- **持久化键**: `video_inference_persist`（Hash）

### 负载构成
- **操作类型**: 混合缓存负载（读写混合）
- **写入操作**: SET key EX ttl（缓存写入）
- **回退写入**: HSET 到持久化hash（按概率触发）
- **读取操作**: GET 缓存键（读或缓存miss）
- **数据格式**: JSON 对象，包含：
  - `frame_id`: 帧ID (frame-XXXXXX)
  - `timestamp`: 时间戳 (毫秒)
  - `objects`: 检测对象数组
- **读写配置**:
  - `write_pct`: 写操作百分比（默认80%）
  - `fallback_rate`: 回退到持久化的概率（默认5%）
  - `do_get_pct`: 写后立即GET的概率（默认0%）
- **TTL管理**: 缓存键有过期时间
- **性能指标**: 延迟统计支持多操作类型累积

### 关键代码示例

**模拟缓存写入和回退逻辑**:
```python
def _make_result(self):
    return {
        "frame_id": f"frame-{random.randint(100000, 999999)}",
        "timestamp": int(time.time() * 1000),
        "objects": [{"class": "person", "score": round(random.random(), 2)}],
    }

# 写入逻辑：SET缓存 或 回退到HSET
if random.randint(1, 100) <= fallback_rate:
    # 回退写入持久化
    r.hset(self.persist_hash, key, payload)
else:
    # 正常缓存写入
    r.set(key, payload, ex=self.cache_ttl)
```

**读写混合操作**:
```python
op_rand = random.randint(1, 100)
if op_rand <= write_pct:
    # 写路径，包含可能的GET
    r.set(key, payload, ex=self.cache_ttl)
    if random.randint(1, 100) <= do_get_pct:
        _ = r.get(key)  # 立即读取模拟
else:
    # 读路径
    _ = r.get(f"frame-{random.randint(100000, 999999)}")
```

### 典型用法
```bash
python3 bench_video_cache.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30 --write-pct 80 --ttl 60 --fallback-rate 5
```

## 综合对比

| 脚本 | 主要场景 | Redis数据类型 | 负载类型 | 读写比例 | 并发模型 |
|------|----------|---------------|----------|----------|----------|
| bench_cartelem.py | 车联网 | Stream | 纯写 | 100%写 | 多线程工作 |
| bench_sensoragg.py | 传感器聚合 | Sorted Set | 混合 | 90%写/10%读 | 多线程工作 |
| bench_video_cache.py | 视频缓存 | String + Hash | 混合 | 80%写/20%读 | 多线程工作 |

### 共同特性
- **配置参数**: 都支持 `--redis-host`、 `--redis-port`、 `--threads`、 `--duration`
- **性能测量**: 标准化延迟收集、QPS计算、成功/失败率统计
- **多线程架构**: 类似的设计模式，使用threading模块实现并发
- **异常处理**: 统一的错误处理和统计
- **日志记录**: 使用logging模块输出操作信息

### 差异要点
- **数据模型**: Stream（时间序列）、Sorted Set（有序数据）、String/Hash（缓存+持久化）
- **读写特性**: cartelem完全写入，其他两个脚本提供读取混合
- **业务语义**: 模拟现实物联网、传感器、视频分析的存储需求
- **配置复杂性**: video_cache提供最多参数控制读写行为、TTL、回退等

## 扩展分析与推荐

### 1. bench_cartelem.py 扩展选项

**当前参数限制:**
- 仅支持基本Redis连接和简单的Stream写入
- 无法模拟复杂的车联网场景
- 缺少地理分布、车速模式等真实性

**推荐扩展参数:**

#### 地理与运动模拟
- `--geo-center-lat/lon`: 中心经纬度坐标 (默认31.0, 121.0中国)
- `--geo-radius-km`: 地理分布半径 (公里)
- `--speed-pattern`: 车速模式选项 (`normal`, `highway`, `city`, `mixed`)
- `--speed-range-min/max`: 车速动态范围

#### 数据规模与复杂度
- `--payload-size`: 每条消息负载大小，带单位（例如 16KB，可模拟传感器数据量）
- `--vehicle-count`: 模拟车辆总数 (影响ID分布)
- `--batch-size`: XADD批处理大小 (默认1)
- `--compression`: 数据压缩选项 (`none`, `gzip`, `lz4`)

#### Redis高级特性
- `--stream-maxlen`: Stream最大长度 (自动TRIM)
- `--redis-password`: 集群认证支持
- `--redis-db`: 指定数据库索引 (默认0)
- `--pipeline-size`: 流水线处理批次大小

#### 负载模式扩展
- `--traffic-pattern`: 通讯模式 (`constant`, `burst`, `wavepattern`)
- `--temporal-distribution`: 时间分布 (`uniform`, `peak-hours`)
- `--network-latency-ms`: 模拟网络延迟

### 2. bench_sensoragg.py 扩展选项

**当前参数限制:**
- 仅基础ZADD/ZRANGEBYSCORE操作
- 简化读写比例控制
- 缺少传感器网络的复杂性

**推荐扩展参数:**

#### 时间序列特性
- `--time-range-seconds`: 时间窗口大小 (默认60秒)
- `--aggregation-method`: ZREVRANGE, ZREMRANGEBYSCORE等操作
- `--data-retention-hours`: 数据保留时间
- `--compaction-interval`: 数据压缩间隔

#### 传感器网络模拟
- `--sensor-count`: 传感器节点总数
- `--sensor-density`: 传感器分布密度
- `--measurement-types`: 测量类型数组 (`temp`, `humidity`, `pressure`)
- `--sensor-failure-rate`: 传感器故障率 (%)

#### 高级查询模式
- `--query-patterns`: 查询模式列表 (`range`, `top-n`, `recent`, `avg`)
- `--time-granularity`: 时间粒度 (`seconds`, `minutes`, `hours`)
- `--filter-conditions`: 过滤条件支持 (sensor_id, value_range)

#### 数据质量与异常
- `--noise-level`: 测量噪声级别
- `--outlier-probability`: 异常值概率
- `--data-compression`: 时间序列压缩算法

### 3. bench_video_cache.py 扩展选项

**当前参数限制:**
- 过于简单的缓存缓存机制
- 缺少视频分析的复杂性
- 回退策略单一

**推荐扩展参数:**

#### 缓存策略扩展
- `--cache-strategy`: 缓存策略 (`lru`, `lfu`, `ttl-only`, `size-limited`)
- `--cache-size-limit`: 缓存大小限制 (键数量或内存大小)
- `--eviction-policy`: 驱逐策略 (`immediate`, `lazy`, `background`)
- `--warmup-data`: 预热数据文件

#### 视频处理特性
- `--frame-rate`: 帧率 (FPS, 影响负载密度)
- `--video-resolution`: 分辨率 (`720p`, `1080p`, `4k`)
- `--codec-type`: 编解码器类型 (影响帧大小)
- `--inference-model`: 推理模型类型 (影响处理时间)

#### 多层缓存架构
- `--l1-cache-ttl`: L1缓存TTL (毫秒级)
- `--l2-cache-ttl`: L2缓存TTL (秒级)
- `--persistent-tier`: 持久存储选项 (`s3`, `hdfs`, `local`)
- `--tiering-delay-ms`: 层间切换延迟

#### 回退与容错
- `--backup-endpoints`: 备份Redis端点
- `--circuit-breaker`: 熔断器配置
- `--retry-strategy`: 重试策略 (`immediate`, `exponential-backoff`)
- `--failover-timeout-ms`: 故障转移超时

### 通用扩展推荐

#### 连接与集群支持
- `--redis-cluster`: 集群模式支持
- `--sentinel-hosts`: Sentinel配置
- `--connection-pool-size`: 连接池大小
- `--ssl-config`: SSL/TLS配置

#### 负载分布与调优
- `--rate-limiter`: 请求率限制器
- `--think-time-ms`: 线程思考时间
- `--ramp-up-seconds`: 负载上升时间
- `--warmup-period-seconds`: 预热时间

#### 指标增强
- `--metrics-interval-seconds`: 指标采集间隔
- `--histogram-buckets`: 延迟直方图桶配置
- `--export-format`: 导出格式 (`json`, `csv`, `prometheus`)
- `--log-level`: 日志级别控制

#### 数据一致性测试
- `--consistency-check`: 一致性验证
- `--read-after-write-seconds`: 读写验证延迟
- `--version-stamping`: 版本控制

## 核心扩展方向深度分析

### 1. 数据规模扩展

#### 当前状况评估
```
bench_cartelem.py   : 静态负载 ~1KB/条 (lat,lng,id,timestamp)
bench_sensoragg.py  : 静态负载 ~200B/条 (id,timestamp,value)
bench_video_cache.py: 动态负载 ~1-5KB/帧 (frame_id,timestamp,objects)
```

#### 扩展实现方案

**数据规模渐进控制:**
- `--payload-size-min/max`: 负载大小范围控制
- `--size-distribution`: 大小分布类型 (`uniform`, `normal`, `zipf`)
- `--scale-factor`: 整体负载缩放倍数
- `--compression-type`: 压缩算法 (`gzip`, `lz4`, `brotli`)

**动态负载模式:**
- `--size-pattern`: 负载变化模式 (`constant`, `sawtooth`, `sine`)
- `--size-variance-pct`: 大小变化百分比 (默认20%)
- `--burst-size-multiplier`: 突发负载倍数

**内存使用监控:**
- `--memory-tracking`: 内存使用追踪
- `--size-threshold-mb`: 告警阈值配置

#### bench_cartelem.py 数据规模扩展示例
```python
def _make_payload(self, size_kb=1):
    # 基础数据
    base_payload = {
        "vehicle_id": f"veh-{random.randint(1000,9999)}",
        "timestamp": int(time.time()*1000),
        "location": {"lat": 31.0 + random.random()*0.1,
                    "lng": 121.0 + random.random()*0.1}
    }

    # 动态扩展到目标大小
    current_size = len(json.dumps(base_payload))
    target_size = size_kb * 1024

    if current_size < target_size:
        # 添加额外传感器数据
        sensors = []
        while len(json.dumps({**base_payload, "sensors": sensors})) < target_size:
            sensor = {
                "type": random.choice(["gps", "speed", "engine", "brake"]),
                "value": random.random() * 100,
                "unit": "metric"
            }
            sensors.append(sensor)

        base_payload["sensors"] = sensors

    return base_payload
```

### 2. 数据类型和真实性增强

#### bench_cartelem.py 真实性增强

**车辆行为模式:**
```python
class VehiclePattern:
    NORMAL_CITY = {"speed_min": 20, "speed_max": 60, "accel_rate": 0.5}
    HIGHWAY = {"speed_min": 60, "speed_max": 120, "accel_rate": 1.2}
    STOP_GO = {"speed_min": 0, "speed_max": 40, "accel_rate": 2.0}

def generate_realistic_telematics(pattern, current_state):
    """基于物理行为的车联网数据生成"""
    # 应用加速度约束
    acceleration = pattern["accel_rate"] * (random.random() - 0.5)
    new_speed = max(0, min(current_state["speed"] + acceleration, pattern["speed_max"]))

    # 地理轨迹计算 (考虑时间和速度)
    distance_moved = (new_speed * 1000 / 3600) * 1  # 1秒移动距离
    # ... 地理坐标更新逻辑

    return {
        "vehicle_id": current_state["vehicle_id"],
        "speed_kmh": new_speed,
        "lat": updated_lat,
        "lng": updated_lng,
        "acceleration": acceleration,
        "obd_codes": generate_diagnostic_codes(),
        "fuel_level": max(0, current_state["fuel"] - fuel_consumption),
        "engine_temp": calculate_engine_temperature(speed, load)
    }
```

**诊断数据类型:**
- OBD故障码 (发动机、变速箱、刹车系统)
- 胎压监控 (TPMS) 数据
- 燃油消耗率
- 发动机温度曲线
- 刹车磨损传感器

#### bench_sensoragg.py 真实传感器数据

**多类型传感器支持:**
```python
SENSOR_TYPES = {
    "temperature": {
        "range": (-40, 85),
        "noise": 0.5,
        "drift_rate": 0.01
    },
    "humidity": {
        "range": (0, 100),
        "noise": 1.0,
        "drift_rate": 0.05
    },
    "pressure": {
        "range": (500, 1100),  # hPa
        "noise": 5.0,
        "drift_rate": 1.0
    },
    "vibration": {
        "range": (0, 1000),
        "noise": 10,
        "burst_probability": 0.1
    }
}

class SensorSimulator:
    def __init__(self, sensor_type, config):
        self.type = sensor_type
        self.config = config
        self.current_value = config["range"][0] + random.random() * (config["range"][1] - config["range"][0])
        self.drift = 0

    def generate_reading(self, environmental_factor=1.0):
        """生成考虑环境因素的真实传感器数据"""

        # 基础随机波动
        noise = random.gauss(0, self.config["noise"])
        self.drift += random.gauss(0, self.config["drift_rate"])

        # 环境影响
        environmental_noise = environmental_factor * self.config["noise"] * 0.5

        # 传感器老化效应
        aging_factor = 1.0 + (time.time() % 86400) / 86400 * 0.02  # 每日2%老化

        new_value = self.current_value + noise + environmental_noise + self.drift
        new_value = aging_factor * new_value

        # 应用硬限制
        new_value = max(self.config["range"][0], min(self.config["range"][1], new_value))

        self.current_value = new_value

        # 突发事件模拟
        anomalous_reading = False
        if random.random() < self.config.get("burst_probability", 0.01):
            new_value *= random.uniform(0.95, 1.05)
            anomalous_reading = True

        return {
            "sensor_id": f"{self.type}-{random.randint(1,1000)}",
            "timestamp": int(time.time() * 1000),
            "value": round(new_value, 3),
            "unit": "metric",
            "sensor_type": self.type,
            "battery_level": 100 - random.random() * 10,
            "calibration_status": random.choice(["good", "drift", "cal_needed"]),
            "anomalous": anomalous_reading
        }
```

**时间序列特征:**
- 传感器漂移 (drift) 模拟
- 校准状态跟踪
- 电池电量变化
- 环境因素影响 (温度、湿度)
- 突发事件模拟
- 传感器故障模式

#### bench_video_cache.py 视频推理数据真实性

**多类别对象检测:**
```python
OBJECT_CLASSES = [
    "person", "car", "truck", "bus", "motorcycle", "bicycle",
    "traffic_light", "stop_sign", "dog", "cat", "bird"
]

INFERENCE_MODELS = {
    "yolov5_small": {"confidence_range": (0.3, 0.9), "speed_ms": 50},
    "yolov5_medium": {"confidence_range": (0.5, 0.95), "speed_ms": 80},
    "ssd_mobile": {"confidence_range": (0.2, 0.85), "speed_ms": 40}
}

def generate_realistic_inference_result(model_type="yolov5_small"):
    """生成更真实的视频推理结果"""

    model_config = INFERENCE_MODELS[model_type]

    # 基于EO对象的数量 (典型的城市场景分布)
    object_distribution = [0.6, 0.5, 0.3, 0.1, 0.2, 0.4, 0.05, 0.02, 0.003, 0.001, 0.001]
    objects = []

    for class_idx, prob in enumerate(object_distribution):
        if random.random() < prob:
            obj_count = max(1, int(random.gauss(2, 1)))  # 1-3个对象
            for _ in range(obj_count):
                objects.append({
                    "class": OBJECT_CLASSES[class_idx],
                    "confidence": round(random.uniform(*model_config["confidence_range"]), 3),
                    "bbox": [random.random() for _ in range(4)],  # [x,y,w,h]
                    "tracking_id": random.randint(0, 10000),
                    "speed_pixels_per_second": random.uniform(0, 10)
                })

    # 添加推理时间
    inference_time = random.gauss(model_config["speed_ms"], 10)

    return {
        "frame_id": f"frame-{random.randint(1000000, 9999999)}",
        "timestamp": int(time.time() * 1000),
        "inference_model": model_type,
        "inference_time_ms": max(10, inference_time),
        "total_objects": len(objects),
        "objects": objects,
        "frame_size": {"width": 1920, "height": 1080},
        "camera_id": f"cam-{random.randint(1,100)}",
        "location": {"lat": 31.0 + random.random()*0.1, "lng": 121.0 + random.random()*0.1}
    }
```

### 3. 连接超时配置扩展

**当前Redis连接参数:**
```python
# 基础配置
conn_kwargs = {
    "host": self.redis_host,
    "port": self.redis_port,
    #"socket_timeout": None,        # 读取超时
    #"socket_connect_timeout": None, # 连接超时
    #"socket_keepalive": False,      # TCP保活
    #"socket_keepalive_options": {}, # TCP保活选项
    #"health_check_interval": 30,    # 健康检查间隔
}
```

**推荐扩展参数:**

#### 超时配置参数
```bash
# 连接超时
--connect-timeout-seconds: Redis服务器连接超时时间 (默认5秒)
--socket-timeout-seconds: socket读取超时 (默认5秒)
--socket-connect-timeout-seconds: socket连接超时 (默认5秒)

# 缓存模式
--pool-size: 连接池大小 (默认数量线程*2)
--pool-timeout-seconds: 从连接池获取连接的等待超时 (默认10秒)

# 重试和容错
--retry-count: 重试次数 (默认3次)
--retry-delay-ms: 重试间隔 (默认100ms)
--circuit-breaker-threshold: 熔断阈值 (默认5次连续失败)

# TCP优化
--tcp-nodelay: 禁用Nagle算法 (默认true)
--tcp-keepalive-seconds: TCP保活间隔 (默认300秒)
--buffer-size-bytes: socket缓冲区大小 (默认64KB)
```

#### 连接管理类扩展
```python
class EnhancedRedisConnection:
    def __init__(self, config):
        self.config = config
        self.pool = redis.ConnectionPool(
            host=config['host'],
            port=config['port'],
            socket_timeout=config['socket_timeout'],
            socket_connect_timeout=config['connect_timeout'],
            socket_keepalive=config['tcp_keepalive'],
            socket_keepalive_options=config['keepalive_options'],
            health_check_interval=config['health_check_interval']
        )

    def get_connection(self):
        """带超时的连接获取"""
        start_time = time.time()

        while time.time() - start_time < self.config['pool_timeout']:
            try:
                return redis.Redis(connection_pool=self.pool)
            except Exception as e:
                if 'connection pool exhausted' in str(e):
                    time.sleep(0.1)
                    continue
                raise

        raise redis.ConnectionError("Connection pool timeout")

    def health_check(self):
        """连接池健康检查"""
        try:
            client = self.get_connection()
            client.ping()
            return True
        except Exception:
            return False
```

#### 负载测试场景下的超时配置示例

**高并发场景 (QPS > 10000):**
```bash
--connect-timeout-seconds=1
--socket-timeout-seconds=1
--pool-size=100
--tcp-nodelay=true
--tcp-keepalive-seconds=60
```

**跨地域场景 (网络延迟高):**
```bash
--connect-timeout-seconds=10
--socket-timeout-seconds=15
--pool-size=20
--retry-count=5
--retry-delay-ms=200
```

**长时间运行测试:**
```bash
--socket-timeout-seconds=30
--health-check-interval=15
--tcp-keepalive-seconds=600
--circuit-breaker-threshold=10
```

#### 故障注入测试
```bash
--network-partition-probability=0.01
--connection-drop-probability=0.005
--latency-injection-ms=50
```

## 实施优先级建议

### 高优先级扩展:
1. **数据规模扩展**: 负载大小控制、内存监控
2. **超时配置**: 连接超时、重试机制
3. **真实性增强**: 物理约束、多数据类型

### 中优先级扩展:
1. **Redis集群与连接**: Cluster, Sentinel, SSL支持
2. **负载模式多样化**: 流量模式、时间分布
3. **缓存策略丰富**: 多层缓存、驱逐策略

### 低优先级扩展:
1. **容错机制**: 熔断器、故障转移
2. **性能监控**: 详细指标采集
3. **数据质量**: 异常值注入、噪声控制

这些扩展将使基准测试工具更接近真实应用场景，提供更准确的性能评估和容量规划参考。

## 结论

这三个脚本构成了一组完整的Redis基准测试套件，覆盖了物联网、传感器网络、视频分析等常见应用场景。每个脚本都注重模拟真实世界的负载模式，包括并发性、数据分布和读写混合比例。通过这些基准测试，可以评估Redis在不同场景下的性能表现，包括QPS、延迟分布和可靠性指标。