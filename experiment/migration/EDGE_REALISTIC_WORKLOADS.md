# 边缘计算真实负载场景与数据规范

本文档定义了本研究中使用的四种核心边缘负载场景。这些负载旨在模拟真实物理约束下的有状态容器行为，验证 `dirty-track` 内核模块在复杂内存存取模式下的迁移表现。

---

## 一、核心场景设计

### 1. 工业物联网：预测性维护 (Industrial IoT - PdM)
**场景描述**：边缘节点实时分析产线振动与温度数据，通过 5 分钟滑动窗口检测设备故障（如轴承磨损）。
**真实性核心**：**状态依赖型分析**。丢失内存状态将导致异常检测基线被重置，产生数分钟的监控盲区。

*   **数据结构**：
    *   `device:{id}:telemetry` (Redis Stream): 原始高频数据流。
    *   `device:{id}:stats` (Redis Hash): 滑动窗口内均值、方差、峰值。
    *   `global:alerts` (Redis Sorted Set): 异常评分排行。
*   **内存行为**：高频、连续的内存追加（Append-only），模拟极高的脏页产生率。

### 2. 车联网：V2X 与协同感知 (Transportation - V2X)
**场景描述**：模拟车辆在路网中的低延迟通信。边缘节点维护动态车辆位置索引，并向 500 米范围内的车辆广播潜在碰撞风险。
**真实性核心**：**地理空间一致性**。迁移过程中必须保持 Geo 索引的原子更新，否则会导致虚假避障或漏警。

*   **数据结构**：
    *   `v2x:positions` (Redis Geo): 车辆经纬度实时索引。
    *   `v2x:events:{sector_id}` (Redis Pub/Sub): 局部交通事件广播通道。
    *   `v2x:vehicle:{id}` (Redis Hash): 车辆航向角、加速度、刹车状态。
*   **内存行为**：频繁的树型索引重构（B-tree/SkipList），导致大量非连续脏页。

### 3. 视频分析：智能监控追踪 (Surveillance - Tracking)
**场景描述**：边缘 AI 推理后，将目标 ID 与追踪状态缓存。系统通过跨帧关联（Tracking）识别停留时长异常或非法进入。
**真实性核心**：**身份持久性**。迁移后若丢失追踪 Hash 表，系统会将同一目标重置为新 ID。

*   **数据结构**：
    *   `track:{obj_id}` (Redis Hash): 包含 `start_time`, `last_coord`, `appearance_feat`。
    *   `cam:{id}:active_objs` (Redis List): 摄像头当前视野内的目标列表（TTL 5s）。
*   **内存行为**：大量的 KV 更新与过期清理（Eviction），模拟内存碎片化读写模式。

### 4. 传感器聚合：高基数时序数据 (Sensor Aggregation)
**场景描述**：模拟数千个传感器向边缘网关上报环境数据。网关进行本地聚合后，定期将结果同步至云端。
**真实性核心**：**高基数索引压测**。模拟成千上万个独立标签（Tags）对数据库索引页的压力。

*   **数据结构 (InfluxDB)**：
    *   Measurement: `environment`
    *   Tags: `sensor_id`, `room_id`, `firmware_ver`
    *   Fields: `temp`, `humidity`, `voc`
*   **内存行为**：LSM-Tree 结构的写入与压缩，产生爆发性的磁盘 I/O 和内存缓冲区更新。

---

## 二、数据格式定义

| 负载类型 | 典型 Payload (JSON) | 平均大小 | 写入频率 |
| :--- | :--- | :--- | :--- |
| **工业 PdM** | `{"ts": 167368, "vib_x": 12.5, "temp": 85.2, "anomaly_score": 0.05}` | ~150 B | 100 - 1000 Hz |
| **V2X** | `{"vid": "car_45", "lat": 39.9, "lon": 116.4, "speed": 80, "heading": 180}` | ~200 B | 10 - 50 Hz |
| **视频分析** | `{"id": "obj_992", "type": "person", "bbox": [10, 20, 50, 100], "confidence": 0.98}` | ~500 B | 15 - 30 FPS |

---

## 三、迁移验证指标 (Key Metrics)

针对上述负载，热迁移应通过以下指标验证其真实价值：

1.  **State Recovery Gap (状态恢复间隙)**：
    *   迁移后重建滑动窗口所需的时间。
    *   *目标*：热迁移应实现 0 间隙（状态完全保留）。
2.  **Tracking ID Continuity (追踪 ID 连续性)**：
    *   迁移前后同一物理目标被识别为同一 ID 的比例。
    *   *目标*：100% 连续。
3.  **Geo-Query Latency Spike (地理查询延迟峰值)**：
    *   迁移瞬时 `GEORADIUS` 指令的 P99 延迟。
    *   *目标*：< 200 ms。

---

## 四、下一步行动计划 (Phase 2)

### 1. 完善脚本实现
*   [ ] 为 `bench_cartelem.py` 增加 `GEOADD` 动作。
*   [ ] 为 `bench_predictive_maintenance.py` 补充 LUA 读取聚合逻辑。
*   [ ] 为 `bench_surveillance_analytics.py` 增加跨 100 帧的模拟追踪状态。

### 2. 真实性验证工具
使用 `realism_audit.py` 定期审计模拟流量的熵值分布（Entropy Distribution），确保其不退化为简单随机数。

---

## 五、脚本使用指南

### 1. 启动 Redis 场景负载
```bash
# 场景 1: 工业预测性维护 (PdM)
python3 industrial/bench_predictive_maintenance.py --redis-host 127.0.0.1 --devices 100 --sampling-rate 50

# 场景 2: 车联网 V2X (含 Geo 索引与周边查询)
python3 redis/bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --vehicle-pattern highway

# 场景 3: 视频追踪分析
python3 video/bench_surveillance_analytics.py --redis-host 127.0.0.1 --cameras 32 --fps 15
```

### 2. 启动 InfluxDB 场景负载
```bash
# 场景 4: 高基数传感器聚合
python3 influxdb/bench_sensoragg.py --influx-url http://localhost:8181 --devices 5000

# 场景 5: 车联网时序历史存贮
python3 influxdb/bench_cartelem.py --influx-url http://localhost:8181 --payload-size 2KB
```

### 3. 在迁移测试中集成
编辑 `mig-scripts/redis_test.py` 或 `mig-scripts/influxdb_test.py` 中的 `scene_configs`，确保指向正确的脚本路径并配置真实性参数（如 `--vehicle-pattern` 或 `--anomaly-rate`）。



#### **场景2: 工业视觉质检 (Automated Visual Inspection)**

**业界应用实例**:
- **Cognex In-Sight**: 视觉检测系统
- **Keyence CV-X**: 图像处理与缺陷检测
- **Landing AI**: 制造业AI视觉平台

**内存优先原因**:
- 高速产线：100-300件/分钟，推理结果需即时缓存
- 上下文关联：当前缺陷与前N个检测结果比对
- 统计分析：实时合格率、缺陷趋势

**容器化方案**:
```yaml
容器类型: Redis + Elasticsearch
- Redis Hashes: 最近1000个检测结果（滚动窗口）
- Redis HyperLogLog: 实时统计合格率
- Elasticsearch: 缺陷数据持久化与历史分析

数据流:
产线相机 → AI推理 → Redis写入 →
  ├─→ 缓存未命中: 新检测
  └─→ 缓存命中: 相同产品重复检测（跳过）
定期批量: Redis → Elasticsearch（每分钟）
```

---