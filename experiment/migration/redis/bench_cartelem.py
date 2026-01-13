#!/usr/bin/env python3
# coding: utf-8
"""
bench_cartelem.py (Enhanced Version)

Advanced Car Telematics Benchmark for Redis Stream
Enhanced with realistic data generation, scalable payload sizes, and robust connection management

FEATURES:
  - 数据规模扩展: 可配置负载大小和分布模式 (uniform/normal/zipf)
  - 数据类型真实性: 车辆行为模式、物理约束模拟、诊断数据生成
  - 连接超时配置: 连接池管理、超时重试、连接健康检查

USAGE:
    python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30 --vehicle-pattern highway --payload-size 5KB --connect-timeout 2
  python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 60 --size-distribution normal --pool-size 50
  python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 30 --rps 100  # 限制为100 RPS

EXTENDED USAGE:
  --vehicle-pattern: normal_city/highway/stop_go (default: normal_city)
    --payload-size: Target payload size with units (default: 1KB). Examples: 256B, 16KB, 1MB
  --size-distribution: uniform/normal/zipf (default: uniform)
  --connect-timeout: Connection timeout seconds (default: 5)
  --socket-timeout: Socket timeout seconds (default: 5)
  --pool-timeout: Pool wait timeout seconds (default: 10)
  --pool-size: Connection pool max size (default: threads*10)
  --rps/--max-requests-per-second: Maximum requests per second (default: no limit)
"""
import argparse
import json
import logging
import random
import threading
import time
import statistics
from collections import Counter
from typing import List, Optional
import os
import base64
import redis
import re
import math

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

class RateLimiter:
    """Thread-safe token bucket to cap requests-per-second across threads."""

    def __init__(self, rate: Optional[int]):
        self.rate = rate
        if rate:
            self._capacity = float(rate)
            self._tokens = float(rate)
            self._last_refill = time.monotonic()
            self._lock = threading.Lock()

    def acquire(self):
        if not self.rate:
            return

        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                if elapsed > 0:
                    refill = elapsed * self._capacity
                    self._tokens = min(self._capacity, self._tokens + refill)
                    self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                deficit = 1.0 - self._tokens
                wait_time = deficit / self._capacity

            time.sleep(wait_time)

class CarTelematicsBench:
    """车联网写入 Redis Stream 的负载发生器"""
    def __init__(self, redis_host: str, redis_port: int, stream_name: str = "vehicle:telemetry",
                 # 数据规模扩展
                 payload_size_bytes: int = 1024, size_distribution: str = "uniform",
                # 数据类型真实性
                vehicle_pattern: str = "normal_city",
                # 连接超时配置
                connect_timeout: int = 5, socket_timeout: int = 5,
                pool_timeout: int = 10, pool_size: Optional[int] = None,
                # 消息速率控制
                max_requests_per_second: Optional[int] = None,
                # 数据库大小控制
                target_db_size_mb: Optional[float] = None, ttl: int = 3600,
                stream_maxlen: Optional[int] = None):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.stream_name = stream_name
        self._stop = threading.Event()

        # 数据规模扩展配置 (bytes)
        self.payload_size_bytes = payload_size_bytes
        self.size_distribution = size_distribution  # uniform, normal, zipf

        # 数据类型真实性配置
        self.vehicle_pattern = vehicle_pattern  # normal_city, highway, stop_go
        self.vehicle_states = {}  # 车辆状态跟踪

        # 连接超时配置
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size or (10 * 4)  # 默认10倍线程数

        # 消息速率控制配置
        self.max_requests_per_second = max_requests_per_second
        self._rate_limiter = RateLimiter(max_requests_per_second)

        # 数据生命周期管理
        self.target_db_size_mb = target_db_size_mb  # 目标数据库大小(MB)
        self.ttl = ttl  # 固定的TTL值
        # TTL为主要机制，stream_maxlen作为可选辅助机制
        self.stream_maxlen = stream_maxlen  # 可以设置为None，默认不使用

        # 自适应TTL参数
        if self.target_db_size_mb:
            self.payload_sizes = []  # 动态跟踪payload大小
            self.avg_payload_size = 512  # 初始估算值，会动态更新
            self.current_ttl = 3600  # 初始TTL 1小时
            self.last_ttl_adjust = time.time()
            self.adjust_interval = 10  # 每10秒检查一次，提高响应速度
            self.size_history = []  # 存储最近的数据库大小历史

        # 统计
        self.latencies_ms = []  # 全局收集（注意内存）
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # device tracking and realism controls
        self.device_counter = Counter()
        self.device_count = 10000  # default vehicle id pool
        self.pacing = "none"  # none|poisson
        self.payload_mixture = None
        self.report_realism = False

        # ensure payload_sizes exists and payload_mode default
        self.payload_sizes = getattr(self, "payload_sizes", [])
        self.payload_mode = "json"

        # 初始化连接池
        self.connection_pool = None

        # 清理间隔计数器
        self.clean_interval = 100  # 每100个请求清理一次
        self.request_count = 0

        # 周期性输出参数
        self.monitor_interval = 1.0  # 监控间隔(秒)
        self.last_report_time = 0
        self.last_success_count = 0

    def _rate_controller(self, op_start_time: float):
        """Control global request rate via token bucket."""
        self._rate_limiter.acquire()

    def _init_connection_pool(self):
        """初始化Redis连接池"""
        if self.connection_pool is None:
            self.connection_pool = redis.ConnectionPool(  # type: ignore[attr-defined]
                host=self.redis_host,
                port=self.redis_port,
                socket_connect_timeout=self.connect_timeout,
                socket_timeout=self.socket_timeout,
                max_connections=self.pool_size
            )
        return self.connection_pool

    def _get_vehicle_state(self, vehicle_id: str) -> dict:
        """获取或初始化车辆状态（用于真实性模拟）"""
        if vehicle_id not in self.vehicle_states:
            self.vehicle_states[vehicle_id] = {
                "speed": random.uniform(10, 30),  # 初始速度
                "fuel": 100 - random.random() * 20,  # 燃油水平
                "lat": 31.0 + random.random() * 0.1,
                "lon": 121.0 + random.random() * 0.1,
                "engine_temp": 85 + random.random() * 15,
                "last_update": time.time()
            }
        return self.vehicle_states[vehicle_id]

    def _update_vehicle_state(self, vehicle_id: str, new_speed: float):
        """根据车辆物理特性更新状态"""
        state = self._get_vehicle_state(vehicle_id)
        curr_time = time.time()
        time_delta = curr_time - state["last_update"]

        # 更新地理位置基于航向角和速度
        if "heading" not in state:
            state["heading"] = random.uniform(0, 360)
        else:
            state["heading"] += random.uniform(-10, 10) # 随机摆动

        dist_km = (new_speed * time_delta) / 3600.0
        rad = math.radians(state["heading"])
        # 1度纬度约111km，1度经度约111km * cos(lat)
        d_lat = dist_km * math.cos(rad) / 111.0
        d_lon = dist_km * math.sin(rad) / (111.0 * math.cos(math.radians(state.get("lat", 31.0))))

        state["lat"] += d_lat
        state["lon"] += d_lon

        # 燃油消耗计算 (L/100km)
        fuel_consumption = (abs(new_speed - state["speed"]) * time_delta + new_speed * time_delta) * 0.001
        state["fuel"] = max(0, state["fuel"] - fuel_consumption)

        # 发动机温度计算
        if new_speed > 80:
            state["engine_temp"] = min(120, state["engine_temp"] + (new_speed * time_delta * 0.1))
        else:
            state["engine_temp"] = max(60, state["engine_temp"] - (time_delta * 5))

        state["speed"] = new_speed
        state["last_update"] = curr_time

    def _generate_obd_codes(self) -> List[str]:
        """生成OBD故障码"""
        obd_codes = ["P0100", "P0101", "P0200", "P0300", "P0400", "P0500", "P0600"]
        return random.sample(obd_codes, random.randint(0, min(3, len(obd_codes))))

    def _calculate_dynamic_speed(self, vehicle_id: str) -> float:
        """基于车辆行为模式计算实时速度"""
        patterns = {
            "normal_city": {"min": 0, "max": 50, "accel_rate": 0.5},
            "highway": {"min": 60, "max": 120, "accel_rate": 1.2},
            "stop_go": {"min": 0, "max": 40, "accel_rate": 2.0}
        }

        pattern = patterns[self.vehicle_pattern]
        base_speed = self._get_vehicle_state(vehicle_id)["speed"]

        # 应用加速/减速约束
        acceleration = pattern["accel_rate"] * (random.random() - 0.5)
        new_speed = max(0, min(base_speed + acceleration, pattern["max"]))

        # 特定模式行为
        if self.vehicle_pattern == "stop_go":
            # 更频繁的停止启动
            if random.random() < 0.3:
                new_speed = random.uniform(20, 40)
            elif random.random() < 0.2:
                new_speed = 0

        elif self.vehicle_pattern == "highway":
            # 高速度保持
            if base_speed < 70 and random.random() < 0.8:
                new_speed = min(pattern["max"], base_speed + 2)

        return round(new_speed, 2)

    def sample_payload_size_from_mixture(self):
        spec = getattr(self, 'payload_mixture', None)
        if not spec:
            return int(self.payload_size_bytes)
        total = sum(w for w, lo, hi in spec)
        p = random.random() * total
        cum = 0.0
        for w, lo, hi in spec:
            cum += w
            if p <= cum:
                return random.randint(lo, hi)
        w, lo, hi = spec[-1]
        return random.randint(lo, hi)

    def sample_interarrival(self):
        if getattr(self, 'pacing', None) == 'poisson':
            rate = getattr(self, 'max_requests_per_second', None) or 1.0
            if not rate or rate <= 0:
                return 0.0
            return random.expovariate(rate)
        return 0.0

    def _make_payload(self, vehicle_id: Optional[str] = None):
        """生成增强的车辆遥测数据"""
        if not vehicle_id:
            if getattr(self, 'device_count', None):
                vehicle_id = f"veh-{random.randint(1, self.device_count)}"
            else:
                vehicle_id = f"veh-{random.randint(1000,9999)}"

        # 获取车辆状态
        state = self._get_vehicle_state(vehicle_id)
        current_speed = self._calculate_dynamic_speed(vehicle_id)

        # 更新车辆状态
        self._update_vehicle_state(vehicle_id, current_speed)

        # 基础数据
        payload = {
            "vehicle_id": vehicle_id,
            "timestamp": int(time.time()*1000),
            "location": {
                "lat": round(state["lat"], 6),
                "lon": round(state["lon"], 6)
            },
            "speed_kmh": current_speed,
            "fuel_level": round(state["fuel"], 2),
            "engine_temp": round(state["engine_temp"], 2),
            "diagnostic": {
                "obd_codes": self._generate_obd_codes(),
                "check_engine_light": random.random() < 0.1,
                "battery_voltage": round(12.6 + (random.random() - 0.5) * 0.5, 2),
                "transmission_temp": round(85 + (random.random() - 0.5) * 10, 2)
            }
        }

        # 数据规模扩展 (应用指定分布)
        current_size = len(json.dumps(payload))
        if getattr(self, 'payload_mixture', None):
            base_target_bytes = int(self.sample_payload_size_from_mixture())
        else:
            base_target_bytes = int(self.payload_size_bytes)  # 基本目标大小

        # 应用分布函数
        if self.size_distribution == "uniform":
            random_multiplier = random.uniform(0.8, 1.2)
        elif self.size_distribution == "normal":
            random_multiplier = random.gauss(1.0, 0.1)  # 正态分布，均值1，标准差0.1
            random_multiplier = max(0.7, min(1.3, random_multiplier))  # 限制在70%-130%
        elif self.size_distribution == "zipf":
            random_multiplier = random.betavariate(2, 5) * 0.8 + 0.6  # Zipf-like分布，偏向较小值
        else:
            random_multiplier = 1.0  # 默认fallback

        target_size_bytes = int(base_target_bytes * random_multiplier)

        if current_size < target_size_bytes:
            # 添加额外的传感器数据
            sensors = []
            sensor_types = ["gps_accuracy", "gyroscope", "accelerometer", "magnetometer",
                          "tire_pressure", "brake_pressure", "throttle_position", "exhaust_sensor"]

            while current_size + len(json.dumps(sensors)) < target_size_bytes and sensors is not None:
                sensor = {
                    "type": random.choice(sensor_types),
                    "value": random.random() * random.choice([100, 200, 500, 1000]),
                    "unit": random.choice(["meters", "degrees", "g", "pa", "percentage"]),
                    "precision": round(random.random() * 0.1, 6),
                    "calibration_date": f"2023-{random.randint(1,12):02d}-01"
                }
                sensors.append(sensor)

                # 检查添加后是否超过限制
                new_size = len(json.dumps({**payload, "sensors": sensors}))
                if new_size > target_size_bytes:
                    sensors.pop()  # 移除添加的传感数据
                    logger.debug(f"Payload size would exceed target {target_size_bytes} bytes (would be {new_size}), truncated")
                    break

            if sensors:
                payload["sensors"] = sensors
            elif current_size > target_size_bytes:
                logger.warning(f"Baseline sensor data already exceeds target size: {current_size} > {target_size_bytes} bytes")

        return payload

    def _worker(self, duration, pool):
        r = redis.Redis(connection_pool=pool, decode_responses=True)  # type: ignore[attr-defined]
        end_time = time.time() + duration
        vehicle_id = None  # 为每个线程维护车辆ID以保持连续性
        op_start_time = time.time()

        while time.time() < end_time and not self._stop.is_set():
            # realism-driven pacing (poisson)
            if getattr(self, 'pacing', None) == 'poisson' and getattr(self, 'max_requests_per_second', None):
                try:
                    interval = self.sample_interarrival()
                    if interval and interval > 0:
                        time.sleep(interval)
                except Exception:
                    pass
            # 应用速率控制 (token-bucket upper bound)
            self._rate_controller(op_start_time)

            payload = self._make_payload(vehicle_id)
            vehicle_id = payload["vehicle_id"]  # 更新车辆ID以保持连续性

            # prepare data according to payload_mode
            if getattr(self, "payload_mode", "json") == "json":
                data_str = json.dumps(payload)
                payload_size = len(data_str.encode("utf-8"))
            else:
                target_bytes = int(self.payload_size_bytes)
                raw = os.urandom(max(1, target_bytes))
                data_str = base64.b64encode(raw).decode("ascii")
                payload_size = len(raw)

            op_start_time = time.perf_counter()
            try:
                # 1. 写入实时遥测流 (XADD)
                r.xadd(self.stream_name, {"data": data_str})

                # 2. 地理位置协同 (真实边缘计算 V2X)
                state = self._get_vehicle_state(vehicle_id)
                geo_key = "v2x:positions"
                r.geoadd(geo_key, (state["lon"], state["lat"], vehicle_id))

                # 模拟 V2X 场景：10% 概率查询周边 500m 车辆进行碰撞风险计算
                if random.random() < 0.1:
                    r.georadius(geo_key, state["lon"], state["lat"], 500, unit='m')

                lat = (time.perf_counter() - op_start_time) * 1000.0
                with self.lock:
                    self.latencies_ms.append(lat)
                    self.success += 1
                    self.request_count += 1

                    # record payload size for metrics
                    try:
                        self.payload_sizes.append(int(payload_size))
                        if len(self.payload_sizes) > 1000:
                            self.payload_sizes.pop(0)
                        # update avg if adaptive
                        if self.target_db_size_mb:
                            self.avg_payload_size = sum(self.payload_sizes) / len(self.payload_sizes)
                        # track device usage
                        try:
                            self.device_counter[vehicle_id] += 1
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # 设置Stream TTL，确保数据会在设定时间后过期
                    if self.target_db_size_mb:
                        # 自适应模式：使用动态计算的TTL，TTL参数无效
                        try:
                            r.expire(self.stream_name, int(self.current_ttl))
                            logger.debug(f"Adaptive TTL set: {self.current_ttl}s")
                        except Exception as ttl_error:
                            logger.debug(f"Adaptive TTL setting failed: {ttl_error}")
                    else:
                        # 固定TTL模式：使用TTL参数值
                        try:
                            r.expire(self.stream_name, self.ttl)
                            logger.debug(f"Fixed TTL set: {self.ttl}s")
                        except Exception as ttl_error:
                            logger.debug(f"Fixed TTL setting failed: {ttl_error}")

                # 数据生命周期管理
                if self.target_db_size_mb:
                    self._adaptive_ttl_adjustment(r)
                elif self.stream_maxlen and self.request_count % self.clean_interval == 0:
                    try:
                        r.xtrim(self.stream_name, maxlen=self.stream_maxlen, approximate=True)
                        logger.debug(f"Cleaned stream {self.stream_name}, maxlen={self.stream_maxlen}")
                    except Exception as e:
                        logger.debug(f"Stream cleanup failed: {e}")

            except Exception as e:
                logger.debug("xadd failed: %s", e)
                with self.lock:
                    self.fail += 1
                # 出错时仍应用速率控制，避免风暴式重试
                time.sleep(0.01)  # 短暂退避

            # 周期性监控输出
            self._periodic_monitoring(duration)

    def _periodic_monitoring(self, total_duration):
        """周期性输出Redis处理吞吐量和延迟"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.monitor_interval:
            # 正确的elapsed时间计算
            elapsed = current_time - self.start_time
            success_count = self.success
            new_operations = success_count - self.last_success_count

            if new_operations >= 0:
                throughput_ops_sec = new_operations / (current_time - self.last_report_time)

                # 计算当前延迟统计
                recent_latencies = []
                with self.lock:
                    if self.latencies_ms:
                        recent_latencies = self.latencies_ms[-1000:] if len(self.latencies_ms) >= 1000 else self.latencies_ms[:]

                if recent_latencies:
                    recent_latencies.sort()
                    avg_lat = statistics.mean(recent_latencies)
                    p95_lat = recent_latencies[int(len(recent_latencies) * 0.95)] if len(recent_latencies) > 1 else recent_latencies[0]
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput_ops_sec:.1f}, Avg Lat: {avg_lat:.2f}ms, P95: {p95_lat:.2f}ms")
                else:
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput_ops_sec:.1f}")

                self.last_report_time = current_time
                self.last_success_count = success_count

    def run(self, threads: int = 4, duration: int = 10):
        # 初始化连接池
        pool = self._init_connection_pool()

        # 记录测试开始时间，用于计算精确的elapsed时间
        start_time = time.time()

        # 初始化监控参数
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, pool), daemon=True)
            t.start()
            tlist.append(t)

        logger.info("Started %d threads for %ds (vehicle_pattern=%s, payload_kb=%d)",
                   threads, duration, self.vehicle_pattern, self.payload_size_bytes)
        for t in tlist:
            t.join()
        logger.info("Workers finished")
        self._print_summary(duration)

    def _adaptive_ttl_adjustment(self, redis_client):
        """自适应TTL调整算法"""
        current_time = time.time()

        # 定期检查和调整TTL
        if current_time - self.last_ttl_adjust >= self.adjust_interval:
            try:
                # 获取当前数据库大小(估算)
                info = redis_client.info('memory')
                current_db_size_mb = info['used_memory'] / 1024 / 1024

                # 获取当前请求速率
                current_rate = self.success / max(1, current_time - self.start_time)

                # 根据目标大小和当前速率计算理想TTL
                if current_rate > 0 and self.target_db_size_mb is not None:
                    # 改进的目标TTL计算：使用动态payload大小
                    target_ttl_bytes = self.target_db_size_mb * 1024 * 1024
                    data_rate_bytes_per_sec = current_rate * self.avg_payload_size
                    target_ttl_seconds = target_ttl_bytes / data_rate_bytes_per_sec

                    # 限制TTL在合理范围内，避免极端值
                    target_ttl_seconds = max(60, min(86400, target_ttl_seconds))  # 1分钟到24小时

                    # 更平滑的TTL调整算法
                    ttl_diff = target_ttl_seconds - self.current_ttl
                    if abs(ttl_diff) > 60:  # 差值超过1分钟就开始调整
                        # 根据差值大小调整步长：小差值稳步调整，大差值快速调整
                        if abs(ttl_diff) < 300:  # 小于5分钟，稳步调整
                            adjust_factor = 0.1
                        elif abs(ttl_diff) < 1800:  # 5-30分钟，中等调整
                            adjust_factor = 0.2
                        else:  # 大于30分钟，快速调整
                            adjust_factor = 0.5

                        self.current_ttl = self.current_ttl + (ttl_diff * adjust_factor)
                        redis_client.expire(self.stream_name, int(self.current_ttl))
                        logger.info(f"Adaptive TTL adjusted: {self.current_ttl:.0f}s (target: {target_ttl_seconds:.0f}s, payload_avg: {self.avg_payload_size:.0f}B, db_size: {current_db_size_mb:.1f}MB)")

                self.last_ttl_adjust = current_time

            except Exception as e:
                logger.debug(f"TTL adjustment failed: {e}")

    def _print_summary(self, duration):
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)
        logger.info("Total ops: %d success=%d fail=%d ops/sec=%.2f", total, self.success, self.fail, ops_per_sec)
        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[int(len(lat)*p/100)]
            logger.info("Latency ms - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                        statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

        # Emit structured metrics for downstream parsers
        try:
            if self.payload_sizes:
                avg_payload = int(statistics.mean(self.payload_sizes))
                median_payload = int(statistics.median(self.payload_sizes))
            else:
                avg_payload = 0
                median_payload = 0
        except Exception:
            avg_payload = 0
            median_payload = 0

        logger.info("METRIC_HEADER\tavg_payload_bytes\tmedian_payload_bytes\ttotal_ops\tops_per_sec")
        logger.info("METRIC_VALUES\t%d\t%d\t%d\t%.2f", avg_payload, median_payload, total, ops_per_sec)

        # optionally write realism report
        if getattr(self, 'report_realism', False):
            try:
                ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
                outdir = os.path.join(ROOT, 'config_tests', 'results')
                os.makedirs(outdir, exist_ok=True)
                fn = os.path.join(outdir, f'realism_report_redis_cartelem_{int(time.time())}.json')
                ps = {'count': len(self.payload_sizes)}
                if self.payload_sizes:
                    ps['avg'] = statistics.mean(self.payload_sizes)
                    ps['median'] = statistics.median(self.payload_sizes)
                    ps['p95'] = sorted(self.payload_sizes)[int(len(self.payload_sizes) * 0.95)]
                top_veh = self.device_counter.most_common(10)
                rep = {'bench': 'redis_cartelem', 'payload': ps, 'unique_vehicles': len(self.device_counter), 'top_vehicles': top_veh, 'pacing': self.pacing, 'device_count': self.device_count, 'payload_mixture': self.payload_mixture}
                with open(fn, 'w', encoding='utf-8') as f:
                    json.dump(rep, f, indent=2, ensure_ascii=False)
                logger.info("Wrote realism report %s", fn)
            except Exception as e:
                logger.debug("Failed to write realism report: %s", e)

def main():
    parser = argparse.ArgumentParser(description="Enhanced Car Telematics Redis Benchmark")

    # 基础Redis连接
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # 数据规模扩展
    parser.add_argument("--payload-size", default="1KB", type=str,
                       help="Target payload size with units (e.g., 256B, 16KB, 1MB). Examples: 512B, 16KB, 1MB")
    parser.add_argument("--payload-mode", default="json", choices=["json", "binary"],
                        help="Payload mode: json (structured) or binary (random bytes, base64-encoded)")
    parser.add_argument("--size-distribution", default="uniform", type=str,
                       choices=["uniform", "normal", "zipf"],
                       help="Distribution type for payload sizes")

    # 数据类型真实性
    parser.add_argument("--vehicle-pattern", default="normal_city", type=str,
                       choices=["normal_city", "highway", "stop_go"],
                       help="Vehicle driving pattern")
    parser.add_argument("--stream", default="vehicle:telemetry",
                       help="Redis stream name")

    # 连接超时配置
    parser.add_argument("--connect-timeout", default=5, type=int,
                       help="Redis connection timeout in seconds")
    parser.add_argument("--socket-timeout", default=5, type=int,
                       help="Socket read timeout in seconds")
    parser.add_argument("--pool-timeout", default=10, type=int,
                       help="Connection pool timeout in seconds")
    parser.add_argument("--pool-size", type=int,
                       help="Connection pool max size (default: threads*10)")

    # 负载参数
    parser.add_argument("--threads", default=4, type=int,
                       help="Number of worker threads")
    parser.add_argument("--duration", default=10, type=int,
                       help="Benchmark duration in seconds")

    # 消息速率控制
    parser.add_argument("--rps", "--max-requests-per-second", dest="rps",
                        type=int,
                        help="Maximum requests per second (default: no limit, based on hardness sleep)")

    # 数据库大小控制
    parser.add_argument("--target-db-size-mb", type=float,
                        help="Target database size in MB (enables adaptive TTL adjustment)")
    parser.add_argument("--ttl", type=int, default=3600,
                        help="TTL in seconds when not using adaptive mode (default: 3600)")
    parser.add_argument("--stream-maxlen", type=int,
                        help="Optional: Maximum Redis Stream length (complements TTL, use as needed)")

    # Realism controls
    parser.add_argument("--device-count", type=int, default=None, help="Total number of vehicles/devices")
    parser.add_argument("--pacing", default="none", choices=["none","poisson"], help="Traffic pacing mode (none|poisson)")
    parser.add_argument("--payload-mixture", default=None, type=str, help="payload mixture e.g. '70:50-200,25:512-1024,5:3500-8000'")
    parser.add_argument("--report-realism", action="store_true", help="Write realism JSON report to config_tests/results on completion")

    args = parser.parse_args()

    # 计算默认池大小
    pool_size = args.pool_size
    if pool_size is None:
        pool_size = args.threads * 10

    # unit-aware payload size parsing: parse --payload-size tokens with units (B/KB/MB)
    def parse_size_token(tok: str) -> int:
        t = tok.strip()
        m = re.match(r"^(\d+(?:\.\d+)?)([a-zA-Z]*)$", t)
        if not m:
            raise ValueError(f"invalid size token: {tok}")
        val = float(m.group(1))
        unit = m.group(2).lower()
        if unit in ("b", "byte", "bytes") or unit == "":
            return int(val)
        if unit in ("k", "kb", "kib"):
            return int(val * 1024)
        if unit in ("m", "mb", "mib"):
            return int(val * 1024 * 1024)
        raise ValueError(f"unknown size unit: {unit}")

    # parse canonical unit-aware --payload-size (string) into bytes
    payload_bytes = parse_size_token(args.payload_size)

    def parse_mixture_spec(spec: str):
        parts = [p.strip() for p in spec.split(',') if p.strip()]
        out = []
        for part in parts:
            try:
                w, rng = part.split(':', 1)
                lo, hi = rng.split('-', 1)
                wv = float(w)
                lval = parse_size_token(lo)
                hval = parse_size_token(hi)
                out.append((wv, int(lval), int(hval)))
            except Exception:
                raise ValueError(f"invalid payload_mixture spec: {part}")
        return out

    bench = CarTelematicsBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        stream_name=args.stream,
        # 数据规模扩展 (bytes)
        payload_size_bytes=payload_bytes,
        size_distribution=args.size_distribution,
        # 数据类型真实性
        vehicle_pattern=args.vehicle_pattern,
        # 连接超时配置
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=pool_size,
        # 消息速率控制
        max_requests_per_second=args.rps,
        # 数据库大小控制
        target_db_size_mb=args.target_db_size_mb,
        ttl=args.ttl,
        stream_maxlen=args.stream_maxlen
    )
    bench.payload_mode = args.payload_mode

    # realism assignments
    bench.device_count = args.device_count or bench.device_count
    bench.pacing = args.pacing
    bench.report_realism = args.report_realism
    bench.payload_mixture = parse_mixture_spec(args.payload_mixture) if args.payload_mixture else None

    bench.run(threads=args.threads, duration=args.duration)

if __name__ == "__main__":
    main()