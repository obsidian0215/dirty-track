#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg.py

Enhanced Sensor Aggregator Benchmark with Realistic Sensors and Scalable Data
Supports multiple sensor types, data scale extension, and robust connection management

FEATURES:
  - 数据规模扩展: 可配置负载大小和传感器数量 (per device)
  - 数据类型真实性: 多传感器类型模拟（温度/湿度/压力/振动），传感器漂移和校准
  - 连接超时配置: 连接池管理、超时重试、连接健康检查
  - 多查询模式: 范围查询/计数查询/最值查询

USAGE:
  python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 8 --duration 30 --read-pct 10
  python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 4 --duration 60 --sensors-per-device 10 --sensor-types temperature,humidity,pressure
    python3 bench_sensoragg.py --redis-host 127.0.0.1 --payload-size 2KB --connect-timeout 3 --pool-size 100
  python3 bench_sensoragg.py --redis-host 127.0.0.1 --threads 4 --duration 30 --rps 100  # 限制为100 RPS

EXTENDED USAGE:
  --sensors-per-device: 每个设备的传感器数量 (default: 5)
  --sensor-types: 传感器类型列表 (default: temperature,humidity,pressure,vibration)
  --environmental-noise: 环境噪音级别 (default: 0.05)
    --payload-size: 负载大小目标，带单位（default: 1KB），示例: 256B, 16KB, 1MB
  --connect-timeout: 连接超时秒数 (default: 5)
  --socket-timeout: socket读取超时 (default: 5)
  --pool-timeout: 连接池等待超时 (default: 10)
  --pool-size: 连接池最大连接数 (default: threads*10)
"""
import argparse
import json
import logging
import random
import threading
import time
import statistics
import os
import base64
from typing import List, Optional, Dict, Any
import redis
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

class RateLimiter:
    """Thread-safe token bucket to enforce a global requests-per-second cap."""

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

class SensorAggBench:
    """增强版传感器聚合基准测试，支持真实传感器模拟、数据规模扩展和连接管理"""
    def __init__(self, redis_host: str, redis_port: int, set_key: str = "sensors:ts",
                # 数据规模扩展
                payload_size_bytes: int = 1024, sensors_per_device: int = 5,
               # 数据类型真实性配置
               sensor_types: Optional[List[str]] = None,
               environmental_noise: float = 0.05,
               # 连接超时配置
               connect_timeout: int = 5, socket_timeout: int = 5,
               pool_timeout: int = 10, pool_size: Optional[int] = None,
               # 消息速率控制
               max_requests_per_second: Optional[int] = None,
               # 数据库大小控制
               target_db_size_mb: Optional[float] = None, ttl: int = 3600):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.set_key = set_key
        self.target_db_size_mb = target_db_size_mb
        self._stop = threading.Event()

        # 数据规模扩展配置 (bytes)
        self.payload_size_bytes = payload_size_bytes
        self.sensors_per_device = sensors_per_device  # 每个设备支持的传感器数量

        # 数据类型真实性配置
        self.sensor_types = sensor_types or ["temperature", "humidity", "pressure", "vibration"]
        self.environmental_noise = environmental_noise
        self.sensor_states: Dict[str, Dict[str, Any]] = {}  # 传感器状态跟踪

        # 连接超时配置
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size

        # 初始化连接池
        self.connection_pool = None

        # 数据生命周期管理
        self.ttl = ttl  # 固定的TTL值
        if target_db_size_mb:
            self.set_ttl = ttl  # 初始TTL值为参数指定的值
            self.clean_interval = 200  # 每200个请求调整一次TTL
            # 自适应TTL参数 - 优化实现
            self.avg_payload_size = 512  # 初始估算值，会动态更新
            self.current_ttl = ttl  # 初始TTL值为参数指定的值
            self.last_ttl_adjust = time.time()
            self.adjust_interval = 10  # 每10秒检查一次，提高响应速度
        else:
            self.set_ttl = ttl  # TTL参数值
            self.clean_interval = 200

        # 周期性监控配置
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # 统计
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()
        # 动态跟踪每次写入的payload大小（以字节计）
        self.payload_sizes = []

        # 速率控制参数
        self.max_requests_per_second = max_requests_per_second
        self._rate_limiter = RateLimiter(max_requests_per_second)

        # payload mode: 'json' or 'binary' (binary stored as base64 string)
        self.payload_mode = "json"

        self.request_count = 0

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

    def _get_sensor_state(self, sensor_id: str, sensor_type: str) -> Dict[str, Any]:
        """获取或初始化传感器状态用于真实性模拟"""
        state_key = f"{sensor_type}_{sensor_id}"
        if state_key not in self.sensor_states:
            # 不同传感器类型的基础配置
            base_configs = {
                "temperature": {
                    "range": (15, 35),
                    "drift_rate": 0.02,
                    "noise": 1.0
                },
                "humidity": {
                    "range": (30, 80),
                    "drift_rate": 0.05,
                    "noise": 2.0
                },
                "pressure": {
                    "range": (980, 1020),
                    "drift_rate": 1.0,
                    "noise": 5.0
                },
                "vibration": {
                    "range": (0, 50),
                    "drift_rate": 0.01,
                    "noise": 1.0
                }
            }

            config = base_configs.get(sensor_type, base_configs["temperature"])
            base_value = config["range"][0] + random.random() * (config["range"][1] - config["range"][0])

            self.sensor_states[state_key] = {
                "value": base_value,
                "drift": 0,
                "calibration_drift": 0,
                "last_reading_time": time.time(),
                "readings_count": 0,
                "battery_level": 95 + random.random() * 5,
                "calibration_status": random.choice(["good", "drift", "needs_cal"])
            }

        return self.sensor_states[state_key]

    def _simulate_sensor_reading(self, sensor_id: str, sensor_type: str) -> float:
        """根据传感器类型和状态生成真实读数"""
        state = self._get_sensor_state(sensor_id, sensor_type)
        current_time = time.time()
        time_delta = current_time - state["last_reading_time"]

        # 更新传感器内部状态
        state["readings_count"] += 1

        # 传感器漂移模拟
        drift_change = (random.gauss(0, 0.01) * time_delta)
        state["drift"] += drift_change

        # 校准漂移
        state["calibration_drift"] += (random.gauss(0, 0.001) * time_delta)

        # 电池电量缓慢下降
        state["battery_level"] = max(5, state["battery_level"] - time_delta * 0.001)

        # 读取计数影响随后的读数
        if state["readings_count"] % 1000 == 0:  # 每1000次读取
            state["calibration_status"] = random.choice(["drift", "needs_cal"])

        # 生成基础读数
        base_reading = state["value"] + state["drift"] + state["calibration_drift"]

        # 添加噪音
        noise_factor = self.environmental_noise
        if sensor_type in ["vibration"]:
            # 振动传感器更易受环境影响
            noise_factor *= 2.0

        noise = random.gauss(0, noise_factor * abs(base_reading) * 0.05)
        final_reading = base_reading + noise

        # 应用物理限制
        config = {
            "temperature": {"range": (-40, 85)},
            "humidity": {"range": (0, 100)},
            "pressure": {"range": (500, 1100)},
            "vibration": {"range": (0, 1000)}
        }.get(sensor_type, {"range": (0, 100)})

        final_reading = max(config["range"][0], min(config["range"][1], final_reading))
        state["last_reading_time"] = current_time

        return round(final_reading, 3)

    def _make_reading(self):
        """生成增强的传感器数据，支持多传感器类型和数据规模扩展"""
        sensor_id = f"dev-{random.randint(1, self.sensors_per_device * 10)}"
        sensor_type = random.choice(self.sensor_types)

        # 生成基础传感器数据
        sensor_data = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "timestamp": int(time.time() * 1000),
            "value": self._simulate_sensor_reading(sensor_id, sensor_type),
            "unit": {
                "temperature": "celsius", "humidity": "percent",
                "pressure": "hPa", "vibration": "mm/s"
            }.get(sensor_type, "unit"),
            "status": "active"
        }

        # 添加传感器状态信息
        state = self._get_sensor_state(sensor_id, sensor_type)
        sensor_data.update({
            "battery_level": round(state["battery_level"], 1),
            "calibration_status": state["calibration_status"],
            "readings_count": state["readings_count"]
        })

        # 数据规模扩展 - 添加额外传感器读数达到目标大小（添加随机性: 80%-120%）
        current_size = len(json.dumps(sensor_data))
        target_size_bytes = int(self.payload_size_bytes)  # 已经是字节
        random_multiplier = random.uniform(0.8, 1.2)  # 80%-120%的随机因子
        random_stop_bytes = int(target_size_bytes * random_multiplier)

        if current_size < random_stop_bytes:
            # 添加环境传感器读数，直到接近随机停止点
            additional_readings = []
            remaining_sensor_types = [st for st in self.sensor_types if st != sensor_type]

            while remaining_sensor_types and current_size + len(json.dumps(additional_readings)) < random_stop_bytes:
                env_type = random.choice(remaining_sensor_types)
                env_reading = {
                    "type": env_type,
                    "value": self._simulate_sensor_reading(f"{sensor_id}_env", env_type),
                    "quality": random.choice(["good", "excellent", "poor"]),
                    "metadata": {
                        "calibration_date": f"2023-{random.randint(1,12):02d}",
                        "firmware_version": f"{random.randint(1,3)}.{random.randint(0,9)}"
                    }
                }
                additional_readings.append(env_reading)

            if additional_readings:
                sensor_data["environment_sensors"] = additional_readings
            elif current_size > target_size_bytes:
                logger.warning(f"Baseline sensor data already exceeds target size: {current_size} > {target_size_bytes} bytes")

        return sensor_data

    def _rate_control(self):
        """Global rate control shared across worker threads."""
        self._rate_limiter.acquire()

    def _worker(self, duration: float, read_pct: int, pool):
        r = redis.Redis(connection_pool=pool, decode_responses=True)  # type: ignore[attr-defined]
        end_time = time.time() + duration
        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()
            # 速率控制检查
            self._rate_control()

            try:
                if do_read:
                    now = int(time.time() * 1000)
                    # 支持不同的查询模式：范围查询、最小值查询等
                    query_type = random.choice(["range", "count", "min", "max"])
                    if query_type == "range":
                        results = r.zrangebyscore(self.set_key, now-60000, now, withscores=True)
                    elif query_type == "count":
                        count = r.zcount(self.set_key, now-60000, now)
                        results = count
                    elif query_type == "min":
                        results = r.zrange(self.set_key, 0, 0, withscores=True)
                    else:  # max
                        results = r.zrange(self.set_key, -1, -1, withscores=True)

                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
                else:
                    rcd = self._make_reading()
                    # generate payload according to mode
                    if getattr(self, "payload_mode", "json") == "json":
                        member = json.dumps(rcd)
                        payload_bytes = len(member.encode("utf-8"))
                    else:
                        # binary: generate random bytes approximating payload_size_bytes
                        target_bytes = int(self.payload_size_bytes)
                        raw = os.urandom(max(1, target_bytes))
                        # store as base64 string so redis client (decode_responses=True) can handle it
                        member = base64.b64encode(raw).decode("ascii")
                        payload_bytes = len(raw)

                    try:
                        r.zadd(self.set_key, {member: float(rcd["timestamp"])})
                    except Exception:
                        # fallback to string member if zadd fails for bytes
                        r.zadd(self.set_key, {json.dumps(rcd): float(rcd["timestamp"])})

                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
                        self.request_count += 1
                        # record payload size for metrics
                        if not hasattr(self, "payload_sizes"):
                            self.payload_sizes = []
                        self.payload_sizes.append(payload_bytes)

                    # 设置Sorted Set TTL，确保数据会在设定时间后过期
                    if self.target_db_size_mb:
                        # 自适应模式：使用动态计算的TTL，TTL参数无效
                        try:
                            r.expire(self.set_key, int(self.current_ttl))
                            logger.debug(f"Adaptive TTL set: {self.current_ttl}s")
                        except Exception as ttl_error:
                            logger.debug(f"Adaptive TTL setting failed: {ttl_error}")
                    else:
                        # 固定TTL模式：使用TTL参数值
                        try:
                            r.expire(self.set_key, self.ttl)
                            logger.debug(f"Fixed TTL set: {self.ttl}s")
                        except Exception as ttl_error:
                            logger.debug(f"Fixed TTL setting failed: {ttl_error}")

                    # 设置Sorted Set TTL以防止无限增长
                    if self.request_count % self.clean_interval == 0:
                        if self.target_db_size_mb:
                            self._adaptive_ttl_adjustment(r)
                        else:
                            try:
                                r.expire(self.set_key, self.set_ttl)
                                logger.debug(f"Set TTL for {self.set_key} to {self.set_ttl}s")
                            except Exception as e:
                                logger.debug(f"TTL setting failed: {e}")
            except Exception as e:
                logger.debug("op failed: %s", e)
                with self.lock:
                    self.fail += 1
                time.sleep(0.01)  # 短暂退避

            # 周期性监控输出
            self._periodic_monitoring(duration)

    def _periodic_monitoring(self, total_duration: float):
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
                        recent_count = min(1000, len(self.latencies_ms))
                        recent_latencies = self.latencies_ms[-recent_count:]

                if recent_latencies:
                    recent_latencies.sort()
                    avg_lat = statistics.mean(recent_latencies)
                    p95_lat = recent_latencies[int(len(recent_latencies) * 0.95)] if len(recent_latencies) > 1 else recent_latencies[0]
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput_ops_sec:.1f}, Avg Lat: {avg_lat:.2f}ms, P95: {p95_lat:.2f}ms")
                else:
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput_ops_sec:.1f}")

                self.last_report_time = current_time
                self.last_success_count = success_count

    def run(self, threads: int = 4, duration: int = 10, read_pct: int = 5):
        pool = self._init_connection_pool()

        # 记录测试开始时间，用于计算精确的elapsed时间
        start_time = time.time()

        # 初始化监控参数
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct, pool), daemon=True)
            t.start()
            tlist.append(t)

        logger.info("Started %d threads for %ds (read_pct=%d, sensors_per_device=%d, sensor_types=%s)",
                   threads, duration, read_pct, self.sensors_per_device, self.sensor_types)
        for t in tlist:
            t.join()
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
                    # 目标TTL = 目标大小 / (请求速率 × 平均负载大小)
                    target_ttl_bytes = self.target_db_size_mb * 1024 * 1024
                    data_rate_bytes_per_sec = current_rate * self.avg_payload_size
                    target_ttl_seconds = target_ttl_bytes / data_rate_bytes_per_sec
                    # 限制TTL在合理范围内
                    target_ttl_seconds = max(60, min(86400, target_ttl_seconds))  # 1分钟到24小时

                    # 渐进调整TTL(避免剧烈变化)
                    if abs(target_ttl_seconds - self.current_ttl) > 300:  # 差值超过5分钟
                        self.current_ttl = (self.current_ttl * 0.7) + (target_ttl_seconds * 0.3)
                        redis_client.expire(self.set_key, int(self.current_ttl))
                        logger.info(f"Adaptive TTL adjusted: {self.current_ttl:.0f}s (target: {target_ttl_seconds:.0f}s, db_size: {current_db_size_mb:.1f}MB)")

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

        # METRIC lines are parsed by mig-scripts/result_writer.extract_stats_from_output
        logger.info("METRIC_HEADER\tavg_payload_bytes\tmedian_payload_bytes\ttotal_ops\tops_per_sec")
        logger.info("METRIC_VALUES\t%d\t%d\t%d\t%.2f", avg_payload, median_payload, total, ops_per_sec)

def main():
    parser = argparse.ArgumentParser(description="Enhanced Sensor Aggregator Redis Benchmark")

    # 基础Redis连接
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # 数据规模扩展
    parser.add_argument("--payload-size", default="1KB", type=str,
                       help="Target payload size with units (e.g., 256B, 16KB, 1MB). Examples: 512B, 16KB, 1MB")
    parser.add_argument("--payload-mode", default="json", choices=["json", "binary"],
                       help="Payload mode: 'json' (default) or 'binary' (random bytes, stored base64)")
    parser.add_argument("--sensors-per-device", default=5, type=int,
                       help="Number of sensors per device")

    # 数据类型真实性
    parser.add_argument("--sensor-types", default="temperature,humidity,pressure,vibration",
                       help="Comma-separated list of sensor types")
    parser.add_argument("--environmental-noise", default=0.05, type=float,
                       help="Environmental noise level for sensor simulation")
    parser.add_argument("--set-key", default="sensors:ts", help="Redis sorted set key")

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
    parser.add_argument("--read-pct", default=10, type=int,
                        help="Percent of operations that are reads")

    # 消息速率控制
    parser.add_argument("--rps", "--max-requests-per-second", dest="rps",
                        type=int, help="Maximum requests per second (default: no limit)")

    # 数据库大小控制
    parser.add_argument("--target-db-size-mb", type=float,
                        help="Target database size in MB (enables adaptive TTL adjustment)")
    parser.add_argument("--ttl", type=int, default=3600,
                        help="TTL in seconds when not using adaptive mode (default: 3600)")

    args = parser.parse_args()

    # 解析传感器类型参数
    sensor_types = [st.strip() for st in args.sensor_types.split(",")]

    # 计算默认池大小
    pool_size = args.pool_size
    if pool_size is None:
        pool_size = args.threads * 10

    # payload_size: parse unit-aware --payload-size flag (supports units: B, KB, MB)
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
    payload_size_bytes = int(payload_bytes)

    bench = SensorAggBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        set_key=args.set_key,
    # 数据规模扩展 (bytes)
    payload_size_bytes=payload_size_bytes,
        sensors_per_device=args.sensors_per_device,
        # 数据类型真实性
        sensor_types=sensor_types,
        environmental_noise=args.environmental_noise,
        # 连接超时配置
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=pool_size,
        # 消息速率控制
        max_requests_per_second=args.rps,
        # 数据库大小控制
        target_db_size_mb=args.target_db_size_mb,
        ttl=args.ttl
    )
    bench.payload_mode = args.payload_mode
    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)

if __name__ == "__main__":
    main()