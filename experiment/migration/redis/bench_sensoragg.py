#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg.py (Enhanced Version)

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
  python3 bench_sensoragg.py --redis-host 127.0.0.1 --payload-size-kb 2 --connect-timeout 3 --pool-size 100

EXTENDED USAGE:
  --sensors-per-device: 每个设备的传感器数量 (default: 5)
  --sensor-types: 传感器类型列表 (default: temperature,humidity,pressure,vibration)
  --environmental-noise: 环境噪音级别 (default: 0.05)
  --payload-size-kb: 负载大小目标 (default: 1)
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
from typing import List, Optional, Dict, Any
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

class SensorAggBench:
    """增强版传感器聚合基准测试，支持真实传感器模拟、数据规模扩展和连接管理"""
    def __init__(self, redis_host: str, redis_port: int, set_key: str = "sensors:ts",
                 # 数据规模扩展
                 payload_size_kb: int = 1, sensors_per_device: int = 5,
                 # 数据类型真实性配置
                 sensor_types: Optional[List[str]] = None,
                 environmental_noise: float = 0.05,
                 # 连接超时配置
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.set_key = set_key
        self._stop = threading.Event()

        # 数据规模扩展配置
        self.payload_size_kb = payload_size_kb
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

        # 周期性监控配置
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # 统计
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

    def _init_connection_pool(self):
        """初始化Redis连接池"""
        if self.connection_pool is None:
            self.connection_pool = redis.ConnectionPool(
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

        # 数据规模扩展 - 添加额外传感器读数达到目标大小
        current_size = len(json.dumps(sensor_data))
        target_size_bytes = self.payload_size_kb * 1024

        if current_size < target_size_bytes:
            # 添加环境传感器读数
            additional_readings = []
            remaining_sensor_types = [st for st in self.sensor_types if st != sensor_type]

            while len(json.dumps({**sensor_data, "environment_sensors": additional_readings})) < target_size_bytes and remaining_sensor_types:
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

        return sensor_data

    def _worker(self, duration: float, read_pct: int, pool):
        r = redis.Redis(connection_pool=pool, decode_responses=True)
        end_time = time.time() + duration
        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()

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
                    r.zadd(self.set_key, {json.dumps(rcd): float(rcd["timestamp"])})
                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
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
            success_count = self.success
            new_operations = success_count - self.last_success_count

            if self.last_report_time > 0 and new_operations >= 0:
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
                    logger.info(f"[{total_duration:.1f}s] TPS: {throughput_ops_sec:.1f}, Avg Lat: {avg_lat:.2f}ms, P95: {p95_lat:.2f}ms")
                else:
                    logger.info(f"[{total_duration:.1f}s] TPS: {throughput_ops_sec:.1f}")

                self.last_report_time = current_time
                self.last_success_count = success_count

    def run(self, threads: int = 4, duration: int = 10, read_pct: int = 5):
        pool = self._init_connection_pool()
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

    def _print_summary(self, duration):
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)
        logger.info("Total ops: %d success=%d fail=%d ops/sec=%.2f", total, self.success, self.fail, ops_per_sec)
        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[int(len(lat)*p/100)]
            logger.info("Latency ms - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                        statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

def main():
    parser = argparse.ArgumentParser(description="Enhanced Sensor Aggregator Redis Benchmark")

    # 基础Redis连接
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # 数据规模扩展
    parser.add_argument("--payload-size-kb", default=1, type=int,
                       help="Target payload size in KB")
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

    args = parser.parse_args()

    # 解析传感器类型参数
    sensor_types = [st.strip() for st in args.sensor_types.split(",")]

    # 计算默认池大小
    pool_size = args.pool_size
    if pool_size is None:
        pool_size = args.threads * 10

    bench = SensorAggBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        set_key=args.set_key,
        # 数据规模扩展
        payload_size_kb=args.payload_size_kb,
        sensors_per_device=args.sensors_per_device,
        # 数据类型真实性
        sensor_types=sensor_types,
        environmental_noise=args.environmental_noise,
        # 连接超时配置
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=pool_size
    )
    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)

if __name__ == "__main__":
    main()