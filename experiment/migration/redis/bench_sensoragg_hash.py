#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg_hash.py - Hash+List Sensor Aggregator Benchmark

全新的传感器聚合基准测试，使用Hash+List数据结构的混合模式
提供与Redis Stream和Sorted Set完全不同的内存访问模式

FEATURES:
   - 数据结构完全重新设计: Hash存储传感器数据 + List维护时间索引
   - 内存访问模式转型: 从排序遍历改为随机键访问 + 哈希字段操作
   - 连接超时配置: 高级连接管理、超时重试
   - 多权限新访问模式: 随机查询、字段更新、复合操作

USAGE:
   python3 bench_sensoragg_hash.py --redis-host 127.0.0.1 --threads 8 --duration 30 --read-pct 10
   python3 bench_sensoragg_hash.py --redis-host 127.0.0.1 --threads 4 --duration 60 --payload-size-kb 2 --connect-timeout 3

EXTENDED USAGE:
   --sensor-hash-prefix: Hash键前缀 (default: sensor:)
   --time-idx-list: 时间索引List键名 (default: sensor_time_idx)
   --payload-size-kb: 负载大小目标 (default: 1)
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

class RateLimiter:
    """Thread-safe token bucket shared across worker threads."""

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


class SensorHashBench:
    """Hash+List传感器聚合基准测试，完全不同的内存访问模式"""
    def __init__(self, redis_host: str, redis_port: int,
                 # 新数据结构配置
                 sensor_hash_prefix: str = "sensor:",
                 time_index_list: str = "sensor_time_idx",
                 # 数据规模扩展
                 payload_size_kb: float = 1.0,
                 # 数据类型真实性配置
                 sensor_types: Optional[List[str]] = None,
                 environmental_noise: float = 0.05,
                 # 连接超时配置
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None,
                 # 消息速率控制
                 max_requests_per_second: Optional[int] = None,
                 # 数据库大小控制
                 ttl: int = 3600):

        self.redis_host = redis_host
        self.redis_port = int(redis_port)

        # 新数据结构配置
        self.sensor_hash_prefix = sensor_hash_prefix
        self.time_index_list = time_index_list

        self._stop = threading.Event()

        # 数据规模扩展配置
        self.payload_size_kb = payload_size_kb

        # 数据类型真实性配置
        self.sensor_types = sensor_types or ["temperature", "humidity", "pressure", "vibration"]
        self.environmental_noise = environmental_noise
        self.sensor_states: Dict[str, Dict[str, Any]] = {}

        # 连接超时配置
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size

        # 统计
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # 速率控制参数
        self.max_requests_per_second = max_requests_per_second
        self._rate_limiter = RateLimiter(max_requests_per_second)

        # 监控配置
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # 数据生命周期管理
        self.ttl = ttl
        self.request_count = 0
        self.clean_interval = 200

    def _init_connection_pool(self):
        """初始化连接池"""
        if not hasattr(self, '_connection_pool'):
            self._connection_pool = redis.ConnectionPool(  # type: ignore[attr-defined]
                host=self.redis_host,
                port=self.redis_port,
                socket_connect_timeout=self.connect_timeout,
                socket_timeout=self.socket_timeout,
                max_connections=self.pool_size or 20
            )
        return self._connection_pool

    def _simulate_sensor_reading(self, sensor_id: str, sensor_type: str) -> float:
        """生成真实的传感器读数"""
        if sensor_id not in self.sensor_states:
            configs = {
                "temperature": {"range": (15, 35), "noise": 1.0},
                "humidity": {"range": (30, 80), "noise": 2.0},
                "pressure": {"range": (980, 1020), "noise": 5.0},
                "vibration": {"range": (0, 50), "noise": 1.0}
            }
            config = configs.get(sensor_type, configs["temperature"])
            self.sensor_states[sensor_id] = {
                "value": config["range"][0] + random.random() * (config["range"][1] - config["range"][0]),
                "noise_factor": config["noise"]
            }

        state = self.sensor_states[sensor_id]
        noise = random.gauss(0, self.environmental_noise * state["noise_factor"])
        return round(state["value"] + noise, 2)

    def _generate_sensor_data(self, timestamp: int) -> Dict[str, Any]:
        """生成传感器数据，适配Hash结构"""
        sensor_id = f"dev-{random.randint(1, 1000)}"
        sensor_type = random.choice(self.sensor_types)

        value = self._simulate_sensor_reading(sensor_id, sensor_type)

        # 为Hash存储格式化数据，扁平化结构以支持HSET/HGET操作
        sensor_data = {
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "timestamp": str(timestamp),
            "value": str(value),
            "unit": {
                "temperature": "celsius", "humidity": "percent",
                "pressure": "hPa", "vibration": "mm/s"
            }.get(sensor_type, "unit"),
            "battery_level": str(round(90 + random.random() * 10, 1)),
            "calibration_status": random.choice(["good", "drift", "needs_cal"]),
            "readings_count": str(random.randint(1000, 10000))
        }

        # 通过添加虚拟字段扩展数据大小
        current_size = len(json.dumps(sensor_data))
        target_size = int(self.payload_size_kb * 1024)

        while current_size < target_size:
            key = f"extra_field_{len(sensor_data)}"
            sensor_data[key] = str(random.random() * 1000)
            current_size = len(json.dumps(sensor_data))

        return sensor_data

    def _rate_control(self):
        """速率控制"""
        self._rate_limiter.acquire()

    def _worker(self, duration: float, read_pct: int, pool):
        r = redis.Redis(connection_pool=pool, decode_responses=True)  # type: ignore[attr-defined]
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()
            self._rate_control()

            try:
                if do_read:
                    # Hash + List读取模式：完全不同的内存访问
                    query_types = ["single_hash_get", "multi_hash_fields", "time_index_sample", "composite_query"]
                    query_type = random.choice(query_types)

                    if query_type == "single_hash_get":
                        # 随机Hash键读取 - 直接键访问
                        hash_key = f"{self.sensor_hash_prefix}{random.randint(1, 1000)}"
                        result = r.hgetall(hash_key)
                    elif query_type == "multi_hash_fields":
                        # 多字段读取 - 哈希字段访问
                        hash_key = f"{self.sensor_hash_prefix}{random.randint(1, 1000)}"
                        fields = ["value", "timestamp", "sensor_type", "battery_level"]
                        result = r.hmget(hash_key, fields)
                    elif query_type == "time_index_sample":
                        # 随机时间索引采样 - List操作
                        list_length = r.llen(self.time_index_list)
                        if list_length > 0:
                            index = random.randint(0, min(9, list_length - 1))
                            result = r.lindex(self.time_index_list, index)
                        else:
                            result = None
                    else:  # composite_query
                        # 复合查询：从List获取键，然后HGETALL
                        list_length = r.llen(self.time_index_list)
                        if list_length > 0:
                            index = random.randint(0, min(9, list_length - 1))
                            hash_key = r.lindex(self.time_index_list, index)
                            result = r.hgetall(hash_key)
                        else:
                            result = None

                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1

                else:
                    # Hash + List写入模式：创建Hash和更新List
                    timestamp = int(time.time() * 1000)
                    sensor_data = self._generate_sensor_data(timestamp)

                    # 创建传感器的Hash
                    hash_key = f"{self.sensor_hash_prefix}{sensor_data['sensor_id']}_{timestamp}"
                    r.hset(hash_key, sensor_data)

                    # 添加到时间索引List (保持最近1000个条目)
                    r.lpush(self.time_index_list, hash_key)
                    r.ltrim(self.time_index_list, 0, 999)

                    # 设置TTL防止内存无限增长
                    r.expire(hash_key, self.ttl)
                    r.expire(self.time_index_list, self.ttl)

                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
                        self.request_count += 1

                    # 定期清理
                    if self.request_count % self.clean_interval == 0:
                        # 清理过期哈希键
                        hashes = r.lrange(self.time_index_list, 0, 99)
                        for hkey in hashes:
                            if not r.exists(hkey):
                                r.lrem(self.time_index_list, 0, hkey)

            except Exception as e:
                logger.debug("Hash operation failed: %s", e)
                with self.lock:
                    self.fail += 1
                time.sleep(0.01)

            self._periodic_monitoring(duration)

    def _periodic_monitoring(self, total_duration: float):
        """定期监控"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.monitor_interval:
            elapsed = current_time - self.start_time if hasattr(self, 'start_time') else 0
            success_count = self.success

            new_operations = success_count - self.last_success_count
            if new_operations >= 0:
                throughput = new_operations / (current_time - self.last_report_time)

                with self.lock:
                    recent_latencies = self.latencies_ms[-1000:] if self.latencies_ms else []

                if recent_latencies:
                    avg_lat = statistics.mean(recent_latencies)
                    p95_lat = sorted(recent_latencies)[int(len(recent_latencies) * 0.95)]
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput:.1f}, Avg Lat: {avg_lat:.2f}ms, P95: {p95_lat:.2f}ms")
                else:
                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput:.1f}")

            self.last_report_time = current_time
            self.last_success_count = success_count

    def run(self, threads: int = 4, duration: int = 10, read_pct: int = 5):
        pool = self._init_connection_pool()
        self.start_time = time.time()
        self.last_report_time = self.start_time
        self.last_success_count = 0

        threads_list = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct, pool), daemon=True)
            t.start()
            threads_list.append(t)

        logger.info("Hash版传感器聚合启动: %d个线程，持续%d秒，读取比例%d%%",
                   threads, duration, read_pct)
        logger.info("数据结构: Hash前缀=%s, 时间索引列表=%s",
                   self.sensor_hash_prefix, self.time_index_list)

        for t in threads_list:
            t.join()

        self._print_summary(duration)

    def _print_summary(self, duration: int):
        """打印总结信息"""
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)
        logger.info("总操作数: %d 成功=%d 失败=%d ops/sec=%.2f",
                   total, self.success, self.fail, ops_per_sec)

        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            p50 = lat[int(len(lat) * 0.5)]
            p90 = lat[int(len(lat) * 0.9)]
            p99 = lat[int(len(lat) * 0.99)]
            logger.info("延迟(ms) - 平均=%.3f p50=%.3f p90=%.3f p99=%.3f 最大=%.3f",
                       statistics.mean(lat), p50, p90, p99, lat[-1])


def main():
    parser = argparse.ArgumentParser(description="Hash+List Sensor Aggregator Redis Benchmark")

    # Redis连接
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis主机")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis端口")

    # 数据结构配置
    parser.add_argument("--sensor-hash-prefix", default="sensor:", help="传感器Hash键前缀")
    parser.add_argument("--time-idx-list", default="sensor_time_idx", help="时间索引List键名")

    # 数据规模扩展
    parser.add_argument("--payload-size-kb", default=1.0, type=float, help="目标负载大小(KB)")

    # 数据类型真实性
    parser.add_argument("--sensor-types", default="temperature,humidity,pressure,vibration",
                       help="传感器类型列表")

    # 连接配置
    parser.add_argument("--connect-timeout", default=5, type=int, help="连接超时(秒)")
    parser.add_argument("--socket-timeout", default=5, type=int, help="socket超时(秒)")
    parser.add_argument("--pool-timeout", default=10, type=int, help="连接池超时(秒)")
    parser.add_argument("--pool-size", type=int, help="连接池大小(默认: 线程数*10)")

    # 负载参数
    parser.add_argument("--threads", default=4, type=int, help="工作线程数")
    parser.add_argument("--duration", default=10, type=int, help="基准测试持续时间(秒)")
    parser.add_argument("--read-pct", default=10, type=int, help="读取操作比例(%)")

    # 消息速率控制
    parser.add_argument("--rps", type=int, help="每秒最大请求数")

    # 生命周期管理
    parser.add_argument("--ttl", type=int, default=3600, help="TTL(秒)")

    args = parser.parse_args()

    # 参数解析
    sensor_types = [st.strip() for st in args.sensor_types.split(",")]
    pool_size = args.pool_size or (args.threads * 10)

    bench = SensorHashBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        sensor_hash_prefix=args.sensor_hash_prefix,
        time_index_list=args.time_idx_list,
        payload_size_kb=args.payload_size_kb,
        sensor_types=sensor_types,
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=pool_size,
        max_requests_per_second=args.rps,
        ttl=args.ttl
    )

    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)


if __name__ == "__main__":
    main()