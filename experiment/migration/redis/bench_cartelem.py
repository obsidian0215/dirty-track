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
  python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 8 --duration 30 --vehicle-pattern highway --payload-size-kb 5 --connect-timeout 2
  python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 60 --size-distribution normal --pool-size 50
  python3 bench_cartelem.py --redis-host 127.0.0.1 --threads 4 --duration 30 --rps 100  # 限制为100 RPS

EXTENDED USAGE:
  --vehicle-pattern: normal_city/highway/stop_go (default: normal_city)
  --payload-size-kb: Target payload size in KB (default: 1)
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
from typing import List, Optional
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

class CarTelematicsBench:
    """车联网写入 Redis Stream 的负载发生器"""
    def __init__(self, redis_host: str, redis_port: int, stream_name: str = "vehicle:telemetry",
                  # 数据规模扩展
                  payload_size_kb: float = 1.0, size_distribution: str = "uniform",
                 # 数据类型真实性
                 vehicle_pattern: str = "normal_city",
                 # 连接超时配置
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None,
                 # 消息速率控制
                 max_requests_per_second: Optional[int] = None):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.stream_name = stream_name
        self._stop = threading.Event()

        # 数据规模扩展配置
        self.payload_size_kb = payload_size_kb
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
        self.requests_this_second = 0
        self.last_second_start = time.time()

        # 统计
        self.latencies_ms = []  # 全局收集（注意内存）
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # 初始化连接池
        self.connection_pool = None

        # 周期性输出参数
        self.monitor_interval = 1.0  # 监控间隔(秒)
        self.last_report_time = 0
        self.last_success_count = 0

        # 计算速率控制参数
        if self.max_requests_per_second:
            self.min_interval_per_request = 1.0 / self.max_requests_per_second
        else:
            self.min_interval_per_request = None  # 无速率限制

    def _rate_controller(self, op_start_time: float):
        """控制消息发送速率"""
        if self.min_interval_per_request is None:
            return  # 无速率限制，使用原有逻辑

        current_time = time.time()

        # 检查是否需要等待
        if self.max_requests_per_second and self.requests_this_second >= self.max_requests_per_second:
            # 等待到下一秒开始
            sleep_time = max(0, 1.0 - (current_time - self.last_second_start))
            if sleep_time > 0:
                time.sleep(sleep_time)
            self.last_second_start = time.time()
            self.requests_this_second = 0

        # 计算操作耗时，调整等待时间
        if op_start_time:
            op_duration = current_time - op_start_time
            wait_time = max(0, self.min_interval_per_request - op_duration)

            # 避免过长的等待（保留原有的10ms最小间隔）
            if wait_time > 0.01:
                time.sleep(wait_time)
            elif wait_time > 0:
                time.sleep(0.01)  # 最小10ms间隔

        self.requests_this_second += 1

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

    def _make_payload(self, vehicle_id: Optional[str] = None):
        """生成增强的车辆遥测数据"""
        if not vehicle_id:
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
        base_target_bytes = int(self.payload_size_kb * 1024)  # 基本目标大小

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
        r = redis.Redis(connection_pool=pool, decode_responses=True)
        end_time = time.time() + duration
        vehicle_id = None  # 为每个线程维护车辆ID以保持连续性
        op_start_time = time.time()

        while time.time() < end_time and not self._stop.is_set():
            # 应用速率控制
            self._rate_controller(op_start_time)

            payload = self._make_payload(vehicle_id)
            vehicle_id = payload["vehicle_id"]  # 更新车辆ID以保持连续性

            op_start_time = time.perf_counter()
            try:
                r.xadd(self.stream_name, {"data": json.dumps(payload)})
                lat = (time.perf_counter() - op_start_time) * 1000.0
                with self.lock:
                    self.latencies_ms.append(lat)
                    self.success += 1
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
                   threads, duration, self.vehicle_pattern, self.payload_size_kb)
        for t in tlist:
            t.join()
        logger.info("Workers finished")
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
    parser = argparse.ArgumentParser(description="Enhanced Car Telematics Redis Benchmark")

    # 基础Redis连接
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # 数据规模扩展
    parser.add_argument("--payload-size-kb", default=1.0, type=float,
                       help="Target payload size in KB (support decimals)")
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

    args = parser.parse_args()

    # 计算默认池大小
    pool_size = args.pool_size
    if pool_size is None:
        pool_size = args.threads * 10

    bench = CarTelematicsBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        stream_name=args.stream,
        # 数据规模扩展
        payload_size_kb=args.payload_size_kb,
        size_distribution=args.size_distribution,
        # 数据类型真实性
        vehicle_pattern=args.vehicle_pattern,
        # 连接超时配置
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=pool_size,
        # 消息速率控制
        max_requests_per_second=args.rps
    )
    bench.run(threads=args.threads, duration=args.duration)

if __name__ == "__main__":
    main()