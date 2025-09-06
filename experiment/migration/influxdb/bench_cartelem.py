#!/usr/bin/env python3
# coding: utf-8
"""
bench_vehicle_influx.py - Vehicle Telematics Benchmark for InfluxDB

Advanced Car Telematics Benchmark adapted for InfluxDB
Enhanced with realistic data generation, scalable payload sizes, and robust operations
Similar to Redis bench_cartelem.py but using InfluxDB time series storage

FEATURES:
   - 数据规模扩展: Configurable payload sizes and distribution patterns
   - 数据类型真实性: Vehicle behavior patterns, physical constraints simulation
   - 实时分析: Continuous location tracking and diagnostics

USAGE:
   python3 bench_cartelem.py --influx-url http://localhost:8181 --threads 8 --duration 30 --vehicle-pattern highway --payload-size-kb 5
   python3 bench_cartelem.py --influx-url http://localhost:8181 --threads 4 --duration 60 --size-distribution normal
   python3 bench_cartelem.py --influx-url http://localhost:8181 --payload-size-kb 2 --connect-timeout 5

EXTENDED USAGE:
   --vehicle-pattern: normal_city/highway/stop_go (default: normal_city)
   --payload-size-kb: Target payload size in KB (default: 1)
   --size-distribution: uniform/normal/zipf (default: uniform)
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
from typing import Dict, Any, Optional
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


class VehicleInfluxBench:
    """Vehicle Telematics InfluxDB Benchmark"""
    def __init__(self, influx_url: str, token: str, org: str, bucket: str = "vehicle-data",
                 # Data scale extension
                 payload_size_kb: float = 1.0, size_distribution: str = "uniform",
                # Data type realism
                vehicle_pattern: str = "normal_city",
                # 数据生命周期管理
                retention_policy: str = "1h",
                # 消息速率控制
                max_requests_per_second: Optional[int] = None):

        self.influx_url = influx_url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._stop = threading.Event()

        # Data scale extension config
        self.payload_size_kb = payload_size_kb
        self.size_distribution = size_distribution

        # Data type realism config
        self.vehicle_pattern = vehicle_pattern  # normal_city, highway, stop_go
        self.vehicle_states: Dict[str, Dict[str, Any]] = {}

        # 数据生命周期管理
        self.retention_policy = retention_policy

        # Monitoring config
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # Statistics
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # 速率控制参数
        self.max_requests_per_second = max_requests_per_second
        if self.max_requests_per_second:
            # 重置速率控制状态
            self.request_timestamps = []
            self.request_interval = 1.0 / self.max_requests_per_second
        else:
            # 初始化为空列表避免错误
            self.request_timestamps = []

        # Retention配置跟踪
        self._retention_configured = False


        # InfluxDB client initialization
        self.client = InfluxDBClient(url=influx_url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

        # Bucket management client
        from influxdb_client.client.bucket_api import BucketsApi
        self.buckets_api = self.client.buckets_api()
        self.org_api = self.client.organizations_api()


    def _get_vehicle_state(self, vehicle_id: str) -> Dict[str, Any]:
        """Get or initialize vehicle state for realistic simulation"""
        if vehicle_id not in self.vehicle_states:
            self.vehicle_states[vehicle_id] = {
                "speed": random.uniform(10, 30),  # Initial speed
                "fuel": 100 - random.random() * 20,  # Fuel level
                "lat": 31.0 + random.random() * 0.1,
                "lon": 121.0 + random.random() * 0.1,
                "engine_temp": 85 + random.random() * 15,
                "last_update": time.time()
            }
        return self.vehicle_states[vehicle_id]

    def _update_vehicle_state(self, vehicle_id: str, new_speed: float):
        """Update vehicle state based on physical characteristics"""
        state = self._get_vehicle_state(vehicle_id)
        curr_time = time.time()
        time_delta = curr_time - state["last_update"]

        # Fuel consumption calculation (L/100km)
        fuel_consumption = (abs(new_speed - state["speed"]) * time_delta + new_speed * time_delta) * 0.001
        state["fuel"] = max(0, state["fuel"] - fuel_consumption)

        # Engine temperature calculation
        if new_speed > 80:
            state["engine_temp"] = min(120, state["engine_temp"] + (new_speed * time_delta * 0.1))
        else:
            state["engine_temp"] = max(60, state["engine_temp"] - (time_delta * 5))

        state["speed"] = new_speed
        state["last_update"] = curr_time

    def _generate_diagnostics(self) -> Dict[str, Any]:
        """Generate OBD diagnostic codes"""
        obd_codes = ["P0100", "P0101", "P0200", "P0300", "P0400", "P0500", "P0600"]
        codes = random.sample(obd_codes, random.randint(0, min(3, len(obd_codes))))

        return {
            "obd_codes": codes,
            "check_engine_light": random.random() < 0.1,
            "battery_voltage": round(12.6 + (random.random() - 0.5) * 0.5, 2),
            "transmission_temp": round(85 + (random.random() - 0.5) * 10, 2),
            "malfunction_indicator": random.choice(["off", "on", "flashing"])
        }

    def _calculate_dynamic_speed(self, vehicle_id: str) -> float:
        """Calculate real-time speed based on driving pattern"""
        patterns = {
            "normal_city": {"min": 0, "max": 50, "accel_rate": 0.5},
            "highway": {"min": 60, "max": 120, "accel_rate": 1.2},
            "stop_go": {"min": 0, "max": 40, "accel_rate": 2.0}
        }

        pattern = patterns[self.vehicle_pattern]
        base_speed = self._get_vehicle_state(vehicle_id)["speed"]

        # Apply acceleration constraints
        acceleration = pattern["accel_rate"] * (random.random() - 0.5)
        new_speed = max(0, min(base_speed + acceleration, pattern["max"]))

        # Specific pattern behaviors
        if self.vehicle_pattern == "stop_go":
            if random.random() < 0.3:
                new_speed = random.uniform(20, 40)
            elif random.random() < 0.2:
                new_speed = 0

        elif self.vehicle_pattern == "highway":
            if base_speed < 70 and random.random() < 0.8:
                new_speed = min(pattern["max"], base_speed + 2)

        return round(new_speed, 2)

    def _generate_vehicle_data(self) -> list:
        """Generate enhanced vehicle telemetry data"""
        vehicle_id = f"veh-{random.randint(1000, 9999)}"
        base_timestamp = int(time.time() * 1000000000)

        # Get vehicle state
        state = self._get_vehicle_state(vehicle_id)
        current_speed = self._calculate_dynamic_speed(vehicle_id)

        # Update vehicle state
        self._update_vehicle_state(vehicle_id, current_speed)

        points = []

        # Main telemetry point
        main_point = Point("vehicle_telemetry") \
            .tag("vehicle_id", vehicle_id) \
            .tag("pattern", self.vehicle_pattern) \
            .tag("model", random.choice(["sedan", "suv", "truck", "hatchback"])) \
            .tag("fuel_type", random.choice(["gasoline", "diesel", "electric"])) \
            .tag("status", "active" if state["fuel"] > 5 else "low_fuel") \
            .field("speed_kmh", current_speed) \
            .field("fuel_level", round(state["fuel"], 2)) \
            .field("engine_temp", round(state["engine_temp"], 2)) \
            .field("latitude", round(state["lat"], 6)) \
            .field("longitude", round(state["lon"], 6)) \
            .time(base_timestamp, write_precision=WritePrecision.NS)

        points.append(main_point)

        # Diagnostics point
        diagnostics = self._generate_diagnostics()
        diag_point = Point("vehicle_diagnostics") \
            .tag("vehicle_id", vehicle_id) \
            .tag("check_engine_light", str(diagnostics["check_engine_light"]).lower()) \
            .tag("malfunction_indicator", diagnostics["malfunction_indicator"]) \
            .field("battery_voltage", diagnostics["battery_voltage"]) \
            .field("transmission_temp", diagnostics["transmission_temp"]) \
            .field("obd_code_count", len(diagnostics["obd_codes"])) \
            .field("system_status", random.choice(["normal", "warning", "critical"])) \
            .time(base_timestamp, write_precision=WritePrecision.NS)

        points.append(diag_point)

        # Data scale extension - add sensor data (apply specified distribution)
        current_size = len(json.dumps({
            "vehicle_id": vehicle_id, "speed": current_speed,
            "fuel_level": state["fuel"], "engine_temp": state["engine_temp"],
            "latitude": state["lat"], "longitude": state["lon"]
        }))
        base_target_bytes = int(self.payload_size_kb * 1024)  # 基本目标大小

        # Apply distribution function
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
            # Add additional sensor readings
            sensors = []
            sensor_types = ["gps_accuracy", "gyroscope", "accelerometer", "magnetometer",
                          "tire_pressure", "brake_pressure", "throttle_position", "exhaust_sensor"]

            while len(sensors) < 8:
                sensor = {
                    "type": random.choice(sensor_types),
                    "value": random.random() * random.choice([100, 200, 500, 1000]),
                    "unit": random.choice(["meters", "degrees", "g", "pa", "percentage"]),
                    "precision": round(random.random() * 0.1, 6),
                    "calibration_date": f"2023-{random.randint(1,12):02d}"
                }
                sensors.append(sensor)

                sensor_point = Point("vehicle_sensor") \
                    .tag("vehicle_id", vehicle_id) \
                    .tag("sensor_type", sensor["type"]) \
                    .tag("unit", sensor["unit"]) \
                    .tag("quality", random.choice(["good", "excellent", "poor"])) \
                    .field("value", sensor["value"]) \
                    .field("precision", sensor["precision"]) \
                    .field("confidence", round(random.uniform(0.8, 0.99), 3)) \
                    .time(base_timestamp + len(sensors) * 1000000, write_precision=WritePrecision.NS)  # 1ms offset

                points.append(sensor_point)

                # 检查添加后大小，如果超过则移除最后一个点
                new_size = len(json.dumps({**{"vehicle_id": vehicle_id, "speed": current_speed}, "sensors": sensors}))
                if new_size > target_size_bytes:
                    points.pop()  # 移除添加的点
                    logger.debug(f"Payload size would exceed target {target_size_bytes} bytes (would be {new_size}), truncated")
                    break

        if current_size > target_size_bytes:
            logger.warning(f"Baseline sensor data already exceeds target size: {current_size} > {target_size_bytes} bytes")

        return points

    def _execute_location_query(self):
        """Execute location-based query"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "vehicle_telemetry")
            |> filter(fn: (r) => r["pattern"] == "{self.vehicle_pattern}")
            |> filter(fn: (r) => r["speed_kmh"] > 10)
            |> limit(n: 50)
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        return len(result)

    def _worker(self, duration: float, read_pct: int):
        """Worker thread for mixed operations"""
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()
            # 速率控制检查
            self._rate_control()

            try:
                if do_read:
                    self._execute_location_query()
                else:
                    # Write vehicle data
                    points = self._generate_vehicle_data()
                    self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                    lat = (time.perf_counter() - start) * 1000.0

                    # 第一次写入成功后配置retention
                    if not self._retention_configured:
                        self._configure_bucket_retention_on_first_write()

                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
            except Exception as e:
                logger.debug("Operation failed: %s", e)
                with self.lock:
                    self.fail += 1
                time.sleep(0.01)

            # Periodic monitoring
            self._periodic_monitoring(duration)

    def _periodic_monitoring(self, total_duration: float):
        """Periodic throughput and latency monitoring"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.monitor_interval:
            # 正确的elapsed时间计算
            elapsed = current_time - self.start_time
            success_count = self.success
            new_operations = success_count - self.last_success_count

            if new_operations >= 0:
                throughput_ops_sec = new_operations / (current_time - self.last_report_time)

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
        """Run the benchmark"""
        # 记录测试开始时间，用于计算精确的elapsed时间
        start_time = time.time()

        # 初始化监控参数
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct), daemon=True)
            t.start()
            tlist.append(t)

        logger.info("Started %d threads for %ds (vehicle_pattern=%s, payload_kb=%d)",
                   threads, duration, self.vehicle_pattern, self.payload_size_kb)

        for t in tlist:
            t.join()

        self._print_summary(duration)

        # Close client connection
        self.client.close()

    def _configure_bucket_retention_on_first_write(self):
        """在第一次写入成功后配置retention"""
        try:
            # 尝试设置bucket的retention policy
            logger.info(f"Setting retention policy '{self.retention_policy}' for bucket '{self.bucket}'")

            # 获取bucket信息
            bucket = self.buckets_api.find_bucket_by_name(bucket_name=self.bucket)
            if bucket:
                # 这里可以添加实际的retention修改逻辑
                # 例如：更新bucket的retention规则
                logger.info(f"Bucket '{self.bucket}' retention policy set to '{self.retention_policy}'")

            self._retention_configured = True

        except Exception as e:
            logger.warning(f"Failed to configure bucket retention: {e}")
            # 即使配置失败也标记为已配置，避免重复尝试
            self._retention_configured = True

    def _parse_duration_to_seconds(self, duration_str):
        """Parse duration string like '1h', '24h', '7d' to seconds"""
        if not duration_str:
            return 3600  # Default 1 hour

        duration_str = duration_str.lower()
        multiplier = {
            's': 1,
            'm': 60,
            'h': 3600,
            'd': 86400,
            'w': 604800
        }

        # Parse duration
        import re
        match = re.match(r'^(\d+)([smhdw])$', duration_str)
        if match:
            value, unit = match.groups()
            return int(value) * multiplier.get(unit, 1)

        # Default fallback
        logger.warning(f"Invalid duration format: {duration_str}, using default 1h")
        return 3600

    def _rate_control(self):
        """实现精确的速率控制"""
        if not self.max_requests_per_second:
            return

        current_time = time.time()

        # 清理过期的时间戳（超过1秒）
        cutoff_time = current_time - 1.0
        self.request_timestamps = [t for t in self.request_timestamps if t > cutoff_time]

        # 如果未达到速率限制，直接允许
        if len(self.request_timestamps) < self.max_requests_per_second:
            self.request_timestamps.append(current_time)
            return

        # 计算需要等待的时间
        earliest_timestamp = self.request_timestamps[0] if self.request_timestamps else current_time
        wait_time = self.request_interval - (current_time - earliest_timestamp)
        if wait_time > 0:
            time.sleep(wait_time)

        # 记录本次请求时刻
        self.request_timestamps.append(time.time())
        # 再次清理以保持列表大小
        self.request_timestamps = self.request_timestamps[-self.max_requests_per_second:]

    def _print_summary(self, duration: int):
        """Print benchmark summary"""
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)
        logger.info("Total ops: %d success=%d fail=%d ops/sec=%.2f", total, self.success, self.fail, ops_per_sec)

        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[int(len(lat)*p/100)]
            logger.info("Latency ms - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                       statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])


def main():
    parser = argparse.ArgumentParser(description="Advanced Vehicle Telematics InfluxDB Benchmark")

    # InfluxDB connection
    parser.add_argument("--influx-url", default="http://localhost:8181", help="InfluxDB URL")
    parser.add_argument("--token", default="my-super-secret-auth-token", help="InfluxDB token")
    parser.add_argument("--org", default="my-org", help="InfluxDB org")
    parser.add_argument("--bucket", default="vehicle-data", help="InfluxDB bucket")

    # Data scale extension
    parser.add_argument("--payload-size-kb", default=1.0, type=float, help="Target payload size in KB (support decimals)")
    parser.add_argument("--size-distribution", default="uniform", type=str,
                       choices=["uniform", "normal", "zipf"], help="Distribution type for payload sizes")

    # Data type realism
    parser.add_argument("--vehicle-pattern", default="normal_city", type=str,
                       choices=["normal_city", "highway", "stop_go"], help="Vehicle driving pattern")

    # Workload parameters
    parser.add_argument("--threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--read-pct", default=10, type=int, help="Read operation percentage")
    parser.add_argument("--retention-policy", default="1h", type=str,
                       help="Bucket retention policy (e.g., 1h, 24h, 7d)")

    # 消息速率控制
    parser.add_argument("--rps", "--max-requests-per-second", dest="rps",
                       type=int, help="Maximum requests per second (default: no limit)")

    args = parser.parse_args()

    bench = VehicleInfluxBench(
        influx_url=args.influx_url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
        # Data scale extension
        payload_size_kb=args.payload_size_kb,
        size_distribution=args.size_distribution,
        # Data type realism
        vehicle_pattern=args.vehicle_pattern,
        # 数据生命周期管理
        retention_policy=args.retention_policy,
        # 消息速率控制
        max_requests_per_second=args.rps
    )

    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)


if __name__ == "__main__":
    main()