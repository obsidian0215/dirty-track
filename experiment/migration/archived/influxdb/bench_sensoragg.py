#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg.py - Sensor Aggregation Benchmark for InfluxDB

Enhanced Sensor Aggregator Benchmark with realistic sensors and scalable data
Supports multiple sensor types, data scale extension, and robust connection management
Similar to Redis bench_sensoragg.py but adapted for InfluxDB time series

FEATURES:
   - 数据规模扩展: Configurable payload sizes and sensors per device
   - 数据类型真实性: Multi-sensor types (temp/humidity/pressure/vibration) with drift simulation
   - 连接池管理: Connection pooling and robust error handling

USAGE:
    python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token --org my-org --bucket sensor-data --threads 8 --duration 30 --read-pct 10
    python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token --org my-org --bucket sensor-data --threads 4 --duration 60 --sensors-per-device 10 --sensor-types temperature,humidity,pressure
    python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token --org my-org --bucket sensor-data --payload-size 2KB
    python3 bench_sensoragg.py --influx-url http://localhost:8181 --token my-token --org my-org --bucket sensor-data --threads 4 --duration 30 --rps 100

EXTENDED USAGE:
   --sensors-per-device: Number of sensors per device (default: 5)
   --sensor-types: Comma-separated sensor types (default: temperature,humidity,pressure,vibration)
   --environmental-noise: Environmental noise level (default: 0.05)
    --payload-size: Target payload size with units (default: 1KB). Examples: 256B, 16KB, 1MB
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
import math
import os
from typing import List, Optional, Dict, Any
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi
import re

# Dynamic bench_common import (searches up the tree for common/bench_common.py)
try:
    import importlib.util as _importlib_util, os as _os
    _cur = _os.path.abspath(_os.path.dirname(__file__))
    _bench_common = None
    for _ in range(6):
        _candidate = _os.path.join(_cur, 'common', 'bench_common.py')
        if _os.path.exists(_candidate):
            spec = _importlib_util.spec_from_file_location('bench_common', _candidate)
            _bench_common = _importlib_util.module_from_spec(spec)
            spec.loader.exec_module(_bench_common)
            break
        _cur = _os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


class RateLimiter:
    """Thread-safe token bucket to enforce RPS in multi-threaded writers."""

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


class SensorInfluxBench:
    """Enhanced Sensor Aggregator InfluxDB Benchmark"""
    def __init__(self, influx_url: str, token: str, org: str, bucket: str = "sensor-data",
                 # Data scale extension
                 payload_size_bytes: int = 1024, sensors_per_device: int = 5,
                 # Data type realism
                 sensor_types: Optional[List[str]] = None,
                 environmental_noise: float = 0.05,
                 # 数据生命周期管理
                 retention_policy: str = "1h",
                 # 消息速率控制
                 max_requests_per_second: Optional[int] = None,
                 # Realism extensions
                 device_count: int = 0,
                 pacing: bool = False,
                 payload_mixture: bool = False,
                 report_realism: bool = False):

        self.influx_url = influx_url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._stop = threading.Event()

        # Realism config
        self.device_count = device_count
        self.pacing = pacing
        self.payload_mixture = payload_mixture
        self.report_realism = report_realism
        self.device_counter = {}  # Track device usage for report
        self.interarrivals = []   # Track inter-arrival times for report
        self.payload_sizes_tracked = [] # Track payload sizes for report
        self.last_gen_time = None
        self._realism_profile = None # Will be set externally if needed

        # Data scale extension config (bytes)
        self.payload_size_bytes = payload_size_bytes
        self.sensors_per_device = sensors_per_device

        # Data type realism config
        self.sensor_types = sensor_types or ["temperature", "humidity", "pressure", "vibration"]
        self.environmental_noise = environmental_noise
        self.sensor_states: Dict[str, Dict[str, Any]] = {}

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

        # payload sizing and mode
        self.payload_sizes: List[int] = []
        self.payload_mode = "json"

        # 速率控制参数
        self.max_requests_per_second = max_requests_per_second
        self._rate_limiter = RateLimiter(max_requests_per_second)

        # Retention配置跟踪
        self._retention_configured = False


        # Initialize InfluxDB client
        # InfluxDB client initialization
        self.client = InfluxDBClient(url=influx_url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

        # Bucket management client
        from influxdb_client.client.bucket_api import BucketsApi
        self.buckets_api = self.client.buckets_api()
        self.org_api = self.client.organizations_api()


    def _get_sensor_state(self, sensor_id: str, sensor_type: str) -> Dict[str, Any]:
        """Get or initialize sensor state for realism simulation"""
        state_key = f"{sensor_type}_{sensor_id}"
        if state_key not in self.sensor_states:
            # Base configs for different sensor types
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
        """Generate realistic sensor readings"""
        state = self._get_sensor_state(sensor_id, sensor_type)
        current_time = time.time()
        time_delta = current_time - state["last_reading_time"]

        # Update sensor internal state
        state["readings_count"] += 1

        # Sensor drift simulation
        drift_change = (random.gauss(0, 0.01) * time_delta)
        state["drift"] += drift_change

        # Calibration drift
        state["calibration_drift"] += (random.gauss(0, 0.001) * time_delta)

        # Battery level degradation
        state["battery_level"] = max(5, state["battery_level"] - time_delta * 0.001)

        # Update calibration status occasionally
        if state["readings_count"] % 1000 == 0:
            state["calibration_status"] = random.choice(["drift", "needs_cal"])

        # Generate base reading
        base_reading = state["value"] + state["drift"] + state["calibration_drift"]

        # Add noise
        noise_factor = self.environmental_noise
        if sensor_type in ["vibration"]:
            noise_factor *= 2.0

        noise = random.gauss(0, noise_factor * abs(base_reading) * 0.05)
        final_reading = base_reading + noise

        # Apply physical constraints
        config = {
            "temperature": {"range": (-40, 85)},
            "humidity": {"range": (0, 100)},
            "pressure": {"range": (500, 1100)},
            "vibration": {"range": (0, 1000)}
        }.get(sensor_type, {"range": (0, 100)})

        final_reading = max(config["range"][0], min(config["range"][1], final_reading))
        state["last_reading_time"] = current_time

        return round(final_reading, 3)

    def sample_interarrival(self) -> float:
        """Sample inter-arrival time (seconds). If pacing is on, use Poisson process."""
        if not self.pacing:
            return 0.0

        # If using external profile
        if self._realism_profile:
            return self._realism_profile.next_interarrival()

        # Default internal Poisson logic if no profile but pacing=True
        target_rate = self._rate_limiter.rate if self._rate_limiter.rate else 100.0
        # Poisson inter-arrival: -ln(U) / lambda
        return -math.log(1.0 - random.random()) / target_rate

    def sample_payload_size_from_mixture(self) -> int:
        """Sample payload size from a mixture model if enabled."""
        if self._realism_profile:
            return self._realism_profile.sample_payload_size()

        if not self.payload_mixture:
            return self.payload_size_bytes

        # Simple internal mixture if no profile: 80% small (base), 20% large (5x)
        if random.random() < 0.8:
            return self.payload_size_bytes
        else:
            return self.payload_size_bytes * 5

    def _generate_sensor_data(self, device_id: Optional[str] = None, target_size_override: Optional[int] = None) -> List[Point]:
        """Generate enhanced sensor data points"""
        points = []
        if not device_id:
            device_id = f"dev-{random.randint(1, self.sensors_per_device * 10)}"
        base_timestamp = int(time.time() * 1000000000)  # nanoseconds

        # Primary sensor readings
        primary_sensor_type = random.choice(self.sensor_types)
        primary_sensor_id = f"{device_id}_{primary_sensor_type}"

        value = self._simulate_sensor_reading(primary_sensor_id, primary_sensor_type)
        state = self._get_sensor_state(primary_sensor_id, primary_sensor_type)

        # Create primary point
        point = Point("sensor_reading") \
            .tag("sensor_id", primary_sensor_id) \
            .tag("sensor_type", primary_sensor_type) \
            .tag("device_id", device_id) \
            .tag("unit", {
                "temperature": "celsius",
                "humidity": "percent",
                "pressure": "hPa",
                "vibration": "mm/s"
            }.get(primary_sensor_type, "unit")) \
            .tag("status", "active") \
            .tag("calibration_status", state["calibration_status"]) \
            .field("value", value) \
            .field("battery_level", round(state["battery_level"], 1)) \
            .field("readings_count", state["readings_count"]) \
            .time(base_timestamp, write_precision=WritePrecision.NS)

        points.append(point)

        # Data scale extension - add environmental sensors to reach target size (adds randomness: 80%-120%)
        current_size = len(json.dumps({
            "sensor_id": primary_sensor_id, "value": value,
            "battery_level": state["battery_level"], "readings_count": state["readings_count"]
        }))
        target_size_bytes = int(target_size_override) if target_size_override else int(self.payload_size_bytes)  # 已经是字节
        # Apply size distribution to vary target payload: support uniform/normal/zipf similar to other benches
        try:
            sd = getattr(self, "size_distribution", "uniform") or "uniform"
        except Exception:
            sd = "uniform"
        if sd == "uniform":
            random_multiplier = random.uniform(0.8, 1.2)
        elif sd == "normal":
            # gaussian around 1.0, clamp to [0.7,1.3]
            val = random.gauss(1.0, 0.1)
            random_multiplier = max(0.7, min(1.3, val))
        elif sd == "zipf":
            # zipf-like skew towards smaller sizes
            random_multiplier = random.betavariate(2, 5) * 0.8 + 0.6
        else:
            random_multiplier = random.uniform(0.8, 1.2)

        random_stop_bytes = int(target_size_bytes * random_multiplier)

        remaining_types = [st for st in self.sensor_types if st != primary_sensor_type]
        additional_points = 0

        while current_size < target_size_bytes and remaining_types and additional_points < 10:
            env_type = random.choice(remaining_types if remaining_types else self.sensor_types)
            env_sensor_id = f"{device_id}_env_{additional_points}_{env_type}"
            env_value = self._simulate_sensor_reading(env_sensor_id, env_type)
            env_state = self._get_sensor_state(env_sensor_id, env_type)

            env_point = Point("environment_sensor") \
                .tag("primary_sensor_id", primary_sensor_id) \
                .tag("sensor_id", env_sensor_id) \
                .tag("sensor_type", env_type) \
                .tag("device_id", device_id) \
                .tag("unit", {
                    "temperature": "celsius",
                    "humidity": "percent",
                    "pressure": "hPa",
                    "vibration": "mm/s"
                }.get(env_type, "unit")) \
                .tag("quality", random.choice(["good", "excellent", "poor"])) \
                .tag("calibration_date", f"2023-{random.randint(1,12):02d}") \
                .field("value", env_value) \
                .field("battery_level", round(env_state["battery_level"], 1)) \
                .field("confidence", round(random.uniform(0.8, 0.99), 3)) \
                .field("firmware_version", f"{random.randint(1,3)}.{random.randint(0,9)}") \
                .time(base_timestamp + additional_points, write_precision=WritePrecision.NS)

            points.append(env_point)

            # 检查添加后大小，如果超过则移除最后一个点
            new_size = len(json.dumps({
                **{"sensor_id": primary_sensor_id, "value": value},
                "additional_sensors": additional_points + 1
            }))
            if new_size > target_size_bytes:
                points.pop()  # 移除添加的点
                logger.debug(f"Payload size would exceed target {target_size_bytes} bytes (would be {new_size}), truncated")
                break

            current_size = new_size
            additional_points += 1

        if current_size > target_size_bytes:
            logger.warning(f"Baseline sensor data already exceeds target size: {current_size} > {target_size_bytes} bytes")

        # record an approximate payload estimate for the last generated points
        try:
            est = len(json.dumps({"points": len(points)}))
            self._last_payload_estimate = int(est)
        except Exception:
            self._last_payload_estimate = 0

        return points

    def _execute_range_query(self) -> List[Point]:
        """Execute range query similar to Redis ZRANGEBYSCORE"""
        now = int(time.time() * 1000)
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
            |> filter(fn: (r) => r["sensor_type"] == "{random.choice(self.sensor_types)}")
            |> limit(n: 100)
            |> yield(name: "last")
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        # Return mock points for consistency
        return [Point("query_result").tag("type", "range").field("count", len(result))]

    def _execute_count_query(self) -> List[Point]:
        """Execute count query similar to Redis ZCOUNT"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
            |> filter(fn: (r) => r["sensor_type"] == "{random.choice(self.sensor_types)}")
            |> count()
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        return [Point("query_result").tag("type", "count").field("count", len(result))]

    def _execute_minmax_query(self) -> List[Point]:
        """Execute min/max query operations"""
        query_type = random.choice(["min", "max"])
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "sensor_reading")
            |> filter(fn: (r) => r["sensor_type"] == "{random.choice(self.sensor_types)}")
            |> {query_type}()
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        return [Point("query_result").tag("type", query_type).field("count", len(result))]

    def _rate_control(self):
        """实现精确的速率控制"""
        self._rate_limiter.acquire()

    def _worker(self, duration: float, read_pct: int):
        """Worker thread for mixed read/write operations"""
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()
            # Pacing
            interval = self.sample_interarrival()
            if interval > 0:
                time.sleep(interval)
                if self.report_realism:
                    with self.lock:
                        self.interarrivals.append(interval)
            # enforce upper bound (token bucket)
            self._rate_control()

            try:
                if do_read:
                    # Execute read operations
                    query_type = random.choice(["range", "count", "minmax"])
                    if query_type == "range":
                        self._execute_range_query()
                    elif query_type == "count":
                        self._execute_count_query()
                    else:
                        self._execute_minmax_query()
                else:
                    # Write sensor data
                    dev = None
                    if self.device_count > 0:
                        idx = random.randint(1, self.device_count)
                        dev = f"sensor-gateway-{idx:04d}"
                    elif hasattr(self, '_realism_profile') and self._realism_profile:
                        dev = self._realism_profile.choose_device_id()

                    if self.report_realism and dev:
                        with self.lock:
                            self.device_counter[dev] = self.device_counter.get(dev, 0) + 1

                    size_override = self.sample_payload_size_from_mixture()
                    if self.report_realism:
                        with self.lock:
                            self.payload_sizes_tracked.append(size_override)

                    points = self._generate_sensor_data(device_id=dev, target_size_override=size_override)
                    self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                    lat = (time.perf_counter() - start) * 1000.0

                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1

                    # Track payload size for stats
                    if not self.report_realism:
                        with self.lock:
                            self.payload_sizes.append(size_override)
                            if len(self.payload_sizes) > 1000:
                                self.payload_sizes.pop(0)
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
        self.start_time = start_time  # 需要设置start_time属性

        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct), daemon=True)
            t.start()
            tlist.append(t)

        logger.info("Started %d threads for %ds (read_pct=%d, sensors_per_device=%d, sensor_types=%s)",
                   threads, duration, read_pct, self.sensors_per_device, self.sensor_types)
        for t in tlist:
            t.join()
        self._print_summary(duration)

        # Close client connection
        self.client.close()


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

        # Emit structured metrics for orchestrator parsing
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

        if self.report_realism:
            try:
                import json
                report = {
                    "bench": "influx_sensoragg",
                    "timestamp": time.time(),
                    "duration": duration,
                    "total_ops": total,
                    "ops_per_sec": ops_per_sec,
                    "interarrivals": self.interarrivals,
                    "payload_sizes": self.payload_sizes_tracked,
                    "device_counts": self.device_counter
                }
                # Write to config_tests/results/realism_report_influx_sensoragg_<ts>.json
                out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../config_tests/results'))
                os.makedirs(out_dir, exist_ok=True)
                fn = os.path.join(out_dir, f"realism_report_influx_sensoragg_{int(time.time())}.json")
                with open(fn, 'w') as f:
                    json.dump(report, f)
                logger.info(f"Wrote realism report to {fn}")
            except Exception as e:
                logger.error(f"Failed to write realism report: {e}")


def main():
    parser = argparse.ArgumentParser(description="Enhanced Sensor Aggregator InfluxDB Benchmark")

    # InfluxDB connection
    parser.add_argument("--influx-url", default="http://localhost:8181", help="InfluxDB URL")
    parser.add_argument("--token", default="my-super-secret-auth-token", help="InfluxDB token")
    parser.add_argument("--org", default="my-org", help="InfluxDB org")
    parser.add_argument("--bucket", default="sensor-data", help="InfluxDB bucket")

    # Data scale extension
    parser.add_argument("--payload-size", default="1KB", type=str, help="Target payload size with units (e.g., 256B, 16KB, 1MB). Examples: 512B, 16KB, 1MB")
    parser.add_argument("--sensors-per-device", default=5, type=int, help="Sensors per device")

    # Data type realism
    parser.add_argument("--sensor-types", default="temperature,humidity,pressure,vibration",
                       help="Comma-separated sensor types")
    parser.add_argument("--environmental-noise", default=0.05, type=float,
                       help="Environmental noise level")

    # Size distribution option (align with other benches)
    parser.add_argument("--size-distribution", default="uniform", type=str,
                       choices=["uniform", "normal", "zipf"],
                       help="Distribution type for payload sizes (uniform, normal, zipf)")

    # Workload parameters
    parser.add_argument("--threads", "--concurrency", dest="threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--frontend-url", dest="frontend_url", default=None, help="Optional HTTP frontend URL to route requests through")
    parser.add_argument("--dataset", default=None, help="Path to dataset directory (default: repo datasets/)")
    parser.add_argument("--read-pct", default=10, type=int, help="Read operation percentage")
    parser.add_argument("--retention-policy", default="1h", type=str,
                       help="Bucket retention policy (e.g., 1h, 24h, 7d)")

    parser.add_argument("--payload-mode", default="json", choices=["json", "binary"],
                        help="Payload mode: json (structured) or binary (base64 blob)")

    # 消息速率控制
    parser.add_argument("--rps", "--qps", "--max-requests-per-second", dest="rps",
                        type=int, default=0, help="Maximum requests per second (0=no limit)")

    parser.add_argument("--realism", default=None, help="Realism profile name (e.g., edge_basic) or 'edge_bursty')")

    # Realism extensions
    parser.add_argument("--device-count", default=0, type=int, help="Limit number of unique devices (0=unlimited)")
    parser.add_argument("--pacing", action="store_true", help="Enable Poisson pacing")
    parser.add_argument("--payload-mixture", action="store_true", help="Enable payload size mixture")
    parser.add_argument("--report-realism", action="store_true", help="Write realism report JSON")

    args = parser.parse_args()

    # Common post-parse adjustments and dataset resolution
    if bench_common:
        if getattr(args, 'dataset', None) is None:
            args.dataset = bench_common.DEFAULT_DATASET_DIR
        args.dataset = bench_common.get_dataset_path(args)
        bench_common.configure_logging()
    else:
        if getattr(args, 'dataset', None) is None:
            args.dataset = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'datasets'))
        args.dataset = os.path.abspath(args.dataset)

    # Parse sensor types
    sensor_types = [st.strip() for st in args.sensor_types.split(",")]

    # support new unit-aware --payload-size string flag (supports units: B/KB/MB)
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

    # parse canonical --payload-size into bytes
    payload_bytes = parse_size_token(args.payload_size)

    bench = SensorInfluxBench(
        influx_url=args.influx_url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
    # Data scale extension (bytes)
    payload_size_bytes=payload_bytes,
        sensors_per_device=args.sensors_per_device,
        # Data type realism
        sensor_types=sensor_types,
        environmental_noise=args.environmental_noise,
        # 数据生命周期管理
        retention_policy=args.retention_policy,
        # 消息速率控制
        max_requests_per_second=args.rps,
        # Realism extensions
        device_count=args.device_count,
        pacing=args.pacing,
        payload_mixture=args.payload_mixture,
        report_realism=args.report_realism
    )

    bench.payload_mode = args.payload_mode
    # size distribution forwarding for payload sizing variability
    bench.size_distribution = args.size_distribution

    # optional realism profile (prototype)
    if args.realism:
        try:
            from experiment.migration.realistic import load_profile
            bench._realism_profile = load_profile(args.realism)
        except Exception:
            bench._realism_profile = None
    else:
        bench._realism_profile = None

    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)


if __name__ == "__main__":
    main()