#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensor_influx.py - Sensor Aggregation Benchmark for InfluxDB

Enhanced Sensor Aggregator Benchmark with realistic sensors and scalable data
Supports multiple sensor types, data scale extension, and robust connection management
Similar to Redis bench_sensoragg.py but adapted for InfluxDB time series

FEATURES:
   - 数据规模扩展: Configurable payload sizes and sensors per device
   - 数据类型真实性: Multi-sensor types (temp/humidity/pressure/vibration) with drift simulation
   - 连接池管理: Connection pooling and robust error handling

USAGE:
   python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token --org my-org --bucket sensor-data --threads 8 --duration 30 --read-pct 10
   python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token --org my-org --bucket sensor-data --threads 4 --duration 60 --sensors-per-device 10 --sensor-types temperature,humidity,pressure
   python3 bench_sensor_influx.py --influx-url http://localhost:8086 --token my-token --org my-org --bucket sensor-data --payload-size-kb 2

EXTENDED USAGE:
   --sensors-per-device: Number of sensors per device (default: 5)
   --sensor-types: Comma-separated sensor types (default: temperature,humidity,pressure,vibration)
   --environmental-noise: Environmental noise level (default: 0.05)
   --payload-size-kb: Target payload size in KB (default: 1)
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
from typing import List, Optional, Dict, Any
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


class SensorInfluxBench:
    """Enhanced Sensor Aggregator InfluxDB Benchmark"""
    def __init__(self, influx_url: str, token: str, org: str, bucket: str = "sensor-data",
                 # Data scale extension
                 payload_size_kb: int = 1, sensors_per_device: int = 5,
                 # Data type realism
                 sensor_types: Optional[List[str]] = None,
                 environmental_noise: float = 0.05):

        self.influx_url = influx_url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._stop = threading.Event()

        # Data scale extension config
        self.payload_size_kb = payload_size_kb
        self.sensors_per_device = sensors_per_device

        # Data type realism config
        self.sensor_types = sensor_types or ["temperature", "humidity", "pressure", "vibration"]
        self.environmental_noise = environmental_noise
        self.sensor_states: Dict[str, Dict[str, Any]] = {}

        # Monitoring config
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # Statistics
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # Initialize InfluxDB client
        self.client = InfluxDBClient(url=influx_url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        self.query_api = self.client.query_api()

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

    def _generate_sensor_data(self) -> List[Point]:
        """Generate enhanced sensor data points"""
        points = []
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

        # Data scale extension - add environmental sensors to reach target size
        current_size = len(json.dumps({
            "sensor_id": primary_sensor_id, "value": value,
            "battery_level": state["battery_level"], "readings_count": state["readings_count"]
        }))
        target_size_bytes = self.payload_size_kb * 1024

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
            current_size = len(json.dumps({
                **{"sensor_id": primary_sensor_id, "value": value},
                "additional_sensors": additional_points + 1
            }))
            additional_points += 1

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

    def _worker(self, duration: float, read_pct: int):
        """Worker thread for mixed read/write operations"""
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()

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
                    points = self._generate_sensor_data()
                    self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                    lat = (time.perf_counter() - start) * 1000.0

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
    parser = argparse.ArgumentParser(description="Enhanced Sensor Aggregator InfluxDB Benchmark")

    # InfluxDB connection
    parser.add_argument("--influx-url", default="http://localhost:8086", help="InfluxDB URL")
    parser.add_argument("--token", default="my-super-secret-auth-token", help="InfluxDB token")
    parser.add_argument("--org", default="my-org", help="InfluxDB org")
    parser.add_argument("--bucket", default="sensor-data", help="InfluxDB bucket")

    # Data scale extension
    parser.add_argument("--payload-size-kb", default=1, type=int, help="Target payload size in KB")
    parser.add_argument("--sensors-per-device", default=5, type=int, help="Sensors per device")

    # Data type realism
    parser.add_argument("--sensor-types", default="temperature,humidity,pressure,vibration",
                       help="Comma-separated sensor types")
    parser.add_argument("--environmental-noise", default=0.05, type=float,
                       help="Environmental noise level")

    # Workload parameters
    parser.add_argument("--threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--read-pct", default=10, type=int, help="Read operation percentage")

    args = parser.parse_args()

    # Parse sensor types
    sensor_types = [st.strip() for st in args.sensor_types.split(",")]

    bench = SensorInfluxBench(
        influx_url=args.influx_url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
        # Data scale extension
        payload_size_kb=args.payload_size_kb,
        sensors_per_device=args.sensors_per_device,
        # Data type realism
        sensor_types=sensor_types,
        environmental_noise=args.environmental_noise
    )

    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)


if __name__ == "__main__":
    main()