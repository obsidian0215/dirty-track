#!/usr/bin/env python3
# coding: utf-8
"""
bench_video_cache.py - Video Inference Benchmark for InfluxDB

Complete Video Inference Benchmark adapted for InfluxDB
Enhanced data scale extension, realistic inference simulation, and robust operations
Similar to Redis bench_video_cache_enhanced.py but using InfluxDB time series storage

FEATURES:
   - 数据规模扩展: Dynamic payload sizes, object count control
   - 数据类型真实性: Camera geographical distribution, AI model characteristics
   - 推理模拟: Multiple inference models (YOLO, SSD, etc.)

USAGE:
    python3 bench_video_cache.py --influx-url http://localhost:8181 --threads 8 --duration 30 --inference-model yolov5_medium --payload-size 5KB
   python3 bench_video_cache.py --influx-url http://localhost:8181 --threads 4 --duration 60 --camera-count 15 --objects-per-frame 5
   python3 bench_video_cache.py --influx-url http://localhost:8181 --write-pct 85 --read-pct 15

EXTENDED USAGE:
    --payload-size: Target payload size (supports units, e.g., 256B, 16KB) (default: 2KB)
   --objects-per-frame: Objects per frame (default: 3)
   --camera-count: Number of cameras (default: 10)
   --inference-model: AI inference model type (yolov5_small/medium/ssd_mobile)
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
from typing import List, Dict, Any
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import ASYNCHRONOUS
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


class VideoInfluxBench:
    """Complete Video Inference InfluxDB Benchmark"""
    def __init__(self, influx_url: str, token: str, org: str, bucket: str = "video-data",
                 # Data scale extension
                 payload_size_bytes: int = 2048, objects_per_frame: int = 3,
                 # Data type realism
                 camera_count: int = 10, inference_model: str = "yolov5_medium"):

        self.influx_url = influx_url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._stop = threading.Event()

        # Data scale extension config (bytes)
        self.payload_size_bytes = int(payload_size_bytes)
        self.objects_per_frame = objects_per_frame

        # Data type realism config
        self.camera_count = camera_count
        self.inference_model = inference_model
        self.camera_positions: Dict[str, Dict[str, float]] = {}
        self.object_distribution = self._get_realistic_object_distribution()

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

        # Initialize components
        self.client = InfluxDBClient(url=influx_url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=ASYNCHRONOUS)
        self.query_api = self.client.query_api()

        # Initialize camera positions
        self._init_camera_positions()

    def _init_camera_positions(self):
        """Initialize camera geographical positions for realism"""
        base_lat, base_lng = 31.0, 121.0  # Shanghai center

        for i in range(self.camera_count):
            # Random distribution within city area
            lat_offset = (random.random() - 0.5) * 0.03  # ±15km
            lng_offset = (random.random() - 0.5) * 0.03

            self.camera_positions[f"cam-{i+1}"] = {
                "lat": base_lat + lat_offset,
                "lng": base_lng + lng_offset
            }

    def _get_realistic_object_distribution(self) -> List[tuple]:
        """Get realistic urban object detection probability distribution"""
        return [
            ("person", 0.6),      # Most common
            ("car", 0.5),         # Very common
            ("truck", 0.3),       # Less common
            ("bus", 0.1),         # Uncommon
            ("motorcycle", 0.2),  # Moderately common
            ("bicycle", 0.4),     # Common
            ("traffic_light", 0.05),  # Rare
            ("stop_sign", 0.02),  # Very rare
            ("dog", 0.003),       # Very rare
            ("cat", 0.001)        # Extremely rare
        ]

    def _generate_inference_objects(self) -> List[Dict[str, Any]]:
        """Generate object detection results based on realistic probabilities"""
        model_configs = {
            "yolov5_small": {"confidence_range": (0.3, 0.9)},
            "yolov5_medium": {"confidence_range": (0.5, 0.95)},
            "ssd_mobile": {"confidence_range": (0.2, 0.85)}
        }

        config = model_configs.get(self.inference_model, model_configs["yolov5_medium"])
        objects = []

        for obj_class, prob in self.object_distribution:
            if random.random() < prob and len(objects) < self.objects_per_frame:
                obj = {
                    "class": obj_class,
                    "confidence": round(random.uniform(*config["confidence_range"]), 3),
                    "bbox": [round(random.random(), 3) for _ in range(4)],  # [x,y,w,h]
                    "tracking_id": random.randint(0, 10000),
                    "speed_pixels_per_second": round(random.uniform(0, 10), 2) if obj_class in ["car", "truck", "bus"] else 0,
                    "direction": random.choice(["north", "south", "east", "west"]) if obj_class in ["person", "car", "truck", "bus"] else "stationary",
                    "size_pixels": round(random.uniform(50, 500), 1)
                }
                objects.append(obj)

        return objects

    def _generate_inference_result(self) -> List[Point]:
        """Generate complete video inference result"""
        points = []

        # Select random camera
        camera_id = f"cam-{random.randint(1, self.camera_count)}"
        camera_pos = self.camera_positions[camera_id]

        # Generate base inference result
        frame_id = f"frame-{random.randint(1000000, 9999999)}"
        timestamp = int(time.time() * 1000000000)

        # Generate object detection results
        objects = self._generate_inference_objects()

        # Calculate inference time based on model type
        model_processing_times = {
            "yolov5_small": random.gauss(50, 10),
            "yolov5_medium": random.gauss(80, 15),
            "ssd_mobile": random.gauss(40, 8)
        }
        inference_time = max(10, model_processing_times.get(self.inference_model, 50))

        # Main inference result point
        main_point = Point("video_inference") \
            .tag("frame_id", frame_id) \
            .tag("camera_id", camera_id) \
            .tag("inference_model", self.inference_model) \
            .tag("scene_type", random.choice(["urban_street", "parking_lot", "crosswalk", "highway"])) \
            .tag("weather_condition", random.choice(["clear", "cloudy", "rainy", "foggy"])) \
            .tag("time_of_day", random.choice(["day", "night", "dusk"])) \
            .field("inference_time_ms", round(inference_time, 2)) \
            .field("total_objects", len(objects)) \
            .field("frame_width", 1920) \
            .field("frame_height", 1080) \
            .field("camera_latitude", round(camera_pos["lat"], 6)) \
            .field("camera_longitude", round(camera_pos["lng"], 6)) \
            .field("brightness", round(random.uniform(0.3, 0.9), 2)) \
            .field("contrast", round(random.uniform(0.4, 1.0), 2)) \
            .field("motion_blur_score", random.choice([0, 0, 0, 1])) \
            .time(timestamp, write_precision=WritePrecision.NS)

        points.append(main_point)

        # Object detection points
        for i, obj in enumerate(objects):
            obj_point = Point("detected_object") \
                .tag("frame_id", frame_id) \
                .tag("camera_id", camera_id) \
                .tag("object_class", obj["class"]) \
                .tag("direction", obj["direction"]) \
                .tag("tracking_status", "tracked" if obj["tracking_id"] > 0 else "new") \
                .field("confidence", obj["confidence"]) \
                .field("bbox_x", obj["bbox"][0]) \
                .field("bbox_y", obj["bbox"][1]) \
                .field("bbox_width", obj["bbox"][2]) \
                .field("bbox_height", obj["bbox"][3]) \
                .field("tracking_id", obj["tracking_id"]) \
                .field("speed_pixels_per_second", obj["speed_pixels_per_second"]) \
                .field("size_pixels", obj["size_pixels"]) \
                .field("center_x", round(obj["bbox"][0] + obj["bbox"][2]/2, 3)) \
                .field("center_y", round(obj["bbox"][1] + obj["bbox"][3]/2, 3)) \
                .time(timestamp, write_precision=WritePrecision.NS)

            points.append(obj_point)

        # Data scale extension - add detailed analysis
        current_size = len(json.dumps({
            "frame_id": frame_id, "objects": len(objects),
            "inference_time": inference_time
        }))
        target_size_bytes = int(self.payload_size_bytes)

        if current_size < target_size_bytes:
            # Add detailed analysis results
            analysis_types = ["pose_estimation", "anomaly_detection", "scene_classification", "behavior_analysis"]
            additional_analysis = {}

            while len(json.dumps({**{"frame_id": frame_id}, "detailed_analysis": additional_analysis})) < target_size_bytes and len(additional_analysis) < 4:
                analysis_type = random.choice(analysis_types)
                if analysis_type not in additional_analysis:
                    if analysis_type == "pose_estimation":
                        person_count = len([obj for obj in objects if obj["class"] == "person"])
                        keypoint_count = 17  # COCO format keypoints
                        keypoints = [[round(random.random(), 3) for _ in range(3)] for _ in range(keypoint_count)]

                        analysis_point = Point("pose_estimation") \
                            .tag("frame_id", frame_id) \
                            .tag("camera_id", camera_id) \
                            .field("person_count", person_count) \
                            .field("keypoint_count", keypoint_count) \
                            .field("pose_confidence", round(random.uniform(0.7, 0.95), 3)) \
                            .time(timestamp, write_precision=WritePrecision.NS)
                        points.append(analysis_point)

                        # Add keypoint data as separate points
                        for kp_idx, kp in enumerate(keypoints[:5]):  # Limit to first 5 keypoints
                            kp_point = Point("pose_keypoint") \
                                .tag("frame_id", frame_id) \
                                .tag("camera_id", camera_id) \
                                .tag("keypoint_id", str(kp_idx)) \
                                .field("x", kp[0]) \
                                .field("y", kp[1]) \
                                .field("confidence", kp[2]) \
                                .time(timestamp, write_precision=WritePrecision.NS)
                            points.append(kp_point)

                    elif analysis_type == "anomaly_detection":
                        anomaly_point = Point("anomaly_detection") \
                            .tag("frame_id", frame_id) \
                            .tag("camera_id", camera_id) \
                            .field("anomaly_score", round(random.uniform(0, 1), 3)) \
                            .field("anomaly_type", random.choice(["crowd_gathering", "wrong_direction", "object_falling", "none"])) \
                            .field("detection_confidence", round(random.uniform(0.8, 0.99), 3)) \
                            .time(timestamp, write_precision=WritePrecision.NS)
                        points.append(anomaly_point)

                    elif analysis_type == "behavior_analysis":
                        behavior_point = Point("behavior_analysis") \
                            .tag("frame_id", frame_id) \
                            .tag("camera_id", camera_id) \
                            .field("group_activity", random.choice(["gathering", "dispersion", "normal_flow"])) \
                            .field("person_behaviors", ",".join(random.sample(["standing", "walking", "running", "interacting"], random.randint(1, 3)))) \
                            .field("traffic_flow_rate", random.randint(0, 100)) \
                            .time(timestamp, write_precision=WritePrecision.NS)
                        points.append(behavior_point)

                    additional_analysis[analysis_type] = True

        # Estimate payload bytes for orchestration metrics (approximate)
        try:
            payload_est = {
                "frame_id": frame_id,
                "objects": len(objects),
            }
            if 'additional_analysis' in locals() and additional_analysis:
                payload_est["analysis_keys"] = len(additional_analysis)
            est_bytes = len(json.dumps(payload_est))
        except Exception:
            est_bytes = 0

        # store last estimate for worker to record
        try:
            self._last_payload_estimate = int(est_bytes)
        except Exception:
            self._last_payload_estimate = 0

        return points

    # NOTE: payload_sizes will be recorded by callers after write when possible

    def _execute_frame_query(self):
        """Execute frame-based query for recent detections"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "video_inference")
            |> filter(fn: (r) => r["inference_model"] == "{self.inference_model}")
            |> filter(fn: (r) => r["total_objects"] > 0)
            |> limit(n: 20)
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        return len(result)

    def _execute_object_query(self):
        """Execute object-specific query"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -1h)
            |> filter(fn: (r) => r["_measurement"] == "detected_object")
            |> filter(fn: (r) => r["object_class"] == "person")
            |> filter(fn: (r) => r["confidence"] > 0.5)
            |> count()
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock:
            self.latencies_ms.append(latency)
            self.success += 1

        return len(result)

    def _worker(self, duration: float, write_pct: int, read_pct: int):
        """Worker thread with mixed read/write operations"""
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            op_rand = random.randint(1, 100)
            start = time.perf_counter()

            try:
                if op_rand <= write_pct:
                    # Write inference results
                    points = self._generate_inference_result()
                    self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                    lat = (time.perf_counter() - start) * 1000.0

                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1

                    # record estimated payload size if available
                    try:
                        p = int(getattr(self, '_last_payload_estimate', 0))
                        self.payload_sizes.append(p)
                        if len(self.payload_sizes) > 1000:
                            self.payload_sizes.pop(0)
                    except Exception:
                        pass

                    # Optional immediate read verification
                    if random.randint(1, 100) <= read_pct:
                        self._execute_frame_query()

                else:
                    # Execute read operations
                    query_type = random.choice(["frame", "object", "anomaly"])
                    if query_type == "frame":
                        self._execute_frame_query()
                    elif query_type == "object":
                        self._execute_object_query()
                    else:
                        # Simple anomaly query
                        self._execute_frame_query()

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

    def run(self, threads: int = 4, duration: int = 10, write_pct: int = 80, read_pct: int = 10):
        """Run the benchmark"""
        # 记录测试开始时间，用于计算精确的elapsed时间
        start_time = time.time()

        # 初始化监控参数
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, write_pct, read_pct), daemon=True)
            t.start()
            tlist.append(t)

        # Report payload size in bytes
        logger.info("Started %d threads for %ds (model=%s, cameras=%d, payload_bytes=%d)",
                    threads, duration, self.inference_model, self.camera_count, self.payload_size_bytes)

        for t in tlist:
            t.join()

        self._print_summary(duration)

        # Close client connection
        self.client.close()

    def _print_summary(self, duration: int):
        """Print benchmark summary"""
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)

        logger.info("=== Enhanced Video Inference Benchmark Results ===")
        logger.info("Total operations: %d (success=%d, fail=%d)", total, self.success, self.fail)
        logger.info("Throughput: %.2f ops/sec", ops_per_sec)

        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[min(int(len(lat) * p / 100), len(lat)-1)]
            logger.info("Latency (ms) - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                       statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

        logger.info("Configuration: %d objects/frame, %d cameras, %s model",
                   self.objects_per_frame, self.camera_count, self.inference_model)

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


def main():
    parser = argparse.ArgumentParser(description="Complete Video Inference InfluxDB Benchmark",
                                   formatter_class=argparse.RawDescriptionHelpFormatter)

    # InfluxDB connection
    parser.add_argument("--influx-url", default="http://localhost:8181", help="InfluxDB URL")
    parser.add_argument("--token", default="my-super-secret-auth-token", help="InfluxDB token")
    parser.add_argument("--org", default="my-org", help="InfluxDB org")
    parser.add_argument("--bucket", default="video-data", help="InfluxDB bucket")

    # Data scale extension
    parser.add_argument("--payload-size", default="2KB", type=str, help="Target payload size with units (e.g., 256B, 16KB, 1MB). Examples: 512B, 16KB, 1MB")
    parser.add_argument("--objects-per-frame", default=3, type=int, help="Objects per frame")

    # Data type realism
    parser.add_argument("--camera-count", default=10, type=int, help="Number of cameras")
    parser.add_argument("--inference-model", default="yolov5_medium",
                       choices=["yolov5_small", "yolov5_medium", "ssd_mobile"], help="AI inference model type")

    # Workload parameters
    parser.add_argument("--threads", "--concurrency", dest="threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--frontend-url", dest="frontend_url", default=None, help="Optional HTTP frontend URL to route requests through")
    parser.add_argument("--dataset", default=None, help="Path to dataset directory (default: repo datasets/)")
    parser.add_argument("--write-pct", default=80, type=int, help="Write operation percentage")
    parser.add_argument("--read-pct", default=10, type=int, help="Read operation percentage")
    parser.add_argument("--payload-mode", default="json", choices=["json", "binary"],
                        help="Payload mode: json (structured) or binary (base64 blob)")

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
    # unit-aware --payload-size support
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

    bench = VideoInfluxBench(
        influx_url=args.influx_url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
        # Data scale extension
        payload_size_bytes=payload_bytes,
        objects_per_frame=args.objects_per_frame,
        # Data type realism
        camera_count=args.camera_count,
        inference_model=args.inference_model
    )

    bench.payload_mode = args.payload_mode

    bench.run(threads=args.threads, duration=args.duration,
             write_pct=args.write_pct, read_pct=args.read_pct)


if __name__ == "__main__":
    main()