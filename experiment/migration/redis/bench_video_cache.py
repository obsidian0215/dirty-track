#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Video Cache Benchmark Script

Full-featured video cache benchmark tool, includes:
- Data scaling: Dynamic load size, object count control
- Data authenticity: Camera geo-distribution, AI model characteristics, multi-class object detection
- Connection timeout config: Connection pooling, retry mechanism, timeout control
- Periodic monitoring: Real-time TPS and latency statistics

EXTENDED USAGE:
    --payload-size: Target payload size (supports units, e.g., 256B, 16KB, default 1KB)
  --objects-per-frame: Number of objects per frame (default 3)
  --camera-count: Camera count in simulation (default 10)
  --inference-model: AI inference model type (yolov5_small/medium/ssd_mobile)
  --connect-timeout: Connection timeout in seconds (default 5)
  --pool-size: Connection pool size (default CPU cores * 4)
"""
import argparse
import json
import logging
import random
import signal
import threading
import time
import statistics
import sys
from typing import List, Optional, Dict, Any
import redis
import os
import base64
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Use stderr instead of stdout to avoid buffering issues
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

class VideoCacheEnhancedBench:
    """Video cache benchmark test"""

    def __init__(self, redis_host: str = "127.0.0.1", redis_port: int = 6379,
              cache_ttl: int = 60, persist_hash: str = "video_inference_persist",
              # Data scaling expansion (bytes)
              payload_size_bytes: int = 2048, objects_per_frame: int = 3,
              # Data authenticity configuration
              camera_count: int = 10, inference_model: str = "yolov5_medium",
                 # Connection timeout configuration
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None):

        # Basic configuration
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.cache_ttl = cache_ttl
        self.persist_hash = persist_hash

        # Data scaling expansion configuration (bytes)
        self.payload_size_bytes = int(payload_size_bytes)
        self.objects_per_frame = objects_per_frame

        # Data authenticity configuration
        self.camera_count = camera_count
        self.inference_model = inference_model
        self.camera_positions: Dict[str, Dict[str, float]] = {}
        self.object_distribution = self._get_realistic_object_distribution()

        # Connection configuration
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size or (4 * 4)  # Default 16

        # Initialize components
        self.connection_pool = None
        self._stop = threading.Event()

        # Monitoring configuration
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # Initialize camera positions
        self._init_camera_positions()

        # Statistics
        self.latencies_ms: List[float] = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()
        # payload sizing and mode
        self.payload_sizes: List[int] = []
        self.payload_mode = "json"

    def _init_connection_pool(self):
        """Initialize Redis connection pool"""
        if self.connection_pool is None:
            try:
                logger.info(f"Connecting to Redis at {self.redis_host}:{self.redis_port}...")
                # Test connection first
                test_conn = redis.Redis(
                    host=self.redis_host,
                    port=self.redis_port,
                    socket_connect_timeout=self.connect_timeout,
                    socket_timeout=self.socket_timeout,
                    decode_responses=True
                )
                test_conn.ping()  # Test if connection is available
                logger.info("Redis connection successful")

                self.connection_pool = redis.ConnectionPool(
                    host=self.redis_host,
                    port=self.redis_port,
                    socket_connect_timeout=self.connect_timeout,
                    socket_timeout=self.socket_timeout,
                    max_connections=self.pool_size
                )
            except redis.ConnectionError as e:
                logger.error(f"Failed to connect to Redis at {self.redis_host}:{self.redis_port}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error connecting to Redis: {e}")
                raise

        return self.connection_pool

    def _init_camera_positions(self):
        """Initialize camera geo positions for realism simulation"""
        base_lat, base_lng = 31.0, 121.0  # Shanghai center

        for i in range(self.camera_count):
            # Random distribution of cameras within urban area
            lat_offset = (random.random() - 0.5) * 0.03  # +/-15km
            lng_offset = (random.random() - 0.5) * 0.03

            self.camera_positions[f"cam-{i+1}"] = {
                "lat": base_lat + lat_offset,
                "lng": base_lng + lng_offset
            }

    def _get_realistic_object_distribution(self) -> List[tuple]:
        """Get realistic urban object detection probability distribution"""
        return [
            ("person", 0.6),      # Most common
            ("car", 0.5),         # Also very common
            ("truck", 0.3),       # Less common
            ("bus", 0.1),         # Uncommon
            ("motorcycle", 0.2),  # Moderate
            ("bicycle", 0.4),     # Common
            ("traffic_light", 0.05),  # Rare
            ("stop_sign", 0.02),  # Even rarer
            ("dog", 0.003),       # Very rare
            ("cat", 0.001)        # Extremely rare
        ]

    def _generate_inference_objects(self) -> List[Dict[str, Any]]:
        """Generate object detection results based on realistic probability distribution"""
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
                    "speed_pixels_per_second": round(random.uniform(0, 10), 2) if obj_class in ["car", "truck", "bus"] else 0
                }
                objects.append(obj)

        return objects

    def _make_result(self) -> dict:
        """Generate enhanced video inference results"""
        # Select random camera
        camera_id = f"cam-{random.randint(1, self.camera_count)}"
        camera_pos = self.camera_positions[camera_id]

        # Generate basic inference results
        frame_id = f"frame-{random.randint(1000000, 9999999)}"
        timestamp = int(time.time() * 1000)

        # Generate object detection results
        objects = self._generate_inference_objects()

        # Calculate inference time (based on model type)
        model_processing_times = {
            "yolov5_small": random.gauss(50, 10),
            "yolov5_medium": random.gauss(80, 15),
            "ssd_mobile": random.gauss(40, 8)
        }
        inference_time = max(10, model_processing_times.get(self.inference_model, 50))

        result = {
            "frame_id": frame_id,
            "timestamp": timestamp,
            "inference_model": self.inference_model,
            "inference_time_ms": round(inference_time, 2),
            "total_objects": len(objects),
            "objects": objects,
            "frame_size": {"width": 1920, "height": 1080},
            "camera_id": camera_id,
            "location": {
                "lat": round(camera_pos["lat"], 6),
                "lng": round(camera_pos["lng"], 6)
            },
            "quality_metrics": {
                "brightness": round(random.uniform(0.3, 0.9), 2),
                "contrast": round(random.uniform(0.4, 1.0), 2),
                "motion_blur": random.choice([0, 0, 0, 1])  # Most frames have no motion blur
            }
        }

        # Data scaling expansion - Add additional video analysis data
        current_size = len(json.dumps(result))
        target_size_bytes = int(self.payload_size_bytes)

        if current_size < target_size_bytes:
            # Add granular analysis results
            analysis_types = ["pose_estimation", "anomaly_detection", "scene_classification", "behavior_analysis"]
            additional_analysis = {}

            while len(json.dumps({**result, "detailed_analysis": additional_analysis})) < target_size_bytes:
                analysis_type = random.choice(analysis_types)
                if analysis_type == "pose_estimation":
                    keypoint_count = 17  # COCO format keypoint count
                    additional_analysis["pose_estimation"] = {
                        "person_count": len([obj for obj in objects if obj["class"] == "person"]),
                        "keypoints": [[round(random.random(), 3) for _ in range(3)] for _ in range(keypoint_count)]
                    }
                elif analysis_type == "anomaly_detection":
                    additional_analysis["anomaly_detection"] = {
                        "score": round(random.uniform(0, 1), 3),
                        "anomalies": ["crowd_gathering", "wrong_direction", "object_falling"][:random.randint(0, 2)]
                    }
                elif analysis_type == "behavior_analysis":
                    additional_analysis["behavior_analysis"] = {
                        "person_behaviors": ["standing", "walking", "running", "interacting"][:random.randint(1, 4)],
                        "group_activities": random.choice(["gathering", "dispersion", "normal_flow"])
                    }
                else:  # scene_classification
                    scenes = ["urban_street", "parking_lot", "crosswalk", "highway"]
                    additional_analysis["scene_classification"] = {
                        "primary_scene": random.choice(scenes),
                        "confidence": round(random.uniform(0.8, 0.99), 3),
                        "scene_attributes": ["busy", "calm", "well_lit", "crowded"][:random.randint(1, 4)]
                    }

            if additional_analysis:
                result["detailed_analysis"] = additional_analysis

        return result

    def _periodic_monitoring(self, total_duration: float):
        """Periodically output Redis processing throughput and latency"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.monitor_interval:
            # Correct elapsed time calculation
            elapsed = current_time - self.start_time
            with self.lock:
                success_count = self.success
                new_operations = success_count - self.last_success_count

                if new_operations >= 0:
                    throughput_ops_sec = new_operations / (current_time - self.last_report_time)

                    # Calculate latency statistics
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
                    else:
                        logger.info(f"[{elapsed:.1f}s] TPS: {throughput_ops_sec:.1f}")

                    self.last_report_time = current_time
                    self.last_success_count = success_count

    def _worker(self, duration: float, write_pct: int, fallback_rate: int,
               do_get_pct: int, pool):
        """Worker thread containing all read/write logic"""
        r = redis.Redis(connection_pool=pool, decode_responses=True)
        end_time = time.time() + duration

        op_count = 0
        last_debug_time = time.time()

        while time.time() < end_time and not self._stop.is_set():

            op_rand = random.randint(1, 100)
            start = time.perf_counter()

            # Periodic debug output to verify thread is active
            current_debug_time = time.time()
            if current_debug_time - last_debug_time >= 5.0:  # Every 5 seconds
                logger.info(f"Thread active: processed {op_count} operations so far (total success: {self.success})")
                last_debug_time = current_debug_time

            try:
                if op_rand <= write_pct:
                    # Write path
                    logger.debug(f"Executing write operation (thread)")
                    res = self._make_result()
                    key = res["frame_id"]
                    payload = json.dumps(res)

                    if random.randint(1, 100) <= fallback_rate:
                        # Fallback to persistent storage
                        try:
                            if getattr(self, "payload_mode", "json") == "json":
                                r.hset(self.persist_hash, key, payload)
                                payload_size = len(payload.encode("utf-8"))
                            else:
                                target_bytes = int(self.payload_size_bytes)
                                raw = os.urandom(max(1, target_bytes))
                                b64 = base64.b64encode(raw).decode("ascii")
                                r.hset(self.persist_hash, key, b64)
                                payload_size = len(raw)

                            lat = (time.perf_counter() - start) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(lat)
                                self.success += 1
                                op_count += 1
                            try:
                                self.payload_sizes.append(int(payload_size))
                                if len(self.payload_sizes) > 1000:
                                    self.payload_sizes.pop(0)
                            except Exception:
                                pass
                            logger.debug(f"HSET operation success: {self.success} operations")
                        except Exception as e:
                            logger.warning(f"HSET operation failed: {e}")
                    else:
                        # Normal cache write
                        try:
                            if getattr(self, "payload_mode", "json") == "json":
                                r.set(key, payload, ex=self.cache_ttl)
                                payload_size = len(payload.encode("utf-8"))
                            else:
                                target_bytes = int(self.payload_size_bytes)
                                raw = os.urandom(max(1, target_bytes))
                                b64 = base64.b64encode(raw).decode("ascii")
                                r.set(key, b64, ex=self.cache_ttl)
                                payload_size = len(raw)

                            lat = (time.perf_counter() - start) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(lat)
                                self.success += 1
                                op_count += 1
                            try:
                                self.payload_sizes.append(int(payload_size))
                                if len(self.payload_sizes) > 1000:
                                    self.payload_sizes.pop(0)
                            except Exception:
                                pass
                            logger.debug(f"SET operation success: {self.success} operations")
                        except Exception as e:
                            logger.warning(f"SET operation failed: {e}")

                        # Optional immediate read verification
                        if random.randint(1, 100) <= do_get_pct:
                            try:
                                gstart = time.perf_counter()
                                _ = r.get(key)
                                glat = (time.perf_counter() - gstart) * 1000.0
                                with self.lock:
                                    self.latencies_ms.append(glat)
                                    self.success += 1
                                    op_count += 1
                                logger.debug(f"GET operation success: {self.success} operations")
                            except Exception as e:
                                logger.warning(f"GET operation failed: {e}")
                else:
                    # Read path
                    camera_id = f"cam-{random.randint(1, self.camera_count)}"
                    key = f"frame-{random.randint(1000000, 9999999)}"
                    start = time.perf_counter()
                    try:
                        _ = r.get(key)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1
                            op_count += 1
                        logger.debug(f"GET operation success: {self.success} operations")
                    except Exception as e:
                        logger.warning(f"GET operation failed: {e}")

            except Exception as e:
                with self.lock:
                    self.fail += 1
                time.sleep(0.01)

            # Periodic monitoring output - moved outside try-except to ensure it's always called
            self._periodic_monitoring(duration)

    def run(self, threads: int = 4, duration: int = 10, write_pct: int = 80,
           fallback_rate: int = 5, do_get_pct: int = 0):
        """Start benchmark test"""
        logger.info("Initializing Redis connection pool...")
        pool = self._init_connection_pool()
        logger.info("Redis connection pool initialized successfully")

        # Record test start time for precise elapsed time calculation
        start_time = time.time()

        # Initialize monitoring parameters
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        # Set stop event
        self._stop.clear()

        try:
            tlist = []

            for _ in range(threads):
                t = threading.Thread(target=self._worker, args=(duration, write_pct,
                                   fallback_rate, do_get_pct, pool), daemon=True)
                t.start()
                tlist.append(t)

            # Report payload size in bytes for accuracy
            logger.info("Started %d threads for %ds (model=%s, cameras=%d, payload_bytes=%d)",
                       threads, duration, self.inference_model, self.camera_count, self.payload_size_bytes)

            # Wait for threads to finish
            logger.info("Waiting for worker threads to complete...")
            for t in tlist:
                # Give threads enough time to finish gracefully (duration + 10 seconds buffer)
                t.join(timeout=max(duration + 10, 30))  # At least 30 seconds timeout
                if t.is_alive():
                    logger.warning("Worker thread %s is still alive, continuing with cleanup", t.name)
                    # Note: daemon threads will be automatically terminated when main process exits

            logger.info("All worker threads completed")
            self._print_summary(duration)

        except KeyboardInterrupt:
            logger.info("Interrupted by user, setting stop event...")
            self._stop.set()
            # Give threads a moment to process the stop event
            for t in tlist:
                t.join(timeout=5.0)  # Short timeout for graceful shutdown

    def _print_summary(self, duration: int):
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)

        logger.info("=== Enhanced Video Cache Benchmark Results ===")
        logger.info("Total operations: %d (success=%d, fail=%d)",
                   total, self.success, self.fail)
        logger.info("Throughput: %.2f ops/sec", ops_per_sec)

        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[min(int(len(lat) * p / 100), len(lat)-1)]
            logger.info("Latency (ms) - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                       statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

        logger.info("Configuration: %d objects/frame, %d cameras, %s model",
                   self.objects_per_frame, self.camera_count, self.inference_model)
        # Emit structured metrics for orchestration parsing
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
    parser = argparse.ArgumentParser(description="Enhanced Video Cache Redis Benchmark",
                                   formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # Data scaling expansion
    parser.add_argument("--payload-size", default="1KB", type=str,
                       help="Target payload size with units (e.g., 256B, 16KB, 1MB). Examples: 512B, 16KB, 1MB")
    parser.add_argument("--objects-per-frame", default=3, type=int,
                       help="Number of objects per frame (default: 3)")

    # Data authenticity
    parser.add_argument("--camera-count", default=10, type=int,
                       help="Number of cameras in simulation (default: 10)")
    parser.add_argument("--inference-model", default="yolov5_medium",
                       choices=["yolov5_small", "yolov5_medium", "ssd_mobile"],
                       help="AI inference model type (default: yolov5_medium)")

    # Connection timeout config
    parser.add_argument("--connect-timeout", default=5, type=int,
                       help="Connection timeout in seconds (default: 5)")
    parser.add_argument("--socket-timeout", default=5, type=int,
                       help="Socket timeout in seconds (default: 5)")
    parser.add_argument("--pool-timeout", default=10, type=int,
                       help="Connection pool timeout in seconds (default: 10)")
    parser.add_argument("--pool-size", type=int,
                       help="Connection pool size (default: CPU cores * 4)")

    # Cache parameters
    parser.add_argument("--ttl", default=60, type=int, help="Cache TTL in seconds")
    parser.add_argument("--persist-hash", default="video_inference_persist",
                       help="Hash key for fallback persistence")

    # Load parameters
    parser.add_argument("--threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--write-pct", default=80, type=int, help="Write operation percentage")
    parser.add_argument("--fallback-rate", default=5, type=int,
                       help="Fallback to persistence percentage")
    parser.add_argument("--do-get-pct", default=0, type=int,
                       help="Immediate get after set percentage")
    parser.add_argument("--payload-mode", default="json", choices=["json", "binary"],
                        help="Payload mode: json (structured) or binary (base64 blob)")

    args = parser.parse_args()

    # Calculate default pool size
    if args.pool_size is None:
        args.pool_size = 4 * 4  # Default 16

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

    bench = VideoCacheEnhancedBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        cache_ttl=args.ttl,
        persist_hash=args.persist_hash,
        # Data scaling expansion
        payload_size_bytes=payload_bytes,
        objects_per_frame=args.objects_per_frame,
        # Data authenticity
        camera_count=args.camera_count,
        inference_model=args.inference_model,
        # Connection timeout config
        connect_timeout=args.connect_timeout,
        socket_timeout=args.socket_timeout,
        pool_timeout=args.pool_timeout,
        pool_size=args.pool_size
    )
    bench.payload_mode = args.payload_mode

    bench.run(threads=args.threads, duration=args.duration,
             write_pct=args.write_pct, fallback_rate=args.fallback_rate,
             do_get_pct=args.do_get_pct)

if __name__ == "__main__":
    main()