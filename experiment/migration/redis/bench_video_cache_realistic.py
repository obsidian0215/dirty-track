#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Realistic Video Cache Benchmark Script with Frame Size and Frame Rate Calculation
增强版视频缓存基准测试脚本，负载大小基于图像帧大小、帧率和分析复杂度自动计算

Full-featured video cache benchmark tool with realistic payload sizes, includes:
- Realistic frame size calculations: HD/4K resolutions based on camera types
- Framerate-based load distribution: 30fps/60fps processing simulation
- Dynamic payload scaling: Based on object count, frame resolution, and analysis types
- Data authenticity: Camera geo-distribution, AI model characteristics, multi-class object detection
- Connection timeout config: Connection pooling, retry mechanism, timeout control
- Periodic monitoring: Real-time TPS and latency statistics

REALISTIC PAYLOAD CALCULATION:
- Base frame size: 1920x1080 (HD), 3840x2160 (4K)
- Per-object overhead: ~200-500B per object for coordinates, confidence, tracking data
- Analysis overhead: Multiplies per frame resolution and object count
- Framerate impact: Higher frame rates mean more data over time

EXTENDED USAGE:
  --frame-width: Frame width (default: 1920)
  --frame-height: Frame height (default: 1080)
  --framerate: Target framerate in fps (default: 30)
  --analysis-intensity: Analysis complexity (mechanical/objective/comprehensive)
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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Use stderr instead of stdout to avoid buffering issues
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

class VideoCacheRealisticBench:
    """Video cache benchmark test with realistic frame size calculations"""

    def __init__(self, redis_host: str = "127.0.0.1", redis_port: int = 6379,
                 cache_ttl: int = 60, persist_hash: str = "video_inference_persist",
                 # Realistic frame parameters
                 frame_width: int = 1920, frame_height: int = 1080,
                 framerate: int = 30, analysis_intensity: str = "comprehensive",
                 objects_per_frame: int = 8,
                 # Data authenticity configuration
                 camera_count: int = 100, inference_model: str = "yolov5_medium",
                 # Connection timeout configuration
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None):

        # Basic configuration
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.cache_ttl = cache_ttl
        self.persist_hash = persist_hash

        # Realistic frame configuration
        self.frame_width = frame_width
        self.frame_height = frame_height
        self.framerate = framerate
        self.analysis_intensity = analysis_intensity
        self.objects_per_frame = objects_per_frame

        # Calculate realistic payload size based on frame parameters
        self.base_payload_size_kb = self._calculate_realistic_payload_size()

        # Analysis intensity multipliers
        self.intensity_multipliers = {
            "mechanical": 0.5,    # Basic detection only
            "objective": 1.0,     # Standard analysis
            "comprehensive": 2.0  # Full AI analysis suite
        }
        self.intensity_multiplier = self.intensity_multipliers.get(self.analysis_intensity, 1.0)
        self.effective_payload_size_kb = int(self.base_payload_size_kb * self.intensity_multiplier)

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

        # Statistics
        self.latencies_ms: List[float] = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # Initialize camera positions
        self._init_camera_positions()

        logger.info("Realistic configuration - Frame: %dx%d, Framerate: %dfps, Analysis: %s",
                   self.frame_width, self.frame_height, self.framerate, self.analysis_intensity)
        logger.info("Calculated payload size: %.1fKB per frame (base: %.1fKB, intensity: %.2f)",
                   self.effective_payload_size_kb, self.base_payload_size_kb, self.intensity_multiplier)

    def _calculate_realistic_payload_size(self) -> float:
        """Calculate realistic payload size based on frame parameters

        Base calculation:
        - Frame resolution factor: (width * height) / (1920 * 1080)
        - Per-object overhead: ~300B per object for coordinates, confidence, tracking
        - Frame metadata overhead: ~500B base + frame resolution factor
        - Compression factor: Realistic JSON vs binary difference
        """
        # Base resolution reference (1920x1080)
        reference_pixels = 1920 * 1080
        current_pixels = self.frame_width * self.frame_height
        resolution_factor = current_pixels / reference_pixels

        # Base metadata size (JSON overhead for frame data)
        base_metadata_kb = 1.5  # Base frame metadata, timestamps, etc.

        # Resolution-contributed size (higher resolution = more detailed analysis)
        resolution_contribution_kb = (current_pixels / 1000000) * 0.8  # ~0.8KB per megapixel

        # Object count contribution (each object adds metadata)
        # Average realistic object count based on frame
        base_object_count = self.objects_per_frame  # Use configured objects per frame
        object_contribution_kb = base_object_count * 0.25  # ~250B per object for coords/confidence/metadata

        # Framerate consideration (higher fps might need more processing metadata)
        framerate_factor = min(self.framerate / 30.0, 2.0)  # Up to 2x at 60fps
        framerate_contribution_kb = base_metadata_kb * (framerate_factor - 1) * 0.1

        calculated_size_kb = (base_metadata_kb + resolution_contribution_kb +
                            object_contribution_kb + framerate_contribution_kb)

        return max(calculated_size_kb, 1.0)  # Minimum 1KB

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
            lat_offset = (random.random() - 0.5) * 0.08  # +/-40km for larger city area
            lng_offset = (random.random() - 0.5) * 0.08
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
        """Generate realistic video inference results based on frame parameters"""
        # Select random camera
        camera_id = f"cam-{random.randint(1, self.camera_count)}"
        camera_pos = self.camera_positions[camera_id]

        # Generate basic inference results
        frame_id = f"frame-{random.randint(1000000, 9999999)}"
        timestamp = int(time.time() * 1000)

        # Generate object detection results
        objects = self._generate_inference_objects()

        # Calculate inference time (based on model type and frame resolution)
        model_processing_times = {
            "yolov5_small": random.gauss(50, 10),
            "yolov5_medium": random.gauss(80, 15),
            "ssd_mobile": random.gauss(40, 8)
        }
        # Resolution affects processing time
        resolution_factor = (self.frame_width * self.frame_height) / (1920 * 1080)
        base_inference_time = model_processing_times.get(self.inference_model, 50)
        inference_time = base_inference_time * (0.8 + 0.2 * resolution_factor)

        result = {
            "frame_id": frame_id,
            "timestamp": timestamp,
            "framerate": self.framerate,
            "inference_model": self.inference_model,
            "inference_time_ms": round(inference_time, 2),
            "total_objects": len(objects),
            "objects": objects,
            "frame_size": {"width": self.frame_width, "height": self.frame_height},
            "camera_id": camera_id,
            "location": {
                "lat": round(camera_pos["lat"], 6),
                "lng": round(camera_pos["lng"], 6)
            },
            "quality_metrics": {
                "brightness": round(random.uniform(0.3, 0.9), 2),
                "contrast": round(random.uniform(0.4, 1.0), 2),
                "motion_blur": random.choice([0, 0, 0, 1])  # Most frames have no motion blur
            },
            "analysis_intensity": self.analysis_intensity
        }

        # Realistic analysis data generation based on intensity
        if self.analysis_intensity in ["objective", "comprehensive"]:
            additional_analysis = {}

            # Pose estimation (if people present)
            people_count = len([obj for obj in objects if obj["class"] == "person"])
            if people_count > 0:
                keypoint_count = 17
                person_estimators = []
                for _ in range(people_count):
                    person_estimators.append({
                        "confidence": round(random.uniform(0.5, 0.95), 3),
                        "keypoints": [round(random.random(), 3) for _ in range(keypoint_count * 3)]
                    })
                additional_analysis["pose_estimation"] = {
                    "person_count": people_count,
                    "person_estimators": person_estimators[:min(people_count, self.intensity_multiplier)]
                }

            if self.analysis_intensity == "comprehensive":
                # Additional comprehensive analysis
                additional_analysis["scene_analysis"] = {
                    "crowd_density": round(random.uniform(0, 1), 3),
                    "traffic_flow": random.choice(["free", "moderate", "heavy", "jammed"]),
                    "anomaly_score": round(random.uniform(0, 1), 3)
                }

                # Enhanced object tracking
                if len(objects) > 0:
                    tracking_data = []
                    for obj in objects[:int(len(objects) * 0.7)]:  # Track 70% of objects
                        track_length = random.randint(2, self.framerate // 10)  # Track for ~1-3 seconds
                        trajectory = []
                        for step in range(track_length):
                            trajectory.append({
                                "x": obj["bbox"][0] + random.gauss(0, 5),
                                "y": obj["bbox"][1] + random.gauss(0, 5),
                                "timestamp": timestamp + step * (1000 // self.framerate)
                            })
                        tracking_data.append({
                            "object_id": obj["tracking_id"],
                            "trajectory": trajectory
                        })
                    additional_analysis["trajectory_tracking"] = {
                        "track_count": len(tracking_data),
                        "trajectories": tracking_data
                    }

            if additional_analysis:
                result["additional_analysis"] = additional_analysis

        # Dynamic payload expansion to meet target size
        current_size = len(json.dumps(result))
        target_size_bytes = int(self.effective_payload_size_kb * 1024)

        if current_size < target_size_bytes:
            if "additional_analysis" not in result:
                result["additional_analysis"] = {}

            # Fill up to target size with realistic additional data
            while len(json.dumps(result)) < target_size_bytes:
                if random.random() < 0.5:
                    # Add more detailed frame analysis
                    analysis_key = f"frame_analysis_{random.randint(1000, 9999)}"
                    result["additional_analysis"][analysis_key] = {
                        "analysis_type": "detailed_" + random.choice(["texture", "color_histogram", "edges"]),
                        "data_points": [[round(random.random(), 3) for _ in range(10)] for _ in range(10)],
                        "confidence": round(random.uniform(0.7, 0.95), 3)
                    }
                else:
                    # Add performance metrics
                    metric_key = f"process_metrics_{random.randint(1000, 9999)}"
                    result["additional_analysis"][metric_key] = {
                        "stages": [
                            {"stage": "preprocessing", "time_ms": random.gauss(10, 2)},
                            {"stage": "inference", "time_ms": random.gauss(60, 8)},
                            {"stage": "postprocessing", "time_ms": random.gauss(15, 3)}
                        ],
                        "memory_usage_mb": random.gauss(500, 50),
                        "cpu_usage_percent": random.gauss(40, 8)
                    }

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
               do_get_pct: int, pool, total_threads: int):
        """Worker thread containing all read/write logic with realistic framerate control"""
        r = redis.Redis(connection_pool=pool, decode_responses=True)
        end_time = time.time() + duration

        # Improved frame generation with higher concurrency
        # Allow more cameras per thread and optimize processing intervals
        cameras_per_thread = max(1, self.camera_count // total_threads)  # Distribute cameras across threads

        # Calculate target operations per second for the thread based on framerate
        target_ops_per_second = self.framerate * cameras_per_thread
        min_inter_operation_delay = 1.0 / target_ops_per_second  # Minimum delay between operations
        last_operation_time = time.time()

        logger.info(f"Thread targeting {target_ops_per_second:.1f} ops/sec (cameras: {cameras_per_thread}, effective: {1.0/min_inter_operation_delay:.3f}s)")

        while time.time() < end_time and not self._stop.is_set():
            current_time = time.time()

            # Control operation rate based on target framerate
            time_since_last_op = current_time - last_operation_time
            if time_since_last_op >= min_inter_operation_delay:

                # Process operation with rate limiting
                op_rand = random.randint(1, 100)
                start = time.perf_counter()

                try:
                    if op_rand <= write_pct:
                        # Write path - simulate video frame processing
                        # Reduce AI inference time from ~17ms to ~3-5ms to allow higher throughput
                        processing_delay = random.gauss(2.0, 1.0) / 1000.0  # ~2ms AI inference for high throughput
                        time.sleep(processing_delay)  # Simulate AI processing time

                        res = self._make_result()
                        key = res["frame_id"]
                        payload = json.dumps(res)

                        if random.randint(1, 100) <= fallback_rate:
                            # Fallback to persistent storage
                            r.hset(self.persist_hash, key, payload)
                            lat = (time.perf_counter() - start) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(lat)
                                self.success += 1
                        else:
                            # Normal cache write
                            r.set(key, payload, ex=self.cache_ttl)
                            lat = (time.perf_counter() - start) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(lat)
                                self.success += 1

                            # Optional immediate read verification
                            if random.randint(1, 100) <= do_get_pct:
                                gstart = time.perf_counter()
                                _ = r.get(key)
                                glat = (time.perf_counter() - gstart) * 1000.0
                                with self.lock:
                                    self.latencies_ms.append(glat)
                                    self.success += 1
                    else:
                        # Read path
                        camera_id = f"cam-{random.randint(1, self.camera_count)}"
                        key = f"frame-{random.randint(1000000, 9999999)}"
                        _ = r.get(key)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1

                except Exception as e:
                    with self.lock:
                        self.fail += 1
                    time.sleep(0.01)

                # Update last operation time for rate control
                last_operation_time = current_time

            else:
                # Brief yield to prevent CPU spinning when rate limiting
                time.sleep(min(0.005, min_inter_operation_delay - time_since_last_op))

            # Periodic monitoring output
            self._periodic_monitoring(duration)

    def run(self, threads: int = 16, duration: int = 120, write_pct: int = 90,
           fallback_rate: int = 0, do_get_pct: int = 5):
        """Start benchmark test"""
        pool = self._init_connection_pool()

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
                                   fallback_rate, do_get_pct, pool, threads), daemon=True)
                t.start()
                tlist.append(t)

            logger.info("Started %d threads for %ds [%dx%d @ %dfps] (model=%s, analysis=%s)",
                       threads, duration, self.frame_width, self.frame_height, self.framerate,
                       self.inference_model, self.analysis_intensity)
            logger.info("Expected payload: %.1fKB/frame, frame rate control: real-time processing",
                       self.effective_payload_size_kb)
            logger.info("Estimated throughput: ~%.1f fps total across all cameras",
                       (threads * self.framerate * self.camera_count) / self.camera_count)

            # Wait for threads to finish
            for t in tlist:
                # Give threads enough time to finish gracefully (more time for longer tests)
                t.join(timeout=max(duration + 20, 60))  # At least 60 seconds timeout
                if t.is_alive():
                    logger.warning("Worker thread %s is still alive, continuing with cleanup", t.name)
                    # Note: daemon threads will be automatically terminated when main process exits

            logger.info("Workers finished")
            self._print_summary(duration)

        except KeyboardInterrupt:
            logger.info("Interrupted by user, setting stop event...")
            self._stop.set()
            # Give threads a moment to process the stop event
            for t in tlist:
                t.join(timeout=10.0)  # Longer timeout for graceful shutdown

    def _print_summary(self, duration: int):
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)

        logger.info("=== Realistic Video Cache Benchmark Results ===")
        logger.info("Total operations: %d (success=%d, fail=%d)",
                   total, self.success, self.fail)
        logger.info("Throughput: %.2f ops/sec", ops_per_sec)

        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[min(int(len(lat) * p / 100), len(lat)-1)]
            logger.info("Latency (ms) - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                       statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

        logger.info("Realistic Configuration:")
        logger.info("  - Frame Size: %dx%d", self.frame_width, self.frame_height)
        logger.info("  - Framerate: %dfps", self.framerate)
        logger.info("  - Analysis Intensity: %s (%.2fx multiplier)", self.analysis_intensity, self.intensity_multiplier)
        logger.info("  - Calculated Payload: %.1fKB per frame", self.effective_payload_size_kb)
        logger.info("  - Cameras: %d, Objects/Frame: ~%d, Threads: 16",
                   self.camera_count, self.objects_per_frame)

def main():
    parser = argparse.ArgumentParser(description="Realistic Video Cache Redis Benchmark with Frame Size Calculations",
                                   formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # Realistic frame parameters
    parser.add_argument("--frame-width", default=1920, type=int,
                       help="Video frame width (default: 1920 for HD)")
    parser.add_argument("--frame-height", default=1080, type=int,
                       help="Video frame height (default: 1080 for HD)")
    parser.add_argument("--framerate", default=30, type=int,
                       help="Target frame rate in fps (default: 30)")
    parser.add_argument("--analysis-intensity", default="comprehensive",
                       choices=["mechanical", "objective", "comprehensive"],
                       help="AI analysis complexity (default: comprehensive)")

    # Data scaling based on realistic parameters
    parser.add_argument("--objects-per-frame", default=8, type=int,
                       help="Number of objects per frame (default: 8 for realistic load)")

    # Data authenticity
    parser.add_argument("--camera-count", default=20, type=int,
                       help="Number of cameras in simulation (default: 100)")
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

    # Enhanced load parameters
    parser.add_argument("--threads", default=16, type=int, help="Worker threads (default: 16)")
    parser.add_argument("--duration", default=120, type=int, help="Test duration in seconds (default: 120s)")
    parser.add_argument("--write-pct", default=90, type=int, help="Write operation percentage")
    parser.add_argument("--fallback-rate", default=0, type=int,
                       help="Fallback to persistence percentage")
    parser.add_argument("--do-get-pct", default=5, type=int,
                       help="Immediate get after set percentage")

    args = parser.parse_args()

    # Enhanced default pool size for higher concurrency
    if args.pool_size is None:
        args.pool_size = 4 * 4  # Default 16

    bench = VideoCacheRealisticBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        cache_ttl=args.ttl,
        persist_hash=args.persist_hash,
        # Realistic frame parameters
        frame_width=args.frame_width,
        frame_height=args.frame_height,
        framerate=args.framerate,
        analysis_intensity=args.analysis_intensity,
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

    bench.run(threads=args.threads, duration=args.duration,
             write_pct=args.write_pct, fallback_rate=args.fallback_rate,
             do_get_pct=args.do_get_pct)

if __name__ == "__main__":
    main()