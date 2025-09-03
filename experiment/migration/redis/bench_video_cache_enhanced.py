#!/usr/bin/env python3
# coding: utf-8
"""
bench_video_cache_enhanced.py (完整增强版本)

完整的视频缓存基准测试工具，包含：
🔧 数据规模扩展: 动态负载大小、物体数量控制
🎯 数据类型真实性: 摄像头地理分布、推理模型特性、多类别物体检测
⚡ 连接超时配置: 连接池管理、重试机制、超时控制
📊 周期性监控输出: 实时TPS和延迟统计

EXTENDED USAGE:
  --payload-size-kb: 目标负载大小 (默认2KB)
  --objects-per-frame: 每帧物体数量 (默认3)
  --camera-count: 摄像头数量 (默认10)
  --inference-model: 推理模型类型 (yolov5_small/medium/ssd_mobile)
  --connect-timeout: 连接超时秒数 (默认5)
  --pool-size: 连接池大小 (默认CPU核心数*4)
"""
import argparse
import json
import logging
import random
import threading
import time
import statistics
import sys
from typing import List, Optional, Dict, Any
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

class VideoCacheEnhancedBench:
    """增强版视频缓存基准测试"""

    def __init__(self, redis_host: str = "127.0.0.1", redis_port: int = 6379,
                 cache_ttl: int = 60, persist_hash: str = "video_inference_persist",
                 # 数据规模扩展
                 payload_size_kb: int = 2, objects_per_frame: int = 3,
                 # 数据类型真实性配置
                 camera_count: int = 10, inference_model: str = "yolov5_medium",
                 # 连接超时配置
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None):

        # 基础配置
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.cache_ttl = cache_ttl
        self.persist_hash = persist_hash

        # 数据规模扩展配置
        self.payload_size_kb = payload_size_kb
        self.objects_per_frame = objects_per_frame

        # 数据类型真实性配置
        self.camera_count = camera_count
        self.inference_model = inference_model
        self.camera_positions: Dict[str, Dict[str, float]] = {}
        self.object_distribution = self._get_realistic_object_distribution()

        # 连接配置
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size or (4 * 4)  # 默认16

        # 初始化组件
        self.connection_pool = None
        self._stop = threading.Event()

        # 监控配置
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # 初始化摄像头位置
        self._init_camera_positions()

        # 统计
        self.latencies_ms: List[float] = []
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

    def _init_camera_positions(self):
        """初始化摄像头地理位置用于真实性模拟"""
        base_lat, base_lng = 31.0, 121.0  # 上海为中心

        for i in range(self.camera_count):
            # 城市区域内随机分布摄像头
            lat_offset = (random.random() - 0.5) * 0.03  # ±15km
            lng_offset = (random.random() - 0.5) * 0.03

            self.camera_positions[f"cam-{i+1}"] = {
                "lat": base_lat + lat_offset,
                "lng": base_lng + lng_offset
            }

    def _get_realistic_object_distribution(self) -> List[tuple]:
        """获取真实的城市物贵州顒物检测概率分布"""
        return [
            ("person", 0.6),      # 人最常见
            ("car", 0.5),         # 汽车也很常见
            ("truck", 0.3),       # 卡车较少
            ("bus", 0.1),         # 公交车不常见
            ("motorcycle", 0.2),  # 摩托车中等
            ("bicycle", 0.4),     # 自行车常见
            ("traffic_light", 0.05),  # 交通灯少见
            ("stop_sign", 0.02),  # 停止标志更少
            ("dog", 0.003),       # 狗很少
            ("cat", 0.001)        # 猫很罕见
        ]

    def _generate_inference_objects(self) -> List[Dict[str, Any]]:
        """根据真实概率分布生成物体检测结果"""
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
        """生成增强的视频推理结果"""
        # 选择随机摄像头
        camera_id = f"cam-{random.randint(1, self.camera_count)}"
        camera_pos = self.camera_positions[camera_id]

        # 生成基础推理结果
        frame_id = f"frame-{random.randint(1000000, 9999999)}"
        timestamp = int(time.time() * 1000)

        # 生成物体检测结果
        objects = self._generate_inference_objects()

        # 计算推理时间（基于模型类型）
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
                "motion_blur": random.choice([0, 0, 0, 1])  # 大部分帧无运动模糊
            }
        }

        # 数据规模扩展 - 添加额外视频分析数据
        current_size = len(json.dumps(result))
        target_size_bytes = self.payload_size_kb * 1024

        if current_size < target_size_bytes:
            # 添加颗粒度分析结果
            analysis_types = ["pose_estimation", "anomaly_detection", "scene_classification", "behavior_analysis"]
            additional_analysis = {}

            while len(json.dumps({**result, "detailed_analysis": additional_analysis})) < target_size_bytes:
                analysis_type = random.choice(analysis_types)
                if analysis_type == "pose_estimation":
                    keypoint_count = 17  # COCO格式关键点数
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
        """周期性输出Redis处理吞吐量和延迟"""
        current_time = time.time()
        if current_time - self.last_report_time >= self.monitor_interval:
            success_count = self.success
            new_operations = success_count - self.last_success_count

            if self.last_report_time > 0 and new_operations >= 0:
                throughput_ops_sec = new_operations / (current_time - self.last_report_time)

                # 计算延迟统计
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

    def _worker(self, duration: float, write_pct: int, fallback_rate: int,
               do_get_pct: int, pool):
        """工作线程，包含所有的读写逻辑"""
        r = redis.Redis(connection_pool=pool, decode_responses=True)
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            op_rand = random.randint(1, 100)
            start = time.perf_counter()

            try:
                if op_rand <= write_pct:
                    # 写路径
                    res = self._make_result()
                    key = res["frame_id"]
                    payload = json.dumps(res)

                    if random.randint(1, 100) <= fallback_rate:
                        # 回退到持久化存储
                        r.hset(self.persist_hash, key, payload)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1
                    else:
                        # 正常缓存写入
                        r.set(key, payload, ex=self.cache_ttl)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1

                        # 可选立即读验证
                        if random.randint(1, 100) <= do_get_pct:
                            gstart = time.perf_counter()
                            _ = r.get(key)
                            glat = (time.perf_counter() - gstart) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(glat)
                                self.success += 1
                else:
                    # 读路径
                    camera_id = f"cam-{random.randint(1, self.camera_count)}"
                    key = f"frame-{random.randint(1000000, 9999999)}"
                    start = time.perf_counter()
                    _ = r.get(key)
                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1

            except Exception as e:
                logger.debug("operation failed: %s", e)
                with self.lock:
                    self.fail += 1
                time.sleep(0.01)

            # 周期性监控输出
            self._periodic_monitoring(duration)

    def run(self, threads: int = 4, duration: int = 10, write_pct: int = 80,
           fallback_rate: int = 5, do_get_pct: int = 0):
        """启动基准测试"""
        pool = self._init_connection_pool()
        tlist = []

        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, write_pct,
                               fallback_rate, do_get_pct, pool), daemon=True)
            t.start()
            tlist.append(t)

        logger.info("Started %d threads for %ds (model=%s, cameras=%d, payload_kb=%d)",
                   threads, duration, self.inference_model, self.camera_count, self.payload_size_kb)

        for t in tlist:
            t.join()

        logger.info("Workers finished")
        self._print_summary(duration)

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

        logger.info(f"Configuration: {self.objects_per_frame} objects/frame, "
                   f"{self.camera_count} cameras, {self.inference_model} model")

def main():
    parser = argparse.ArgumentParser(description="Enhanced Video Cache Redis Benchmark",
                                   formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")

    # 数据规模扩展
    parser.add_argument("--payload-size-kb", default=2, type=int,
                       help="Target payload size in KB (default: 2)")
    parser.add_argument("--objects-per-frame", default=3, type=int,
                       help="Number of objects per frame (default: 3)")

    # 数据类型真实性
    parser.add_argument("--camera-count", default=10, type=int,
                       help="Number of cameras in simulation (default: 10)")
    parser.add_argument("--inference-model", default="yolov5_medium",
                       choices=["yolov5_small", "yolov5_medium", "ssd_mobile"],
                       help="AI inference model type (default: yolov5_medium)")

    # 连接超时配置
    parser.add_argument("--connect-timeout", default=5, type=int,
                       help="Connection timeout in seconds (default: 5)")
    parser.add_argument("--socket-timeout", default=5, type=int,
                       help="Socket timeout in seconds (default: 5)")
    parser.add_argument("--pool-timeout", default=10, type=int,
                       help="Connection pool timeout in seconds (default: 10)")
    parser.add_argument("--pool-size", type=int,
                       help="Connection pool size (default: CPU cores * 4)")

    # 缓存参数
    parser.add_argument("--ttl", default=60, type=int, help="Cache TTL in seconds")
    parser.add_argument("--persist-hash", default="video_inference_persist",
                       help="Hash key for fallback persistence")

    # 负载参数
    parser.add_argument("--threads", default=4, type=int, help="Worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Test duration in seconds")
    parser.add_argument("--write-pct", default=80, type=int, help="Write operation percentage")
    parser.add_argument("--fallback-rate", default=5, type=int,
                       help="Fallback to persistence percentage")
    parser.add_argument("--do-get-pct", default=0, type=int,
                       help="Immediate get after set percentage")

    args = parser.parse_args()

    # 计算默认池大小
    if args.pool_size is None:
        args.pool_size = 4 * 4  # 默认16

    bench = VideoCacheEnhancedBench(
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        cache_ttl=args.ttl,
        persist_hash=args.persist_hash,
        # 数据规模扩展
        payload_size_kb=args.payload_size_kb,
        objects_per_frame=args.objects_per_frame,
        # 数据类型真实性
        camera_count=args.camera_count,
        inference_model=args.inference_model,
        # 连接超时配置
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