#!/usr/bin/env python3
# coding: utf-8
"""
bench_video_cache_redis.py
Benchmark Redis as the primary cache for VideoAnalyticsCache scenario.

行为：
- 主操作为 Redis SET key value EX ttl （模拟缓存写入）
- 可选混合 GET 操作模拟缓存读取
- fallback_rate 表示按百分比概率走“回落写入”（示例: 写入到另一个 Redis hash 作为持久化）
- 记录延迟分布与成功/失败统计

用法示例：
  python3 bench_video_cache_redis.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30 --write-pct 80 --ttl 60 --fallback-rate 5
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

class VideoCacheRedisBench:
    """
    Redis-based video cache benchmark.
    - redis_host/redis_port: Redis server
    - cache_ttl: TTL for SET (seconds)
    - persist_hash: Redis hash key for fallback/persistence
    """
    def __init__(self, redis_host: str = "127.0.0.1", redis_port: int = 6379,
                 cache_ttl: int = 60, persist_hash: str = "video_inference_persist",
                 # 数据规模扩展
                 payload_size_kb: int = 2, objects_per_frame: int = 3,
                 # 数据类型真实性配置
                 camera_count: int = 10, inference_model: str = "yolov5_medium",
                 # 连接超时配置
                 connect_timeout: int = 5, socket_timeout: int = 5,
                 pool_timeout: int = 10, pool_size: Optional[int] = None):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.cache_ttl = int(cache_ttl)
        self.persist_hash = persist_hash

        # 数据规模扩展配置
        self.payload_size_kb = payload_size_kb
        self.objects_per_frame = objects_per_frame

        # 数据类型真实性配置
        self.camera_count = camera_count
        self.inference_model = inference_model  # yolov5_small/medium/ssd_mobile
        self.camera_positions: Dict[str, Dict[str, float]] = {}  # 摄像头位置跟踪

        # 连接超时配置
        self.connect_timeout = connect_timeout
        self.socket_timeout = socket_timeout
        self.pool_timeout = pool_timeout
        self.pool_size = pool_size

        # 线程控制
        self._stop = threading.Event()

        # 初始化连接池
        self.connection_pool = None

        # 周期性监控配置
        self.monitor_interval = 1.0
        self.last_report_time = 0
        self.last_success_count = 0

        # 统计
        self.latencies_ms: List[float] = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

        # 初始化摄像头位置
        self._init_camera_positions()

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
            # 随机分布在城市区域内
            lat_offset = (random.random() - 0.5) * 0.02  # ±10km
            lng_offset = (random.random() - 0.5) * 0.02

            self.camera_positions[f"cam-{i+1}"] = {
                "lat": base_lat + lat_offset,
                "lng": base_lng + lng_offset
            }

    def _make_result(self) -> dict:
        """生成模拟推理结果"""
        return {
            "frame_id": f"frame-{random.randint(100000, 999999)}",
            "timestamp": int(time.time() * 1000),
            "objects": [{"class": "person", "score": round(random.random(), 2)}],
        }

    def _worker(self, duration: int, write_pct: int, fallback_rate: int, do_get_pct: int, conn_kwargs):
        """
        单线程工作函数
        - duration: 运行时长（秒）
        - write_pct: 百分比概率做写操作（否则做读操作）
        - fallback_rate: 写操作时按概率回落到持久化（写入 persist_hash）
        - do_get_pct: 在写路径中，额外按此概率执行一次 GET 来测读写混合影响
        """
        r = redis.Redis(**conn_kwargs)
        end_time = time.time() + duration
        while time.time() < end_time and not self._stop.is_set():
            op_rand = random.randint(1, 100)
            try:
                if op_rand <= write_pct:
                    # 写路径
                    res = self._make_result()
                    key = res["frame_id"]
                    payload = json.dumps(res)
                    start = time.perf_counter()
                    # 模拟回落条件：按概率直接写入持久化（而不是 SET to cache）
                    if random.randint(1, 100) <= fallback_rate:
                        # fallback 写入到 persist_hash（HSET）
                        r.hset(self.persist_hash, key, payload)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1
                    else:
                        # 正常缓存写入：SET key value EX ttl
                        r.set(key, payload, ex=self.cache_ttl)
                        lat = (time.perf_counter() - start) * 1000.0
                        with self.lock:
                            self.latencies_ms.append(lat)
                            self.success += 1
                        # 可选在写后进行一次 GET（模拟紧接的读取）
                        if random.randint(1, 100) <= do_get_pct:
                            gstart = time.perf_counter()
                            _ = r.get(key)
                            glat = (time.perf_counter() - gstart) * 1000.0
                            with self.lock:
                                self.latencies_ms.append(glat)
                                # treat GET as success
                                self.success += 1
                else:
                    # 读路径：随机读取一个可能存在的 key（这里以新生成的 frame id 很可能 miss）
                    # 为模拟更真实的读负载，可改为从一个已知 key 集中取随机 key
                    key = f"frame-{random.randint(100000, 999999)}"
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
                # 发生错误时短暂退避
                time.sleep(0.01)

            # 周期性监控输出
            self._periodic_monitoring(duration)

    def _periodic_monitoring(self, total_duration: int):
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

    def run(self, threads: int = 4, duration: int = 10, write_pct: int = 80, fallback_rate: int = 5, do_get_pct: int = 0):
        """
        启动多个线程运行 benchmark
        - threads: 并发线程数
        - duration: 场景总时长（秒）
        - write_pct: 百分比做写操作（0-100）
        - fallback_rate: 写操作中回落到 persist 的概率（0-100）
        - do_get_pct: 写操作后触发一次 GET 的概率（0-100）
        """
        # 记录测试开始时间，用于计算精确的elapsed时间
        start_time = time.time()

        # 初始化监控参数
        self.last_report_time = start_time
        self.last_success_count = 0
        self.start_time = start_time

        conn_kwargs = {"host": self.redis_host, "port": self.redis_port, "decode_responses": False}
        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, write_pct, fallback_rate, do_get_pct, conn_kwargs), daemon=True)
            t.start()
            tlist.append(t)
        logger.info("Started %d threads for %ds (write_pct=%d fallback_rate=%d do_get_pct=%d)", threads, duration, write_pct, fallback_rate, do_get_pct)
        for t in tlist:
            t.join()
        self._print_summary(duration)

    def _print_summary(self, duration: int):
        total = self.success + self.fail
        ops_per_sec = self.success / max(1e-9, duration)
        logger.info("Total ops: %d success=%d fail=%d ops/sec=%.2f", total, self.success, self.fail, ops_per_sec)
        if self.latencies_ms:
            lat = sorted(self.latencies_ms)
            def pct(p): return lat[min(int(len(lat) * p / 100), len(lat)-1)]
            logger.info("Latency ms - avg=%.3f p50=%.3f p90=%.3f p99=%.3f max=%.3f",
                        statistics.mean(lat), pct(50), pct(90), pct(99), lat[-1])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host", default="127.0.0.1", help="Redis host")
    parser.add_argument("--redis-port", default=6379, type=int, help="Redis port")
    parser.add_argument("--threads", default=4, type=int, help="Number of worker threads")
    parser.add_argument("--duration", default=10, type=int, help="Benchmark duration in seconds")
    parser.add_argument("--write-pct", default=80, type=int, help="Percent of operations that are writes (SET)")
    parser.add_argument("--fallback-rate", default=5, type=int, help="Percent chance to fallback to persistent HSET instead of SET")
    parser.add_argument("--ttl", dest="ttl", default=60, type=int, help="TTL for cache SET (seconds)")
    parser.add_argument("--do-get-pct", default=0, type=int, help="When writing, percent chance to immediately GET the key (to simulate read-after-write)")
    args = parser.parse_args()

    bench = VideoCacheRedisBench(redis_host=args.redis_host, redis_port=args.redis_port,
                                 cache_ttl=args.ttl, persist_hash="video_inference_persist")
    bench.run(threads=args.threads, duration=args.duration, write_pct=args.write_pct,
              fallback_rate=args.fallback_rate, do_get_pct=args.do_get_pct)

if __name__ == "__main__":
    main()