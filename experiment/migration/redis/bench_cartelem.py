#!/usr/bin/env python3
# coding: utf-8
"""
bench_cartelem.py
Benchmark for CarTelematics scenario (writes to Redis Stream)
Usage:
  python3 bench_cartelem.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30
"""
import argparse
import json
import logging
import random
import threading
import time
import statistics
from typing import List
import redis

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)

class CarTelematicsBench:
    """车联网写入 Redis Stream 的负载发生器"""
    def __init__(self, redis_host: str, redis_port: int, stream_name: str = "vehicle:telemetry"):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.stream_name = stream_name
        self._stop = threading.Event()
        # 统计
        self.latencies_ms = []  # 全局收集（注意内存）
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

    def _make_payload(self):
        payload = {
            "vehicle_id": f"veh-{random.randint(1000,9999)}",
            "timestamp": int(time.time()*1000),
            "lat": round(31.0 + random.random()*0.1, 6),
            "lon": round(121.0 + random.random()*0.1, 6),
            "speed_kmh": round(random.random()*120, 2)
        }
        return payload

    def _worker(self, duration, conn_kwargs):
        r = redis.Redis(**conn_kwargs, decode_responses=True)
        end_time = time.time() + duration
        while time.time() < end_time and not self._stop.is_set():
            payload = self._make_payload()
            start = time.perf_counter()
            try:
                r.xadd(self.stream_name, {"data": json.dumps(payload)})
                lat = (time.perf_counter() - start) * 1000.0
                with self.lock:
                    self.latencies_ms.append(lat)
                    self.success += 1
            except Exception as e:
                logger.debug("xadd failed: %s", e)
                with self.lock:
                    self.fail += 1

    def run(self, threads: int = 4, duration: int = 10):
        conn_kwargs = {"host": self.redis_host, "port": self.redis_port}
        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, conn_kwargs), daemon=True)
            t.start()
            tlist.append(t)
        logger.info("Started %d threads for %ds", threads, duration)
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
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", default=6379, type=int)
    parser.add_argument("--stream", default="vehicle:telemetry")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--duration", default=10, type=int)
    args = parser.parse_args()

    bench = CarTelematicsBench(redis_host=args.redis_host, redis_port=args.redis_port, stream_name=args.stream)
    bench.run(threads=args.threads, duration=args.duration)

if __name__ == "__main__":
    main()