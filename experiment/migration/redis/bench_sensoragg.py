#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg.py
Benchmark for SensorAggregator scenario (ZADD writes + occasional ZRANGEBYSCORE reads)
Usage:
  python3 bench_sensoragg.py --redis-host 127.0.0.1 --redis-port 6379 --threads 8 --duration 30 --read-pct 10
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

class SensorAggBench:
    """传感器时间序列写入（sorted set），并支持按窗口查询的混合负载"""
    def __init__(self, redis_host: str, redis_port: int, set_key: str = "sensors:ts"):
        self.redis_host = redis_host
        self.redis_port = int(redis_port)
        self.set_key = set_key
        self._stop = threading.Event()
        self.latencies_ms = []
        self.success = 0
        self.fail = 0
        self.lock = threading.Lock()

    def _make_reading(self):
        return {
            "sensor_id": f"sen-{random.randint(1,50)}",
            "timestamp": int(time.time()*1000),
            "value": round(random.random()*100.0, 3)
        }

    def _worker(self, duration, read_pct, conn_kwargs):
        r = redis.Redis(**conn_kwargs, decode_responses=True)
        end_time = time.time() + duration
        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1,100) <= read_pct
            start = time.perf_counter()
            try:
                if do_read:
                    now = int(time.time()*1000)
                    _ = r.zrangebyscore(self.set_key, now-60000, now)
                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
                else:
                    rcd = self._make_reading()
                    r.zadd(self.set_key, {json.dumps(rcd): float(rcd["timestamp"])})
                    lat = (time.perf_counter() - start) * 1000.0
                    with self.lock:
                        self.latencies_ms.append(lat)
                        self.success += 1
            except Exception as e:
                logger.debug("op failed: %s", e)
                with self.lock:
                    self.fail += 1

    def run(self, threads: int = 4, duration: int = 10, read_pct: int = 5):
        conn_kwargs = {"host": self.redis_host, "port": self.redis_port}
        tlist = []
        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct, conn_kwargs), daemon=True)
            t.start()
            tlist.append(t)
        logger.info("Started %d threads for %ds (read_pct=%d)", threads, duration, read_pct)
        for t in tlist:
            t.join()
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
    parser.add_argument("--set-key", default="sensors:ts")
    parser.add_argument("--threads", default=4, type=int)
    parser.add_argument("--duration", default=10, type=int)
    parser.add_argument("--read-pct", default=10, type=int, help="Percent of operations that are reads (ZRANGEBYSCORE)")
    args = parser.parse_args()

    bench = SensorAggBench(redis_host=args.redis_host, redis_port=args.redis_port, set_key=args.set_key)
    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)

if __name__ == "__main__":
    main()