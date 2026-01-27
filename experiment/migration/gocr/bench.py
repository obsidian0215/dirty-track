#!/usr/bin/env python3
"""
Standardized GOCR bench supporting duration/rps/threads and dataset resolution.
"""
import argparse
import csv
import os
import random
import statistics
import threading
import time

try:
    import requests
except Exception:
    raise

# dynamic import of bench_common if available
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
        _cur = os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

parser = argparse.ArgumentParser()
if bench_common:
    bench_common.add_common_args(parser)
else:
    parser.add_argument('--duration', type=int, default=60)
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=4)
    parser.add_argument('--rps', '--qps', dest='rps', type=int, default=0)
    parser.add_argument('--dataset', default=None, help='Path to image dataset directory')
parser.add_argument('--url', required=True, help='Server base URL (e.g., http://127.0.0.1:8080)')
parser.add_argument('--requests', '--iters', dest='requests', type=int, default=100, help='Total requests when not using --duration')
parser.add_argument('--out', default='bench_gocr.csv')
args = parser.parse_args()

if bench_common:
    args.dataset = bench_common.get_dataset_path(args)
    bench_common.configure_logging()
else:
    if not args.dataset:
        args.dataset = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))

# prepare inputs
inputs = []
if args.dataset and os.path.isdir(args.dataset):
    for fn in os.listdir(args.dataset):
        if fn.lower().endswith(('.png', '.jpg', '.jpeg')):
            with open(os.path.join(args.dataset, fn), 'rb') as f:
                inputs.append(f.read())
if not inputs:
    for i in range(20):
        # generate a tiny synthetic png via a minimal PNG header placeholder
        inputs.append(b'\x89PNG\r\n\x1a\n' + bytes(str(i), 'utf-8'))

results = []
results_lock = threading.Lock()

class RateLimiter:
    def __init__(self, rps):
        self.rps = int(rps) if rps else 0
        if self.rps > 0:
            self.capacity = float(self.rps)
            self.tokens = float(self.rps)
            self.last = time.monotonic()
            self.lock = threading.Lock()
    def acquire(self):
        if not self.rps:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    refill = elapsed * self.capacity
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                deficit = 1.0 - self.tokens
                wait_time = deficit / self.capacity if self.capacity > 0 else 0.01
            time.sleep(wait_time)

rl = RateLimiter(getattr(args, 'rps', 0))


def worker_duration(url, end_time):
    sess = requests.Session()
    while time.time() < end_time:
        rl.acquire()
        payload = random.choice(inputs)
        start = time.monotonic()
        try:
            files = {'file': ('img.png', payload, 'image/png')}
            r = sess.post(url.rstrip('/') + '/ocr', files=files, timeout=30)
            status = r.status_code
        except Exception as e:
            status = None
        latency = (time.monotonic() - start) * 1000.0
        with results_lock:
            results.append((time.time(), status, latency))


def worker_requests(url, total_requests):
    sess = requests.Session()
    for _ in range(total_requests):
        payload = random.choice(inputs)
        start = time.monotonic()
        try:
            files = {'file': ('img.png', payload, 'image/png')}
            r = sess.post(url.rstrip('/') + '/ocr', files=files, timeout=30)
            status = r.status_code
        except Exception as e:
            status = None
        latency = (time.monotonic() - start) * 1000.0
        with results_lock:
            results.append((time.time(), status, latency))

# Run
# Determine effective number of threads (support both --threads and --concurrency)
nthreads = getattr(args, 'threads', None) or getattr(args, 'concurrency', 1)
if getattr(args, 'duration', 0) and args.duration > 0:
    end_time = time.time() + args.duration
    threads = []
    for _ in range(max(1, int(nthreads))):
        t = threading.Thread(target=worker_duration, args=(args.url, end_time))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
else:
    # fallback to request count
    concurrency = getattr(args, 'concurrency', None) or getattr(args, 'threads', 1)
    per_thread = max(1, args.requests // max(1, int(concurrency)))
    threads = []
    for _ in range(max(1, int(concurrency))):
        t = threading.Thread(target=worker_requests, args=(args.url, per_thread))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

# Write CSV
outf = args.out
with open(outf, 'w', newline='') as csvf:
    writer = csv.writer(csvf)
    writer.writerow(['ts', 'status', 'ms'])
    with results_lock:
        for r in results:
            writer.writerow(r)
print('Wrote', outf)
