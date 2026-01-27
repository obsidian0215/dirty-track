#!/usr/bin/env python3
"""
Yolo HTTP bench supporting --duration / --threads and dataset resolution.
"""
import argparse
import csv
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except Exception:
    raise

# dynamic bench_common import (searches up the tree for common/bench_common.py)
try:
    import importlib.util as _importlib_util, os as _os
    _cur = _os.path.abspath(_os.path.dirname(__file__))
    _bench_common = None
    for _ in range(6):
        _candidate = _os.path.join(_cur, 'common', 'bench_common.py')
        if _os.path.exists(_candidate):
            spec = _importlib_util.spec_from_file_location('bench_common', _candidate)
            _bench_common = _importlib.util.module_from_spec(spec)
            spec.loader.exec_module(_bench_common)
            break
        _cur = os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

IntervalMetrics = getattr(bench_common, 'IntervalMetrics', None) if bench_common else None

parser = argparse.ArgumentParser()
if bench_common:
    bench_common.add_common_args(parser)
else:
    parser.add_argument('--duration', type=int, default=0)
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=4)
    parser.add_argument('--rps', '--qps', dest='rps', type=int, default=0)
parser.add_argument('--url', required=False, help='Endpoint URL e.g. http://127.0.0.1:8080/detect')
parser.add_argument('--server', dest='url_alias', required=False, help='Alias for --url')
parser.add_argument('--file', default=None, help='Image file to send (relative to --dataset if not absolute)')
parser.add_argument('--files', default=None, help='Comma-separated list of images to cycle through (relative to --dataset if not absolute)')
parser.add_argument('--requests', type=int, default=50, help='Total requests when not using --duration')
if not bench_common:
    parser.add_argument('--dataset', default='/runc/datasets', help='Path to datasets directory (default: /runc/datasets)')
    parser.add_argument('--metrics-out', default=None, help='Output path for interval metrics (JSON)')
    parser.add_argument('--metrics-interval', type=float, default=1.0, help='Sampling interval seconds (default: 1.0)')
parser.add_argument('--out', default=None, help='Optional output CSV file to write summary')
args = parser.parse_args()

# Resolve server URL (accept --server or --url)
server = args.url or args.url_alias
if not server:
    raise SystemExit('server URL required, pass --url (or --server)')

# Resolve dataset
if bench_common:
    args.dataset = bench_common.get_dataset_path(args)
else:
    if not args.dataset:
        args.dataset = '/runc/datasets'

# Resolve files (support --files comma-separated, or --file single). Build rotating payloads.
file_paths = []
if args.files:
    for part in [p.strip() for p in args.files.split(',') if p.strip()]:
        if os.path.isabs(part) and os.path.exists(part):
            file_paths.append(part)
            continue
        candidate = os.path.join(args.dataset, part) if args.dataset else part
        if os.path.exists(candidate):
            file_paths.append(candidate)
            continue
        found = None
        if args.dataset and os.path.isdir(args.dataset):
            for root, _, files in os.walk(args.dataset):
                if part in files:
                    found = os.path.join(root, part)
                    break
        if found:
            file_paths.append(found)
        else:
            raise SystemExit(f'file not found from --files: {part}')
elif args.file:
    f = args.file
    if os.path.isabs(f) and os.path.exists(f):
        file_paths.append(f)
    else:
        candidate = os.path.join(args.dataset, f) if args.dataset else f
        if os.path.exists(candidate):
            file_paths.append(candidate)
        else:
            found = None
            if args.dataset and os.path.isdir(args.dataset):
                for root, _, files in os.walk(args.dataset):
                    if f in files:
                        found = os.path.join(root, f)
                        break
            if found:
                file_paths.append(found)
            else:
                raise SystemExit(f'file not found: {f}')
else:
    ds = args.dataset
    if not ds or not os.path.isdir(ds):
        raise SystemExit('dataset dir not found: %s' % ds)
    for root, _, files in os.walk(ds):
        for fn in files:
            if fn.lower().endswith(('.jpg', '.jpeg', '.png')):
                file_paths.append(os.path.join(root, fn))
    if not file_paths:
        raise SystemExit('no image files found in dataset dir: %s' % ds)

payloads = []
for p in file_paths:
    try:
        with open(p, 'rb') as fh:
            payloads.append((p, fh.read()))
    except Exception:
        raise SystemExit(f'failed to read file: {p}')

file_idx = 0
file_lock = threading.Lock()

def get_next_payload():
    global file_idx
    with file_lock:
        idx = file_idx
        file_idx = (file_idx + 1) % len(payloads)
    return payloads[idx][1]

# Rate limiter (optional)
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

metrics = None
if IntervalMetrics:
    metrics = IntervalMetrics(
        interval_sec=getattr(args, 'metrics_interval', 1.0),
        out_path=getattr(args, 'metrics_out', None),
        label='yolo',
    )
    try:
        bench_common.register_metrics_signal_handlers(metrics)
    except Exception:
        pass

results_lock = threading.Lock()
latencies = []
success = 0

sess = requests.Session()


def _is_success(status):
    return status is not None and 200 <= int(status) < 400


def do_one_request():
    start = time.time()
    try:
        payload = get_next_payload()
        r = sess.post(server, files={'file': ('img.jpg', payload)}, timeout=60)
        status = r.status_code
        preview = r.text[:200] if hasattr(r, 'text') else ''
    except Exception as e:
        status = None
        preview = str(e)[:200]
    elapsed = (time.time() - start) * 1000.0
    if metrics:
        metrics.record(_is_success(status), elapsed)
    with results_lock:
        if status and 200 <= status < 300:
            latencies.append(elapsed)
            global success
            success += 1


def worker_duration(end_time):
    while time.time() < end_time:
        rl.acquire()
        do_one_request()

# Run
if metrics:
    metrics.start()

if getattr(args, 'duration', 0) and args.duration > 0:
    end_time = time.time() + args.duration
    threads = []
    for _ in range(max(1, args.threads)):
        t = threading.Thread(target=worker_duration, args=(end_time,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
else:
    # fallback to request count
    concurrency = getattr(args, 'concurrency', None) or getattr(args, 'threads', 1)
    per_thread = max(1, args.requests // max(1, int(concurrency)))
    threads = []
    def worker_count(n):
        for _ in range(n):
            do_one_request()
    for _ in range(max(1, int(concurrency))):
        t = threading.Thread(target=worker_count, args=(per_thread,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

if metrics:
    metrics.stop()
    metrics.write()

ops = len(latencies)
avg = statistics.mean(latencies) if latencies else 0.0
p50 = statistics.median(latencies) if latencies else 0.0
p95 = (sorted(latencies)[int(len(latencies)*0.95)-1] if latencies and len(latencies)>=1 else 0.0)
duration_secs = sum(latencies)/1000.0 if ops else 0.0
ops_per_sec = ops/duration_secs if duration_secs>0 else 0.0

print("METRIC_HEADER\tavg_latency_ms\tp50_ms\tp95_ms\tops_per_sec\ttotal_success")
print(f"METRIC_VALUES\t{avg:.3f}\t{p50:.3f}\t{p95:.3f}\t{ops_per_sec:.3f}\t{success}")
if args.out:
    try:
        with open(args.out, 'w', newline='') as csvf:
            writer = csv.writer(csvf)
            writer.writerow(['avg_latency_ms','p50_ms','p95_ms','ops_per_sec','total_success'])
            writer.writerow([f"{avg:.3f}", f"{p50:.3f}", f"{p95:.3f}", f"{ops_per_sec:.3f}", success])
        print(f"Wrote {args.out}")
    except Exception as e:
        print(f"Failed to write out file {args.out}: {e}")
