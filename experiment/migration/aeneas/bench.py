#!/usr/bin/env python3
"""
Standardized aeneas bench supporting duration/rps/threads and dataset resolution.
"""
import argparse
import csv
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

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
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=1)
    parser.add_argument('--rps', '--qps', dest='rps', type=int, default=0)
parser.add_argument('--url', required=True, help='Endpoint URL (e.g., http://localhost:8080/align)')
parser.add_argument('--audio', default=None, help='Audio file to upload (relative to --dataset if not absolute)')
parser.add_argument('--text', default=None, help='Text file to upload (relative to --dataset if not absolute)')
if not bench_common:
    parser.add_argument('--dataset', default=None, help='Path to datasets directory (default: repo datasets/)')
parser.add_argument('--iters', type=int, default=10, help='Fallback iterations when --duration is not used')
parser.add_argument('--out', default='bench_aeneas.csv')
args = parser.parse_args()

if bench_common:
    args.dataset = bench_common.get_dataset_path(args)
    bench_common.configure_logging()
else:
    if not args.dataset:
        args.dataset = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasets'))

# Resolve audio/text

def _resolve_path(p, subdir_hint=None):
    if p:
        if os.path.isabs(p) and os.path.exists(p):
            return p
        candidate = os.path.join(args.dataset, p)
        if os.path.exists(candidate):
            return candidate
        for root, _, files in os.walk(args.dataset):
            if p in files:
                return os.path.join(root, p)
        raise SystemExit(f'file not found: {p} (dataset: {args.dataset})')
    else:
        if not os.path.isdir(args.dataset):
            raise SystemExit(f'dataset dir not found: {args.dataset}')
        candidates = []
        for root, _, files in os.walk(args.dataset):
            for f in files:
                if subdir_hint and subdir_hint not in f and subdir_hint is not None:
                    continue
                candidates.append(os.path.join(root, f))
        if not candidates:
            raise SystemExit(f'no files found in dataset dir: {args.dataset}')
        return random.choice(candidates)

args.audio = _resolve_path(args.audio, subdir_hint='audio')
args.text = _resolve_path(args.text, subdir_hint='ocr')

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


def do_request_once(idx=None):
    start = time.time()
    try:
        with open(args.audio, 'rb') as a, open(args.text, 'rb') as t:
            files = {'audio': (os.path.basename(args.audio), a), 'text': (os.path.basename(args.text), t)}
            r = requests.post(args.url, files=files, timeout=120)
            status = r.status_code
            preview = r.text[:200]
    except Exception as e:
        status = None
        preview = str(e)[:200]
    elapsed = time.time() - start
    with results_lock:
        results.append((idx if idx is not None else time.time(), status, elapsed, preview))


def worker_duration(url, end_time):
    sess = requests.Session()
    while time.time() < end_time:
        rl.acquire()
        start = time.time()
        try:
            with open(args.audio, 'rb') as a, open(args.text, 'rb') as t:
                files = {'audio': (os.path.basename(args.audio), a), 'text': (os.path.basename(args.text), t)}
                r = sess.post(args.url, files=files, timeout=120)
                status = r.status_code
                preview = r.text[:200]
        except Exception as e:
            status = None
            preview = str(e)[:200]
        elapsed = time.time() - start
        with results_lock:
            results.append((time.time(), status, elapsed, preview))

# Run
if getattr(args, 'duration', 0) and args.duration > 0:
    end_time = time.time() + args.duration
    threads = []
    for _ in range(max(1, args.threads)):
        t = threading.Thread(target=worker_duration, args=(args.url, end_time))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
else:
    # fallback to iters
    with ThreadPoolExecutor(max_workers=max(1, args.threads)) as ex:
        futures = [ex.submit(do_request_once, i) for i in range(args.iters)]
        for f in futures:
            try:
                f.result()
            except Exception:
                pass

# Write CSV
outf = args.out
with open(outf, 'w', newline='') as csvf:
    writer = csv.writer(csvf)
    writer.writerow(['iter_or_ts', 'status', 'secs', 'preview'])
    with results_lock:
        for r in results:
            writer.writerow(r)
print('Wrote', outf)
