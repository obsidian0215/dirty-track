#!/usr/bin/env python3
import argparse
import concurrent.futures
import os
import random
import statistics
import sys
import time

try:
    import requests
except Exception:
    print("Please install requests: pip3 install requests", file=sys.stderr)
    raise


def gen_payload(size=65536):
    return os.urandom(size)


def worker(session, url, payload):
    start = time.monotonic()
    try:
        r = session.post(url.rstrip("/") + "/compress", data=payload, timeout=30)
        status = r.status_code
    except Exception:
        return None, None, None
    latency = (time.monotonic() - start) * 1000.0
    orig = len(payload)
    comp = int(r.headers.get('X-Compressed-Size', 0)) if r.headers.get('X-Compressed-Size') else None
    return status, latency, comp


def run_load(server, concurrency, total_requests, dataset_dir=None, size=65536):
    url = server
    sess = requests.Session()
    inputs = []
    if dataset_dir and os.path.isdir(dataset_dir):
        for fn in os.listdir(dataset_dir):
            path = os.path.join(dataset_dir, fn)
            with open(path, 'rb') as f:
                inputs.append(f.read())
    if not inputs:
        for i in range(20):
            inputs.append(gen_payload(size=size))

    latencies = []
    comps = []
    success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        for i in range(total_requests):
            payload = random.choice(inputs)
            futures.append(ex.submit(worker, sess, url, payload))
        for fut in concurrent.futures.as_completed(futures):
            status, lat, comp = fut.result()
            if status and (200 <= status < 300):
                success += 1
                if lat:
                    latencies.append(lat)
                if comp is not None:
                    comps.append(comp)

    ops = len(latencies)
    duration = sum(latencies) / 1000.0 if ops else 0.0
    avg = statistics.mean(latencies) if latencies else 0.0
    p50 = statistics.median(latencies) if latencies else 0.0
    p95 = (sorted(latencies)[int(len(latencies) * 0.95) - 1] if latencies and len(latencies) >= 1 else 0.0)
    ops_per_sec = ops / duration if duration > 0 else 0.0
    avg_comp = statistics.mean(comps) if comps else 0

    print("METRIC_HEADER\tavg_latency_ms\tp50_ms\tp95_ms\tops_per_sec\ttotal_success\tavg_comp_size")
    print(f"METRIC_VALUES\t{avg:.3f}\t{p50:.3f}\t{p95:.3f}\t{ops_per_sec:.3f}\t{success}\t{avg_comp}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://127.0.0.1:8080", help="Server base URL")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--dataset", default=None, help="Path to binary payloads directory")
    parser.add_argument("--size", type=int, default=65536)
    parser.add_argument("--monitor-container", default=None, help="Optional container name to monitor during the run")
    parser.add_argument("--monitor-interval", type=float, default=1.0)
    args = parser.parse_args()

    mon_proc = None
    try:
        if args.monitor_container:
            import subprocess, signal
            mon_proc = subprocess.Popen([
                "python3",
                "/runc/dirty-track/mig-scripts/monitor_tool.py",
                "--container",
                args.monitor_container,
                "--interval",
                str(args.monitor_interval),
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            time.sleep(1.0)

        run_load(args.server, args.concurrency, args.requests, args.dataset, args.size)

    finally:
        if mon_proc:
            try:
                mon_proc.send_signal(signal.SIGINT)
                out, err = mon_proc.communicate(timeout=10)
                for ln in out.splitlines():
                    if ln.startswith("METRIC_PARAM"):
                        print(ln)
            except Exception:
                pass
