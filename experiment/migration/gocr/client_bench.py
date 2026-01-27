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

from io import BytesIO
from PIL import Image, ImageDraw, ImageFont


def gen_image_bytes(text: str, width=320, height=80):
    im = Image.new("RGB", (width, height), color=(255, 255, 255))
    d = ImageDraw.Draw(im)
    try:
        f = ImageFont.load_default()
        d.text((10, 10), text, font=f, fill=(0, 0, 0))
    except Exception:
        d.text((10, 10), text, fill=(0, 0, 0))
    buf = BytesIO()
    im.save(buf, format="PNG")
    return buf.getvalue()


def worker(session, url, payload_bytes):
    start = time.monotonic()
    try:
        files = {"file": ("img.png", payload_bytes, "image/png")}
        r = session.post(url.rstrip("/") + "/ocr", files=files, timeout=30)
        status = r.status_code
    except Exception as e:
        return None, None
    latency = (time.monotonic() - start) * 1000.0
    return status, latency


def run_load(server, concurrency, total_requests, dataset_dir=None):
    url = server
    sess = requests.Session()
    inputs = []
    if dataset_dir and os.path.isdir(dataset_dir):
        for fn in os.listdir(dataset_dir):
            if fn.lower().endswith((".png", ".jpg", ".jpeg")):
                with open(os.path.join(dataset_dir, fn), "rb") as f:
                    inputs.append(f.read())
    if not inputs:
        # generate a few synthetic images
        for i in range(20):
            inputs.append(gen_image_bytes(f"SAMPLE {i}"))

    latencies = []
    success = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = []
        for i in range(total_requests):
            payload = random.choice(inputs)
            futures.append(ex.submit(worker, sess, url, payload))
        for fut in concurrent.futures.as_completed(futures):
            status, lat = fut.result()
            if status and (200 <= status < 300):
                success += 1
                latencies.append(lat)

    ops = len(latencies)
    duration = sum(latencies) / 1000.0 if ops else 0.0
    avg = statistics.mean(latencies) if latencies else 0.0
    p50 = statistics.median(latencies) if latencies else 0.0
    p95 = (sorted(latencies)[int(len(latencies) * 0.95) - 1] if latencies and len(latencies) >= 1 else 0.0)
    ops_per_sec = ops / duration if duration > 0 else 0.0

    print("METRIC_HEADER\tavg_latency_ms\tp50_ms\tp95_ms\tops_per_sec\ttotal_success")
    print(f"METRIC_VALUES\t{avg:.3f}\t{p50:.3f}\t{p95:.3f}\t{ops_per_sec:.3f}\t{success}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://127.0.0.1:8080", help="Server base URL")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--dataset", default=None, help="Path to image dataset directory")
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
            # give monitor a moment to warm up
            time.sleep(1.0)

        run_load(args.server, args.concurrency, args.requests, args.dataset)

    finally:
        if mon_proc:
            try:
                mon_proc.send_signal(signal.SIGINT)
                out, err = mon_proc.communicate(timeout=10)
                # forward METRIC_PARAM lines from monitor
                for ln in out.splitlines():
                    if ln.startswith("METRIC_PARAM"):
                        print(ln)
            except Exception:
                pass

