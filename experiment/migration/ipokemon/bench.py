#!/usr/bin/env python3
"""
Simple iPokeMon jmx runner wrapper. Requires `jmeter` binary available on PATH.
Supports --duration and --threads and produces a JTL output file.
"""
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--jmx', default='/runc/fog_workloads/ipokemon/rootfs/root/iPokeMon/ipokemon/Application/iPokeMon-Client/iPokeMon.jmx', help='Path to iPokeMon JMX file')
parser.add_argument('--jmeter', default='jmeter', help='Path to jmeter binary')
parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=50, help='JMeter thread count')
parser.add_argument('--host', default='127.0.0.1', help='Target host for JMeter (JMeter -JHOST)')
parser.add_argument('--port', type=int, default=8000, help='Target port for JMeter (JMeter -JPORT)')
parser.add_argument('--out', default=None, help='Output JTL file (defaults to ipokemon_<ts>.jtl)')
parser.add_argument('--metrics-out', default=None, help='Output path for interval metrics (JSON)')
parser.add_argument('--metrics-interval', type=float, default=1.0, help='Sampling interval seconds (default: 1.0)')
args = parser.parse_args()

# Dynamic bench_common import (for IntervalMetrics)
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
        _cur = _os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

IntervalMetrics = getattr(bench_common, 'IntervalMetrics', None) if bench_common else None

jmeter = shutil.which(args.jmeter)
if not jmeter:
    print('[error] jmeter binary not found on PATH. Please install JMeter or provide --jmeter path.', file=sys.stderr)
    sys.exit(2)

if not os.path.exists(args.jmx):
    print(f'[error] JMX file not found: {args.jmx}', file=sys.stderr)
    sys.exit(2)

if not args.out:
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    args.out = f'ipokemon_{ts}.jtl'

cmd = [jmeter, '-n', '-t', args.jmx, f'-JHOST={args.host}', f'-JPORT={int(args.port)}', f'-JDuration={int(args.duration)}', f'-JThreads={int(args.threads)}', '-Jjmeter.save.saveservice.output_format=csv', '-l', args.out]
print('[bench] running:', ' '.join(cmd))


def _parse_jtl_line(parts, hmap):
    if not parts:
        return None, None
    elapsed_idx = hmap.get('elapsed') if hmap else None
    success_idx = hmap.get('success') if hmap else None
    if elapsed_idx is None:
        elapsed_idx = 1 if len(parts) > 1 else None
    latency_ms = None
    if elapsed_idx is not None and elapsed_idx < len(parts):
        try:
            latency_ms = float(parts[elapsed_idx])
        except Exception:
            latency_ms = None
    success = True
    if success_idx is not None and success_idx < len(parts):
        token = str(parts[success_idx]).strip().lower()
        success = token in ('true', '1', 'yes')
    return success, latency_ms


def _tail_jtl_for_metrics(jtl_path, metrics, stop_event, poll_interval=0.2):
    header = None
    hmap = {}
    pos = 0
    while not stop_event.is_set():
        if not os.path.exists(jtl_path):
            time.sleep(poll_interval)
            continue
        try:
            with open(jtl_path, 'r', encoding='utf-8', errors='ignore') as fh:
                fh.seek(pos)
                while True:
                    line = fh.readline()
                    if not line:
                        pos = fh.tell()
                        break
                    line = line.strip()
                    if not line:
                        continue
                    if header is None:
                        candidate = [h.strip().lower() for h in line.split(',')]
                        if any('elapsed' in h for h in candidate) or any('timestamp' in h for h in candidate):
                            header = candidate
                            hmap = {h: i for i, h in enumerate(header)}
                            continue
                        # no header present, treat as data with fallback indices
                        header = []
                    parts = line.split(',')
                    success, latency_ms = _parse_jtl_line(parts, hmap)
                    if metrics and success is not None:
                        metrics.record(success, latency_ms)
        except Exception:
            pass
        time.sleep(poll_interval)


def _parse_jtl_and_print_metrics(jtl_path, duration):
    """Parse JMeter JTL (CSV or XML) and print a simple metric summary."""
    import csv
    import xml.etree.ElementTree as ET
    import math

    if not os.path.exists(jtl_path):
        print('[bench] jtl file not found, no metrics')
        return
    try:
        # Heuristic: XML starts with '<'
        with open(jtl_path, 'r', encoding='utf-8', errors='ignore') as fh:
            start = fh.read(1024)
        latencies = []
        successes = 0
        total = 0
        if start.lstrip().startswith('<'):
            # XML JTL
            for event, elem in ET.iterparse(jtl_path):
                tag = (elem.tag or '').lower()
                if 'sample' in tag:
                    total += 1
                    t = elem.attrib.get('t') or elem.attrib.get('time') or elem.attrib.get('elapsed')
                    s = elem.attrib.get('s') or elem.attrib.get('success')
                    try:
                        if t is not None:
                            latencies.append(int(float(t)))
                    except Exception:
                        pass
                    if s is None or str(s).lower() in ('true', '1', 'yes'):
                        successes += 1
                    elem.clear()
        else:
            # CSV JTL
            with open(jtl_path, 'r', encoding='utf-8', errors='ignore') as fh:
                reader = csv.reader(fh)
                # Peek header
                try:
                    header = next(reader)
                except StopIteration:
                    header = []
                hmap = {h.strip().lower(): i for i, h in enumerate(header)}
                # Try to find elapsed and success columns
                elapsed_idx = hmap.get('elapsed') if hmap else None
                success_idx = hmap.get('success') if hmap else None
                # fallback: assume elapsed at index 1
                if elapsed_idx is None:
                    elapsed_idx = 1
                for row in reader:
                    if not row:
                        continue
                    total += 1
                    try:
                        val = row[elapsed_idx]
                        latencies.append(int(float(val)))
                    except Exception:
                        pass
                    if success_idx is not None:
                        try:
                            s = row[success_idx]
                            if str(s).lower() in ('true', '1', 'yes'):
                                successes += 1
                        except Exception:
                            pass
                if success_idx is None:
                    successes = total if total > 0 else 0

        # compute stats
        if latencies:
            lat_sorted = sorted(latencies)
            avg_lat = sum(lat_sorted) / len(lat_sorted)
            def pct(p):
                if not lat_sorted:
                    return 0
                idx = int(math.ceil((p / 100.0) * len(lat_sorted))) - 1
                idx = max(0, min(idx, len(lat_sorted) - 1))
                return lat_sorted[idx]
            p50 = pct(50)
            p95 = pct(95)
        else:
            avg_lat = p50 = p95 = 0
        ops_per_sec = successes / max(1, int(duration))
        print('METRIC_HEADER\tavg_latency_ms\tp50_ms\tp95_ms\tops_per_sec\ttotal_success')
        print(f"METRIC\t{avg_lat:.3f}\t{int(p50)}\t{int(p95)}\t{ops_per_sec:.3f}\t{int(successes)}")
    except Exception as e:
        print(f'[bench] metrics parse error: {e}')

try:
    metrics = None
    if IntervalMetrics:
        metrics = IntervalMetrics(
            interval_sec=getattr(args, 'metrics_interval', 1.0),
            out_path=getattr(args, 'metrics_out', None),
            label='ipokemon',
        )
    if bench_common and metrics:
        try:
            bench_common.register_metrics_signal_handlers(metrics)
        except Exception:
            pass
    if metrics:
        metrics.start()
        stop_event = threading.Event()
        tail_thread = threading.Thread(target=_tail_jtl_for_metrics, args=(args.out, metrics, stop_event), daemon=True)
        tail_thread.start()

    subprocess.run(cmd, check=True, timeout=args.duration + 120)
    print('[bench] jmeter completed, output ->', args.out)
    if metrics:
        stop_event.set()
        tail_thread.join(timeout=2)
        metrics.stop()
        metrics.write()
    # Try to parse JTL and print METRIC summary
    try:
        _parse_jtl_and_print_metrics(args.out, args.duration)
    except Exception as _e:
        print('[bench] failed to parse jtl for metrics:', _e)
    sys.exit(0)
except subprocess.CalledProcessError as e:
    print(f'[error] jmeter failed: {e}', file=sys.stderr)
    sys.exit(1)
except subprocess.TimeoutExpired:
    print('[error] jmeter timed out', file=sys.stderr)
    sys.exit(1)
