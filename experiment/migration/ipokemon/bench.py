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
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('--jmx', default='/runc/fog_workloads/ipokemon/rootfs/root/iPokeMon/ipokemon/Application/iPokeMon-Client/iPokeMon.jmx', help='Path to iPokeMon JMX file')
parser.add_argument('--jmeter', default='jmeter', help='Path to jmeter binary')
parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=50, help='JMeter thread count')
parser.add_argument('--host', default='127.0.0.1', help='Target host for JMeter (JMeter -JHOST)')
parser.add_argument('--port', type=int, default=8000, help='Target port for JMeter (JMeter -JPORT)')
parser.add_argument('--out', default=None, help='Output JTL file (defaults to ipokemon_<ts>.jtl)')
args = parser.parse_args()

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
    subprocess.run(cmd, check=True, timeout=args.duration + 120)
    print('[bench] jmeter completed, output ->', args.out)
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
