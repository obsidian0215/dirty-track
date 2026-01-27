#!/usr/bin/env python3
"""Simple CLI wrapper to run ContainerResourceMonitor for a named runc container.

Usage:
  python3 monitor_tool.py --container defog-gocr --interval 1.0 --duration 60 --out /tmp/resource_usage.tsv

On completion prints METRIC_PARAM	container_resource_usage	<path>
"""
import argparse
import datetime
import importlib.util
import os
import signal
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument("--container", required=True)
parser.add_argument("--interval", type=float, default=1.0)
parser.add_argument("--duration", type=int, default=0, help="seconds to run (0 = until ctrl-c)")
parser.add_argument("--out", default=None)
parser.add_argument("--host-iface", default="ens33")
args = parser.parse_args()

# Import monitor module (fallback to file import)
try:
    import mig_scripts.monitor as monitor_mod
except Exception:
    spec = importlib.util.spec_from_file_location("monitor_mod", os.path.join(os.path.dirname(__file__), "monitor.py"))
    monitor_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor_mod)

if not args.out:
    ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    args.out = f"/runc/containers/{args.container}/migrate/d_log/resource_usage.cli.{ts}.tsv"

print(f"Starting monitor for container {args.container} -> {args.out} (interval={args.interval}s)")
resmon = monitor_mod.ContainerResourceMonitor(
    args.container, interval=args.interval, out_path=args.out, include_host=True, enable_net=True, host_iface=args.host_iface
)
resmon.start()

stop_requested = False

def _stop(_signum, _frame):
    global stop_requested
    stop_requested = True

signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)

start = time.time()
try:
    if args.duration and args.duration > 0:
        endt = start + float(args.duration)
        while time.time() < endt and not stop_requested:
            time.sleep(0.5)
    else:
        while not stop_requested:
            time.sleep(0.5)
except KeyboardInterrupt:
    pass
finally:
    print("Stopping monitor...")
    try:
        monitor_mod.set_phase("done")
    except Exception:
        pass
    resmon.stop()
    print(f"METRIC_PARAM\tcontainer_resource_usage\t{resmon.out_path}")
    print("Monitor finished")
