#!/usr/bin/env python3
"""run_workload.py - Generic runner for a migration service workload
Reads ./backends/<backend>/workload.yaml and launches DB (in-memory), optional frontend, and bench client.
"""
import argparse
import subprocess
import yaml
import os
import signal
import sys
import time

parser = argparse.ArgumentParser(description="Run a sensoragg workload (service-level runner)")
parser.add_argument("--backend", choices=["redis", "influxdb"], required=True)
parser.add_argument("--workload", default="backends/{backend}/workload.yaml")
parser.add_argument("--override-duration", type=int, default=None)
parser.add_argument("--override-qps", type=int, default=None)
parser.add_argument("--override-concurrency", type=int, default=None)
parser.add_argument("--no-start-db", action="store_true", help="Don't auto-start DB container")
args = parser.parse_args()

ROOT = os.path.dirname(__file__)
backend = args.backend
workload_path = os.path.join(ROOT, "backends", backend, "workload.yaml")
if not os.path.exists(workload_path):
    print(f"Workload file not found: {workload_path}")
    sys.exit(1)

with open(workload_path, 'r') as f:
    w = yaml.safe_load(f)

# Apply overrides
if args.override_duration:
    w['duration'] = args.override_duration
if args.override_qps:
    w['qps'] = args.override_qps
if args.override_concurrency:
    w['concurrency'] = args.override_concurrency

# Start DB if requested
if w.get('start_db', True) and not args.no_start_db:
    if backend == 'redis':
        start_sh = os.path.join(ROOT, 'backends', 'redis', 'start_redis_in_memory.sh')
        port = w.get('db_port', 6379)
        print(f"Starting redis on port {port} ...")
        subprocess.check_call([start_sh, str(port)])
        time.sleep(1)
    else:
        # influx: start an in-memory container (tmpfs) if docker available
        start_sh = os.path.join(ROOT, 'backends', 'influxdb', 'start_influx_in_memory.sh')
        port = w.get('db_port', 8086)
        print(f"Starting influx on port {port} ...")
        subprocess.check_call([start_sh, str(port)])
        time.sleep(2)

# Optionally start frontend
frontend_url = None
if w.get('use_frontend'):
    if backend == 'redis':
        frontend_port = w.get('frontend_port', 5000)
        fe = os.path.join(ROOT, 'backends', 'redis', 'service_frontend.py')
        print(f"Starting redis frontend on port {frontend_port} ...")
        fe_proc = subprocess.Popen([sys.executable, fe], env={**os.environ, 'FRONTEND_PORT': str(frontend_port)})
        frontend_url = f"http://127.0.0.1:{frontend_port}"
        time.sleep(1)
    else:
        # influx frontend not implemented yet - placeholder
        frontend_url = None

# Compose bench command
if backend == 'redis':
    bench = os.path.join(os.path.dirname(ROOT), 'redis', 'bench_sensoragg.py')
    cmd = [sys.executable, bench, '--redis-host', '127.0.0.1', '--redis-port', str(w.get('db_port', 6379)), '--threads', str(w['concurrency']), '--duration', str(w['duration']), '--rps', str(w['qps']), '--payload-size', str(w['payload_size']), '--payload-mode', str(w['payload_mode'])]
    if w.get('extra_args'):
        cmd += w['extra_args'].split()
    if frontend_url:
        cmd += ['--frontend-url', frontend_url]
else:
    bench = os.path.join(os.path.dirname(ROOT), 'influxdb', 'bench_sensoragg.py')
    cmd = [sys.executable, bench, '--influx-url', f"http://127.0.0.1:{w.get('db_port', 8086)}", '--threads', str(w['concurrency']), '--duration', str(w['duration']), '--rps', str(w['qps']), '--payload-size', str(w['payload_size']), '--payload-mode', str(w['payload_mode'])]
    if w.get('extra_args'):
        cmd += w['extra_args'].split()
    if frontend_url:
        cmd += ['--frontend-url', frontend_url]

print("Running:", ' '.join(cmd))
proc = subprocess.Popen(cmd)

# Wait and forward signals
try:
    proc.wait()
finally:
    # teardown
    if w.get('use_frontend') and backend == 'redis' and 'fe_proc' in locals():
        fe_proc.terminate()
        fe_proc.wait()
    print("Workload finished")
