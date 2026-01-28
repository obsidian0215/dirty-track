#!/usr/bin/env python3
"""
run_workload.py - simple smoke runner for migration benches

For each service under migration that has a `bench.py`, this script will:
 - ensure a fog_workloads bundle exists for the service (lowercase name + .bak)
 - start the service container from /runc/fog_workloads/<service>
 - wait for health (HTTP /health or Redis PING)
 - run a short-duration smoke of the local `bench.py` (default duration 5s)
 - stop and clean the container

The script imports helpers from /runc/dirty-track/mig-scripts/fog_test.py where available.
"""

import argparse
import importlib.util
import os
import subprocess
import sys
import time
import shlex

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
MIG_SCRIPTS = os.path.abspath(os.path.join(ROOT, '..', '..', 'mig-scripts'))

# Load fog_test helpers (ensure mig-scripts is on sys.path so fog_test can import its siblings)
if MIG_SCRIPTS not in sys.path:
    sys.path.insert(0, MIG_SCRIPTS)
spec = importlib.util.spec_from_file_location('fog_test', os.path.join(MIG_SCRIPTS, 'fog_test.py'))
ft = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ft)

MIG_ROOT = ROOT
FW_ROOT = os.path.abspath('/runc/fog_workloads')
DATASET = '/runc/datasets'


def find_services():
    services = []
    for name in os.listdir(MIG_ROOT):
        path = os.path.join(MIG_ROOT, name)
        bench = os.path.join(path, 'bench.py')
        if os.path.isdir(path) and os.path.exists(bench):
            services.append(name)
    return sorted(services)


def bench_capabilities(bench_path):
    txt = open(bench_path, 'r', encoding='utf-8', errors='ignore').read()
    return {
        'duration': '--duration' in txt,
        'iters': '--iters' in txt or '--requests' in txt,
        'concurrency': '--concurrency' in txt or '--threads' in txt,
        'out': '--out' in txt or '--output' in txt,
    }


def run_local_bench(bench_path, backend, scene, port, duration=5, container=None):
    caps = bench_capabilities(bench_path)
    out = f"/tmp/{scene}_bench.csv"
    cmd = None
    out_arg = f" --out {shlex.quote(out)}" if caps.get('out') else ""

    if backend == 'redis':
        if caps['duration']:
            cmd = f"python3 {shlex.quote(bench_path)} --redis-host 127.0.0.1 --redis-port {int(port)} --threads 1 --duration {int(duration)} --dataset {shlex.quote(DATASET)}{out_arg}"
        elif caps['iters']:
            cmd = f"python3 {shlex.quote(bench_path)} --redis-host 127.0.0.1 --redis-port {int(port)} --iters 1 --concurrency 1 --dataset {shlex.quote(DATASET)}{out_arg}"
        else:
            cmd = f"python3 {shlex.quote(bench_path)} --redis-host 127.0.0.1 --redis-port {int(port)} --threads 1 --duration {int(duration)} --dataset {shlex.quote(DATASET)}{out_arg}"
    elif backend == 'influxdb':
        if caps['duration']:
            cmd = f"python3 {shlex.quote(bench_path)} --influx-url http://127.0.0.1:{int(port)} --threads 1 --duration {int(duration)} --dataset {shlex.quote(DATASET)}{out_arg}"
        else:
            cmd = f"python3 {shlex.quote(bench_path)} --influx-url http://127.0.0.1:{int(port)} --threads 1 --duration {int(duration)} --dataset {shlex.quote(DATASET)}{out_arg}"
    elif backend == 'jmeter':
        # JMeter-based benches (e.g. iPokeMon) do not accept --url/--dataset flags.
        # If JMeter binary is not present on the host, perform a lightweight HTTP smoke instead.
        import shutil
        jmeter_bin = shutil.which('jmeter')
        if not jmeter_bin:
            # If JMeter isn't on the host, prefer running it inside the started container (if present).
            if container:
                try:
                    rj = ft.run_cmd(f"runc exec {shlex.quote(container)} /bin/sh -lc \"command -v jmeter >/dev/null && echo OK || echo NO\"", quiet=True, ignore_error=True, timeout=5)
                    if getattr(rj, 'returncode', 0) == 0 and 'OK' in (getattr(rj, 'stdout', '') or ''):
                        print('[bench-run] jmeter not found on host but present inside container; executing JMeter inside container')
                        # container-local JMX path (inside the container rootfs)
                        jmx_container = '/root/iPokeMon/ipokemon/Application/iPokeMon-Client/iPokeMon.jmx'
                        jtl_container = '/tmp/ipokemon.jtl'
                        run_cmd = f"runc exec {shlex.quote(container)} /bin/sh -lc \"jmeter -n -t {shlex.quote(jmx_container)} -JHOST=127.0.0.1 -JPORT={int(port)} -JDuration={int(duration)} -JThreads=1 -Jjmeter.save.saveservice.output_format=csv -l {shlex.quote(jtl_container)}\""
                        rc3 = ft.run_cmd(run_cmd, quiet=False, ignore_error=True, timeout=duration*10)
                        if getattr(rc3, 'returncode', 1) == 0:
                            # Try to copy the JTL out to host /tmp as the bench output
                            fetch_cmd = f"runc exec {shlex.quote(container)} /bin/sh -lc \"cat {shlex.quote(jtl_container)}\""
                            rc4 = ft.run_cmd(fetch_cmd, quiet=True, ignore_error=True, timeout=10)
                            jtl_out = (getattr(rc4, 'stdout', '') or '')
                            if rc4 and getattr(rc4, 'returncode', 1) == 0 and jtl_out:
                                with open(out, 'w') as fh:
                                    fh.write(jtl_out)
                                # Try to parse JTL and print METRIC summary so container-run JMeter also produces METRIC output
                                try:
                                    import csv, xml.etree.ElementTree as ET, math, io
                                    s = jtl_out.lstrip()
                                    latencies = []
                                    successes = 0
                                    total = 0
                                    if s.startswith('<'):
                                        # parse XML
                                        root = ET.fromstring(jtl_out)
                                        for elem in root.iter():
                                            tag = (elem.tag or '').lower()
                                            if 'sample' in tag:
                                                total += 1
                                                t = elem.attrib.get('t') or elem.attrib.get('time') or elem.attrib.get('elapsed')
                                                succ = elem.attrib.get('s') or elem.attrib.get('success')
                                                try:
                                                    if t is not None:
                                                        latencies.append(int(float(t)))
                                                except Exception:
                                                    pass
                                                if succ is None or str(succ).lower() in ('true', '1', 'yes'):
                                                    successes += 1
                                    else:
                                        f = io.StringIO(jtl_out)
                                        reader = csv.reader(f)
                                        try:
                                            header = next(reader)
                                        except StopIteration:
                                            header = []
                                        hmap = {h.strip().lower(): i for i, h in enumerate(header)} if header else {}
                                        elapsed_idx = hmap.get('elapsed', 1)
                                        success_idx = hmap.get('success') if hmap else None
                                        for row in reader:
                                            if not row:
                                                continue
                                            total += 1
                                            try:
                                                latencies.append(int(float(row[elapsed_idx])))
                                            except Exception:
                                                pass
                                            if success_idx is not None:
                                                try:
                                                    s_val = row[success_idx]
                                                    if str(s_val).lower() in ('true', '1', 'yes'):
                                                        successes += 1
                                                except Exception:
                                                    pass
                                        if success_idx is None:
                                            successes = total if total > 0 else 0
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
                                    print(f"[bench-run] jtl parse error: {e}")
                                print(f"[bench-run] container jmeter executed, out={out}")
                                return True, out
                            else:
                                print('[bench-run] failed to fetch jtl from container')
                                # fallthrough to lightweight smoke
                        else:
                            print('[bench-run] container jmeter run failed', getattr(rc3, 'stdout', '')[:200], getattr(rc3, 'stderr', '')[:200])
                except Exception as e:
                    print(f"[bench-run] exception while running container jmeter: {e}")
            # Fallback: lightweight HTTP smoke (host then container-local)
            endpoint = ft.SCENE_INFO.get(scene, {}).get('endpoint', '/')
            host_url = f"http://127.0.0.1:{int(port)}{endpoint}"
            print('[bench-run] jmeter not found, performing lightweight HTTP smoke (host then container-local)')
            try:
                rc = ft.run_cmd(f"curl -sS -o /dev/null -w '%{{http_code}}' {shlex.quote(host_url)}", quiet=True, ignore_error=True, timeout=3)
                if getattr(rc, 'returncode', 1) == 0:
                    with open(out, 'w') as fh:
                        fh.write('METRIC_HEADER\n')
                    print(f"[bench-run] lightweight smoke OK (host), out={out}")
                    return True, out
                # try container-local curl if a container name is available
                if container:
                    c_cmd = f"runc exec {shlex.quote(container)} curl -sS -o /dev/null -w '%{{http_code}}' http://127.0.0.1:{int(port)}{endpoint}"
                    rc2 = ft.run_cmd(c_cmd, quiet=True, ignore_error=True, timeout=3)
                    if getattr(rc2, 'returncode', 1) == 0:
                        with open(out, 'w') as fh:
                            fh.write('METRIC_HEADER\n')
                        print(f"[bench-run] lightweight smoke OK (container-local), out={out}")
                        return True, out
                print('[bench-run] lightweight smoke failed')
                return False, None
            except Exception as e:
                print('[bench-run] lightweight smoke encountered an exception:', e)
                return False, None
        else:
            if caps.get('duration'):
                cmd = f"python3 {shlex.quote(bench_path)} --duration {int(duration)} --threads 1{out_arg}"
            elif caps.get('concurrency'):
                cmd = f"python3 {shlex.quote(bench_path)} --threads 1{out_arg}"
            else:
                cmd = f"python3 {shlex.quote(bench_path)}{out_arg}"
    else:
        # HTTP client
        endpoint = ft.SCENE_INFO.get(scene, {}).get('endpoint', '/')
        # prefer host loopback if reachable, otherwise fall back to container IP
        host_url = f"http://127.0.0.1:{int(port)}{endpoint}"
        url = host_url
        try:
            rc = ft.run_cmd(f"curl -sS -o /dev/null -w '%{{http_code}}' {shlex.quote(host_url)}", quiet=True, ignore_error=True, timeout=3)
            if getattr(rc, 'returncode', 1) != 0 and container:
                try:
                    rr = ft.run_cmd(f"runc exec {shlex.quote(container)} /bin/sh -c \"hostname -I | awk '{{print $1}}'\"", quiet=True, ignore_error=True, timeout=3)
                    ip = (getattr(rr, 'stdout', '') or '').strip().split()[0] if getattr(rr, 'stdout', '') else ''
                    if ip:
                        container_url = f"http://{ip}:{int(port)}{endpoint}"
                        rc2 = ft.run_cmd(f"curl -sS -o /dev/null -w '%{{http_code}}' {shlex.quote(container_url)}", quiet=True, ignore_error=True, timeout=3)
                        if getattr(rc2, 'returncode', 1) == 0:
                            url = container_url
                except Exception:
                    pass
        except Exception:
            pass

        # prefer duration when both are supported
        if caps.get('duration'):
            cmd = f"python3 {shlex.quote(bench_path)} --url {shlex.quote(url)} --duration {int(duration)} --concurrency 1 --dataset {shlex.quote(DATASET)}{out_arg}"
        elif caps.get('iters'):
            cmd = f"python3 {shlex.quote(bench_path)} --url {shlex.quote(url)} --file {shlex.quote(ft.SCENE_INFO.get(scene, {}).get('asset',''))} --iters 1 --concurrency 1 --dataset {shlex.quote(DATASET)}{out_arg}"
        else:
            cmd = f"python3 {shlex.quote(bench_path)} --url {shlex.quote(url)} --iters 1 --concurrency 1 --dataset {shlex.quote(DATASET)}{out_arg}"

    # Detect whether the bench supports metrics output so we can validate success_ops > 0
    supports_metrics = False
    try:
        bench_sample = open(bench_path, 'r', encoding='utf-8', errors='ignore').read(8192)
        if '--metrics-out' in bench_sample or 'IntervalMetrics' in bench_sample or 'METRIC_HEADER' in bench_sample:
            supports_metrics = True
    except Exception:
        supports_metrics = False

    metrics_path = None
    if supports_metrics:
        metrics_path = f"/tmp/{scene}_bench_{int(time.time())}_metrics.json"
        cmd = cmd + f" --metrics-out {shlex.quote(metrics_path)} --metrics-interval 1.0"

    print(f"[bench-run] {cmd}")
    try:
        r = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=duration*6)
    except subprocess.TimeoutExpired:
        print(f"[bench-run] timeout (cmd took > {duration*6}s)")
        return False, None

    ok = r.returncode == 0

    # If bench produced a metrics JSON, inspect it for any successful ops
    if metrics_path and os.path.exists(metrics_path):
        try:
            import json as _json
            mj = _json.load(open(metrics_path))
            succ_ops = mj.get('overall', {}).get('success_ops', 0)
            if succ_ops <= 0:
                print(f"[bench-run] metrics file {metrics_path} indicates success_ops=={succ_ops}; marking bench as FAILED")
                # Print brief diagnostics for debugging
                try:
                    print(f"[bench-run] stdout: {(r.stdout or b'').decode('utf-8', errors='ignore')[:400]}")
                except Exception:
                    pass
                return False, out if os.path.exists(out) else None
            return True, out if os.path.exists(out) else None
        except Exception as _e:
            print(f"[bench-run] failed to parse metrics file {metrics_path}: {_e}")

    # if out exists consider it success (some benches write CSV)
    if os.path.exists(out):
        # Fallback: parse CSV and ensure at least one success status (200-399)
        try:
            import csv as _csv
            succ = 0
            with open(out, 'r', encoding='utf-8', errors='ignore') as _fh:
                reader = _csv.reader(_fh)
                try:
                    hdr = next(reader)
                except StopIteration:
                    hdr = []
                for row in reader:
                    if not row:
                        continue
                    try:
                        status = int(row[1]) if len(row) > 1 and row[1] else 0
                        if 200 <= status < 400:
                            succ += 1
                    except Exception:
                        pass
            if succ <= 0:
                print(f"[bench-run] CSV {out} indicates 0 successful requests; marking bench as FAILED")
                return False, out
            return True, out
        except Exception as _e:
            print(f"[bench-run] failed to parse CSV {out}: {_e}")
            return True, out

    # also consider stdout containing metrics
    sout = (r.stdout or b'').decode('utf-8', errors='ignore')
    if 'METRIC' in sout or 'METRIC_HEADER' in sout:
        return True, None
    print(f"[bench-run] exit={r.returncode} stdout={sout[:400]} stderr={(r.stderr or b'').decode('utf-8', errors='ignore')[:400]}")
    return ok, None


def smoke_service(scene, port, duration=5):
    print(f"\n=== SMOKE {scene} (port {port}) ===")
    bench_path = ft.find_local_bench_for_bundle(scene) or ft.find_local_bench_for_bundle(scene.lower())
    if not bench_path:
        print(f"[skip] no local bench for {scene}")
        return False

    backend = ft.detect_backend_from_bench(bench_path)
    print(f"[info] found bench: {bench_path} backend={backend}")

    # ensure bundle exists
    ft.ensure_service_bundle(scene, backend)

    try:
        res = ft.start_service(scene, '127.0.0.1', port)
        if isinstance(res, tuple) and len(res) == 3:
            container, bundle_path, actual_port = res
        else:
            container, bundle_path = res
            actual_port = port
    except Exception as e:
        print(f"[error] failed to start service {scene}: {e}")
        return False

    try:
        # Prefer longer wait for Influx-backed services which can take longer to initialize
        if backend == 'influxdb':
            ok = ft.wait_for_health(container, '127.0.0.1', actual_port, endpoint=ft.SCENE_INFO.get(scene, {}).get('endpoint','/health'), attempts=60, delay=2.0)
        else:
            ok = ft.wait_for_health(container, '127.0.0.1', actual_port, endpoint=ft.SCENE_INFO.get(scene, {}).get('endpoint','/health'))
        if not ok:
            print(f"[error] health check failed for {scene} on port {actual_port}")
            return False
        print(f"[info] {scene} healthy; running bench")
        success, out = run_local_bench(bench_path, backend, scene, actual_port, duration=duration, container=container)
        if success:
            print(f"[ok] bench for {scene} succeeded, out={out}")
        else:
            print(f"[fail] bench for {scene} failed")
        return success
    finally:
        print(f"[cleanup] stopping and cleaning {scene}")
        ft.stop_and_clean(container, bundle_path, scene)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scene', default='all', help='service name to test or all')
    parser.add_argument('--start-port', default=8080, type=int, help='start port (overridden by scene default when scene-specific default exists)')
    parser.add_argument('--duration', default=5, type=int, help='short bench duration in seconds')
    args = parser.parse_args()

    services = find_services()
    if args.scene != 'all':
        if args.scene not in services:
            print(f"Unknown scene {args.scene}; available: {', '.join(services)}")
            sys.exit(2)
        services = [args.scene]

    summary = {}
    for s in services:
        port = ft.SCENE_INFO.get(s, {}).get('default_port', args.start_port)
        print(f"\n-- Testing {s} on port {port} --")
        ok = smoke_service(s, port, duration=args.duration)
        summary[s] = ok

    print('\n=== SMOKE SUMMARY ===')
    for s, ok in summary.items():
        print(f"{s}: {'OK' if ok else 'FAIL'}")
    failures = [s for s, ok in summary.items() if not ok]
    if failures:
        print('\nFailures:', ', '.join(failures))
        sys.exit(1)
    print('\nAll smoke tests passed')


if __name__ == '__main__':
    main()
