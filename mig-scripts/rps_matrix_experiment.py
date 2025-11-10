#!/usr/bin/env python3
"""
Run a full matrix of RPS x payload x data-pattern tests, collect container memory, DB info and dirty-map per run.
This script is more feature-rich than rps_experiment.py and attempts to build/load light-dt if needed.

Usage (example):
    python3 mig-scripts/rps_matrix_experiment.py
        --container myctr
        --bench-template "python3 experiment/migration/redis/bench_sensoragg.py --rps {rps} --duration {duration} --threads {threads} --payload-size {payload} --pattern {pattern}"
        --rps-list 10,50
        --payload-sizes 64,512,4096 --patterns random,zeros --duration 30 --threads 4 --runs 3 --output results/rps_matrix.csv --dirtymap

Notes:
- bench-template must include {rps} and {duration}. If you want payload/pattern substitution, include {payload} and {pattern}.
- For dirty-map support the script will try to build/load the kernel module from repo/light-dt (requires make & sudo).
- Dirty-map collection per-run is best-effort and uses helpers from mig-scripts/source.py when available.
"""
import argparse
import atexit
import json
import os
import shlex
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

from cmd_utils import run_cmd, unmount_local_migration_tmpfs
from result_writer import extract_stats_from_output

# Try to import helpers from source.py (light-dt ioctl helpers)
try:
    from source import (
        DEVICE_PATH,
        set_dirty_map_path,
        ioctl_start_pid,
        ioctl_stop_pid,
        get_runc_container_pidtree,
        container_pids,
        container_may_dump_size,
        execute_dirty_track,
    )
except Exception:
    DEVICE_PATH = None
    set_dirty_map_path = None
    ioctl_start_pid = None
    ioctl_stop_pid = None
    get_runc_container_pidtree = None
    container_pids = None
    container_may_dump_size = None
    execute_dirty_track = None


def try_build_and_load_light_dt(repo_root: str) -> bool:
    light_dir = os.path.join(repo_root, "light-dt")
    device_node = "/dev/dirty-track"
    if not os.path.isdir(light_dir):
        print(f"light-dt source directory not found at {light_dir}")
        return False

    print(f"Attempting to build and install light-dt in {light_dir} (may require sudo)...")
    run_cmd(f"make -C {shlex.quote(light_dir)}", ignore_error=True, quiet=False)
    run_cmd(f"sudo make -C {shlex.quote(light_dir)} install", ignore_error=True, quiet=False)
    for modname in ("dirty-track", "dirty_track", "dirtytrack"):
        run_cmd(f"sudo modprobe {shlex.quote(modname)}", ignore_error=True, quiet=True)
    time.sleep(1)
    return os.path.exists(device_node)


def get_container_pid(container: str) -> Optional[int]:
    try:
        res = run_cmd(f"runc state {shlex.quote(container)}", quiet=True)
        out = getattr(res, "stdout", "") or ""
        data = json.loads(out)
        pid = int(data.get("pid", 0))
        if pid <= 0:
            return None
        return pid
    except Exception:
        return None


def sample_mem(pid: int) -> Dict[str, Optional[int]]:
    stats = {"vmrss_kb": None, "vmsize_kb": None, "pss_kb": None}
    try:
        rollup = f"/proc/{pid}/smaps_rollup"
        if os.path.exists(rollup):
            with open(rollup, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("Rss:"):
                        stats["vmrss_kb"] = int(line.split()[1])
                    elif line.startswith("Size:"):
                        stats["vmsize_kb"] = int(line.split()[1])
                    elif line.startswith("Pss:"):
                        stats["pss_kb"] = int(line.split()[1])
            return stats
        status = f"/proc/{pid}/status"
        if os.path.exists(status):
            with open(status, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        stats["vmrss_kb"] = int(line.split()[1])
                    elif line.startswith("VmSize:"):
                        stats["vmsize_kb"] = int(line.split()[1])
            return stats
    except Exception:
        pass
    return stats


def run_bench_background(cmd: str, logfile: str) -> Optional[int]:
    shell_cmd = f"nohup {cmd} > {shlex.quote(logfile)} 2>&1 < /dev/null & echo $!"
    try:
        res = run_cmd(shell_cmd, quiet=True)
        out = (getattr(res, "stdout", "") or "").strip()
        if out:
            return int(out.splitlines()[-1].strip())
    except Exception:
        return None
    return None


def wait_for_pid_exit(pid: int, timeout: int) -> None:
    start = time.time()
    while True:
        try:
            os.kill(pid, 0)
        except OSError:
            return
        if time.time() - start > timeout:
            return
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser(description="Run RPS x payload x pattern experiments")
    parser.add_argument("--container", required=True)
    parser.add_argument("--bench-template", required=True)
    parser.add_argument("--rps-list", required=True)
    parser.add_argument("--payload-sizes", default="64,512,4096")
    parser.add_argument("--patterns", default="random,zeros,repeat")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", default="results/rps_matrix.csv")
    parser.add_argument("--log-dir", default="/tmp")
    parser.add_argument("--dirtymap", action="store_true")
    parser.add_argument("--client-ip", default="", help="IP of remote client where bench should run")
    parser.add_argument(
        "--remote-client",
        action="store_true",
        help="If set, run bench on --client-ip via SSH instead of locally",
    )
    parser.add_argument(
        "--fetch-remote-log",
        action="store_true",
        help="When running on --remote-client, scp the remote log back to local --log-dir for parsing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not start benches; simulate runs locally and write small dry-run logs",
    )
    args = parser.parse_args()

    rps_values = [int(x) for x in args.rps_list.split(",") if x.strip()]
    payloads = [int(x) for x in args.payload_sizes.split(",") if x.strip()]
    patterns = [x for x in args.patterns.split(",") if x.strip()]

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    header = [
        "ts_utc",
        "rps",
        "payload_bytes",
        "pattern",
        "run",
        "container",
        "pid",
        "vmrss_before_kb",
        "vmsize_before_kb",
        "vmrss_mid_kb",
        "vmsize_mid_kb",
        "vmrss_after_kb",
        "vmsize_after_kb",
        "dirtymap_bytes",
        "bench_stats",
    ]
    if not os.path.exists(args.output):
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(",".join(header) + "\n")

    device_file = None
    device_fd = None
    dirtymap_path = None

    if args.dirtymap:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        device_node = DEVICE_PATH if DEVICE_PATH else "/dev/dirty-track"
        if not os.path.exists(device_node):
            try_build_and_load_light_dt(repo_root)
        if os.path.exists(device_node):
            try:
                device_file = open(device_node, "wb")
                device_fd = device_file.fileno()
                dirtymap_path = f"/runc/containers/{args.container}/migrate/dirty_map"
                if get_runc_container_pidtree:
                    get_runc_container_pidtree(args.container)
                if set_dirty_map_path:
                    set_dirty_map_path(device_fd, dirtymap_path)
            except Exception as e:
                print(f"dirtymap init failed: {e}")

    # register cleanup that will try to stop tracking and unmount
    def _cleanup():
        try:
            if device_fd and execute_dirty_track:
                try:
                    execute_dirty_track(device_fd, first=False)
                except Exception as e:
                    print(f"execute_dirty_track failed at cleanup: {e}")
            if args.dirtymap and container_pids and dirtymap_path and container_may_dump_size:
                try:
                    total = container_may_dump_size(container_pids, dirtymap_path)
                    print(f"Final dirtymap est: {total} bytes")
                except Exception as e:
                    print(f"failed final dirtymap analysis: {e}")
        finally:
            try:
                unmount_local_migration_tmpfs(args.container, ignore_error=True, quiet=True)
            except Exception:
                pass

    atexit.register(_cleanup)

    for rps in rps_values:
        for payload in payloads:
            for pattern in patterns:
                for run_idx in range(1, args.runs + 1):
                    print(f"Run rps={rps} payload={payload} pattern={pattern} run={run_idx}")
                    if args.dry_run:
                        pid = None
                    else:
                        pid = get_container_pid(args.container)
                    before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                    ts = int(time.time())
                    local_logname = os.path.join(args.log_dir, f"bench_{rps}_{payload}_{pattern}_{run_idx}_{ts}.log")
                    bench_cmd = args.bench_template.format(
                        rps=rps, duration=args.duration, threads=args.threads, payload=payload, pattern=pattern
                    )

                    bench_pid = None
                    remote_logname = local_logname
                    client_target = args.client_ip

                    if args.dry_run:
                        # simulate run: write a tiny local log and do not start any process
                        print(f"DRY-RUN: would run: {bench_cmd} -> {local_logname}")
                        try:
                            with open(local_logname, "w", encoding="utf-8") as df:
                                df.write("DRY-RUN\n")
                                df.write(bench_cmd + "\n")
                        except Exception:
                            pass
                    else:
                        if args.remote_client:
                            if not args.client_ip:
                                print("--remote-client set but --client-ip is empty; skipping run")
                                continue
                            # default remote user to root when none is provided
                            if "@" not in args.client_ip:
                                client_target = f"root@{args.client_ip}"
                            # run on remote client via SSH, create a log on remote host
                            remote_logname = f"/tmp/bench_{rps}_{payload}_{pattern}_{run_idx}_{ts}.log"
                            remote_cmd = f"nohup {bench_cmd} > {shlex.quote(remote_logname)} 2>&1 < /dev/null & echo $!"
                            try:
                                res = run_cmd(f"ssh -n {client_target} {shlex.quote(remote_cmd)}", quiet=True)
                                out = (getattr(res, "stdout", "") or "").strip()
                                if out:
                                    try:
                                        bench_pid = int(out.splitlines()[-1].strip())
                                    except Exception:
                                        bench_pid = None
                            except Exception as e:
                                print(f"Failed to start remote bench on {client_target}: {e}")
                                continue
                        else:
                            print(f"Starting local: {bench_cmd} -> log {local_logname}")
                            bench_pid = run_bench_background(bench_cmd, local_logname)
                            if bench_pid is None:
                                print("bench failed to start; skipping")
                                continue

                    ramp = max(2, min(8, args.duration // 6))
                    time.sleep(ramp)
                    mid = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                    # stop and collect dirtymap for this run (best-effort)
                    dirty_bytes = ""
                    if args.dirtymap and device_fd and execute_dirty_track and container_pids and container_may_dump_size and dirtymap_path:
                        try:
                            # attempt to generate a dirtymap snapshot
                            execute_dirty_track(device_fd, first=True)
                            dirty_bytes = str(container_may_dump_size(container_pids, dirtymap_path))
                        except Exception as e:
                            print(f"per-run dirtymap capture failed: {e}")
                            dirty_bytes = ""

                    # wait for bench to finish (local or remote)
                    if args.remote_client and bench_pid is not None:
                        # wait remotely by polling the pid on the client
                        wait_cmd = (
                            "bash -c 'start=$(date +%s); while kill -0 "
                            + str(bench_pid)
                            + " 2>/dev/null; do sleep 1; if [ $(( $(date +%s) - $start )) -gt "
                            + str(args.duration + 10)
                            + " ]; then echo timeout; exit 0; fi; done; echo done'"
                        )
                        try:
                            run_cmd(f"ssh {args.client_ip} {shlex.quote(wait_cmd)}", quiet=True)
                        except Exception:
                            pass
                    else:
                        if bench_pid is not None:
                            wait_for_pid_exit(bench_pid, timeout=args.duration + 10)
                        else:
                            print("No bench PID available to wait on (local); continuing")
                    time.sleep(1)
                    after = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                    bench_stats = ""
                    # If remote run, optionally fetch the remote log; otherwise try to read it via ssh cat
                    target_log = local_logname
                    content = None
                    if args.remote_client:
                        if args.fetch_remote_log:
                            # copy remote log to local path
                            try:
                                scp_cmd = f"scp {args.client_ip}:{shlex.quote(remote_logname)} {shlex.quote(local_logname)}"
                                run_cmd(scp_cmd, quiet=True)
                                target_log = local_logname
                            except Exception as e:
                                print(f"Failed to scp remote log: {e}")
                                target_log = remote_logname
                        else:
                            # try to read remote log via ssh cat and parse content directly
                            try:
                                res = run_cmd(f"ssh {args.client_ip} cat {shlex.quote(remote_logname)}", quiet=True)
                                content = (getattr(res, "stdout", "") or "")
                            except Exception as e:
                                print(f"Failed to read remote log via ssh: {e}")
                                content = None

                    if content is None:
                        try:
                            with open(target_log, "r", encoding="utf-8", errors="ignore") as f:
                                content = f.read()
                        except Exception:
                            content = None

                    if content:
                        header, values = extract_stats_from_output(content)
                        if values:
                            bench_stats = values.replace("\t", "|")
                        else:
                            bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
                    else:
                        bench_stats = ""

                    row = [
                        datetime.utcnow().isoformat() + "Z",
                        str(rps),
                        str(payload),
                        str(pattern),
                        str(run_idx),
                        args.container,
                        str(pid) if pid else "",
                        str(before.get("vmrss_kb") or ""),
                        str(before.get("vmsize_kb") or ""),
                        str(mid.get("vmrss_kb") or ""),
                        str(mid.get("vmsize_kb") or ""),
                        str(after.get("vmrss_kb") or ""),
                        str(after.get("vmsize_kb") or ""),
                        str(dirty_bytes),
                        shlex.quote(bench_stats),
                    ]
                    with open(args.output, "a", encoding="utf-8") as f:
                        f.write(",".join(row) + "\n")

                    print(f"Finished run, wrote row to {args.output}")
                    time.sleep(3)


if __name__ == "__main__":
    main()
