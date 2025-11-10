#!/usr/bin/env python3
"""
Run a series of benchmark runs at different RPS targets and sample container memory state.

Usage example:
  python3 mig-scripts/rps_experiment.py \
    --container myctr \
    --bench-template "python3 experiment/migration/redis/bench_sensoragg.py --rps {rps} --duration {duration} --threads {threads}" \
    --rps-list 10,50,100 --duration 60 --threads 4 --runs 3 --output results/rps_experiment.csv

The bench template must contain a literal '{rps}' and '{duration}' placeholder which will be
substituted for each run.
"""
import argparse
import atexit
import json
import os
import shlex
import subprocess
import time
from datetime import datetime
from typing import Dict, Optional

from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs
from result_writer import extract_stats_from_output

# optional integration with light-dt helpers from source.py
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
        """Try to build and install light-dt from the repository, then modprobe possible module names.
        Returns True if /dev/dirty-track appears after the attempts, False otherwise.
        This is best-effort and will not raise on failure.
        """
        light_dir = os.path.join(repo_root, "light-dt")
        device_node = "/dev/dirty-track"
        if not os.path.isdir(light_dir):
            print(f"light-dt source directory not found at {light_dir}")
            return False

        print(f"Attempting to build and install light-dt in {light_dir} (may require sudo)...")
        # Run make then sudo make install (best-effort, ignore errors to avoid killing the script)
        try:
            run_cmd(f"make -C {shlex.quote(light_dir)}", ignore_error=True, quiet=False)
        except SystemExit:
            # run_cmd may call sys.exit on failure unless ignore_error=True; we passed True but catch defensively
            pass

        try:
            run_cmd(f"sudo make -C {shlex.quote(light_dir)} install", ignore_error=True, quiet=False)
        except SystemExit:
            pass

        # Try modprobe with a few plausible names
        for modname in ("dirty-track", "dirty_track", "dirtytrack"):
            try:
                run_cmd(f"sudo modprobe {shlex.quote(modname)}", ignore_error=True, quiet=True)
            except SystemExit:
                pass

        # small pause for udev/dev creation
        time.sleep(1)
        exists = os.path.exists(device_node)
        if exists:
            print(f"Found {device_node} after build/load attempts")
        else:
            print(f"{device_node} still not present after attempts")
        return exists


def get_container_pid(container: str) -> Optional[int]:
    """Return the container's main PID via `runc state` or None on failure."""
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
    """Sample basic memory metrics for a pid. Values are returned in KB when available."""
    stats: Dict[str, Optional[int]] = {"vmrss_kb": None, "vmsize_kb": None, "pss_kb": None}
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

        # Fallback to /proc/<pid>/status
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
    """Start bench command in background via nohup and return its PID, or None on failure."""
    # Use a shell invocation so we can get $! back
    shell_cmd = f"nohup {cmd} > {shlex.quote(logfile)} 2>&1 < /dev/null & echo $!"
    try:
        res = run_cmd(shell_cmd, quiet=True)
        out = (getattr(res, "stdout", "") or "").strip()
        if out:
            try:
                return int(out.splitlines()[-1].strip())
            except Exception:
                return None
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
    parser = argparse.ArgumentParser(description="Run RPS experiments and sample container memory.")
    parser.add_argument("--container", required=True, help="runc container name to inspect memory for")
    parser.add_argument(
        "--bench-template",
        required=True,
        help="Bench command template, must include {rps} and {duration} placeholders",
    )
    parser.add_argument("--rps-list", required=True, help="Comma-separated list of RPS values, e.g. 10,50,100")
    parser.add_argument("--duration", type=int, default=60, help="Duration (seconds) for each bench run")
    parser.add_argument("--threads", type=int, default=1, help="Number of threads to pass to bench (if supported)")
    parser.add_argument("--runs", type=int, default=1, help="Repetitions per RPS value")
    parser.add_argument("--output", default="results/rps_experiment.csv", help="CSV output file")
    parser.add_argument("--log-dir", default="/tmp", help="Directory to store bench logs")
    parser.add_argument("--cooldown", type=int, default=5, help="Seconds to wait between runs")
    parser.add_argument(
        "--dirtymap",
        action="store_true",
        help="If set, initialize light-dt dirty-map for the container, collect and analyze on exit",
    )
    # keep this script simple: iterate over rps and runs. For matrix tests, use rps_matrix_experiment.py
    args = parser.parse_args()

    if "{rps}" not in args.bench_template or "{duration}" not in args.bench_template:
        print("bench-template must contain {rps} and {duration} placeholders")
        return

    rps_values = [int(x) for x in args.rps_list.split(",") if x.strip()]
    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    # Write CSV header if file does not exist
    header_cols = [
        "ts_utc",
        "rps",
        "run",
        "container",
        "pid",
        "vmrss_before_kb",
        "vmsize_before_kb",
        "vmrss_mid_kb",
        "vmsize_mid_kb",
        "vmrss_after_kb",
        "vmsize_after_kb",
        "bench_stats",
    ]
    if not os.path.exists(args.output):
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(",".join(header_cols) + "\n")

    # If requested, try to initialize light-dt for this container.
    device_file = None
    device_fd = None
    dirtymap_path = None
    if args.dirtymap:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        device_node = DEVICE_PATH if DEVICE_PATH else "/dev/dirty-track"

        # If device node doesn't exist, attempt to build and load the kernel module from repo
        if not os.path.exists(device_node):
            print(f"{device_node} not found; attempting to build/load light-dt from repository")
            try_build_and_load_light_dt(repo_root)

        if not DEVICE_PATH and not os.path.exists(device_node):
            print("Warning: light-dt helpers not available in this environment; skipping dirtymap init")
        elif not os.path.exists(device_node):
            print(f"Warning: {device_node} not found. Is the dirty-track kernel module loaded? Skipping dirtymap init.")
        else:
            # populate container pid list
            try:
                if get_runc_container_pidtree:
                    get_runc_container_pidtree(args.container)
            except Exception as e:
                print(f"Warning: failed to populate container pid tree: {e}")

            # open device and set dirty map path
            try:
                device_file = open(device_node, "wb")
                device_fd = device_file.fileno()
                dirtymap_path = f"/runc/containers/{args.container}/migrate/dirty_map"
                print(f"Initializing dirty-map at {dirtymap_path} via {DEVICE_PATH}")
                if set_dirty_map_path:
                    set_dirty_map_path(device_fd, dirtymap_path)

                # start tracking for all container pids (if available)
                if container_pids and ioctl_start_pid:
                    for pid in container_pids:
                        try:
                            ioctl_start_pid(device_fd, pid)
                        except Exception:
                            pass

                # register cleanup to stop tracking, analyze and unmount
                def _cleanup():
                    try:
                        print("Stopping dirty-track and collecting final dirty-map...")
                        if device_fd and execute_dirty_track:
                            # call execute_dirty_track which will stop tracking and generate dirtymap files
                            try:
                                execute_dirty_track(device_fd, first=False)
                            except Exception as e:
                                print(f"execute_dirty_track failed: {e}")

                        # analyze dirtymap if possible
                        if container_pids and dirtymap_path and container_may_dump_size:
                            try:
                                total_bytes = container_may_dump_size(container_pids, dirtymap_path)
                                print(f"Estimated transfer size from dirty-map: {total_bytes} bytes")
                                # append a summary line to output file
                                with open(args.output, "a", encoding="utf-8") as fout:
                                    fout.write(
                                        ",".join([
                                            datetime.utcnow().isoformat() + "Z",
                                            "dirtymap_summary",
                                            args.container,
                                            str(total_bytes),
                                        ])
                                        + "\n"
                                    )
                            except Exception as e:
                                print(f"Failed to analyze dirtymap: {e}")

                    finally:
                        # unmount tmpfs and cleanup
                        try:
                            unmount_local_migration_tmpfs(args.container, ignore_error=True, quiet=True)
                        except Exception:
                            # best-effort only
                            pass

                atexit.register(_cleanup)
            except Exception as e:
                print(f"Warning: failed to initialize dirty-map: {e}")

    for rps in rps_values:
        for run_idx in range(1, args.runs + 1):
            print(f"Running rps={rps} run={run_idx}")
            pid = get_container_pid(args.container)
            if pid is None:
                print(f"Warning: cannot determine pid for container {args.container}; continuing with empty samples")
            before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

            ts = int(time.time())
            logname = os.path.join(args.log_dir, f"bench_{rps}_{run_idx}_{ts}.log")
            bench_cmd = args.bench_template.format(rps=rps, duration=args.duration, threads=args.threads)
            print(f"Starting bench: {bench_cmd} -> log {logname}")
            bench_pid = run_bench_background(bench_cmd, logname)
            if bench_pid is None:
                print("Failed to start bench, skipping this run")
                continue

            # wait a bit for ramp-up then sample mid
            ramp = max(3, min(10, args.duration // 6))
            time.sleep(ramp)
            mid = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

            # wait until bench should have finished (duration + small margin)
            wait_for_pid_exit(bench_pid, timeout=args.duration + 10)

            # give system a second to settle then sample after
            time.sleep(1)
            after = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

            # read bench log to extract stats
            bench_stats = ""
            try:
                with open(logname, "r", encoding="utf-8", errors="ignore") as f:
                    content = f.read()
                    header, values = extract_stats_from_output(content)
                    if values:
                        bench_stats = values.replace("\t", "|")
                    else:
                        # fallback: last 200 chars
                        bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
            except Exception:
                bench_stats = ""

            row = [
                datetime.utcnow().isoformat() + "Z",
                str(rps),
                str(run_idx),
                args.container,
                str(pid) if pid else "",
                str(before.get("vmrss_kb") or ""),
                str(before.get("vmsize_kb") or ""),
                str(mid.get("vmrss_kb") or ""),
                str(mid.get("vmsize_kb") or ""),
                str(after.get("vmrss_kb") or ""),
                str(after.get("vmsize_kb") or ""),
                shlex.quote(bench_stats),
            ]
            with open(args.output, "a", encoding="utf-8") as f:
                f.write(",".join(row) + "\n")

            print(f"Completed rps={rps} run={run_idx}, wrote row to {args.output}")
            time.sleep(args.cooldown)


if __name__ == "__main__":
    main()
