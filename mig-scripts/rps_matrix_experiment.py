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
import sys
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

# Note: loading of migration / dirty-map helpers from `source.py` is done
# lazily inside main() only when --dirtymap is requested. This avoids noisy
# import-time side-effects from other mig-scripts modules that parse CLI args
# at import time.
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


def load_dirtymap_helpers(container: str, repo_root: Optional[str] = None):
    """Lazy, safe loader for mig-scripts/source.py helpers.

    Returns a tuple (device_file, device_fd, dirtymap_path). Any of the
    returned values may be None on failure. This function sets module-level
    globals (DEVICE_PATH, set_dirty_map_path, execute_dirty_track, etc.) when
    the source module is available.
    """
    global DEVICE_PATH, set_dirty_map_path, ioctl_start_pid, ioctl_stop_pid, get_runc_container_pidtree, container_pids, container_may_dump_size, execute_dirty_track
    device_file = None
    device_fd = None
    dirtymap_path = None
    try:
        import importlib.util

        if repo_root is None:
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        src_path = os.path.join(os.path.dirname(__file__), "source.py")
        if os.path.exists(src_path):
            spec = importlib.util.spec_from_file_location("mig_scripts_source", src_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                # temporarily silence stdout/stderr and reset argv to avoid parse-time output
                old_argv = list(sys.argv)
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                try:
                    devnull = open(os.devnull, "w")
                    sys.stdout = devnull
                    sys.stderr = devnull
                    sys.argv = [sys.argv[0]]
                    try:
                        spec.loader.exec_module(module)
                    except SystemExit:
                        # some mig-scripts call parse_args() at import-time; ignore exit
                        pass
                    finally:
                        devnull.close()
                finally:
                    sys.argv = old_argv
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
                # extract expected symbols if available
                DEVICE_PATH = getattr(module, "DEVICE_PATH", None)
                set_dirty_map_path = getattr(module, "set_dirty_map_path", None)
                ioctl_start_pid = getattr(module, "ioctl_start_pid", None)
                ioctl_stop_pid = getattr(module, "ioctl_stop_pid", None)
                get_runc_container_pidtree = getattr(module, "get_runc_container_pidtree", None)
                container_pids = getattr(module, "container_pids", None)
                container_may_dump_size = getattr(module, "container_may_dump_size", None)
                execute_dirty_track = getattr(module, "execute_dirty_track", None)

        device_node = DEVICE_PATH if DEVICE_PATH else "/dev/dirty-track"
        if not os.path.exists(device_node):
            try_build_and_load_light_dt(repo_root)
        if os.path.exists(device_node):
            try:
                device_file = open(device_node, "wb")
                device_fd = device_file.fileno()
                dirtymap_path = f"/runc/containers/{container}/migrate/dirty_map"
                if get_runc_container_pidtree:
                    get_runc_container_pidtree(container)
                if set_dirty_map_path:
                    set_dirty_map_path(device_fd, dirtymap_path)
            except Exception:
                # ignore init failures; keep best-effort semantics
                device_file = None
                device_fd = None
                dirtymap_path = None
    except Exception:
        # silent on any loader error
        device_file = None
        device_fd = None
        dirtymap_path = None
    return device_file, device_fd, dirtymap_path


def main():
    parser = argparse.ArgumentParser(description="Run RPS x payload x pattern experiments")
    parser.add_argument("--container", required=False)
    parser.add_argument("--bench-template", required=False)
    parser.add_argument("--rps-list", required=False)
    parser.add_argument("--payload-sizes", default="64,512,4096", help="Comma-separated payload sizes. Default unit is bytes when no suffix given. Suffixes accepted: B, KB, MB (case-insensitive). Examples: 256B, 16KB, 1MB, 64")
    parser.add_argument(
        "--payload-modes",
        default="",
        help="Comma-separated payload modes to test (e.g. json,binary). If provided, will substitute {payload_mode} in the bench-template.",
    )
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
    parser.add_argument(
        "--tests-file",
        default="",
        help="Optional JSON file containing a list of test specifications. If provided, this script will spawn one rps_matrix run per entry and exit.",
    )
    args = parser.parse_args()

    # If not using --tests-file, enforce required options for single-run mode
    if not args.tests_file:
        missing = []
        if not args.container:
            missing.append("--container")
        if not args.bench_template:
            missing.append("--bench-template")
        if not args.rps_list:
            missing.append("--rps-list")
        if missing:
            parser.error(f"the following arguments are required when --tests-file is not used: {', '.join(missing)}")

    # If tests-file provided, interpret each entry and spawn this script per-entry.
    if args.tests_file:
        try:
            # allow files that may have a UTF-8 BOM
            with open(args.tests_file, "r", encoding="utf-8-sig") as tf:
                tests = json.load(tf)
        except Exception as e:
            print(f"Failed to load tests file {args.tests_file}: {e}")
            return

        # Build an overrides namespace from the current CLI so explicit CLI
        # arguments override values present in the JSON tests file.
        # We create a lightweight parser with the same option names but
        # default=None so we can detect which options were provided by the user.
        override_parser = argparse.ArgumentParser(add_help=False)
        override_parser.add_argument("--container", dest="container")
        override_parser.add_argument("--bench-template", dest="bench_template")
        override_parser.add_argument("--rps-list", dest="rps_list")
        override_parser.add_argument("--payload-sizes", dest="payload_sizes")
        override_parser.add_argument("--payload-modes", dest="payload_modes")
        override_parser.add_argument("--patterns", dest="patterns")
        override_parser.add_argument("--duration", dest="duration", type=int)
        override_parser.add_argument("--threads", dest="threads", type=int)
        override_parser.add_argument("--runs", dest="runs", type=int)
        override_parser.add_argument("--output", dest="output")
        override_parser.add_argument("--log-dir", dest="log_dir")
        override_parser.add_argument("--client-ip", dest="client_ip")
        # parse_known_args so we ignore unrelated args; this reads from sys.argv
        override_args, _ = override_parser.parse_known_args()
        # detect boolean flags presence on the CLI
        argv = sys.argv[1:]
        dirtymap_present = "--dirtymap" in argv
        remote_client_present = "--remote-client" in argv
        fetch_remote_log_present = "--fetch-remote-log" in argv
        dry_run_present = "--dry-run" in argv

        for entry in tests:
            # create a mutable copy and apply CLI overrides when present
            merged = dict(entry)
            # simple mapping of override attribute -> entry key
            to_copy = [
                "container",
                "bench_template",
                "rps_list",
                "payload_sizes",
                "payload_modes",
                "patterns",
                "duration",
                "threads",
                "runs",
                "output",
                "log_dir",
                "client_ip",
            ]
            for name in to_copy:
                val = getattr(override_args, name, None)
                if val is not None:
                    # normalize dest name for JSON keys that use hyphens
                    json_key = name
                    # bench_template and log_dir use same keys in JSON
                    merged[json_key] = val
            # override booleans if flag present on CLI
            if dirtymap_present:
                merged["dirtymap"] = True
            if remote_client_present:
                merged["remote_client"] = True
            if fetch_remote_log_present:
                merged["fetch_remote_log"] = True
            if dry_run_present:
                merged["dry_run"] = True
            # replace entry with merged view for downstream logic
            entry = merged
            # Ensure 'container' is specified in each test entry. Tests-file entries must set the container name
            # (this follows mig-scripts convention where container is the runc bundle name/path key).
            if not entry.get("container"):
                print(f"tests-file entry missing 'container' field: {entry.get('name','unnamed')}")
                continue
            # If requested, start backends using runc by container name (no docker/image/ports required)
            def start_runc_backends(names, dry_run=False):
                """Start runc backends by following the mig-scripts convention.

                Exact sequence (per mig-scripts/redis_test.py and influxdb_test.py):
                1) rm -rf /runc/containers/<name>
                2) cp -r /runc/containers/<name>.bak /runc/containers/<name>
                3) nohup recvtty -m <mode> /runc/containers/<name>/console.sock & echo $! > /tmp/recvtty_<name>.pid
                4) runc run --console-socket /runc/containers/<name>/console.sock -d -b /runc/containers/<name> <name>

                This function implements that sequence. In dry_run mode it prints the commands
                instead of executing them.
                """
                started = []
                if not names:
                    return started
                for name in names:
                    name = name.strip()
                    if not name:
                        continue
                    bundle_dir = os.path.join("/runc/containers", name)
                        # ensure any previous runc container record is removed (best-effort)
                        pre_kill_cmd = f"runc kill {shlex.quote(name)} || true"
                        pre_delete_cmd = f"runc delete {shlex.quote(name)} || true"
                        print(f"[backend] -> {pre_kill_cmd}")
                        print(f"[backend] -> {pre_delete_cmd}")
                        # Best-effort: try kill+delete and verify the container no longer
                        # appears in runc list. Retry a few times because runc state may
                        # be transient.
                        if not dry_run:
                            try:
                                run_cmd(pre_kill_cmd, quiet=True, ignore_error=True)
                            except Exception:
                                pass
                            try:
                                run_cmd(pre_delete_cmd, quiet=True, ignore_error=True)
                            except Exception:
                                pass
                            # also remove any leftover console socket to avoid recvtty conflicts
                            try:
                                console_sock = os.path.join(bundle_dir, "console.sock")
                                run_cmd(f"rm -f {shlex.quote(console_sock)}", quiet=True, ignore_error=True)
                            except Exception:
                                pass
                            # retry check: if container still shows in runc list, try a few more times
                            for _ in range(3):
                                try:
                                    chk = run_cmd(f"runc list | grep -w {shlex.quote(name)}", quiet=True, ignore_error=True)
                                    out = getattr(chk, "stdout", "") or ""
                                    if not out.strip():
                                        break
                                    # attempt again
                                    run_cmd(pre_kill_cmd, quiet=True, ignore_error=True)
                                    run_cmd(pre_delete_cmd, quiet=True, ignore_error=True)
                                    time.sleep(1)
                                except Exception:
                                    time.sleep(1)
                        # 1/2: reset bundle from .bak if available (best-effort)
                        rm_cmd = f"rm -rf {shlex.quote(bundle_dir)}"
                        cp_cmd = f"cp -r {shlex.quote(bundle_dir + '.bak')} {shlex.quote(bundle_dir)}"
                    recvtty_pidfile = f"/tmp/recvtty_{name}.pid"
                    console_sock = os.path.join(bundle_dir, "console.sock")
                    run_cmd_str = f"runc run --console-socket {shlex.quote(console_sock)} -d -b {shlex.quote(bundle_dir)} {shlex.quote(name)}"

                    print(f"[backend] -> {rm_cmd}")
                    print(f"[backend] -> {cp_cmd}")
                    print(f"[backend] -> nohup recvtty -m null {console_sock} > /dev/null 2>&1 & echo $! > {recvtty_pidfile}")
                    print(f"[backend] -> {run_cmd_str}")
                    if not dry_run:
                        try:
                            run_cmd(rm_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            run_cmd(cp_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        # Start recvtty to create the console socket; do not fail hard if it errors
                        try:
                            recvtty_cmd = f"nohup recvtty -m null {shlex.quote(console_sock)} > /dev/null 2>&1 & echo $! > {shlex.quote(recvtty_pidfile)}"
                            run_cmd(recvtty_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        # Finally run the container using the console socket
                        try:
                            run_cmd(run_cmd_str, quiet=False)
                            started.append(name)
                        except Exception as e:
                            print(f"Failed to runc run {name}: {e}")
                    else:
                        started.append(name)
                return started

            def stop_runc_backends(names, dry_run=False):
                """Stop runc backends using the mig-scripts cleanup pattern.

                Attempts to kill the recvtty pidfile (if present), unmount tmpfs and
                then runc kill/delete. All operations are best-effort and tolerate errors.
                """
                if not names:
                    return
                for name in names:
                    name = name.strip()
                    if not name:
                        continue
                    recvtty_pidfile = f"/tmp/recvtty_{name}.pid"
                    kill_recvtty = f"kill -9 $(cat {shlex.quote(recvtty_pidfile)}) 2>/dev/null || true"
                    # unmount any local tmpfs under /runc/containers/<name>/migrate
                    umount_note = f"# unmount tmpfs for {name} (best-effort)"
                    kill_cmd = f"runc kill {shlex.quote(name)} || true"
                    delete_cmd = f"runc delete {shlex.quote(name)} || true"
                    print(f"[backend] -> {kill_recvtty}")
                    print(f"[backend] -> {umount_note}")
                    print(f"[backend] -> {kill_cmd}")
                    print(f"[backend] -> {delete_cmd}")
                    if not dry_run:
                        try:
                            run_cmd(kill_recvtty, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            # unmount helper will quietly ignore failures
                            unmount_local_migration_tmpfs(name, ignore_error=True, quiet=True)
                        except Exception:
                            pass
                        try:
                            run_cmd(kill_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            run_cmd(delete_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass

            # Build command to call this script for the entry
            cmd = [sys.executable, os.path.abspath(__file__)]
            # map supported fields from entry to CLI args
            def add_flag(k, flag=None):
                v = entry.get(k)
                if v is None:
                    return
                fk = flag or f"--{k.replace('_', '-') }"
                if isinstance(v, bool):
                    if v:
                        cmd.append(fk)
                else:
                    # use extend to avoid rebinding outer 'cmd' in nested scope
                    cmd.extend([fk, str(v)])

            # required/typical fields
            add_flag("container")
            add_flag("bench_template", "--bench-template")
            add_flag("rps_list", "--rps-list")
            add_flag("payload_sizes", "--payload-sizes")
            add_flag("payload_modes", "--payload-modes")
            add_flag("patterns")
            add_flag("duration")
            add_flag("threads")
            add_flag("runs")
            # optional switches
            if entry.get("dirtymap"):
                cmd.append("--dirtymap")
            if entry.get("remote_client"):
                cmd.append("--remote-client")
                if entry.get("client_ip"):
                    cmd += ["--client-ip", entry.get("client_ip")]
            if entry.get("fetch_remote_log"):
                cmd.append("--fetch-remote-log")
            if entry.get("dry_run"):
                cmd.append("--dry-run")

            # output/logdir defaults are created under results/logs
            out = entry.get("output") or os.path.join("results", f"{entry.get('name','test')}_matrix.csv")
            logd = entry.get("log_dir") or os.path.join("logs", entry.get("name", "test"))
            cmd += ["--output", out, "--log-dir", logd]

            print("Spawning rps_matrix for test:", entry.get("name", "unnamed"))
            # Start backends via runc by container name (best-effort). We always
            # attempt to start the container for the test's `container` field.
            # Treat per-entry dry_run or global --dry-run as flags to avoid real actions.
            container_name = entry.get("container")
            entry_dry = bool(entry.get("dry_run"))
            started = start_runc_backends([container_name], dry_run=entry_dry or args.dry_run)
            if started:
                print(f"Started runc backend: {', '.join(started)}")

            print(" ", shlex.join(cmd))
            try:
                run_cmd(shlex.join(cmd), quiet=False)
            except Exception as e:
                print(f"Failed to run test {entry.get('name')}: {e}")
            finally:
                # Always attempt cleanup of the container we tried to start. The
                # stop helper is best-effort and tolerates missing/failed states.
                stop_runc_backends([container_name], dry_run=bool(entry.get("dry_run")) or args.dry_run)
        return

    def parse_size_token(tok: str) -> int:
        """Parse a size token that may have unit suffix. Default unit is bytes when no suffix.

        Returns size in bytes as int.
        Accepts integers or floats with optional suffix: b, kb/k, mb/m (case-insensitive).
        Examples:
            256B -> 256
            16KB -> 16384
            1.5MB -> 1572864
            64 -> 64 (default bytes)
        """
        if not tok:
            raise ValueError("empty size token")
        s = tok.strip()
        s_low = s.lower()
        # detect suffix
        unit = None
        num = s_low
        if s_low.endswith("kb"):
            unit = "kb"
            num = s_low[: -2]
        elif s_low.endswith("k") and not s_low.endswith("kb"):
            unit = "kb"
            num = s_low[: -1]
        elif s_low.endswith("mb"):
            unit = "mb"
            num = s_low[: -2]
        elif s_low.endswith("m") and not s_low.endswith("mb"):
            unit = "mb"
            num = s_low[: -1]
        elif s_low.endswith("b") and not s_low.endswith("kb") and not s_low.endswith("mb"):
            unit = "b"
            num = s_low[: -1]
        else:
            # no suffix -> default to bytes
            unit = "b"
            num = s_low
        try:
            val = float(num)
        except Exception:
            raise ValueError(f"invalid size token: {tok}")
        if unit == "b":
            bytes_val = int(val)
        elif unit == "kb":
            bytes_val = int(val * 1024)
        elif unit == "mb":
            bytes_val = int(val * 1024 * 1024)
        else:
            bytes_val = int(val)
        return bytes_val

    rps_values = [int(x) for x in args.rps_list.split(",") if x.strip()]
    # convert payload sizes to bytes (default unit KB)
    payloads = [parse_size_token(x) for x in args.payload_sizes.split(",") if x.strip()]
    patterns = [x for x in args.patterns.split(",") if x.strip()]
    payload_modes = [x for x in args.payload_modes.split(",") if x.strip()] if args.payload_modes else [""]

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    header = [
        "ts_utc",
        "rps",
        "payload_mode",
        "payload_bytes",
        "avg_payload_bytes",
        "median_payload_bytes",
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

    # Lazy-load migration / dirty-map helpers only when requested (encapsulated)
    if args.dirtymap:
        device_file, device_fd, dirtymap_path = load_dirtymap_helpers(args.container)

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
            # close device file if we opened one
            try:
                if device_file:
                    try:
                        device_file.close()
                    except Exception:
                        pass
            except NameError:
                # device_file may not be defined in some paths
                pass

    atexit.register(_cleanup)

    for rps in rps_values:
        for payload in payloads:
            for payload_mode in payload_modes:
                for pattern in patterns:
                    for run_idx in range(1, args.runs + 1):
                        print(
                            f"Run rps={rps} payload={payload} payload_mode={payload_mode or 'default'} pattern={pattern} run={run_idx}"
                        )
                    if args.dry_run:
                        pid = None
                    else:
                        pid = get_container_pid(args.container)
                    before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                    ts = int(time.time())
                    # include payload_mode in logname so different modes produce separate logs
                    safe_mode = payload_mode if payload_mode else "default"
                    local_logname = os.path.join(
                        args.log_dir, f"bench_{rps}_{payload}_{safe_mode}_{pattern}_{run_idx}_{ts}.log"
                    )
                    bench_cmd = args.bench_template.format(
                        rps=rps,
                        duration=args.duration,
                        threads=args.threads,
                        payload=payload,
                        pattern=pattern,
                        payload_mode=payload_mode,
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
                                # also emit a minimal METRIC_VALUES line so parsing shows payload_mode/size
                                df.write("METRIC_HEADER\tavg_payload_bytes\tmedian_payload_bytes\ttotal_ops\tops_per_sec\n")
                                df.write(f"METRIC_VALUES\t{payload}\t{payload}\t0\t0\n")
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
                    avg_payload_val = ""
                    median_payload_val = ""
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
                        if header and values:
                            # header and values are tab-separated strings
                            h_fields = header.split("\t")
                            v_fields = values.split("\t")
                            # map header to values
                            hv = dict(zip(h_fields, v_fields))
                            avg_payload_val = hv.get("avg_payload_bytes", "")
                            median_payload_val = hv.get("median_payload_bytes", "")
                            # keep bench_stats as before for compatibility
                            bench_stats = values.replace("\t", "|")
                        else:
                            bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
                    else:
                        bench_stats = ""
                    row = [
                        datetime.utcnow().isoformat() + "Z",
                        str(rps),
                        str(payload_mode),
                        str(payload),
                        str(avg_payload_val),
                        str(median_payload_val),
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
