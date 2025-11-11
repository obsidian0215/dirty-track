#!/usr/bin/env python3
"""
Run a full matrix of parameterized experiments (rate, payload, pattern, etc.)

This script generalizes the previous 'rps' focused tool into a generic
configuration matrix runner. It spawns benchmark processes, optionally
captures dirty-map/kernel snapshots, and can checkpoint+decode containers
for metric tests.

Usage example:
    python3 mig-scripts/config_matrix_experiment.py \
        --container myctr \
        --bench-template "python3 experiment/migration/redis/bench_sensoragg.py --threads {threads} --payload-mode json --rps {rate} --duration {duration} --payload-size {payload}" \
        --rps-list 10,50 --payload-sizes 64,512,4096 --patterns random,zeros --duration 30 --threads 4 --runs 3 --output results/matrix.csv --dirtymap

Notes:
- bench-template must include placeholders for the parameters you intend to vary.
- Use {rate} in your template if you want to be agnostic to whether rate means rps or framerate.
"""
import argparse
import sys
import atexit
import json
import os
import re
import shlex
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional

from cmd_utils import run_cmd, unmount_local_migration_tmpfs
from result_writer import extract_stats_from_output

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
                        pass
                    finally:
                        devnull.close()
                finally:
                    sys.argv = old_argv
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
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
                device_file = None
                device_fd = None
                dirtymap_path = None
    except Exception:
        device_file = None
        device_fd = None
        dirtymap_path = None
    return device_file, device_fd, dirtymap_path


def main():
    parser = argparse.ArgumentParser(description="Run a generic configuration matrix of experiments")
    parser.add_argument("--container", required=False)
    parser.add_argument("--bench-template", required=False)
    parser.add_argument("--rps-list", required=False)
    parser.add_argument("--payload-sizes", default="64,512,4096", help="Comma-separated payload sizes. Default unit is bytes when no suffix given. Suffixes accepted: B, KB, MB (case-insensitive).")
    parser.add_argument("--payload-modes", default="", help="Comma-separated payload modes to test (e.g. json,binary).")
    parser.add_argument("--patterns", default="random,zeros,repeat")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", default="results/matrix.csv")
    parser.add_argument("--log-dir", default="/tmp")
    parser.add_argument("--dirtymap", action="store_true")
    parser.add_argument("--client-ip", default="", help="IP of remote client where bench should run")
    parser.add_argument("--remote-client", action="store_true", help="If set, run bench on --client-ip via SSH instead of locally")
    parser.add_argument("--fetch-remote-log", action="store_true", help="When running on --remote-client, scp the remote log back to local --log-dir for parsing")
    parser.add_argument("--dry-run", action="store_true", help="Do not start benches; simulate runs locally and write small dry-run logs")
    parser.add_argument("--tests-file", default="", help="Optional JSON file containing a list of test specifications. If provided, this script will spawn one matrix run per entry and exit.")
    parser.add_argument("--framerate-list", default="", help="Optional comma-separated framerate list for video tests (fps). Used as alternative to --rps-list for some benches.")
    parser.add_argument("--frame-width", type=int, default=None, help="Frame width in pixels (forwarded from templates or resolution expansion).")
    parser.add_argument("--frame-height", type=int, default=None, help="Frame height in pixels (forwarded from templates or resolution expansion).")
    parser.add_argument("--analysis-intensity", default="", help="Optional analysis intensity string forwarded to video benches.")
    parser.add_argument("--resolution-list", default="", help="Optional comma-separated list of resolutions WxH (e.g. 1920x1080,1280x720). When provided, resolution_list drives payload/size variation for video benches by passing frame_width/frame_height values per-run.")
    args = parser.parse_args()
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def _bench_script_resolves(bench_cmd: str):
        try:
            parts = shlex.split(bench_cmd)
        except Exception:
            return False, bench_cmd
        if not parts:
            return False, bench_cmd
        if os.path.basename(parts[0]).startswith("python") and len(parts) > 1:
            script = parts[1]
        else:
            script = parts[0]
        cand = script
        if os.path.isabs(cand) and os.path.exists(cand):
            return True, cand
        cwd_cand = os.path.join(os.getcwd(), cand)
        if os.path.exists(cwd_cand):
            return True, cwd_cand
        repo_cand = os.path.join(repo_root, cand)
        if os.path.exists(repo_cand):
            return True, repo_cand
        return False, cand

    def _get_bench_supported_flags(script_path: str):
        flags = set()
        try:
            import re as _re
            with open(script_path, "r", encoding="utf-8", errors="ignore") as sf:
                src = sf.read()
            for m in _re.finditer(r"add_argument\(([^)]*)\)", src):
                args_text = m.group(1)
                for token in _re.findall(r"\'--[A-Za-z0-9\-]+'|\"--[A-Za-z0-9\-]+\"|--[A-Za-z0-9\-]+", args_text):
                    t = token.strip('"\'')
                    if t.startswith("--"):
                        flags.add(t.split()[0])
        except Exception:
            return set()
        return flags

    if not args.tests_file:
        missing = []
        if not args.container:
            missing.append("--container")
        if not args.bench_template:
            missing.append("--bench-template")
        if not args.rps_list and not args.framerate_list:
            missing.append("--rps-list or --framerate-list")
        if args.rps_list and args.framerate_list:
            parser.error("--rps-list and --framerate-list are mutually exclusive; provide only one")
        if missing:
            parser.error(f"the following arguments are required when --tests-file is not used: {', '.join(missing)}")

    if args.tests_file:
        try:
            with open(args.tests_file, "r", encoding="utf-8-sig") as tf:
                tests = json.load(tf)
        except Exception as e:
            print(f"Failed to load tests file {args.tests_file}: {e}")
            return

        override_parser = argparse.ArgumentParser(add_help=False)
        override_parser.add_argument("--container", dest="container")
        override_parser.add_argument("--bench-template", dest="bench_template")
        override_parser.add_argument("--rps-list", dest="rps_list")
        override_parser.add_argument("--framerate-list", dest="framerate_list")
        override_parser.add_argument("--payload-sizes", dest="payload_sizes")
        override_parser.add_argument("--payload-modes", dest="payload_modes")
        override_parser.add_argument("--patterns", dest="patterns")
        override_parser.add_argument("--duration", dest="duration", type=int)
        override_parser.add_argument("--threads", dest="threads", type=int)
        override_parser.add_argument("--runs", dest="runs", type=int)
        override_parser.add_argument("--output", dest="output")
        override_parser.add_argument("--log-dir", dest="log_dir")
        override_parser.add_argument("--client-ip", dest="client_ip")
        override_args, _ = override_parser.parse_known_args()
        argv = sys.argv[1:]
        dirtymap_present = "--dirtymap" in argv
        remote_client_present = "--remote-client" in argv
        fetch_remote_log_present = "--fetch-remote-log" in argv
        dry_run_present = "--dry-run" in argv

        for entry in tests:
            merged = dict(entry)
            to_copy = [
                "container",
                "bench_template",
                "rps_list",
                "framerate_list",
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
                    merged[name] = val
            if dirtymap_present:
                merged["dirtymap"] = True
            if remote_client_present:
                merged["remote_client"] = True
            if fetch_remote_log_present:
                merged["fetch_remote_log"] = True
            if dry_run_present:
                merged["dry_run"] = True
            entry = merged
            has_rps = entry.get("rps_list") is not None or entry.get("rps-list") is not None
            has_fr = entry.get("framerate_list") is not None or entry.get("framerate-list") is not None
            if has_rps and has_fr:
                print(f"tests-file entry '{entry.get('name', 'unnamed')}' invalid: both rps_list and framerate_list present; they are mutually exclusive. Skipping.")
                continue

            try:
                bt = entry.get("bench_template")
                if bt:
                    parts = shlex.split(bt)
                    if parts:
                        if os.path.basename(parts[0]).startswith("python") and len(parts) > 1:
                            script_token_index = 1
                        else:
                            script_token_index = 0
                        script = parts[script_token_index]
                        repo_cand = os.path.join(repo_root, script)
                        cwd_cand = os.path.join(os.getcwd(), script)
                        if os.path.exists(repo_cand):
                            parts[script_token_index] = repo_cand
                            entry["bench_template"] = " ".join(shlex.quote(p) for p in parts)
                        elif os.path.exists(cwd_cand):
                            parts[script_token_index] = cwd_cand
                            entry["bench_template"] = " ".join(shlex.quote(p) for p in parts)
            except Exception:
                pass

            if not entry.get("container"):
                print(f"tests-file entry missing 'container' field: {entry.get('name','unnamed')}")
                continue

            def start_runc_backends(names, dry_run=False):
                started = []
                if not names:
                    return started
                for name in names:
                    name = name.strip()
                    if not name:
                        continue
                    bundle_dir = os.path.join("/runc/containers", name)
                    pre_kill_cmd = f"runc kill {shlex.quote(name)} || true"
                    pre_delete_cmd = f"runc delete {shlex.quote(name)} || true"
                    print(f"[backend] -> {pre_kill_cmd}")
                    print(f"[backend] -> {pre_delete_cmd}")
                    if not dry_run:
                        try:
                            run_cmd(pre_kill_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            run_cmd(pre_delete_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            console_sock = os.path.join(bundle_dir, "console.sock")
                            run_cmd(f"rm -f {shlex.quote(console_sock)}", quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        for _ in range(3):
                            try:
                                chk = run_cmd(f"runc list | grep -w {shlex.quote(name)}", quiet=True, ignore_error=True)
                                out = getattr(chk, "stdout", "") or ""
                                if not out.strip():
                                    break
                                run_cmd(pre_kill_cmd, quiet=True, ignore_error=True)
                                run_cmd(pre_delete_cmd, quiet=True, ignore_error=True)
                                time.sleep(1)
                            except Exception:
                                time.sleep(1)
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
                        try:
                            recvtty_cmd = f"nohup recvtty -m null {shlex.quote(console_sock)} > /dev/null 2>&1 & echo $! > {shlex.quote(recvtty_pidfile)}"
                            run_cmd(recvtty_cmd, quiet=True, ignore_error=True)
                        except Exception:
                            pass
                        try:
                            run_cmd(run_cmd_str, quiet=False)
                            started.append(name)
                        except Exception as e:
                            print(f"Failed to runc run {name}: {e}")
                    else:
                        started.append(name)
                return started

            def stop_runc_backends(names, dry_run=False):
                if not names:
                    return
                for name in names:
                    name = name.strip()
                    if not name:
                        continue
                    recvtty_pidfile = f"/tmp/recvtty_{name}.pid"
                    kill_recvtty = f"kill -9 $(cat {shlex.quote(recvtty_pidfile)}) 2>/dev/null || true"
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

            cmd = [sys.executable, os.path.abspath(__file__)]
            def add_flag(k, flag=None):
                v = entry.get(k)
                if v is None:
                    return
                fk = flag or f"--{k.replace('_', '-') }"
                if isinstance(v, bool):
                    if v:
                        cmd.append(fk)
                else:
                    if isinstance(v, (list, tuple)):
                        v_str = ",".join(str(x) for x in v)
                    else:
                        v_str = str(v)
                    cmd.extend([fk, v_str])

            add_flag("container")
            add_flag("bench_template", "--bench-template")
            # Do not pass --payload-sizes to video benches that use framerate/frame dimensions
            bt_lower = (entry.get("bench_template") or "").lower()
            is_video_bench = "video_cache_realistic" in bt_lower or "bench_video" in bt_lower or entry.get("framerate_list") is not None or entry.get("framerate-list") is not None
            if not is_video_bench:
                add_flag("payload_sizes", "--payload-sizes")
            add_flag("payload_modes", "--payload-modes")
            add_flag("patterns")
            add_flag("duration")
            add_flag("threads")
            add_flag("runs")
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

            out = entry.get("output") or os.path.join("results", f"{entry.get('name','test')}_matrix.csv")
            logd = entry.get("log_dir") or os.path.join("logs", entry.get("name", "test"))
            try:
                os.makedirs(logd, exist_ok=True)
            except Exception:
                pass
            cmd += ["--output", out, "--log-dir", logd]

            # Forward any experiment-specific parameters from the template entry
            # (e.g. frame_width, frame_height, analysis_intensity) so bench
            # templates receive the exact parameters they expect.
            meta_keys = {
                "name",
                "container",
                "bench_template",
                "rps_list",
                "rps-list",
                "framerate_list",
                "framerate-list",
                "payload_sizes",
                "payload_modes",
                "patterns",
                "duration",
                "threads",
                "runs",
                "output",
                "log_dir",
                "client_ip",
                "dirtymap",
                "remote_client",
                "fetch_remote_log",
                "dry_run",
            }
            for k, v in entry.items():
                if k in meta_keys:
                    continue
                # Skip None/empty
                if v is None:
                    continue
                # already added via add_flag
                flag = f"--{k.replace('_', '-')}"
                if flag in cmd:
                    continue
                # booleans
                if isinstance(v, bool):
                    if v:
                        cmd.append(flag)
                    continue
                # lists/tuples -> comma string
                if isinstance(v, (list, tuple)):
                    v_str = ",".join(str(x) for x in v)
                else:
                    v_str = str(v)
                cmd.extend([flag, v_str])

            fr_val = entry.get("framerate_list") if entry.get("framerate_list") is not None else entry.get("framerate-list")
            rp_val = entry.get("rps_list") if entry.get("rps_list") is not None else entry.get("rps-list")
            def _norm_list_val(v):
                if v is None:
                    return None
                if isinstance(v, (list, tuple)):
                    return ",".join(str(x) for x in v)
                return str(v)

            fr_list = _norm_list_val(fr_val)
            rp_list = _norm_list_val(rp_val)
            if fr_list:
                if "--framerate-list" not in cmd:
                    cmd += ["--framerate-list", fr_list]
            elif rp_list:
                if "--rps-list" not in cmd:
                    cmd += ["--rps-list", rp_list]

            print("Spawning matrix for test:", entry.get("name", "unnamed"))
            container_name = entry.get("container")
            entry_dry = bool(entry.get("dry_run"))
            started = start_runc_backends([container_name], dry_run=entry_dry or args.dry_run)
            if started:
                print(f"Started runc backend: {', '.join(started)}")

            print(" ", shlex.join(cmd))
            try:
                if not (entry_dry or args.dry_run):
                    run_cmd(shlex.join(cmd), quiet=False)
                else:
                    print(f"[tests-file dry-run] skipping spawn for {entry.get('name')}")
            except Exception as e:
                print(f"Failed to run test {entry.get('name')}: {e}")
            finally:
                test_name = entry.get("name", "unnamed")
                is_metric_test = False
                if isinstance(test_name, str):
                    if test_name == "rps_metric" or test_name.endswith("_metric"):
                        is_metric_test = True

                if is_metric_test:
                    param_keys = ["rps_list", "framerate_list", "payload_sizes", "threads", "duration"]
                    parts = []
                    for k in param_keys:
                        v = entry.get(k)
                        if v is None:
                            continue
                        s = str(v)
                        s = s.replace(",", "+")
                        s = s.replace(" ", "_")
                        parts.append(f"{k}={s}")
                    paramstr = "__".join(parts) if parts else "default"
                    paramstr = re.sub(r"[^A-Za-z0-9._=-]+", "_", paramstr)
                    safe_container = re.sub(r"[^A-Za-z0-9._-]+", "_", str(container_name))
                    safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", str(test_name))
                    dump_dirname = f"{safe_container}_{safe_test}_{paramstr}_dump"
                    dump_base = os.path.join("results", dump_dirname)
                    if entry_dry or args.dry_run:
                        print(f"[tests-file dry-run] would create dump dir: {dump_base}")
                        chk_cmd = f"runc checkpoint --image-path image --work-path d_log --leave-running --tcp-established --shell-job {shlex.quote(str(container_name))}"
                        print(f"[tests-file dry-run] would run checkpoint (cwd={dump_base}): {chk_cmd}")
                        decode_script = os.path.join(repo_root, "mig-scripts", "decode_criu_memimages.py")
                        decode_cmd = f"{sys.executable} {shlex.quote(decode_script)} analyze {os.path.join(dump_base, 'image')} --output {os.path.join(dump_base, 'analysis.json')}"
                        print(f"[tests-file dry-run] would run decode: {decode_cmd}")
                    else:
                        try:
                            os.makedirs(dump_base, exist_ok=True)
                        except Exception:
                            pass
                        old_cwd = os.getcwd()
                        try:
                            os.chdir(dump_base)
                            chk_cmd = f"runc checkpoint --image-path image --work-path d_log --leave-running --tcp-established --shell-job {shlex.quote(str(container_name))}"
                            print(f"[checkpoint] -> {chk_cmd}")
                            try:
                                run_cmd(chk_cmd, quiet=False, ignore_error=True)
                            except Exception as e:
                                print(f"runc checkpoint failed for {container_name}: {e}")
                            try:
                                chk_res = run_cmd(chk_cmd, quiet=False, ignore_error=True)
                            except Exception as e:
                                chk_res = None
                                print(f"runc checkpoint raised exception for {container_name}: {e}")

                            def wait_for_image_dir(image_dir: str, timeout: int = 60, poll: float = 1.0) -> bool:
                                start = time.time()
                                while True:
                                    if os.path.isdir(image_dir):
                                        try:
                                            files = os.listdir(image_dir)
                                        except Exception:
                                            files = []
                                        if files:
                                            for fn in files:
                                                if fn.startswith("pagemap-") or fn.startswith("pages-") or fn.startswith("mm-"):
                                                    return True
                                            return True
                                    wl = os.path.join(dump_base, "d_log")
                                    if os.path.isdir(wl):
                                        try:
                                            for name in os.listdir(wl):
                                                p = os.path.join(wl, name)
                                                try:
                                                    with open(p, "r", encoding="utf-8", errors="ignore") as df:
                                                        txt = df.read()
                                                        if "Dumping finished" in txt or "Successfully wrote" in txt or "locked pages" in txt:
                                                            return True
                                                except Exception:
                                                    continue
                                        except Exception:
                                            pass
                                    if time.time() - start > timeout:
                                        return False
                                    time.sleep(poll)

                            image_path = os.path.join(dump_base, "image")
                            ok = wait_for_image_dir(image_path, timeout=60)
                            if not ok:
                                print(f"Image not found: {image_path}")
                                dlog_dir = os.path.join(dump_base, "d_log")
                                if os.path.isdir(dlog_dir):
                                    try:
                                        for fname in sorted(os.listdir(dlog_dir))[-3:]:
                                            p = os.path.join(dlog_dir, fname)
                                            print(f"--- d_log/{fname} (tail) ---")
                                            try:
                                                with open(p, "r", encoding="utf-8", errors="ignore") as df:
                                                    lines = df.read().splitlines()
                                                    for l in lines[-20:]:
                                                        print(l)
                                            except Exception:
                                                pass
                                    except Exception:
                                        pass
                                else:
                                    print(f"No work-path logs at {dlog_dir}")

                            else:
                                decode_script = os.path.join(repo_root, "mig-scripts", "decode_criu_memimages.py")
                                out_path = os.path.join(dump_base, "analysis.json")
                                decode_cmd = f"{shlex.quote(sys.executable)} {shlex.quote(decode_script)} analyze {shlex.quote(image_path)} --output {shlex.quote(out_path)}"
                                print(f"[per-combo decode] -> {decode_cmd}")
                                try:
                                    run_cmd(decode_cmd, quiet=False, ignore_error=True)
                                except Exception as e:
                                    print(f"decode_criu_memimages failed for {image_path}: {e}")
                        except Exception as e:
                            print(f"checkpoint/decode orchestration failed for {test_name}: {e}")
                        finally:
                            try:
                                os.chdir(old_cwd)
                            except Exception:
                                pass

                stop_runc_backends([container_name], dry_run=bool(entry.get("dry_run")) or args.dry_run)
        return

    def parse_size_token(tok: str) -> int:
        if not tok:
            raise ValueError("empty size token")
        s = tok.strip()
        s_low = s.lower()
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

    def parse_resolution_token(tok: str):
        """Parse a resolution token like '1920x1080' and return (width:int, height:int).

        Raises ValueError on invalid formats.
        """
        if not tok:
            raise ValueError("empty resolution token")
        s = tok.strip()
        if "x" not in s and "X" not in s:
            raise ValueError(f"invalid resolution token: {tok}")
        parts = re.split(r"[xX]", s)
        if len(parts) != 2:
            raise ValueError(f"invalid resolution token: {tok}")
        try:
            w = int(parts[0])
            h = int(parts[1])
        except Exception:
            raise ValueError(f"invalid resolution token: {tok}")
        return w, h

    if args.framerate_list:
        rate_name = "framerate"
        rate_values = [int(x) for x in args.framerate_list.split(",") if x.strip()]
    else:
        rate_name = "rps"
        rate_values = [int(x) for x in (args.rps_list or "").split(",") if x.strip()]
    payloads = [parse_size_token(x) for x in args.payload_sizes.split(",") if x.strip()]
    patterns = [x for x in args.patterns.split(",") if x.strip()]
    payload_modes = [x for x in args.payload_modes.split(",") if x.strip()] if args.payload_modes else [""]

    # resolution_list: optional comma-separated list of WxH tokens. When
    # provided, these resolutions will be used to vary frame size per-run
    # (video benches should compute payload based on frame dimensions).
    resolution_list = [x for x in args.resolution_list.split(",") if x.strip()] if getattr(args, "resolution_list", None) else []
    resolution_tuples = []
    if resolution_list:
        for tok in resolution_list:
            try:
                resolution_tuples.append(parse_resolution_token(tok))
            except Exception as e:
                print(f"warning: invalid resolution token '{tok}': {e}")

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except Exception:
        pass
    header = [
        "ts_utc",
        "rate_name",
        "rate_value",
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
    header += [
        "dump_total_tracked_pages",
        "dump_total_writes",
        "dump_hot_pages",
        "dump_analysis_path",
        "dump_vmrss_before_kb",
        "dump_vmsize_before_kb",
        "dump_vmrss_after_kb",
        "dump_vmsize_after_kb",
    ]
    if not os.path.exists(args.output):
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(",".join(header) + "\n")

    device_file = None
    device_fd = None
    dirtymap_path = None
    if args.dirtymap:
        device_file, device_fd, dirtymap_path = load_dirtymap_helpers(args.container)

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
            try:
                if device_file:
                    try:
                        device_file.close()
                    except Exception:
                        pass
            except NameError:
                pass

    atexit.register(_cleanup)

    # If resolution_tuples is provided, use resolutions to vary frame sizes
    # (video benches should compute payload based on resolution+framerate).
    if resolution_tuples:
        for rate in rate_values:
            for (fw, fh) in resolution_tuples:
                for payload_mode in payload_modes:
                    for pattern in patterns:
                        for run_idx in range(1, args.runs + 1):
                            print(f"Run {rate_name}={rate} resolution={fw}x{fh} payload_mode={payload_mode or 'default'} pattern={pattern} run={run_idx}")
                        if args.dry_run:
                            pid = None
                        else:
                            pid = get_container_pid(args.container)
                        before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                        ts = int(time.time())
                        safe_mode = payload_mode if payload_mode else "default"
                        local_logname = os.path.join(args.log_dir, f"bench_{rate}_{fw}x{fh}_{safe_mode}_{pattern}_{run_idx}_{ts}.log")
                        fmt_kwargs = {
                            "duration": args.duration,
                            "threads": args.threads,
                            "pattern": pattern,
                            "payload_mode": payload_mode,
                            "frame_width": fw,
                            "frame_height": fh,
                        }
                        if rate_name == "rps":
                            fmt_kwargs["rps"] = rate
                        else:
                            fmt_kwargs["framerate"] = rate
                        bench_template_used = args.bench_template
                        # If we are driving by resolution and template still contains a
                        # {payload} placeholder, remove common --payload-size occurrences
                        # so formatting doesn't KeyError. Video benches will compute
                        # payload from frame dimensions themselves.
                        if "payload" not in fmt_kwargs and "{payload}" in bench_template_used:
                            # remove variants like: --payload-size {payload}  or  --payload-size='{payload}'  or  --payload-size="{payload}"
                            bench_template_used = re.sub(r"--payload-size\s*(?:=\s*)?(?:'\{payload\}'|\"\{payload\}\"|\{payload\})", "", bench_template_used)
                            bench_template_used = bench_template_used.replace("{payload}", "")
                            bench_template_used = re.sub(r"\s{2,}", " ", bench_template_used).strip()
                        bench_cmd = bench_template_used.format(**fmt_kwargs)
    else:
        for rate in rate_values:
            for payload in payloads:
                for payload_mode in payload_modes:
                    for pattern in patterns:
                        for run_idx in range(1, args.runs + 1):
                            print(f"Run {rate_name}={rate} payload={payload} payload_mode={payload_mode or 'default'} pattern={pattern} run={run_idx}")
                        if args.dry_run:
                            pid = None
                        else:
                            pid = get_container_pid(args.container)
                        before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                        ts = int(time.time())
                        safe_mode = payload_mode if payload_mode else "default"
                        local_logname = os.path.join(args.log_dir, f"bench_{rate}_{payload}_{safe_mode}_{pattern}_{run_idx}_{ts}.log")
                        fmt_kwargs = {
                            "duration": args.duration,
                            "threads": args.threads,
                            "payload": payload,
                            "pattern": pattern,
                            "payload_mode": payload_mode,
                        }
                        if rate_name == "rps":
                            fmt_kwargs["rps"] = rate
                        else:
                            fmt_kwargs["framerate"] = rate
                        bench_template_used = args.bench_template
                        if "payload" not in fmt_kwargs and "{payload}" in bench_template_used:
                            bench_template_used = re.sub(r"--payload-size\s*(?:=\s*)?(?:'\{payload\}'|\"\{payload\}\"|\{payload\})", "", bench_template_used)
                            bench_template_used = bench_template_used.replace("{payload}", "")
                            bench_template_used = re.sub(r"\s{2,}", " ", bench_template_used).strip()
                        bench_cmd = bench_template_used.format(**fmt_kwargs)

                    try:
                        ok_resolve, tried_path = _bench_script_resolves(bench_cmd)
                        supported = set()
                        if ok_resolve:
                            supported = _get_bench_supported_flags(tried_path)
                        if supported:
                            print(f"[bench-adapt] {tried_path} supports flags: {', '.join(sorted(supported))}")
                            video_flags = {"--framerate", "--frame-width", "--frame-height"}
                            if (supported & video_flags) and "--payload-size" not in supported:
                                try:
                                    parts = shlex.split(bench_cmd)
                                except Exception:
                                    parts = []
                                if parts:
                                    new_parts = []
                                    skip_next = False
                                    for tok in parts:
                                        if skip_next:
                                            skip_next = False
                                            continue
                                        if tok.startswith("--payload-size"):
                                            if "=" in tok:
                                                continue
                                            else:
                                                skip_next = True
                                                continue
                                        new_parts.append(tok)
                                    if new_parts:
                                        bench_cmd = " ".join(shlex.quote(p) for p in new_parts)
                    except Exception:
                        pass

                    bench_pid = None
                    remote_logname = local_logname
                    client_target = args.client_ip

                    if args.dry_run:
                        print(f"DRY-RUN: would run: {bench_cmd} -> {local_logname}")
                        try:
                            with open(local_logname, "w", encoding="utf-8") as df:
                                df.write("DRY-RUN\n")
                                df.write(bench_cmd + "\n")
                                df.write("METRIC_HEADER\tavg_payload_bytes\tmedian_payload_bytes\ttotal_ops\tops_per_sec\n")
                                df.write(f"METRIC_VALUES\t{payload}\t{payload}\t0\t0\n")
                        except Exception:
                            pass
                    else:
                        if args.remote_client:
                            if not args.client_ip:
                                print("--remote-client set but --client-ip is empty; skipping run")
                                continue
                            if "@" not in args.client_ip:
                                client_target = f"root@{args.client_ip}"
                            remote_logname = f"/tmp/bench_{rate}_{payload}_{pattern}_{run_idx}_{ts}.log"
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
                            ok, tried = _bench_script_resolves(bench_cmd)
                            if not ok:
                                print(f"Bench script not found locally (tried: {tried}). Skipping run.\n  Tip: run this from the repo root or use an absolute path in --bench-template.")
                                continue
                            try:
                                parts = shlex.split(bench_cmd)
                                if os.path.basename(parts[0]).startswith("python") and len(parts) > 1:
                                    parts[1] = tried
                                else:
                                    parts[0] = tried
                                bench_cmd = " ".join(shlex.quote(p) for p in parts)
                            except Exception:
                                pass
                            print(f"Starting local: {bench_cmd} -> log {local_logname}")
                            bench_pid = run_bench_background(bench_cmd, local_logname)
                            if bench_pid is None:
                                print("bench failed to start; skipping")
                                continue

                    ramp = max(2, min(8, args.duration // 6))
                    time.sleep(ramp)
                    mid = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                    dirty_bytes = ""
                    if args.dirtymap and device_fd and execute_dirty_track and container_pids and container_may_dump_size and dirtymap_path:
                        try:
                            execute_dirty_track(device_fd, first=True)
                            dirty_bytes = str(container_may_dump_size(container_pids, dirtymap_path))
                        except Exception as e:
                            print(f"per-run dirtymap capture failed: {e}")
                            dirty_bytes = ""

                    if args.remote_client and bench_pid is not None:
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
                    target_log = local_logname
                    content = None
                    if args.remote_client:
                        if args.fetch_remote_log:
                            try:
                                scp_cmd = f"scp {args.client_ip}:{shlex.quote(remote_logname)} {shlex.quote(local_logname)}"
                                run_cmd(scp_cmd, quiet=True)
                                target_log = local_logname
                            except Exception as e:
                                print(f"Failed to scp remote log: {e}")
                                target_log = remote_logname
                        else:
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
                        header_s, values = extract_stats_from_output(content)
                        if header_s and values:
                            h_fields = header_s.split("\t")
                            v_fields = values.split("\t")
                            hv = dict(zip(h_fields, v_fields))
                            avg_payload_val = hv.get("avg_payload_bytes", "")
                            median_payload_val = hv.get("median_payload_bytes", "")
                            bench_stats = values.replace("\t", "|")
                        else:
                            bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
                    else:
                        bench_stats = ""

                    row = [
                        datetime.utcnow().isoformat() + "Z",
                        rate_name,
                        str(rate),
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
                    row += ["", "", "", "", "", "", "", ""]
                    with open(args.output, "a", encoding="utf-8") as f:
                        f.write(",".join(row) + "\n")

                    print(f"Finished run, wrote row to {args.output}")
                    time.sleep(3)

    # end main


if __name__ == "__main__":
    main()
