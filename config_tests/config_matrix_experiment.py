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


                            if entry_dry or args.dry_run:
Notes:
- bench-template must include placeholders for the parameters you intend to vary.
- Use {rate} in your template if you want to be agnostic to whether rate means rps or framerate.
"""
import argparse
import os
import sys
import time
import shlex
import json
import glob
import re
import atexit
from datetime import datetime
from typing import Optional, Dict, List

import subprocess


def run_cmd(cmd: str, quiet: bool = False, ignore_error: bool = False, timeout: Optional[int] = None):
    """Run `cmd` in the shell and return a simple result object.

    The returned object exposes `stdout`, `stderr` and `returncode` attributes.
    When `quiet` is False the command is printed before execution. When
    `ignore_error` is True failures will not raise an exception.
    """
    class _R:
        def __init__(self, stdout: str, stderr: str, returncode: int):
            self.stdout = stdout
            self.stderr = stderr
            self.returncode = returncode

    if not quiet:
        try:
            print(f"$ {cmd}")
        except Exception:
            pass
    try:
        completed = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return _R(getattr(completed, 'stdout', '') or '', getattr(completed, 'stderr', '') or '', getattr(completed, 'returncode', 1))
    except subprocess.TimeoutExpired as e:
        if ignore_error:
            return _R(getattr(e, 'stdout', '') or '', getattr(e, 'stderr', '') or '', 124)
        raise
    except Exception as e:
        if ignore_error:
            return _R('', str(e), 1)
        raise


def get_container_pid(container_name: str) -> Optional[int]:
    """Return the host PID of a runc container name, or None if not found.

    Tries `runc state` and parses JSON output when available; falls back to
    a regex search in stdout/stderr for older runc versions.
    """
    if not container_name:
        return None
    try:
        res = run_cmd(f"runc state {shlex.quote(container_name)}", quiet=True, ignore_error=True)
    except Exception:
        return None
    out = (getattr(res, 'stdout', '') or '') + "\n" + (getattr(res, 'stderr', '') or '')
    try:
        # runc state prints JSON with a 'pid' field on modern versions
        j = json.loads(out)
        pid = j.get('pid')
        if pid:
            return int(pid)
    except Exception:
        pass
    # fallback regex: look for 'pid': N or pid: N
    m = re.search(r'"pid"\s*:\s*(\d+)', out)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            pass
    m2 = re.search(r'pid\s*[:=]\s*(\d+)', out)
    if m2:
        try:
            return int(m2.group(1))
        except Exception:
            pass
    return None


def _effective_param(key: str, ctx_entry: Optional[dict], args_obj, local_vars: dict):
    """Return the effective single value for `key` for this run.

    Priority: runtime local_vars (non-empty, single) -> ctx_entry dict -> args_obj attr.
    If a comma-separated string is encountered, prefer the first element (current run).
    Returns None when no value available.
    """
    if not key:
        return None
    k_underscore = key.replace('-', '_')
    try:
        # Prefer a local runtime value (this is usually the loop variable)
        v_local = None
        try:
            v_local = local_vars.get(key)
        except Exception:
            v_local = None
        if v_local is None:
            try:
                v_local = local_vars.get(k_underscore)
            except Exception:
                v_local = None
        if v_local:
            if isinstance(v_local, str) and ',' in v_local:
                parts = [x.strip() for x in v_local.split(',') if x.strip()]
                if parts:
                    return parts[0]
            return v_local

        # Next prefer tests-file entry
        if isinstance(ctx_entry, dict):
            try:
                v_entry = ctx_entry.get(key) or ctx_entry.get(k_underscore)
            except Exception:
                v_entry = None
            if v_entry:
                if isinstance(v_entry, str) and ',' in v_entry:
                    parts = [x.strip() for x in v_entry.split(',') if x.strip()]
                    if parts:
                        return parts[0]
                return v_entry

        # Finally, fallback to top-level CLI args
        try:
            v_arg = getattr(args_obj, k_underscore, None)
        except Exception:
            v_arg = None
        if v_arg:
            if isinstance(v_arg, str) and ',' in v_arg:
                parts = [x.strip() for x in v_arg.split(',') if x.strip()]
                if parts:
                    return parts[0]
            return v_arg
    except Exception:
        pass
    return None


def _effective_pattern(ctx_entry: Optional[dict], args_obj, local_vars: dict):
    """Return the first available pattern-like value for this run.

    Checks in order: 'pattern', 'size-distribution'/'size_distribution',
    'vehicle-pattern'/'vehicle_pattern', 'sensors-per-device'/'sensors_per_device'.
    Uses _effective_param to apply the same resolution rules.
    """
    alt_keys = [
        "pattern",
        "size-distribution",
        "size_distribution",
        "vehicle-pattern",
        "vehicle_pattern",
        "sensors-per-device",
        "sensors_per_device",
    ]
    for ak in alt_keys:
        try:
            v = _effective_param(ak, ctx_entry, args_obj, local_vars)
            if v:
                return v
        except Exception:
            continue
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


def _start_one_backend(container_name: str, repo_root: str, dry_run: bool = False, log_dir: Optional[str] = None):
    """Start a single runc backend for the given container name.

    This mirrors the logic used in the tests-file start_runc_backends helper but
    is safe to call from the main matrix loop. It is best-effort and swallows
    errors so callers can call it liberally.
    """

    # local paths used for runc bundle and helper pidfile
    bundle_dir = os.path.join("/runc/containers", container_name)
    recvtty_pidfile = f"/tmp/recvtty_{container_name}.pid"
    console_sock = os.path.join(bundle_dir, "console.sock")

    try:
        # ensure no stale container with same name
        try:
            ensure_container_quiescent(container_name, bundle_dir, dry_run_local=False)
        except Exception:
            pass
        # restore bundle from .bak
        try:
            run_cmd(f"rm -rf {shlex.quote(bundle_dir)}", quiet=True, ignore_error=True)
        except Exception:
            pass
        try:
            run_cmd(f"cp -r {shlex.quote(bundle_dir + '.bak')} {shlex.quote(bundle_dir)}", quiet=True, ignore_error=True)
        except Exception:
            pass
        try:
            run_cmd(f"rm -f {shlex.quote(console_sock)}", quiet=True, ignore_error=True)
        except Exception:
            pass
        # start recvtty helper and runc run
        try:
            recvtty_cmd = f"nohup recvtty -m null {shlex.quote(console_sock)} > /dev/null 2>&1 & echo $! > {shlex.quote(recvtty_pidfile)}"
            run_cmd(recvtty_cmd, quiet=True, ignore_error=True)
        except Exception:
            pass

        try:
            # Try starting the container, retrying if console.sock is not yet
            # available (recvtty not ready). On failure attempt to (re)start
            # recvtty and retry a few times before giving up.
            run_success = False
            runc_cmd = f"runc run --console-socket {shlex.quote(console_sock)} -d -b {shlex.quote(bundle_dir)} {shlex.quote(container_name)}"
            for attempt in range(3):
                try:
                    res = run_cmd(runc_cmd, quiet=False, ignore_error=True)
                except Exception:
                    res = None
                rc = getattr(res, "returncode", 1) if res is not None else 1
                if rc == 0:
                    run_success = True
                    break
                # If we see a console.sock/connect error, try restarting recvtty
                out = (getattr(res, "stdout", "") or "") if res is not None else ""
                err = (getattr(res, "stderr", "") or "") if res is not None else ""
                if "console.sock" in out or "console.sock" in err or "connect: no such file" in err or "connect: no such file" in out:
                    try:
                        recvtty_cmd = f"nohup recvtty -m null {shlex.quote(console_sock)} > /dev/null 2>&1 & echo $! > {shlex.quote(recvtty_pidfile)}"
                        run_cmd(recvtty_cmd, quiet=True, ignore_error=True)
                    except Exception:
                        pass
                    time.sleep(1)
                else:
                    # Non-console.sock error; wait a bit and retry anyway
                    time.sleep(1)
            if not run_success:
                print(f"Warning: failed to runc run {container_name} after retries; last rc={getattr(res, 'returncode', 'unknown')}")
        except Exception:
            pass
    except Exception:
        pass


def _stop_one_backend(container_name: str, dry_run: bool = False):
    if dry_run:
        return
    try:
        recvtty_pidfile = f"/tmp/recvtty_{container_name}.pid"
        kill_recvtty = f"kill -9 $(cat {shlex.quote(recvtty_pidfile)}) 2>/dev/null || true"
        try:
            run_cmd(kill_recvtty, quiet=True, ignore_error=True)
        except Exception:
            pass
        try:
            run_cmd(f"runc kill {shlex.quote(container_name)} || true", quiet=True, ignore_error=True)
        except Exception:
            pass
        try:
            ensure_container_quiescent(container_name, os.path.join("/runc/containers", container_name), dry_run_local=False)
        except Exception:
            pass
        try:
            run_cmd(f"runc delete {shlex.quote(container_name)} || true", quiet=True, ignore_error=True)
        except Exception:
            pass
    except Exception:
        pass


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


def ensure_container_quiescent(ct_name: str, bundle_dir: str, dry_run_local: bool = False) -> None:
    """Best-effort: ensure a runc container with name `ct_name` is not running/listed.

    This helper follows a safe escalation path and is idempotent:
      1. Check if the container name appears in `runc list`.
      2. Try `runc kill` (best-effort).
      3. If still present, attempt to resolve host PID via `runc state` and `kill -9`.
      4. Try `runc delete` (best-effort).
      5. Repeat a few times to recover from races.

    The function swallows errors on purpose so callers can call it liberally without
    failing the entire experiment orchestration.
    """
    if dry_run_local:
        return
    try:
        # Loop a few times to allow runc to settle if it's in the middle of state change
        for _ in range(6):
            try:
                chk = run_cmd(f"runc list | grep -w {shlex.quote(ct_name)}", quiet=True, ignore_error=True)
                out = (getattr(chk, "stdout", "") or "").strip()
            except Exception:
                out = ""
            if not out:
                # not listed
                break

            # Try graceful kill via runc
            try:
                run_cmd(f"runc kill {shlex.quote(ct_name)} || true", quiet=True, ignore_error=True)
            except Exception:
                pass

            # If still present, escalate to host PID kill
            try:
                pid = get_container_pid(ct_name)
            except Exception:
                pid = None
            if pid:
                try:
                    run_cmd(f"kill -9 {pid} || true", quiet=True, ignore_error=True)
                except Exception:
                    pass

            time.sleep(0.5)

        # Finally, attempt delete
        try:
            run_cmd(f"runc delete {shlex.quote(ct_name)} || true", quiet=True, ignore_error=True)
        except Exception:
            pass
    except Exception:
        # never fail callers
        pass




def _has_criu_image_files(dump_base: str) -> bool:
    """Return True if CRIU image files (pagemap/mm/pages) appear under the dump.

    Immediate, deterministic check: when `runc checkpoint` reports success
    the `image/` artifacts are expected to be present. This helper performs a
    direct filesystem check without retries so failures are surfaced for
    diagnosis.
    """
    # Normalize to an absolute path to avoid surprises when caller has
    # already switched cwd into the dump directory. Using an absolute
    # path ensures joins are deterministic.
    try:
        dump_base = os.path.abspath(dump_base)
    except Exception:
        pass
    image_dir = os.path.join(dump_base, "image")
    # (temporary debug print removed)
    try:
        # Prefer the explicit image/ dir being present and non-empty
        if os.path.isdir(image_dir):
            try:
                if os.listdir(image_dir):
                    return True
            except Exception:
                pass
    except Exception:
        pass

    # Fall back to matching common CRIU image patterns at dump root or image/
    patterns = [
        os.path.join(image_dir, "pagemap-*.img"),
        os.path.join(image_dir, "mm-*.img"),
        os.path.join(image_dir, "pages-*.img"),
        os.path.join(dump_base, "pagemap-*.img"),
        os.path.join(dump_base, "mm-*.img"),
        os.path.join(dump_base, "pages-*.img"),
    ]
    for p in patterns:
        try:
            if glob.glob(p):
                return True
        except Exception:
            pass
    return False


def _host_http_probe(url: str, retries: int = 5, delay: float = 1.0) -> bool:
    """Probe an HTTP endpoint on the host using curl from the same shell.

    Return True if a 2xx HTTP status is observed. On failure print diagnostic
    output (including a verbose curl trace on the last attempt).
    """
    for i in range(1, retries + 1):
        try:
            # Support HTTP and Redis-style probes. For HTTP use curl and expect
            # a 2xx status code. For Redis use `redis-cli PING` and expect 'PONG'.
            if url.startswith("redis://"):
                # Format: redis://host:port
                rp = url[len("redis://") :]
                if ":" in rp:
                    host, port = rp.split(":", 1)
                else:
                    host = rp
                    port = "6379"
                # First try redis-cli PING when available
                cmd = f"redis-cli -h {shlex.quote(host)} -p {shlex.quote(port)} PING"
                res = run_cmd(cmd, quiet=True, ignore_error=True)
                out = (getattr(res, "stdout", "") or "").strip()
                if out and out.upper().startswith("PONG"):
                    print(f"[host-probe] {url} reachable (PONG)")
                    return True

                # Fallback: try nc if present (zero/exit success indicates reachable)
                try:
                    nc_cmd = f"command -v nc >/dev/null 2>&1 && nc -z -w 2 {shlex.quote(host)} {shlex.quote(port)} && echo OK || echo FAIL"
                    nc_res = run_cmd(nc_cmd, quiet=True, ignore_error=True)
                    nc_out = (getattr(nc_res, "stdout", "") or "").strip()
                    if nc_out == "OK":
                        print(f"[host-probe] {url} reachable (nc)")
                        return True
                except Exception:
                    pass

                # Final fallback: use bash /dev/tcp method
                try:
                    devtcp_cmd = (
                        "bash -c '" +
                        f"(echo > /dev/tcp/{host}/{port}) >/dev/null 2>&1 && echo OK || echo FAIL'"
                    )
                    dt_res = run_cmd(devtcp_cmd, quiet=True, ignore_error=True)
                    dt_out = (getattr(dt_res, "stdout", "") or "").strip()
                    if dt_out == "OK":
                        print(f"[host-probe] {url} reachable (/dev/tcp)")
                        return True
                except Exception:
                    pass

                print(f"[host-probe] attempt {i}/{retries}: {url} returned '{out}'")
            else:
                # Use curl to return only the status code. Avoid `-D -` which prints
                # response headers to stdout and can confuse parsing (we saw headers
                # plus the code in logs causing isdigit() to fail).
                cmd = f"curl -sS -o /dev/null -w '%{{http_code}}' {shlex.quote(url)}"
                res = run_cmd(cmd, quiet=True, ignore_error=True)
                out = (getattr(res, "stdout", "") or "").strip()
                if out and out.isdigit() and out.startswith("2"):
                    print(f"[host-probe] {url} reachable (HTTP {out})")
                    return True
                else:
                    print(f"[host-probe] attempt {i}/{retries}: {url} returned '{out}'")
        except Exception as e:
            print(f"[host-probe] attempt {i}/{retries} raised: {e}")
        time.sleep(delay)

    # Final verbose diagnostic
    try:
        if url.startswith("redis://"):
            rp = url[len("redis://") :]
            if ":" in rp:
                host, port = rp.split(":", 1)
            else:
                host = rp
                port = "6379"
            print(f"[host-probe] final verbose redis-cli PING to {host}:{port} for diagnostics:")
            dbg_cmd = f"redis-cli -h {shlex.quote(host)} -p {shlex.quote(port)} PING"
            try:
                dbg_res = run_cmd(dbg_cmd, quiet=False, ignore_error=True)
                _ = getattr(dbg_res, "stdout", "")
            except Exception:
                pass
        else:
            print(f"[host-probe] final verbose curl to {url} for diagnostics:")
            dbg_cmd = f"curl -v --max-time 3 {shlex.quote(url)}"
            try:
                dbg_res = run_cmd(dbg_cmd, quiet=False, ignore_error=True)
                _ = getattr(dbg_res, "stdout", "")
            except Exception:
                pass
    except Exception:
        pass
    print(f"[host-probe] host probe failed for {url} after {retries} attempts")
    return False


def _probe_url_for(container_name: Optional[str], bench_cmd: str, client_ip: Optional[str] = None) -> Optional[str]:
    """Return a sensible host probe URL for the given container/bench.

    Returns None when no host probe should be attempted for this combo.
    """
    try:
        cand = (container_name or "").lower()
        host = client_ip or "localhost"
        # InfluxDB (our test setup) exposes a health endpoint on 8181
        if "influx" in cand:
            return f"http://{host}:8181/health"
        # Elasticsearch usually listens on 9200 for HTTP; use cluster health endpoint
        if "elastic" in cand or "elasticsearch" in cand:
            return f"http://{host}:9200/_cluster/health?local=true"
        # Redis: use redis-cli PING (return a redis:// scheme interpreted by probe)
        if "redis" in cand:
            # Prefer explicit client_ip when provided (probe that host:port)
            return f"redis://{host}:6379"
        # For other containers/benches, do not probe by default
        return None
    except Exception:
        return None


def _should_skip_due_to_probe(container_name: Optional[str], bench_cmd: str, client_ip: Optional[str] = None, retries: int = 5, delay: float = 1.0) -> bool:
    """Return True when a host-side probe exists for this container/bench and fails.

    This wraps _probe_url_for + _host_http_probe to centralize error handling
    and avoid duplicating the probe logic in multiple branches.
    """
    try:
        probe_url = _probe_url_for(container_name, bench_cmd, client_ip)
    except Exception:
        return False
    if not probe_url:
        return False
    try:
        return not _host_http_probe(probe_url, retries=retries, delay=delay)
    except Exception:
        # If the probe helper itself fails, do not abort the run.
        return False


def _checkpoint_and_decode(ctx_container: str, ctx_entry: Optional[dict], repo_root: str, dump_base: str, args, leave_running: bool = False) -> bool:
    """Perform runc checkpoint into `dump_base` and run the CRIU decode helper.

    Returns True when decode succeeded (or was skipped in dry-run); False otherwise.
    This centralizes the orchestration that used to be duplicated across branches.
    """
    try:
        # Create dump dir
        try:
            os.makedirs(dump_base, exist_ok=True)
        except Exception:
            pass
        old_cwd = os.getcwd()
        try:
            # Ensure container running (attempt to start backend once if missing)
            cur_pid = get_container_pid(ctx_container)
            if not cur_pid:
                try:
                    _start_one_backend(ctx_container, repo_root, dry_run=False, log_dir=getattr(args, "log_dir", "."))
                except Exception:
                    pass
                time.sleep(1)
                cur_pid = get_container_pid(ctx_container)
            if not cur_pid:
                print(f"[checkpoint] aborting checkpoint: container {ctx_container} not present")
                return False

            # Compute absolute dump path before changing cwd to avoid
            # accidental path duplication when callers pass a relative
            # `dump_base`. Use `abs_dump` for all subsequent filesystem
            # checks and for passing into helpers.
            try:
                abs_dump = os.path.abspath(dump_base)
            except Exception:
                abs_dump = dump_base
            # Switch into dump dir and run checkpoint
            try:
                os.chdir(abs_dump)
            except Exception:
                # Fallback to original dump_base when chdir fails
                try:
                    os.chdir(dump_base)
                except Exception:
                    pass
            # Determine extra checkpoint opts (forward per-entry or top-level flag)
            def _local_extra_opts(entry_dict, args_local):
                opts_l = []
                try:
                    if entry_dict and bool(entry_dict.get("checkpoint_file_locks", False)):
                        opts_l.append("--file-locks")
                    elif getattr(args_local, "checkpoint_file_locks", False):
                        opts_l.append("--file-locks")
                except Exception:
                    pass
                return " ".join(opts_l)

            extra = _local_extra_opts(ctx_entry, args)
            base_opts = "--image-path image --work-path d_log --tcp-established --shell-job"
            if leave_running:
                base_opts = base_opts + " --leave-running"
            if extra:
                base_opts = base_opts + " " + extra
            chk_cmd = f"runc checkpoint {base_opts} {shlex.quote(str(ctx_container))}".strip()
            print(f"[checkpoint] -> {chk_cmd}")
            try:
                chk_res = run_cmd(chk_cmd, quiet=False, ignore_error=True)
            except Exception as e:
                chk_res = None
                print(f"runc checkpoint raised exception for {ctx_container}: {e}")

            # Wait for image files
            files_ok = False
            try:
                # Pass the absolute dump path computed above so the helper
                # does not call abspath() relative to a cwd that was just
                # switched into (which caused duplicated paths).
                files_ok = _has_criu_image_files(abs_dump)
            except Exception:
                files_ok = False

            decode_script = os.path.join(repo_root, "mig-scripts", "decode_criu_memimages.py")
            out_path = os.path.join(abs_dump, "analysis.json")
            decode_cmd = f"{shlex.quote(sys.executable)} {shlex.quote(decode_script)} analyze {shlex.quote(abs_dump)} --output {shlex.quote(out_path)}"

            if not chk_res or getattr(chk_res, "returncode", 1) != 0:
                print(f"runc checkpoint did not report success for {ctx_container}")
                # If work-path logs exist, tail recent lines for diagnostics
                dlog_dir = os.path.join(abs_dump, "d_log")
                if os.path.isdir(dlog_dir):
                    try:
                        for fname in sorted(os.listdir(dlog_dir))[-5:]:
                            p = os.path.join(dlog_dir, fname)
                            print(f"--- d_log/{fname} (tail) ---")
                            try:
                                with open(p, "r", encoding="utf-8", errors="ignore") as df:
                                    lines = df.read().splitlines()
                                    for l in lines[-40:]:
                                        print(l)
                            except Exception:
                                pass
                    except Exception:
                        pass
                return False
            elif not files_ok:
                print(f"runc checkpoint returned OK but CRIU image files not observed in {abs_dump}")
                try:
                    if chk_res:
                        so = getattr(chk_res, "stdout", "") or ""
                        se = getattr(chk_res, "stderr", "") or ""
                        if so:
                            print("--- runc stdout ---")
                            print(so.strip())
                        if se:
                            print("--- runc stderr ---")
                            print(se.strip())
                except Exception:
                    pass
                try:
                    print(f"abs_dump: {abs_dump}")
                    print("dump dir listing:")
                    for n in sorted(os.listdir(abs_dump)):
                        p = os.path.join(abs_dump, n)
                        try:
                            st = os.stat(p)
                            print(f"  {n}  mode={oct(st.st_mode)} size={st.st_size}")
                        except Exception:
                            print(f"  {n}  (stat-error)")
                except Exception:
                    pass
                try:
                    image_dir = os.path.join(abs_dump, "image")
                    print("image dir listing:")
                    if os.path.isdir(image_dir):
                        for n in sorted(os.listdir(image_dir))[:200]:
                            p = os.path.join(image_dir, n)
                            try:
                                st = os.stat(p)
                                print(f"  image/{n}  mode={oct(st.st_mode)} size={st.st_size}")
                            except Exception:
                                print(f"  image/{n}  (stat-error)")
                    else:
                        print("  image/ does not exist")
                except Exception:
                    pass
                try:
                    dlog_dir = os.path.join(abs_dump, "d_log")
                    if os.path.isdir(dlog_dir):
                        for fname in sorted(os.listdir(dlog_dir))[-5:]:
                            p = os.path.join(dlog_dir, fname)
                            print(f"--- d_log/{fname} (tail) ---")
                            try:
                                with open(p, "r", encoding="utf-8", errors="ignore") as df:
                                    lines = df.read().splitlines()
                                    for l in lines[-80:]:
                                        print(l)
                            except Exception:
                                pass
                except Exception:
                    pass
                return False
            else:
                print(f"[per-combo decode] -> {decode_cmd}")
                decode_succeeded = False
                for _try in range(12):
                    try:
                        res = run_cmd(decode_cmd, quiet=False, ignore_error=True)
                        if res and getattr(res, "returncode", 1) == 0:
                            decode_succeeded = True
                            break
                    except Exception:
                        pass
                    time.sleep(0.5)
                if not decode_succeeded:
                    print(f"Warning: decode_criu_memimages did not succeed for dump: {abs_dump}")
                    try:
                        print("Listing dump dir contents:")
                        for p in sorted(os.listdir(dump_base)):
                            print(" ", p)
                    except Exception:
                        pass
                    try:
                        dlog_dir = os.path.join(abs_dump, "d_log")
                        if os.path.isdir(dlog_dir):
                            for fname in sorted(os.listdir(dlog_dir))[-5:]:
                                p = os.path.join(dlog_dir, fname)
                                print(f"--- d_log/{fname} (tail) ---")
                                try:
                                    with open(p, "r", encoding="utf-8", errors="ignore") as df:
                                        lines = df.read().splitlines()
                                        for l in lines[-80:]:
                                            print(l)
                                except Exception:
                                    pass
                    except Exception:
                        pass
                    return False
                return True
        finally:
            try:
                os.chdir(old_cwd)
            except Exception:
                pass
    except Exception as e:
        print(f"checkpoint/decode orchestration failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Run a generic configuration matrix of experiments")
    parser.add_argument("--container", required=False)
    parser.add_argument("--bench-template", required=False)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", default="results/matrix.csv")
    parser.add_argument("--log-dir", default="/tmp")
    parser.add_argument("--analysis-intensity", default="", help="Optional analysis intensity forwarded to video benches (e.g. mechanical,objective,comprehensive)")
    parser.add_argument("--inference-model", default="", help="Optional inference model forwarded to video benches (e.g. yolov5_medium, ssd_mobile)")
    parser.add_argument("--objects-per-frame", default="", help="Optional objects-per-frame to forward to video benches")
    parser.add_argument("--client-ip", default="", help="IP of remote client where bench should run")
    parser.add_argument("--remote-client", action="store_true", help="If set, run bench on --client-ip via SSH instead of locally")
    parser.add_argument("--fetch-remote-log", action="store_true", help="When running on --remote-client, scp the remote log back to local --log-dir for parsing")
    parser.add_argument("--dry-run", action="store_true", help="Do not start benches; simulate runs locally and write small dry-run logs")
    parser.add_argument("--tests-file", default="", help="Optional JSON file containing a list of test specifications. If provided, this script will spawn one matrix run per entry and exit.")
    parser.add_argument("--checkpoint-file-locks", action="store_true", help="(internal) forward per-test request to include CRIU --file-locks on checkpoint")
    # Accept singular CLI aliases for convenience (map later to plural internals)
    try:
        parser.add_argument("--rps", dest="rps", required=False)
    except Exception:
        pass
    try:
        parser.add_argument("--resolution", dest="resolution", required=False)
        parser.add_argument("--framerate", dest="framerate", required=False, help="Optional framerate (for video benches). Accepts comma-separated values")
    except Exception:
        pass
    try:
        parser.add_argument("--payload-size", dest="payload_size", required=False, help="Comma-separated payload sizes. Default unit is bytes when no suffix given. Suffixes accepted: B, KB, MB (case-insensitive).")
    except Exception:
        pass
    try:
        parser.add_argument("--payload-mode", dest="payload_mode", required=False, help="Comma-separated payload modes to test (e.g. json,binary).")
    except Exception:
        pass
    # Generic '--pattern' removed: benches use benchmark-specific options
    # such as '--vehicle-pattern', '--size-distribution' or
    # '--sensors-per-device'. Those alternate args were added above.
    # Some benches don't use a generic 'pattern' token. Accept common
    # alternative parameter names so users can pass them via CLI and so
    # the matrix runner will forward them into bench templates and CSVs.
    try:
        parser.add_argument("--size-distribution", dest="size_distribution", required=False)
    except Exception:
        pass
    try:
        parser.add_argument("--vehicle-pattern", dest="vehicle_pattern", required=False)
    except Exception:
        pass
    try:
        parser.add_argument("--sensors-per-device", dest="sensors_per_device", required=False)
    except Exception:
        pass

    args = parser.parse_args()
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def _checkpoint_extra_opts_for(container_name, entry_dict=None):
        """Return a string with extra runc checkpoint options for this container/entry.

        Currently supports per-entry flag `checkpoint_file_locks` (bool).
        Returns an empty string when no extra opts are required.
        """
        opts = []
        try:
            # Prefer explicit per-entry setting when available
            if entry_dict and bool(entry_dict.get("checkpoint_file_locks", False)):
                opts.append("--file-locks")
            # If no per-entry dict (e.g. child run spawned with flags), allow
            # a top-level CLI flag to request file-locks forwarding.
            elif getattr(args, "checkpoint_file_locks", False):
                opts.append("--file-locks")
        except Exception:
            pass
        return " ".join(opts)

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

    def _start_bench_and_collect(bench_cmd: str, local_logname: str, args_local, repo_root_local, client_ip=None):
        """Resolve, start a bench (local or remote), wait for completion and return (pid, target_log, content).

        This consolidates the repeated logic for resolving bench scripts, stripping
        unsupported flags, probing host endpoints, starting the bench (local or
        remote), and retrieving the log content.
        """
        bench_pid = None
        target_log = local_logname
        content = None
        try:
            ok, tried = _bench_script_resolves(bench_cmd)
        except Exception:
            ok, tried = False, bench_cmd

        # Prepare parts and strip unsupported flags
        parts = None
        try:
            parts = shlex.split(bench_cmd)
            if os.path.basename(parts[0]).startswith("python") and len(parts) > 1:
                parts[1] = tried
            else:
                parts[0] = tried
        except Exception:
            parts = None

        supported = set()
        try:
            if parts:
                supported = _get_bench_supported_flags(tried)
        except Exception:
            supported = set()

        try:
            if parts:
                new_parts = []
                skip_next = False
                for tok in parts:
                    if skip_next:
                        skip_next = False
                        continue
                    if tok.startswith("--"):
                        key = tok.split("=")[0]
                        if key not in supported:
                            if "=" not in tok:
                                skip_next = True
                            continue
                    new_parts.append(tok)
                if new_parts:
                    bench_cmd = " ".join(shlex.quote(p) for p in new_parts)
        except Exception:
            pass

        # Probe endpoint and possibly skip. Skip probe entirely for dry-run
        try:
            if not getattr(args_local, "dry_run", False):
                if _should_skip_due_to_probe(args_local.container, bench_cmd, args_local.client_ip if args_local.remote_client else None):
                    print(f"[host-probe] aborting run: host endpoint not reachable for bench {bench_cmd}")
                    return None, target_log, None
        except Exception:
            # In non-dry-run mode probe failures should not crash the run
            pass

        if args_local.dry_run:
            try:
                with open(local_logname, "w", encoding="utf-8") as df:
                    df.write("DRY-RUN\n")
                    df.write(bench_cmd + "\n")
                    df.write("METRIC_HEADER\ttotal_ops\tops_per_sec\n")
                    df.write("METRIC_VALUES\t0\t0\n")
            except Exception:
                pass
            return None, target_log, None

        # Remote client path
        if getattr(args_local, "remote_client", False):
            if not getattr(args_local, "client_ip", None):
                print("--remote-client set but --client-ip is empty; skipping run")
                return None, target_log, None
            client_target = args_local.client_ip if "@" in args_local.client_ip else f"root@{args_local.client_ip}"
            remote_logname = f"/tmp/{os.path.basename(local_logname)}"
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
                return None, target_log, None
            target_log = local_logname
            # Attempt to scp back the remote log once bench completes
            try:
                if bench_pid is not None:
                    wait_for_pid_exit(bench_pid, timeout=args_local.duration + 10)
            except Exception:
                pass
            try:
                scp_cmd = f"scp {args_local.client_ip}:{shlex.quote(remote_logname)} {shlex.quote(local_logname)}"
                run_cmd(scp_cmd, quiet=True)
                target_log = local_logname
            except Exception:
                target_log = remote_logname
        else:
            # Local bench
            ok, tried = _bench_script_resolves(bench_cmd)
            if not ok:
                print(f"Bench script not found locally (tried: {tried}). Skipping run.\n  Tip: run this from the repo root or use an absolute path in --bench-template.")
                return None, target_log, None
            try:
                parts = shlex.split(bench_cmd)
                if os.path.basename(parts[0]).startswith("python") and len(parts) > 1:
                    parts[1] = tried
                else:
                    parts[0] = tried
            except Exception:
                pass
            try:
                if parts:
                    bench_cmd = " ".join(shlex.quote(p) for p in parts)
            except Exception:
                pass
            print(f"Starting local: {bench_cmd} -> log {local_logname}")
            bench_pid = run_bench_background(bench_cmd, local_logname)
            if bench_pid is None:
                print("bench failed to start; skipping")
                return None, target_log, None
            try:
                wait_for_pid_exit(bench_pid, timeout=args_local.duration + 10)
            except Exception:
                pass

        # Read content if available
        try:
            with open(target_log, "r", encoding="utf-8", errors="ignore") as f:
                content = f.read()
        except Exception:
            content = None

        return bench_pid, target_log, content

    def _extract_stats_from_log(content: Optional[str]):
        try:
            if not content:
                return "", ""
            header_s, values = extract_stats_from_output(content)
            if header_s and values:
                bench_stats = values.replace("\t", "|")
            else:
                bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
            return header_s, bench_stats
        except Exception:
            return "", ""

    # Backwards-compatibility: older code used `extract_stats_from_output`.
    try:
        extract_stats_from_output = _extract_stats_from_log
    except Exception:
        def extract_stats_from_output(content: Optional[str]):
            return "", ""

    if not args.tests_file:
        missing = []
        if not args.container:
            missing.append("--container")
        if not args.bench_template:
            missing.append("--bench-template")
        if not args.rps and not args.framerate:
            missing.append("--rps or --framerate")
        if args.rps and args.framerate:
            parser.error("--rps and --framerate are mutually exclusive; provide only one")
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
        override_parser.add_argument("--rps", dest="rps")
        override_parser.add_argument("--framerate", dest="framerate")
        override_parser.add_argument("--payload-size", dest="payload_size")
        override_parser.add_argument("--payload-mode", dest="payload_mode")
        override_parser.add_argument("--analysis-intensity", dest="analysis_intensity")
        override_parser.add_argument("--inference-model", dest="inference_model")
        override_parser.add_argument("--objects-per-frame", dest="objects_per_frame")
        # do not include generic pattern here; use specific overrides if present
        override_parser.add_argument("--size-distribution", dest="size_distribution")
        override_parser.add_argument("--vehicle-pattern", dest="vehicle_pattern")
        override_parser.add_argument("--sensors-per-device", dest="sensors_per_device")
        override_parser.add_argument("--duration", dest="duration", type=int)
        override_parser.add_argument("--threads", dest="threads", type=int)
        override_parser.add_argument("--runs", dest="runs", type=int)
        override_parser.add_argument("--output", dest="output")
        override_parser.add_argument("--log-dir", dest="log_dir")
        override_parser.add_argument("--client-ip", dest="client_ip")
        override_args, _ = override_parser.parse_known_args()
        argv = sys.argv[1:]
        remote_client_present = "--remote-client" in argv
        fetch_remote_log_present = "--fetch-remote-log" in argv
        dry_run_present = "--dry-run" in argv

        for entry in tests:
            merged = dict(entry)
            to_copy = [
                "container",
                "bench_template",
                "rps",
                "framerate",
                "payload_size",
                "payload_mode",
                "analysis_intensity",
                "inference_model",
                "objects_per_frame",
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
            if remote_client_present:
                merged["remote_client"] = True
            if fetch_remote_log_present:
                merged["fetch_remote_log"] = True
            if dry_run_present:
                merged["dry_run"] = True
            entry = merged
            has_rps = entry.get("rps") is not None or entry.get("rps") is not None
            has_fr = entry.get("framerate") is not None or entry.get("framerate") is not None
            if has_rps and has_fr:
                print(f"tests-file entry '{entry.get('name', 'unnamed')}' invalid: both rps and framerate present; they are mutually exclusive. Skipping.")
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
                            # Ensure no stale container with same name is present before run
                            try:
                                ensure_container_quiescent(name, bundle_dir, dry_run_local=dry_run)
                            except Exception:
                                pass
                            # Retry runc run when console.sock isn't ready (recvtty)
                            run_success = False
                            last_res = None
                            for attempt in range(3):
                                try:
                                    last_res = run_cmd(run_cmd_str, quiet=False, ignore_error=True)
                                except Exception:
                                    last_res = None
                                rc = getattr(last_res, 'returncode', 1) if last_res is not None else 1
                                if rc == 0:
                                    run_success = True
                                    break
                                out = (getattr(last_res, 'stdout', '') or '') if last_res is not None else ''
                                err = (getattr(last_res, 'stderr', '') or '') if last_res is not None else ''
                                if 'console.sock' in out or 'console.sock' in err or 'connect: no such file' in out or 'connect: no such file' in err:
                                    try:
                                        recvtty_cmd = f"nohup recvtty -m null {shlex.quote(console_sock)} > /dev/null 2>&1 & echo $! > {shlex.quote(recvtty_pidfile)}"
                                        run_cmd(recvtty_cmd, quiet=True, ignore_error=True)
                                    except Exception:
                                        pass
                                    time.sleep(1)
                                else:
                                    time.sleep(1)
                            if not run_success:
                                print(f"Failed to runc run {name} after retries; last rc={getattr(last_res, 'returncode', 'unknown')}")
                            else:
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
                        # Ensure container is quiescent before delete
                        try:
                            try:
                                ensure_container_quiescent(name, os.path.join("/runc/containers", name), dry_run_local=dry_run)
                            except Exception:
                                pass
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
            # Accept possible framerate key from tests entries (guard None)
            fr_val = entry.get("framerate")
            has_fr = fr_val is not None and str(fr_val).strip() != ""
            is_video_bench = (
                "video_cache_realistic" in bt_lower
                or "bench_video" in bt_lower
                or has_fr
            )
            # payload_size applies only to non-video (rps-driven) benches
            # Forward benchmark-specific pattern-like flags instead of a
            # generic '--pattern' which is unused by bench scripts.
            if not is_video_bench:
                add_flag("payload_size", "--payload-size")
                add_flag("size_distribution", "--size-distribution")
                add_flag("vehicle_pattern", "--vehicle-pattern")
                add_flag("sensors_per_device", "--sensors-per-device")

            # payload_mode and generic run params apply to both
            add_flag("payload_mode", "--payload-mode")

            # Analysis/inference related flags are only relevant for video/framerate-driven benches
            if is_video_bench:
                add_flag("analysis_intensity", "--analysis-intensity")
                add_flag("inference_model", "--inference-model")
                add_flag("objects_per_frame", "--objects-per-frame")

            add_flag("duration")
            add_flag("threads")
            add_flag("runs")
            # Forward top-level --checkpoint-file-locks to child invocation when present
            try:
                if getattr(args, "checkpoint_file_locks", False):
                    cmd.append("--checkpoint-file-locks")
            except Exception:
                pass
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
                "rps",
                "framerate",
                "payload_size",
                "payload_mode",
                "analysis_intensity",
                "inference_model",
                "objects_per_frame",
                "pattern",
                "duration",
                "threads",
                "runs",
                "output",
                "log_dir",
                "client_ip",
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

            fr_val = entry.get("framerate") if entry.get("framerate") is not None else entry.get("framerate")
            rp_val = entry.get("rps") if entry.get("rps") is not None else entry.get("rps")
            def _norm_list_val(v):
                if v is None:
                    return None
                if isinstance(v, (list, tuple)):
                    return ",".join(str(x) for x in v)
                return str(v)

            fr_list = _norm_list_val(fr_val)
            rp_list = _norm_list_val(rp_val)
            if fr_list:
                if "--framerate" not in cmd:
                    cmd += ["--framerate", fr_list]
            elif rp_list:
                if "--rps" not in cmd:
                    cmd += ["--rps", rp_list]

            print("Spawning matrix for test:", entry.get("name", "unnamed"))
            container_name = entry.get("container")
            entry_dry = bool(entry.get("dry_run"))
            started = start_runc_backends([container_name], dry_run=entry_dry or args.dry_run)
            if started:
                print(f"Started runc backend: {', '.join(started)}")
                # Give the container a short moment to initialize internal services
                # (some images start services via entrypoint and need time before
                # the benchmark can connect). This avoids immediate connection
                # failures when the bench runs immediately after runc run.
                try:
                    time.sleep(2)
                except Exception:
                    pass

            print(" ", shlex.join(cmd))
            try:
                if not (entry_dry or args.dry_run):
                    run_cmd(shlex.join(cmd), quiet=False)
                else:
                    print(f"[tests-file dry-run] skipping spawn for {entry.get('name')}")
            except Exception as e:
                print(f"Failed to run test {entry.get('name')}: {e}")
            finally:
                # Stop backends for this tests-file entry. Parent-level
                # checkpoint/metric handling removed as it's unused in current
                # workflows.
                try:
                    stop_runc_backends([container_name], dry_run=bool(entry.get("dry_run")) or args.dry_run)
                except Exception:
                    pass
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

    if args.framerate:
        rate_name = "framerate"
        rate_values = [int(x) for x in args.framerate.split(",") if x.strip()]
    else:
        rate_name = "rps"
        rate_values = [int(x) for x in (args.rps or "").split(",") if x.strip()]
    payloads = [parse_size_token(x) for x in (args.payload_size or "").split(",") if x.strip()]
    # Generic --pattern removed; derive patterns from benchmark-specific
    # alternate flags when present so matrix iteration remains sensible.
    patterns = []
    try:
        sd = getattr(args, "size_distribution", None) or ""
        vp = getattr(args, "vehicle_pattern", None) or ""
        spd = getattr(args, "sensors_per_device", None) or ""
        alt = sd or vp or spd
        if alt:
            patterns = [x for x in str(alt).split(",") if x.strip()]
    except Exception:
        pass
    payload_modes = [x for x in args.payload_mode.split(",") if x.strip()] if args.payload_mode else [""]

    # resolution_list: optional comma-separated list of WxH tokens. When
    # provided, these resolutions will be used to vary frame size per-run
    # (video benches should compute payload based on frame dimensions).
    resolution_list = [x for x in args.resolution.split(",") if x.strip()] if getattr(args, "resolution", None) else []
    resolution_tuples = []
    if resolution_list:
        for tok in resolution_list:
            try:
                resolution_tuples.append(parse_resolution_token(tok))
            except Exception as e:
                print(f"warning: invalid resolution token '{tok}': {e}")

    # Analysis-related lists: when provided as comma-separated values, we should
    # iterate the matrix across them for video/framerate-driven experiments.
    analysis_list = [x for x in (getattr(args, "analysis_intensity", "") or "").split(",") if x.strip()]
    inference_list = [x for x in (getattr(args, "inference_model", "") or "").split(",") if x.strip()]
    objects_list = [x for x in (getattr(args, "objects_per_frame", "") or "").split(",") if x.strip()]
    # Ensure at least one empty entry so loops run once when not provided
    if not analysis_list:
        analysis_list = [""]
    if not inference_list:
        inference_list = [""]
    if not objects_list:
        objects_list = [""]

    # Provide sensible defaults for non-video (rps-driven) branch so that
    # a user can omit --pattern and/or --payload-mode on the CLI and the
    # matrix runner will still start benchmarks. For video/resolution-driven
    # experiments we intentionally leave payload_mode/pattern empty so the
    # bench can compute payload from frame dimensions and other video args.
    is_resolution_driven = bool(resolution_list)
    if not is_resolution_driven:
        # Default pattern to 'random' if nothing was provided
        patterns = patterns or ["random"]
        # Default payload_mode to 'json' if omitted (bench_sensoragg supports json/binary)
        if payload_modes == [""]:
            payload_modes = ["json"]

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    try:
        os.makedirs(args.log_dir, exist_ok=True)
    except Exception:
        pass
    # Simplified header: keep the essential memory columns and dump analysis path
    header = [
        "ts_utc",
        "rate_name",
        "rate_value",
        "resolution",
        "payload_mode",
        "payload_bytes",
        # Bench-specific columns: only one will be populated depending on bench
        "size_distribution",
        "vehicle_pattern",
        "sensors_per_device",
        "run",
        "container",
        "vmrss_before_kb",
        "vmrss_mid_kb",
        "vmrss_after_kb",
        "bench_stats",
        "analysis_intensity",
        "inference_model",
        "objects_per_frame",
        "dump_analysis_path",
    ]
    if not os.path.exists(args.output):
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(",".join(header) + "\n")

    # Kernel/device dirty-map support removed. Keep a minimal cleanup hook
    # that unmounts any tmpfs and stops the backend for a clean state.
    def _cleanup():
        try:
            unmount_local_migration_tmpfs(args.container, ignore_error=True, quiet=True)
        except Exception:
            pass
        try:
            _stop_one_backend(args.container, dry_run=args.dry_run)
        except Exception:
            pass

    atexit.register(_cleanup)

    # If resolution_tuples is provided, use resolutions to vary frame sizes
    # (video benches should compute payload based on resolution+framerate).
    if resolution_tuples:
        for rate in rate_values:
            for (fw, fh) in resolution_tuples:
                for payload_mode in payload_modes:
                    # resolution-driven runs do not iterate multiple patterns
                    pattern = ""
                    # iterate analysis/inference/objects lists to form the matrix
                    for analysis_intensity in analysis_list:
                        for inference_model in inference_list:
                            for objects_per_frame in objects_list:
                                for run_idx in range(1, args.runs + 1):
                                    print(f"Run {rate_name}={rate} resolution={fw}x{fh} payload_mode={payload_mode or 'default'} pattern={pattern} run={run_idx}")
                                    if args.dry_run:
                                        pid = None
                                    else:
                                        # Recreate container for this run so each run uses a fresh instance
                                        try:
                                            _stop_one_backend(args.container, dry_run=args.dry_run)
                                        except Exception:
                                            pass
                                        try:
                                            _start_one_backend(args.container, repo_root, dry_run=args.dry_run, log_dir=args.log_dir)
                                        except Exception:
                                            pass
                                        pid = get_container_pid(args.container)

                                    before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                                ts = int(time.time())
                                safe_mode = payload_mode if payload_mode else "default"
                                local_logname = os.path.join(args.log_dir, f"bench_{rate}_{fw}x{fh}_{safe_mode}_{pattern}_{run_idx}_{ts}.log")
                                fmt_kwargs = {
                                    "duration": args.duration,
                                    "threads": args.threads,
                                    "pattern": pattern,
                                    # Provide alternate placeholders so bench templates
                                    # that expect these names won't KeyError during
                                    # `.format()` substitution. Value may be empty.
                                    "size_distribution": getattr(args, "size_distribution", ""),
                                    "vehicle_pattern": getattr(args, "vehicle_pattern", ""),
                                    "sensors_per_device": getattr(args, "sensors_per_device", ""),
                                    "payload_mode": payload_mode,
                                    "frame_width": fw,
                                    "frame_height": fh,
                                    # fill analysis placeholders from the current matrix values
                                    "analysis_intensity": analysis_intensity,
                                    "inference_model": inference_model,
                                    "objects_per_frame": objects_per_frame,
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
                            # removed debug print
                            # --- Start of inserted bench-start + log-capture for resolution-driven runs ---
                            try:
                                bench_pid, target_log, content = _start_bench_and_collect(bench_cmd, local_logname, args, repo_root)
                                # make content available to later aggregation logic
                                try:
                                    globals()['content'] = content
                                except Exception:
                                    pass
                            except Exception:
                                bench_pid = None
                            # --- End of inserted block ---
                            # --- Begin per-run aggregation + checkpoint (resolution branch) ---
                            try:
                                # Ramp period similar to non-resolution branch
                                ramp = max(2, min(8, args.duration // 6))
                                time.sleep(ramp)
                                mid = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                                dirty_bytes = ""

                                # content variable may already be set from tail read above; if not, attempt to read
                                if 'content' not in locals() or content is None:
                                    content = None
                                    try:
                                        with open(local_logname, "r", encoding="utf-8", errors="ignore") as f:
                                            content = f.read()
                                    except Exception:
                                        content = None

                                bench_stats = ""
                                target_log = local_logname
                                if content:
                                    header_s, values = extract_stats_from_output(content)
                                    if header_s and values:
                                        bench_stats = values.replace("\t", "|")
                                    else:
                                        bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
                                    # Bench .tail generation and instrument logging removed

                                time.sleep(1)
                                after = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                                # Build a compact row matching the simplified header
                                safe_bench_stats = (bench_stats or "").replace('\n', ' ').replace('\r', ' ').replace(',', ';')
                                # resolution branch doesn't have explicit payload value; leave blank
                                payload_val = ""
                                resolution_str = f"{fw}x{fh}"
                                # derive bench-specific columns: prefer per-entry values when present
                                try:
                                    entry_local = locals().get("entry", None)
                                except Exception:
                                    entry_local = None
                                try:
                                    sd_val = ""
                                    if isinstance(entry_local, dict) and entry_local.get("size_distribution") is not None:
                                        sd_val = str(entry_local.get("size_distribution"))
                                    else:
                                        sd_val = str(getattr(args, "size_distribution", "") or "")
                                except Exception:
                                    sd_val = ""
                                try:
                                    vp_val = ""
                                    if isinstance(entry_local, dict) and entry_local.get("vehicle_pattern") is not None:
                                        vp_val = str(entry_local.get("vehicle_pattern"))
                                    else:
                                        vp_val = str(getattr(args, "vehicle_pattern", "") or "")
                                except Exception:
                                    vp_val = ""
                                try:
                                    spd_val = ""
                                    if isinstance(entry_local, dict) and entry_local.get("sensors_per_device") is not None:
                                        spd_val = str(entry_local.get("sensors_per_device"))
                                    else:
                                        spd_val = str(getattr(args, "sensors_per_device", "") or "")
                                except Exception:
                                    spd_val = ""

                                row = [
                                    datetime.utcnow().isoformat() + "Z",
                                    rate_name,
                                    str(rate),
                                    resolution_str,
                                    str(payload_mode),
                                    str(payload_val),
                                    sd_val,
                                    vp_val,
                                    spd_val,
                                    str(run_idx),
                                    args.container,
                                    str(before.get("vmrss_kb") or ""),
                                    str(mid.get("vmrss_kb") or ""),
                                    str(after.get("vmrss_kb") or ""),
                                    safe_bench_stats,
                                    str(analysis_intensity or ""),
                                    str(inference_model or ""),
                                    str(objects_per_frame or ""),
                                    "",
                                ]
                                try:
                                    with open(args.output, "a", encoding="utf-8") as f:
                                        f.write(",".join(row) + "\n")
                                except Exception:
                                    pass

                                print(f"Finished run, wrote row to {args.output}")

                                # Inline per-run checkpoint+decode (run_idx==1)
                                try:
                                    # debug prints removed
                                    if run_idx == args.runs:
                                        ctx_entry = locals().get("entry", None)
                                        ctx_test_name = locals().get("test_name", None)
                                        if isinstance(ctx_entry, dict):
                                            ctx_dry = bool(ctx_entry.get("dry_run", False))
                                            ctx_container = ctx_entry.get("container") or args.container
                                        else:
                                            ctx_dry = args.dry_run
                                            ctx_container = args.container

                                        parts = []
                                        if ctx_entry:
                                            # Prefer to enumerate any explicit fields present
                                            # in the tests-file entry. For video-like benches
                                            # (resolution/framerate driven) include analysis
                                            # specific keys rather than payload/pattern keys.
                                            try:
                                                bt_str = str(ctx_entry.get("bench_template", "")).lower()
                                                is_video_like = bool(ctx_entry.get("resolution") or ctx_entry.get("framerate") or ("video" in bt_str))
                                                if is_video_like:
                                                    for k in ("framerate", "analysis_intensity", "inference_model", "objects_per_frame"):
                                                        v = ctx_entry.get(k)
                                                        if v is None:
                                                            continue
                                                        s = str(v).replace(",", "+").replace(" ", "_")
                                                        parts.append(f"{k}={s}")
                                                else:
                                                    for k in ("rps", "framerate", "payload_size", "payload_mode"):
                                                        v = ctx_entry.get(k)
                                                        if v is None:
                                                            continue
                                                        s = str(v).replace(",", "+").replace(" ", "_")
                                                        parts.append(f"{k}={s}")
                                                    # pattern-like alternatives
                                                    alt_keys = ["pattern", "size-distribution", "size_distribution", "vehicle-pattern", "vehicle_pattern", "sensors-per-device", "sensors_per_device"]
                                                    for ak in alt_keys:
                                                        v = ctx_entry.get(ak)
                                                        if v is None:
                                                            continue
                                                        s = str(v).replace(",", "+").replace(" ", "_")
                                                        parts.append(f"{ak}={s}")
                                            except Exception:
                                                pass
                                        else:
                                            try:
                                                parts.append(f"{rate_name}={rate}")
                                            except Exception:
                                                pass
                                            try:
                                                parts.append(f"payload={payload_val}")
                                            except Exception:
                                                pass
                                            try:
                                                if payload_mode:
                                                    parts.append(f"mode={payload_mode}")
                                            except Exception:
                                                pass
                                            try:
                                                # include any alternate pattern-like placeholders
                                                # Prefer runtime 'pattern'/local value when available
                                                try:
                                                    ap_local = _effective_pattern(ctx_entry, args, locals())
                                                except Exception:
                                                    ap_local = None
                                                if ap_local:
                                                    parts.append(f"pattern={str(ap_local).replace(',', '+').replace(' ','_')}")
                                                else:
                                                    parts.append(f"pattern={pattern}")
                                            except Exception:
                                                pass
                                            # omit threads/duration from dump dir name to keep names stable

                                        try:
                                            parts.append(f"resolution={fw}x{fh}")
                                        except Exception:
                                            pass
                                        # Build a short, stable param string: only include
                                        # non-empty parts and use short abbreviations so
                                        # dump directory names stay compact and readable.
                                        abbr = {
                                            "framerate": "fr",
                                            "rps": "r",
                                            "payload": "p",
                                            "mode": "m",
                                            "payload_mode": "m",
                                            "threads": "th",
                                            "duration": "d",
                                            "pattern": "pat",
                                            "size-distribution": "sd",
                                            "size_distribution": "sd",
                                            "vehicle-pattern": "vp",
                                            "vehicle_pattern": "vp",
                                            "sensors-per-device": "spd",
                                            "sensors_per_device": "spd",
                                            "analysis_intensity": "ai",
                                            "inference_model": "im",
                                            "objects_per_frame": "opf",
                                            "resolution": "res",
                                        }
                                        short_parts = []
                                        for p in parts:
                                            try:
                                                if "=" in p:
                                                    k, v = p.split("=", 1)
                                                    v = v.strip()
                                                    if not v:
                                                        continue
                                                    k = k.replace("-", "_")
                                                    key = k
                                                    ab = abbr.get(key, key)
                                                    # compress commas to + for compactness
                                                    v2 = v.replace(",", "+").replace(" ", "_")
                                                    short_parts.append(f"{ab}{v2}")
                                                else:
                                                    s = p.strip()
                                                    if s:
                                                        short_parts.append(s)
                                            except Exception:
                                                continue
                                        paramstr = "__".join(short_parts) if short_parts else "default"
                                        paramstr = re.sub(r"[^A-Za-z0-9._+=-]+", "_", paramstr)

                                        if ctx_test_name:
                                            safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_test_name))
                                        else:
                                            try:
                                                bt = args.bench_template or "test"
                                                btparts = shlex.split(bt)
                                                cand = btparts[1] if os.path.basename(btparts[0]).startswith("python") and len(btparts) > 1 else btparts[0]
                                                safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.splitext(os.path.basename(cand))[0])
                                            except Exception:
                                                safe_test = "test"

                                        safe_container = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_container or args.container))
                                        safe_test_clean = re.sub(r"^bench_", "", safe_test)
                                        safe_test_clean = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_test_clean)
                                        dump_dirname = f"{safe_container}_{safe_test_clean}__{paramstr}_dump"
                                        dump_base = os.path.join("results", dump_dirname)
                                        if ctx_dry or args.dry_run:
                                            print(f"[dry-run] would create dump dir: {dump_base}")
                                        else:
                                            try:
                                                # Delegate checkpoint+decode orchestration to centralized helper
                                                _checkpoint_and_decode(ctx_container, ctx_entry, repo_root, dump_base, args, leave_running=True)
                                            except Exception as e:
                                                print(f"checkpoint/decode inline orchestration failed: {e}")
                                except Exception as e:
                                    print(f"Finished run, wrote row to {args.output}")

                                    # Per-run recvtty housekeeping
                                    try:
                                        recv_pidfile = f"/tmp/recvtty_{args.container}.pid"
                                        if os.path.exists(recv_pidfile):
                                            try:
                                                with open(recv_pidfile, "r", encoding="utf-8", errors="ignore") as pf:
                                                    txt = pf.read().strip()
                                            except Exception:
                                                txt = ""
                                            if not txt:
                                                try:
                                                    os.remove(recv_pidfile)
                                                except Exception:
                                                    pass
                                            else:
                                                try:
                                                    p = int(txt)
                                                except Exception:
                                                    p = None
                                                if p is None:
                                                    try:
                                                        os.remove(recv_pidfile)
                                                    except Exception:
                                                        pass
                                                else:
                                                    try:
                                                        os.kill(p, 0)
                                                    except Exception:
                                                        try:
                                                            os.remove(recv_pidfile)
                                                        except Exception:
                                                            pass
                                    except Exception:
                                        pass

                                    # Allow a short settle period before checkpoint orchestration
                                    time.sleep(3)

                                    # Checkpoint+decode branch (already executed above for run_idx==1)
                                    try:
                                        pass
                                    except Exception:
                                        pass
                            except Exception:
                                pass
                            # --- End per-run aggregation + checkpoint (resolution branch) ---
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
                                # Recreate container for this run so each run uses a fresh instance
                                try:
                                    _stop_one_backend(args.container, dry_run=args.dry_run)
                                except Exception:
                                    pass
                                try:
                                    _start_one_backend(args.container, repo_root, dry_run=args.dry_run, log_dir=args.log_dir)
                                except Exception:
                                    pass
                                pid = get_container_pid(args.container)

                            before = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                            ts = int(time.time())
                            safe_mode = payload_mode if payload_mode else "default"
                            local_logname = os.path.join(args.log_dir, f"bench_{rate}_{payload}_{safe_mode}_{pattern}_{run_idx}_{ts}.log")
                            # default target log path for local runs
                            target_log = local_logname
                            fmt_kwargs = {
                                "duration": args.duration,
                                "threads": args.threads,
                                "payload": payload,
                                # Generic 'pattern' kept for backwards-compatibility.
                                "pattern": pattern,
                                # Alternate parameter names (may be empty)
                                "size_distribution": getattr(args, "size_distribution", ""),
                                "vehicle_pattern": getattr(args, "vehicle_pattern", ""),
                                "sensors_per_device": getattr(args, "sensors_per_device", ""),
                                "payload_mode": payload_mode,
                            }
                            # Ensure objects_per_frame placeholder is present when templates reference it
                            try:
                                opf = args.objects_per_frame
                                if isinstance(opf, str) and "," in opf:
                                    opf = opf.split(",")[0]
                                fmt_kwargs["objects_per_frame"] = opf
                            except Exception:
                                fmt_kwargs["objects_per_frame"] = ""
                            # Ensure analysis_intensity and inference_model placeholders exist
                            try:
                                ai = args.analysis_intensity
                                if isinstance(ai, str) and "," in ai:
                                    ai = ai.split(",")[0]
                                fmt_kwargs["analysis_intensity"] = ai
                            except Exception:
                                fmt_kwargs["analysis_intensity"] = ""
                            try:
                                im = args.inference_model
                                if isinstance(im, str) and "," in im:
                                    im = im.split(",")[0]
                                fmt_kwargs["inference_model"] = im
                            except Exception:
                                fmt_kwargs["inference_model"] = ""
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
                                # Strip any unsupported --flags from the constructed bench_cmd
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
                                        if tok.startswith("--"):
                                            key = tok.split("=")[0]
                                            if supported and key not in supported:
                                                # unsupported flag: skip it and its separate value (if any)
                                                if "=" not in tok:
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

                            try:
                                bench_pid, target_log, content = _start_bench_and_collect(bench_cmd, local_logname, args, repo_root)
                            except Exception:
                                bench_pid = None
                                target_log = local_logname
                                content = None
                            # ensure remote_logname variable exists for later scp/ssh handling
                            remote_logname = f"/tmp/bench_{rate}_{payload}_{pattern}_{run_idx}_{ts}.log"

                            ramp = max(2, min(8, args.duration // 6))
                            time.sleep(ramp)
                            mid = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                            dirty_bytes = ""

                            # Ensure we capture 'after' sample for final row
                            after = sample_mem(pid) if pid else {"vmrss_kb": None, "vmsize_kb": None}

                            if args.remote_client and bench_pid is not None:
                                wait_cmd = (
                                    "bash -c 'start=$(date +%s); while kill -0 "
                                    + str(bench_pid)
                                    + " 2>/dev/null; do sleep 1; if [ $(( $(date +%s) - $start )) -gt "
                                    + str(args.duration + 10)
                                    + " ]; then echo timeout; exit 0; fi; done; echo done'"
                                )
                                # (no bench-tail saving in non-resolution branch)
                                try:
                                    scp_cmd = f"scp {args.client_ip}:{shlex.quote(remote_logname)} {shlex.quote(local_logname)}"
                                    run_cmd(scp_cmd, quiet=True)
                                    target_log = local_logname
                                except Exception as e:
                                    print(f"Failed to scp remote log: {e}")
                                    target_log = remote_logname
                            else:
                                # Only attempt ssh cat when a client IP/target is provided.
                                # Previously an empty args.client_ip produced the shell
                                # string `ssh  cat ...` which makes ssh treat `cat` as
                                # the hostname (hence "Could not resolve hostname cat").
                                if getattr(args, "client_ip", None):
                                    try:
                                        client_target = args.client_ip if "@" in args.client_ip else f"root@{args.client_ip}"
                                        res = run_cmd(f"ssh -n {client_target} cat {shlex.quote(remote_logname)}", quiet=True)
                                        content = (getattr(res, "stdout", "") or "")
                                    except Exception as e:
                                        print(f"Failed to read remote log via ssh: {e}")
                                        content = None
                                else:
                                    # No remote client specified; leave content None so
                                    # the local file path is used below for reading.
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
                                    bench_stats = values.replace("\t", "|")
                                else:
                                    bench_stats = content.strip().splitlines()[-1] if content.strip() else ""
                                # bench .tail generation and instrument logging removed per user request
                            else:
                                bench_stats = ""

                            # Build a compact row matching the simplified header
                            # sanitize bench_stats to avoid commas/newlines
                            safe_bench_stats = (bench_stats or "").replace('\n', ' ').replace('\r', ' ').replace(',', ';')
                            # derive bench-specific columns for non-resolution runs
                            try:
                                entry_local = locals().get("entry", None)
                            except Exception:
                                entry_local = None
                            try:
                                sd_val = ""
                                if isinstance(entry_local, dict) and entry_local.get("size_distribution") is not None:
                                    sd_val = str(entry_local.get("size_distribution"))
                                else:
                                    sd_val = str(getattr(args, "size_distribution", "") or "")
                            except Exception:
                                sd_val = ""
                            try:
                                vp_val = ""
                                if isinstance(entry_local, dict) and entry_local.get("vehicle_pattern") is not None:
                                    vp_val = str(entry_local.get("vehicle_pattern"))
                                else:
                                    vp_val = str(getattr(args, "vehicle_pattern", "") or "")
                            except Exception:
                                vp_val = ""
                            try:
                                spd_val = ""
                                if isinstance(entry_local, dict) and entry_local.get("sensors_per_device") is not None:
                                    spd_val = str(entry_local.get("sensors_per_device"))
                                else:
                                    spd_val = str(getattr(args, "sensors_per_device", "") or "")
                            except Exception:
                                spd_val = ""

                            row = [
                                datetime.utcnow().isoformat() + "Z",
                                rate_name,
                                str(rate),
                                "",  # resolution empty for non-resolution (rps) runs
                                str(payload_mode),
                                str(payload),
                                sd_val,
                                vp_val,
                                spd_val,
                                str(run_idx),
                                args.container,
                                str(before.get("vmrss_kb") or ""),
                                str(mid.get("vmrss_kb") or ""),
                                str(after.get("vmrss_kb") or ""),
                                safe_bench_stats,
                                "",
                                "",
                                "",
                                "",
                            ]
                            with open(args.output, "a", encoding="utf-8") as f:
                                f.write(",".join(row) + "\n")

                            print(f"Finished run, wrote row to {args.output}")

                            # Inline per-run checkpoint+decode to ensure we perform the
                            # checkpoint during the first run (run_idx==1). Some
                            # invocation modes previously executed the checkpoint
                            # after the run loop (causing run_idx to be the last
                            # value). This inline block is conservative: it only runs
                            # for run_idx==1 and will attempt decode with retries.
                            try:
                                if run_idx == args.runs:
                                    ctx_entry = locals().get("entry", None)
                                    ctx_test_name = locals().get("test_name", None)
                                    if isinstance(ctx_entry, dict):
                                        ctx_dry = bool(ctx_entry.get("dry_run", False))
                                        ctx_container = ctx_entry.get("container") or args.container
                                    else:
                                        ctx_dry = args.dry_run
                                        ctx_container = args.container

                                    parts = []
                                    if ctx_entry:
                                        try:
                                            # Prefer runtime loop values when available so
                                            # dump names reflect the actual run (not the
                                            # comma-separated list from the tests file).
                                            keys = ("rps", "framerate", "payload_size", "payload_mode")
                                            for k in keys:
                                                try:
                                                    eff = _effective_param(k, ctx_entry, args, locals())
                                                except Exception:
                                                    eff = None
                                                if eff is None:
                                                    continue
                                                s = str(eff).replace(',', '+').replace(' ', '_')
                                                parts.append(f"{k}={s}")

                                            alt_keys = ["pattern", "size-distribution", "size_distribution", "vehicle-pattern", "vehicle_pattern", "sensors-per-device", "sensors_per_device"]
                                            for ak in alt_keys:
                                                try:
                                                    eff = _effective_param(ak, ctx_entry, args, locals())
                                                except Exception:
                                                    eff = None
                                                if eff is None:
                                                    continue
                                                s = str(eff).replace(',', '+').replace(' ', '_')
                                                parts.append(f"{ak}={s}")
                                        except Exception:
                                            pass
                                    else:
                                        try:
                                            parts.append(f"{rate_name}={rate}")
                                        except Exception:
                                            pass
                                        try:
                                            parts.append(f"payload={payload}")
                                        except Exception:
                                            pass
                                        try:
                                            if payload_mode:
                                                parts.append(f"mode={payload_mode}")
                                        except Exception:
                                            pass
                                        try:
                                            try:
                                                ap_local = _effective_pattern(ctx_entry, args, locals())
                                            except Exception:
                                                ap_local = None
                                            if ap_local:
                                                parts.append(f"pattern={str(ap_local).replace(',', '+').replace(' ','_')}")
                                            else:
                                                parts.append(f"pattern={pattern}")
                                        except Exception:
                                            pass
                                        # omit threads/duration from dump dir name to keep names stable

                                    # Shorten and filter param parts as above
                                    abbr = {
                                        "framerate": "fr",
                                        "rps": "r",
                                        "payload": "p",
                                        "mode": "m",
                                        "payload_mode": "m",
                                        "threads": "th",
                                        "duration": "d",
                                        "pattern": "pat",
                                        "size-distribution": "sd",
                                        "size_distribution": "sd",
                                        "vehicle-pattern": "vp",
                                        "vehicle_pattern": "vp",
                                        "sensors-per-device": "spd",
                                        "sensors_per_device": "spd",
                                        "analysis_intensity": "ai",
                                        "inference_model": "im",
                                        "objects_per_frame": "opf",
                                        "resolution": "res",
                                    }
                                    short_parts = []
                                    for p in parts:
                                        try:
                                            if "=" in p:
                                                k, v = p.split("=", 1)
                                                v = v.strip()
                                                if not v:
                                                    continue
                                                k = k.replace("-", "_")
                                                ab = abbr.get(k, k)
                                                v2 = v.replace(",", "+").replace(" ", "_")
                                                short_parts.append(f"{ab}{v2}")
                                            else:
                                                s = p.strip()
                                                if s:
                                                    short_parts.append(s)
                                        except Exception:
                                            continue
                                    paramstr = "__".join(short_parts) if short_parts else "default"
                                    paramstr = re.sub(r"[^A-Za-z0-9._+=-]+", "_", paramstr)

                                    if ctx_test_name:
                                        safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_test_name))
                                    else:
                                        try:
                                            bt = args.bench_template or "test"
                                            btparts = shlex.split(bt)
                                            cand = btparts[1] if os.path.basename(btparts[0]).startswith("python") and len(btparts) > 1 else btparts[0]
                                            safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.splitext(os.path.basename(cand))[0])
                                        except Exception:
                                            safe_test = "test"

                                    # Prefer dump names to include the container prefix so it's
                                    # obvious which backend produced the dump. Also strip a
                                    # leading 'bench_' prefix from bench script basenames
                                    # (e.g. bench_sensoragg -> sensoragg) for readability.
                                    safe_container = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_container or args.container))
                                    safe_test_clean = re.sub(r"^bench_", "", safe_test)
                                    safe_test_clean = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_test_clean)
                                    dump_dirname = f"{safe_container}_{safe_test_clean}__{paramstr}_dump"
                                    dump_base = os.path.join("results", dump_dirname)
                                    if ctx_dry or args.dry_run:
                                        print(f"[dry-run] would create dump dir: {dump_base}")
                                    else:
                                        try:
                                            _checkpoint_and_decode(ctx_container, ctx_entry, repo_root, dump_base, args, leave_running=False)
                                        except Exception as e:
                                            print(f"checkpoint/decode inline orchestration failed: {e}")
                            except Exception as e:
                                print(f"Finished run, wrote row to {args.output}")

                                # Per-run recvtty housekeeping
                                try:
                                    recv_pidfile = f"/tmp/recvtty_{args.container}.pid"
                                    if os.path.exists(recv_pidfile):
                                        try:
                                            with open(recv_pidfile, "r", encoding="utf-8", errors="ignore") as pf:
                                                txt = pf.read().strip()
                                        except Exception:
                                            txt = ""
                                        if not txt:
                                            try:
                                                os.remove(recv_pidfile)
                                            except Exception:
                                                pass
                                        else:
                                            try:
                                                p = int(txt)
                                            except Exception:
                                                p = None
                                            if p is None:
                                                try:
                                                    os.remove(recv_pidfile)
                                                except Exception:
                                                    pass
                                            else:
                                                try:
                                                    os.kill(p, 0)
                                                except Exception:
                                                    try:
                                                        os.remove(recv_pidfile)
                                                    except Exception:
                                                        pass
                                except Exception:
                                    pass

                                # Allow a short settle period before checkpoint orchestration
                                time.sleep(3)

                                # Checkpoint+decode: perform once per experiment parameterization on run_idx==1
                                if USE_POST_CHECKPOINT:
                                    try:
                                        if run_idx == args.runs:
                                            ctx_entry = locals().get("entry", None)
                                            ctx_test_name = locals().get("test_name", None)
                                            if isinstance(ctx_entry, dict):
                                                ctx_dry = bool(ctx_entry.get("dry_run", False))
                                                ctx_container = ctx_entry.get("container") or args.container
                                            else:
                                                ctx_dry = args.dry_run
                                                ctx_container = args.container

                                            parts = []
                                            if ctx_entry:
                                                try:
                                                    for k in ("rps", "framerate", "payload_size", "payload_mode"):
                                                        v = None
                                                        try:
                                                            v = locals().get(k)
                                                        except Exception:
                                                            v = None
                                                        if v is None:
                                                            try:
                                                                v = locals().get(k.replace('-', '_'))
                                                            except Exception:
                                                                v = None
                                                        if v is None:
                                                            v = ctx_entry.get(k)
                                                        if v is None:
                                                            continue
                                                        s = str(v).replace(",", "+").replace(" ", "_")
                                                        parts.append(f"{k}={s}")
                                                    alt_keys = ["pattern", "size-distribution", "size_distribution", "vehicle-pattern", "vehicle_pattern", "sensors-per-device", "sensors_per_device"]
                                                    for ak in alt_keys:
                                                        v = None
                                                        try:
                                                            v = locals().get(ak)
                                                        except Exception:
                                                            v = None
                                                        if v is None:
                                                            try:
                                                                v = locals().get(ak.replace('-', '_'))
                                                            except Exception:
                                                                v = None
                                                        if v is None:
                                                            v = ctx_entry.get(ak)
                                                        if v is None:
                                                            continue
                                                        s = str(v).replace(",", "+").replace(" ", "_")
                                                        parts.append(f"{ak}={s}")
                                                except Exception:
                                                    pass
                                            else:
                                                try:
                                                    parts.append(f"{rate_name}={rate}")
                                                except Exception:
                                                    pass
                                                try:
                                                    parts.append(f"payload={payload}")
                                                except Exception:
                                                    pass
                                                try:
                                                    if payload_mode:
                                                        parts.append(f"mode={payload_mode}")
                                                except Exception:
                                                    pass
                                                try:
                                                    try:
                                                        try:
                                                            ap_local = _effective_pattern(ctx_entry, args, locals())
                                                        except Exception:
                                                            ap_local = None
                                                    except Exception:
                                                        ap_local = getattr(args, "size_distribution", None) or getattr(args, "vehicle_pattern", None) or getattr(args, "sensors_per_device", None)
                                                    if ap_local:
                                                        parts.append(f"pattern={str(ap_local).replace(',', '+').replace(' ','_')}")
                                                    else:
                                                        parts.append(f"pattern={pattern}")
                                                except Exception:
                                                    pass
                                                # omit threads/duration from dump dir name to keep names stable

                                            # Shorten and filter param parts as above
                                            abbr = {
                                                "framerate": "fr",
                                                "rps": "r",
                                                "payload": "p",
                                                "mode": "m",
                                                "payload_mode": "m",
                                                "threads": "th",
                                                "duration": "d",
                                                "pattern": "pat",
                                                "size-distribution": "sd",
                                                "size_distribution": "sd",
                                                "vehicle-pattern": "vp",
                                                "vehicle_pattern": "vp",
                                                "sensors-per-device": "spd",
                                                "sensors_per_device": "spd",
                                                "analysis_intensity": "ai",
                                                "inference_model": "im",
                                                "objects_per_frame": "opf",
                                                "resolution": "res",
                                            }
                                            short_parts = []
                                            for p in parts:
                                                try:
                                                    if "=" in p:
                                                        k, v = p.split("=", 1)
                                                        v = v.strip()
                                                        if not v:
                                                            continue
                                                        k = k.replace("-", "_")
                                                        ab = abbr.get(k, k)
                                                        v2 = v.replace(",", "+").replace(" ", "_")
                                                        short_parts.append(f"{ab}{v2}")
                                                    else:
                                                        s = p.strip()
                                                        if s:
                                                            short_parts.append(s)
                                                except Exception:
                                                    continue
                                            paramstr = "__".join(short_parts) if short_parts else "default"
                                            paramstr = re.sub(r"[^A-Za-z0-9._+=-]+", "_", paramstr)

                                            if ctx_test_name:
                                                safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_test_name))
                                            else:
                                                try:
                                                    bt = args.bench_template or "test"
                                                    btparts = shlex.split(bt)
                                                    cand = btparts[1] if os.path.basename(btparts[0]).startswith("python") and len(btparts) > 1 else btparts[0]
                                                    safe_test = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.splitext(os.path.basename(cand))[0])
                                                except Exception:
                                                    safe_test = "test"

                                            # Prefer dump names to include the container prefix so it's
                                            # obvious which backend produced the dump. Also strip a
                                            # leading 'bench_' prefix from bench script basenames
                                            # (e.g. bench_sensoragg -> sensoragg) for readability.
                                            safe_container = re.sub(r"[^A-Za-z0-9._-]+", "_", str(ctx_container or args.container))
                                            safe_test_clean = re.sub(r"^bench_", "", safe_test)
                                            safe_test_clean = re.sub(r"[^A-Za-z0-9._-]+", "_", safe_test_clean)
                                            dump_dirname = f"{safe_container}_{safe_test_clean}__{paramstr}_dump"
                                            dump_base = os.path.join("results", dump_dirname)
                                            if ctx_dry or args.dry_run:
                                                print(f"[dry-run] would create dump dir: {dump_base}")
                                            else:
                                                try:
                                                    _checkpoint_and_decode(ctx_container, ctx_entry, repo_root, dump_base, args, leave_running=True)
                                                except Exception as e:
                                                    print(f"checkpoint/decode orchestration failed: {e}")
                                    except Exception as e:
                                        print(f"checkpoint branch raised exception: {e}")

    # end main


if __name__ == "__main__":
    main()
