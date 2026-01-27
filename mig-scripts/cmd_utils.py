import glob
import os
import shlex
import subprocess
import sys
from typing import Optional, Sequence, Union

Command = Union[str, Sequence[str]]


def _display_cmd(cmd: Command) -> str:
    if isinstance(cmd, str):
        return cmd
    return " ".join(str(part) for part in cmd)


def run_cmd(
    cmd: Command,
    *,
    ignore_error: bool = False,
    quiet: bool = False,
    stdin: Optional[int] = subprocess.DEVNULL,
    shell: Optional[bool] = None,
    label: str = "local",
    timeout: Optional[float] = None,
    cwd: Optional[str] = None,
) -> subprocess.CompletedProcess:
    """Run a command and surface stdout/stderr when it fails.

    Added `timeout` (seconds) to avoid indefinite hangs on commands that block.
    On timeout, exits with code 124 unless `ignore_error=True`, in which case a
    CompletedProcess with returncode 124 is returned.
    """
    if shell is None:
        shell = isinstance(cmd, str)

    display = _display_cmd(cmd)
    if not quiet:
        print(f"[{label}]$ {display}")

    try:
        result = subprocess.run(
            cmd,
            shell=shell,
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            cwd=cwd,
        )
    except subprocess.TimeoutExpired as e:
        # Timeout: report and either exit or return a CompletedProcess-like object
        if not quiet:
            print(f"[{label}] command timed out after {timeout}s: {display}")
            if getattr(e, 'stdout', None):
                _out = e.stdout
                if isinstance(_out, (bytes, bytearray)):
                    _out = _out.decode('utf-8', errors='ignore')
                print("[stdout]\n" + (_out or '').rstrip())
            if getattr(e, 'stderr', None):
                _err = e.stderr
                if isinstance(_err, (bytes, bytearray)):
                    _err = _err.decode('utf-8', errors='ignore')
                print("[stderr]\n" + (_err or '').rstrip())
        if not ignore_error:
            # 124 is commonly used for timeout
            sys.exit(124)
        # Normalize outputs to strings for CompletedProcess-like return
        _stdout = e.stdout
        _stderr = e.stderr if e.stderr is not None else f"Timeout after {timeout}s"
        if isinstance(_stdout, (bytes, bytearray)):
            _stdout = _stdout.decode('utf-8', errors='ignore')
        if isinstance(_stderr, (bytes, bytearray)):
            _stderr = _stderr.decode('utf-8', errors='ignore')
        return subprocess.CompletedProcess(cmd, 124, stdout=(_stdout or ''), stderr=(_stderr or f"Timeout after {timeout}s"))

    if result.returncode != 0:
        suppress_output = quiet and ignore_error

        if not suppress_output:
            print(f"[{label}] command failed (exit {result.returncode}): {display}")
            if result.stdout:
                print("[stdout]\n" + result.stdout.rstrip())
            if result.stderr:
                print("[stderr]\n" + result.stderr.rstrip())

        if not ignore_error:
            sys.exit(result.returncode if result.returncode else 1)
    else:
        if not quiet and result.stdout:
            print(result.stdout.rstrip())
    return result


def run_remote_cmd(
    cmd: str,
    target_ip: str,
    *,
    ignore_error: bool = False,
    background: bool = False,
    quiet: bool = False,
) -> subprocess.CompletedProcess:
    if not target_ip:
        raise ValueError("target_ip is required for run_remote_cmd")

    if background:
        remote_cmd = f"nohup {cmd} >/tmp/remote_bg.log 2>&1 < /dev/null & echo $!"
        quoted = shlex.quote(remote_cmd)
        full_cmd: Command = f"ssh -n {target_ip} {quoted}"
    else:
        quoted = shlex.quote(cmd)
        full_cmd = f"ssh {target_ip} {quoted}"

    return run_cmd(full_cmd, ignore_error=ignore_error, quiet=quiet, label=f"remote:{target_ip}")


def unmount_local_migration_tmpfs(
    container_name: str,
    *,
    ignore_error: bool = True,
    quiet: bool = True,
) -> None:
    """Unmount expected tmpfs mountpoints under the container's migrate directory."""

    base = os.path.join("/runc/containers", container_name, "migrate")
    candidates = [os.path.join(base, leaf) for leaf in ("image", "dirty_map")]
    candidates.extend(sorted(glob.glob(os.path.join(base, "parent_*"))))

    for path in candidates:
        if os.path.isdir(path) and os.path.ismount(path):
            run_cmd(["umount", path], ignore_error=ignore_error, quiet=quiet)
            if os.path.ismount(path):
                # fallback to lazy sudo umount if still mounted
                run_cmd(["sudo", "umount", "-l", path], ignore_error=ignore_error, quiet=quiet)
