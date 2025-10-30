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
    shell: Optional[bool] = None,
    label: str = "local",
) -> subprocess.CompletedProcess:
    """Run a command and surface stdout/stderr when it fails."""
    if shell is None:
        shell = isinstance(cmd, str)

    display = _display_cmd(cmd)
    if not quiet:
        print(f"[{label}]$ {display}")

    result = subprocess.run(
        cmd,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if result.returncode != 0:
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
        full_cmd: Command = f"ssh -n {target_ip} \"{remote_cmd}\""
    else:
        full_cmd = f"ssh {target_ip} '{cmd}'"

    return run_cmd(full_cmd, ignore_error=ignore_error, quiet=quiet, label=f"remote:{target_ip}")
