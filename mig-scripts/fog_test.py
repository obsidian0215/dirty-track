#!/usr/bin/env python3
# coding: utf-8
"""
fog_test.py - Migration wrapper for fog_workloads services (GOCR, GZIP, YOLO, PocketSphinx, Aeneas, FogLAMP, iPokeMon)

Provides destination/source prepare/clean and a source_run_migration method that exercises
fog_workloads bundles under /runc/fog_workloads and records per-run metrics using result_writer.append_result().

Usage:
  python3 fog_test.py --scene yolo --runs 3 --experiment-types pre-copy

This wrapper follows the style of redis_test.py and is intended to be used by the migration runner to
exercise the long-running service variants of these workloads during migration experiments.
"""

# (Start by reusing the implementation from the previous defog_test wrapper)
from typing import Optional, List
import argparse
import os
import shlex
import shutil
import subprocess
import sys
import time
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

from result_writer import extract_stats_from_output, append_result, summarize_results
from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs
from script_defaults import choose_scripts, get_default_ips

# Defaults (reuse helpers that are common across wrappers)
SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
SEC_MODE = False
BANDWIDTH = "50mbit"
# Number of attempts to try applying remote/local network commands before falling back
NETWORK_CMD_RETRIES = 3
NETWORK_CMD_BACKOFF_BASE = 1  # seconds, exponential backoff base
DEFAULT_HOST = "127.0.0.1"
RUN_LABEL = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
# TMP_ROOT organizes all transient fog_test logs and pidfiles to avoid littering /tmp
TMP_ROOT = os.path.join('/tmp', 'fog_test', RUN_LABEL)
TMP_LOGS = os.path.join(TMP_ROOT, 'logs')
TMP_DEST_LOGS = os.path.join(TMP_LOGS, 'destination')
TMP_BENCH_LOGS = os.path.join(TMP_LOGS, 'bench')
TMP_RECVTTY_LOGS = os.path.join(TMP_LOGS, 'recvtty')
TMP_PIDS = os.path.join(TMP_ROOT, 'pids')
RESULTS_ROOT = '/runc/results'
LOCAL_HOSTS = {None, '127.0.0.1', 'localhost'}


# --- Network shaping helpers (aligned with redis_test.py / influxdb_test.py) ---
def clean_configure_network():
    primary_iface = "enp2s0"
    client_iface = "enp2s0" if CLIENT_IP == DEST_IP else "ens33"

    def _clear_local(iface: str):
        # remove root qdisc and any ingress qdisc to ensure full cleanup; re-enable offloads
        cmds = [
            f"sudo tc qdisc del dev {iface} root || true",
            f"sudo tc qdisc del dev {iface} ingress || true",
            f"sudo ethtool -K {iface} gso on gro on tso on || true",
        ]
        run_cmd("; ".join(cmds), ignore_error=True, quiet=True)

    # cleanup local primary interface first
    _clear_local(primary_iface)

    # Build a mapping of remote target -> set(of interfaces) to clean to avoid duplicate SSH calls
    remote_targets = {}
    if DEST_IP and DEST_IP != SOURCE_IP:
        remote_targets.setdefault(DEST_IP, set()).add(primary_iface)

    if CLIENT_IP:
        target = CLIENT_IP
        iface = client_iface
        if target == SOURCE_IP or target in (None, '127.0.0.1', 'localhost'):
            # client is local: clean locally
            _clear_local(iface)
        else:
            remote_targets.setdefault(target, set()).add(iface)

    # Execute batched cleanup commands per remote target (concurrently per host)
    def _run_remote_cleanup(target, ifaces):
        cmds = []
        for iface in sorted(ifaces):
            cmds.extend([
                f"sudo tc qdisc del dev {iface} root || true",
                f"sudo tc qdisc del dev {iface} ingress || true",
                f"sudo ethtool -K {iface} gso on gro on tso on || true",
            ])
        combined = " ; ".join(cmds)
        try:
            run_remote_cmd(combined, target, ignore_error=True, quiet=True)
        except Exception as e:
            print(f"[net] remote cleanup failed for {target}: {e}")

    if remote_targets:
        # Try concurrent cleanup per-host; if executor cannot be used (e.g., interpreter shutdown), fall back to serial
        try:
            max_workers = min(8, max(1, len(remote_targets)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {ex.submit(_run_remote_cleanup, target, ifaces): target for target, ifaces in remote_targets.items()}
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        print(f"[net] remote cleanup failed for {futures[fut]}: {e}")
        except RuntimeError as e:
            # Likely interpreter shutdown or thread subsystem unavailable; perform serially
            print(f"[net] executor unavailable for cleanup, falling back to serial: {e}")
            for target, ifaces in remote_targets.items():
                try:
                    _run_remote_cleanup(target, ifaces)
                except Exception as ee:
                    print(f"[net] remote cleanup failed for {target}: {ee}")
        except Exception as e:
            print(f"[net] unexpected error using executor for cleanup: {e}")
            for target, ifaces in remote_targets.items():
                try:
                    _run_remote_cleanup(target, ifaces)
                except Exception as ee:
                    print(f"[net] remote cleanup failed for {target}: {ee}")


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """Simplified network shaping: apply CAKE root qdisc with bandwidth limit and disable NIC offloads.

    This implementation is intentionally simple and robust: it applies a single CAKE qdisc per
    interface using the first rule's rate (or the global BANDWIDTH). It then attempts to disable
    GSO/GRO/TSO using ethtool synchronously (the user has installed ethtool per request).
    """
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    remote_ip = str(target_ip) if target_ip else None
    remote_mode = bool(is_remote and remote_ip not in (None, '127.0.0.1', 'localhost', SOURCE_IP))
    location = f"remote:{remote_ip}" if remote_mode else "local"

    # Deduplicate rules and pick bandwidth
    unique_rules = []
    seen = set()
    for rule in rules:
        key = (rule.get('dst'), rule.get('rate'), rule.get('delay'))
        if key in seen:
            continue
        seen.add(key)
        unique_rules.append(rule)

    bw = unique_rules[0]['rate'] if unique_rules else BANDWIDTH
    print(f"[net] applying CAKE on {location} {interface} bw={bw}")

    cake_cmd = f"sudo tc qdisc del dev {interface} root || true ; sudo tc qdisc add dev {interface} root cake bandwidth {bw} || true"

    def _exec_with_retries(cmd, remote=False, target=None, attempts=NETWORK_CMD_RETRIES):
        for attempt in range(1, attempts + 1):
            try:
                if remote:
                    res = run_remote_cmd(cmd, target, ignore_error=True, quiet=True)
                else:
                    res = run_cmd(cmd, ignore_error=True, quiet=True)
                if getattr(res, 'returncode', 0) == 0:
                    return res
            except SystemExit:
                pass
            except Exception as e:
                if attempt == attempts:
                    print(f"[net] command failed after {attempts} attempts on {(target if remote else 'local')}: {e}")
                    return None
            time.sleep(NETWORK_CMD_BACKOFF_BASE * (2 ** (attempt - 1)))
        return None

    res = _exec_with_retries(cake_cmd, remote=remote_mode, target=remote_ip)
    if res is None or getattr(res, 'returncode', 0) != 0:
        print(f"[net] warning: failed to apply CAKE on {location} {interface}")
        return

    # Disable offloads synchronously (user installed ethtool); this should help enforce limits on short bursts
    offload_cmd = f"sudo ethtool -K {interface} gso off gro off tso off"
    off_r = _exec_with_retries(offload_cmd, remote=remote_mode, target=remote_ip)
    if off_r is None or getattr(off_r, 'returncode', 0) != 0:
        print(f"[net] warning: ethtool offload disable failed on {location} {interface}")
    else:
        # Verify offloads state (best-effort)
        try:
            check_cmd = f"sudo ethtool -k {interface} | egrep 'gso|gro|tso' || true"
            chk = _exec_with_retries(check_cmd, remote=remote_mode, target=remote_ip)
            out = (getattr(chk, 'stdout', '') or '').lower() if chk is not None else ''
            if 'off' in out or 'disabled' in out:
                print(f"[net] offloads appear disabled on {location} {interface}")
            else:
                print(f"[net] offloads verification inconclusive on {location} {interface}; output: {(out or '')[:200]}")
        except Exception:
            pass

    # Report qdisc status for visibility
    try:
        qc = f"tc -s qdisc show dev {interface} | sed -n '1,120p'"
        qc_res = _exec_with_retries(qc, remote=remote_mode, target=remote_ip)
        out_qc = (getattr(qc_res, 'stdout', '') or '') if qc_res is not None else ''
        print(f"[net] qdisc status on {location} {interface}:\n{out_qc[:400]}")
    except Exception:
        pass

    return


def configure_network(bandwidth: Optional[str] = None):
    bw = bandwidth or BANDWIDTH
    primary_iface = "enp2s0"
    client_iface = "enp2s0" if CLIENT_IP == DEST_IP else "ens33"

    source_rules = [{"rate": bw, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": bw, "delay": "0.5ms", "dst": SOURCE_IP}]
    client_rules = [
        {"rate": bw, "delay": "0.5ms", "dst": SOURCE_IP},
        {"rate": bw, "delay": "0.05ms", "dst": DEST_IP},
    ]

    # Aggregate tasks keyed by (is_remote, target_ip, interface) to avoid duplicate work
    tasks = {}
    # local source iface
    tasks.setdefault((False, None, primary_iface), []).extend(source_rules)

    # dest rules: remote or local depending on DEST_IP
    if DEST_IP:
        if DEST_IP == SOURCE_IP or DEST_IP in (None, '127.0.0.1', 'localhost'):
            tasks.setdefault((False, None, primary_iface), []).extend(dest_rules)
        else:
            tasks.setdefault((True, DEST_IP, primary_iface), []).extend(dest_rules)

    # client rules: may be local or remote
    if CLIENT_IP:
        if CLIENT_IP == SOURCE_IP or CLIENT_IP in (None, '127.0.0.1', 'localhost'):
            tasks.setdefault((False, None, client_iface), []).extend(client_rules)
        else:
            tasks.setdefault((True, CLIENT_IP, client_iface), []).extend(client_rules)

    # apply tasks (one call per unique host/interface) — parallelize across hosts
    def _apply_task(is_remote, target_ip, iface, rules):
        try:
            configure_network_do(interface=iface, rules=rules, is_remote=is_remote, target_ip=target_ip)
        except Exception as e:
            t = f"remote:{target_ip}" if is_remote else "local"
            print(f"[net] configure_network: failed to apply rules on {t} {iface}: {e}")

    if tasks:
        try:
            max_workers = min(8, max(1, len(tasks)))
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = []
                for (is_remote, target_ip, iface), rules in tasks.items():
                    futures.append(ex.submit(_apply_task, is_remote, target_ip, iface, rules))
                for fut in as_completed(futures):
                    try:
                        fut.result()
                    except Exception as e:
                        print(f"[net] configure_network: unexpected error in task: {e}")
        except RuntimeError as e:
            # fall back to serial application if executor cannot be used
            print(f"[net] executor unavailable for configure_network, falling back to serial: {e}")
            for (is_remote, target_ip, iface), rules in tasks.items():
                _apply_task(is_remote, target_ip, iface, rules)
        except Exception as e:
            print(f"[net] unexpected error using executor for configure_network: {e}")
            for (is_remote, target_ip, iface), rules in tasks.items():
                _apply_task(is_remote, target_ip, iface, rules)

# --- VIP helpers (ensure VIP is present on source and optionally restore using keepalived) ---

def _vip_present(vip: str | None) -> bool:
    """Return True if `vip` is configured on any local IPv4 address."""
    if not vip:
        return False
    try:
        # Use ip command to detect IPv4 address presence (grepping the vip string)
        # Use grep -F for literal match (IP includes dots/slashes which may not be word characters for -w)
        cmd = f"ip -4 addr show | grep -F {shlex.quote(vip)}"
        r = run_cmd(cmd, quiet=True, ignore_error=True, timeout=2)
        out = (getattr(r, 'stdout', '') or '').strip()
        return bool(out)
    except Exception:
        return False


def _find_primary_iface() -> str | None:
    """Best-effort discover a primary non-loopback interface (from default route or ip output)."""
    try:
        r = run_cmd('ip route show default', quiet=True, ignore_error=True, timeout=2)
        out = (getattr(r, 'stdout', '') or '')
        import re as _re

        m = _re.search(r"dev\s+(\S+)", out)
        if m:
            return m.group(1)
    except Exception:
        pass

    # Fallback: parse first interface from ip -4 addr show
    try:
        r2 = run_cmd('ip -4 addr show', quiet=True, ignore_error=True, timeout=2)
        out = (getattr(r2, 'stdout', '') or '')
        import re as _re

        m2 = _re.search(r"^\d+:\s+(\S+?):", out, _re.M)
        if m2:
            iface = m2.group(1)
            if iface and iface != 'lo':
                return iface
    except Exception:
        pass
    return None


# --- Remote VIP helpers ---
def _remote_vip_present(target_ip: str | None) -> bool:
    """Return True if `VIP` appears configured on remote `target_ip`."""
    if not target_ip or target_ip in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        return False
    try:
        res = run_remote_cmd(f"ip -4 addr show | grep -F {shlex.quote(VIP)}", target_ip, ignore_error=True, quiet=True)
        out = (getattr(res, 'stdout', '') or '').strip()
        return bool(out)
    except Exception:
        return False


def _remote_find_primary_iface(target_ip: str | None) -> str | None:
    if not target_ip or target_ip in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        return None
    try:
        r = run_remote_cmd('ip route show default', target_ip, ignore_error=True, quiet=True)
        out = (getattr(r, 'stdout', '') or '')
        import re as _re
        m = _re.search(r"dev\s+(\S+)", out)
        if m:
            return m.group(1)
    except Exception:
        pass
    try:
        r2 = run_remote_cmd('ip -4 addr show', target_ip, ignore_error=True, quiet=True)
        out = (getattr(r2, 'stdout', '') or '')
        import re as _re
        m2 = _re.search(r"^\d+:\s+(\S+?):", out, _re.M)
        if m2:
            iface = m2.group(1)
            if iface and iface != 'lo':
                return iface
    except Exception:
        pass
    return None


def _remote_remove_vip(target_ip: str | None) -> bool:
    """Attempt to remove VIP from remote host (uses sudo ip addr del). Returns True when remote no longer has VIP."""
    if not target_ip or target_ip in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        return False
    try:
        iface = _remote_find_primary_iface(target_ip)
        if iface:
            cmd = f"sudo ip addr del {shlex.quote(VIP)}/32 dev {shlex.quote(iface)} || true"
        else:
            cmd = f"sudo ip addr del {shlex.quote(VIP)}/32 || true"
        run_remote_cmd(cmd, target_ip, ignore_error=True, quiet=True)
        # give it a brief moment
        time.sleep(0.5)
        return not _remote_vip_present(target_ip)
    except Exception:
        return False


def _set_local_keepalived_priority(priority: str | int) -> bool:
    """Set local keepalived priority via vipctl (returns True on success)."""
    try:
        try:
            import mig_scripts.vipctl as vipctl
        except Exception:
            import importlib.util as _il

            spec = _il.spec_from_file_location("vipctl_mod", os.path.join(os.path.dirname(__file__), "vipctl.py"))
            vipctl = _il.module_from_spec(spec)
            spec.loader.exec_module(vipctl)
        rc = vipctl.set_keepalived_priority(priority)
        if rc == 0:
            print(f"[vip] local keepalived priority set to {priority}")
            return True
        print(f"[vip] local set_keepalived_priority returned {rc}")
        return False
    except Exception as e:
        print(f"[vip] failed to set local keepalived priority: {e}")
        return False


def _set_remote_keepalived_priority(target_ip: str | None, priority: str | int, attempts: int = 3, backoff: float = 1.0) -> bool:
    """Set keepalived priority on remote host by invoking vipctl via sudo SSH. Returns True on success."""
    if not target_ip or target_ip in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        return False
    cmd = f"sudo python3 /runc/dirty-track/mig-scripts/vipctl.py set-priority --priority {shlex.quote(str(priority))}"
    for attempt in range(1, attempts + 1):
        try:
            res = run_remote_cmd(cmd, target_ip, ignore_error=True, quiet=True)
            rc = getattr(res, 'returncode', 1)
            if rc == 0:
                print(f"[vip] remote {target_ip} keepalived priority set to {priority} (attempt {attempt})")
                return True
            else:
                print(f"[vip] remote set-priority returned rc={rc} on attempt {attempt}")
        except Exception as e:
            print(f"[vip] remote set-priority attempt {attempt} failed: {e}")
        time.sleep(backoff * (2 ** (attempt - 1)))
    print(f"[vip] remote set-priority failed for {target_ip} after {attempts} attempts")
    return False


def _coordinate_keepalived_takeover(dest_ip: str | None, source_priority: str | int = "100", dest_priority: str | int = "30", timeout: float = 20.0) -> bool:
    """Try to make the source win the VIP by coordinating keepalived priorities.

    Steps:
    1. Try to lower destination priority (best-effort).
    2. Raise local priority.
    3. Wait briefly for election and for VIP to appear locally.
    4. Retry a small number of times before giving up.
    """
    print(f"[vip] coordinate takeover: source_prio={source_priority} dest_prio={dest_priority} dest={dest_ip}")
    deadline = time.time() + float(timeout)
    for attempt in range(1, 4):
        print(f"[vip] takeover attempt {attempt}")
        # Lower destination priority first (if reachable)
        if dest_ip and dest_ip not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
            try:
                _ = _set_remote_keepalived_priority(dest_ip, dest_priority, attempts=2)
            except Exception as e:
                print(f"[vip] remote priority set error: {e}")
        # Set local priority high
        try:
            _ = _set_local_keepalived_priority(source_priority)
        except Exception as e:
            print(f"[vip] local priority set error: {e}")
        # Wait briefly for leader election
        inner_deadline = min(time.time() + 5.0, deadline)
        while time.time() < inner_deadline:
            if _vip_present(VIP):
                print(f"[vip] VIP {VIP} acquired by source after attempt {attempt}")
                try:
                    iface = _find_primary_iface()
                    if iface:
                        try:
                            import mig_scripts.vipctl as vipctl

                            _ = vipctl.arping_announce(iface, VIP, count=2)
                            print(f"[vip] sent gratuitous ARP for {VIP} on {iface}")
                        except Exception:
                            pass
                except Exception:
                    pass
                return True
            time.sleep(0.5)
        # small back-off before next attempt
        time.sleep(0.5 * attempt)
    print(f"[vip] coordinate takeover failed after attempts")
    return False


def ensure_vip_on_source(timeout: float = 20.0, priority: str = "100") -> bool:
    """Ensure the configured VIP is present on the local (source) host using keepalived coordination.

    Uses coordinated priority changes between source and destination instead of removing remote IPs.
    Returns True on success, False otherwise.
    """
    if not VIP:
        print("[vip] no VIP configured; cannot ensure vip on source")
        return False

    local_has = _vip_present(VIP)
    dest_has = _remote_vip_present(DEST_IP) if DEST_IP else False

    # If local already has VIP but destination also has it, attempt to resolve via priorities
    if local_has:
        print(f"[vip] VIP {VIP} already present on source")
        if dest_has:
            print(f"[vip] conflict: destination {DEST_IP} also reports {VIP}; attempting keepalived coordination")
            if _coordinate_keepalived_takeover(DEST_IP, source_priority=priority, dest_priority="30", timeout=timeout):
                return True
            print("[vip] failed to resolve conflict by priority coordination; aborting to avoid split-brain")
            return False
        return True

    # If destination holds VIP, attempt coordinated takeover
    if dest_has:
        print(f"[vip] VIP {VIP} present on destination {DEST_IP}; attempting coordinated takeover")
        if _coordinate_keepalived_takeover(DEST_IP, source_priority=priority, dest_priority="30", timeout=timeout):
            return True
        print("[vip] coordinated takeover failed; not modifying remote IPs to avoid split-brain")
        return False

    # Neither host claims VIP: try to claim via keepalived priority locally
    print(f"[vip] VIP {VIP} not present anywhere; trying to set local keepalived priority {priority}")
    try:
        if _set_local_keepalived_priority(priority):
            deadline = time.time() + float(timeout)
            try:
                import mig_scripts.vipctl as vipctl
            except Exception:
                import importlib.util as _il

                spec = _il.spec_from_file_location("vipctl_mod", os.path.join(os.path.dirname(__file__), "vipctl.py"))
                vipctl = _il.module_from_spec(spec)
                spec.loader.exec_module(vipctl)
            while time.time() < deadline:
                if _vip_present(VIP):
                    print(f"[vip] VIP {VIP} restored to source by keepalived priority")
                    try:
                        iface = _find_primary_iface()
                        if iface:
                            try:
                                _ = vipctl.arping_announce(iface, VIP, count=2)
                                print(f"[vip] sent gratuitous ARP for {VIP} on {iface}")
                            except Exception:
                                pass
                    except Exception:
                        pass
                    return True
                time.sleep(0.5)
    except Exception as e:
        print(f"[vip] setting local priority failed: {e}")

    # Fallback to local ip add only if remote does not hold VIP
    try:
        if DEST_IP and _remote_vip_present(DEST_IP):
            print(f"[vip] destination {DEST_IP} still holds VIP; refusing fallback ip-add to avoid split-brain")
            return False
        iface = _find_primary_iface()
        if iface:
            try:
                import mig_scripts.vipctl as vipctl
            except Exception:
                import importlib.util as _il

                spec = _il.spec_from_file_location("vipctl_mod", os.path.join(os.path.dirname(__file__), "vipctl.py"))
                vipctl = _il.module_from_spec(spec)
                spec.loader.exec_module(vipctl)
            print(f"[vip] fallback: adding IP {VIP} on {iface} and sending ARP")
            try:
                rc2 = vipctl.switch_local_add_then_announce(iface, VIP, dry_run=False)
                if rc2 == 0:
                    deadline2 = time.time() + 5
                    while time.time() < deadline2:
                        if _vip_present(VIP):
                            print(f"[vip] VIP {VIP} added and present on {iface}")
                            return True
                        time.sleep(0.5)
            except Exception as e:
                print(f"[vip] switch_local_add_then_announce failed: {e}")
    except Exception:
        pass

    print(f"[vip] failed to ensure VIP {VIP} on source")
    return False


def parse_bandwidth(bw: str) -> float:
    """Parse simple bandwidth strings like '25mbit', '10kbit' into BYTES/sec."""
    if not bw:
        return 0.0
    b = str(bw).strip().lower()
    try:
        if b.endswith('mbit'):
            return float(b[:-4]) * 1_000_000.0 / 8.0
        if b.endswith('kbit'):
            return float(b[:-4]) * 1_000.0 / 8.0
        if b.endswith('gbit'):
            return float(b[:-4]) * 1_000_000_000.0 / 8.0
        # bare number interpreted as bytes/sec
        return float(b)
    except Exception:
        return 0.0

# Scenes list (align with /runc/fog_workloads bundles). This is informational for CLI help;
# the authoritative per-scene metadata lives in SCENE_INFO below.
SCENES = [
    "aeneas",
    "gocr",
    "gzip",
    "yolo",
    "pocketsphinx",
    "ipokemon",
    "sensoragg",
    "cartelem",
    "industrial",
    "transportation",
    "video",
    # "foglamp",
    "elasticsearch",
]

# Experiments (reuse simple mappings)
EXPERIMENTS = {
    "pre-copy": "-pre -d --tcp-established --shell-job",
    # "pre-copy-1": "-pre -d --tcp-established --shell-job -z 1",
    # "pre-copy-2": "-pre -d --tcp-established --shell-job -z 2",
    # "pre-copy-3": "-pre -d --tcp-established --shell-job -z 3",
    # "pre-copy-4": "-pre -d --tcp-established --shell-job -z 4",
    "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    "post-copy": "-post -d --tcp-established --shell-job",
    "hybrid": "-pre -post -d --tcp-established --shell-job",
    "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}

# NOTE: fog_test is self-contained and no longer relies on defog_test.py. All helpers and cleanup
# are implemented below to ensure reliable cleaning and per-service testing.

# Self-contained implementation: robust cleanup, per-service start/smoke/bench/collect/baseline
import json
import signal
import atexit

# override SCENES with explicit scene-to-bundle/endpoint/asset/bench mapping
SCENE_INFO = {
    "gocr": {"bundle": "gocr", "endpoint": "/ocr", "asset": "/runc/datasets/ocr/0001.png", "bench": None, "persistent": False},
    "gzip": {"bundle": "gzip", "endpoint": "/compress", "asset": "/runc/datasets/compress/sample.bin", "bench": None, "persistent": False},  # gzip 为无状态；默认不需要文件锁/持久化
    "yolo": {"bundle": "yolo", "endpoint": "/detect", "asset": "/runc/datasets/images/dog.jpg", "bench": None, "persistent": False},
    "pocketsphinx": {"bundle": "pocketsphinx", "endpoint": "/transcribe", "asset": "/runc/datasets/audio/sample.wav", "bench": None, "persistent": False},
    "aeneas": {"bundle": "aeneas", "endpoint": "/align", "asset": "/runc/datasets/audio/sample.mp3,/runc/datasets/ocr/sample.xhtml", "bench": None, "persistent": False},

    # Service-level scenes
    "sensoragg": {"bundle": "sensoragg", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8181, "persistent": False},
    "cartelem": {"bundle": "cartelem", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8181, "persistent": False},
    "ipokemon": {"bundle": "ipokemon", "endpoint": "/", "asset": None, "bench": None, "default_port": 8000, "persistent": True},
    "video": {"bundle": "video", "endpoint": "redis", "asset": "/runc/datasets/images/dog.jpg", "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    "transportation": {"bundle": "transportation", "endpoint": "redis", "asset": None, "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    "industrial": {"bundle": "industrial", "endpoint": "redis", "asset": None, "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    # "foglamp": {"bundle": "foglamp", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8080, "persistent": False},
    "elasticsearch": {"bundle": "elasticsearch", "endpoint": "/_cluster/health", "asset": None, "bench": None, "default_port": 9200, "persistent": True},
}

KEEP_RUNNING = True
CURRENT_RUNNING = []
# Optional bench override flags (if set, fog_test will pass these to bench scripts). By default we do not
# pass --dataset or --file(s) so benches can use their own defaults.
BENCH_DATASET: Optional[str] = None
BENCH_FILES: Optional[str] = None


def ensure_dirs():
    # Ensure necessary result and containers directories exist
    os.makedirs(RESULTS_ROOT, exist_ok=True)
    os.makedirs(os.path.join(RESULTS_ROOT, RUN_LABEL), exist_ok=True)
    os.makedirs(os.path.join(RESULTS_ROOT, RUN_LABEL, 'workloads'), exist_ok=True)
    os.makedirs('/runc/containers', exist_ok=True)

    # Prepare structured tmp area for fog_test logs/pids to keep /tmp tidy
    try:
        os.makedirs(TMP_DEST_LOGS, exist_ok=True)
        os.makedirs(TMP_BENCH_LOGS, exist_ok=True)
        os.makedirs(TMP_RECVTTY_LOGS, exist_ok=True)
        os.makedirs(TMP_PIDS, exist_ok=True)
    except Exception:
        pass

    purge_fog_workload_baks()


def purge_fog_workload_baks():
    """Remove any /runc/fog_workloads/*.bak bundles (deprecated)."""
    fw_root = '/runc/fog_workloads'
    if not os.path.isdir(fw_root):
        return
    removed = []
    for name in os.listdir(fw_root):
        if not name.endswith('.bak'):
            continue
        path = os.path.join(fw_root, name)
        if os.path.isdir(path):
            try:
                shutil.rmtree(path, ignore_errors=True)
                removed.append(path)
            except Exception:
                pass
    if removed:
        print(f"[bundle] removed deprecated fog_workloads backups: {', '.join(removed)}")


def get_run_dir() -> str:
    return os.path.join(RESULTS_ROOT, RUN_LABEL)


def ensure_workload_dir(scene: str) -> str:
    base = os.path.join(get_run_dir(), 'workloads', scene)
    os.makedirs(base, exist_ok=True)
    return base


def build_bench_output_paths(scene: str, exp_name: str, run_index: int, backend: Optional[str] = None):
    base = ensure_workload_dir(scene)
    ext = 'jtl' if backend == 'jmeter' else 'csv'
    raw_out = os.path.join(base, f"{scene}_{exp_name}_run{run_index}.{ext}")
    metrics_out = os.path.join(base, f"{scene}_{exp_name}_run{run_index}_metrics.json")
    return raw_out, metrics_out


def sanitize_profile(bundle_path: str, remote: bool = False, target_ip: Optional[str] = None):
    profile_path = os.path.join(bundle_path, 'rootfs', 'root', '.profile')
    patcher = (
        "python3 - <<'PY'\n"
        f"from pathlib import Path\npath = Path('{profile_path}')\n"
        "if path.exists():\n"
        "    txt = path.read_text()\n"
        "    new = txt.replace('mesg n || true', 'tty -s && mesg n || true')\n"
        "    new = new.replace('mesg n 2> /dev/null || true', 'tty -s && mesg n 2> /dev/null || true')\n"
        "    if 'mesg n' in txt and txt != new:\n"
        "        path.write_text(new)\n"
        "PY\n"
    )
    if remote and target_ip not in LOCAL_HOSTS and target_ip:
        run_remote_cmd(patcher, target_ip, ignore_error=True, quiet=True)
    else:
        if os.path.exists(profile_path):
            try:
                txt = open(profile_path).read()
                new = txt.replace('mesg n || true', 'tty -s && mesg n || true')
                new = new.replace('mesg n 2> /dev/null || true', 'tty -s && mesg n 2> /dev/null || true')
                if 'mesg n' in txt and txt != new:
                    with open(profile_path, 'w') as pf:
                        pf.write(new)
                    print(f"[bundle] sanitized .profile mesg guard in {profile_path}")
            except Exception:
                pass


def kill_destination_listener(target_ip: Optional[str] = None):
    """Ensure no stale destination.py server keeps port 18863 busy."""
    cmds = [
        "fuser -k 18863/tcp",
        "pkill -f 'mig-scripts/destination.py'",
        "pkill -f 'destination.py'",
    ]
    target = target_ip or DEST_IP
    if target in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        for c in cmds:
            run_cmd(c, ignore_error=True, quiet=True)
    else:
        for c in cmds:
            run_remote_cmd(c, target, ignore_error=True, quiet=True)


def write_run_record(mode: str, scene: str, exp: Optional[str], run_idx: int, payload: dict):
    out_dir = os.path.join(RESULTS_ROOT, RUN_LABEL, mode)
    os.makedirs(out_dir, exist_ok=True)
    exp_suffix = f"_{exp}" if exp else ""
    fname = f"{scene}_run-{run_idx}{exp_suffix}.json"
    out_path = os.path.join(out_dir, fname)
    try:
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
    except Exception as exc:
        print(f"[result] failed to write per-run record {out_path}: {exc}")
    else:
        print(f"[result] per-run record -> {out_path}")


def write_integrated_table(results: list, mode: str) -> Optional[str]:
    rows = [r for r in results if isinstance(r, dict) and isinstance(r.get('metrics'), dict)]
    if not rows:
        return None

    metric_keys: list[str] = []
    for r in rows:
        for k in r.get('metrics', {}).keys():
            if k not in metric_keys:
                metric_keys.append(k)

    headers = ["mode", "exp", "scene", "run"] + metric_keys
    out_dir = os.path.join(RESULTS_ROOT, RUN_LABEL)
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{mode}_integrated.tsv")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\t".join(headers) + "\n")
        for r in rows:
            row = [mode, str(r.get("exp", "")), str(r.get("scene", "")), str(r.get("run", ""))]
            metrics = r.get("metrics", {}) or {}
            for key in metric_keys:
                row.append(str(metrics.get(key, "")))
            f.write("\t".join(row) + "\n")

    print(f"[result] integrated table -> {out_path}")
    return out_path


# --- Diagnostics & error recording helpers ---
import traceback as _traceback
import tarfile as _tarfile
from collections import deque as _deque


def _tail_file_to_path(src: str, dst: str, lines: int = 500) -> Optional[str]:
    try:
        if not os.path.exists(src):
            return None
        with open(src, 'r', errors='ignore') as fi:
            dq = _deque(fi, maxlen=lines)
        with open(dst, 'w', encoding='utf-8') as fo:
            fo.writelines(dq)
        return dst
    except Exception:
        try:
            shutil.copy2(src, dst)
            return dst
        except Exception:
            return None


def _safe_copy_to_dst(src: str, dst: str, tail_lines: int = 500) -> str | None:
    if not os.path.exists(src):
        return None
    if tail_lines:
        return _tail_file_to_path(src, dst, lines=tail_lines)
    try:
        shutil.copy2(src, dst)
        return dst
    except Exception:
        return None


def _fetch_remote_files(remote_ip: str, patterns: list, dest_dir: str, tail_lines: int = 500) -> list:
    saved = []
    import shlex as _shlex
    for pat in patterns:
        # list matching files on remote (best-effort)
        list_cmd = f"sh -c 'ls -1 {pat} 2>/dev/null || true'"
        try:
            res = run_remote_cmd(list_cmd, remote_ip, ignore_error=True, quiet=True)
        except Exception:
            continue
        out = getattr(res, 'stdout', '') or ''
        files = [ln.strip() for ln in out.splitlines() if ln.strip()]
        for remote_file in files:
            base = os.path.basename(remote_file)
            local_path = os.path.join(dest_dir, f"remote_{base}")
            tail_cmd = f"tail -n {int(tail_lines)} {_shlex.quote(remote_file)} || true"
            try:
                r2 = run_remote_cmd(tail_cmd, remote_ip, ignore_error=True, quiet=True)
                content = getattr(r2, 'stdout', '') or ''
                with open(local_path, 'w', encoding='utf-8') as fo:
                    fo.write(content)
                saved.append(local_path)
            except Exception:
                pass
    return saved


def collect_run_diagnostics(scene: str, run_idx: int, exp_name: str, dest_ip: Optional[str] = None, client_ip: Optional[str] = None, tail_lines: int = 500) -> list:
    """Collect a set of useful logs (local + remote when available) and package them for later inspection.

    Returns a list of saved file paths (including the created tarball if successful).
    """
    diag_base = os.path.join(get_run_dir(), 'diagnostics')
    os.makedirs(diag_base, exist_ok=True)
    ts = int(time.time())
    diag_dir = os.path.join(diag_base, f"{scene}_{exp_name}_run{run_idx}_{ts}")
    os.makedirs(diag_dir, exist_ok=True)
    saved = []

    # Local patterns (prefer structured TMP_ROOT but include legacy /tmp fallbacks)
    local_patterns = [
        os.path.join(TMP_RECVTTY_LOGS, f"recvtty_{scene}.log"),
        os.path.join(TMP_RECVTTY_LOGS, f"recvtty_{scene}*.log"),
        os.path.join(TMP_BENCH_LOGS, f"bench_{scene}.log"),
        os.path.join(TMP_DEST_LOGS, f"{DEST_SCRIPT.replace('.', '_')}_{scene}_*.log"),
        os.path.join(TMP_ROOT, f"destination_pre_restore_*.log"),
        os.path.join(TMP_ROOT, f"destination_reply*"),
        f"/tmp/recvtty_{scene}.log",
        f"/tmp/recvtty_{scene}*.log",
        f"/tmp/bench_{scene}.log",
        f"/tmp/destination_py_{scene}_*.log",
        f"/tmp/destination_pre_restore_*.log",
        f"/tmp/destination_reply*",
    ]

    for pat in local_patterns:
        for f in glob.glob(pat):
            try:
                dst = os.path.join(diag_dir, os.path.basename(f))
                sp = _safe_copy_to_dst(f, dst, tail_lines=tail_lines)
                if sp:
                    saved.append(sp)
            except Exception:
                pass

    # local CRIU restore log
    local_restore = os.path.join('/runc/containers', scene, 'migrate', 'image', 'r_log', 'restore.log')
    if os.path.exists(local_restore):
        try:
            dst = os.path.join(diag_dir, 'restore.log')
            _safe_copy_to_dst(local_restore, dst, tail_lines=tail_lines)
            saved.append(dst)
        except Exception:
            pass

    # Fetch remote logs if dest is remote
    if dest_ip and dest_ip not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_patterns = [
            os.path.join(remote_tmp, 'logs', 'destination', f"{DEST_SCRIPT.replace('.', '_')}_{scene}_*.log"),
            os.path.join(remote_tmp, f"destination_pre_restore_*.log"),
            os.path.join(remote_tmp, f"destination_reply*"),
            os.path.join('/runc/containers', scene, 'migrate', 'image', 'r_log', 'restore.log'),
            f"/tmp/destination_py_{scene}_*.log",
            f"/tmp/destination_pre_restore_*.log",
            f"/tmp/destination_reply*",
        ]
        try:
            saved_remote = _fetch_remote_files(dest_ip, remote_patterns, diag_dir, tail_lines=tail_lines)
            saved.extend(saved_remote)
        except Exception:
            pass

        # capture remote ss and runc state
        try:
            ss = run_remote_cmd('ss -tnp || true', dest_ip, ignore_error=True, quiet=True)
            p = os.path.join(diag_dir, f"remote_ss_{dest_ip}.txt")
            with open(p, 'w', encoding='utf-8') as fo:
                fo.write(getattr(ss, 'stdout', '') or '')
            saved.append(p)
        except Exception:
            pass
        try:
            st = run_remote_cmd(f"runc state {shlex.quote(scene)} || true", dest_ip, ignore_error=True, quiet=True)
            p = os.path.join(diag_dir, f"remote_runc_state_{dest_ip}.txt")
            with open(p, 'w', encoding='utf-8') as fo:
                fo.write((getattr(st, 'stdout', '') or '') + (getattr(st, 'stderr', '') or ''))
            saved.append(p)
        except Exception:
            pass

    # capture local network/process state
    try:
        ss = run_cmd('ss -tnp || true', quiet=True, ignore_error=True)
        p = os.path.join(diag_dir, 'local_ss.txt')
        with open(p, 'w', encoding='utf-8') as fo:
            fo.write(getattr(ss, 'stdout', '') or '')
        saved.append(p)
    except Exception:
        pass
    try:
        st = run_cmd(f"runc state {shlex.quote(scene)} || true", quiet=True, ignore_error=True)
        p = os.path.join(diag_dir, 'local_runc_state.txt')
        with open(p, 'w', encoding='utf-8') as fo:
            fo.write((getattr(st, 'stdout', '') or '') + (getattr(st, 'stderr', '') or ''))
        saved.append(p)
    except Exception:
        pass

    # Capture local IP addresses and routes for VIP verification
    try:
        ipaddr = run_cmd('ip -4 addr show', quiet=True, ignore_error=True)
        p = os.path.join(diag_dir, 'local_ip_addrs.txt')
        with open(p, 'w', encoding='utf-8') as fo:
            fo.write(getattr(ipaddr, 'stdout', '') or '')
        saved.append(p)
    except Exception:
        pass
    try:
        iprt = run_cmd('ip route show', quiet=True, ignore_error=True)
        p = os.path.join(diag_dir, 'local_ip_route.txt')
        with open(p, 'w', encoding='utf-8') as fo:
            fo.write(getattr(iprt, 'stdout', '') or '')
        saved.append(p)
    except Exception:
        pass

    # Fetch remote IP information if dest is remote
    if dest_ip and dest_ip not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        try:
            rip = run_remote_cmd('ip -4 addr show', dest_ip, ignore_error=True, quiet=True)
            p = os.path.join(diag_dir, f'remote_ip_addrs_{dest_ip}.txt')
            with open(p, 'w', encoding='utf-8') as fo:
                fo.write(getattr(rip, 'stdout', '') or '')
            saved.append(p)
        except Exception:
            pass
        try:
            rrt = run_remote_cmd('ip route show', dest_ip, ignore_error=True, quiet=True)
            p = os.path.join(diag_dir, f'remote_ip_route_{dest_ip}.txt')
            with open(p, 'w', encoding='utf-8') as fo:
                fo.write(getattr(rrt, 'stdout', '') or '')
            saved.append(p)
        except Exception:
            pass

    # create tarball
    tarball = os.path.join(diag_base, f"{scene}_{exp_name}_run{run_idx}_diagnostics_{ts}.tar.gz")
    try:
        with _tarfile.open(tarball, 'w:gz') as tf:
            for f in saved:
                try:
                    tf.add(f, arcname=os.path.basename(f))
                except Exception:
                    pass
        saved.append(tarball)
    except Exception:
        pass

    return saved


def fetch_remote_workload_metrics(scene: str, exp_name: str, run_idx: int, remote_host: str | None = None, tail_lines: int = 500, remove_remote: bool = False) -> list:
    """Fetch bench outputs (metrics JSON and raw) from remote client host into local workload dir.

    This is best-effort and will try several candidate locations on the remote host. Returns a
    list of local file paths successfully fetched (empty list on none).
    """
    saved = []
    if not remote_host or remote_host in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        return saved
    try:
        raw_out, metrics_out = build_bench_output_paths(scene, exp_name, run_idx)
        local_dir = os.path.dirname(metrics_out)
        os.makedirs(local_dir, exist_ok=True)
    except Exception as e:
        print(f"[bench-fetch] failed to compute local output paths: {e}")
        return saved

    candidates = []
    candidates.append(metrics_out)
    candidates.append(raw_out)
    candidates.append(os.path.join(f"/tmp/fog_test/{RUN_LABEL}/workloads/{scene}", os.path.basename(metrics_out)))
    candidates.append(os.path.join(f"/tmp/fog_test/{RUN_LABEL}/workloads/{scene}", os.path.basename(raw_out)))
    candidates.append(os.path.join("/tmp", os.path.basename(metrics_out)))
    candidates.append(os.path.join("/tmp", os.path.basename(raw_out)))

    for remote_path in candidates:
        try:
            if not remote_path:
                continue
            check = run_remote_cmd(f"test -f {shlex.quote(remote_path)} && echo EXISTS || echo MISSING", remote_host, ignore_error=True, quiet=True)
            out = (getattr(check, 'stdout', '') or '').strip()
            if not out or out.splitlines()[0].strip() != 'EXISTS':
                continue
            cat = run_remote_cmd(f"cat {shlex.quote(remote_path)}", remote_host, ignore_error=True, quiet=True)
            content = getattr(cat, 'stdout', '') or ''
            if not content:
                continue
            local_name = os.path.basename(remote_path)
            local_path = os.path.join(local_dir, local_name)
            try:
                with open(local_path, 'w', encoding='utf-8') as fo:
                    fo.write(content)
                saved.append(local_path)
                print(f"[bench-fetch] fetched {remote_path} from {remote_host} -> {local_path}")
                # optionally remove remote copy
                if remove_remote:
                    try:
                        run_remote_cmd(f"rm -f {shlex.quote(remote_path)}", remote_host, ignore_error=True, quiet=True)
                    except Exception:
                        pass
            except Exception as e:
                print(f"[bench-fetch] failed to write fetched file {local_path}: {e}")

            # also try to grab remote bench log tail
            try:
                remote_log = f"/tmp/fog_test/{RUN_LABEL}/logs/bench/bench_{scene}.log"
                rlog = run_remote_cmd(f"tail -n {int(tail_lines)} {shlex.quote(remote_log)} || true", remote_host, ignore_error=True, quiet=True)
                logcontent = getattr(rlog, 'stdout', '') or ''
                if logcontent:
                    local_log = os.path.join(TMP_BENCH_LOGS, f"remote_bench_{scene}.log")
                    os.makedirs(os.path.dirname(local_log), exist_ok=True)
                    with open(local_log, 'w', encoding='utf-8') as lf:
                        lf.write(logcontent)
                    saved.append(local_log)
            except Exception:
                pass
        except Exception as e:
            print(f"[bench-fetch] error checking remote file {remote_path} on {remote_host}: {e}")
            continue

    if not saved:
        print(f"[bench-fetch] no remote bench metrics found on {remote_host} for {scene} run {run_idx}")
    else:
        print(f"[bench-fetch] fetched files: {saved}")
    return saved


def write_run_error_file(run_result: dict, scene: str, run_idx: int, exp_name: str) -> str | None:
    errors_dir = os.path.join(get_run_dir(), 'errors')
    os.makedirs(errors_dir, exist_ok=True)
    fname = f"{scene}_run-{run_idx}_{exp_name}_error.json"
    path = os.path.join(errors_dir, fname)
    try:
        with open(path, 'w', encoding='utf-8') as fo:
            json.dump(run_result, fo, indent=2)
        return path
    except Exception:
        return None


def append_scene_exp_summary(scene: str, exp_name: str, run_metrics: List[dict]) -> Optional[str]:
    if not run_metrics:
        return None

    metric_values: dict[str, list[float]] = {}
    for m in run_metrics:
        if not isinstance(m, dict):
            continue
        for k, v in m.items():
            try:
                fv = float(v)
            except Exception:
                continue
            metric_values.setdefault(k, []).append(fv)

    if not metric_values:
        return None

    out_path = os.path.join(get_run_dir(), f"mig_test_{scene}.csv")
    is_new = not os.path.exists(out_path)
    with open(out_path, "a", encoding="utf-8") as f:
        if is_new:
            f.write("exp\tmetric\tmean\tstdev\truns\n")
        for metric, vals in metric_values.items():
            mean = statistics.mean(vals)
            stdev = statistics.stdev(vals) if len(vals) > 1 else 0.0
            f.write(f"{exp_name}\t{metric}\t{mean:.6f}\t{stdev:.6f}\t{len(vals)}\n")
    return out_path


def ensure_vm_max_map_count(target: int = 262144):
    """Ensure vm.max_map_count is high enough for Elasticsearch."""
    try:
        res = run_cmd("sysctl -n vm.max_map_count", quiet=True, ignore_error=True)
        cur_raw = (getattr(res, 'stdout', '') or '').strip()
        current = int(cur_raw) if cur_raw else 0
    except Exception:
        current = 0
    if current < target:
        try:
            run_cmd(f"sysctl -w vm.max_map_count={int(target)}", quiet=True, ignore_error=True)
            print(f"[sysctl] vm.max_map_count raised to {int(target)}")
        except Exception as exc:
            print(f"[sysctl] failed to raise vm.max_map_count: {exc}")


def patch_bundle_port(bundle_path: str, port: int):
    """Patch config.json under bundle to pass the desired port to the service entrypoint.

    This mirrors the logic used in start_service but is extracted so migration staging can
    reuse it for /runc/containers bundles.
    """
    cfg = os.path.join(bundle_path, 'config.json')
    if not os.path.exists(cfg):
        return
    try:
        import json as _json
        cfgj = _json.load(open(cfg))
        cfgj.setdefault('process', {})
        cfgj['process']['terminal'] = False
        exec_sh = os.path.join(bundle_path, 'rootfs', 'root', 'scripts', 'execute.sh')
        entry_candidates = [
            (os.path.join(bundle_path, 'rootfs', 'bin', 'entrypoint.sh'), '/bin/entrypoint.sh'),
            (os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'entrypoint.sh'), '/usr/bin/entrypoint.sh'),
            (os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'docker-entrypoint.sh'), '/usr/local/bin/docker-entrypoint.sh'),
        ]
        if os.path.exists(exec_sh):
            cfgj['process']['args'] = ['sh', '-lc', f"/root/scripts/execute.sh --server --port {int(port)} && exec sleep infinity"]
            with open(cfg, 'w') as _cfh:
                _json.dump(cfgj, _cfh)
            print(f"[start] patched {cfg} to pass --port {int(port)} to execute.sh")
            return

        used = None
        for p, container_path in entry_candidates:
            if os.path.exists(p):
                used = container_path
                break
        if used:
            influx_bin = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'influxdb3')
            redis_bin1 = os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'redis-server')
            redis_bin2 = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'redis-server')
            if os.path.exists(influx_bin):
                cfgj['process']['args'] = ['sh', '-lc', f"{used} influxdb3 serve --object-store=memory --node-id=node0 --without-auth --wal-flush-interval=20ms --http-bind 0.0.0.0:{int(port)} && exec sleep infinity"]
                print(f"[start] patched {cfg} to start {used} influxdb3 serve on port {int(port)}")
            elif os.path.exists(redis_bin1) or os.path.exists(redis_bin2):
                cfgj['process']['args'] = ['sh', '-lc', f"{used} redis-server --bind 0.0.0.0 --port {int(port)} && exec sleep infinity"]
                print(f"[start] patched {cfg} to start {used} redis-server on port {int(port)}")
            else:
                cfgj['process']['args'] = ['sh', '-lc', f"{used} serve --http-bind 0.0.0.0:{int(port)} && exec sleep infinity"]
                print(f"[start] patched {cfg} to start {used} serve on port {int(port)}")
            with open(cfg, 'w') as _cfh:
                _json.dump(cfgj, _cfh)
        else:
            # fallback: if redis-server binary exists but no entrypoint detected
            redis_bin1 = os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'redis-server')
            redis_bin2 = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'redis-server')
            if os.path.exists(redis_bin1) or os.path.exists(redis_bin2):
                cfgj['process']['args'] = ['sh', '-lc', f"/usr/local/bin/docker-entrypoint.sh redis-server --bind 0.0.0.0 --port {int(port)} && exec sleep infinity"]
                with open(cfg, 'w') as _cfh:
                    _json.dump(cfgj, _cfh)
                print(f"[start] patched {cfg} to start /usr/local/bin/docker-entrypoint.sh redis-server on port {int(port)}")
    except Exception as e:
        print(f"[start] failed to patch {bundle_path} port: {e}")


def stage_bundle_from_fog_to_containers(scene: str, remote: bool = False, target_ip: Optional[str] = None) -> str:
    """Copy canonical fog_workloads/<scene> into /runc/containers/<scene> for migration."""
    src_root = '/runc/fog_workloads'
    dest_root = '/runc/containers'
    purge_fog_workload_baks()
    src = os.path.join(src_root, scene)
    if not os.path.isdir(src):
        raise FileNotFoundError(f"canonical bundle missing: {src}")
    dest = os.path.join(dest_root, scene)
    try:
        # ensure any tmpfs mounts from previous runs are removed before deleting the bundle
        unmount_local_migration_tmpfs(scene)
    except Exception:
        pass
    cmd = f"rm -rf {shlex.quote(dest)} && cp -r {shlex.quote(src)} {shlex.quote(dest)}"
    target = target_ip or DEST_IP
    if remote and target not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        run_remote_cmd(cmd, target, ignore_error=False)
        sanitize_profile(dest, remote=True, target_ip=target)
        # Force container bundle to request a terminal for migration (so recvtty will be used)
        try:
            term_cmd = (
                "python3 - <<'PY'\n"
                f"import json, os\ncfg='{dest}/config.json'\n"
                "try:\n"
                "    if os.path.exists(cfg):\n"
                "        data=json.load(open(cfg))\n"
                "        data.setdefault('process', {})\n"
                "        data['process']['terminal']=True\n"
                "        json.dump(data, open(cfg,'w'))\n"
                "except Exception as e:\n"
                "    print('term-patch-failed', e)\n"
                "PY"
            )
            run_remote_cmd(term_cmd, target, ignore_error=True, quiet=True)
        except Exception:
            pass
    else:
        run_cmd(cmd, ignore_error=False)
        try:
            ensure_assets_in_bundle(dest, scene)
        except Exception:
            pass
        sanitize_profile(dest, remote=False)
        # Force local staged bundle to request a terminal for migration
        try:
            cfg_path_local = os.path.join(dest, 'config.json')
            if os.path.exists(cfg_path_local):
                try:
                    j = json.load(open(cfg_path_local))
                    j.setdefault('process', {})
                    j['process']['terminal'] = True
                    with open(cfg_path_local, 'w') as _cfh:
                        json.dump(j, _cfh)
                    print(f"[stage] forced process.terminal=true in {cfg_path_local}")
                except Exception as e:
                    print(f"[stage] failed to set terminal true: {e}")
        except Exception:
            pass
    return dest


def start_recvtty_for_bundle(bundle_path: str, scene: str, remote: bool = False, target_ip: Optional[str] = None, mode: str = 'null'):
    console_sock = os.path.join(bundle_path, 'console.sock')
    # local paths
    local_pidfile = os.path.join(TMP_PIDS, f"recvtty_{scene}_{'remote' if remote else 'local'}.pid")
    local_log = os.path.join(TMP_RECVTTY_LOGS, f"recvtty_{scene}.log")
    target = target_ip or DEST_IP

    if remote and target not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_pidfile = os.path.join(remote_tmp, 'pids', f"recvtty_{scene}_remote.pid")
        remote_log = os.path.join(remote_tmp, 'logs', 'recvtty', f"recvtty_{scene}.log")
        cmd = f"mkdir -p {shlex.quote(os.path.dirname(remote_log))} {shlex.quote(os.path.dirname(remote_pidfile))} ; PATH=$PATH:/root/go/bin recvtty -m {mode} {shlex.quote(console_sock)} > {shlex.quote(remote_log)} 2>&1 & echo $! > {shlex.quote(remote_pidfile)}"
        run_remote_cmd(cmd, target, ignore_error=True, quiet=True)
        return console_sock, remote_pidfile
    else:
        cmd = f"PATH=$PATH:/root/go/bin recvtty -m {mode} {shlex.quote(console_sock)} > {shlex.quote(local_log)} 2>&1 & echo $! > {shlex.quote(local_pidfile)}"
        # ensure local dirs exist
        try:
            os.makedirs(os.path.dirname(local_log), exist_ok=True)
            os.makedirs(os.path.dirname(local_pidfile), exist_ok=True)
        except Exception:
            pass
        run_cmd(cmd, ignore_error=True, quiet=True)
        return console_sock, local_pidfile


def start_container_for_migration(scene: str, port: int) -> str:
    if scene == 'elasticsearch':
        ensure_vm_max_map_count()
    bundle_path = os.path.join('/runc/containers', scene)
    patch_bundle_port(bundle_path, port)
    console_opt = ''
    # Force the staged bundle to request a terminal so migration runs use recvtty
    try:
        cfg_path_local = os.path.join(bundle_path, 'config.json')
        if os.path.exists(cfg_path_local):
            try:
                cfg_j = json.load(open(cfg_path_local))
                cfg_j.setdefault('process', {})
                if not cfg_j['process'].get('terminal'):
                    cfg_j['process']['terminal'] = True
                    with open(cfg_path_local, 'w') as _cfh:
                        json.dump(cfg_j, _cfh)
                    print(f"[start] forced process.terminal=true in {cfg_path_local}")
            except Exception as e:
                print(f"[start] failed to force terminal in {cfg_path_local}: {e}")
        # re-evaluate needs_console from the (possibly modified) config
        cfgj = json.load(open(os.path.join(bundle_path, 'config.json')))
        needs_console = bool(cfgj.get('process', {}).get('terminal', False))
    except Exception:
        needs_console = False
    if needs_console:
        console_sock, pidfile = start_recvtty_for_bundle(bundle_path, scene, remote=False)
        print(f"[start] recvtty started for migration bundle {scene} on {console_sock} (pidfile {pidfile})")
        console_opt = f"--console-socket {shlex.quote(console_sock)}"

    # ensure no stale container
    run_cmd(f"runc kill {shlex.quote(scene)} KILL", ignore_error=True, quiet=True)
    run_cmd(f"runc delete {shlex.quote(scene)}", ignore_error=True, quiet=True)
    try:
        ensure_deleted(scene)
    except Exception:
        pass
    cmd = f"runc run {console_opt} -d -b {shlex.quote(bundle_path)} {shlex.quote(scene)}".strip()
    start_timeout = 60 if scene == 'elasticsearch' else 30
    res = run_cmd(cmd, ignore_error=True, quiet=False, timeout=start_timeout)
    if getattr(res, 'returncode', 1) != 0:
        try:
            rr = run_cmd("runc list -q", quiet=True, ignore_error=True)
            names = [ln.strip() for ln in rr.stdout.splitlines()]
        except Exception:
            names = []
        if scene in names:
            print(f"[start-mig] note: container {scene} appears running despite rc={getattr(res,'returncode',None)}")
        else:
            out = (getattr(res, 'stdout', '') or '') + (getattr(res, 'stderr', '') or '')
            print(f"[start-mig] runc run failed for {scene}: {out[:200]}")
    else:
        print(f"[start-mig] container {scene} running from {bundle_path} on port {port}")
    return bundle_path


def source_prepare_migration(scene: str, port: int):
    local_dest = DEST_IP in (None, '127.0.0.1', 'localhost', SOURCE_IP)
    bundle_path = os.path.join('/runc/containers', scene)
    if local_dest and os.path.isdir(bundle_path):
        print(f"[stage] reusing existing bundle at {bundle_path} for local dest")
        bundle = bundle_path
    else:
        bundle = stage_bundle_from_fog_to_containers(scene, remote=False)
    patch_bundle_port(bundle, port)
    start_container_for_migration(scene, port)


def destination_prepare_migration(scene: str, port: int):
    kill_destination_listener(DEST_IP)
    bundle = stage_bundle_from_fog_to_containers(scene, remote=True, target_ip=DEST_IP)
    # Best-effort port patch on destination via inline python to avoid assuming local FS access
    try:
        patch_cmd = (
            "python3 - <<'PY'\n"
            f"import json, os\ncfg='{bundle}/config.json'\n"
            "\n"
            "def patch(cfg, port):\n"
            "    if not os.path.exists(cfg):\n"
            "        return\n"
            "    data=json.load(open(cfg))\n"
            "    data.setdefault('process', {})\n"
            "    args=data.get('process',{}).get('args',[])\n"
            "    for i,a in enumerate(args):\n"
            "        if '--port' in str(a):\n"
            "            args[i]='--port'\n"
            "            if i+1 < len(args): args[i+1]=str(port)\n"
            "    data['process']['args']=args\n"
            "    json.dump(data, open(cfg,'w'))\n"
            "patch(cfg,{int(port)})\n"
            "PY"
        )
        if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
            run_cmd(patch_cmd, ignore_error=True, quiet=True)
        else:
            run_remote_cmd(patch_cmd, DEST_IP, ignore_error=True, quiet=True)
        # Ensure the bundle requests a terminal on destination so recvtty will be used
        term_cmd = (
            "python3 - <<'PY'\n"
            f"import json, os\ncfg='{bundle}/config.json'\n"
            "try:\n"
            "    if os.path.exists(cfg):\n"
            "        data=json.load(open(cfg))\n"
            "        data.setdefault('process', {})\n"
            "        data['process']['terminal']=True\n"
            "        json.dump(data, open(cfg,'w'))\n"
            "except Exception as e:\n"
            "    print('term-patch-failed', e)\n"
            "PY"
        )
        if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
            run_cmd(term_cmd, ignore_error=True, quiet=True)
        else:
            run_remote_cmd(term_cmd, DEST_IP, ignore_error=True, quiet=True)
    except Exception:
        pass
    # Start recvtty on destination only if the bundle requests a console
    needs_console = False
    try:
        cfg_remote = os.path.join(bundle, 'config.json')
        if DEST_IP in (None, '127.0.0.1', 'localhost', SOURCE_IP):
            if os.path.exists(cfg_remote):
                cfg = json.load(open(cfg_remote))
                needs_console = bool(cfg.get('process', {}).get('terminal', False))
        else:
            probe_cmd = (
                "python3 - <<'PY'\n"
                "import json\n"
                f"cfg='{cfg_remote}'\n"
                "try:\n"
                "    data=json.load(open(cfg))\n"
                "    print(bool(data.get('process', {}).get('terminal', False)))\n"
                "except Exception:\n"
                "    print(False)\n"
                "PY"
            )
            try:
                res = run_remote_cmd(probe_cmd, DEST_IP, ignore_error=True, quiet=True)
                out = (getattr(res, 'stdout', '') or '').strip().lower()
                needs_console = out.startswith('true')
            except Exception:
                needs_console = False
    except Exception:
        needs_console = False

    if needs_console:
        start_recvtty_for_bundle(bundle, scene, remote=True, target_ip=DEST_IP)

    ts = int(time.time())
    # local structured tmp paths
    dest_log = os.path.join(TMP_DEST_LOGS, f"{DEST_SCRIPT.replace('.', '_')}_{scene}_{ts}.log")
    dest_pidfile = os.path.join(TMP_PIDS, f"destination_{scene}.pid")
    python_bin_remote = sys.executable if DEST_IP in (None, '127.0.0.1', 'localhost', SOURCE_IP) else 'python3'
    # Ensure local dirs exist
    try:
        os.makedirs(os.path.dirname(dest_log), exist_ok=True)
        os.makedirs(os.path.dirname(dest_pidfile), exist_ok=True)
    except Exception:
        pass
    start_dest_cmd = (
        f"FOG_TMP_DIR={shlex.quote(TMP_ROOT)} nohup {shlex.quote(python_bin_remote)} /runc/dirty-track/mig-scripts/{DEST_SCRIPT} > {shlex.quote(dest_log)} 2>&1 & echo $! > {shlex.quote(dest_pidfile)}"
    )
    if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
        run_cmd(start_dest_cmd, ignore_error=False)
    else:
        # remote structured tmp paths
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_dest_log = os.path.join(remote_tmp, 'logs', 'destination', f"{DEST_SCRIPT.replace('.', '_')}_{scene}_{ts}.log")
        remote_dest_pid = os.path.join(remote_tmp, 'pids', f"destination_{scene}.pid")
        remote_cmd = (
            f"mkdir -p {shlex.quote(os.path.dirname(remote_dest_log))} {shlex.quote(os.path.dirname(remote_dest_pid))} ; FOG_TMP_DIR={shlex.quote(remote_tmp)} nohup python3 /runc/dirty-track/mig-scripts/{DEST_SCRIPT} > {shlex.quote(remote_dest_log)} 2>&1 & echo $! > {shlex.quote(remote_dest_pid)}"
        )
        run_remote_cmd(remote_cmd, DEST_IP, ignore_error=False)


def destination_clean_migration(scene: str):
    kill_destination_listener(DEST_IP)
    cmds = [
        (f"runc kill {scene} KILL", True),
        (f"runc delete {scene}", True),
        (f"sh -c 'for f in {shlex.quote(os.path.join(TMP_PIDS, f'recvtty_{scene}_*.pid'))}; do if [ -f \"$f\" ]; then kill -TERM $(cat \"$f\") 2>/dev/null || true; rm -f \"$f\"; fi; done'", True),
    ]

    if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
        for c, ign in cmds:
            run_cmd(c, ignore_error=ign, quiet=True)
        run_cmd("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)
        run_cmd(f"if [ -f {shlex.quote(os.path.join(TMP_PIDS, f'destination_{scene}.pid'))} ]; then kill -TERM $(cat {shlex.quote(os.path.join(TMP_PIDS, f'destination_{scene}.pid'))}) 2>/dev/null || true; rm -f {shlex.quote(os.path.join(TMP_PIDS, f'destination_{scene}.pid'))}; fi", ignore_error=True, quiet=True)
    else:
        # remote cleanup using structured remote tmp paths
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_cmds = [
            (f"runc kill {scene} KILL", True),
            (f"runc delete {scene}", True),
            (f"sh -c 'for f in {shlex.quote(os.path.join(remote_tmp,'pids', f'recvtty_{scene}_*.pid'))}; do if [ -f \"$f\" ]; then kill -TERM $(cat \"$f\") 2>/dev/null || true; rm -f \"$f\"; fi; done'", True),
        ]
        for c, ign in remote_cmds:
            run_remote_cmd(c, DEST_IP, ignore_error=ign, quiet=True)
        run_remote_cmd("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", DEST_IP, ignore_error=True, quiet=True)
        run_remote_cmd(f"if [ -f {shlex.quote(os.path.join(remote_tmp,'pids', f'destination_{scene}.pid'))} ]; then kill -TERM $(cat {shlex.quote(os.path.join(remote_tmp,'pids', f'destination_{scene}.pid'))}) 2>/dev/null || true; rm -f {shlex.quote(os.path.join(remote_tmp,'pids', f'destination_{scene}.pid'))}; fi", DEST_IP, ignore_error=True, quiet=True)
        # Ensure the container is actually deleted on remote host (robust retries)
        try:
            ok = ensure_deleted_remote(DEST_IP, scene)
            if not ok:
                print(f"[clean] warning: remote container {scene} may still exist on {DEST_IP}")
        except Exception as e:
            print(f"[clean] ensure_deleted_remote failed for {scene} on {DEST_IP}: {e}")


def source_clean_migration(scene: str):
    run_cmd(f"kill -9 $(cat {shlex.quote(os.path.join(TMP_PIDS, 'recvtty_source.pid'))}) 2>/dev/null || true", ignore_error=True, quiet=True)
    unmount_local_migration_tmpfs(scene)
    run_cmd(f"runc kill {scene} KILL", ignore_error=True, quiet=True)
    run_cmd(f"runc delete {scene}", ignore_error=True, quiet=True)
    run_cmd("ps aux | grep 'inotifywait' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)
    run_cmd("ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)


def _signal_handler(signum, frame):
    global KEEP_RUNNING
    print(f"[sig] caught signal {signum}, will stop after current iteration and clean up")
    KEEP_RUNNING = False

# register signals
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def safe_clean_all(quiet: bool = False):
    """Best-effort cleaning using robust runc kill/delete and bundle/pid cleanup."""
    def _log(msg: str):
        if not quiet:
            print(msg)

    _log("[clean] Performing robust clean of defog containers and tmp artifacts...")

    # Ensure network shaping is cleared first (local and remote) so there's no leftover
    # tc configuration after runs. This is best-effort and won't fail the cleanup.
    try:
        clean_configure_network()
    except Exception as e:
        _log(f"[clean] warning: clean_configure_network failed: {e}")

    try:
        res = run_cmd("runc list -q", quiet=True)
        names = [ln.strip() for ln in res.stdout.splitlines()]
    except Exception:
        names = []
    # build candidate set (defog-* plus explicit scene names)
    candidates = set()
    for n in names:
        if n.startswith('defog-') or n in SCENE_INFO:
            candidates.add(n)
    for s in SCENE_INFO.keys():
        candidates.add(s)
    # escalate KILL
    for n in candidates:
        _log(f"[clean] KILL {n}")
        run_cmd(f"runc kill {shlex.quote(n)} KILL", ignore_error=True, quiet=True)
    time.sleep(0.3)
    # delete with retries
    for n in candidates:
        for attempt in range(6):
            run_cmd(f"runc delete {shlex.quote(n)}", ignore_error=True, quiet=True)
            res = run_cmd("runc list -q", quiet=True)
            if n not in res.stdout:
                break
            time.sleep(0.2)

    # Ensure remote hosts also have the containers removed (best-effort)
    try:
        if DEST_IP and DEST_IP not in ('127.0.0.1', 'localhost', SOURCE_IP):
            for n in candidates:
                try:
                    ensure_deleted_remote(DEST_IP, n)
                except Exception as _e:
                    _log(f"[clean] ensure_deleted_remote failed for {n} on {DEST_IP}: {_e}")
    except Exception:
        pass
    try:
        if CLIENT_IP and CLIENT_IP not in ('127.0.0.1', 'localhost', SOURCE_IP):
            for n in candidates:
                try:
                    ensure_deleted_remote(CLIENT_IP, n)
                except Exception as _e:
                    _log(f"[clean] ensure_deleted_remote failed for {n} on {CLIENT_IP}: {_e}")
    except Exception:
        pass
    # remove tmp bundles and recvtty artifacts (include structured TMP_ROOT)
    run_cmd("rm -rf /tmp/fog_bundle.* /tmp/*_start.pid /tmp/*_loop.pid /tmp/recvtty_*.pid /tmp/recvtty-*.log /tmp/recvtty_debug.log", ignore_error=True, quiet=True)
    # remove our structured tmp area if present
    try:
        run_cmd(f"rm -rf {shlex.quote(TMP_ROOT)}", ignore_error=True, quiet=True)
    except Exception:
        pass
    # kill any lingering recvtty processes
    run_cmd("ps aux | grep recvtty | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)
    # additional attempt: pgrep/pkill for recvtty to handle different invocation forms
    run_cmd("pgrep -f recvtty | xargs -r kill -9 || true", ignore_error=True, quiet=True)
    # remove common console/recvtty socket artifacts
    run_cmd("rm -f /tmp/console_* /tmp/*_recvtty* /tmp/recvtty_* /tmp/recvtty-*.log || true", ignore_error=True, quiet=True)
    # attempt to kill stuck runc exec processes which can cause setns errors and block terminals
    run_cmd("ps aux | grep 'runc exec' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true", ignore_error=True, quiet=True)
    run_cmd("pgrep -f 'runc exec' | xargs -r kill -9 || true", ignore_error=True, quiet=True)

    # Clean up stale console socket files under /runc/containers/*/console.sock. These files can remain
    # after a crash and will cause recvtty to report "bind: address already in use" even though
    # there is no active listener. Inspect /proc/net/unix for active Unix-path entries and only
    # unlink the filesystem socket when it is not present there.
    try:
        bound_paths = set()
        try:
            with open('/proc/net/unix', 'r') as f:
                for line in f:
                    parts = line.split()
                    if parts and parts[-1].startswith('/'):
                        bound_paths.add(parts[-1])
        except Exception:
            bound_paths = set()
        import glob, stat
        for sock in glob.glob('/runc/containers/*/console.sock'):
            try:
                st = os.stat(sock)
                if stat.S_ISSOCK(st.st_mode):
                    if sock not in bound_paths:
                        _log(f"[clean] removing stale console socket {sock}")
                        run_cmd(f"rm -f {shlex.quote(sock)}", ignore_error=True, quiet=True)
                    else:
                        _log(f"[clean] leaving active console socket {sock}")
                else:
                    _log(f"[clean] removing non-socket file {sock}")
                    run_cmd(f"rm -f {shlex.quote(sock)}", ignore_error=True, quiet=True)
            except FileNotFoundError:
                pass
            except Exception as e:
                _log(f"[clean] error inspecting socket {sock}: {e}")
    except Exception:
        pass

# Exec failure tracking & asset injection helpers for fog_test
exec_fail_count = {}
exec_blocked = {}

def kill_stale_runc_exec():
    run_cmd("ps aux | grep 'runc exec' | grep -v grep | awk '{print $2}' | xargs -r kill -9 || true", quiet=True, ignore_error=True)
    run_cmd("pgrep -f 'runc exec' | xargs -r kill -9 || true", quiet=True, ignore_error=True)

def maybe_block_exec(container, reason='setns'):
    cnt = exec_fail_count.get(container, 0) + 1
    exec_fail_count[container] = cnt
    if cnt >= 3:
        exec_blocked[container] = time.time() + 60
        kill_stale_runc_exec()
        print(f"[warn] frequent runc exec failures for {container}: blocking exec for 60s (reason={reason})")
        exec_fail_count[container] = 0

def ensure_assets_in_bundle(bundle_path, scene):
    """Ensure canonical assets exist in the bundle's /mnt/assets.

    Handles `gocr` specially by copying all images from /runc/datasets/ocr (supports both flattened and images/ layouts).
    """
    destdir = os.path.join(bundle_path, 'rootfs', 'mnt', 'assets')
    run_cmd(f"mkdir -p {shlex.quote(destdir)}", quiet=True, ignore_error=True)

    # Special-case: place all OCR images into assets (support both /runc/datasets/ocr and /runc/datasets/ocr/images)
    if scene == 'gocr':
        src_dir = '/runc/datasets/ocr'
        cand_dirs = [src_dir, os.path.join(src_dir, 'images')]
        copied = 0
        for d in cand_dirs:
            if os.path.isdir(d):
                for root, _, files in os.walk(d):
                    for fn in files:
                        if fn.lower().endswith(('.png', '.jpg', '.jpeg')):
                            src = os.path.join(root, fn)
                            dst = os.path.join(destdir, fn)
                            run_cmd(f"cp -f {shlex.quote(src)} {shlex.quote(dst)}", quiet=True, ignore_error=True)
                            run_cmd(f"chmod 644 {shlex.quote(dst)}", quiet=True, ignore_error=True)
                            copied += 1
        if copied == 0:
            print(f"[data] no OCR image files found under {src_dir}, skipping gocr asset copy")
        return

    # use original basenames so container-side /mnt/assets checks match SCENE_INFO asset names
    mapping = {
        'pocketsphinx':[('/runc/datasets/audio/sample.wav','psphinx.wav')],
        'aeneas':[('/runc/datasets/audio/sample.mp3','sample.mp3')],
        'yolo':[('/runc/datasets/images/dog.jpg','yoloimage.jpg'),('/runc/datasets/images/dog.jpg','dog.jpg')],
        'video':[('/runc/datasets/images/dog.jpg','dog.jpg')],
        'gzip':[('/runc/datasets/compress/sample.bin','sample.bin')],
    }

    for src, destname in mapping.get(scene, []):
        if os.path.exists(src):
            dst = os.path.join(destdir, destname)
            run_cmd(f"cp -f {shlex.quote(src)} {shlex.quote(dst)}", quiet=True, ignore_error=True)
            run_cmd(f"chmod 644 {shlex.quote(dst)}", quiet=True, ignore_error=True)
        else:
            print(f"[data] missing canonical asset {src} for {scene} - continuing")

def normalize_ocr_dataset(remote_host: str | None = None) -> bool:
    """Flatten /runc/datasets/ocr/images into /runc/datasets/ocr and remove sample.xhtml.

    Runs locally when `remote_host` is None; otherwise runs the equivalent commands via SSH on `remote_host`.
    Returns True on success (best-effort), False on failure.
    """
    base = '/runc/datasets/ocr'
    images_dir = os.path.join(base, 'images')
    # Remote normalization via SSH
    if remote_host and remote_host not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        try:
            cmd = (
                "mkdir -p /runc/datasets/ocr; "
                "if [ -d /runc/datasets/ocr/images ]; then mv -f /runc/datasets/ocr/images/* /runc/datasets/ocr/ || true; rmdir /runc/datasets/ocr/images 2>/dev/null || true; fi; "
                "rm -f /runc/datasets/ocr/sample.xhtml || true; "
                "chmod 644 /runc/datasets/ocr/* 2>/dev/null || true;"
            )
            run_remote_cmd(cmd, remote_host, ignore_error=True, quiet=True)
            print(f"[data] normalized OCR dataset on remote host {remote_host}")
            return True
        except Exception as e:
            print(f"[data] failed to normalize OCR dataset on {remote_host}: {e}")
            return False
    # Local normalization
    try:
        if os.path.isdir(images_dir):
            for fn in os.listdir(images_dir):
                src = os.path.join(images_dir, fn)
                dst = os.path.join(base, fn)
                try:
                    if os.path.exists(dst):
                        os.remove(dst)
                    shutil.move(src, dst)
                except Exception as e:
                    print(f"[data] failed to move {src} -> {dst}: {e}")
            try:
                os.rmdir(images_dir)
            except Exception:
                pass
        sample = os.path.join(base, 'sample.xhtml')
        if os.path.exists(sample):
            try:
                os.remove(sample)
                print(f"[data] removed sample.xhtml in {base}")
            except Exception as e:
                print(f"[data] failed to remove sample.xhtml: {e}")
        # fix permissions for images
        for root, _, files in os.walk(base):
            for f in files:
                if f.lower().endswith(('.png', '.jpg', '.jpeg', '.xhtml')):
                    try:
                        os.chmod(os.path.join(root, f), 0o644)
                    except Exception:
                        pass
        return True
    except Exception as e:
        print(f"[data] failed to normalize OCR dataset locally: {e}")
        return False

# ensure cleanup runs at process exit (but don't auto-run when user requested help)
# If the user passed -h/--help, argparse will print help and exit; avoid running cleanup in that case.
if not any(arg in ('-h', '--help') for arg in sys.argv):
    atexit.register(safe_clean_all)
else:
    # Avoid performing destructive cleaning when only showing help
    print("[clean] Skipping atexit safe_clean_all registration due to help request")


def ensure_deleted(container: str, attempts: int = 6, delay: float = 0.5) -> bool:
    """Wait for a container to disappear, trying kill/delete repeatedly."""
    for i in range(attempts):
        try:
            r = run_cmd("runc list -q", quiet=True)
            names = [ln.strip() for ln in r.stdout.splitlines()]
        except Exception:
            names = []
        if container not in names:
            return True
        run_cmd(f"runc kill {shlex.quote(container)} KILL", ignore_error=True, quiet=True)
        run_cmd(f"runc delete {shlex.quote(container)}", ignore_error=True, quiet=True)
        time.sleep(delay)
    return False


def ensure_deleted_remote(target_ip: str, container: str, attempts: int = 6, delay: float = 0.5) -> bool:
    """Wait for a container to disappear on a remote host, trying kill/delete repeatedly via ssh."""
    for i in range(attempts):
        try:
            r = run_remote_cmd("runc list -q", target_ip, ignore_error=True, quiet=True)
            names = [ln.strip() for ln in (getattr(r, 'stdout', '') or '').splitlines()]
        except Exception:
            names = []
        if container not in names:
            return True
        run_remote_cmd(f"runc kill {shlex.quote(container)} KILL", target_ip, ignore_error=True, quiet=True)
        run_remote_cmd(f"runc delete {shlex.quote(container)}", target_ip, ignore_error=True, quiet=True)
        time.sleep(delay)
    print(f"[clean][warn] failed to delete remote container {container} on {target_ip}")
    return False


# minimal helpers reused from earlier implementation (start_service/wait_for_health/run_smoke/run_bench/collect/create_baseline/cleanup)
# (Implementation follows the robust pattern used previously, but in-file to make fog_test.py self-contained)

import shlex
from datetime import datetime

def restore_baseline(scene: str, bundle: str):
    """Ensure /runc/fog_workloads/<scene> exists from canonical bundle (no .bak usage)."""
    purge_fog_workload_baks()
    dest = os.path.join('/runc/fog_workloads', scene)
    src = os.path.join('/runc/fog_workloads', bundle)
    if os.path.abspath(src) == os.path.abspath(dest):
        if not os.path.isdir(src):
            print(f"[error] canonical bundle missing at {src}; cannot prepare baseline for {scene}")
        else:
            print(f"[baseline] canonical bundle already in place at {src}; skipping copy")
        return
    if not os.path.isdir(src):
        print(f"[error] no canonical bundle at {src}; cannot prepare baseline for {scene}")
        return
    print(f"[baseline] prepared from canonical bundle {src} -> {dest}")
    run_cmd(f"rm -rf {shlex.quote(dest)} || true", quiet=True, ignore_error=True)
    run_cmd(f"cp -r {shlex.quote(src)} {shlex.quote(dest)}", quiet=False)


def verify_no_persistence(bundle_path: str, bundle: str) -> None:
    # Check common persistence indicators for Redis/Influx and warn if persistence appears enabled
    if not bundle_path:
        return
    # Redis check
    rc = os.path.join(bundle_path, 'rootfs', 'etc', 'redis.conf')
    if os.path.exists(rc):
        try:
            txt = open(rc).read()
            if 'save ' in txt and 'save ""' not in txt:
                print(f"[warn] redis snapshotting enabled in {rc}")
            if 'appendonly yes' in txt:
                print(f"[warn] redis appendonly enabled in {rc}")
        except Exception:
            pass
    # Influx check (only warn if bundle appears to contain Influx or mentions influx in args)
    cfg = os.path.join(bundle_path, 'config.json')
    if os.path.exists(cfg):
        try:
            import json as _json
            cfgj = _json.load(open(cfg))
            args = cfgj.get('process', {}).get('args', [])
            joined = ' '.join(args).lower()
            influx_bin_paths = [
                os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'influxdb3'),
                os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'influxdb3'),
                os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'influxd'),
                os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'influxd'),
            ]
            influx_present = any(os.path.exists(p) for p in influx_bin_paths) or 'influx' in joined or 'influxdb' in joined
            if influx_present:
                if '--object-store=memory' in joined or '--object-store=tmpfs' in joined:
                    print(f"[info] Influx object-store appears memory-backed in {cfg}")
                else:
                    print(f"[warn] Influx persistence may be enabled (no --object-store=memory) in {cfg}")
        except Exception:
            pass


def start_service(scene: str, host: str, port: int):
    info = SCENE_INFO.get(scene, {})
    bundle = info.get('bundle', scene)

    if scene == 'elasticsearch':
        ensure_vm_max_map_count()

    # If a service-level bench exists under migration/<scene>/, prefer a service-named bundle
    local_bench_for_scene = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(scene.lower())
    if local_bench_for_scene:
        backend_detected = detect_backend_from_bench(local_bench_for_scene)
        if backend_detected in ('influxdb', 'redis'):
            ensure_service_bundle(scene, backend_detected)
            bundle = scene

    # restore baseline bundle under /runc/fog_workloads
    restore_baseline(scene, bundle)
    bundle_path = os.path.join('/runc/fog_workloads', scene)
    if not os.path.isdir(bundle_path):
        alt = os.path.join('/runc/fog_workloads', bundle)
        if os.path.isdir(alt):
            bundle_path = alt
    if not os.path.isdir(bundle_path):
        raise RuntimeError(f"missing fog_workloads bundle dir: {bundle_path}")

    # Normalize local OCR dataset layout (flatten images/ and remove sample.xhtml) to satisfy gocr/aeneas expectations
    try:
        normalize_ocr_dataset()
    except Exception:
        pass

    ensure_assets_in_bundle(bundle_path, scene)
    # Verify no persistence misconfigurations
    verify_no_persistence(bundle_path, bundle)

    # Patch bundle config to explicitly pass the desired port to the server so
    # failures due to host/port conflicts are easier to detect and avoid using
    # an unexpected port.
    try:
        cfg = os.path.join(bundle_path, 'config.json')
        if os.path.exists(cfg):
            import json as _json
            cfgj = _json.load(open(cfg))
            cfgj.setdefault('process', {})
            # Prefer bundle /root/scripts/execute.sh for server-aware bundles; otherwise try container entrypoint
            exec_sh = os.path.join(bundle_path, 'rootfs', 'root', 'scripts', 'execute.sh')
            entry_candidates = [
                (os.path.join(bundle_path, 'rootfs', 'bin', 'entrypoint.sh'), '/bin/entrypoint.sh'),
                (os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'entrypoint.sh'), '/usr/bin/entrypoint.sh'),
                (os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'docker-entrypoint.sh'), '/usr/local/bin/docker-entrypoint.sh'),
            ]
            if os.path.exists(exec_sh):
                cfgj['process']['args'] = ['sh', '-lc', f"/root/scripts/execute.sh --server --port {int(port)} && exec sleep infinity"]
                # Preserve explicit terminal requests; default to detached mode only when unspecified
                if 'terminal' not in cfgj.get('process', {}):
                    cfgj['process']['terminal'] = False
                with open(cfg, 'w') as _cfh:
                    _json.dump(cfgj, _cfh)
                print(f"[start] patched {cfg} to pass --port {int(port)} to execute.sh")
            else:
                used = None
                for p, container_path in entry_candidates:
                    if os.path.exists(p):
                        used = container_path
                        break
                if used:
                    # If this bundle contains influxdb3, use known Influx flags and correct --http-bind flag
                    influx_bin = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'influxdb3')
                    if os.path.exists(influx_bin):
                        cfgj['process']['args'] = ['sh', '-lc', f"{used} influxdb3 serve --object-store=memory --node-id=node0 --without-auth --wal-flush-interval=20ms --http-bind 0.0.0.0:{int(port)} && exec sleep infinity"]
                        print(f"[start] patched {cfg} to start {used} influxdb3 serve on port {int(port)}")
                    else:
                        # If this appears to be a Redis-derived bundle, start Redis via docker-entrypoint if available
                        redis_bin1 = os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'redis-server')
                        redis_bin2 = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'redis-server')
                        if os.path.exists(redis_bin1) or os.path.exists(redis_bin2):
                            cfgj['process']['args'] = ['sh', '-lc', f"{used} redis-server --bind 0.0.0.0 --port {int(port)} && exec sleep infinity"]
                            print(f"[start] patched {cfg} to start {used} redis-server on port {int(port)}")
                        else:
                            cfgj['process']['args'] = ['sh', '-lc', f"{used} serve --http-bind 0.0.0.0:{int(port)} && exec sleep infinity"]
                            print(f"[start] patched {cfg} to start {used} serve on port {int(port)}")
                    with open(cfg, 'w') as _cfh:
                        _json.dump(cfgj, _cfh)
                else:
                    print(f"[start] no /root/scripts/execute.sh or entrypoint in {bundle_path}; leaving process.args unchanged")
    except Exception:
        pass

    # start per-scene recvtty (migration-style) and run container from /runc/fog_workloads
    recvtty_pid = f"/tmp/recvtty_{scene}.pid"
    recvtty_log = f"/tmp/recvtty_{scene}.log"
    console_opt = ''
    console_sock = os.path.join(bundle_path, 'console.sock')

    if os.path.exists(console_sock):
        try:
            run_cmd(f"PATH=$PATH:/root/go/bin recvtty -m null {shlex.quote(console_sock)} > {shlex.quote(recvtty_log)} 2>&1 & echo $! > {shlex.quote(recvtty_pid)}", quiet=True, ignore_error=True)
            # record that recvtty was started (pidfile may be created asynchronously)
            print(f"[start] recvtty started for {scene} on {console_sock} (pidfile {recvtty_pid})")
            console_opt = f"--console-socket {shlex.quote(console_sock)}"
        except Exception:
            console_opt = ''
    else:
        # If bundle's config requests a terminal, create a temporary console socket under /tmp and start recvtty there
        cfg = os.path.join(bundle_path, 'config.json')
        try:
            if os.path.exists(cfg):
                import json as _json
                cfgj = _json.load(open(cfg))
                if cfgj.get('process', {}).get('terminal', False):
                    temp_console = f"/tmp/recvtty_{scene}.console.sock"
                    run_cmd(f"rm -f {shlex.quote(temp_console)}", quiet=True, ignore_error=True)
                    run_cmd(f"PATH=$PATH:/root/go/bin recvtty -m null {shlex.quote(temp_console)} > {shlex.quote(recvtty_log)} 2>&1 & echo $! > {shlex.quote(recvtty_pid)}", quiet=True, ignore_error=True)
                    # If the socket was created, use it; otherwise patch the bundle to remove terminal requirement
                    if os.path.exists(temp_console):
                        print(f"[start] temporary recvtty socket created {temp_console} (pidfile {recvtty_pid})")
                        console_opt = f"--console-socket {shlex.quote(temp_console)}"
                    else:
                        try:
                            bak_cfg = cfg + ".orig"
                            if not os.path.exists(bak_cfg):
                                run_cmd(f"cp -f {shlex.quote(cfg)} {shlex.quote(bak_cfg)}", quiet=True, ignore_error=True)
                            cfgj['process']['terminal'] = False
                            with open(cfg, 'w') as _cfh:
                                import json as _json2
                                _json2.dump(cfgj, _cfh)
                            print(f"[start] patched {cfg} to set process.terminal=false to allow detached runc run")
                        except Exception:
                            pass
        except Exception:
            pass

    # ensure no conflicting container: stop/delete and wait for disappearance to avoid race "container with given ID already exists"
    run_cmd(f"runc kill {scene} KILL", ignore_error=True, quiet=True)
    run_cmd(f"runc delete {scene}", ignore_error=True, quiet=True)
    try:
        ensure_deleted(scene)
    except Exception:
        pass
    # Attempt to start the container; if the desired port is in use, try a few subsequent ports
    cur_port = int(port)
    attempt = 0
    max_attempts = 5
    started_ok = False
    last_out = ''
    while attempt < max_attempts:
        # If the desired port is already bound on the host (e.g., another service), skip it
        try:
            import socket as _socket
            _s = _socket.socket()
            try:
                _s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
                _s.bind(('127.0.0.1', cur_port))
                _s.close()
            except Exception:
                print(f"[start][warn] port {cur_port} appears in use on host; trying next port")
                attempt += 1
                cur_port += 1
                continue
        except Exception:
            # If socket check fails for unexpected reasons, continue with attempt and rely on runc/run error detection
            pass

        # patch config for the attempted port
        try:
            cfg = os.path.join(bundle_path, 'config.json')
            import json as _json
            if os.path.exists(cfg):
                cfgj = _json.load(open(cfg))
                cfgj.setdefault('process', {})
                # Prefer bundle /root/scripts/execute.sh for server-aware bundles; otherwise try container entrypoint
                exec_sh = os.path.join(bundle_path, 'rootfs', 'root', 'scripts', 'execute.sh')
                entry_candidates = [
                    (os.path.join(bundle_path, 'rootfs', 'bin', 'entrypoint.sh'), '/bin/entrypoint.sh'),
                    (os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'entrypoint.sh'), '/usr/bin/entrypoint.sh'),
                ]
                if os.path.exists(exec_sh):
                    cfgj['process']['args'] = ['sh', '-lc', f"/root/scripts/execute.sh --server --port {cur_port} && exec sleep infinity"]
                    with open(cfg, 'w') as _cfh:
                        _json.dump(cfgj, _cfh)
                    print(f"[start] patched {cfg} to pass --port {cur_port} to execute.sh")
                else:
                    used = None
                    for p, container_path in entry_candidates:
                        if os.path.exists(p):
                            used = container_path
                            break
                    if used:
                        influx_bin = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'influxdb3')
                        if os.path.exists(influx_bin):
                            cfgj['process']['args'] = ['sh', '-lc', f"{used} influxdb3 serve --object-store=memory --node-id=node0 --without-auth --wal-flush-interval=20ms --http-bind 0.0.0.0:{cur_port} && exec sleep infinity"]
                            print(f"[start] patched {cfg} to start {used} influxdb3 serve on port {cur_port}")
                        else:
                            redis_bin1 = os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'redis-server')
                            redis_bin2 = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'redis-server')
                            if os.path.exists(redis_bin1) or os.path.exists(redis_bin2):
                                cfgj['process']['args'] = ['sh', '-lc', f"{used} redis-server --bind 0.0.0.0 --port {cur_port} && exec sleep infinity"]
                                print(f"[start] patched {cfg} to start {used} redis-server on port {cur_port}")
                            else:
                                cfgj['process']['args'] = ['sh', '-lc', f"{used} serve --http-bind 0.0.0.0:{cur_port} && exec sleep infinity"]
                                print(f"[start] patched {cfg} to start {used} serve on port {cur_port}")
                        with open(cfg, 'w') as _cfh:
                            _json.dump(cfgj, _cfh)
                    else:
                        # fallback: if we didn't find an entrypoint but the bundle contains redis-server, use docker-entrypoint directly
                        redis_bin1 = os.path.join(bundle_path, 'rootfs', 'usr', 'local', 'bin', 'redis-server')
                        redis_bin2 = os.path.join(bundle_path, 'rootfs', 'usr', 'bin', 'redis-server')
                        if os.path.exists(redis_bin1) or os.path.exists(redis_bin2):
                            cfgj['process']['args'] = ['sh', '-lc', f"/usr/local/bin/docker-entrypoint.sh redis-server --bind 0.0.0.0 --port {cur_port} && exec sleep infinity"]
                            with open(cfg, 'w') as _cfh:
                                _json.dump(cfgj, _cfh)
                            print(f"[start] patched {cfg} to start /usr/local/bin/docker-entrypoint.sh redis-server on port {cur_port}")
                        else:
                            print(f"[start] no /root/scripts/execute.sh or entrypoint in {bundle_path}; leaving process.args unchanged")
        except Exception:
            pass
        print(f"[start] runc run -b {bundle_path} -> {scene} (port {cur_port})")
        cmd = f"runc run {console_opt} -d -b {shlex.quote(bundle_path)} {shlex.quote(scene)}"
        res = run_cmd(cmd, quiet=False, ignore_error=True, timeout=15)
        # Normalize stdout/stderr which may be bytes depending on run_cmd implementation
        so = getattr(res, 'stdout', '') or ''
        se = getattr(res, 'stderr', '') or ''
        try:
            if isinstance(so, bytes):
                so = so.decode('utf-8', errors='ignore')
            if isinstance(se, bytes):
                se = se.decode('utf-8', errors='ignore')
        except Exception:
            pass
        out = f"{so}\n{se}"

        started_ok = getattr(res, 'returncode', 1) == 0
        if not started_ok:
            # If bind errors, try the next port
            if 'Address already in use' in out:
                print(f"[start][warn] port {cur_port} appears in use; cleaning and trying next port")
                try:
                    run_cmd(f"runc delete {shlex.quote(scene)}", ignore_error=True, quiet=True)
                except Exception:
                    pass
                attempt += 1
                cur_port += 1
                last_out = out
                continue
            # If container exists despite non-zero rc, consider it started (best-effort)
            try:
                rr = run_cmd("runc list -q", quiet=True, ignore_error=True, timeout=5)
                names = [ln.strip() for ln in rr.stdout.splitlines()]
            except Exception:
                names = []
            if scene in names:
                print(f"[start] note: container {scene} exists after runc run (treating as started)")
                started_ok = True
                break
            # otherwise record last output and try once more
            last_out = out
            attempt += 1
            cur_port += 1
        else:
            break

    if not started_ok:
        try:
            run_cmd(f"runc delete {shlex.quote(scene)}", ignore_error=True, quiet=True)
        except Exception:
            pass
        raise RuntimeError(f"failed to start container {scene}: last output: {last_out[:400]}")

    container = scene
    bundle_path = bundle_path
    print(f"[start] container={container} bundle={bundle_path} port={cur_port}")
    return container, bundle_path, cur_port


def wait_for_health(container: str, host: str, port: int, endpoint: str = '/health', attempts: int = 20, delay: float = 1.0):
    """Wait for service to be healthy.

    Supports HTTP /health checks and Redis PING checks for Redis-backed services by
    detecting a local migration bench and its backend.
    """
    scene_info = SCENE_INFO.get(container, {})
    backend_hint = scene_info.get('backend')
    # Heuristic backend detection from local bench (if any)
    local_bench = find_local_bench_for_bundle(container) or find_local_bench_for_bundle(container.lower())
    backend = backend_hint or (detect_backend_from_bench(local_bench) if local_bench else None)
    if endpoint == 'redis':
        backend = 'redis'

    for _ in range(attempts):
        # If this appears to be a Redis-backed service, try a Redis PING first
        if backend == 'redis':
            try:
                # prefer a container-side Python socket ping (most bundles will have python)
                blocked_until = exec_blocked.get(container, 0)
                if time.time() < blocked_until:
                    time.sleep(delay)
                    continue
                cmd = (
                    f"runc exec {container} python3 - <<'PY'\n"
                    "import socket\n"
                    f"s=socket.socket(); s.settimeout(2); s.connect(('127.0.0.1',{int(port)})); s.send(b'*1\\r\\n$4\\r\\nPING\\r\\n'); print(s.recv(1024).decode())\n"
                    "PY"
                )
                r = run_cmd(cmd, quiet=True, ignore_error=True, timeout=3)
                out = (getattr(r, 'stdout', '') or '')
                if 'PONG' in out.upper():
                    exec_fail_count[container] = 0
                    return True
                # fallback: redis-cli ping
                r2 = run_cmd(f"runc exec {container} redis-cli -h 127.0.0.1 -p {int(port)} ping", quiet=True, ignore_error=True, timeout=2)
                if getattr(r2, 'returncode', 1) == 0 and 'PONG' in (getattr(r2, 'stdout', '') or ''):
                    exec_fail_count[container] = 0
                    return True
            except Exception:
                pass

            # As last resort try a basic TCP connect from host
            try:
                import socket as _socket
                s = _socket.create_connection((host, int(port)), timeout=1)
                s.close()
                return True
            except Exception:
                pass

        # If the scene looks like a file-upload endpoint (we have an asset), try a POST health probe
        try:
            scene_asset = SCENE_INFO.get(container, {}).get('asset')
            if scene_asset and endpoint != '/health':
                # handle Aeneas (audio,text) specially
                if ',' in scene_asset:
                    try:
                        a_part, t_part = scene_asset.split(',')
                        post_cmd = f"curl -sf -F audio=@{shlex.quote(a_part)} -F text=@{shlex.quote(t_part)} http://{host}:{int(port)}{endpoint}"
                        rpost = run_cmd(post_cmd, quiet=True, ignore_error=True, timeout=15)
                        if getattr(rpost, 'returncode', 1) == 0:
                            return True
                        else:
                            out = getattr(rpost, 'stdout', '') or ''
                            err = getattr(rpost, 'stderr', '') or ''
                            print(f"[health] host POST failed for {container} rc={getattr(rpost,'returncode',None)} stdout={str(out)[:200]} stderr={str(err)[:200]}")
                    except Exception as e:
                        print(f"[health] host POST exception for {container}: {e}")
                    try:
                        # list assets inside the container for diagnostics
                        try:
                            ls = run_cmd(f"runc exec {container} ls -la /mnt/assets", quiet=True, ignore_error=True, timeout=5)
                            print(f"[health] /mnt/assets contents: {(getattr(ls,'stdout','') or '')[:400]}")
                        except Exception:
                            pass
                        a_cand = f"/mnt/assets/{os.path.basename(a_part)}"
                        t_cand = f"/mnt/assets/{os.path.basename(t_part)}"
                        r = run_cmd(f"runc exec {container} curl -sf -F audio=@{a_cand} -F text=@{t_cand} http://127.0.0.1:{int(port)}{endpoint}", quiet=True, timeout=15, ignore_error=True)
                        if getattr(r, 'returncode', 1) == 0:
                            exec_fail_count[container] = 0
                            return True
                        else:
                            out = getattr(r, 'stdout', '') or ''
                            err = getattr(r, 'stderr', '') or ''
                            print(f"[health] container POST failed for {container} rc={getattr(r,'returncode',None)} stdout={str(out)[:200]} stderr={str(err)[:200]}")                            # Diagnostic: try Flask test_client inside container to validate WSGI handler directly
                            try:
                                tc_code = f"""import importlib.util
spec=importlib.util.spec_from_file_location('mod','/root/scripts/{container}_server.py')
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
C=mod.app.test_client()
with open('/mnt/assets/{os.path.basename(a_part)}','rb') as a, open('/mnt/assets/{os.path.basename(t_part)}','rb') as t:
    data = {{'audio': (a, '{os.path.basename(a_part)}'), 'text': (t, '{os.path.basename(t_part)}')}}
    r = C.post('{endpoint}', data=data, content_type='multipart/form-data')
    print('TC_STATUS', r.status_code)
"""
                                tr = run_cmd(f"runc exec {container} python3 - <<'PY'\n{tc_code}\nPY", quiet=True, ignore_error=True, timeout=8)
                                tout = (getattr(tr,'stdout','') or '') + (getattr(tr,'stderr','') or '')
                                print(f"[health] test_client check output for {container}: {tout[:400]}")
                                if 'TC_STATUS 200' in tout or 'TC_STATUS 201' in tout:
                                    print(f"[health] container {container} WSGI OK via test_client but network POST failing; attempting restart and re-check")
                                    # Diagnostic: capture listening sockets, processes and recent logs to help root-cause bind/404
                                    try:
                                        s1 = run_cmd(f"runc exec {container} ss -ltnp", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] ss -ltnp: {(getattr(s1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] ss failed: {_e_d}")
                                    try:
                                        p1 = run_cmd(f"runc exec {container} ps aux", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] ps aux: {(getattr(p1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] ps failed: {_e_d}")
                                    try:
                                        l1 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_server.log", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_server.log: {(getattr(l1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_server.log read failed: {_e_d}")
                                    try:
                                        l2 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_debug.log", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_debug.log: {(getattr(l2,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_debug.log read failed: {_e_d}")
                                    try:
                                        l3 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_server.start", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_server.start: {(getattr(l3,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_server.start read failed: {_e_d}")
                                    try:
                                        bundle_path = os.path.join('/runc/fog_workloads', container)
                                        stop_and_clean(container, bundle_path, container)
                                        time.sleep(0.5)
                                        ncont, nb, np = start_service(container, host, port)
                                        time.sleep(1.0)
                                        r2 = run_cmd(f"runc exec {ncont} curl -sf -F audio=@{a_cand} -F text=@{t_cand} http://127.0.0.1:{int(np)}{endpoint}", quiet=True, timeout=8, ignore_error=True)
                                        if getattr(r2, 'returncode', 1) == 0:
                                            exec_fail_count[ncont] = 0
                                            return True
                                        else:
                                            print(f"[health] container POST still failing after restart rc={getattr(r2,'returncode',None)} stdout={(getattr(r2,'stdout','') or '')[:200]} stderr={(getattr(r2,'stderr','') or '')[:200]}")
                                    except Exception as e2:
                                        print(f"[health] restart attempt failed: {e2}")
                            except Exception as e3:
                                print(f"[health] test_client diagnostic failed: {e3}")
                    except Exception as e:
                        print(f"[health] container POST exception for {container}: {e}")
                else:
                    # try host-side POST with host asset first
                    try:
                        post_cmd = f"curl -sf -F file=@{shlex.quote(scene_asset)} http://{host}:{int(port)}{endpoint}"
                        rpost = run_cmd(post_cmd, quiet=True, ignore_error=True, timeout=3)
                        if getattr(rpost, 'returncode', 1) == 0:
                            return True
                        else:
                            out = getattr(rpost, 'stdout', '') or ''
                            err = getattr(rpost, 'stderr', '') or ''
                            print(f"[health] host POST failed for {container} rc={getattr(rpost,'returncode',None)} stdout={str(out)[:200]} stderr={str(err)[:200]}")
                    except Exception as e:
                        print(f"[health] host POST exception for {container}: {e}")
                    # try container-side POST using /mnt/assets/<basename>
                    try:
                        bname = os.path.basename(scene_asset)
                        c_asset = f"/mnt/assets/{bname}"
                        r = run_cmd(f"runc exec {container} curl -sf -F file=@{c_asset} http://127.0.0.1:{int(port)}{endpoint}", quiet=True, timeout=3, ignore_error=True)
                        if getattr(r, 'returncode', 1) == 0:
                            exec_fail_count[container] = 0
                            return True
                        else:
                            out = getattr(r, 'stdout', '') or ''
                            err = getattr(r, 'stderr', '') or ''
                            print(f"[health] container POST failed for {container} rc={getattr(r,'returncode',None)} stdout={str(out)[:200]} stderr={str(err)[:200]}")
                            # Diagnostic: try Flask test_client inside container to validate WSGI handler directly
                            try:
                                tc_code = f"""import importlib.util
spec=importlib.util.spec_from_file_location('mod','/root/scripts/{container}_server.py')
mod=importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
C=mod.app.test_client()
with open('/mnt/assets/{bname}','rb') as f:
    data = {{'file': (f, '{bname}')}}
    r = C.post('{endpoint}', data=data, content_type='multipart/form-data')
    print('TC_STATUS', r.status_code)
"""
                                tr = run_cmd(f"runc exec {container} python3 - <<'PY'\n{tc_code}\nPY", quiet=True, ignore_error=True, timeout=8)
                                tout = (getattr(tr,'stdout','') or '') + (getattr(tr,'stderr','') or '')
                                print(f"[health] test_client check output for {container}: {tout[:400]}")
                                if 'TC_STATUS 200' in tout or 'TC_STATUS 201' in tout:
                                    print(f"[health] container {container} WSGI OK via test_client but network POST failing; attempting restart and re-check")
                                    # Diagnostic: capture listening sockets, processes and recent logs to help root-cause bind/404
                                    try:
                                        s1 = run_cmd(f"runc exec {container} ss -ltnp", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] ss -ltnp: {(getattr(s1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] ss failed: {_e_d}")
                                    try:
                                        p1 = run_cmd(f"runc exec {container} ps aux", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] ps aux: {(getattr(p1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] ps failed: {_e_d}")
                                    try:
                                        l1 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_server.log", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_server.log: {(getattr(l1,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_server.log read failed: {_e_d}")
                                    try:
                                        l2 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_debug.log", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_debug.log: {(getattr(l2,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_debug.log read failed: {_e_d}")
                                    try:
                                        l3 = run_cmd(f"runc exec {container} head -n 50 /tmp/gocr_server.start", quiet=True, ignore_error=True, timeout=3)
                                        print(f"[health-diagn] /tmp/gocr_server.start: {(getattr(l3,'stdout','') or '')[:400]}")
                                    except Exception as _e_d:
                                        print(f"[health-diagn] /tmp/gocr_server.start read failed: {_e_d}")
                                    try:
                                        bundle_path = os.path.join('/runc/fog_workloads', container)
                                        stop_and_clean(container, bundle_path, container)
                                        time.sleep(0.5)
                                        ncont, nb, np = start_service(container, host, port)
                                        time.sleep(1.0)
                                        r2 = run_cmd(f"runc exec {ncont} curl -sf -F file=@{c_asset} http://127.0.0.1:{int(np)}{endpoint}", quiet=True, timeout=8, ignore_error=True)
                                        if getattr(r2, 'returncode', 1) == 0:
                                            exec_fail_count[ncont] = 0
                                            return True
                                        else:
                                            print(f"[health] container POST still failing after restart rc={getattr(r2,'returncode',None)} stdout={(getattr(r2,'stdout','') or '')[:200]} stderr={(getattr(r2,'stderr','') or '')[:200]}")
                                    except Exception as e2:
                                        print(f"[health] restart attempt failed: {e2}")
                            except Exception as e3:
                                print(f"[health] test_client diagnostic failed: {e3}")
                    except Exception as e:
                        print(f"[health] container POST exception for {container}: {e}")
        except Exception:
            pass

        # Generic HTTP /health check (host-first, then container exec)
        try:
            r2 = run_cmd(f"curl -sf http://{host}:{int(port)}{endpoint}", quiet=True, timeout=3, ignore_error=True)
            if getattr(r2, 'returncode', 1) == 0:
                return True
        except Exception:
            pass

        try:
            blocked_until = exec_blocked.get(container, 0)
            if time.time() < blocked_until:
                time.sleep(delay)
                continue
            r = run_cmd(f"runc exec {container} curl -sf http://127.0.0.1:{int(port)}{endpoint}", quiet=True, timeout=3, ignore_error=True)
            if getattr(r, 'returncode', 1) == 0:
                exec_fail_count[container] = 0
                return True
            err = getattr(r, 'stderr', '') or ''
            if 'setns' in err or 'failed to open /proc' in err or 'No such file or directory' in err:
                maybe_block_exec(container, reason='setns')
                return False
        except Exception:
            pass

        time.sleep(delay)
    return False


def is_container_running(container: str) -> bool:
    try:
        r = run_cmd(f"runc state {shlex.quote(container)}", ignore_error=True, quiet=True, timeout=3)
        if getattr(r, 'returncode', 1) != 0:
            return False
        try:
            import json as _json

            st = _json.loads(getattr(r, 'stdout', '') or '{}')
            return st.get('status') == 'running'
        except Exception:
            return '"status": "running"' in ((getattr(r, 'stdout', '') or '') + (getattr(r, 'stderr', '') or ''))
    except Exception:
        return False


def restart_container_for_migration(scene: str, port: int):
    run_cmd(f"runc kill {shlex.quote(scene)} KILL", ignore_error=True, quiet=True)
    run_cmd(f"runc delete {shlex.quote(scene)}", ignore_error=True, quiet=True)
    try:
        ensure_deleted(scene)
    except Exception:
        pass
    start_container_for_migration(scene, port)


def ensure_service_ready(scene: str, port: int, endpoint: str, max_wait_seconds: int = 20) -> bool:
    wait_secs = max_wait_seconds or 20
    wait_secs = min(wait_secs, 20)
    wait_secs = max(wait_secs, 1)

    def basic_http_ready() -> bool:
        url = f"http://{SOURCE_IP}:{int(port)}{endpoint}"
        r = run_cmd(f"curl -sf --max-time 2 {shlex.quote(url)}", quiet=True, ignore_error=True, timeout=3)
        return getattr(r, 'returncode', 1) == 0

    def poll_basic_http(label: str) -> bool:
        deadline = time.time() + wait_secs
        while time.time() < deadline:
            # Prefer host-level HTTP check first; if service is reachable, treat as ready even if runc state is inconsistent
            if basic_http_ready():
                return True
            if not is_container_running(scene):
                print(f"[health] {scene} not running during {label}; restarting")
                restart_container_for_migration(scene, port)
                # give container a moment to come up before re-checking
                time.sleep(1.0)
                continue
            time.sleep(1.0)
        return False

    # For HTTP health endpoints, prefer a simple reachability loop honoring the 20s budget
    if endpoint in ('/health', '/_cluster/health'):
        if poll_basic_http("initial health loop"):
            return True
        print(f"[health] {scene} not ready after {wait_secs}s; restarting container")
        restart_container_for_migration(scene, port)
        if poll_basic_http("post-restart health loop"):
            return True
        print(f"[health] {scene} still not ready after restart; aborting migration run")
        try:
            run_cmd(f"runc state {shlex.quote(scene)}", ignore_error=True, quiet=False)
        except Exception:
            pass
        return False

    # For non-HTTP-health endpoints, reuse broader health probes
    if not is_container_running(scene):
        print(f"[health] {scene} not running; restarting before health check")
        restart_container_for_migration(scene, port)

    ok = wait_for_health(scene, SOURCE_IP, port, endpoint=endpoint, attempts=wait_secs, delay=1.0)
    if ok:
        return True

    print(f"[health] {scene} not ready after {wait_secs}s; restarting container")
    restart_container_for_migration(scene, port)
    ok = wait_for_health(scene, SOURCE_IP, port, endpoint=endpoint, attempts=wait_secs, delay=1.0)
    if not ok:
        print(f"[health] {scene} still not ready after restart; aborting migration run")
        try:
            run_cmd(f"runc state {shlex.quote(scene)}", ignore_error=True, quiet=False)
        except Exception:
            pass
        return False
    return True


def run_smoke(container: str, scene: str, port: int):
    info = SCENE_INFO[scene]
    endpoint = info['endpoint']
    asset = info['asset']
    url_host = f"http://{DEFAULT_HOST}:{int(port)}{endpoint}"
    print(f"[smoke] starting run_smoke scene={scene} endpoint={endpoint} asset={asset} port={port}")
    # Prefer container-side request for file-upload scenes (asset provided and not a /health check)
    try:
        if asset and endpoint != '/health':
            print(f"[smoke] preferring container-side request for asset={asset} endpoint={endpoint}")
            container_asset = f"/mnt/assets/{os.path.basename(asset)}"
            if scene == 'gzip':
                cmd = f"runc exec {container} /bin/sh -c \"curl -sS --data-binary @{container_asset} -H 'Content-Type: application/octet-stream' http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
            elif scene == 'aeneas' and ',' in (asset or ''):
                a_part, t_part = asset.split(',')
                a_cand = f"/mnt/assets/{os.path.basename(a_part)}"
                t_cand = f"/mnt/assets/{os.path.basename(t_part)}"
                cmd = f"runc exec {container} /bin/sh -c \"curl -sS -F audio=@{a_cand} -F text=@{t_cand} http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
            else:
                cmd = f"runc exec {container} /bin/sh -c \"curl -sS -F file=@{container_asset} http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
            r = run_cmd(cmd, quiet=True, ignore_error=True, timeout=20)
            if getattr(r, 'returncode', 1) == 0:
                try:
                    run_cmd(f"runc exec {container} cat /tmp/service_response.json", quiet=True, timeout=5)
                except Exception:
                    pass
                print(f"[smoke] container-side direct request succeeded for {scene}")
                return True
        # Try host-side next (prefer host network to avoid runc exec)
        if scene == 'gzip':
            rc = run_cmd(f"curl -sS --data-binary @{shlex.quote(asset)} -H 'Content-Type: application/octet-stream' -w '%{{http_code}}' -o /dev/null {shlex.quote(url_host)}", quiet=True, ignore_error=True, timeout=15)
            if rc.returncode == 0:
                return True
        elif scene == 'aeneas':
            parts = asset.split(',')
            cmd = f"curl -sS -F audio=@{shlex.quote(parts[0])} -F text=@{shlex.quote(parts[1])} -w '%{{http_code}}' -o /dev/null {shlex.quote(url_host)}"
            rc = run_cmd(cmd, quiet=True, ignore_error=True, timeout=30)
            if rc.returncode == 0:
                return True
        elif endpoint == '/health':
            rc = run_cmd(f"curl -sS {shlex.quote(url_host)} -o /dev/null -w '%{{http_code}}'", quiet=True, ignore_error=True, timeout=5)
            if rc.returncode == 0:
                return True
        else:
            rc = run_cmd(f"curl -sS -F file=@{shlex.quote(asset)} -w '%{{http_code}}' -o /dev/null {shlex.quote(url_host)}", quiet=True, ignore_error=True, timeout=15)
            if rc.returncode == 0:
                return True
    except Exception:
        pass
    # Fallback: attempt to perform request inside container (prefer /mnt/assets/<file> which ensure_assets_in_bundle copies into bundle)
    container_asset = None
    if asset:
        container_asset = f"/mnt/assets/{os.path.basename(asset)}"
    if scene == 'gzip':
        use_asset = container_asset if container_asset else asset
        cmd = f"runc exec {container} /bin/sh -c \"curl -sS --data-binary @{use_asset} -H 'Content-Type: application/octet-stream' http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
    elif scene == 'aeneas':
        if asset and ',' in asset:
            a_part, t_part = asset.split(',')
            a_candidate = f"/mnt/assets/{os.path.basename(a_part)}"
            t_candidate = f"/mnt/assets/{os.path.basename(t_part)}"
            audio = a_candidate
            text = t_candidate
        else:
            audio = container_asset if container_asset else asset
            text = container_asset if container_asset else asset
        cmd = f"runc exec {container} /bin/sh -c \"curl -sS -F audio=@{audio} -F text=@{text} http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
    elif endpoint == '/health':
        cmd = f"runc exec {container} /bin/sh -c \"curl -sS http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
    else:
        use_asset = container_asset if container_asset else asset
        cmd = f"runc exec {container} /bin/sh -c \"curl -sS -F file=@{use_asset} http://127.0.0.1:{int(port)}{endpoint} -o /tmp/service_response.json -w '%{{http_code}}'\""
    r = run_cmd(cmd, quiet=True, ignore_error=True, timeout=20)
    if r.returncode != 0:
        err = getattr(r, 'stderr', '') or ''
        out = getattr(r, 'stdout', '') or ''
        print(f"[smoke] container-side first attempt failed rc={getattr(r,'returncode',None)} stdout={out[:200]} stderr={err[:200]}")
        if 'setns' in err or 'failed to open /proc' in err:
            maybe_block_exec(container, reason='setns')
        # Try alternate container-local address if container bound to a non-loopback IP
        try:
            rr = run_cmd(f"runc exec {container} /bin/sh -c \"hostname -I | awk '{{print $1}}'\"", quiet=True, ignore_error=True, timeout=3)
            ip = (getattr(rr, 'stdout', '') or '').strip().split()[0] if getattr(rr, 'stdout', '') else ''
            if ip:
                # attempt the request against container's primary IP
                alt_cmd = cmd.replace('127.0.0.1', ip)
                r2 = run_cmd(alt_cmd, quiet=True, ignore_error=True, timeout=20)
                if getattr(r2, 'returncode', 1) == 0:
                    try:
                        run_cmd(f"runc exec {container} cat /tmp/service_response.json", quiet=True, timeout=5)
                    except Exception:
                        pass
                    print(f"[smoke] container-side fallback (ip {ip}) succeeded for {scene}")
                    return True
                else:
                    print(f"[smoke] container-side second attempt failed rc={getattr(r2,'returncode',None)} stdout={(getattr(r2,'stdout','') or '')[:200]} stderr={(getattr(r2,'stderr','') or '')[:200]}")
        except Exception as e:
            print(f"[smoke] container-side ip fallback error: {e}")

        print('[smoke] request failed (container-side fallback)')
        return False
    try:
        run_cmd(f"runc exec {container} cat /tmp/service_response.json", quiet=True, timeout=5)
    except Exception:
        pass
    return True


def find_local_bench_for_bundle(bundle: str):
    # Search the migration tree for a canonical `bench.py` first, then fall back to `bench_*.py`.
    # Finally, accept any file with 'bench' in its name (e.g., client_bench.py).
    roots = [
        os.path.join('/runc/dirty-track/experiment/migration', bundle),
        os.path.join('/runc/dirty-track/experiment/migration', bundle.lower()),
    ]
    for root in roots:
        if not os.path.isdir(root):
            continue
        # Prefer canonical bench.py
        for r, _, files in os.walk(root):
            if 'bench.py' in files:
                return os.path.join(r, 'bench.py')
        # Fallback to bench_*.py
        for r, _, files in os.walk(root):
            for f in files:
                if f.startswith('bench_') and f.endswith('.py'):
                    return os.path.join(r, f)
        # Broad fallback: any python file that contains 'bench' in its name (client_bench.py etc.)
        for r, _, files in os.walk(root):
            for f in files:
                if 'bench' in f.lower() and f.endswith('.py'):
                    return os.path.join(r, f)
    return None


def detect_backend_from_bench(bench_path: str) -> str:
    """Heuristically detect backend type from the bench script contents."""
    try:
        with open(bench_path, 'r', encoding='utf-8', errors='ignore') as fh:
            txt = fh.read(8192).lower()
    except Exception:
        return 'unknown'
    if '--influx-url' in txt or 'influxdb' in txt or 'influxdbclient' in txt:
        return 'influxdb'
    if '--redis-host' in txt or 'import redis' in txt or 'redis.' in txt:
        return 'redis'
    if 'elasticsearch' in txt or '--es-host' in txt:
        return 'elasticsearch'
    # Detect JMeter-based benches (iPokeMon) which use a JMX file / jmeter binary
    if '--jmx' in txt or 'jmeter' in txt:
        return 'jmeter'
    if '--url' in txt or 'requests.' in txt or 'server' in txt:
        return 'http'
    return 'unknown'


def ensure_service_bundle(service_name: str, backend: str) -> None:
    """Ensure a fog_workloads bundle exists for `service_name` using canonical bundles only (no .bak)."""
    fw_root = os.path.join('/runc', 'fog_workloads')
    purge_fog_workload_baks()
    desired_work = os.path.join(fw_root, f"{service_name}")
    base_work = os.path.join(fw_root, f"{backend}")

    if os.path.isdir(desired_work):
        return

    if os.path.isdir(base_work):
        print(f"[fog_test] creating service bundle {service_name} from {base_work}")
        run_cmd(f"cp -r {shlex.quote(base_work)} {shlex.quote(desired_work)}", quiet=False)
        return
    print(f"[warn] cannot find base bundle for {backend} to create {service_name}")


# If SCENE_INFO[scene]['asset'] exists, fog_test passes these to local benches as
# `--files` (comma-separated basenames) or `--pairs` (audio:text pairs for aeneas).
def run_bench(scene: str, port: int):
    py = shlex.quote(sys.executable)
    info = SCENE_INFO.get(scene, {})
    bench = info.get('bench')
    bundle = info.get('bundle', scene)

    # Prefer service-specific benches under migration/<scene>/ first, then fallback to bundle-level benches
    local_bench = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(bundle)

    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    work_dir = ensure_workload_dir(scene)
    outf = os.path.join(work_dir, f"{scene}_smoke_{ts}.csv")
    metrics_out = os.path.join(work_dir, f"{scene}_smoke_{ts}_metrics.json")
    metrics_args = f" --metrics-out {shlex.quote(metrics_out)} --metrics-interval 1.0"

    if local_bench:
        backend = detect_backend_from_bench(local_bench)
        files_arg = ''
        # If caller requested specific files to use, construct appropriate args; otherwise let the bench use its defaults
        if BENCH_FILES:
            if scene == 'aeneas' and ',' in BENCH_FILES:
                parts = [p.strip() for p in BENCH_FILES.split(',') if p.strip()]
                pairs = []
                for i in range(0, len(parts)-1, 2):
                    a = parts[i]
                    t = parts[i+1]
                    pairs.append(f"{os.path.basename(a)}:{os.path.basename(t)}")
                if pairs:
                    files_arg = f" --pairs {shlex.quote(','.join(pairs))}"
            else:
                parts = [p.strip() for p in BENCH_FILES.split(',') if p.strip()]
                if len(parts) == 1:
                    if local_bench and _bench_supports_flag(local_bench, '--file'):
                        files_arg = f" --file {shlex.quote(os.path.basename(parts[0]))}"
                    elif local_bench and _bench_supports_flag(local_bench, '--files'):
                        files_arg = f" --files {shlex.quote(os.path.basename(parts[0]))}"
                    else:
                        files_arg = f" --file {shlex.quote(os.path.basename(parts[0]))}"
                else:
                    basenames = ','.join([os.path.basename(p) for p in parts])
                    files_arg = f" --files {shlex.quote(basenames)}"

        dataset_arg = ''
        if BENCH_DATASET and local_bench and _bench_supports_flag(local_bench, '--dataset'):
            dataset_arg = f" --dataset {shlex.quote(BENCH_DATASET)}"

        if backend == 'redis':
            cmd = f"{py} {shlex.quote(local_bench)} --redis-host 127.0.0.1 --redis-port {int(port)} --threads 4 --duration 30{dataset_arg}"
        elif backend == 'influxdb':
            cmd = f"{py} {shlex.quote(local_bench)} --influx-url http://127.0.0.1:{int(port)} --threads 4 --duration 30{dataset_arg}"
        elif backend == 'elasticsearch':
            cmd = f"{py} {shlex.quote(local_bench)} --es-host 127.0.0.1 --es-port {int(port)} --threads 4 --rps 100 --duration 30"
        elif backend == 'http':
            url = f"http://127.0.0.1:{int(port)}{info.get('endpoint','/')}"
            cmd = f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)}{dataset_arg}{files_arg}"
        else:
            # Legacy fallback based on path
            if os.path.sep + 'redis' + os.path.sep in local_bench:
                cmd = f"{py} {shlex.quote(local_bench)} --redis-host 127.0.0.1 --redis-port {int(port)} --threads 4 --duration 30 --dataset /runc/datasets"
            elif os.path.sep + 'influxdb' + os.path.sep in local_bench:
                cmd = f"{py} {shlex.quote(local_bench)} --influx-url http://127.0.0.1:{int(port)} --threads 4 --duration 30 --dataset /runc/datasets"
            elif 'elasticsearch' in local_bench:
                cmd = f"{py} {shlex.quote(local_bench)} --es-host 127.0.0.1 --es-port {int(port)} --threads 4 --rps 100 --duration 30"
            else:
                url = f"http://127.0.0.1:{int(port)}{info.get('endpoint','/')}"
                cmd = f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)} --dataset /runc/datasets{files_arg}"
        if backend in ('http', 'jmeter'):
            cmd_full = f"{cmd} --out {shlex.quote(outf)}{metrics_args}"
        else:
            cmd_full = f"{cmd}{metrics_args}"
        print(f"[bench] running local migration bench: {cmd_full}")
        run_cmd(cmd_full, quiet=False)
    elif bench:
        cmd = bench.replace('PORT', str(port))
        if cmd.startswith('python3 '):
            cmd = cmd.replace('python3', py, 1)
        elif cmd.startswith('python '):
            cmd = cmd.replace('python', py, 1)
        cmd_full = f"{cmd} --out {shlex.quote(outf)}"
        print(f"[bench] running configured bench: {cmd_full}")
        run_cmd(cmd_full, quiet=False)
    else:
        return None

    if os.path.exists(outf):
        # If metrics JSON was requested, validate it shows at least one successful op
        if os.path.exists(metrics_out):
            try:
                _mj = json.load(open(metrics_out))
                succ = _mj.get('overall', {}).get('success_ops', 0)
                if succ <= 0:
                    print(f"[bench] metrics {metrics_out} reports success_ops={succ}; failing bench")
                    # attempt to surface container diagnostics when available
                    try:
                        if 'gocr' == scene:
                            # show a few server logs to help triage common gocr issues
                            l1 = run_cmd(f"runc exec {scene} head -n 50 /tmp/gocr_server.log", quiet=True, ignore_error=True, timeout=3)
                            print(f"[health-diagn] /tmp/gocr_server.log: {(getattr(l1,'stdout','') or '')[:400]}")
                    except Exception:
                        pass
                    return None
            except Exception as e:
                print(f"[bench] failed to parse metrics {metrics_out}: {e}")
                # Fall through to CSV parsing below
        # Fallback: inspect CSV for successful status codes (200-399)
        try:
            import csv as _csv
            succ = 0
            with open(outf, 'r', encoding='utf-8', errors='ignore') as _fh:
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
                print(f"[bench] CSV {outf} indicates 0 successful requests; failing bench")
                return None
        except Exception as _e:
            print(f"[bench] failed to parse CSV {outf}: {_e}")
        return outf
    return None


def _derive_dataset_for_scene(scene: str, info: dict) -> str | None:
    """Best-effort derive a per-scene dataset directory.

    Examples:
      /runc/datasets/ocr/images/0001.png -> /runc/datasets/ocr
      /runc/datasets/compress/sample.bin -> /runc/datasets/compress
    Returns None when no sensible dataset dir can be determined.
    """
    try:
        asset = info.get('asset')
        if not asset:
            return None
        parts = [p.strip() for p in str(asset).split(',') if p.strip()]
        abs_paths = [os.path.abspath(p) for p in parts if p]
        base = os.path.abspath('/runc/datasets')
        try:
            common = os.path.commonpath(abs_paths)
        except Exception:
            common = abs_paths[0] if abs_paths else None
        if common and common.startswith(base):
            rel = os.path.relpath(common, base)
            first = rel.split(os.sep)[0] if rel and rel != '.' else ''
            if first:
                return os.path.join(base, first)
            return base
        for p in abs_paths:
            if p.startswith(base):
                rel = os.path.relpath(p, base)
                first = rel.split(os.sep)[0]
                return os.path.join(base, first)
        return os.path.dirname(abs_paths[0]) if abs_paths else None
    except Exception:
        return None


def _bench_supports_flag(local_bench: str | None, flag: str) -> bool:
    """Heuristic: return True if `local_bench` script appears to accept `flag` (e.g. '--file')."""
    if not local_bench or not os.path.exists(local_bench):
        return False
    try:
        txt = open(local_bench, 'r', encoding='utf-8', errors='ignore').read(8192)
        import re as _re
        if _re.search(r"add_argument\(\s*['\"]" + _re.escape(flag) + r"['\"]", txt):
            return True
        if flag in txt:
            return True
    except Exception:
        return False
    return False


def build_bench_command(scene: str, port: int, host: Optional[str] = None, duration: int = 600, threads: int = 2, out_path: Optional[str] = None, metrics_out: Optional[str] = None, metrics_interval: float = 1.0, backend: Optional[str] = None, bench_dataset: Optional[str] = None, bench_files: Optional[str] = None) -> Optional[str]:
    py = shlex.quote(sys.executable)
    host = host or SOURCE_IP or DEFAULT_HOST
    info = SCENE_INFO.get(scene, {})
    bundle = info.get('bundle', scene)
    endpoint = info.get('endpoint', '/')
    bench = info.get('bench')
    local_bench = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(bundle)
    out_path = out_path or f"/tmp/{scene}_bench_{int(time.time())}.csv"
    metrics_args = ""
    if metrics_out:
        metrics_args = f" --metrics-out {shlex.quote(metrics_out)} --metrics-interval {float(metrics_interval)}"

    if backend is None:
        backend = info.get('backend')
    if backend is None and local_bench:
        backend = detect_backend_from_bench(local_bench)

    if local_bench:
        if backend == 'redis':
            # Do not force a dataset by default; allow bench to use its own defaults.
            dataset_arg = ''
            if bench_dataset and local_bench and _bench_supports_flag(local_bench, '--dataset'):
                dataset_arg = f" --dataset {shlex.quote(bench_dataset)}"
            return f"{py} {shlex.quote(local_bench)} --redis-host {host} --redis-port {int(port)} --threads {int(threads)} --duration {int(duration)}{dataset_arg}{metrics_args}"
        if backend == 'influxdb':
            dataset_arg = ''
            if bench_dataset and local_bench and _bench_supports_flag(local_bench, '--dataset'):
                dataset_arg = f" --dataset {shlex.quote(bench_dataset)}"
            return f"{py} {shlex.quote(local_bench)} --influx-url http://{host}:{int(port)} --threads {int(threads)} --duration {int(duration)}{dataset_arg}{metrics_args}"
        if backend == 'elasticsearch':
            return f"{py} {shlex.quote(local_bench)} --es-host {host} --es-port {int(port)} --threads {int(threads)} --rps 100 --duration {int(duration)}{metrics_args}"
        if backend == 'http':
            url = f"http://{host}:{int(port)}{endpoint}"
            files_args = ''
            dataset_arg = ''
            # Only pass dataset if explicitly requested by the caller
            if bench_dataset and local_bench and _bench_supports_flag(local_bench, '--dataset'):
                dataset_arg = f" --dataset {shlex.quote(bench_dataset)}"
            asset = info.get('asset')
            # Only pass explicit asset file(s) when the caller requested them via bench_files
            if bench_files:
                if scene == 'aeneas' and ',' in bench_files:
                    parts = [p.strip() for p in bench_files.split(',') if p.strip()]
                    pairs = []
                    for i in range(0, len(parts)-1, 2):
                        a = parts[i]
                        t = parts[i+1]
                        pairs.append(f"{os.path.basename(a)}:{os.path.basename(t)}")
                    if pairs:
                        files_args = f" --pairs {shlex.quote(','.join(pairs))}"
                else:
                    parts = [p.strip() for p in bench_files.split(',') if p.strip()]
                    if len(parts) == 1:
                        # Prefer --file when available (most benches accept it); fall back to --files
                        if local_bench and _bench_supports_flag(local_bench, '--file'):
                            files_args = f" --file {shlex.quote(os.path.basename(parts[0]))}"
                        elif local_bench and _bench_supports_flag(local_bench, '--files'):
                            files_args = f" --files {shlex.quote(os.path.basename(parts[0]))}"
                        else:
                            files_args = f" --file {shlex.quote(os.path.basename(parts[0]))}"
                    else:
                        basenames = ','.join([os.path.basename(p) for p in parts])
                        files_args = f" --files {shlex.quote(basenames)}"
            return f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)} --duration {int(duration)} --threads {int(threads)}{dataset_arg} --out {shlex.quote(out_path)}{files_args}{metrics_args}"
        if backend == 'jmeter':
            return f"{py} {shlex.quote(local_bench)} --host {host} --port {int(port)} --duration {int(duration)} --threads {int(threads)} --out {shlex.quote(out_path)}{metrics_args}"
    elif bench:
        cmd = bench.replace('PORT', str(port)).replace('127.0.0.1', host)
        if cmd.startswith('python3 '):
            cmd = cmd.replace('python3', py, 1)
        elif cmd.startswith('python '):
            cmd = cmd.replace('python', py, 1)
        cmd = f"{cmd} --duration {int(duration)} --threads {int(threads)}"
        return f"{cmd} --out {shlex.quote(out_path)}{metrics_args}"
    return None


def start_bench_background(scene: str, port: int, host: Optional[str] = None, duration: int = 600, threads: int = 2, remote: bool = False, exp_name: str = 'pre-copy', run_index: int = 1, bench_dataset: Optional[str] = None, bench_files: Optional[str] = None) -> Optional[str]:
    info = SCENE_INFO.get(scene, {})
    bundle = info.get('bundle', scene)
    local_bench = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(bundle)
    backend = info.get('backend')
    if backend is None and local_bench:
        backend = detect_backend_from_bench(local_bench)
    raw_out, metrics_out = build_bench_output_paths(scene, exp_name, run_index, backend=backend)
    # If remote bench and backend is jmeter: check remote machine has jmeter; if not, fallback to running locally
    if remote and backend == 'jmeter':
        try:
            which_res = run_remote_cmd('which jmeter || true', CLIENT_IP, ignore_error=True, quiet=True)
            if not (getattr(which_res, 'stdout', '') or '').strip():
                print(f"[bench] remote jmeter not found on {CLIENT_IP}; running jmeter locally instead")
                remote = False
        except Exception:
            print(f"[bench] failed to detect jmeter on remote {CLIENT_IP}; running locally")
            remote = False

    # If running remotely, prefer to write outputs into the structured remote tmp area
    if remote:
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_workdir = os.path.join(remote_tmp, 'workloads', scene)
        remote_raw_out = os.path.join(remote_workdir, os.path.basename(raw_out))
        remote_metrics_out = os.path.join(remote_workdir, os.path.basename(metrics_out))
        # Normalize remote OCR dataset if needed (gocr)
        if scene == 'gocr':
            try:
                normalize_ocr_dataset(remote_host=CLIENT_IP)
            except Exception as e:
                print(f"[data] failed to normalize OCR dataset on remote {CLIENT_IP}: {e}")
        cmd = build_bench_command(
            scene,
            port,
            host=host,
            duration=duration,
            threads=threads,
            out_path=remote_raw_out,
            metrics_out=remote_metrics_out,
            metrics_interval=1.0,
            backend=backend,
            bench_dataset=bench_dataset,
            bench_files=bench_files,
        )
    else:
        # Ensure local workload output directory exists
        try:
            os.makedirs(os.path.dirname(raw_out), exist_ok=True)
            os.makedirs(os.path.dirname(metrics_out), exist_ok=True)
        except Exception:
            pass
        cmd = build_bench_command(
            scene,
            port,
            host=host,
            duration=duration,
            threads=threads,
            out_path=raw_out,
            metrics_out=metrics_out,
            metrics_interval=1.0,
            backend=backend,
            bench_dataset=bench_dataset,
            bench_files=bench_files,
        )

    if not cmd:
        print(f"[bench] no bench command for {scene}, skipping background load")
        return None

    # local paths
    pidfile = os.path.join(TMP_PIDS, f"bench_{scene}.pid")
    log = os.path.join(TMP_BENCH_LOGS, f"bench_{scene}.log")

    # If running remotely, prefer invoking 'python3' on the remote host instead of
    # using the local sys.executable path which may not exist on the remote system.
    if remote:
        cmd = cmd.replace(shlex.quote(sys.executable), 'python3')
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_log = os.path.join(remote_tmp, 'logs', 'bench', f"bench_{scene}.log")
        remote_pidfile = os.path.join(remote_tmp, 'pids', f"bench_{scene}.pid")
        # Ensure remote workload dir exists so benches can write metrics to it
        # remote_raw_out/remote_metrics_out were passed into build_bench_command earlier
        # create parent directories for log/pid and the workload outputs
        try:
            # compute families
            remote_out_dir = os.path.join(remote_tmp, 'workloads', scene)
            mkdirs = [os.path.dirname(remote_log), os.path.dirname(remote_pidfile), remote_out_dir]
            mkdirs_cmd = ' '.join(shlex.quote(d) for d in mkdirs)
        except Exception:
            mkdirs_cmd = f"{shlex.quote(os.path.dirname(remote_log))} {shlex.quote(os.path.dirname(remote_pidfile))}"
        full = f"mkdir -p {mkdirs_cmd} ; nohup {cmd} > {shlex.quote(remote_log)} 2>&1 & echo $! > {shlex.quote(remote_pidfile)}"
        run_remote_cmd(full, CLIENT_IP, ignore_error=True)
        return remote_pidfile
    else:
        full = f"nohup {cmd} > {shlex.quote(log)} 2>&1 & echo $! > {shlex.quote(pidfile)}"
        try:
            os.makedirs(os.path.dirname(log), exist_ok=True)
            os.makedirs(os.path.dirname(pidfile), exist_ok=True)
        except Exception:
            pass
        run_cmd(full, ignore_error=True)
        return pidfile


def stop_bench_background(scene: str, remote: bool = False):
    pidfile = os.path.join(TMP_PIDS, f"bench_{scene}.pid")
    stop_cmd = (
        f"if [ -f {shlex.quote(pidfile)} ]; then PID=$(cat {shlex.quote(pidfile)}); "
        "if [ -n \"$PID\" ]; then kill $PID 2>/dev/null || true; "
        "for i in 1 2 3 4 5 6 7 8 9 10; do if ! kill -0 $PID 2>/dev/null; then break; fi; sleep 0.2; done; "
        "if kill -0 $PID 2>/dev/null; then kill -9 $PID 2>/dev/null || true; fi; "
        "fi; "
        f"rm -f {shlex.quote(pidfile)}; fi"
    )
    if remote:
        remote_tmp = f"/tmp/fog_test/{RUN_LABEL}"
        remote_pidfile = os.path.join(remote_tmp, 'pids', f"bench_{scene}.pid")
        remote_stop_cmd = (
            f"if [ -f {shlex.quote(remote_pidfile)} ]; then PID=$(cat {shlex.quote(remote_pidfile)}); "
            "if [ -n \"$PID\" ]; then kill $PID 2>/dev/null || true; "
            "for i in 1 2 3 4 5 6 7 8 9 10; do if ! kill -0 $PID 2>/dev/null; then break; fi; sleep 0.2; done; "
            "if kill -0 $PID 2>/dev/null; then kill -9 $PID 2>/dev/null || true; fi; "
            "fi; "
            f"rm -f {shlex.quote(remote_pidfile)}; fi"
        )
        run_remote_cmd(remote_stop_cmd, CLIENT_IP, ignore_error=True)
    else:
        run_cmd(stop_cmd, ignore_error=True)


def run_migration_once(scene: str, port: int, exp_args: str, exp_name: str, run_index: int, bench_duration: int = 600, bench_threads: int = 2, apply_network: bool = True, bench_host: Optional[str] = None, bench_dataset: Optional[str] = None, bench_files: Optional[str] = None):
    print(f"[mig] preparing migration for scene={scene} run={run_index} exp={exp_name}")
    destination_prepare_migration(scene, port)
    source_prepare_migration(scene, port)

    endpoint = SCENE_INFO.get(scene, {}).get('endpoint', '/health')
    if not ensure_service_ready(scene, port, endpoint, max_wait_seconds=20):
        source_clean_migration(scene)
        destination_clean_migration(scene)
        if apply_network:
            clean_configure_network()
        return

    bench_remote = bool(CLIENT_IP and CLIENT_IP != SOURCE_IP)
    # Resolve bench host: prefer explicit bench_host; for remote clients, default to VIP so
    # load generators target the virtual IP (required for VIP transfer testing).
    resolved_bench_host = bench_host if bench_host is not None else (VIP if bench_remote else SOURCE_IP)
    print(f"[bench] resolved bench host: {resolved_bench_host}")

    # If the bench targets the VIP, ensure the VIP is present on the source before starting the bench.
    if resolved_bench_host == VIP:
        if not ensure_vip_on_source():
            raise RuntimeError("VIP not present on source and could not be restored; aborting run")

    pidfile = start_bench_background(
        scene,
        port,
        host=resolved_bench_host,
        duration=bench_duration,
        threads=bench_threads,
        remote=bench_remote,
        exp_name=exp_name,
        run_index=run_index,
        bench_dataset=bench_dataset,
        bench_files=bench_files,
    )

    if apply_network:
        try:
            configure_network(bandwidth=BANDWIDTH)
        except Exception as e:
            print(f"[net] configure_network error: {e}")

    # Wait a randomized ramp-up time (bench runs 20-30s before migration starts)
    try:
        delay_secs = random.uniform(12.0, 16.0)
        print(f"[mig] waiting {delay_secs:.1f}s before starting migration to let bench ramp up")
        time.sleep(delay_secs)
    except Exception:
        # Fallback minimal wait
        time.sleep(10)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_script_path = os.path.join(script_dir, SOURCE_SCRIPT)
    is_local_run = SOURCE_IP in LOCAL_HOSTS and DEST_IP in LOCAL_HOSTS

    # Determine whether we should include the --shell-job flag based on the destination bundle's config
    try:
        needs_console = False
        cfg_path = f"/runc/containers/{scene}/config.json"
        if DEST_IP in (None, '127.0.0.1', 'localhost', SOURCE_IP):
            try:
                cfg = json.load(open(cfg_path))
                needs_console = bool(cfg.get('process', {}).get('terminal', False))
            except Exception:
                needs_console = False
        else:
            probe_cmd = (
                "python3 - <<'PY'\n"
                "import json\n"
                f"cfg='{cfg_path}'\n"
                "try:\n"
                "    data=json.load(open(cfg))\n"
                "    print(bool(data.get('process', {}).get('terminal', False)))\n"
                "except Exception:\n"
                "    print(False)\n"
                "PY"
            )
            try:
                res = run_remote_cmd(probe_cmd, DEST_IP, ignore_error=True, quiet=True)
                out = (getattr(res, 'stdout', '') or '').strip().lower()
                needs_console = out.startswith('true')
            except Exception:
                needs_console = False
    except Exception:
        needs_console = False

    # Remove the --shell-job flag when the bundle doesn't require a console
    if not needs_console:
        parts = shlex.split(exp_args)
        parts = [p for p in parts if p != '--shell-job']
        exp_args = ' '.join(parts)

    # Pass BANDWIDTH to the source script only for local-loopback runs (used for migration-local simulations).
    cmd_list = [sys.executable, source_script_path]
    if is_local_run:
        cmd_list += ["--bandwidth", BANDWIDTH]
    cmd_list += shlex.split(exp_args)
    if SCENE_INFO.get(scene, {}).get('persistent'):
        cmd_list.append('--file-locks')
    cmd_list += [scene, DEST_IP]
    print("[mig] Running migration:", " ".join(cmd_list))

    # Ensure cleanup always runs even if the migration run raises/returns early
    run_result_payload = {
        "header": None,
        "stats": None,
        "params": [],
        "metrics": None,
        "stdout": "",
    }

    try:
        try:
            result = run_cmd(cmd_list, quiet=True, cwd=script_dir)
            stdout = getattr(result, 'stdout', '') or ''
        except Exception as exc:
            print(f"[mig] migration command failed: {exc}")
            # If run_cmd raised SystemExit inside, this will be handled by outer exception flow
            stdout = getattr(exc, 'stdout', '') or ''
            result = None

        if stdout:
            print(stdout.rstrip())

        header = None
        stats = None
        params = []
        metrics_dict = None
        try:
            header, stats, params = extract_stats_from_output(stdout)
            if header and stats:
                keys = header.split("\t")
                vals = stats.split("\t")
                metrics_dict = {k: v for k, v in zip(keys, vals)}
            if stats:
                params_summary = f"exp: {exp_args} | scene={scene}"
                append_result(
                    exp_name,
                    scene,
                    run_index,
                    stats,
                    header,
                    params_summary,
                    is_secure=SEC_MODE,
                    extra_param_lines=params,
                    first_in_run=(run_index == 1),
                    results_dir=get_run_dir(),
                )
                print(f"[mig] wrote stats for {exp_name} run {run_index}")
        except Exception as e:
            print(f"[mig] failed to write stats: {e}")

        # Populate run_result_payload for return
        run_result_payload["header"] = header
        run_result_payload["stats"] = stats
        run_result_payload["params"] = params
        run_result_payload["metrics"] = metrics_dict
        run_result_payload["stdout"] = stdout

    finally:
        # Always attempt to stop bench, clean source/destination and reset network
        try:
            stop_bench_background(scene, remote=bench_remote)
        except Exception as e:
            print(f"[clean] stop_bench_background failed: {e}")
        # Attempt to fetch remote bench metrics so results are available locally
        try:
            if bench_remote:
                fetched = fetch_remote_workload_metrics(scene, exp_name, run_index, remote_host=CLIENT_IP)
                if fetched:
                    run_result_payload.setdefault('fetched_bench_files', []).extend(fetched)
                else:
                    run_result_payload.setdefault('warnings', []).append('bench_metrics_not_fetched')
        except Exception as e:
            print(f"[bench] fetch_remote_workload_metrics failed: {e}")
        try:
            source_clean_migration(scene)
        except Exception as e:
            print(f"[clean] source_clean_migration failed: {e}")
        try:
            destination_clean_migration(scene)
        except Exception as e:
            print(f"[clean] destination_clean_migration failed: {e}")
        if apply_network:
            try:
                clean_configure_network()
            except Exception as e:
                print(f"[clean] clean_configure_network failed: {e}")

    return run_result_payload


def run_migration_local_once(scene: str, port: int, exp_args: str, exp_name: str, run_index: int, bench_duration: int = 600, bench_threads: int = 2, apply_network: bool = False):
    global SOURCE_IP, DEST_IP, CLIENT_IP

    prev_source, prev_dest, prev_client = SOURCE_IP, DEST_IP, CLIENT_IP
    SOURCE_IP = DEST_IP = CLIENT_IP = DEFAULT_HOST
    try:
        return run_migration_once(
            scene,
            port,
            exp_args,
            exp_name,
            run_index,
            bench_duration=bench_duration,
            bench_threads=bench_threads,
            apply_network=apply_network,
            bench_host=DEFAULT_HOST,
            bench_dataset=BENCH_DATASET,
            bench_files=BENCH_FILES,
        )
    finally:
        SOURCE_IP, DEST_IP, CLIENT_IP = prev_source, prev_dest, prev_client


def parse_size_bytes(spec: str) -> int:
    if not spec:
        return 0
    s = str(spec).strip().lower()
    try:
        if s.endswith('kb'):
            return int(float(s[:-2]) * 1024)
        if s.endswith('mb'):
            return int(float(s[:-2]) * 1024 * 1024)
        if s.endswith('gb'):
            return int(float(s[:-2]) * 1024 * 1024 * 1024)
        return int(float(s))
    except Exception:
        return 0


def simulate_local_migration(scene: str, bandwidth: str, iterations: int = 3, base_dump: str = '64MB', lazy_pages: str = '128MB', simulate_transfer: bool = True):
    bw_bps = parse_bandwidth(bandwidth)
    base_bytes = parse_size_bytes(base_dump)
    lazy_bytes = parse_size_bytes(lazy_pages)
    if bw_bps <= 0:
        print(f"[local-mig] invalid bandwidth spec {bandwidth}; skipping sleep simulation")
    print(f"[local-mig] scene={scene} bandwidth={bandwidth} iterations={iterations} base_dump={base_dump} lazy_pages={lazy_pages}")
    for i in range(iterations):
        size = base_bytes * (i + 1) if base_bytes > 0 else (1 + i) * 1024 * 1024
        transfer_time = size / bw_bps if bw_bps > 0 else 0
        print(f"[local-mig] pre-dump iter {i+1}/{iterations}: size={size/1024/1024:.2f}MB, est_time={transfer_time:.2f}s")
        if simulate_transfer and transfer_time > 0:
            time.sleep(min(transfer_time, 60))

    dump_time = base_bytes / bw_bps if bw_bps > 0 else 0
    print(f"[local-mig] final dump size={base_bytes/1024/1024:.2f}MB est_time={dump_time:.2f}s")
    if simulate_transfer and dump_time > 0:
        time.sleep(min(dump_time, 60))

    if lazy_bytes > 0:
        lazy_time = lazy_bytes / bw_bps if bw_bps > 0 else 0
        print(f"[local-mig] lazy-pages transfer size={lazy_bytes/1024/1024:.2f}MB est_time={lazy_time:.2f}s (simulating remote page transfer over bandwidth)")
        if simulate_transfer and lazy_time > 0:
            time.sleep(min(lazy_time, 60))


def collect_results(bundle_path: Optional[str], scene: str, run_idx: int):
    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    outdir = os.path.join(get_run_dir(), f"{scene}_{ts}_run{run_idx}")
    os.makedirs(outdir, exist_ok=True)
    if bundle_path and os.path.isdir(bundle_path):
        run_cmd(f"cp -r {shlex.quote(os.path.join(bundle_path, 'results'))} {shlex.quote(outdir)}", quiet=True, ignore_error=True)
        ci = os.path.join(bundle_path, '.container_info')
        if os.path.exists(ci):
            run_cmd(f"cp {shlex.quote(ci)} {shlex.quote(outdir)}", quiet=True, ignore_error=True)
    # try to locate arrresult
    for root, _, files in os.walk(outdir):
        if 'arrresult.txt' in files:
            return os.path.join(root, 'arrresult.txt')
    return None


def create_baseline(bundle_path: Optional[str], scene: str):
    if not bundle_path or not os.path.isdir(bundle_path):
        print(f"[baseline] no bundle at {bundle_path}")
        return
    dest = os.path.join('/runc/fog_workloads', scene)
    purge_fog_workload_baks()
    if os.path.abspath(bundle_path) == os.path.abspath(dest):
        print(f"[baseline] bundle_path already canonical ({bundle_path}), skip copy")
        return
    run_cmd(f"rm -rf {shlex.quote(dest)} || true", quiet=True, ignore_error=True)
    run_cmd(f"cp -r {shlex.quote(bundle_path)} {shlex.quote(dest)}", quiet=False)


def stop_and_clean(container: Optional[str], bundle_path: Optional[str], scene: str):
    # Attempt to stop and remove the container robustly, then clean tmp artifacts
    if container:
        run_cmd(f"runc kill {shlex.quote(container)} KILL", ignore_error=True, quiet=True)
        run_cmd(f"runc delete {shlex.quote(container)}", ignore_error=True, quiet=True)
        ensure_deleted(container)
    # if a tmp bundle exists, try to stop its recvtty and remove it
    if bundle_path and bundle_path.startswith('/tmp/'):
        try:
            pidf = os.path.join(bundle_path, 'recvtty.pid')
            if os.path.exists(pidf):
                with open(pidf) as f:
                    pid = f.read().strip()
                run_cmd(f"kill -9 {shlex.quote(pid)}", ignore_error=True, quiet=True)
        except Exception:
            pass
        run_cmd(f"rm -rf {shlex.quote(bundle_path)}", ignore_error=True, quiet=True)
    # per-scene recvtty pid if present
    run_cmd(f"kill -9 $(cat /tmp/recvtty_{scene}.pid) 2>/dev/null || true", ignore_error=True, quiet=True)
    run_cmd(f"rm -f /tmp/{scene}_start.pid /tmp/{scene}_loop.pid /tmp/recvtty_{scene}.pid /tmp/defog_{scene}_container.info", ignore_error=True, quiet=True)
    # final hedges
    run_cmd("rm -rf /tmp/fog_bundle.* /tmp/recvtty-*.log", ignore_error=True, quiet=True)
    run_cmd("ps aux | grep recvtty | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)


def run_scene(scene: str, start_port: int, collect_baseline: bool, keep_containers: bool, local: bool = False, bandwidth: str = '50mbit', simulate_transfer: bool = False):
    global KEEP_RUNNING
    # If user didn't explicitly override start_port (default 8080), prefer per-scene default_port when available
    info = SCENE_INFO.get(scene, {})
    if start_port != 8080:
        port = start_port
    else:
        port = info.get('default_port', start_port)
    print(f"[run] running scene {scene} on port {port}")
    container = None
    bundle_path = None

    try:
        container, bundle_path, port = start_service(scene, DEFAULT_HOST, port)
        ok = wait_for_health(container, DEFAULT_HOST, port)
        if not ok:
            print(f"[run] {scene} health failed, aborting scene")
            return False
        smoke_ok = run_smoke(container, scene, port)
        if not smoke_ok:
            print(f"[run] {scene} smoke failed")
        bench = run_bench(scene, port)
        arr = collect_results(bundle_path, scene, 0)

        # Local-mode simulation: pre-dump/dump/transfer/restore (simulated)
        if local:
            try:
                print(f"[local] simulating pre-dump/dump/transfer/restore for {scene}")
                dump_path = f"/tmp/{scene}_dump.bin"
                size = 0
                if bundle_path and os.path.isdir(bundle_path):
                    for root, _, files in os.walk(bundle_path):
                        for f in files:
                            try:
                                size += os.path.getsize(os.path.join(root, f))
                            except Exception:
                                pass
                if size <= 0:
                    size = 1 * 1024 * 1024  # 1MB default
                with open(dump_path, 'wb') as df:
                    df.truncate(size)
                print(f"[local] created simulated dump {dump_path} ({size} bytes)")
                bw_bps = parse_bandwidth(bandwidth)
                if bw_bps > 0:
                    transfer_time = size / bw_bps
                    if simulate_transfer:
                        print(f"[local] simulating transfer: sleeping {transfer_time:.2f}s")
                        time.sleep(min(transfer_time, 60))
                        print("[local] simulated transfer complete")
                    else:
                        print(f"[local] estimated transfer time: {transfer_time:.2f}s (use --simulate-transfer to actually sleep)")
                else:
                    print(f"[local] invalid bandwidth spec: {bandwidth}")
                restore_marker = f"/tmp/{scene}_restored_{int(time.time())}"
                open(restore_marker, 'w').close()
                print(f"[local] simulated restore marker: {restore_marker}")
            except Exception as e:
                print(f"[local] error during local simulation: {e}")

        if collect_baseline:
            create_baseline(bundle_path, scene)
        return True
    finally:
        if not keep_containers:
            stop_and_clean(container, bundle_path, scene)


def main():
    global SOURCE_IP, DEST_IP, CLIENT_IP, SOURCE_SCRIPT, DEST_SCRIPT, SEC_MODE, BANDWIDTH
    global KEEP_RUNNING

    parser = argparse.ArgumentParser()
    parser.add_argument('--scenes', default='all')
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--start-port', type=int, default=8080)
    parser.add_argument('--clean-first', action='store_true')
    parser.add_argument('--collect-baseline', action='store_true')
    parser.add_argument('--keep-containers', action='store_true')
    parser.add_argument('--local', action='store_true', help='Run in local simulation mode (simulate pre-dump/transfer/restore)')
    parser.add_argument('--bandwidth', default='50mbit', help='Bandwidth to simulate in local mode or network shaping (e.g., 50mbit)')
    parser.add_argument('--simulate-transfer', action='store_true', help='If set, actually sleep to simulate transfer times')
    parser.add_argument('--mode', choices=['smoke', 'migration', 'migration-local'], default='smoke', help='smoke: existing bench/start; migration: run source/destination live-migration; migration-local: run live-migration locally without ssh')
    parser.add_argument('--experiment-types', default='pre-copy', help='Comma-separated experiment types (pre-copy, pre-copy-dirtymap, post-copy, hybrid, hybrid-dirtymap) for migration mode')
    parser.add_argument('--sec', action='store_true', help='Use secure source/destination scripts (source-sec.py/destination-sec.py)')
    parser.add_argument('--source-ip', default=SOURCE_IP)
    parser.add_argument('--dest-ip', default=DEST_IP)
    parser.add_argument('--client-ip', default=CLIENT_IP)
    parser.add_argument('--bench-host', default=None, help='Override bench target host (default SOURCE_IP)')
    parser.add_argument('--bench-dataset', default=None, help='Optional dataset path to pass to benches via --dataset (default: do not pass, let bench choose)')
    parser.add_argument('--bench-files', default=None, help='Optional comma-separated file(s) to pass to benches via --file/--files or --pairs for aeneas (default: do not pass)')
    parser.add_argument('--bench-duration', type=int, default=600, help='Bench duration for migration mode background load')
    parser.add_argument('--bench-threads', type=int, default=2, help='Bench threads/concurrency for migration mode background load')
    parser.add_argument('--skip-network-shaping', action='store_true', help='Skip tc shaping during migration mode')
    parser.add_argument('--stop-on-error', action='store_true', help='Stop the full test run on first error (default: continue)')
    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    BANDWIDTH = args.bandwidth
    SEC_MODE = bool(args.sec)
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(SEC_MODE)
    # apply optional bench overrides
    global BENCH_DATASET, BENCH_FILES
    BENCH_DATASET = getattr(args, 'bench_dataset', None)
    BENCH_FILES = getattr(args, 'bench_files', None)
    ensure_dirs()

    # Disallow loopback addresses only for remote migration runs; smoke & migration-local allow loopback.
    if args.mode == 'migration':
        loopbacks = ('127.0.0.1', 'localhost', '::1')
        if SOURCE_IP in loopbacks or DEST_IP in loopbacks or CLIENT_IP in loopbacks:
            raise SystemExit("Loopback addresses are disallowed in 'migration' mode; please provide real host IPs (or use 'smoke' / 'migration-local').")

    if args.clean_first:
        safe_clean_all(quiet=True)

    scenes = []
    if args.scenes == 'all':
        scenes = list(SCENE_INFO.keys())
    else:
        for s in args.scenes.split(','):
            s = s.strip()
            if s and s in SCENE_INFO:
                scenes.append(s)

    port = args.start_port

    # Migration mode (live-migration aligned with redis/influx flows)
    if args.mode == 'migration':
        exp_list = []
        if args.experiment_types:
            exp_list = [e.strip() for e in args.experiment_types.split(',') if e.strip()]
        if not exp_list:
            exp_list = ['pre-copy']
        results = []
        for exp_name in exp_list:
            exp_args = EXPERIMENTS.get(exp_name)
            if not exp_args:
                print(f"[mig] unknown experiment type {exp_name}, skipping")
                continue

            # Ensure VIP is present on source at the start of this experiment
            try:
                if not ensure_vip_on_source():
                    print(f"[vip] failed to ensure VIP on source before experiment {exp_name}; aborting further experiments")
                    KEEP_RUNNING = False
                    break
            except Exception as e:
                print(f"[vip] ensure_vip_on_source error at experiment start: {e}")
                KEEP_RUNNING = False
                break
            for scene in scenes:
                scene_info = SCENE_INFO.get(scene, {})
                scene_metrics = []
                for r in range(args.runs):
                    if not KEEP_RUNNING:
                        break
                    run_port = port if args.start_port != 8080 else scene_info.get('default_port', port)
                    print(f"--- MIGRATION {scene} (run {r+1}) exp={exp_name} ---")
                    safe_clean_all(quiet=True)
                    clean_configure_network()

                    # Pre-run: ensure no leftover bench/container on remote target or local source.
                    bench_remote_pre = bool(args.client_ip and args.client_ip != args.source_ip)
                    try:
                        # stop any leftover bench from previous runs
                        stop_bench_background(scene, remote=bench_remote_pre)
                    except Exception as e:
                        print(f"[pre-run] warning: stop_bench_background failed: {e}")
                    try:
                        # aggressively ensure destination is cleaned on remote
                        destination_clean_migration(scene)
                    except Exception as e:
                        print(f"[pre-run] warning: destination_clean_migration failed: {e}")
                    try:
                        # ensure source is clean too
                        source_clean_migration(scene)
                    except Exception as e:
                        print(f"[pre-run] warning: source_clean_migration failed: {e}")

                    # small delay to let remote cleanup settle
                    time.sleep(0.5)

                    mig_data = None
                    run_result = {'scene': scene, 'run': r + 1, 'exp': exp_name, 'ok': False}
                    try:
                        mig_data = run_migration_once(
                            scene,
                            run_port,
                            exp_args,
                            exp_name,
                            r + 1,
                            bench_duration=args.bench_duration,
                            bench_threads=args.bench_threads,
                            apply_network=not args.skip_network_shaping,
                            bench_host=args.bench_host,
                            bench_dataset=getattr(args, 'bench_dataset', None),
                            bench_files=getattr(args, 'bench_files', None),
                        )
                        run_result['ok'] = True
                    except Exception as _e:
                        # Record the failure but continue to next run/scene; collect diagnostics and clean up
                        tb = _traceback.format_exc()
                        print(f"[mig] scene {scene} run {r+1} failed: {_e}", file=sys.stderr)
                        run_result['error'] = str(_e)
                        run_result['error_trace'] = tb

                        # Collect diagnostics (local + remote when available)
                        try:
                            diag_files = collect_run_diagnostics(scene, r + 1, exp_name, dest_ip=DEST_IP, client_ip=CLIENT_IP)
                            run_result['error_files'] = diag_files
                        except Exception as _d_e:
                            print(f"[mig] diagnostics collection failed: {_d_e}", file=sys.stderr)
                        # Attempt to fetch bench metrics from client if bench was remote
                        try:
                            if bench_remote_pre:
                                fetched = fetch_remote_workload_metrics(scene, exp_name, r + 1, remote_host=CLIENT_IP)
                                if fetched:
                                    run_result.setdefault('fetched_bench_files', []).extend(fetched)
                        except Exception as _f_e:
                            print(f"[bench] fetch failed during exception handling: {_f_e}")

                        # Persist a per-run error file into results/errors
                        try:
                            errpath = write_run_error_file(run_result, scene, r + 1, exp_name)
                            if errpath:
                                run_result['error_file'] = errpath
                        except Exception:
                            pass

                        # Ensure robust cleanup before next run
                        try:
                            stop_bench_background(scene, remote=bench_remote)
                        except Exception:
                            pass
                        try:
                            destination_clean_migration(scene)
                        except Exception:
                            pass
                        try:
                            source_clean_migration(scene)
                        except Exception:
                            pass
                        try:
                            safe_clean_all(quiet=True)
                        except Exception:
                            pass

                        # Attempt to restore VIP back to source even when a run failed
                        try:
                            ok_vip_fail = ensure_vip_on_source()
                            run_result['vip_restored'] = bool(ok_vip_fail)
                            if not ok_vip_fail:
                                print(f"[vip] warning: failed to restore VIP to source after failed run {r+1} for {scene}")
                                run_result.setdefault('warnings', []).append('vip_not_restored_after_failure')
                        except Exception as _e:
                            print(f"[vip] ensure_vip_on_source failed during exception handling: {_e}")
                            try:
                                run_result['vip_restored'] = False
                            except Exception:
                                pass

                        # Honor user preference to stop on first error
                        if getattr(args, 'stop_on_error', False):
                            print("[mig] stop-on-error requested: aborting further runs")
                            KEEP_RUNNING = False
                            break
                    if mig_data:
                        if mig_data.get('metrics'):
                            run_result['metrics'] = mig_data['metrics']
                            scene_metrics.append(mig_data['metrics'])
                        if mig_data.get('header'):
                            run_result['metrics_header'] = mig_data['header']
                        if mig_data.get('stats'):
                            run_result['metrics_values'] = mig_data['stats']
                        if mig_data.get('params'):
                            run_result['metric_params'] = mig_data['params']

                    # Post-run: ensure VIP is restored/held on the source and capture diagnostics if not
                    try:
                        ok_vip_after = ensure_vip_on_source()
                        run_result['vip_restored'] = bool(ok_vip_after)
                        if not ok_vip_after:
                            print(f"[vip] warning: VIP not restored to source after run {r+1} for {scene}")
                            run_result.setdefault('warnings', []).append('vip_not_restored_after_run')
                            try:
                                diag_files = collect_run_diagnostics(scene, r + 1, exp_name, dest_ip=DEST_IP, client_ip=CLIENT_IP)
                                if diag_files:
                                    run_result.setdefault('error_files', []).extend(diag_files)
                            except Exception as _e:
                                print(f"[vip] collect_run_diagnostics failed: {_e}")
                            if getattr(args, 'stop_on_error', False):
                                print("[vip] stop-on-error requested due to VIP restore failure: aborting further runs")
                                KEEP_RUNNING = False
                                break
                    except Exception as e:
                        print(f"[vip] ensure_vip_on_source raised unexpected error: {e}")
                        try:
                            run_result['vip_restored'] = False
                        except Exception:
                            pass

                    results.append(run_result)
                    write_run_record('migration', scene, exp_name, r + 1, run_result)
                    port = run_port + 1
                append_scene_exp_summary(scene, exp_name, scene_metrics)
                if not KEEP_RUNNING:
                    break
        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        out_dir = os.path.join(RESULTS_ROOT, RUN_LABEL)
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, f'fog_test_migration_{ts}.json')
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        write_integrated_table(results, "migration")
        print(f"[done] migration summary -> {out}")
        return

    # Local-only real migration that reuses source.py iteration/metrics without ssh
    if args.mode == 'migration-local':
        exp_list = []
        if args.experiment_types:
            exp_list = [e.strip() for e in args.experiment_types.split(',') if e.strip()]
        if not exp_list:
            exp_list = ['pre-copy']

        results = []
        prev_source, prev_dest, prev_client = SOURCE_IP, DEST_IP, CLIENT_IP
        SOURCE_IP = DEST_IP = CLIENT_IP = DEFAULT_HOST
        try:
            for exp_name in exp_list:
                exp_args = EXPERIMENTS.get(exp_name)
                if not exp_args:
                    print(f"[mig-local] unknown experiment type {exp_name}, skipping")
                    continue
                for scene in scenes:
                    scene_info = SCENE_INFO.get(scene, {})
                    # Ensure VIP present on source before running experiments for this scene
                    try:
                        if not ensure_vip_on_source():
                            print(f"[vip] failed to ensure VIP on source before running scene {scene}; aborting further runs")
                            KEEP_RUNNING = False
                            break
                    except Exception as e:
                        print(f"[vip] ensure_vip_on_source error: {e}")
                        KEEP_RUNNING = False
                        break
                    scene_metrics = []
                    for r in range(args.runs):
                        if not KEEP_RUNNING:
                            break
                        run_port = port if args.start_port != 8080 else scene_info.get('default_port', port)
                        print(f"--- MIGRATION-LOCAL {scene} (run {r+1}) exp={exp_name} ---")
                        safe_clean_all(quiet=True)
                        mig_data = None
                        run_result = {'scene': scene, 'run': r + 1, 'exp': exp_name, 'ok': False}

                        # If the experiment includes post-copy, we do not support running
                        # post-copy locally. Skip and mark the run as skipped so the
                        # summary reflects that the experiment was intentionally not run.
                        tokens = shlex.split(exp_args or "")
                        if any(t in ("-post", "--post") for t in tokens):
                            print(f"[mig-local] experiment {exp_name!r} includes post-copy; skipping local run for scene {scene}")
                            run_result['skipped'] = 'post-copy not supported in migration-local mode'
                        else:
                            try:
                                mig_data = run_migration_local_once(
                                    scene,
                                    run_port,
                                    exp_args,
                                    exp_name,
                                    r + 1,
                                    bench_duration=args.bench_duration,
                                    bench_threads=args.bench_threads,
                                    apply_network=not args.skip_network_shaping,
                                )
                                run_result['ok'] = True
                            except Exception as _e:
                                tb = _traceback.format_exc()
                                print(f"[mig-local] scene {scene} run {r+1} failed: {_e}", file=sys.stderr)
                                run_result['error'] = str(_e)
                                run_result['error_trace'] = tb
                                try:
                                    diag_files = collect_run_diagnostics(scene, r + 1, exp_name, dest_ip=None, client_ip=None)
                                    run_result['error_files'] = diag_files
                                except Exception as _d_e:
                                    print(f"[mig-local] diagnostics collection failed: {_d_e}", file=sys.stderr)
                                try:
                                    errpath = write_run_error_file(run_result, scene, r + 1, exp_name)
                                    if errpath:
                                        run_result['error_file'] = errpath
                                except Exception:
                                    pass
                                # cleanup
                                try:
                                    safe_clean_all(quiet=True)
                                except Exception:
                                    pass
                                if getattr(args, 'stop_on_error', False):
                                    print("[mig-local] stop-on-error requested: aborting further runs")
                                    KEEP_RUNNING = False
                                    break
                        if mig_data:
                            if mig_data.get('metrics'):
                                run_result['metrics'] = mig_data['metrics']
                                scene_metrics.append(mig_data['metrics'])
                            if mig_data.get('header'):
                                run_result['metrics_header'] = mig_data['header']
                            if mig_data.get('stats'):
                                run_result['metrics_values'] = mig_data['stats']
                            if mig_data.get('params'):
                                run_result['metric_params'] = mig_data['params']
                        results.append(run_result)
                        write_run_record('migration-local', scene, exp_name, r + 1, run_result)
                        port = run_port + 1
                    append_scene_exp_summary(scene, exp_name, scene_metrics)
                    if not KEEP_RUNNING:
                        break
        finally:
            SOURCE_IP, DEST_IP, CLIENT_IP = prev_source, prev_dest, prev_client

        ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        out_dir = os.path.join(RESULTS_ROOT, RUN_LABEL)
        os.makedirs(out_dir, exist_ok=True)
        out = os.path.join(out_dir, f'fog_test_migration_local_{ts}.json')
        with open(out, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        write_integrated_table(results, "migration-local")
        print(f"[done] migration-local summary -> {out}")
        return

    # Default: smoke/start/bench flow
    results = []
    for scene in scenes:
        for r in range(args.runs):
            if not KEEP_RUNNING:
                break
            print(f"--- Testing {scene} (run {r}) ---")
            # ensure fresh state
            safe_clean_all()
            try:
                ok = run_scene(scene, port, args.collect_baseline, args.keep_containers, local=args.local, bandwidth=args.bandwidth, simulate_transfer=args.simulate_transfer)
                run_result = {'scene': scene, 'run': r + 1, 'ok': ok}
            except Exception as _e:
                tb = _traceback.format_exc()
                print(f"[smoke] scene {scene} run {r} failed: {_e}", file=sys.stderr)
                run_result = {'scene': scene, 'run': r + 1, 'ok': False, 'error': str(_e), 'error_trace': tb}
                try:
                    diag_files = collect_run_diagnostics(scene, r + 1, 'smoke', dest_ip=DEST_IP, client_ip=CLIENT_IP)
                    run_result['error_files'] = diag_files
                except Exception as _d_e:
                    print(f"[smoke] diagnostics collection failed: {_d_e}", file=sys.stderr)
                try:
                    errpath = write_run_error_file(run_result, scene, r + 1, 'smoke')
                    if errpath:
                        run_result['error_file'] = errpath
                except Exception:
                    pass
                try:
                    safe_clean_all(quiet=True)
                except Exception:
                    pass
                if getattr(args, 'stop_on_error', False):
                    print("[smoke] stop-on-error requested: aborting further runs")
                    KEEP_RUNNING = False
                    break
            results.append(run_result)
            write_run_record('smoke', scene, None, r + 1, run_result)
            port += 1
        if not KEEP_RUNNING:
            break

    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    out_dir = os.path.join(RESULTS_ROOT, RUN_LABEL)
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f'fog_test_summary_{ts}.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print(f"[done] summary -> {out}")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"[error] unhandled exception: {e}", file=sys.stderr)
        raise
    finally:
        safe_clean_all(quiet=True)
