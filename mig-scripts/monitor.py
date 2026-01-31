#!/usr/bin/env python3
"""Centralized monitor utilities for dirty-track migrations.

Provides:
- set_phase(phase)
- NetworkMonitor: per-container network sampling via nsenter + host iface counters
- ContainerResourceMonitor: samples container cpu/mem + host cpu/mem and appends network
- HostResourceMonitor: lightweight host-side monitor for destination during transfers

This consolidates code previously spread across `source-cpu-mem(-net).py` and `destination.py`.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import statistics
import subprocess
import threading
import time
import csv
from fcntl import ioctl
import socket
import typing
import psutil

_monitor_lock = threading.Lock()
monitor_phase = "idle"


def set_phase(p: str):
    global monitor_phase
    with _monitor_lock:
        monitor_phase = p


class NetworkMonitor:
    """Container network sampler: reads /proc/net/dev inside the container netns and host iface counters.

    sample() -> dict or None (on first sample or on failure)
    returns keys: c_rx_mbps, c_tx_mbps, h_rx_mbps, h_tx_mbps
    """

    def __init__(self, init_pid: int, host_iface: str = "ens33"):
        self.init_pid = int(init_pid)
        self.host_iface = host_iface
        self._last = None
        self._if_re = re.compile(r"^\s*([^:]+):\s*(.+)$")

    def _read_container_bytes(self):
        try:
            out = subprocess.check_output(
                ["nsenter", "-t", str(self.init_pid), "-n", "cat", "/proc/net/dev"], text=True, timeout=1.0
            )
        except Exception:
            return None, None

        c_rx, c_tx = None, None
        fallback = None
        for line in out.splitlines():
            m = self._if_re.match(line)
            if not m:
                continue
            ifname, rest = m.group(1).strip(), m.group(2).split()
            if ifname == "lo":
                continue
            rx_bytes = int(rest[0])
            tx_bytes = int(rest[8])
            if ifname == "eth0":
                return rx_bytes, tx_bytes
            if fallback is None:
                fallback = (rx_bytes, tx_bytes)

        if fallback:
            c_rx, c_tx = fallback
        return c_rx, c_tx

    def _read_host_bytes(self):
        try:
            io = psutil.net_io_counters(pernic=True)
            if self.host_iface in io:
                ni = io[self.host_iface]
                return ni.bytes_recv, ni.bytes_sent
        except Exception:
            pass
        return None, None

    def sample(self):
        now = time.time()
        c = self._read_container_bytes()
        h = self._read_host_bytes()
        if c == (None, None) or h == (None, None):
            return None

        c_rx, c_tx = c
        h_rx, h_tx = h
        if self._last is None:
            self._last = (now, c_rx, c_tx, h_rx, h_tx)
            return None

        t0, c_rx0, c_tx0, h_rx0, h_tx0 = self._last
        dt = max(1e-6, now - t0)
        self._last = (now, c_rx, c_tx, h_rx, h_tx)

        def to_mbps(dbytes):
            return (max(0, dbytes) * 8.0) / dt / 1e6

        return {
            "c_rx_mbps": to_mbps(c_rx - c_rx0),
            "c_tx_mbps": to_mbps(c_tx - c_tx0),
            "h_rx_mbps": to_mbps(h_rx - h_rx0),
            "h_tx_mbps": to_mbps(h_tx - h_tx0),
        }


class ContainerResourceMonitor:
    """Sample container resource usage periodically and append TSV lines.

    Output columns:
    timestamp\trel_s\tphase\tcpu_pct\tmem_MB\thost_cpu_pct\thost_mem_MB\tcore_usage\tmethod\tc_rx_Mbps\tc_tx_Mbps\th_rx_Mbps\th_tx_Mbps\tiface
    """

    def __init__(
        self,
        container_name: str,
        interval: float = 1.0,
        out_path: str | None = None,
        include_host: bool = True,
        enable_net: bool = True,
        host_iface: str = "ens33",
    ):
        self.container = container_name
        self.interval = float(interval)
        self.include_host = include_host
        self.enable_net = enable_net
        self.host_iface = host_iface

        self.stop_evt = threading.Event()
        self.thread: threading.Thread | None = None
        self.start_time = None

        self._t_prev = None
        self._cg_prev = None
        self._ticks_prev = None

        self.cg_mode = None
        self.cg_cpu_path = None
        self.cg_mem_path = None
        self.cg_dir = None
        self.effective_cpus = None
        self.hz = os.sysconf(os.sysconf_names["SC_CLK_TCK"]) if hasattr(os, 'sysconf') else 100

        base_path = f"/runc/containers/{container_name}/migrate/d_log"
        os.makedirs(base_path, exist_ok=True)
        self.out_path = out_path or os.path.join(base_path, "resource_usage.csv")

        with open(f"/run/runc/{container_name}/state.json", "r") as f:
            self.init_pid = int(json.load(f)["init_process_pid"])

        self.netmon = NetworkMonitor(self.init_pid, host_iface=self.host_iface) if self.enable_net else None

        self._detect_cgroup_paths()
        self.effective_cpus = self._detect_effective_cpus()

        if not os.path.exists(self.out_path) or os.path.getsize(self.out_path) == 0:
            with open(self.out_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "rel_s",
                        "phase",
                        "cpu_pct",
                        "mem_MB",
                        "host_cpu_pct",
                        "host_mem_MB",
                        "core_usage",
                        "method",
                        "c_rx_Mbps",
                        "c_tx_Mbps",
                        "h_rx_Mbps",
                        "h_tx_Mbps",
                        "iface",
                    ]
                )

        self._prime()

        try:
            psutil.cpu_percent(None)
        except Exception:
            pass

    def _detect_cgroup_paths(self):
        try:
            lines = [ln.strip() for ln in open(f"/proc/{self.init_pid}/cgroup")]
        except Exception:
            self.cg_mode = None
            return

        v2_line = next((ln for ln in lines if ln.split(":")[0] == "0"), None)
        if v2_line:
            rel = v2_line.split(":", 2)[-1]
            root = "/sys/fs/cgroup"
            self.cg_mode = "cgv2"
            self.cg_dir = os.path.join(root, rel.lstrip("/"))
            self.cg_cpu_path = os.path.join(self.cg_dir, "cpu.stat")
            self.cg_mem_path = os.path.join(self.cg_dir, "memory.current")
            return

        def _find_ctrl(ctrl: str):
            for ln in lines:
                parts = ln.split(":")
                if len(parts) != 3:
                    continue
                ctrls, rel = parts[1], parts[2]
                if ctrl in ctrls.split(","):
                    for base in (f"/sys/fs/cgroup/{ctrl}", f"/sys/fs/cgroup/{ctrl},cpu", f"/sys/fs/cgroup/cpu,{ctrl}"):
                        full = os.path.join(base, rel.lstrip("/"))
                        if os.path.exists(full):
                            return full
            return None

        mem_dir = _find_ctrl("memory")
        cpu_dir = _find_ctrl("cpuacct")
        if mem_dir and cpu_dir:
            self.cg_mode = "cgv1"
            self.cg_dir = cpu_dir
            self.cg_mem_path = os.path.join(mem_dir, "memory.usage_in_bytes")
            self.cg_cpu_path = os.path.join(cpu_dir, "cpuacct.usage")
        else:
            self.cg_mode = None

    @staticmethod
    def _count_cpus_from_list(s: str) -> int:
        total = 0
        for part in s.strip().split(","):
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-", 1)
                total += int(b) - int(a) + 1
            else:
                total += 1
        return total

    def _detect_effective_cpus(self) -> int:
        try:
            if self.cg_mode == "cgv2" and self.cg_dir:
                p = os.path.join(self.cg_dir, "cpuset.cpus.effective")
                if os.path.exists(p):
                    s = open(p).read().strip()
                    n = self._count_cpus_from_list(s)
                    if n > 0:
                        return n
            elif self.cg_mode == "cgv1":
                with open(f"/proc/{self.init_pid}/cgroup", "r") as f:
                    for ln in f:
                        ps = ln.strip().split(":")
                        if len(ps) == 3 and "cpuset" in ps[1].split(","):
                            for root in ("/sys/fs/cgroup/cpuset", "/sys/fs/cgroup/cpuset,cpu", "/sys/fs/cgroup/cpu,cpuset"):
                                d = os.path.join(root, ps[2].lstrip("/"))
                                if os.path.exists(d):
                                    s = open(os.path.join(d, "cpuset.cpus")).read().strip()
                                    n = self._count_cpus_from_list(s)
                                    if n > 0:
                                        return n
        except Exception:
            pass
        try:
            for ln in open(f"/proc/{self.init_pid}/status"):
                if ln.startswith("Cpus_allowed_list:"):
                    s = ln.split(":", 1)[1].strip()
                    n = self._count_cpus_from_list(s)
                    if n > 0:
                        return n
        except Exception:
            pass
        return os.cpu_count() or 1

    def _prime(self):
        # initialize previous counters
        self._t_prev = time.time()
        try:
            if self.cg_mode in ("cgv1", "cgv2") and self.cg_cpu_path:
                if self.cg_mode == "cgv2":
                    kv = {}
                    with open(self.cg_cpu_path, "r") as f:
                        for ln in f:
                            sp = ln.split()
                            if len(sp) == 2 and sp[1].isdigit():
                                kv[sp[0]] = int(sp[1])
                    self._cg_prev = (kv.get("usage_usec", 0), "usec")
                else:
                    val = int(open(self.cg_cpu_path, "r").read().strip())
                    self._cg_prev = (val, "ns")
        except Exception:
            self._cg_prev = None

        try:
            self._ticks_prev = sum(
                float(x) for x in subprocess.check_output(["ps", "-o", "pid,utime,stime", "-p", str(self.init_pid)], text=True).split()
            )
        except Exception:
            self._ticks_prev = None

        # prime network
        if self.netmon:
            self.netmon.sample()

    def _sample_once(self):
        now = time.time()
        if self.start_time is None:
            self.start_time = now
        rel = now - self.start_time
        phase = monitor_phase

        # host stats
        try:
            host_cpu = psutil.cpu_percent(None)
            host_mem_mb = psutil.virtual_memory().used / (1024.0 * 1024.0)
        except Exception:
            host_cpu = 0.0
            host_mem_mb = 0.0

        # container mem
        mem_mb = 0.0
        cpu_pct = 0.0
        method = "NA"
        core_usage = 0.0

        # try cgroup readings first
        try:
            if self.cg_mode == "cgv2" and self.cg_cpu_path:
                kv = {}
                with open(self.cg_cpu_path, "r") as f:
                    for ln in f:
                        sp = ln.split()
                        if len(sp) == 2 and sp[1].isdigit():
                            kv[sp[0]] = int(sp[1])
                cur = kv.get("usage_usec", 0)
                unit = "usec"
                if self._cg_prev is not None:
                    prev, prev_unit = self._cg_prev
                    dt = max(1e-6, now - self._t_prev)
                    delta = (cur - prev) / 1_000_000.0  # usec -> sec fraction of CPU-seconds
                    # CPU percent across effective cpus
                    cpu_pct = (delta / dt) * 100.0 / float(self.effective_cpus)
                    core_usage = (delta / dt) * float(self.effective_cpus)
                self._cg_prev = (cur, unit)
                method = "cgroup"
            elif self.cg_mode == "cgv1" and self.cg_cpu_path:
                cur = int(open(self.cg_cpu_path, "r").read().strip())
                unit = "ns"
                if self._cg_prev is not None:
                    prev, prev_unit = self._cg_prev
                    dt = max(1e-6, now - self._t_prev)
                    # cur and prev are in ns
                    delta = (cur - prev) / 1_000_000_000.0
                    cpu_pct = (delta / dt) * 100.0 / float(self.effective_cpus)
                    core_usage = (delta / dt) * float(self.effective_cpus)
                self._cg_prev = (cur, unit)
                method = "cgroup"
            else:
                raise RuntimeError("no cgroup")
        except Exception:
            # fallback: procsum (approx)
            try:
                out = subprocess.check_output(["ps", "-p", str(self.init_pid), "-o", "pcpu,rss"], text=True)
                lines = [ln for ln in out.splitlines() if ln.strip()]
                if len(lines) >= 2:
                    parts = lines[1].split()
                    cpu_pct = float(parts[0])
                    mem_mb = int(parts[1]) / (1024.0)
                method = "procsum"
            except Exception:
                pass

        # if cgroup mem available try reading
        try:
            if self.cg_mode in ("cgv1", "cgv2") and self.cg_mem_path:
                mem = int(open(self.cg_mem_path, "r").read().strip())
                mem_mb = mem / (1024.0 * 1024.0)
        except Exception:
            pass

        # network
        net_vals = None
        try:
            if self.netmon:
                net_vals = self.netmon.sample()
        except Exception:
            net_vals = None

        c_rx = c_tx = h_rx = h_tx = "NA"
        iface = self.host_iface
        if net_vals:
            c_rx = f"{net_vals['c_rx_mbps']:.3f}"
            c_tx = f"{net_vals['c_tx_mbps']:.3f}"
            h_rx = f"{net_vals['h_rx_mbps']:.3f}"
            h_tx = f"{net_vals['h_tx_mbps']:.3f}"

        # write line
        try:
            with open(self.out_path, "a", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        f"{now}",
                        f"{rel:.3f}",
                        phase,
                        f"{cpu_pct:.2f}",
                        f"{mem_mb:.2f}",
                        f"{host_cpu:.2f}",
                        f"{host_mem_mb:.2f}",
                        f"{core_usage:.3f}",
                        method,
                        c_rx,
                        c_tx,
                        h_rx,
                        h_tx,
                        iface,
                    ]
                )
        except Exception:
            pass

        self._t_prev = now

    def _run(self):
        self.start_time = None
        self._t_prev = time.time()
        while not self.stop_evt.wait(self.interval):
            try:
                self._sample_once()
            except Exception:
                pass

    def start(self):
        self.stop_evt.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_evt.set()
        if self.thread:
            self.thread.join(timeout=2.0)


class HostResourceMonitor:
    """Lightweight host monitor writing host CPU/memory and iface bandwidth to TSV.

    Columns: timestamp\trel_s\tphase\thost_cpu_pct\thost_mem_MB\th_rx_Mbps\th_tx_Mbps\tiface\n
    """

    def __init__(self, out_path: str, interval: float = 1.0, iface: str = "ens33"):
        self.out_path = out_path
        self.interval = float(interval)
        self.iface = iface
        self._stop = threading.Event()
        self.thread: threading.Thread | None = None
        self.start_time = None
        self._last_net = None

        os.makedirs(os.path.dirname(self.out_path), exist_ok=True)
        if not os.path.exists(self.out_path) or os.path.getsize(self.out_path) == 0:
            with open(self.out_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["timestamp", "rel_s", "phase", "host_cpu_pct", "host_mem_MB", "h_rx_Mbps", "h_tx_Mbps", "iface"]
                )

        try:
            psutil.cpu_percent(None)
            psutil.net_io_counters(pernic=True)
        except Exception:
            pass

    def _sample_once(self):
        now = time.time()
        if self.start_time is None:
            self.start_time = now
        rel_s = now - self.start_time
        try:
            cpu_pct = psutil.cpu_percent(None)
            mem_mb = psutil.virtual_memory().used / (1024.0 * 1024.0)
        except Exception:
            cpu_pct = 0.0
            mem_mb = 0.0
        io = {}
        try:
            io = psutil.net_io_counters(pernic=True)
        except Exception:
            pass
        rx = tx = None
        if self.iface in io:
            ni = io[self.iface]
            rx = ni.bytes_recv
            tx = ni.bytes_sent
        else:
            for k, v in io.items():
                if k != "lo":
                    rx = v.bytes_recv
                    tx = v.bytes_sent
                    break

        if rx is None or tx is None:
            if self._last_net is None:
                self._last_net = (now, rx or 0, tx or 0)
                return
            else:
                c_rx = "NA"
                c_tx = "NA"
        else:
            if self._last_net is None:
                self._last_net = (now, rx, tx)
                return
            t0, rx0, tx0 = self._last_net
            dt = max(1e-6, now - t0)
            c_rx = (rx - rx0) * 8.0 / dt / 1e6
            c_tx = (tx - tx0) * 8.0 / dt / 1e6
            self._last_net = (now, rx, tx)

        with open(self.out_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    f"{now}",
                    f"{rel_s:.3f}",
                    "transfer",
                    f"{cpu_pct:.2f}",
                    f"{mem_mb:.2f}",
                    c_rx if isinstance(c_rx, str) else f"{c_rx:.3f}",
                    c_tx if isinstance(c_tx, str) else f"{c_tx:.3f}",
                    self.iface,
                ]
            )

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                self._sample_once()
            except Exception:
                pass

    def start(self):
        self.start_time = None
        self._stop.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self):
        self._stop.set()
        if self.thread:
            self.thread.join(timeout=2.0)
