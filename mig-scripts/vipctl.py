#!/usr/bin/env python3
"""VIP control utilities for fast VIP switching.

Functions are designed to be callable from migration scripts on the local host
(or executed remotely via SSH). Unit tests mock subprocess calls so they are
safe to run without root privileges.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import re
import os
from typing import Sequence, Optional

KEEPALIVED_CONF = "/etc/keepalived/keepalived.conf"
KEEPALIVED_BAK = KEEPALIVED_CONF + ".bak"
PRIORITY_RE = re.compile(r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})", re.S)


def set_keepalived_priority(new_priority: str | int, config_path: str = KEEPALIVED_CONF, backup_path: Optional[str] = None, reload_cmd: Optional[Sequence[str]] = None) -> int:
    """Set the priority in a Keepalived config and reload the service.

    Returns 0 on success, non-zero on failure.
    """
    backup_path = backup_path or (config_path + ".bak")
    reload_cmd = reload_cmd or ["sudo", "systemctl", "reload", "keepalived"]

    try:
        shutil.copy(config_path, backup_path)
        with open(config_path, "r") as f:
            config = f.read()

        def repl(m: re.Match):
            return f"{m.group(1)}{new_priority}{m.group(3)}"

        new_config, count = re.subn(PRIORITY_RE, repl, config)
        if count == 0:
            raise ValueError("vrrp_instance VI_1 priority not found in config")

        with open(config_path, "w") as f:
            f.write(new_config)

        res = subprocess.run(list(reload_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if res.returncode != 0:
            # restore
            shutil.copy(backup_path, config_path)
            subprocess.run(list(reload_cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            return 1
        return 0
    except PermissionError:
        print("Permission error; need to be root to modify Keepalived config")
        return 1
    except FileNotFoundError:
        print(f"Keepalived config not found: {config_path}")
        return 1
    except Exception as e:
        print(f"Error setting keepalived priority: {e}")
        return 1


def ip_addr_add(iface: str, vip: str, prefix: int = 32, scope: Optional[str] = None, dry_run: bool = False) -> int:
    cmd = ["ip", "addr", "add", f"{vip}/{prefix}", "dev", iface]
    if scope:
        cmd += ["scope", scope]
    if dry_run:
        print("DRY-RUN:", cmd)
        return 0
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res.returncode


def ip_addr_del(iface: str, vip: str, prefix: int = 32, dry_run: bool = False) -> int:
    cmd = ["ip", "addr", "del", f"{vip}/{prefix}", "dev", iface]
    if dry_run:
        print("DRY-RUN:", cmd)
        return 0
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res.returncode


def arping_announce(iface: str, vip: str, count: int = 2, dry_run: bool = False) -> int:
    # Use arping -U to send unsolicited ARP
    cmd = ["arping", "-U", "-I", iface, vip, "-c", str(count)]
    if dry_run:
        print("DRY-RUN:", cmd)
        return 0
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return res.returncode


def switch_local_add_then_announce(iface: str, vip: str, prefix: int = 32, announce_count: int = 2, dry_run: bool = False) -> int:
    ret = ip_addr_add(iface, vip, prefix=prefix, dry_run=dry_run)
    if ret != 0:
        return ret
    # best-effort announce
    _ = arping_announce(iface, vip, count=announce_count, dry_run=dry_run)
    return 0


def main():
    parser = argparse.ArgumentParser(prog="vipctl", description="VIP control helper for migrations")
    sub = parser.add_subparsers(dest="cmd")

    p = sub.add_parser("set-priority")
    p.add_argument("--priority", required=True)
    p.add_argument("--conf", default=KEEPALIVED_CONF)
    p.add_argument("--backup")

    p = sub.add_parser("ip-add")
    p.add_argument("--iface", required=True)
    p.add_argument("--vip", required=True)
    p.add_argument("--prefix", type=int, default=32)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("ip-del")
    p.add_argument("--iface", required=True)
    p.add_argument("--vip", required=True)
    p.add_argument("--prefix", type=int, default=32)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("announce")
    p.add_argument("--iface", required=True)
    p.add_argument("--vip", required=True)
    p.add_argument("--count", type=int, default=2)
    p.add_argument("--dry-run", action="store_true")

    p = sub.add_parser("switch-local")
    p.add_argument("--iface", required=True)
    p.add_argument("--vip", required=True)
    p.add_argument("--prefix", type=int, default=32)
    p.add_argument("--count", type=int, default=2)
    p.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()
    if args.cmd == "set-priority":
        rc = set_keepalived_priority(args.priority, config_path=args.conf, backup_path=args.backup)
        raise SystemExit(rc)
    if args.cmd == "ip-add":
        rc = ip_addr_add(args.iface, args.vip, prefix=args.prefix, dry_run=args.dry_run)
        raise SystemExit(rc)
    if args.cmd == "ip-del":
        rc = ip_addr_del(args.iface, args.vip, prefix=args.prefix, dry_run=args.dry_run)
        raise SystemExit(rc)
    if args.cmd == "announce":
        rc = arping_announce(args.iface, args.vip, count=args.count, dry_run=args.dry_run)
        raise SystemExit(rc)
    if args.cmd == "switch-local":
        rc = switch_local_add_then_announce(args.iface, args.vip, prefix=args.prefix, announce_count=args.count, dry_run=args.dry_run)
        raise SystemExit(rc)


if __name__ == "__main__":
    main()
