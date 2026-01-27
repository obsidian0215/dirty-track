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
from typing import Optional
import argparse
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime

from result_writer import extract_stats_from_output, append_result, summarize_results
from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs
from script_defaults import choose_scripts, get_default_ips

# Defaults (reuse helpers that are common across wrappers)
SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
SEC_MODE = False
BANDWIDTH = "50mbit"
DEFAULT_HOST = "127.0.0.1"
RUN_LABEL = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
RESULTS_ROOT = '/runc/dirty-track/results/fog_tests'
LOCAL_HOSTS = {None, '127.0.0.1', 'localhost'}


# --- Network shaping helpers (aligned with redis_test.py / influxdb_test.py) ---
def clean_configure_network():
    primary_iface = "enp2s0"
    client_iface = "enp2s0" if CLIENT_IP == DEST_IP else "ens33"

    def _clear_local(iface: str):
        run_cmd(f"sudo tc qdisc del dev {iface} root", ignore_error=True, quiet=True)

    _clear_local(primary_iface)
    if DEST_IP and DEST_IP != SOURCE_IP:
        run_remote_cmd(f"sudo tc qdisc del dev {primary_iface} root", DEST_IP, ignore_error=True, quiet=True)

    if CLIENT_IP:
        target = CLIENT_IP
        iface = client_iface
        if target == SOURCE_IP:
            _clear_local(iface)
        else:
            run_remote_cmd(f"sudo tc qdisc del dev {iface} root", target, ignore_error=True, quiet=True)


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    cleanup_cmd = f"sudo tc qdisc del dev {interface} root"
    init_cmd = f"sudo tc qdisc add dev {interface} root handle 1: htb"
    remote_ip = str(target_ip) if target_ip else None
    remote_mode = bool(is_remote and remote_ip not in (None, '127.0.0.1', 'localhost', SOURCE_IP))

    if remote_mode:
        assert remote_ip is not None
        print(f"[net] clearing rules on remote:{remote_ip} {interface}")
        run_remote_cmd(cleanup_cmd, remote_ip, ignore_error=True, quiet=True)
        run_remote_cmd(init_cmd, remote_ip, ignore_error=ignore_error, quiet=True)
    else:
        location = f"local ({remote_ip})" if remote_ip else "local"
        print(f"[net] clearing rules on {location} {interface}")
        run_cmd(cleanup_cmd, ignore_error=True, quiet=True)
        run_cmd(init_cmd, ignore_error=ignore_error, quiet=True)

    for idx, rule in enumerate(rules, start=1):
        classid = f"1:{idx}"
        handle = f"{10 * idx}:"
        rate = rule["rate"]
        delay = rule["delay"]
        dst = rule["dst"]
        location = f"remote:{remote_ip}" if remote_mode else "local"
        print(f"[net] rule#{idx} on {location}: dst={dst} rate={rate} delay={delay}")

        cmds = [
            f"sudo tc class add dev {interface} parent 1: classid {classid} htb rate {rate}",
            f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {dst} flowid {classid}",
            f"sudo tc qdisc add dev {interface} parent {classid} handle {handle} netem delay {delay}",
        ]

        for cmd in cmds:
            if remote_mode:
                assert remote_ip is not None
                run_remote_cmd(cmd, remote_ip, ignore_error=ignore_error, quiet=True)
            else:
                run_cmd(cmd, ignore_error=ignore_error, quiet=True)


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

    configure_network_do(interface=primary_iface, rules=source_rules, is_remote=False)
    configure_network_do(interface=primary_iface, rules=dest_rules, is_remote=True, target_ip=DEST_IP)
    if CLIENT_IP:
        if CLIENT_IP == SOURCE_IP:
            configure_network_do(interface=client_iface, rules=client_rules, is_remote=False)
        else:
            configure_network_do(interface=client_iface, rules=client_rules, is_remote=True, target_ip=CLIENT_IP)


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
    "gocr": {"bundle": "gocr", "endpoint": "/ocr", "asset": "/runc/datasets/ocr/images/0001.png", "bench": "python3 /runc/bench_clients/run_bench.py --url http://127.0.0.1:PORT/ocr --file /runc/datasets/ocr/images/0001.png --iters 50 --concurrency 4", "persistent": False},
    "gzip": {"bundle": "gzip", "endpoint": "/compress", "asset": "/runc/datasets/compress/sample.bin", "bench": "python3 /runc/bench_clients/bench_gzip.py --url http://127.0.0.1:PORT/compress --file /runc/datasets/compress/sample.bin --iters 50 --concurrency 4", "persistent": True},
    "yolo": {"bundle": "yolo", "endpoint": "/detect", "asset": "/runc/datasets/images/dog.jpg", "bench": "python3 /runc/bench_clients/run_bench.py --url http://127.0.0.1:PORT/detect --file /runc/datasets/images/dog.jpg --iters 50 --concurrency 3", "persistent": False},
    "pocketsphinx": {"bundle": "pocketsphinx", "endpoint": "/transcribe", "asset": "/runc/datasets/audio/sample.wav", "bench": "python3 /runc/bench_clients/run_bench.py --url http://127.0.0.1:PORT/transcribe --file /runc/datasets/audio/sample.wav --iters 20 --concurrency 2", "persistent": False},
    "aeneas": {"bundle": "aeneas", "endpoint": "/align", "asset": "/runc/datasets/audio/sample.mp3,/runc/datasets/ocr/sample.xhtml", "bench": "python3 /runc/bench_clients/bench_aeneas.py --url http://127.0.0.1:PORT/align --audio /runc/datasets/audio/sample.mp3 --text /runc/datasets/ocr/sample.xhtml --iters 20 --concurrency 2", "persistent": False},

    # Service-level scenes
    "sensoragg": {"bundle": "sensoragg", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8181, "persistent": False},
    "cartelem": {"bundle": "cartelem", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8181, "persistent": False},
    "ipokemon": {"bundle": "ipokemon", "endpoint": "/", "asset": None, "bench": None, "default_port": 8000, "persistent": False},
    "video": {"bundle": "video", "endpoint": "redis", "asset": "/runc/datasets/images/dog.jpg", "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    "transportation": {"bundle": "transportation", "endpoint": "redis", "asset": None, "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    "industrial": {"bundle": "industrial", "endpoint": "redis", "asset": None, "bench": None, "default_port": 6379, "persistent": False, "backend": "redis"},
    # "foglamp": {"bundle": "foglamp", "endpoint": "/health", "asset": None, "bench": None, "default_port": 8080, "persistent": False},
    "elasticsearch": {"bundle": "elasticsearch", "endpoint": "/_cluster/health", "asset": None, "bench": "python3 /runc/dirty-track/experiment/migration/elasticsearch/benchmark.py --es-host 127.0.0.1 --es-port PORT --threads 4 --rps 100 --duration 30 --test-mode index", "default_port": 9200, "persistent": True},
}

KEEP_RUNNING = True
CURRENT_RUNNING = []


def ensure_dirs():
    # Ensure necessary result and containers directories exist
    os.makedirs(RESULTS_ROOT, exist_ok=True)
    os.makedirs(os.path.join(RESULTS_ROOT, RUN_LABEL), exist_ok=True)
    os.makedirs('/runc/containers', exist_ok=True)
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
    else:
        run_cmd(cmd, ignore_error=False)
        try:
            ensure_assets_in_bundle(dest, scene)
        except Exception:
            pass
        sanitize_profile(dest, remote=False)
    return dest


def start_recvtty_for_bundle(bundle_path: str, scene: str, remote: bool = False, target_ip: Optional[str] = None, mode: str = 'null'):
    console_sock = os.path.join(bundle_path, 'console.sock')
    pidfile = f"/tmp/recvtty_{scene}_{'remote' if remote else 'local'}.pid"
    cmd = f"PATH=$PATH:/root/go/bin recvtty -m {mode} {shlex.quote(console_sock)} > /tmp/recvtty_{scene}.log 2>&1 & echo $! > {shlex.quote(pidfile)}"
    target = target_ip or DEST_IP
    if remote and target not in (None, '127.0.0.1', 'localhost', SOURCE_IP):
        run_remote_cmd(cmd, target, ignore_error=True, quiet=True)
    else:
        run_cmd(cmd, ignore_error=True, quiet=True)
    return console_sock, pidfile


def start_container_for_migration(scene: str, port: int) -> str:
    if scene == 'elasticsearch':
        ensure_vm_max_map_count()
    bundle_path = os.path.join('/runc/containers', scene)
    patch_bundle_port(bundle_path, port)
    console_opt = ''
    try:
        cfgj = json.load(open(os.path.join(bundle_path, 'config.json')))
        needs_console = bool(cfgj.get('process', {}).get('terminal', False))
    except Exception:
        needs_console = False
    if needs_console:
        console_sock, _ = start_recvtty_for_bundle(bundle_path, scene, remote=False)
        console_opt = f"--console-socket {shlex.quote(console_sock)}"

    # ensure no stale container
    run_cmd(f"runc kill {shlex.quote(scene)}", ignore_error=True, quiet=True)
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
            "    data['process']['terminal']=True\n"
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
    except Exception:
        pass
    start_recvtty_for_bundle(bundle, scene, remote=True, target_ip=DEST_IP)
    ts = int(time.time())
    dest_log = f"/tmp/{DEST_SCRIPT.replace('.', '_')}_{scene}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{scene}.pid"
    start_dest_cmd = (
        f"nohup {shlex.quote(sys.executable)} /runc/dirty-track/mig-scripts/{DEST_SCRIPT} > {dest_log} 2>&1 "
        f"& echo $! > {dest_pidfile}"
    )
    if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
        run_cmd(start_dest_cmd, ignore_error=False)
    else:
        run_remote_cmd(start_dest_cmd, DEST_IP, ignore_error=False)


def destination_clean_migration(scene: str):
    kill_destination_listener(DEST_IP)
    cmds = [
        (f"runc kill {scene}", True),
        (f"runc delete {scene}", True),
        ("kill -9 $(cat /tmp/recvtty_destination.pid) 2>/dev/null", True),
    ]
    if DEST_IP in ('127.0.0.1', 'localhost', SOURCE_IP):
        for c, ign in cmds:
            run_cmd(c, ignore_error=ign, quiet=True)
        run_cmd("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True, quiet=True)
        run_cmd(f"if [ -f /tmp/destination_{scene}.pid ]; then kill -TERM $(cat /tmp/destination_{scene}.pid) 2>/dev/null || true; rm -f /tmp/destination_{scene}.pid; fi", ignore_error=True, quiet=True)
    else:
        for c, ign in cmds:
            run_remote_cmd(c, DEST_IP, ignore_error=ign, quiet=True)
        run_remote_cmd("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", DEST_IP, ignore_error=True, quiet=True)
        run_remote_cmd(f"if [ -f /tmp/destination_{scene}.pid ]; then kill -TERM $(cat /tmp/destination_{scene}.pid) 2>/dev/null || true; rm -f /tmp/destination_{scene}.pid; fi", DEST_IP, ignore_error=True, quiet=True)


def source_clean_migration(scene: str):
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True, quiet=True)
    unmount_local_migration_tmpfs(scene)
    run_cmd(f"runc kill {scene}", ignore_error=True, quiet=True)
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
    # remove tmp bundles and recvtty artifacts
    run_cmd("rm -rf /tmp/fog_bundle.* /tmp/*_start.pid /tmp/*_loop.pid /tmp/recvtty_*.pid /tmp/recvtty-*.log /tmp/recvtty_debug.log", ignore_error=True, quiet=True)
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
    mapping = {
        'pocketsphinx':[('/runc/datasets/audio/sample.wav','psphinx.wav')],
        # use original basenames so container-side /mnt/assets checks match SCENE_INFO asset names
        'aeneas':[('/runc/datasets/audio/sample.mp3','sample.mp3'),('/runc/datasets/ocr/sample.xhtml','sample.xhtml')],
        'yolo':[('/runc/datasets/images/dog.jpg','yoloimage.jpg'),('/runc/datasets/images/dog.jpg','dog.jpg')],
        'video':[('/runc/datasets/images/dog.jpg','dog.jpg')],
        'gocr':[('/runc/datasets/ocr/images/0001.png','0001.png')],
        'gzip':[('/runc/datasets/compress/sample.bin','sample.bin')],
    }
    if scene not in mapping:
        return
    destdir = os.path.join(bundle_path, 'rootfs', 'mnt', 'assets')
    run_cmd(f"mkdir -p {shlex.quote(destdir)}", quiet=True, ignore_error=True)
    for src, destname in mapping[scene]:
        if os.path.exists(src):
            dst = os.path.join(destdir, destname)
            run_cmd(f"cp -f {shlex.quote(src)} {shlex.quote(dst)}", quiet=True, ignore_error=True)
            run_cmd(f"chmod 644 {shlex.quote(dst)}", quiet=True, ignore_error=True)
        else:
            print(f"[data] missing canonical asset {src} for {scene} - continuing")

# ensure cleanup runs at process exit
atexit.register(safe_clean_all)


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
        run_cmd(f"runc kill {shlex.quote(container)}", ignore_error=True, quiet=True)
        run_cmd(f"runc kill {shlex.quote(container)} KILL", ignore_error=True, quiet=True)
        run_cmd(f"runc delete {shlex.quote(container)}", ignore_error=True, quiet=True)
        time.sleep(delay)
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
    # Influx check
    cfg = os.path.join(bundle_path, 'config.json')
    if os.path.exists(cfg):
        try:
            import json as _json
            cfgj = _json.load(open(cfg))
            args = cfgj.get('process', {}).get('args', [])
            joined = ' '.join(args)
            if '--object-store=memory' in joined or '--object-store=tmpfs' in joined or '--object-store=memory' in joined:
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
    run_cmd(f"runc kill {scene}", ignore_error=True, quiet=True)
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
    run_cmd(f"runc kill {shlex.quote(scene)}", ignore_error=True, quiet=True)
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
            if not is_container_running(scene):
                print(f"[health] {scene} not running during {label}; restarting")
                restart_container_for_migration(scene, port)
                continue
            if basic_http_ready():
                return True
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
    # Search the migration tree for a canonical `bench.py` first, then fall back to `bench_*.py`
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
        # Fallback to legacy bench_*.py
        for r, _, files in os.walk(root):
            for f in files:
                if f.startswith('bench_') and f.endswith('.py'):
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


def run_bench(scene: str, port: int):
    py = shlex.quote(sys.executable)
    info = SCENE_INFO.get(scene, {})
    bench = info.get('bench')
    bundle = info.get('bundle', scene)

    # Prefer service-specific benches under migration/<scene>/ first, then fallback to bundle-level benches
    local_bench = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(bundle)

    ts = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    outf = os.path.join('/runc/dirty-track/results/fog_tests', f"{scene}_bench_{ts}.csv")

    if local_bench:
        backend = detect_backend_from_bench(local_bench)
        if backend == 'redis':
            cmd = f"{py} {shlex.quote(local_bench)} --redis-host 127.0.0.1 --redis-port {int(port)} --threads 4 --duration 30 --dataset /runc/datasets"
        elif backend == 'influxdb':
            cmd = f"{py} {shlex.quote(local_bench)} --influx-url http://127.0.0.1:{int(port)} --threads 4 --duration 30 --dataset /runc/datasets"
        elif backend == 'elasticsearch':
            cmd = f"{py} {shlex.quote(local_bench)} --es-host 127.0.0.1 --es-port {int(port)} --threads 4 --rps 100 --duration 30"
        elif backend == 'http':
            url = f"http://127.0.0.1:{int(port)}{info.get('endpoint','/')}"
            cmd = f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)} --dataset /runc/datasets"
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
                cmd = f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)} --dataset /runc/datasets"
        cmd_full = f"{cmd} --out {shlex.quote(outf)}"
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
        return outf
    return None


def build_bench_command(scene: str, port: int, host: Optional[str] = None, duration: int = 600, threads: int = 2, out_path: Optional[str] = None) -> Optional[str]:
    py = shlex.quote(sys.executable)
    host = host or SOURCE_IP or DEFAULT_HOST
    info = SCENE_INFO.get(scene, {})
    bundle = info.get('bundle', scene)
    endpoint = info.get('endpoint', '/')
    bench = info.get('bench')
    local_bench = find_local_bench_for_bundle(scene) or find_local_bench_for_bundle(bundle)
    out_path = out_path or f"/tmp/{scene}_bench_{int(time.time())}.csv"

    if local_bench:
        backend = detect_backend_from_bench(local_bench)
        if backend == 'redis':
            return f"{py} {shlex.quote(local_bench)} --redis-host {host} --redis-port {int(port)} --threads {int(threads)} --duration {int(duration)} --dataset /runc/datasets --out {shlex.quote(out_path)}"
        if backend == 'influxdb':
            return f"{py} {shlex.quote(local_bench)} --influx-url http://{host}:{int(port)} --threads {int(threads)} --duration {int(duration)} --dataset /runc/datasets --out {shlex.quote(out_path)}"
        if backend == 'elasticsearch':
            return f"{py} {shlex.quote(local_bench)} --es-host {host} --es-port {int(port)} --threads {int(threads)} --rps 100 --duration {int(duration)} --out {shlex.quote(out_path)}"
        if backend == 'http':
            url = f"http://{host}:{int(port)}{endpoint}"
            return f"{py} {shlex.quote(local_bench)} --url {shlex.quote(url)} --duration {int(duration)} --threads {int(threads)} --dataset /runc/datasets --out {shlex.quote(out_path)}"
        if backend == 'jmeter':
            # JMeter benches (iPokeMon) are harder to run remotely; skip for migration mode for now
            print(f"[bench] backend jmeter detected for {scene}; skipping background bench command")
            return None
    elif bench:
        cmd = bench.replace('PORT', str(port)).replace('127.0.0.1', host)
        if cmd.startswith('python3 '):
            cmd = cmd.replace('python3', py, 1)
        elif cmd.startswith('python '):
            cmd = cmd.replace('python', py, 1)
        cmd = f"{cmd} --duration {int(duration)} --threads {int(threads)}"
        return f"{cmd} --out {shlex.quote(out_path)}"
    return None


def start_bench_background(scene: str, port: int, host: Optional[str] = None, duration: int = 600, threads: int = 2, remote: bool = False) -> Optional[str]:
    cmd = build_bench_command(scene, port, host=host, duration=duration, threads=threads)
    if not cmd:
        print(f"[bench] no bench command for {scene}, skipping background load")
        return None
    pidfile = f"/tmp/bench_{scene}.pid"
    log = f"/tmp/bench_{scene}.log"
    full = f"nohup {cmd} > {log} 2>&1 & echo $! > {pidfile}"
    if remote:
        run_remote_cmd(full, CLIENT_IP, ignore_error=True)
    else:
        run_cmd(full, ignore_error=True)
    return pidfile


def stop_bench_background(scene: str, remote: bool = False):
    pidfile = f"/tmp/bench_{scene}.pid"
    stop_cmd = (
        f"if [ -f {pidfile} ]; then PID=$(cat {pidfile}); "
        "if [ -n \"$PID\" ]; then kill $PID 2>/dev/null || true; sleep 0.5; kill -9 $PID 2>/dev/null || true; fi; "
        f"rm -f {pidfile}; fi"
    )
    if remote:
        run_remote_cmd(stop_cmd, CLIENT_IP, ignore_error=True)
    else:
        run_cmd(stop_cmd, ignore_error=True)


def run_migration_once(scene: str, port: int, exp_args: str, exp_name: str, run_index: int, bench_duration: int = 600, bench_threads: int = 2, apply_network: bool = True, bench_host: Optional[str] = None):
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
    pidfile = start_bench_background(scene, port, host=bench_host or SOURCE_IP, duration=bench_duration, threads=bench_threads, remote=bench_remote)

    if apply_network:
        try:
            configure_network(bandwidth=BANDWIDTH)
        except Exception as e:
            print(f"[net] configure_network error: {e}")

    time.sleep(10)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_script_path = os.path.join(script_dir, SOURCE_SCRIPT)
    is_local_run = SOURCE_IP in LOCAL_HOSTS and DEST_IP in LOCAL_HOSTS

    cmd_list = [sys.executable, source_script_path]
    if is_local_run:
        cmd_list += ["--bandwidth", BANDWIDTH]
    cmd_list += shlex.split(exp_args)
    if SCENE_INFO.get(scene, {}).get('persistent'):
        cmd_list.append('--file-locks')
    cmd_list += [scene, DEST_IP]
    print("[mig] Running migration:", " ".join(cmd_list))
    result = run_cmd(cmd_list, quiet=True, cwd=script_dir)
    stdout = getattr(result, 'stdout', '') or ''
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
            append_result(exp_name, scene, run_index, stats, header, params_summary, is_secure=SEC_MODE, extra_param_lines=params, first_in_run=(run_index == 1))
            print(f"[mig] wrote stats for {exp_name} run {run_index}")
    except Exception as e:
        print(f"[mig] failed to write stats: {e}")

    stop_bench_background(scene, remote=bench_remote)
    source_clean_migration(scene)
    destination_clean_migration(scene)
    if apply_network:
        clean_configure_network()

    return {
        "header": header,
        "stats": stats,
        "params": params,
        "metrics": metrics_dict,
        "stdout": stdout,
    }


def run_migration_local_once(scene: str, port: int, exp_args: str, exp_name: str, run_index: int, bench_duration: int = 600, bench_threads: int = 2):
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
            apply_network=False,
            bench_host=DEFAULT_HOST,
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
    outdir = os.path.join('/runc/dirty-track/results/fog_tests', f"{scene}_{ts}_run{run_idx}")
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
        run_cmd(f"runc kill {shlex.quote(container)}", ignore_error=True, quiet=True)
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


def run_scene(scene: str, start_port: int, collect_baseline: bool, skip_bench: bool, keep_containers: bool, local: bool = False, bandwidth: str = '50mbit', simulate_transfer: bool = False):
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
        bench = None
        if not skip_bench:
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

    parser = argparse.ArgumentParser()
    parser.add_argument('--scenes', default='all')
    parser.add_argument('--runs', type=int, default=1)
    parser.add_argument('--start-port', type=int, default=8080)
    parser.add_argument('--clean-first', action='store_true')
    parser.add_argument('--collect-baseline', action='store_true')
    parser.add_argument('--keep-containers', action='store_true')
    parser.add_argument('--skip-bench', action='store_true')
    parser.add_argument('--local', action='store_true', help='Run in local simulation mode (simulate pre-dump/transfer/restore)')
    parser.add_argument('--bandwidth', default='50mbit', help='Bandwidth to simulate in local mode or network shaping (e.g., 50mbit)')
    parser.add_argument('--simulate-transfer', action='store_true', help='If set, actually sleep to simulate transfer times')
    parser.add_argument('--mode', choices=['smoke', 'migration', 'migration-local'], default='smoke', help='smoke: existing bench/start; migration: run source/destination hot migration; migration-local: run hot migration locally without ssh')
    parser.add_argument('--experiment-types', default='pre-copy', help='Comma-separated experiment types (pre-copy, pre-copy-dirtymap, post-copy, hybrid, hybrid-dirtymap) for migration mode')
    parser.add_argument('--sec', action='store_true', help='Use secure source/destination scripts (source-sec.py/destination-sec.py)')
    parser.add_argument('--source-ip', default=SOURCE_IP)
    parser.add_argument('--dest-ip', default=DEST_IP)
    parser.add_argument('--client-ip', default=CLIENT_IP)
    parser.add_argument('--bench-host', default=None, help='Override bench target host (default SOURCE_IP)')
    parser.add_argument('--bench-duration', type=int, default=600, help='Bench duration for migration mode background load')
    parser.add_argument('--bench-threads', type=int, default=2, help='Bench threads/concurrency for migration mode background load')
    parser.add_argument('--skip-network-shaping', action='store_true', help='Skip tc shaping during migration mode')
    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    BANDWIDTH = args.bandwidth
    SEC_MODE = bool(args.sec)
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(SEC_MODE)
    ensure_dirs()

    if args.mode != 'migration-local':
        if SOURCE_IP in ('127.0.0.1', 'localhost') or DEST_IP in ('127.0.0.1', 'localhost') or CLIENT_IP in (
            '127.0.0.1', 'localhost'
        ):
            raise SystemExit("Loopback addresses are only allowed in migration-local mode; please provide real host IPs.")

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

    # Migration mode (hot migration aligned with redis/influx flows)
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
            for scene in scenes:
                scene_info = SCENE_INFO.get(scene, {})
                for r in range(args.runs):
                    if not KEEP_RUNNING:
                        break
                    run_port = port if args.start_port != 8080 else scene_info.get('default_port', port)
                    print(f"--- MIGRATION {scene} (run {r+1}) exp={exp_name} ---")
                    safe_clean_all(quiet=True)
                    clean_configure_network()
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
                    )
                    run_result = {'scene': scene, 'run': r + 1, 'exp': exp_name, 'ok': True}
                    if mig_data:
                        if mig_data.get('metrics'):
                            run_result['metrics'] = mig_data['metrics']
                        if mig_data.get('header'):
                            run_result['metrics_header'] = mig_data['header']
                        if mig_data.get('stats'):
                            run_result['metrics_values'] = mig_data['stats']
                        if mig_data.get('params'):
                            run_result['metric_params'] = mig_data['params']
                    results.append(run_result)
                    write_run_record('migration', scene, exp_name, r + 1, run_result)
                    port = run_port + 1
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
                    for r in range(args.runs):
                        if not KEEP_RUNNING:
                            break
                        run_port = port if args.start_port != 8080 else scene_info.get('default_port', port)
                        print(f"--- MIGRATION-LOCAL {scene} (run {r+1}) exp={exp_name} ---")
                        safe_clean_all(quiet=True)
                        mig_data = run_migration_local_once(
                            scene,
                            run_port,
                            exp_args,
                            exp_name,
                            r + 1,
                            bench_duration=args.bench_duration,
                            bench_threads=args.bench_threads,
                        )
                        run_result = {'scene': scene, 'run': r + 1, 'exp': exp_name, 'ok': True}
                        if mig_data:
                            if mig_data.get('metrics'):
                                run_result['metrics'] = mig_data['metrics']
                            if mig_data.get('header'):
                                run_result['metrics_header'] = mig_data['header']
                            if mig_data.get('stats'):
                                run_result['metrics_values'] = mig_data['stats']
                            if mig_data.get('params'):
                                run_result['metric_params'] = mig_data['params']
                        results.append(run_result)
                        write_run_record('migration-local', scene, exp_name, r + 1, run_result)
                        port = run_port + 1
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
            ok = run_scene(scene, port, args.collect_baseline, args.skip_bench, args.keep_containers, local=args.local, bandwidth=args.bandwidth, simulate_transfer=args.simulate_transfer)
            run_result = {'scene': scene, 'run': r, 'ok': ok}
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
