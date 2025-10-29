#!/usr/bin/env python3
# coding: utf-8
"""
influxdb_test.py - InfluxDB自动化负载测试脚本

为InfluxDB的video, sensor, vehicle场景提供自动化测试，支持类似redis.py的循环运行。
支持参数配置传递给bench脚本。

使用：
  python influxdb_test.py --scene video --threads 8 --duration 30 --runs 3
  python influxdb_test.py --scene sensor --threads 4 --duration 60
  python influxdb_test.py --scene vehicle --threads 4 --duration 60 --experiment-types post-copy

配置在scene_configs字典中定义各场景的bench文件和默认参数。
"""

import argparse
import re
import subprocess
import sys
import time
from result_writer import extract_stats_from_output, append_result

# 默认设置
from script_defaults import choose_scripts, get_default_ips

SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
# default bandwidth (can be overridden by --bandwidth)
BANDWIDTH = "25mbit"

# default script to call; can be switched to source-sec.py via --sec
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)

# 定义实验类型
experiments = {
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-1": "-pre -d --tcp-established --shell-job -z 1",
    "pre-copy-2": "-pre -d --tcp-established --shell-job -z 2",
    "pre-copy-3": "-pre -d --tcp-established --shell-job -z 3",
    "pre-copy-4": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "post-copy": "-post -d --tcp-established --shell-job"
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}

# 场景配置：InfluxDB的video, sensor, vehicle场景
scene_configs = {
    "sensor": {
        "bench": "experiment/migration/influxdb/bench_sensoragg.py",
        "base_args": {
            "--influx-url": "http://192.168.2.100:8181",
            # '--token': 'token',
            # '--org': 'org',
            # '--bucket': 'sensor-data',
            # '--threads': '4',
            "--payload-size-kb": "2",
            "--sensors-per-device": "10",
            "--read-pct": "0",
            "--duration": "120",
        },
    },
    "vehicle": {
        "bench": "experiment/migration/influxdb/bench_cartelem.py",
        "base_args": {
            "--influx-url": "http://192.168.2.100:8181",
            # '--token': 'token',
            # '--org': 'org',
            # '--bucket': 'vehicle-data',
            # '--threads': '4',
            "--payload-size-kb": "2",
            # '--size-distribution':'normal',
            # '--vehicle-pattern': 'highway',
            "--duration": "120",
            "--read-pct": "0",
            # '--target-db-size-mb':'120'
            "--retention-policy": "60s",
        },
    },
}


def run_cmd(cmd, ignore_error=False):
    print("Executing:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    print(result.stdout)
    return result


def run_remote_cmd(cmd, target_ip, ignore_error=False, background=False):
    """Execute command on remote machine."""
    if background:
        cmd = f"nohup {cmd} > /dev/null 2>&1 & echo $!"

    full_cmd = f"ssh {target_ip} '{cmd}'"
    print("Executing remotely:", end=" ")
    print(f"(on {target_ip}):", cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    print(result.stdout)
    return result


def destination_prepare():
    container_name = "influxdb"  # 假设InfluxDB容器名为influxdb
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)

    recvtty_cmd = (
        f"PATH=$PATH:/root/go/bin /root/go/bin/recvtty -m single "
        f"/runc/containers/{container_name}/console.sock "
        "> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_destination.pid"
    )
    run_remote_cmd(recvtty_cmd, DEST_IP, ignore_error=False)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp（可通过 --sec 切换）
    ts = int(time.time())
    dest_log = f"/tmp/{DEST_SCRIPT.replace('.', '_')}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    start_dest_cmd = (
        f"nohup python3 /runc/dirty-track/mig-scripts/{DEST_SCRIPT} > {dest_log} "
        "2>&1 & echo $! > "
        f"{dest_pidfile}"
    )
    run_remote_cmd(start_dest_cmd, DEST_IP, ignore_error=False)
    # 等待目标端写入 pidfile
    # 等待目标端写入 pidfile（指数退避，最多 10 次）
    wait = 0.5
    max_attempts = 10
    for attempt in range(max_attempts):
        res = run_remote_cmd(f"test -f {dest_pidfile}", DEST_IP, ignore_error=True)
        if getattr(res, "returncode", 1) == 0:
            break
        time.sleep(wait)
        wait = min(wait * 2, 5)
    else:
        print(f"Warning: destination pidfile {dest_pidfile} not found on {DEST_IP} after wait")
        _ = run_remote_cmd(f"ls -l {dest_pidfile} || true", DEST_IP, ignore_error=True)
    print(f"Started remote destination on {DEST_IP}, log: {dest_log}, pidfile: {dest_pidfile}")


def destination_clean():
    container_name = "influxdb"
    cmds = [
        (f"runc kill {container_name}", True),
        (f"runc delete {container_name}", True),
        ("kill -9 $(cat /tmp/recvtty_destination.pid) 2>/dev/null", True),
        ("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", True),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)
    # 停止 destination 后台进程并移除 pidfile（如果存在）
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
    )
    run_remote_cmd(stop_cmd, DEST_IP, ignore_error=True)


def source_prepare():
    container_name = "influxdb"
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        (
            (
                f"nohup recvtty -m single /runc/containers/{container_name}/console.sock "
                "> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_source.pid"
            ),
            False,
        ),
        (
            (
                "runc run --console-socket "
                f"/runc/containers/{container_name}/console.sock "
                f"-d -b /runc/containers/{container_name} {container_name}"
            ),
            False,
        ),
    ]
    for c, ign in cmds:
        run_cmd(c, ignore_error=ign)


def source_clean():
    container_name = "influxdb"
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    run_cmd(f"umount /runc/containers/{container_name}/migrate/*", ignore_error=True)
    run_cmd(f"runc kill {container_name}", ignore_error=True)
    run_cmd(f"runc delete {container_name}", ignore_error=True)
    run_cmd("ps aux | grep 'inotifywait' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)
    run_cmd("ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)


def update_keepalived_priority(new_priority, is_remote=False, target_ip=None):
    config_path = "/etc/keepalived/keepalived.conf"
    pattern = r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})"

    restart_cmd = "sudo systemctl restart keepalived"
    status_cmd = "sudo systemctl is-active keepalived"

    try:
        if is_remote:
            if not target_ip:
                raise ValueError("Target IP required for remote")
            result = subprocess.run(
                f"ssh {target_ip} 'cat {config_path}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                print("Failed to read remote config:", result.stderr)
                return False
            content = result.stdout
            updated_content = re.sub(pattern, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", content)
            write_result = subprocess.run(
                f"ssh {target_ip} \"echo '{updated_content}' > {config_path}\"",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if write_result.returncode != 0:
                print("Failed to write remote config:", write_result.stderr)
                return False
            result = subprocess.run(
                f"ssh {target_ip} '{restart_cmd}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                print("Failed to restart remote keepalived:", result.stderr)
                return False
            status = subprocess.run(
                f"ssh {target_ip} '{status_cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if status.returncode == 0 and status.stdout.strip() == "active":
                print("Keepalived restarted successfully on remote.")
                return True
            else:
                print("Keepalived failed to restart on remote.")
                return False
        else:
            with open(config_path, "r") as f:
                content = f.read()
            updated_content = re.sub(pattern, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", content)
            with open(config_path, "w") as f:
                f.write(updated_content)
            result = subprocess.run(restart_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print("Failed to restart local keepalived:", result.stderr)
                return False
            status = subprocess.run(status_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if status.returncode == 0 and status.stdout.strip() == "active":
                print("Keepalived restarted successfully locally.")
                return True
            else:
                print("Keepalived failed to restart locally.")
                return False
    except Exception as e:
        print(f"Error updating keepalived: {e}")
        return False


def clean_configure_network():
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", DEST_IP, ignore_error=True)
    if CLIENT_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", CLIENT_IP, ignore_error=True)


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """配置网络规则，使用 tc 命令设置带宽和延迟"""
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    base_cmds = [
        f"sudo tc qdisc add dev {interface} root handle 1: htb",
    ]

    for idx, rule in enumerate(rules, start=1):
        classid = f"1:{idx}"
        handle = f"{10 * idx}:"
        rate = rule["rate"]
        delay = rule["delay"]
        dst = rule["dst"]

        base_cmds.extend(
            [
                f"sudo tc class add dev {interface} parent 1: classid {classid} htb rate {rate}",
                f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 "
                f"match ip dst {dst} flowid {classid}",
                f"sudo tc qdisc add dev {interface} parent {classid} handle {handle} netem delay {delay}",
            ]
        )

    for cmd in base_cmds:
        print(f"Executing network config: {cmd}")
        if is_remote:
            run_remote_cmd(cmd, target_ip=target_ip, ignore_error=ignore_error)
        else:
            run_cmd(cmd, ignore_error=ignore_error)


def configure_network():
    """配置网络限制"""
    source_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP}]
    if CLIENT_IP:
        source_rules.append({"rate": BANDWIDTH, "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": BANDWIDTH, "delay": "0.05ms", "dst": CLIENT_IP})

    # 配置source的网络限制
    configure_network_do(interface="enp2s0", rules=source_rules, is_remote=False)  # 本地执行

    # 配置dest的网络限制
    configure_network_do(interface="enp2s0", rules=dest_rules, is_remote=True, target_ip=DEST_IP)  # 远程执行

    # 配置client的网络限制
    if CLIENT_IP:
        configure_network_do(
            interface="ens33",
            rules=[
                {"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": BANDWIDTH, "delay": "0.05ms", "dst": DEST_IP},
            ],
            is_remote=True,  # 远程执行
            target_ip=CLIENT_IP,
        )


def source_run_migration(exp_args, scene_config, extra_args, scene, run_index=0, exp_name="unknown"):
    container_name = "influxdb"
    time.sleep(6)  # 等待容器启动稳定

    print("Running bench test on the client machine...")
    run_remote_cmd('pkill -f "python.*bench"', CLIENT_IP, ignore_error=True)
    run_remote_cmd("rm -f /tmp/bench_client.pid /tmp/bench_run.log || true", CLIENT_IP, ignore_error=True)

    # 设置环境变量并执行bench（load）
    bench_dir = "/runc/dirty-track/experiment/migration/influxdb"
    bench_file = scene_config["bench"].split("/")[-1]

    def args_to_str(d):
        return " ".join(f"{k} {v}" for k, v in d.items() if v is not None and v != "")

    # --- load 阶段：用 scene_configs 里的 base_args ---
    load_args = extra_args.copy()
    load_cmd = f"cd {bench_dir} && python3 {bench_file} {args_to_str(load_args)}"
    run_remote_cmd(load_cmd, CLIENT_IP)

    # 在load和run之间设置网络配置
    configure_network()
    print("Network configuration applied between bench load and run.")

    # --- run 阶段：覆盖 payload-size-kb / sensors-per-device ---
    run_args = extra_args.copy()
    if scene == "sensor":
        run_args["--payload-size-kb"] = "4"  # ★ 你要的新值
        run_args["--sensors-per-device"] = "15"  # ★ 你要的新值\

    if scene == "vehicle":
        run_args["--payload-size-kb"] = "4"  # ★ 你要的新值
        run_args["--size-distribution"] = "normal"  #
        run_args["--vehicle-pattern"] = "highway"

    # run
    run_bg_cmd = (
        f"cd {bench_dir} && "
        f"nohup python3 {bench_file} {args_to_str(run_args)} "
        "> /tmp/bench_run.log 2>&1 & echo $! > /tmp/bench_client.pid"
    )
    run_remote_cmd(run_bg_cmd, CLIENT_IP, ignore_error=False)

    time.sleep(3)  # 等待bench启动稳定

    # 执行 source 脚本进行迁移（可通过 --sec 切换到 source-sec.py）
    migration_cmd = f"python3 {SOURCE_SCRIPT} {exp_args} {container_name} {DEST_IP}"
    result = run_cmd(migration_cmd)

    # 尝试从 stdout 中提取统计行并写入 results
    try:
        stdout = getattr(result, "stdout", "") or ""
        stats = extract_stats_from_output(stdout)
        if stats:
            append_result(exp_name, "influxdb", run_index, stats)
            print(f"Wrote stats for {exp_name} run {run_index} -> results/{exp_name}.tsv")
        else:
            print("No statistics line found in source output; skipping result write.")
    except Exception as e:
        print(f"Error writing stats: {e}")

    # ---------- 5) 迁移后清理后台 bench ----------
    # 先温柔 SIGTERM，再强制 SIGKILL（避免残留）
    kill_bg = (
        "if [ -f /tmp/bench_client.pid ]; then "
        "  PID=$(cat /tmp/bench_client.pid) 2>/dev/null; "
        '  if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then '
        "    kill $PID 2>/dev/null || true; "
        "    sleep 0.5; "
        "    kill -9 $PID 2>/dev/null || true; "
        "  fi; "
        "fi; "
        "rm -f /tmp/bench_client.pid"
    )
    run_remote_cmd(kill_bg, CLIENT_IP, ignore_error=True)
    # clean
    cleanup_cmd = "kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true"
    run_cmd(cleanup_cmd, ignore_error=False)


def run_migration(experiment_args, container_name):
    """运行迁移命令"""
    migration_cmd = f"python3 {SOURCE_SCRIPT} {experiment_args} {container_name} {DEST_IP}"
    print(f"Running migration: {migration_cmd}")
    result = subprocess.run(migration_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"Migration failed: {result.stderr}")
        return False
    print("Migration completed successfully.")
    return True


def main():
    # 使用参数值更新全局变量
    global SOURCE_IP, DEST_IP, CLIENT_IP, BANDWIDTH
    parser = argparse.ArgumentParser(description="InfluxDB自动化负载测试脚本")
    parser.add_argument("-s", "--source-ip", default=SOURCE_IP, help="迁移源IP")
    parser.add_argument("-d", "--dest-ip", default=DEST_IP, help="迁移目标IP")
    parser.add_argument("-c", "--client-ip", default=CLIENT_IP, help="客户端IP")
    parser.add_argument(
        "--scene", choices=["video", "sensor", "vehicle"], required=True, help="场景: video, sensor, vehicle"
    )
    # parser.add_argument("--influx-token", default='token', help="InfluxDB token")
    # parser.add_argument("--org", default='org', help="InfluxDB org")
    parser.add_argument("--bucket", help="InfluxDB bucket (默认根据场景设置)")
    parser.add_argument("--threads", type=int, help="线程数")
    parser.add_argument("--duration", type=int, help="测试时长(s)")
    parser.add_argument("--payload-size-kb", type=int, help="负载大小(KB)")
    parser.add_argument("--read-pct", type=int, help="读操作百分比")
    # Scene特有参数
    parser.add_argument("--objects-per-frame", type=int, help="每帧对象数 (video场景)")
    parser.add_argument("--sensors-per-device", type=int, help="每设备传感器数 (sensor场景)")
    parser.add_argument(
        "--vehicle-pattern", choices=["normal_city", "highway", "stop_go"], help="车辆模式 (vehicle场景)"
    )
    parser.add_argument("--runs", type=int, default=5, help="每个实验类型的运行次数")
    parser.add_argument(
        "--experiment-types",
        nargs="*",
        choices=list(experiments.keys()),
        default=list(experiments.keys()),
        help="要运行的实验类型，默认全部",
    )
    parser.add_argument("--sec", action="store_true", help="use source-sec/destination-sec scripts")
    parser.add_argument("--bandwidth", default="25mbit", help="Network bandwidth limit (e.g. 25mbit). Default: 25mbit")

    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    BANDWIDTH = args.bandwidth

    # set script selection
    global SOURCE_SCRIPT
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(args.sec)

    # 设置场景特有bucket
    if not args.bucket:
        if args.scene == "video":
            args.bucket = "data"
        elif args.scene == "sensor":
            args.bucket = "sensor-data"
        elif args.scene == "vehicle":
            args.bucket = "vehicle-data"

    experiment_types_to_run = args.experiment_types if args.experiment_types else list(experiments.keys())

    # 主流程，支持多个实验类型和循环
    for exp_name in experiment_types_to_run:
        exp_args = experiments[exp_name]
        print(f"================ Running InfluxDB '{exp_name}' experiment ===============")

        for run_num in range(1, args.runs + 1):
            print(f"-------- Experiment {exp_name}, run {run_num} --------")
            try:
                # 环境准备（总是执行）
                print("Preparing destination and source environments...")
                destination_prepare()
                source_prepare()

                # 获取场景配置
                scene_config = scene_configs[args.scene]

                # 构建额外参数，使用场景配置的默认值并覆盖用户指定参数
                extra_args = {}
                for key, value in scene_config["base_args"].items():
                    extra_args[key] = value

                # 覆盖特定参数 - 更新默认bucket覆盖
                extra_args["--bucket"] = args.bucket

                if args.scene == "video":
                    if args.threads:
                        extra_args["--threads"] = str(args.threads)
                    if args.duration:
                        extra_args["--duration"] = str(args.duration)
                    if args.payload_size_kb:
                        extra_args["--payload-size-kb"] = str(args.payload_size_kb)
                    if args.objects_per_frame:
                        extra_args["--objects-per-frame"] = str(args.objects_per_frame)
                elif args.scene == "sensor":
                    if args.threads:
                        extra_args["--threads"] = str(args.threads)
                    if args.duration:
                        extra_args["--duration"] = str(args.duration)
                    if args.payload_size_kb:
                        extra_args["--payload-size-kb"] = str(args.payload_size_kb)
                    if args.read_pct:
                        extra_args["--read-pct"] = str(args.read_pct)
                    if args.sensors_per_device:
                        extra_args["--sensors-per-device"] = str(args.sensors_per_device)
                elif args.scene == "vehicle":
                    if args.threads:
                        extra_args["--threads"] = str(args.threads)
                    if args.duration:
                        extra_args["--duration"] = str(args.duration)
                    if args.payload_size_kb:
                        extra_args["--payload-size-kb"] = str(args.payload_size_kb)
                    if args.read_pct:
                        extra_args["--read-pct"] = str(args.read_pct)
                    if args.vehicle_pattern:
                        extra_args["--vehicle-pattern"] = args.vehicle_pattern

                # 执行bench和迁移
                source_run_migration(exp_args, scene_config, extra_args, args.scene, run_num, exp_name)
                print(f"Bench test and migration completed successfully for {exp_name} run {run_num}.")

                print(f"Experiment {exp_name}, run {run_num} completed.")

            except Exception as e:
                print(f"Error during {exp_name} run {run_num}: {e}")
                import traceback

                traceback.print_exc()

            finally:
                # 清理资源（总是清理）
                print("Cleaning up resources...")
                destination_clean()
                source_clean()
                # 清理网络配置
                clean_configure_network()

            time.sleep(17)  # 等待清理缓冲

        print(f"================ Finished InfluxDB {exp_name} experiment ===============")

    print("All InfluxDB experiments completed.")


if __name__ == "__main__":
    main()
