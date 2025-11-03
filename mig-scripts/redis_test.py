import argparse
import re
import subprocess
import sys
import time
from typing import Optional
from result_writer import extract_stats_from_output, append_result
import shlex

from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs

# 默认设置
from script_defaults import choose_scripts, get_default_ips

SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
SEC_MODE = False
# default bandwidth
BANDWIDTH = "25mbit"
# 场景配置：Redis的video和sensor场景
scene_configs = {
    "video": {
        "bench": "experiment/migration/redis/bench_video_cache.py",
        "base_args": {
            "--redis-host": "192.168.2.100",
            "--redis-port": "6379",
            "--duration": "120",  #
            "--payload-size-kb": "1",
            # '--threads': '4',
            # '--duration': '10',
            # '--write-pct': '80',
            # '--ttl': '60'
        },
    },
    "sensor": {
        "bench": "experiment/migration/redis/bench_sensoragg.py",
        "base_args": {
            "--redis-host": "192.168.37.150",
            "--redis-port": "6379",
            "--payload-size-kb": "2",
            "--sensors-per-device": "10",
            "--read-pct": "0",
            "--duration": "90",  # 90s
            "--target-db-size-mb": "120",
        },
    },
    "vehicle": {
        "bench": "experiment/migration/redis/bench_cartelem.py",
        "base_args": {
            "--redis-host": "192.168.37.150",
            "--redis-port": "6379",
            # '--token': 'token',
            # '--org': 'org',
            # '--bucket': 'vehicle-data',
            # '--threads': '4',
            "--payload-size-kb": "2",
            # '--size-distribution':'normal',
            # '--vehicle-pattern': 'highway',
            "--duration": "90",
            "--target-db-size-mb": "1000",
            # '--read-pct': '0'
        },
    },
}



def destination_prepare():
    container_name = "redis"
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)

    recvtty_cmd = (
        f"PATH=$PATH:/root/go/bin recvtty -m null /runc/containers/{container_name}/console.sock "
        "> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_destination.pid"
    )
    run_remote_cmd(recvtty_cmd, DEST_IP, ignore_error=False)
    # 启动前，先尝试停止已存在的 destination 进程，避免端口占用
    dest_pidfile_pre = f"/tmp/destination_{container_name}.pid"
    stop_prev_cmd = (
        f"if [ -f {dest_pidfile_pre} ]; then kill -TERM $(cat {dest_pidfile_pre}) 2>/dev/null || true; rm -f {dest_pidfile_pre}; fi"
    )
    run_remote_cmd(stop_prev_cmd, DEST_IP, ignore_error=True)
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print $2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, DEST_IP, ignore_error=True)
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
    container_name = "redis"
    cmds = [
        (f"runc kill {container_name}", True),
        (f"runc delete {container_name}", True),
        ("kill -9 $(cat /tmp/recvtty_destination.pid) 2>/dev/null", True),
        ("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", True),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)
    # 停止destination后台进程并移除pid（如果存在）
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
    )
    run_remote_cmd(stop_cmd, DEST_IP, ignore_error=True)
    # 兜底：清理任何仍在运行的 destination 脚本
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print $2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, DEST_IP, ignore_error=True)


def source_prepare():
    container_name = "redis"
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        (
            (
                f"nohup recvtty -m null /runc/containers/{container_name}/console.sock "
                "> /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid"
            ),
            False,
        ),
        (
            (
                "runc run --console-socket "
                f"/runc/containers/{container_name}/console.sock -d -b /runc/containers/{container_name} "
                f"{container_name}"
            ),
            False,
        ),
    ]
    for c, ign in cmds:
        run_cmd(c, ignore_error=ign)


def source_clean():
    container_name = "redis"
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    unmount_local_migration_tmpfs(container_name)
    run_cmd(f"runc kill {container_name}", ignore_error=True)
    run_cmd(f"runc delete {container_name}", ignore_error=True)
    run_cmd("ps aux | grep 'inotifywait' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)
    run_cmd("ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)


def update_keepalived_priority(new_priority, is_remote=False, target_ip=None):
    """Adjust keepalived priority with concise logging."""

    config_path = "/etc/keepalived/keepalived.conf"
    pattern = r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})"
    prefix = "[vip]"
    location = "remote" if is_remote else "local"

    def modify_file(content: str) -> Optional[str]:
        match = re.search(pattern, content)
        if not match:
            print(f"{prefix} {location}: priority entry not found")
            return None
        original_priority = match.group(2)
        print(f"{prefix} {location}: priority {original_priority} -> {new_priority}")
        return re.sub(pattern, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", content, count=1)

    def restart_keepalived_remote(ip: str) -> bool:
        restart_cmd = "sudo systemctl restart keepalived"
        status_cmd = "sudo systemctl is-active keepalived"
        print(f"{prefix} remote: restart keepalived")
        result = subprocess.run(
            f"ssh {ip} '{restart_cmd}'",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            print(f"{prefix} remote: restart failed -> {result.stderr.strip()}")
            return False
        status = subprocess.run(
            f"ssh {ip} '{status_cmd}'",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if status.returncode == 0 and status.stdout.strip() == "active":
            print(f"{prefix} remote: keepalived active")
            return True
        print(f"{prefix} remote: keepalived not active -> {status.stderr.strip()}")
        return False

    def restart_keepalived_local() -> bool:
        restart_cmd = "sudo systemctl restart keepalived"
        status_cmd = "sudo systemctl is-active keepalived"
        print(f"{prefix} local: restart keepalived")
        result = subprocess.run(restart_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            print(f"{prefix} local: restart failed -> {result.stderr.strip()}")
            return False
        status = subprocess.run(status_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if status.returncode == 0 and status.stdout.strip() == "active":
            print(f"{prefix} local: keepalived active")
            return True
        print(f"{prefix} local: keepalived not active -> {status.stderr.strip()}")
        return False

    try:
        if is_remote:
            if not target_ip:
                raise ValueError("target_ip is required when is_remote=True")
            print(f"{prefix} remote: read {config_path}")
            result = subprocess.run(
                f"ssh {target_ip} 'cat {config_path}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                print(f"{prefix} remote: read failed -> {result.stderr.strip()}")
                return False
            updated = modify_file(result.stdout)
            if updated is None:
                return False
            print(f"{prefix} remote: write {config_path}")
            write_result = subprocess.run(
                f"ssh {target_ip} \"cat <<'EOF' > {config_path}\n{updated}\nEOF\"",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if write_result.returncode != 0:
                print(f"{prefix} remote: write failed -> {write_result.stderr.strip()}")
                return False
            if not restart_keepalived_remote(target_ip):
                return False
        else:
            print(f"{prefix} local: read {config_path}")
            with open(config_path, "r", encoding="utf-8") as f:
                content = f.read()
            updated = modify_file(content)
            if updated is None:
                return False
            print(f"{prefix} local: write {config_path}")
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(updated)
            if not restart_keepalived_local():
                return False
        print(f"{prefix} {location}: priority update done")
        return True
    except Exception as exc:
        print(f"{prefix} {location}: error -> {exc}")
        return False


def clean_configure_network():
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True, quiet=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", DEST_IP, ignore_error=True, quiet=True)
    if CLIENT_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", CLIENT_IP, ignore_error=True, quiet=True)


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    cleanup_cmd = f"sudo tc qdisc del dev {interface} root"
    init_cmd = f"sudo tc qdisc add dev {interface} root handle 1: htb"
    remote_ip = str(target_ip) if target_ip else None

    if remote_ip:
        print(f"[net] clearing rules on remote:{remote_ip} {interface}")
        run_remote_cmd(cleanup_cmd, remote_ip, ignore_error=True, quiet=True)
        run_remote_cmd(init_cmd, remote_ip, ignore_error=ignore_error, quiet=True)
    else:
        print(f"[net] clearing rules on local {interface}")
        run_cmd(cleanup_cmd, ignore_error=True, quiet=True)
        run_cmd(init_cmd, ignore_error=ignore_error, quiet=True)

    for idx, rule in enumerate(rules, start=1):
        classid = f"1:{idx}"
        handle = f"{10 * idx}:"
        rate = rule["rate"]
        delay = rule["delay"]
        dst = rule["dst"]
        location = f"remote:{remote_ip}" if remote_ip else "local"
        print(f"[net] rule#{idx} on {location}: dst={dst} rate={rate} delay={delay}")

        cmds = [
            f"sudo tc class add dev {interface} parent 1: classid {classid} htb rate {rate}",
            f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {dst} flowid {classid}",
            f"sudo tc qdisc add dev {interface} parent {classid} handle {handle} netem delay {delay}",
        ]

        for cmd in cmds:
            if remote_ip:
                run_remote_cmd(cmd, remote_ip, ignore_error=ignore_error, quiet=True)
            else:
                run_cmd(cmd, ignore_error=ignore_error, quiet=True)


def configure_network():
    source_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP}]
    if CLIENT_IP:
        source_rules.append({"rate": BANDWIDTH, "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": BANDWIDTH, "delay": "0.05ms", "dst": CLIENT_IP})

    configure_network_do(interface="enp2s0", rules=source_rules, is_remote=False)
    configure_network_do(interface="enp2s0", rules=dest_rules, is_remote=True, target_ip=DEST_IP)
    if CLIENT_IP:
        client_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": VIP}]
        configure_network_do(interface="ens33", rules=client_rules, is_remote=True, target_ip=CLIENT_IP)


# 定义实验类型
experiments = {
    # "post-copy": "-post -d --tcp-established --shell-job",
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-1": "-pre -d --tcp-established --shell-job -z 1",
    "pre-copy-2": "-pre -d --tcp-established --shell-job -z 2",
    "pre-copy-3": "-pre -d --tcp-established --shell-job -z 3",
    "pre-copy-4": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}


def source_run_migration(exp_args, scene_config, extra_args, scene, run_index=0, exp_name="unknown"):
    container_name = "redis"
    time.sleep(6)  # 等待容器启动稳定

    print("Running bench test on the client machine...")
    run_remote_cmd('pkill -f "python.*bench"', CLIENT_IP, ignore_error=True)
    run_remote_cmd("rm -f /tmp/bench_client.pid /tmp/bench_run.log || true", CLIENT_IP, ignore_error=True)

    # 设置环境变量并执行bench（load）
    bench_dir = "/runc/dirty-track/experiment/migration/redis"
    bench_file = scene_config["bench"].split("/")[-1]

    def args_to_str(d):
        return " ".join(f"{k} {v}" for k, v in d.items() if v is not None and v != "")

    # --- load 阶段：用 scene_configs 里的 base_args ---
    if scene != "video":
        load_args = extra_args.copy()
        load_cmd = f"cd {bench_dir} && python3 {bench_file} {args_to_str(load_args)}"
        run_remote_cmd(load_cmd, CLIENT_IP)
    # load_args = extra_args.copy()
    # load_cmd = f"cd {bench_dir} && python3 {bench_file} {args_to_str(load_args)}"
    # run_remote_cmd(load_cmd, CLIENT_IP)

    # 在load和run之间设置网络配置
    configure_network()
    print("Network configuration applied between bench load and run.")

    # --- run 阶段：覆盖 payload-size-kb / sensors-per-device ---
    run_args = extra_args.copy()
    if scene == "sensor":
        run_args["--payload-size-kb"] = "4"
        run_args["--sensors-per-device"] = "15"
        run_args["--duration"] = "240"
        # run_args['--rps'] = '100'
    if scene == "vehicle":
        run_args["--payload-size-kb"] = "4"
        run_args["--size-distribution"] = "normal"
        run_args["--vehicle-pattern"] = "highway"

    # run
    run_bg_cmd = (
        f"cd {bench_dir} && "
        f"nohup python3 {bench_file} {args_to_str(run_args)} "
        "> /tmp/bench_run.log 2>&1 & echo $! > /tmp/bench_client.pid"
    )
    run_remote_cmd(run_bg_cmd, CLIENT_IP, ignore_error=False)
    time.sleep(3)

    # 执行 source 脚本进行迁移（支持 secure 变体）
    # 使用参数列表调用本地 python 以避免不必要的 shell=True
    cmd_list = ["python3", SOURCE_SCRIPT] + shlex.split(exp_args) + [container_name, DEST_IP]
    print("Running migration:", " ".join(cmd_list))
    result = run_cmd(cmd_list, quiet=True)
    stdout = getattr(result, "stdout", "") or ""
    if stdout:
        print(stdout.rstrip())

    # 尝试从 stdout 中提取统计行并写入 results
    try:
        header, stats = extract_stats_from_output(stdout)
        if stats:
            # 生成 workload 概览（scene 单独在主行；load/run 各一行）
            def args_to_str(d):
                return " ".join(f"{k} {v}" for k, v in d.items() if v is not None and v != "")
            params_summary = f"exp: {exp_args} | scene={scene}"
            extra_lines = [
                f"workload-load: {args_to_str(extra_args)}",
                f"workload-run: {args_to_str(run_args)}",
            ]
            append_result(
                exp_name,
                "redis",
                run_index,
                stats,
                header,
                params_summary,
                is_secure=SEC_MODE,
                extra_param_lines=extra_lines,
            )
            print(f"Wrote stats for {exp_name} run {run_index} -> results/{exp_name}.tsv")
        else:
            print("No statistics line found in source output; skipping result write.")
    except Exception as e:
        print(f"Error writing stats: {e}")

    # clean

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

    cleanup_cmd = "kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true"
    run_cmd(cleanup_cmd, ignore_error=False)


def main():
    # 使用参数值更新全局变量
    global SOURCE_IP, DEST_IP, CLIENT_IP, SOURCE_SCRIPT, DEST_SCRIPT
    parser = argparse.ArgumentParser(description="Redis自动化负载测试脚本")
    parser.add_argument("-s", "--source-ip", default=SOURCE_IP, help="迁移源IP")
    parser.add_argument("-d", "--dest-ip", default=DEST_IP, help="迁移目标IP")
    parser.add_argument("-c", "--client-ip", default=CLIENT_IP, help="客户端IP")
    parser.add_argument(
        "--sec", action="store_true", help="use secure source/destination scripts (source-sec.py / destination-sec.py)"
    )
    parser.add_argument("--scene", choices=["video", "sensor", "vehicle"], required=True, help="场景: video或sensor")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis端口")
    parser.add_argument("--threads", type=int, help="线程数")
    parser.add_argument("--duration", type=int, help="测试时长(s)")
    parser.add_argument("--write-pct", type=int, help="写操作百分比 (video场景)")
    parser.add_argument("--ttl", type=int, help="TTL (video场景)")
    parser.add_argument("--payload-size-kb", type=int, help="负载大小(KB)")
    parser.add_argument("--sensors-per-device", type=int, help="每设备传感器数")
    parser.add_argument("--bandwidth", default="25mbit", help="Network bandwidth limit (e.g. 25mbit). Default: 25mbit")
    parser.add_argument("--runs", type=int, default=5, help="每个实验类型的运行次数")
    parser.add_argument(
        "--experiment-types",
        nargs="*",
        choices=list(experiments.keys()),
        default=list(experiments.keys()),
        help="要运行的迁移实验类型，默认全部",
    )
    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    # 根据 --sec 切换为 secure 变体
    # set bandwidth
    globals()["BANDWIDTH"] = getattr(args, "bandwidth", BANDWIDTH)
    sec_enabled = getattr(args, "sec", False)
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(sec_enabled)
    globals()["SEC_MODE"] = sec_enabled
    print(f"[sec] mode={'on' if sec_enabled else 'off'} using DEST_SCRIPT={DEST_SCRIPT}, SOURCE_SCRIPT={SOURCE_SCRIPT}")

    experiment_types_to_run = args.experiment_types if args.experiment_types else list(experiments.keys())

    # 主流程，支持多个实验类型和循环
    for exp_name in experiment_types_to_run:
        exp_args = experiments[exp_name]
        print(f"================ Running '{exp_name}' experiment ===============")

        for run_num in range(1, args.runs + 1):
            print(f"-------- Experiment {exp_name}, run {run_num} --------")
            try:
                # 确保本轮开始前未残留任何限速规则，避免影响 load 阶段
                clean_configure_network()
                # 环境准备（总是执行）
                print("Preparing destination and source environments...")
                destination_prepare()
                source_prepare()
                # 可选网络配置
                # configure_network()
                # 可选keepalived
                # update_keepalived_priority(70)
                # update_keepalived_priority(30, True, DEST_IP)

                scene_config = scene_configs[args.scene]

                # 构建额外参数
                # extra_args = {
                #     '--redis-host': args.redis_host,
                #     '--redis-port': args.redis_port,
                #     '--threads': args.threads,
                #     '--duration': args.duration,
                # }

                # 从场景配置拷贝一份默认参数
                extra_args = scene_config["base_args"].copy()

                # 如果命令行传了参数，就覆盖默认值
                # if args.threads is not None:
                #     extra_args['--threads'] = str(args.threads)
                # if args.duration is not None:
                #     extra_args['--duration'] = str(args.duration)

                # if args.scene == 'video':
                #     extra_args['--write-pct'] = args.write_pct
                #     extra_args['--ttl'] = args.ttl
                # elif args.scene == 'sensor':
                #     extra_args['--payload-size-kb'] = args.payload_size_kb
                #     extra_args['--sensors-per-device'] = args.sensors_per_device

                # 执行迁移（包含bench测试）
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
                # clean_configure_network()
                # 更新keepalived
                # update_keepalived_priority(70)
                # update_keepalived_priority(30, True, DEST_IP)

                time.sleep(5)  # 等待清理缓冲

        print(f"================ Finished {exp_name} experiment ===============")

    print("All experiments completed.")


if __name__ == "__main__":
    import atexit
    # 进程退出时做一次最终网络清理（统一善后）
    atexit.register(clean_configure_network)
    main()
