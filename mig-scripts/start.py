import argparse
import os
import re
import shlex
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional

from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs
from result_writer import append_result, extract_stats_from_output

# 默认设置
from script_defaults import choose_scripts, get_default_ips

SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)

# default bandwidth
BANDWIDTH = "25mbit"


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


def source_prepare(args):
    container_name = args.container
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                f"nohup recvtty -m single /runc/containers/{container_name}/console.sock "
                "> /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid"
            ),
            False,
        ),
        # 启动容器
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

def source_run_migration(args, exp_args, run_index: int, exp_name: str):
    container_name = args.container
    # 开启工具测试 (后台)
    run_remote_cmd(
        "wrk -t4 -c50 -d120s --timeout 10s http://192.168.2.100/",
        target_ip=CLIENT_IP,
        ignore_error=True,
        background=True,
    )

    time.sleep(3)  # 等待bench启动稳定
    # 执行 source 脚本进行迁移 (可切换为 secure 变体)，并捕获输出
    result = run_cmd(f"python3 {SOURCE_SCRIPT} {container_name} {DEST_IP} {exp_args}")

    # 从 stdout 提取统计行并写入 results 文件
    stdout = getattr(result, "stdout", "") or ""
    header, stats = extract_stats_from_output(stdout)
    if stats:
        try:
            append_result(exp_name, container_name, run_index, stats, header, exp_args, is_secure=getattr(args, "sec", False))
            print(f"Wrote stats for {exp_name} run {run_index} -> results/{exp_name}.tsv")
        except Exception as e:
            print(f"Failed to write stats file: {e}")
    else:
        print("No statistics line found in source output; skipping result write.")


def source_clean(args):
    container_name = args.container
    # 清理 console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true", ignore_error=True, quiet=True)
    # 清理 dirtypages 的挂载
    unmount_local_migration_tmpfs(container_name)
    run_cmd(f"runc kill {container_name}", ignore_error=True, quiet=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True, quiet=True)  # 如果容器不存在可忽略错误
    run_cmd(
        "ps aux | grep 'inotifywait' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9",
        ignore_error=True,
        quiet=True,
    )
    run_cmd(
        "ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9",
        ignore_error=True,
        quiet=True,
    )


def destination_prepare(args):
    # 在目标节点执行准备操作
    # 有些命令可能出现非致命错误，用ignore_error=True
    container_name = args.container
    cmds = [
        # 准备新的bundle,
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                f"nohup  /root/go/bin/recvtty -m single /runc/containers/{container_name}/console.sock  "
                "> /dev/null 2>&1 & echo $! > /tmp/recvtty.pid"
            ),
            False,
        ),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)
    # 启动前，先尝试停止已存在的 destination 进程，避免端口占用
    dest_pidfile_pre = f"/tmp/destination_{container_name}.pid"
    stop_prev_cmd = (
        f"if [ -f {dest_pidfile_pre} ]; then kill -TERM $(cat {dest_pidfile_pre}) 2>/dev/null || true; rm -f {dest_pidfile_pre}; fi"
    )
    run_remote_cmd(stop_prev_cmd, target_ip=DEST_IP, ignore_error=True, quiet=True)
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print \\$2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, target_ip=DEST_IP, ignore_error=True, quiet=True)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp
    ts = int(time.time())
    dest_log = f"/tmp/{DEST_SCRIPT.replace('.', '_')}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    # 使用仓库中的脚本完整路径，避免远程默认工作目录导致找不到脚本
    capture_dir = os.environ.get("DT_CAPTURE_DIR")
    env_prefix = ""
    if capture_dir:
        env_prefix = f"DT_CAPTURE_DIR={shlex.quote(capture_dir)} "

    start_dest_cmd = (
        f"{env_prefix}nohup python3 /runc/dirty-track/mig-scripts/{DEST_SCRIPT} > {dest_log} "
        "2>&1 & echo $! > "
        f"{dest_pidfile}"
    )
    run_remote_cmd(start_dest_cmd, target_ip=DEST_IP, ignore_error=False, background=False)
    # 等待目标端写入 pidfile（指数退避，最多 10 次）
    wait = 0.5
    max_attempts = 10
    for attempt in range(max_attempts):
        res = run_remote_cmd(f"test -f {dest_pidfile}", target_ip=DEST_IP, ignore_error=True)
        if getattr(res, "returncode", 1) == 0:
            break
        time.sleep(wait)
        wait = min(wait * 2, 5)
    else:
        print(f"Warning: destination pidfile {dest_pidfile} not found on {DEST_IP} after wait")
        _ = run_remote_cmd(f"ls -l {dest_pidfile} || true", target_ip=DEST_IP, ignore_error=True)
    print(f"Started remote destination on {DEST_IP}, log: {dest_log}, pidfile: {dest_pidfile}")


def destination_clean(args):
    # 恢复过程结束时杀死 recvtty 与临时监听
    container_name = args.container

    cmds = [
        (f"runc kill {container_name}", True),  # 如果容器不存在可忽略错误
        (f"runc delete {container_name}", True),  # 如果容器不存在可忽略错误
            ("kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null || true", True),  # 杀死 recvtty 进程
            ("ps aux | grep 'recvtty' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9", True),
    ]

    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign, quiet=True)

    # 兜底直接 pkill nc 监听，确保不遗留还有后台进程

    # 停止 destination 后台进程（如果存在）并移除 pid 文件
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
    )
    run_remote_cmd(stop_cmd, target_ip=DEST_IP, ignore_error=True, quiet=True)
    # 兜底清理任何遗留的 destination 脚本
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print \\$2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, target_ip=DEST_IP, ignore_error=True, quiet=True)


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """
    配置网络规则，使用 `tc` 命令设置带宽和延迟，支持本地和远程执行。

    参数:
    - interface: 网络接口名称，例如 "ens33"。
    - rules: 规则列表，每个规则是一个字典，包含:
        - `rate`: 带宽限制，例如 "100mbit"。
        - `delay`: 延迟，例如 "3ms"。
        - `dst`: 目标 IP 地址，例如 "192.168.37.157"。
    - is_remote: 是否在远程机器上执行 (默认: False)。
    - target_ip: 远程目标机器的 IP 地址 (仅在 is_remote=True 时有效)。
    - ignore_error: 是否忽略错误 (默认: False)。
    """
    cleanup_cmd = f"sudo tc qdisc del dev {interface} root"
    init_cmd = f"sudo tc qdisc add dev {interface} root handle 1: htb"
    remote_ip = None

    if is_remote:
        if not target_ip:
            raise ValueError("Target IP must be provided for remote execution.")
        remote_ip = str(target_ip)
        print(f"[net] clearing rules on remote:{remote_ip} {interface}")
        run_remote_cmd(cleanup_cmd, target_ip=remote_ip, ignore_error=True, quiet=True)
        run_remote_cmd(init_cmd, target_ip=remote_ip, ignore_error=ignore_error, quiet=True)
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
                run_remote_cmd(cmd, target_ip=remote_ip, ignore_error=ignore_error, quiet=True)
            else:
                run_cmd(cmd, ignore_error=ignore_error, quiet=True)


def clean_configure_network():
    # 清空网络配置
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True, quiet=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", target_ip=DEST_IP, ignore_error=True, quiet=True)
    if VIP_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", target_ip=CLIENT_IP, ignore_error=True, quiet=True)


def configure_network():
    source_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP}]
    if VIP_IP:
        source_rules.append({"rate": BANDWIDTH, "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": BANDWIDTH, "delay": "0.05ms", "dst": CLIENT_IP})

    # 配置source的网络限制  source->dest  source->client
    configure_network_do(interface="enp2s0", rules=source_rules, is_remote=False)  # 本地执行
    # 配置dest的网络限制  dest->source  dest->client
    configure_network_do(
        interface="enp2s0", rules=dest_rules, is_remote=True, target_ip=DEST_IP  # 远程执行  # 远程执行命令机器 IP
    )
    # 配置client的网络限制  client->vip
    if VIP_IP:
        configure_network_do(
            interface="ens33",
            rules=[
                {"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": BANDWIDTH, "delay": "0.05ms", "dst": DEST_IP},
            ],
            is_remote=True,  # 远程执行
            target_ip=CLIENT_IP,  # 远程执行命令机器 IP
        )


# 定义实验类型与参数
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


# 每种实验进行5次（可通过命令行 --runs 覆盖）


# 主流程
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automate container migration using runc and CRIU.")
    parser.add_argument("-c", "--container", required=True, help="The name of the container to migrate.")
    parser.add_argument("-t", "--tool", required=False, help="Specify the test tool to use.")
    parser.add_argument("-s", "--source-ip", required=True, help="IP address of the source machine.")
    parser.add_argument("-d", "--dest-ip", required=True, help="IP address of the destination machine.")
    parser.add_argument("--client-ip", required=False, help="IP address of the machine running client such as ycsb.")
    parser.add_argument("--virtual-ip", required=False, help="virtual ip ")
    parser.add_argument(
        "--sec", action="store_true", help="use secure source/destination scripts (source-sec.py / destination-sec.py)"
    )
    parser.add_argument("--bandwidth", default="25mbit", help="Network bandwidth limit (e.g. 25mbit). Default: 25mbit")
    parser.add_argument("--runs", type=int, default=5, help="Number of experimental runs per experiment type.")
    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    VIP_IP = args.virtual_ip
    # set bandwidth global
    globals()["BANDWIDTH"] = args.bandwidth
    # runs from CLI
    runs = args.runs
    # 选择 source 脚本：使用 centralized helper
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(args.sec)
    # print(DEST_IP)
    # input()
    for exp_name, exp_args in experiments.items():
        for i in range(1, runs + 1):
            print(f"======== Running {exp_name} experiment run {i} ========")

            try:
                # 准备目标节点的资源 (复制新的bundle、开启console.sock)
                destination_prepare(args)

                # 准备源节点的资源 (复制新的bundle、开启console.sock、启动容器)
                source_prepare(args)

                # 还原keepalived配置
                # update_keepalived_priority(100)
                # update_keepalived_priority(50,True,DEST_IP)
                # 开启网络资源限制
                configure_network()

                # 执行迁移
                source_run_migration(args, exp_args, i, exp_name)
                # input()
                print(f"======== Finished {exp_name} experiment run {i} ========")
            except Exception as e:
                # 捕获异常并打印错误信息
                print(f"Error during experiment '{exp_name}' run {i}: {e}")
            finally:
                # 无论前面是否出错，清理源和目标节点资源
                print("Cleaning up source and destination resources...")
                # input()
                source_clean(args)
                destination_clean(args)
                clean_configure_network()

                # 还原keepalived配置
                update_keepalived_priority(70)
                update_keepalived_priority(30, True, DEST_IP)

            # print("======== sleep ========")
            # time.sleep(30000) #
            # input()
