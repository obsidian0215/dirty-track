import argparse
import re
import subprocess
import shlex
import sys
import time
from typing import Optional
from result_writer import extract_stats_from_output, append_result

from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs

# 默认设置
from script_defaults import choose_scripts, get_default_ips

SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
YCSB_IP = CLIENT_IP  # 保持向后兼容性
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
SEC_MODE = False

# default bandwidth
BANDWIDTH = "50mbit"

# 使用argparse解析命令行参数以动态设置
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elasticsearch container migration with YCSB testing.")
    parser.add_argument("--source-ip", default=SOURCE_IP, help="IP address of the source machine.")
    parser.add_argument("--dest-ip", default=DEST_IP, help="IP address of the destination machine.")
    parser.add_argument("--client-ip", "--ycsb-ip", default=YCSB_IP, help="IP address of the YCSB client machine.")
    parser.add_argument("--vip", default=VIP, help="Virtual IP address (optional).")
    parser.add_argument(
        "--sec", action="store_true", help="use secure source/destination scripts (source-sec.py / destination-sec.py)"
    )
    parser.add_argument(
        "--recordcount", type=int, default=10000, help="Record count for YCSB (recordcount == operationcount)."
    )
    parser.add_argument("--bandwidth", default="50mbit", help="Network bandwidth limit (e.g. 50mbit). Default: 50mbit")
    parser.add_argument("--runs", type=int, default=5, help="Number of experimental runs per experiment type.")
    parsed_args = parser.parse_args()

    # 全局变量
    SOURCE_IP = parsed_args.source_ip
    DEST_IP = parsed_args.dest_ip
    YCSB_IP = parsed_args.client_ip
    VIP = parsed_args.vip
    RECORD_COUNT = parsed_args.recordcount
    OPERATION_COUNT = RECORD_COUNT  # 两者相等
    runs = parsed_args.runs
    # set bandwidth
    globals()["BANDWIDTH"] = getattr(parsed_args, "bandwidth", BANDWIDTH)
    # 根据 --sec 切换为 secure 变体
    sec_enabled = getattr(parsed_args, "sec", False)
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(sec_enabled)
    globals()["SEC_MODE"] = sec_enabled
    print(f"[sec] mode={'on' if sec_enabled else 'off'} using DEST_SCRIPT={DEST_SCRIPT}, SOURCE_SCRIPT={SOURCE_SCRIPT}")

# 定义实验类型与参数
experiments = {
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-1": "-pre -d --tcp-established --shell-job -z 1",
    "pre-copy-2": "-pre -d --tcp-established --shell-job -z 2",
    "pre-copy-3": "-pre -d --tcp-established --shell-job -z 3",
    "pre-copy-4": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "post-copy": "-post -d --tcp-established --shell-job",
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}

def run_ycsb_cmd(cmd, *, ignore_error: bool = False, background: bool = False, quiet: bool = False):
    """Execute a YCSB command on the client host."""
    if not YCSB_IP:
        raise ValueError("YCSB_IP is not configured for run_ycsb_cmd")

    remote_cmd = cmd
    if background:
        remote_cmd = f"nohup {cmd} < /dev/null & echo $!"

    result = run_remote_cmd(remote_cmd, YCSB_IP, ignore_error=ignore_error, quiet=quiet)
    if not ignore_error and getattr(result, "returncode", 0) != 0:
        sys.exit(result.returncode or 1)
    return result


def destination_prepare():
    # 在目标节点执行准备操作
    # 有些命令可能出现非致命错误，用ignore_error=True
    container_name = "elasticsearch"
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                "nohup /root/go/bin/recvtty -m single "
                f"/runc/containers/{container_name}/console.sock > /dev/null 2>&1 "
                "& echo $! > /tmp/recvtty.pid"
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
    run_remote_cmd(stop_prev_cmd, target_ip=DEST_IP, ignore_error=True)
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print $2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, target_ip=DEST_IP, ignore_error=True)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp
    ts = int(time.time())
    dest_script = globals().get("DEST_SCRIPT", "destination.py")
    log_tag = dest_script.replace('.', '_')
    dest_log = f"/tmp/{log_tag}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    start_dest_cmd = (
        f"nohup python3 /runc/dirty-track/mig-scripts/{dest_script} > {dest_log} 2>&1 "
        "& echo $! > "
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


def destination_clean():
    """在目标节点清理资源"""
    container_name = "elasticsearch"

    cmds = [
        (f"runc kill {container_name}", True),  # 如果容器不存在可忽略错误
        (f"runc delete {container_name}", True),  # 如果容器不存在可忽略错误
        ("kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null", True),  # 杀死recvtty进程
        ("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", True),
    ]

    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)
    # 停止 destination 后台进程并移除 pidfile（如果存在）
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        "if [ -f "
        f"{dest_pidfile}"
        "]; then kill -TERM $(cat "
        f"{dest_pidfile}"
        ") 2>/dev/null || true; rm -f "
        f"{dest_pidfile}"
        "; fi"
    )
    run_remote_cmd(stop_cmd, target_ip=DEST_IP, ignore_error=True)
    # 兜底：清理任何仍在运行的 destination 脚本
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print $2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, target_ip=DEST_IP, ignore_error=True)


def source_prepare():
    # 源节点准备，某些命令也可能非致命错误
    container_name = "elasticsearch"
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                "nohup recvtty -m single "
                f"/runc/containers/{container_name}/console.sock > /dev/null 2>&1 "
                "& echo $! > /tmp/recvtty_source.pid"
            ),
            False,
        ),
        # 启动容器
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
    """在源节点清理资源"""
    container_name = "elasticsearch"
    # 清理console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    # 清理 dirtypages的挂载
    unmount_local_migration_tmpfs(container_name)
    run_cmd(f"runc kill {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    # run_cmd("rm -rf /runc/containers/elasticsearch/rootfs/usr/share/elasticsearch/data/*", ignore_error=True)
    # 如果容器不存在可忽略错误
    # run_cmd(f"ps aux | grep 'inotifywait' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9", ignore_error=True)
    # run_cmd(f"ps aux | grep 'sync_rootfs' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9", ignore_error=True)


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

def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """Configure tc shaping rules locally or on a remote host."""
    if is_remote and not target_ip:
        raise ValueError("configure_network_do requires target_ip when is_remote=True")

    cleanup_cmd = f"sudo tc qdisc del dev {interface} root"
    init_cmd = f"sudo tc qdisc add dev {interface} root handle 1: htb"
    remote_ip = str(target_ip) if is_remote else None

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


def clean_configure_network():
    """清空网络配置"""
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True, quiet=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", DEST_IP, ignore_error=True, quiet=True)
    if YCSB_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", YCSB_IP, ignore_error=True, quiet=True)


def configure_network():
    """配置网络限制"""
    source_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP}]
    if YCSB_IP:
        source_rules.append({"rate": BANDWIDTH, "delay": "0.5ms", "dst": YCSB_IP})
        dest_rules.append({"rate": BANDWIDTH, "delay": "0.05ms", "dst": YCSB_IP})

    # 配置source的网络限制 (source->dest, source->ycsb)
    configure_network_do(interface="enp2s0", rules=source_rules, is_remote=False)  # 本地执行

    # 配置dest的网络限制 (dest->source, dest->ycsb)
    configure_network_do(interface="enp2s0", rules=dest_rules, is_remote=True, target_ip=DEST_IP)  # 远程执行

    # 配置ycsb的网络限制 (ycsb->vip，如果有VIP，否则使用源IP或自定义)
    if YCSB_IP:
        configure_network_do(
            interface="ens33",
            rules=[
                # {"rate": "50mbit", "delay": "1ms", "dst": VIP if VIP else SOURCE_IP},
                {"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": BANDWIDTH, "delay": "0.05ms", "dst": DEST_IP},
            ],
            is_remote=True,  # 远程执行
            target_ip=YCSB_IP,
        )


def source_run_migration(exp_args, run_index=0, exp_name="unknown"):
    # 启动容器
    # container_cmd = (
    #     "runc run --console-socket /runc/containers/elasticsearch/console.sock "
    #     "-d -b /runc/containers/elasticsearch elasticsearch"
    # )
    # run_cmd(container_cmd)

    time.sleep(8)  # 等待容器启动稳定
    # 执行YCSB测试
    print("Running YCSB test on the third machine...")
    run_ycsb_cmd('pkill -f "ycsb run elasticsearch"; pkill -f "site.ycsb.Client"', ignore_error=True)

    # 设置环境变量包含Maven路径，然后执行YCSB
    env_setup = "export PATH=$PATH:/usr/bin:/usr/local/bin:/usr/bin/maven/bin/; "

    # YCSB load with Maven path
    load_cmd = (
        f"{env_setup}"
        "cd /root/YCSB && ./bin/ycsb load elasticsearch5-rest -s -P "
        "/root/YCSB/workloads/workloada "
        f"-p \"es.hosts.list={VIP}:9200\" "
        f"-p recordcount={RECORD_COUNT} "
        "> /root/YCSB/logs/outputLoad.txt"
    )
    run_ycsb_cmd(load_cmd)

    # 在load和run之间设置网络带宽和延迟控制
    configure_network()
    print("Network configuration applied between YCSB load and run.")

    # YCSB run (两者recordcount和operationcount相等) with Maven path
    ycsb_run_cmd = (
        f"{env_setup}"
        "cd /root/YCSB && nohup ./bin/ycsb run elasticsearch5-rest -s -P "
        "/root/YCSB/workloads/workloada "
        f"-p operationcount={OPERATION_COUNT} "
        f"-p \"es.hosts.list={VIP}:9200\" "
        "> /root/YCSB/logs/outputRun.txt "
        "2>&1 &"
    )
    run_ycsb_cmd(ycsb_run_cmd, background=False)

    time.sleep(6)  # 等待YCSB启动稳定
    # 执行 source 脚本进行迁移（支持 secure 变体）
    # Execute migration command as an argument list to avoid local shell quoting issues.
    migration_args = ["python3", SOURCE_SCRIPT]
    migration_args += shlex.split(exp_args)
    migration_args += ["--file-locks", "elasticsearch", DEST_IP]
    print("Executing migration:", " ".join(migration_args))
    result = subprocess.run(migration_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print("Migration command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)

    # 尝试从 stdout 中提取统计行并写入 results
    try:
        stdout = getattr(result, "stdout", "") or ""
        header, stats, params = extract_stats_from_output(stdout)
        if stats:
            params_summary = f"exp: {exp_args}"
            extra_lines = [
                f"workload-load: ycsbA es.hosts.list={VIP}:9200 recordcount={RECORD_COUNT}",
                f"workload-run: ycsbA es.hosts.list={VIP}:9200 operationcount={OPERATION_COUNT}",
            ]
            if params:
                extra_lines += params
            append_result(
                exp_name,
                "elasticsearch",
                run_index,
                stats,
                header,
                params_summary,
                is_secure=SEC_MODE,
                extra_param_lines=extra_lines,
                first_in_run=(run_index == 1),
            )
            print(f"Wrote stats for {exp_name} run {run_index} -> results/{exp_name}.csv")
        else:
            print("No statistics line found in source output; skipping result write.")
    except Exception as e:
        print(f"Error writing stats: {e}")

    # clean
    cleanup_cmd = "kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true"
    run_cmd(cleanup_cmd, ignore_error=False)


if __name__ == "__main__":
    import atexit
    # 进程退出时做一次最终网络清理（统一善后）
    atexit.register(clean_configure_network)
    # 确保网络配置初始化
    # update_keepalived_priority(70)
    # update_keepalived_priority(30,True,DEST_IP)
    clean_configure_network()
    # 主流程
    for exp_name, exp_args in experiments.items():
        for i in range(1, runs + 1):
            print(f"======== Running {exp_name} experiment run {i} ========")
            try:
                # 确保本轮开始前未残留任何限速规则，避免影响 load 阶段
                clean_configure_network()
                # 准备目标节点
                destination_prepare()

                # 准备源节点
                source_prepare()

                # 执行迁移（此步骤结束说明source.py执行完成，迁移完成）
                source_run_migration(exp_args, i, exp_name)

                print(f"======== Finished {exp_name} experiment run {i} ========")
            except Exception as e:
                # 捕获异常并打印错误信息
                print(f"Error during experiment '{exp_name}' run {i}: {e}")
            finally:
                # 无论前面是否出错，清理源和目标节点资源
                print("Cleaning up source and destination resources...")
                # input()

                # 迁移完成后清理源和目标节点
                source_clean()
                destination_clean()
                # source_clean(parsed_args)
                # destination_clean(args)

                # 清理网络配置
                # 还原keepalived配置
                update_keepalived_priority(70)
                update_keepalived_priority(30, True, DEST_IP)
            time.sleep(17)  #
            # input()
