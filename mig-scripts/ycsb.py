import argparse
import re
import subprocess
import shlex
import sys
import time
from result_writer import extract_stats_from_output, append_result

from script_defaults import choose_scripts, get_default_ips

# 默认设置（从集中 defaults 读取）
SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
YCSB_IP = CLIENT_IP  # 保持向后兼容性
# RECORD_COUNT = 100000
# OPERATION_COUNT = 100000  # 默认两者相等

# default bandwidth
BANDWIDTH = "25mbit"
SEC_MODE = False

# 使用argparse解析命令行参数以动态设置
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis container migration with YCSB testing.")
    parser.add_argument("-s", "--source-ip", default=SOURCE_IP, help="IP address of the source machine.")
    parser.add_argument("-d", "--dest-ip", default=DEST_IP, help="IP address of the destination machine.")
    parser.add_argument(
        "-c", "--client-ip", "--ycsb-ip", default=YCSB_IP, help="IP address of the YCSB client machine."
    )
    parser.add_argument("--vip", default=VIP, help="Virtual IP address (optional).")
    parser.add_argument(
        "--timeout", "--redis-timeout", type=int, default=10000, help="Redis timeout in milliseconds for YCSB."
    )
    parser.add_argument(
        "-rc", "--recordcount", type=int, default=100000, help="Record count for YCSB (recordcount == operationcount)."
    )
    parser.add_argument("--runs", type=int, default=5, help="Number of experimental runs per experiment type.")
    parser.add_argument("--sec", action="store_true", help="use source-sec/destination-sec scripts")
    parser.add_argument("--bandwidth", default="25mbit", help="Network bandwidth limit (e.g. 25mbit). Default: 25mbit")
    parsed_args = parser.parse_args()

    # 全局变量
    SOURCE_IP = parsed_args.source_ip
    DEST_IP = parsed_args.dest_ip
    YCSB_IP = parsed_args.client_ip
    VIP = parsed_args.vip
    RECORD_COUNT = parsed_args.recordcount
    OPERATION_COUNT = RECORD_COUNT  # 两者相等
    REDIS_TIMEOUT = parsed_args.timeout
    runs = parsed_args.runs
    # script selection: allow secure variants
    sec_enabled = getattr(parsed_args, "sec", False)
    SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(sec_enabled)
    globals()["SEC_MODE"] = sec_enabled
    # set bandwidth
    globals()["BANDWIDTH"] = getattr(parsed_args, "bandwidth", BANDWIDTH)

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
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job",
}


def run_cmd(cmd, ignore_error=False):
    print("Executing on source:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)
    return result


def run_remote_cmd(cmd, target_ip=None, ignore_error=False, background=False):
    """
    Execute command on remote machine.
    """
    if not target_ip:
        target_ip = DEST_IP

    if background:
        remote_cmd = f"nohup {cmd} > /dev/null 2>&1 & echo $!"
        full_cmd = f"ssh {target_ip} \"{remote_cmd}\""
    else:
        full_cmd = f"ssh {target_ip} '{cmd}'"

    print("Executing remotely on", target_ip, ":", cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)
    return result


def run_ycsb_cmd(cmd, ignore_error=False, background=False):
    # 在第三台机器上执行YCSB命令
    # 如果background=True，则在目标机器上使用nohup和&将进程放入后台
    if background:
        remote_cmd = f"nohup {cmd}  &"
    else:
        remote_cmd = cmd

    ssh_args = ["ssh", YCSB_IP, remote_cmd]
    print("Executing on YCSB machine:", " ".join(ssh_args))
    result = subprocess.run(ssh_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("YCSB command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)


def destination_prepare():
    # 在目标节点执行准备操作
    # 有些命令可能出现非致命错误，用ignore_error=True
    container_name = "redis"
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
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

    recvtty_cmd = (
        "PATH=$PATH:/root/go/bin "
        f"nohup recvtty -m null /runc/containers/{container_name}/console.sock "
        "> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_dest.pid"
    )
    run_remote_cmd(recvtty_cmd, target_ip=DEST_IP, ignore_error=False)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp
    ts = int(time.time())
    script_name = DEST_SCRIPT.replace('.', '_')
    dest_log = f"/tmp/{script_name}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    start_dest_cmd = (
        f"nohup python3 /runc/dirty-track/mig-scripts/{script_name}.py > {dest_log} 2>&1 "
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
    container_name = "redis"

    cmds = [
        (f"runc kill {container_name}", True),  # 如果容器不存在可忽略错误
        (f"runc delete {container_name}", True),  # 如果容器不存在可忽略错误
        ("kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null", True),  # 杀死recvtty进程
        ("ps aux | grep 'recvtty' | grep -v grep | awk '{print $2}' | xargs -r kill -9", True),
    ]

    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)


def source_prepare():
    # 源节点准备，某些命令也可能非致命错误
    container_name = "redis"
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                "nohup recvtty -m null "
                f"/runc/containers/{container_name}/console.sock > /dev/null 2>&1 "
                "& echo $! > /tmp/recvtty_source.pid"
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


def source_clean():
    """在源节点清理资源"""
    container_name = "redis"
    # 清理console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    # 清理 dirtypages的挂载
    run_cmd(f"umount /runc/containers/{container_name}/migrate/*", ignore_error=True)
    run_cmd(f"runc kill {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd("ps aux | grep 'inotifywait' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)
    run_cmd("ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print $2}' | xargs -r kill -9", ignore_error=True)


def update_keepalived_priority(new_priority, is_remote=False, target_ip=None):
    """
    修改 keepalived 配置文件中的 priority 参数，支持本地和远程执行。

    参数:
    - new_priority: 要设置的新优先级 (整数)。
    - is_remote: 是否在远程机器上执行 (默认: False)。
    - target_ip: 远程目标机器的 IP 地址 (仅在 is_remote=True 时有效)。

    返回:
    - True: 修改成功。
    - False: 修改失败。
    """

    # 配置文件路径 (固定值)
    config_path = "/etc/keepalived/keepalived.conf"
    # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
    pattern = r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})"

    # 定义修改文件的函数
    def modify_file(content):
        if not re.search(pattern, content):
            print("未找到匹配的 vrrp_instance VI_1 块或 priority 参数")
            return None

        def repl(match):
            original_priority = match.group(2)
            print(f"将 VIP 的优先级从 {original_priority} 修改为 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"

        return re.sub(pattern, repl, content)

    def restart_keepalived(is_remote=False, target_ip=None):
        restart_cmd = "sudo systemctl restart keepalived"
        status_cmd = "sudo systemctl is-active keepalived"

        if is_remote:
            if not target_ip:
                raise ValueError("在远程执行时，必须提供目标机器的 IP 地址。")
            # 重启远程服务
            print(f"在远程机器 {target_ip} 重启 keepalived 服务...")
            result = subprocess.run(
                f"ssh {target_ip} '{restart_cmd}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                print(f"远程重启 keepalived 失败: {result.stderr}")
                return False

            # 检查服务状态
            status = subprocess.run(
                f"ssh {target_ip} '{status_cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if status.returncode == 0 and status.stdout.strip() == "active":
                print("远程 keepalived 服务已成功重启并处于活动状态。")
                return True
            else:
                print("远程 keepalived 服务未能成功重启或未处于活动状态。")
                return False

        else:
            # 重启本地服务
            print("在本地机器重启 keepalived 服务...")
            result = subprocess.run(restart_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print(f"本地重启 keepalived 失败: {result.stderr}")
                return False

            # 检查服务状态
            status = subprocess.run(status_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if status.returncode == 0 and status.stdout.strip() == "active":
                print("本地 keepalived 服务已成功重启并处于活动状态。")
                return True
            else:
                print("本地 keepalived 服务未能成功重启或未处于活动状态。")
                return False

    try:
        if is_remote:
            if not target_ip:
                raise ValueError("在远程执行时，必须提供目标机器的 IP 地址。")

            # 远程读取配置文件内容
            print(f"从远程机器 {target_ip} 读取配置文件...")
            result = subprocess.run(
                f"ssh {target_ip} 'cat {config_path}'",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if result.returncode != 0:
                print(f"远程读取配置文件失败: {result.stderr}")
                return False

            content = result.stdout
            updated_content = modify_file(content)
            if updated_content is None:
                return False

            # 将修改后的内容写回远程文件
            print(f"将修改后的内容写回远程机器 {target_ip}...")
            write_result = subprocess.run(
                f"ssh {target_ip} \"echo '{updated_content}' > {config_path}\"",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if write_result.returncode != 0:
                print(f"远程写入配置文件失败: {write_result.stderr}")
                return False

            # 重启远程服务
            if not restart_keepalived(is_remote=True, target_ip=target_ip):
                return False
        else:
            # 本地读取配置文件内容
            print("从本地读取配置文件...")
            with open(config_path, "r") as f:
                content = f.read()

            updated_content = modify_file(content)
            if updated_content is None:
                return False

            # 写回本地配置文件
            print("将修改后的内容写回本地配置文件...")
            with open(config_path, "w") as f:
                f.write(updated_content)

            if not restart_keepalived(is_remote=False):
                return False

        print("成功修改配置文件中的 priority 参数")
        return True

    except Exception as e:
        print(f"修改配置文件时出错: {e}")
        return False


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

    返回:
    - None
    """
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    cleanup_cmd = f"sudo tc qdisc del dev {interface} root"
    print(f"Executing cleanup: {cleanup_cmd}")
    if is_remote:
        run_remote_cmd(cleanup_cmd, target_ip=target_ip, ignore_error=True)
    else:
        run_cmd(cleanup_cmd, ignore_error=True)

    # 基础命令
    base_cmds = [
        f"sudo tc qdisc add dev {interface} root handle 1: htb",
    ]

    # 动态添加规则
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

    # 执行命令 (本地或远程)
    for cmd in base_cmds:
        print(f"Executing: {cmd}")
        if is_remote:
            run_remote_cmd(cmd, target_ip=target_ip, ignore_error=ignore_error)
        else:
            run_cmd(cmd, ignore_error=ignore_error)


def clean_configure_network():
    """清空网络配置"""
    run_cmd("sudo tc qdisc del dev ens33 root", ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev ens33 root", target_ip=DEST_IP, ignore_error=True)
    if YCSB_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", target_ip=YCSB_IP, ignore_error=True)


def configure_network():
    """配置网络限制"""
    source_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": BANDWIDTH, "delay": "0.5ms", "dst": SOURCE_IP}]
    if YCSB_IP:
        source_rules.append({"rate": BANDWIDTH, "delay": "0.5ms", "dst": YCSB_IP})
        dest_rules.append({"rate": BANDWIDTH, "delay": "0.05ms", "dst": YCSB_IP})

    # 配置source的网络限制 (source->dest, source->ycsb)
    configure_network_do(interface="ens33", rules=source_rules, is_remote=False)  # 本地执行

    # 配置dest的网络限制 (dest->source, dest->ycsb)
    configure_network_do(interface="ens33", rules=dest_rules, is_remote=True, target_ip=DEST_IP)  # 远程执行

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
    # container_cmd = "runc run --console-socket /runc/containers/redis/console.sock -d -b /runc/containers/redis redis"
    # run_cmd(container_cmd)

    time.sleep(6)  # 等待容器启动稳定
    # 执行YCSB测试
    print("Running YCSB test on the third machine...")
    run_ycsb_cmd('pkill -f "ycsb run redis"; pkill -f "site.ycsb.Client"', ignore_error=True)

    # 设置环境变量包含Maven路径，然后执行YCSB
    env_setup = "export PATH=$PATH:/usr/bin:/usr/local/bin:/usr/bin/maven/bin/; "

    # YCSB load with Maven path
    load_cmd = (
        f"{env_setup}"
        "cd /root/YCSB && ./bin/ycsb load redis -s -P /root/YCSB/workloads/workloada "
        f"-p redis.host={VIP} -p redis.port=6379 -p recordcount={RECORD_COUNT} "
        "> /root/YCSB/logs/outputLoad.txt"
    )
    run_ycsb_cmd(load_cmd)

    # 在load和run之间设置网络带宽和延迟控制
    configure_network()
    print("Network configuration applied between YCSB load and run.")

    timestamp = time.time()
    # YCSB run (两者recordcount和operationcount相等) with Maven path
    ycsb_run_cmd = (
        f"{env_setup}"
        "cd /root/YCSB && nohup ./bin/ycsb run redis -s -P /root/YCSB/workloads/workloada "
        f"-p operationcount={OPERATION_COUNT} -p redis.host={VIP} -p redis.port=6379 "
        "> /root/YCSB/logs/outputRun_" + str(timestamp) + "_tmp.txt 2>&1 &"
    )
    run_ycsb_cmd(ycsb_run_cmd, background=False)

    time.sleep(8)  # 等待YCSB启动稳定
    # 执行source.py进行迁移
    migration_args = ["python3", SOURCE_SCRIPT] + shlex.split(exp_args) + ["redis", DEST_IP]
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
        header, stats = extract_stats_from_output(stdout)
        if stats:
            append_result(exp_name, "redis", run_index, stats, header, exp_args, is_secure=SEC_MODE)
            print(f"Wrote stats for {exp_name} run {run_index} -> results/{exp_name}.tsv")
        else:
            print("No statistics line found in source output; skipping result write.")
    except Exception as e:
        print(f"Error writing stats: {e}")

    # clean
    cleanup_cmd = "kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true"
    run_cmd(cleanup_cmd, ignore_error=False)


if __name__ == "__main__":
    # 确保网络配置初始化
    # update_keepalived_priority(70)
    # update_keepalived_priority(30,True,DEST_IP)
    # clean_configure_network()
    # 主流程
    for exp_name, exp_args in experiments.items():
        for i in range(1, runs + 1):
            print(f"======== Running {exp_name} experiment run {i} ========")
            try:
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
                clean_configure_network()
                # 还原keepalived配置
                update_keepalived_priority(70)
                update_keepalived_priority(30, True, DEST_IP)
            time.sleep(17)  #
