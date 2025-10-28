import argparse
import re
import subprocess
import sys
import time

# 默认设置
SOURCE_IP = "192.168.2.105"
DEST_IP = "192.168.2.225"
CLIENT_IP = "192.168.2.245"
VIP = "192.168.2.100"
YCSB_IP = CLIENT_IP  # 保持向后兼容性
# default script selection
from script_defaults import choose_scripts

SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
# RECORD_COUNT = 100000
# OPERATION_COUNT = 100000  # 默认两者相等

# 使用argparse解析命令行参数以动态设置
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis container migration with YCSB testing.")
    parser.add_argument("-s", "--source-ip", default=SOURCE_IP, help="IP address of the source machine.")
    parser.add_argument("-d", "--dest-ip", default=DEST_IP, help="IP address of the destination machine.")
    parser.add_argument("-c", "--client-ip", default=CLIENT_IP, help="IP address of the YCSB client machine.")
    parser.add_argument("--vip", default=VIP, help="Virtual IP address (optional).")

    # [memtier] changed: 替换/新增 memtier 参数（默认值按你给的命令）
    parser.add_argument("--mt-n", type=int, default=150000, help="memtier: total requests per thread (-n).")
    parser.add_argument("--mt-c", type=int, default=5, help="memtier: connections per thread (-c).")
    parser.add_argument("--mt-t", type=int, default=4, help="memtier: number of threads (-t).")
    parser.add_argument("--mt-ratio", default="9:1", help="memtier: --ratio (reads:writes).")
    # parser.add_argument("--mt-dsl", default="32:0.3,64:0.1,512:0.3,1024:0.2,4096:0.1",
    #                     help="memtier: --data-size-list.")
    parser.add_argument(
        "--mt-dsl", default="32:3,64:1,512:4,1024:1,2048:1", help="memtier: --data-size-list (权重必须是整数)."
    )
    parser.add_argument("--mt-rand", action="store_true", default=True, help="memtier: use random keys (-R).")
    parser.add_argument("--runs", type=int, default=1, help="Number of experimental runs per experiment type.")
    parser.add_argument("--sec", action="store_true", help="use source-sec/destination-sec scripts")

    parsed_args = parser.parse_args()

    # 全局变量
    SOURCE_IP = parsed_args.source_ip
    DEST_IP = parsed_args.dest_ip
    CLIENT_IP = parsed_args.client_ip  # [memtier] changed
    VIP = parsed_args.vip
    runs = parsed_args.runs
    # set source script selection
    src, dst = choose_scripts(getattr(parsed_args, "sec", False))
    globals()["SOURCE_SCRIPT"] = src
    globals()["DEST_SCRIPT"] = dst

    # memtier 相关参数  # [memtier] changed
    MT_N = parsed_args.mt_n
    MT_C = parsed_args.mt_c
    MT_T = parsed_args.mt_t
    MT_RATIO = parsed_args.mt_ratio
    MT_DSL = parsed_args.mt_dsl
    MT_RAND = parsed_args.mt_rand

# 定义实验类型与参数
experiments = {
    # "post-copy": "-post -d --tcp-established --shell-job",
    "pre-copy": "-pre -d --tcp-established --shell-job -z 0",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}


def run_cmd(cmd, ignore_error=False):
    print("Executing on source:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)


def run_remote_cmd(cmd, target_ip=None, ignore_error=False, background=False):
    """
    Execute command on remote machine.
    """
    if not target_ip:
        target_ip = DEST_IP

    # 如果background=True，则在目标机器上使用nohup和&将进程放入后台
    if background:
        # 使用nohup和&后台运行，让ssh立即返回
        # 同时将输出重定向到文件，防止阻塞
        cmd = f"nohup {cmd}  &"

    full_cmd = f"ssh {target_ip} '{cmd}'"
    print("Executing remotely on", target_ip, ":", cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)


def run_client_cmd(
    cmd, ignore_error=False, background=False
):  # [memtier] changed: 通用客户端执行函数（替代 run_ycsb_cmd）
    if background:
        cmd = f"nohup {cmd}  &"
    full_cmd = f"ssh {CLIENT_IP} '{cmd}'"
    print("Executing on client (memtier) machine:", full_cmd)  # [memtier] changed
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Client command failed with error:", result.stderr)
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
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
    )
    run_remote_cmd(stop_cmd, target_ip=DEST_IP, ignore_error=True)

    # recvtty_cmd = f"PATH=$PATH:/root/go/bin recvtty -m null /runc/containers/{container_name}/console.sock > /tmp/recvtty_debug.log 2>&1 & & echo $! > /tmp/recvtty_source.pid"
    # run_remote_cmd(recvtty_cmd, target_ip=DEST_IP, ignore_error=False)

    recvtty_cmd = (
        f"PATH=$PATH:/root/go/bin "
        f"nohup recvtty -m null /runc/containers/{container_name}/console.sock "
        f"> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_dest.pid"
    )
    run_remote_cmd(recvtty_cmd, target_ip=DEST_IP, ignore_error=False)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp（可通过 --sec 切换）
    ts = int(time.time())
    dest_log = f"/tmp/{globals().get('DEST_SCRIPT','destination.py').replace('.','_')}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    start_dest_cmd = (
        f"nohup python3 {globals().get('DEST_SCRIPT','destination.py')} > {dest_log} 2>&1 & echo $! > {dest_pidfile}"
    )
    run_remote_cmd(start_dest_cmd, target_ip=DEST_IP, ignore_error=False, background=False)
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
            f"nohup recvtty -m null /runc/containers/{container_name}/console.sock > /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid",
            False,
        ),
        # 启动容器
        (
            f"runc run --console-socket /runc/containers/{container_name}/console.sock -d -b /runc/containers/{container_name} {container_name}",
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
                f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {dst} flowid {classid}",
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
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", target_ip=DEST_IP, ignore_error=True)
    if YCSB_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", target_ip=YCSB_IP, ignore_error=True)


def configure_network():
    """配置网络限制"""
    source_rules = [{"rate": "50mbit", "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP}]
    if YCSB_IP:
        source_rules.append({"rate": "50mbit", "delay": "0.5ms", "dst": YCSB_IP})
        dest_rules.append({"rate": "50mbit", "delay": "0.05ms", "dst": YCSB_IP})

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
                {"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": "50mbit", "delay": "0.05ms", "dst": DEST_IP},
            ],
            is_remote=True,  # 远程执行
            target_ip=YCSB_IP,
        )


# 新增：构造 load 阶段的 memtier 命令（写入-only，前台阻塞直到完成）
def build_memtier_load_cmd():
    parts = [
        "memtier_benchmark",
        f"-s {VIP}",
        "-p 6379",
        f"-n {MT_N}",  # 使用同样的请求数
        "-c 5",  # 你要求的 load 阶段：-c 1
        "-t 4",  # 你要求的 load 阶段：-t 1
        "--ratio=9:1",  # 你要求的 load 阶段：写入-only
        "--data-size-list=32:3,64:4,128:3,512:1,1024:1",
    ]
    if MT_RAND:
        parts.append("-R")  # 随机 key，保持你的默认设定
    return " ".join(parts)


# [memtier] changed: 组装 memtier 命令（使用 VIP:6379）
def build_memtier_cmd():
    parts = [
        "memtier_benchmark",
        f"-s {VIP}",
        "-p 6379",
        "-n 210000",
        f"-c {MT_C}",
        f"-t {MT_T}",
        "--ratio=6:1",
        "--data-size-list=32:2,64:4,128:5,512:4,1024:1,2048:1",
    ]
    if MT_RAND:
        parts.append("-R")
    return " ".join(parts)


def source_run_migration(exp_args):
    time.sleep(6)  # 等待容器启动稳定

    print("Running memtier (load + run) on the client machine...")
    # 确保没有遗留的 memtier
    run_client_cmd('pkill -f "memtier_benchmark"', ignore_error=True)

    # ===== 第 1 阶段：LOAD（前台阻塞，直到写入完成） =====
    load_cmd = build_memtier_load_cmd()  # [changed] 使用写入-only配置
    ts_load = int(time.time())
    # 前台执行 -> SSH 会等待 memtier 退出（完成 -n 请求）
    run_client_cmd(f"{load_cmd} > /tmp/memtier_load_{ts_load}.log 2>&1", background=False)  # [changed]

    # ===== load 与 run 之间：配置网络（保持你的原逻辑） =====
    configure_network()
    print("Network configuration applied between memtier load and run.")

    # ===== 第 2 阶段：RUN（后台持续打压，用你之前提供的参数） =====
    run_cmdline = build_memtier_cmd()  # [unchanged] 原来已有：-n 300000 -c 5 -t 4 --ratio=5:1 --data-size-list=... -R
    ts_run = int(time.time())
    run_client_cmd(f"{run_cmdline} > /tmp/memtier_run_{ts_run}.log 2>&1", background=True)  # [changed] 后台执行

    time.sleep(8)  # 等待 run 启动稳定

    # ===== 执行迁移 =====
    migration_cmd = f"python3 {SOURCE_SCRIPT} {exp_args} redis {DEST_IP}"
    run_cmd(migration_cmd)

    # ===== 清理源端 recvtty =====
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
                source_run_migration(exp_args)

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
            time.sleep(7)  #
