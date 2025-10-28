import argparse
import re
import subprocess
import sys
import time

# 默认设置
from script_defaults import choose_scripts, get_default_ips

SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)


def run_cmd(cmd, ignore_error=False):
    print("Executing on source:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)


# def run_remote_cmd(cmd,target_ip,ignore_error=False, background=False):
#     # 如果background=True，则在目标机器上使用nohup和&将进程放入后台
#     if background:
#         # 使用nohup和&后台运行，让ssh立即返回
#         # 同时将输出重定向到文件，防止阻塞
#         cmd = f"nohup {cmd}  &"

#     full_cmd = f"ssh {target_ip} \"{cmd}\""
#     print("Executing remotely:", full_cmd)
#     result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
#     if result.returncode != 0 and not ignore_error:
#         print("Remote command failed with error:", result.stderr)
#         sys.exit(1)
#     else:
#         print(result.stdout)


def run_remote_cmd(cmd, target_ip, ignore_error=False, background=False):
    if not target_ip:
        raise ValueError("target_ip is required for run_remote_cmd")

    if background:
        # 关键点：重定向 stdin/out/err，并让 ssh -n 不转发本地 stdin
        remote = f"nohup {cmd} >/tmp/remote_bg.log 2>&1 < /dev/null & echo $! > /tmp/remote_bg.pid"
        full_cmd = f"ssh -n {target_ip} '{remote}'"
    else:
        full_cmd = f"ssh {target_ip} '{cmd}'"

    print("Executing remotely:", full_cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)


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

            # 重启本地服务
            if not restart_keepalived(is_remote=False):
                return False

        print("成功修改配置文件中的 priority 参数")
        return True

    except Exception as e:
        print(f"修改配置文件时出错: {e}")
        return False


def source_prepare(args):
    container_name = args.container
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            f"nohup recvtty -m single /runc/containers/{container_name}/console.sock > /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid",
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


def source_run_migration(args, exp_args):
    container_name = args.container
    # 开启工具测试
    run_remote_cmd(
        "wrk -t4 -c50 -d120s --timeout 10s http://192.168.2.100/", CLIENT_IP, ignore_error=True, background=True
    )

    time.sleep(3)  # 等待bench启动稳定
    # 执行 source 脚本进行迁移 (可切换为 secure 变体)
    run_cmd(f"python3 {SOURCE_SCRIPT} {container_name} {DEST_IP} {exp_args}")


def source_clean(args):
    container_name = args.container
    # 清理console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    # 清理 dirtypages的挂载, 可忽略错误(有时候挂载都清理完毕了)
    run_cmd(f"umount /runc/containers/{container_name}/migrate/*", ignore_error=True)
    run_cmd(f"runc kill {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd("ps aux | grep 'inotifywait' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9", ignore_error=True)
    run_cmd("ps aux | grep 'sync_rootfs' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9", ignore_error=True)


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
            f"nohup  /root/go/bin/recvtty -m single /runc/containers/{container_name}/console.sock  > /dev/null 2>&1 & echo $! > /tmp/recvtty.pid",
            False,
        ),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp
    ts = int(time.time())
    dest_log = f"/tmp/{DEST_SCRIPT.replace('.','_')}_{container_name}_{ts}.log"
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    start_dest_cmd = f"nohup python3 {DEST_SCRIPT} > {dest_log} 2>&1 & echo $! > {dest_pidfile}"
    run_remote_cmd(start_dest_cmd, target_ip=DEST_IP, ignore_error=False, background=False)
    print(f"Started remote destination on {DEST_IP}, log: {dest_log}, pidfile: {dest_pidfile}")


def destination_clean(args):
    # 恢复过程结束时杀死recvtty进程
    container_name = args.container

    cmds = [
        (f"runc kill {container_name}", True),  # 如果容器不存在可忽略错误
        (f"runc delete {container_name}", True),  # 如果容器不存在可忽略错误
        ("kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null", True),  # 杀死recvtty进程
        ("ps aux | grep 'recvtty' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9", True),
        # (f"ps aux | grep '[n]c -lp' | awk '{{print $2}}' | xargs -r kill -9", False),
        ("ps aux | grep '[n]c -lp' | grep -v grep | awk '{print \\$2}' | xargs -r kill -9", True),
    ]

    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)
    # 停止 destination 后台进程（如果存在）并移除 pid 文件
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
    )
    run_remote_cmd(stop_cmd, target_ip=DEST_IP, ignore_error=True)


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
    # 清空网络配置
    run_cmd("sudo tc qdisc del dev enp2s0 root", ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev enp2s0 root", target_ip=DEST_IP, ignore_error=True)
    if VIP_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", target_ip=CLIENT_IP, ignore_error=True)


def configure_network():
    source_rules = [{"rate": "25mbit", "delay": "0.5ms", "dst": DEST_IP}]
    dest_rules = [{"rate": "25mbit", "delay": "0.5ms", "dst": SOURCE_IP}]
    if VIP_IP:
        source_rules.append({"rate": "25mbit", "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": "25mbit", "delay": "0.05ms", "dst": CLIENT_IP})

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
                {"rate": "25mbit", "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": "25mbit", "delay": "0.05ms", "dst": DEST_IP},
            ],
            is_remote=True,  # 远程执行
            target_ip=CLIENT_IP,  # 远程执行命令机器 IP
        )


# 定义实验类型与参数
experiments = {
    # "post-copy": "-post -d --tcp-established --shell-job",
    # "pre-copy": "-pre -d --tcp-established --shell-job",
    # "pre-copy": "-pre -d --tcp-established --shell-job -z 1",
    # "pre-copy": "-pre -d --tcp-established --shell-job -z 2",
    "pre-copy": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}


# 每种实验进行5次
runs = 1

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
    args = parser.parse_args()

    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    VIP_IP = args.virtual_ip
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
                source_run_migration(args, exp_args)
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

            print("======== sleep ========")
            # time.sleep(30000) #
            # input()
