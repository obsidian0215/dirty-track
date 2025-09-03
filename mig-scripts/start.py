import subprocess
import sys
import time
import argparse
import re


SOURCE_IP = None
DEST_IP = None
CLIENT_IP = None
VIP_IP = None

def run_cmd(cmd, ignore_error=False):
    print("Executing on source:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)

def run_remote_cmd(cmd,target_ip,ignore_error=False, background=False):
    # 如果background=True，则在目标机器上使用nohup和&将进程放入后台
    if background:
        # 使用nohup和&后台运行，让ssh立即返回
        # 同时将输出重定向到文件，防止阻塞
        cmd = f"nohup {cmd}  &"

    full_cmd = f"ssh {DEST_IP} \"{cmd}\""
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
    config_path = '/etc/keepalived/keepalived.conf'
    # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
    pattern = r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})'

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
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if result.returncode != 0:
                print(f"远程重启 keepalived 失败: {result.stderr}")
                return False

            # 检查服务状态
            status = subprocess.run(
                f"ssh {target_ip} '{status_cmd}'",
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if status.returncode == 0 and status.stdout.strip() == "active":
                print(f"远程 keepalived 服务已成功重启并处于活动状态。")
                return True
            else:
                print(f"远程 keepalived 服务未能成功重启或未处于活动状态。")
                return False

        else:
            # 重启本地服务
            print("在本地机器重启 keepalived 服务...")
            result = subprocess.run(
                restart_cmd,
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
            if result.returncode != 0:
                print(f"本地重启 keepalived 失败: {result.stderr}")
                return False

            # 检查服务状态
            status = subprocess.run(
                status_cmd,
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )
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
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
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
                shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
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
            with open(config_path, 'r') as f:
                content = f.read()

            updated_content = modify_file(content)
            if updated_content is None:
                return False

            # 写回本地配置文件
            print("将修改后的内容写回本地配置文件...")
            with open(config_path, 'w') as f:
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
        (f"rm -rf /runc/containers/{container_name}",False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}",False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (f"nohup recvtty -m single /runc/containers/{container_name}/console.sock > /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid",False),
        # 启动容器
        (f"runc run --console-socket /runc/containers/{container_name}/console.sock -d -b /runc/containers/{container_name} {container_name}",False)
    ]
    for c, ign in cmds:
        run_cmd(c, ignore_error=ign)

def source_run_migration(args,exp_args):
    container_name = args.container
    # 执行source.py进行迁移
    run_cmd(f"python3 source.py {container_name} {DEST_IP} {exp_args}")


def source_clean(args):
    container_name = args.container
    # 清理dirtypages残余进程
    if args.tool.lower() == "dirtypages":
        run_cmd("sudo pkill -SIGKILL -f '/bin/dirty-pages'",ignore_error=True)
    # 清理console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null",ignore_error=True)
    # 清理 dirtypages的挂载, 可忽略错误(有时候挂载都清理完毕了)
    run_cmd(f"umount /runc/containers/{container_name}/migrate/*",ignore_error=True)
    run_cmd(f"runc kill {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True) # 如果容器不存在可忽略错误
    run_cmd(f"ps aux | grep 'inotifywait' | grep -v grep | awk '{{print \\$2}}' | xargs -r kill -9",ignore_error=True)
    run_cmd(f"ps aux | grep 'sync_rootfs' | grep -v grep | awk '{{print \\$2}}' | xargs -r kill -9",ignore_error=True)

def destination_prepare(args):
    # 在目标节点执行准备操作
    # 有些命令可能出现非致命错误，用ignore_error=True
    container_name = args.container
    cmds = [
     # 准备新的bundle,
    (f"rm -rf /runc/containers/{container_name}",False),
    (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}",False),
    # 启动console.sock并把进程号存储起来,后续清理时kill掉
    (f"nohup recvtty -m single /runc/containers/{container_name}/console.sock  > /dev/null 2>&1 & echo $! > /tmp/recvtty.pid", False)
    ]
    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP,ignore_error=ign)

def destination_clean(args):
    # 恢复过程结束时杀死recvtty进程
    container_name = args.container

    # 清理dirtypages残余进程
    if args.tool.lower() == "dirtypages":
        run_remote_cmd("sudo pkill -SIGKILL -f 'dirty-pages'",target_ip=DEST_IP,ignore_error=True);

    cmds = [
        (f"runc kill {container_name}", True),  # 如果容器不存在可忽略错误
        (f"runc delete {container_name}", True), # 如果容器不存在可忽略错误
        (f"kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null",True), # 杀死recvtty进程
        (f"ps aux | grep 'recvtty' | grep -v grep | awk '{{print \\$2}}' | xargs -r kill -9", True),
        #(f"ps aux | grep '[n]c -lp' | awk '{{print $2}}' | xargs -r kill -9", False),
        (f"ps aux | grep '[n]c -lp' | grep -v grep | awk '{{print \\$2}}' | xargs -r kill -9", True)
    ]

    for c, ign in cmds:
        run_remote_cmd(c, target_ip=DEST_IP, ignore_error=ign)

def execute_ycsb_tests():

    # 清空网络配置,在执行load前
    run_cmd("sudo tc qdisc del dev ens33 root")
    run_remote_cmd("sudo tc qdisc del dev ens33 root",target_ip=DEST_IP)
    run_remote_cmd("sudo tc qdisc del dev ens33 root",target_ip=CLIENT_IP)


    # 还原keepalived配置
    update_keepalived_priority(70)
    update_keepalived_priority(30,True,DEST_IP)


    # 杀死之前可能未完成的run进程, 开启load
    run_remote_cmd('pkill -f "ycsb run redis"; pkill -f "site.ycsb.Client"',target_ip=CLIENT_IP,ignore_error=True)
    run_remote_cmd(f"cd /root/YCSB  && ./bin/ycsb load redis -s -P /root/YCSB/workloads/workloada -p redis.host={VIP_IP} -p redis.port=6379 -p recordcount=50000 > /root/YCSB//logs/outputLoad.txt",target_ip=CLIENT_IP)
    # 配置好网络限制, 此前清除是因为防止load过慢
    configure_network()
    run_remote_cmd(f"cd /root/YCSB && nohup ./bin/ycsb run redis -s -P /root/YCSB/workloads/workloada -p operationcount=50000 -p redis.host={VIP_IP} -p redis.port=6379 > /root/YCSB//logs/outputRun.txt 2>&1 &",target_ip=CLIENT_IP)

def execute_tests(args):
    tool = args.tool.lower()
    if tool == "ycsb":
        execute_ycsb_tests()
    pass

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

        base_cmds.extend([
            f"sudo tc class add dev {interface} parent 1: classid {classid} htb rate {rate}",
            f"sudo tc filter add dev {interface} protocol ip parent 1:0 prio 1 u32 match ip dst {dst} flowid {classid}",
            f"sudo tc qdisc add dev {interface} parent {classid} handle {handle} netem delay {delay}",
        ])

    # 执行命令 (本地或远程)
    for cmd in base_cmds:
        print(f"Executing: {cmd}")
        if is_remote:
            run_remote_cmd(cmd, target_ip=target_ip, ignore_error=ignore_error)
        else:
            run_cmd(cmd, ignore_error=ignore_error)


def clean_configure_network():
    # 清空网络配置
    run_cmd("sudo tc qdisc del dev ens33 root",ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev ens33 root",target_ip=DEST_IP,ignore_error=True)
    if VIP_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root",target_ip=CLIENT_IP,ignore_error=True)

def configure_network():
    source_rules = [
    {"rate": "50mbit", "delay": "0.5ms", "dst": DEST_IP}
]
    dest_rules = [
    {"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP}
]
    if VIP_IP:
        source_rules.append({"rate": "50mbit", "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": "50mbit", "delay": "0.05ms", "dst": CLIENT_IP})

  # 配置source的网络限制  source->dest  source->client
    configure_network_do(
    interface="ens33",
    rules=source_rules,
    is_remote=False  # 本地执行
)
 # 配置dest的网络限制  dest->source  dest->client
    configure_network_do(
    interface="ens33",
    rules=dest_rules,
    is_remote=True,  # 远程执行
    target_ip=SOURCE_IP  # 远程执行命令机器 IP
)
    # 配置client的网络限制  client->vip
    if VIP_IP:
        configure_network_do(
        interface="ens33",
        rules=[
            {"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP},
            {"rate": "50mbit", "delay": "0.05ms", "dst": DEST_IP}
        ],
        is_remote=True,  # 远程执行
        target_ip=CLIENT_IP  # 远程执行命令机器 IP
    )

# 定义实验类型与参数
experiments = {
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    "post-copy": "-post -d --tcp-established --shell-job",
    "hybrid": "-pre -post -d --tcp-established --shell-job",
    "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}


# 每种实验进行5次
runs = 5

# 主流程
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Automate container migration using runc and CRIU.")
    parser.add_argument("-c", "--container", required=True, help="The name of the container to migrate.")
    parser.add_argument("-t", "--tool", required=False, help="Specify the test tool to use.")
    parser.add_argument("-s", "--source-ip", required=True, help="IP address of the source machine.")
    parser.add_argument("-d", "--dest-ip", required=True, help="IP address of the destination machine.")
    parser.add_argument("--client-ip", required=False, help="IP address of the machine running client such as ycsb.")
    parser.add_argument("--virtual-ip", required=False, help="virtual ip ")
    args = parser.parse_args()


    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip
    VIP_IP = args.virtual_ip
    #print(DEST_IP)
    #input()
    for exp_name, exp_args in experiments.items():
        for i in range(1, runs+1):
            print(f"======== Running {exp_name} experiment run {i} ========")

            try:
                # 准备目标节点的资源 (复制新的bundle、开启console.sock)
                destination_prepare(args)

                # 准备源节点的资源 (复制新的bundle、开启console.sock、启动容器)
                source_prepare(args)


                    # 还原keepalived配置
                #update_keepalived_priority(100)
                #update_keepalived_priority(50,True,DEST_IP)
                # 开启网络资源限制
                configure_network()

                # 开启工具测试
               # execute_tests(args)

                # 执行迁移
                source_run_migration(args,exp_args)
                #input()
                print(f"======== Finished {exp_name} experiment run {i} ========")
            except Exception as e:
                # 捕获异常并打印错误信息
                print(f"Error during experiment '{exp_name}' run {i}: {e}")
            finally:
                # 无论前面是否出错，清理源和目标节点资源
                print("Cleaning up source and destination resources...")
                #input()
                source_clean(args)
                destination_clean(args)
                clean_configure_network()

            print(f"======== sleep ========")
            # time.sleep(30000) #
            input()


