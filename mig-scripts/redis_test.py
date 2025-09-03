import subprocess
import sys
import argparse
import re
import time

# 默认设置
SOURCE_IP = "192.168.15.199"
DEST_IP = "192.168.15.239"
CLIENT_IP = "192.168.15.181"
VIP = "192.168.15.100"

# 场景配置：Redis的video和sensor场景
scene_configs = {
    'video': {
        'bench': 'experiment/migration/redis/bench_video_cache.py',
        'base_args': {
            '--redis-host': '127.0.0.1',
            '--redis-port': '6379',
            '--threads': '4',
            '--duration': '10',
            '--write-pct': '80',
            '--ttl': '60'
        }
    },
    'sensor': {
        'bench': 'experiment/migration/redis/bench_sensoragg.py',
        'base_args': {
            '--redis-host': '127.0.0.1',
            '--redis-port': '6379',
            '--threads': '4',
            '--duration': '30',
            '--payload-size-kb': '2',
            '--sensors-per-device': '5'
        }
    }
}

def run_cmd(cmd, ignore_error=False):
    print("Executing:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)
    return result

def destination_prepare():
    container_name = "redis"
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)

    recvtty_cmd = f"PATH=$PATH:/root/go/bin recvtty -m null /runc/containers/{container_name}/console.sock > /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_destination.pid"
    run_remote_cmd(recvtty_cmd, DEST_IP, ignore_error=False)

def destination_clean():
    container_name = "redis"
    cmds = [
        (f"runc kill {container_name}", True),
        (f"runc delete {container_name}", True),
        (f"kill -9 $(cat /tmp/recvtty_destination.pid) 2>/dev/null", True),
        (f"ps aux | grep 'recvtty' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9", True),
    ]
    for c, ign in cmds:
        run_remote_cmd(c, DEST_IP, ignore_error=ign)

def source_prepare():
    container_name = "redis"
    cmds = [
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        (f"nohup recvtty -m null /runc/containers/{container_name}/console.sock > /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid", False),
        (f"runc run --console-socket /runc/containers/{container_name}/console.sock -d -b /runc/containers/{container_name} {container_name}", False)
    ]
    for c, ign in cmds:
        run_cmd(c, ignore_error=ign)

def source_clean():
    container_name = "redis"
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    run_cmd(f"umount /runc/containers/{container_name}/migrate/*", ignore_error=True)
    run_cmd(f"runc kill {container_name}", ignore_error=True)
    run_cmd(f"runc delete {container_name}", ignore_error=True)
    run_cmd(f"ps aux | grep 'inotifywait' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9", ignore_error=True)
    run_cmd(f"ps aux | grep 'sync_rootfs' | grep -v grep | awk '{{print $2}}' | xargs -r kill -9", ignore_error=True)

def update_keepalived_priority(new_priority, is_remote=False, target_ip=None):
    config_path = '/etc/keepalived/keepalived.conf'
    pattern = r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})'

    restart_cmd = "sudo systemctl restart keepalived"
    status_cmd = "sudo systemctl is-active keepalived"

    try:
        if is_remote:
            if not target_ip:
                raise ValueError("Target IP required for remote")
            result = subprocess.run(f"ssh {target_ip} 'cat {config_path}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print("Failed to read remote config:", result.stderr)
                return False
            content = result.stdout
            updated_content = re.sub(pattern, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", content)
            write_result = subprocess.run(f"ssh {target_ip} \"echo '{updated_content}' > {config_path}\"", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if write_result.returncode != 0:
                print("Failed to write remote config:", write_result.stderr)
                return False
            result = subprocess.run(f"ssh {target_ip} '{restart_cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print("Failed to restart remote keepalived:", result.stderr)
                return False
            status = subprocess.run(f"ssh {target_ip} '{status_cmd}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if status.returncode == 0 and status.stdout.strip() == "active":
                print("Keepalived restarted successfully on remote.")
                return True
            else:
                print("Keepalived failed to restart on remote.")
                return False
        else:
            with open(config_path, 'r') as f:
                content = f.read()
            updated_content = re.sub(pattern, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", content)
            with open(config_path, 'w') as f:
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
    run_cmd("sudo tc qdisc del dev ens33 root", ignore_error=True)
    run_remote_cmd("sudo tc qdisc del dev ens33 root", DEST_IP, ignore_error=True)
    if CLIENT_IP:
        run_remote_cmd("sudo tc qdisc del dev ens33 root", CLIENT_IP, ignore_error=True)

def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """配置网络规则，使用 tc 命令设置带宽和延迟"""
    if is_remote and not target_ip:
        raise ValueError("Target IP must be provided for remote execution.")

    base_cmds = [
        "sudo tc qdisc add dev ens33 root handle 1: htb",
    ]

    for idx, rule in enumerate(rules, start=1):
        classid = f"1:{idx}"
        handle = f"{10 * idx}:"
        rate = rule["rate"]
        delay = rule["delay"]
        dst = rule["dst"]

        base_cmds.extend([
            f"sudo tc class add dev ens33 parent 1: classid {classid} htb rate {rate}",
            f"sudo tc filter add dev ens33 protocol ip parent 1:0 prio 1 u32 match ip dst {dst} flowid {classid}",
            f"sudo tc qdisc add dev ens33 parent {classid} handle {handle} netem delay {delay}",
        ])

    for cmd in base_cmds:
        print(f"Executing: {cmd}")
        if is_remote:
            run_remote_cmd(cmd, target_ip=target_ip, ignore_error=ignore_error)
        else:
            run_cmd(cmd, ignore_error=ignore_error)

def configure_network():
    """配置网络限制"""
    source_rules = [
        {"rate": "50mbit", "delay": "0.5ms", "dst": DEST_IP}
    ]
    dest_rules = [
        {"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP}
    ]
    if CLIENT_IP:
        source_rules.append({"rate": "50mbit", "delay": "0.5ms", "dst": CLIENT_IP})
        dest_rules.append({"rate": "50mbit", "delay": "0.05ms", "dst": CLIENT_IP})

    # 配置source的网络限制
    configure_network_do(
        interface="ens33",
        rules=source_rules,
        is_remote=False  # 本地执行
    )

    # 配置dest的网络限制
    configure_network_do(
        interface="ens33",
        rules=dest_rules,
        is_remote=True,  # 远程执行
        target_ip=DEST_IP
    )

    # 配置ycsb的网络限制
    if CLIENT_IP:
        configure_network_do(
            interface="ens33",
            rules=[
                {"rate": "50mbit", "delay": "0.5ms", "dst": SOURCE_IP},
                {"rate": "50mbit", "delay": "0.05ms", "dst": DEST_IP}
            ],
            is_remote=True,  # 远程执行
            target_ip=CLIENT_IP
        )

def run_remote_cmd(cmd, target_ip, ignore_error=False, background=False):
    """Execute command on remote machine."""
    if background:
        cmd = f"nohup {cmd}  &"

    full_cmd = f"ssh {target_ip} '{cmd}'"
    print("Executing remotely:", end=' ')
    print(f"(on {target_ip}):", cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)

# 定义实验类型
experiments = {
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    "post-copy": "-post -d --tcp-established --shell-job",
    "hybrid": "-pre -post -d --tcp-established --shell-job",
    "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}

def source_run_migration(exp_args, scene_config, extra_args):
    container_name = "redis"
    time.sleep(6)  # 等待容器启动稳定

    print("Running bench test on the client machine...")
    run_remote_cmd('pkill -f "python.*bench"', CLIENT_IP, ignore_error=True)

    # 设置环境变量并执行bench（load）
    env_setup = "cd /root/dirty-track"
    bench_cmd = f"python3 {scene_config['bench'].split('/')[-1]} {' '.join([f'{k} {v}' for k, v in extra_args.items()])}"

    full_bench_cmd = f"{env_setup} && {bench_cmd}"
    run_remote_cmd(full_bench_cmd, CLIENT_IP)

    # 在load和run之间设置网络配置
    configure_network()
    print("Network configuration applied between bench load and run.")

    time.sleep(3)  # 等待bench启动稳定

    # 执行source.py进行迁移
    migration_cmd = f"python3 source.py {exp_args} {container_name} {DEST_IP}"
    run_cmd(migration_cmd)

    # clean
    cleanup_cmd = "kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true"
    run_cmd(cleanup_cmd, ignore_error=False)

def main():
    parser = argparse.ArgumentParser(description="Redis自动化负载测试脚本")
    parser.add_argument("-s", "--source-ip", default=SOURCE_IP, help="迁移源IP")
    parser.add_argument("-d", "--dest-ip", default=DEST_IP, help="迁移目标IP")
    parser.add_argument("-c", "--client-ip", default=CLIENT_IP, help="客户端IP")
    parser.add_argument("--scene", choices=['video', 'sensor'], required=True, help="场景: video或sensor")
    parser.add_argument("--redis-port", type=int, default=6379, help="Redis端口")
    parser.add_argument("--threads", type=int, help="线程数")
    parser.add_argument("--duration", type=int, help="测试时长(s)")
    parser.add_argument("--write-pct", type=int, help="写操作百分比 (video场景)")
    parser.add_argument("--ttl", type=int, help="TTL (video场景)")
    parser.add_argument("--payload-size-kb", type=int, help="负载大小(KB)")
    parser.add_argument("--sensors-per-device", type=int, help="每设备传感器数")
    parser.add_argument("--runs", type=int, default=1, help="每个实验类型的运行次数")
    parser.add_argument("--experiment-types", nargs='*', choices=list(experiments.keys()),
                       default=list(experiments.keys()), help="要运行的迁移实验类型，默认全部")
    args = parser.parse_args()

    # 使用参数值更新全局变量
    global SOURCE_IP, DEST_IP, CLIENT_IP
    SOURCE_IP = args.source_ip
    DEST_IP = args.dest_ip
    CLIENT_IP = args.client_ip

    experiment_types_to_run = args.experiment_types if args.experiment_types else list(experiments.keys())

    # 主流程，支持多个实验类型和循环
    for exp_name in experiment_types_to_run:
        exp_args = experiments[exp_name]
        print(f"================ Running '{exp_name}' experiment ===============")

        for run_num in range(1, args.runs + 1):
            print(f"-------- Experiment {exp_name}, run {run_num} --------")
            try:
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
                extra_args = {
                    '--redis-host': args.redis_host,
                    '--redis-port': args.redis_port,
                    '--threads': args.threads,
                    '--duration': args.duration,
                }
                if args.scene == 'video':
                    extra_args['--write-pct'] = args.write_pct
                    extra_args['--ttl'] = args.ttl
                elif args.scene == 'sensor':
                    extra_args['--payload-size-kb'] = args.payload_size_kb
                    extra_args['--sensors-per-device'] = args.sensors_per_device

                # 执行迁移（包含bench测试）
                source_run_migration(exp_args, scene_config, extra_args)
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

                time.sleep(17)  # 等待清理缓冲

        print(f"================ Finished {exp_name} experiment ===============")

    print("All experiments completed.")

if __name__ == "__main__":
    main()