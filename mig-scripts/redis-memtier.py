import argparse
import re
import subprocess
import sys
import time
from typing import Optional
from result_writer import extract_stats_from_output, append_result

from cmd_utils import run_cmd, run_remote_cmd, unmount_local_migration_tmpfs

# default script selection
from script_defaults import choose_scripts, get_default_ips

# 默认设置（从集中 defaults 读取）
SOURCE_IP, DEST_IP, CLIENT_IP, VIP = get_default_ips()
YCSB_IP = CLIENT_IP  # 保持向后兼容性

# default bandwidth
BANDWIDTH = "25mbit"

SOURCE_SCRIPT, DEST_SCRIPT = choose_scripts(False)
SEC_MODE = False
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
    parser.add_argument("--runs", type=int, default=5, help="Number of experimental runs per experiment type.")
    parser.add_argument("--sec", action="store_true", help="use source-sec/destination-sec scripts")
    parser.add_argument("--bandwidth", default="25mbit", help="Network bandwidth limit (e.g. 25mbit). Default: 25mbit")

    parsed_args = parser.parse_args()

    # 全局变量
    SOURCE_IP = parsed_args.source_ip
    DEST_IP = parsed_args.dest_ip
    CLIENT_IP = parsed_args.client_ip  # [memtier] changed
    VIP = parsed_args.vip
    runs = parsed_args.runs
    BANDWIDTH = parsed_args.bandwidth
    # set source script selection
    sec_enabled = getattr(parsed_args, "sec", False)
    src, dst = choose_scripts(sec_enabled)
    globals()["SOURCE_SCRIPT"] = src
    globals()["DEST_SCRIPT"] = dst
    globals()["SEC_MODE"] = sec_enabled

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
    "pre-copy": "-pre -d --tcp-established --shell-job",
    "pre-copy-1": "-pre -d --tcp-established --shell-job -z 1",
    "pre-copy-2": "-pre -d --tcp-established --shell-job -z 2",
    "pre-copy-3": "-pre -d --tcp-established --shell-job -z 3",
    "pre-copy-4": "-pre -d --tcp-established --shell-job -z 4",
    # "pre-copy-dirtymap": "-pre -d -dm --tcp-established --shell-job",
    # "hybrid": "-pre -post -d --tcp-established --shell-job",
    # "hybrid-dirtymap": "-pre -post -d -dm --tcp-established --shell-job"
}
def run_client_cmd(cmd, *, ignore_error=False, background=False, quiet=False):
    """Execute a command on the memtier client host."""
    if not CLIENT_IP:
        raise ValueError("CLIENT_IP is not configured for run_client_cmd")

    remote_cmd = cmd
    if background:
        remote_cmd = f"nohup {cmd} < /dev/null & echo $!"

    result = run_remote_cmd(remote_cmd, CLIENT_IP, ignore_error=ignore_error, quiet=quiet)
    if not ignore_error and getattr(result, "returncode", 0) != 0:
        sys.exit(result.returncode or 1)
    return result


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

    # 兜底：强制清理遗留的 destination.py/destination-sec.py，避免端口被占用
    kill_leftover = (
        "ps aux | grep -E 'mig-scripts/(destination|destination-sec)\\.py' | "
        "grep -v grep | awk '{print $2}' | xargs -r kill -9"
    )
    run_remote_cmd(kill_leftover, target_ip=DEST_IP, ignore_error=True)

    # recvtty_cmd = (
    #     f"PATH=$PATH:/root/go/bin recvtty -m null /runc/containers/{container_name}/console.sock "
    #     "> /tmp/recvtty_debug.log 2>&1 & & echo $! > /tmp/recvtty_source.pid"
    # )
    # run_remote_cmd(recvtty_cmd, target_ip=DEST_IP, ignore_error=False)
    recvtty_cmd = (
        "PATH=$PATH:/root/go/bin "
        f"nohup recvtty -m null /runc/containers/{container_name}/console.sock "
        "> /tmp/recvtty_debug.log 2>&1 & echo $! > /tmp/recvtty_dest.pid"
    )
    run_remote_cmd(recvtty_cmd, target_ip=DEST_IP, ignore_error=False)
    # 启动 destination 后台进程以接收归档，并把输出写入 /tmp（可通过 --sec 切换）
    ts = int(time.time())
    dest_script = globals().get("DEST_SCRIPT", "destination.py")
    script_tag = dest_script.replace('.', '_')
    dest_log = f"/tmp/{script_tag}_{container_name}_{ts}.log"
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
        # 打印远端诊断信息，帮助排查
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

    # 停止 destination 后台进程（如果存在）并移除 pid 文件
    dest_pidfile = f"/tmp/destination_{container_name}.pid"
    stop_cmd = (
        f"if [ -f {dest_pidfile} ]; then kill -TERM $(cat {dest_pidfile}) 2>/dev/null || true; rm -f {dest_pidfile}; fi"
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
    container_name = "redis"
    cmds = [
        # 准备新的bundle
        (f"rm -rf /runc/containers/{container_name}", False),
        (f"cp -r /runc/containers/{container_name}.bak /runc/containers/{container_name}", False),
        # 启动console.sock并把进程号存储起来,后续清理时kill掉
        (
            (
                f"nohup recvtty -m null /runc/containers/{container_name}/console.sock "
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


def source_clean():
    """在源节点清理资源"""
    container_name = "redis"
    # 清理console.sock
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null", ignore_error=True)
    # 清理 dirtypages的挂载
    unmount_local_migration_tmpfs(container_name)
    run_cmd(f"runc kill {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
    run_cmd(f"runc delete {container_name}", ignore_error=True)  # 如果容器不存在可忽略错误
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


def configure_network_do(interface, rules, is_remote=False, target_ip=None, ignore_error=False):
    """Apply tc shaping rules locally or on a remote host."""
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
    """Remove tc shaping rules from all participating hosts."""
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


def source_run_migration(exp_args, run_index=0, exp_name="unknown"):
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
    result = run_cmd(migration_cmd)

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
            time.sleep(7)  #
