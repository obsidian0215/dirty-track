import subprocess
import sys

DEST_IP = "192.168.37.157"

# 定义实验类型与参数
experiments = {
    "pre-copy": "-pre -d",
    "pre-copy-dirtymap": "-pre -d -dm",
    "post-copy": "-post -d",
    "hybrid": "-pre -post -d",
    "hybrid-dirtymap": "-pre -post -d -dm"
}

runs = 3

def run_cmd(cmd, ignore_error=False):
    print("Executing on source:", cmd)
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)

def run_remote_cmd(cmd, ignore_error=False):
    full_cmd = f"ssh {DEST_IP} '{cmd}'"
    print("Executing remotely:", full_cmd)
    result = subprocess.run(full_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 and not ignore_error:
        print("Remote command failed with error:", result.stderr)
        sys.exit(1)
    else:
        print(result.stdout)

def destination_prepare():
    # 在目标节点执行准备操作
    # 有些命令可能出现非致命错误，用ignore_error=True
    cmds = [
      # 假设pkill可能失败（如果进程不存在），不想中断脚本执行，使用ignore_error=True
       # ("sudo pkill -SIGKILL -f '^/bin/dirty-pages 300 20'", True),
       ("sudo pkill -SIGKILL -f 'dirty-pages'", True),
        ("runc kill dirty-pages", True),  # 如果容器不存在可忽略错误
        ("runc delete dirty-pages", True),
        ("rm -rf /runc/containers/dirty-pages", True),  # 如果文件不存在也不是致命错误
        ("cp -r /runc/containers/dirty-pages.bak /runc/containers/dirty-pages", False),
        ("nohup recvtty -m null /runc/containers/dirty-pages/console.sock > /dev/null 2>&1 & echo $! > /tmp/recvtty.pid", False)
    ]
    for c, ign in cmds:
        run_remote_cmd(c, ignore_error=ign)


def destination_recover():
    # 恢复过程结束时杀死recvtty进程

    # 杀死recvtty进程
    run_remote_cmd("kill -9 $(cat /tmp/recvtty.pid) 2>/dev/null || true", ignore_error=False)
    run_remote_cmd("rm -f /tmp/recvtty.pid", ignore_error=False)


def source_prepare():
    # 源节点准备，某些命令也可能非致命错误
    cmds = [
        ("umount /runc/containers/dirty-pages/migrate/*",True),
        ("rm -rf /runc/containers/dirty-pages", True),
        ("cp -r /runc/containers/dirty-pages.bak /runc/containers/dirty-pages", False),
        ("nohup recvtty -m null /runc/containers/dirty-pages/console.sock  > /dev/null 2>&1 & echo $! > /tmp/recvtty_source.pid", False)
    ]
    for c, ign in cmds:
        run_cmd(c, ignore_error=ign)

def source_run_migration(exp_args):
    # 启动容器
    run_cmd("runc run --console-socket /runc/containers/dirty-pages/console.sock -d -b /runc/containers/dirty-pages dirty-pages")
    # 执行source.py进行迁移
    cmd = f"python3 source.py dirty-pages {DEST_IP} {exp_args}"
    run_cmd(cmd)
    run_cmd("kill -9 $(cat /tmp/recvtty_source.pid) 2>/dev/null || true", ignore_error=False)
    run_cmd("rm -f /tmp/recvtty_source.pid", ignore_error=False)

# 主流程
for exp_name, exp_args in experiments.items():
    for i in range(1, runs+1):
        print(f"======== Running {exp_name} experiment run {i} ========")

        # 准备目标节点
        destination_prepare()

        # 准备源节点
        source_prepare()

        # 执行迁移（此步骤结束说明source.py执行完成，迁移完成）
        source_run_migration(exp_args)

        # 迁移完成后调用目标节点恢复
        destination_recover()

        print(f"======== Finished {exp_name} experiment run {i} ========")


