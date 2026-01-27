#!/usr/bin/env python
# code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
# import distutils.util
import argparse
import atexit
import json
import os
import re
import select
import shlex
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
try:
    # fcntl is POSIX-only (Linux/Unix). Wrap import to allow static analysis on other platforms.
    import fcntl  # type: ignore
    from fcntl import ioctl  # type: ignore
except Exception:
    fcntl = None
    ioctl = None

try:
    import psutil
except Exception:
    psutil = None

import importlib.util
import datetime

# centralized monitor module will be imported on-demand inside the monitoring start block

# 定义字符设备路径
DEVICE_PATH = "/dev/dirty-track"

# 定义ioctl命令相关参数
IOC_NRBITS = 8
IOC_TYPEBITS = 8
IOC_SIZEBITS = 14
IOC_DIRBITS = 2

IOC_NRSHIFT = 0
IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS  # 8
IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS  # 16
IOC_DIRSHIFT = IOC_SIZESHIFT + IOC_SIZEBITS  # 30

IOC_NONE = 0
IOC_WRITE = 1
IOC_READ = 2

IOC_IN = IOC_WRITE << IOC_DIRSHIFT
IOC_OUT = IOC_READ << IOC_DIRSHIFT
IOC_IO = (IOC_WRITE | IOC_READ) << IOC_DIRSHIFT


def _IO(type, nr):
    return IOC_NONE | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT)


def _IOR(type, nr, size):
    return IOC_OUT | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT)


def _IOW(type, nr, size):
    return IOC_IN | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT)


def _IOWR(type, nr, size):
    return IOC_IO | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT)


# 定义 ioctl 命令
DIRTY_TRACK_MAGIC = ord("d")
IOCTL_SET_DIRTY_MAP_PATH = _IOW(DIRTY_TRACK_MAGIC, 1, 256)
IOCTL_START_PID = _IOW(DIRTY_TRACK_MAGIC, 2, 4)
IOCTL_STOP_PID = _IOW(DIRTY_TRACK_MAGIC, 3, 4)
IOCTL_CHECK_PID = _IOWR(DIRTY_TRACK_MAGIC, 4, 5)
IOCTL_GET_DIRTY_MAP_PATH = _IOR(DIRTY_TRACK_MAGIC, 5, 256)

# 定义容器进程树的 pid 列表
container_pids = []

# 全局变量用于跟踪 sync_rootfs 进程
sync_rootfs_process = None
sync_rootfs_log_file = None
pre_dump_iters = 0

# Module-level defaults used by functions that declare these names as `global`.
# These ensure static analyzers (flake8/pyflakes) see the names assigned at module
# scope and avoid F824 reports when functions reference them.
pre_dump_time_total = 0.0
pre_dump_size_total = 0
esti_dump_time = 0.0
esti_dump_size_pre = 0.0
esti_dump_size_post = 0.0
dump_size = 0.0
dump_xfer_time = 0.0
max_predump_size = 0.0
final_archive_size_bytes = 0.0  # 记录最终 dump 压缩包的大小，便于统计时使用

def get_compressed_files_size(directory, compress_level):
    """
    计算目录下 (仅限顶层) 的压缩文件总大小。
    根据 compress_level 决定是查找 .tar 还是 .lzo。
    """
    total_size = 0
    if not os.path.isdir(directory):
        return 0

    # 根据压缩级别确定要查找的文件后缀
    if compress_level == 0:
        suffix_to_find = ".tar"
    elif compress_level >= 1:
        suffix_to_find = ".lzo"
    else:
        # 如果是负数或无效值（尽管 argparse 限制了），不计算
        return 0

    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                # [修改] 只查找当前运行所对应的压缩文件类型
                if entry.is_file() and not entry.is_symlink() and entry.name.endswith(suffix_to_find):
                    total_size += entry.stat().st_size
    except Exception as e:
        print(f"Error calculating compressed file size in {directory}: {e}")

    return total_size


# 停止 sync_rootfs 进程的函数
def stop_sync_rootfs():
    """停止 sync_rootfs 进程及其所有子进程（包括rsync进程和后台定时器）"""
    global sync_rootfs_process, sync_rootfs_log_file

    if sync_rootfs_process:
        try:
            print("正在停止 sync_rootfs 进程及其所有子进程...")

            # 终止主进程
            sync_rootfs_process.terminate()

            # 等待进程终止，最多等待5秒
            sync_rootfs_process.wait(timeout=5.0)
            print("sync_rootfs 主进程已终止")

            # 使用系统命令清理残留的子进程
            try:
                # 获取父进程PID并查找所有子进程
                if hasattr(sync_rootfs_process, "pid") and sync_rootfs_process.pid:
                    pid = sync_rootfs_process.pid
                    # 查找并终止所有相关进程（ps -列出进程，grep -筛选，awk -提取PID，xargs -传递PID给kill）
                    _ = subprocess.run(f"pkill -P {pid} || true", shell=True, capture_output=True, text=True)
                    print("已清理 sync_rootfs.sh 的所有子进程")

                    # 使用进程组ID来确保清理所有后台进程和子进程
                    try:
                        # 获取进程组ID
                        proc = subprocess.run(["ps", "-p", str(pid), "-o", "pgid="], capture_output=True, text=True)
                        if proc.returncode == 0 and proc.stdout.strip():
                            pgid = proc.stdout.strip()
                            print(f"清理进程组 {pgid}")
                            # 发送SIGKILL到整个进程组
                            subprocess.run(["kill", "-KILL", "-" + pgid], capture_output=True, text=True)
                    except Exception as e:
                        print(f"清理进程组时出现警告: {e}")

            except Exception as e:
                print(f"清理子进程时出现警告（这通常没有问题）: {e}")

        except subprocess.TimeoutExpired:
            print("警告：sync_rootfs 进程无法正常终止，强制杀死")
            try:
                sync_rootfs_process.kill()
                sync_rootfs_process.wait(timeout=2.0)
                print("sync_rootfs 主进程已被强制杀死")

                # 再次尝试清理子进程
                if hasattr(sync_rootfs_process, "pid") and sync_rootfs_process.pid:
                    _ = subprocess.run(
                        f"pkill -P {sync_rootfs_process.pid} || true", shell=True, capture_output=True, text=True
                    )

                    # 使用进程组ID强制清理所有相关进程
                    try:
                        proc = subprocess.run(
                            ["ps", "-p", str(sync_rootfs_process.pid), "-o", "pgid="], capture_output=True, text=True
                        )
                        if proc.returncode == 0 and proc.stdout.strip():
                            pgid = proc.stdout.strip()
                            print(f"强制清理进程组 {pgid}")
                            subprocess.run(["kill", "-KILL", "-" + pgid], capture_output=True, text=True)
                    except Exception as e:
                        print(f"强制清理进程组时出现警告: {e}")

            except subprocess.TimeoutExpired:
                print("错误：无法杀死 sync_rootfs 进程的所有子进程")

        except Exception as e:
            print(f"停止 sync_rootfs 进程时发生错误: {e}")

        finally:
            sync_rootfs_process = None

    # 确保清理所有残留的sync进程
    try:
        # 查找所有剩余的sync_rootfs.sh进程并强制杀死
        remaining_proc = subprocess.run("pgrep -f sync_rootfs.sh || true", shell=True, capture_output=True, text=True)
        if remaining_proc.returncode == 0 and remaining_proc.stdout.strip():
            remaining_pids = remaining_proc.stdout.strip().split("\n")
            for pid in remaining_pids:
                try:
                    subprocess.run(["kill", "-KILL", pid.strip()], capture_output=True, text=True)
                    print(f"清理残留 sync_rootfs.sh 进程 {pid.strip()}")
                except Exception as e:
                    print(f"清理残留进程 {pid.strip()} 时错误: {e}")
    except Exception as e:
        print(f"查找残留进程时出现错误: {e}")

    if sync_rootfs_log_file:
        try:
            sync_rootfs_log_file.close()
            print("sync_rootfs 日志文件已关闭")
        except Exception as e:
            print(f"关闭 sync_rootfs 日志文件时发生错误: {e}")
        finally:
            sync_rootfs_log_file = None


# 信号处理器函数
def signal_handler(signum, frame):
    """处理 сигнал终止"""
    print(f"\n接收到信号 {signum}，正在清理并退出...")
    stop_sync_rootfs()
    sys.exit(0)


# [新] 添加这个函数
def get_lzo_files_size(directory):
    """
    计算目录下 (仅限顶层) 所有 .lzo 文件的总大小。
    这些文件是 xfer_pre_dump 和 xfer_final 创建的压缩包。
    """
    total_size = 0
    if not os.path.isdir(directory):
        print(f"Warning: Directory not found, cannot calculate LZO size: {directory}")
        return 0

    try:
        # 我们只扫描顶层目录，因为 .lzo 文件都存储在 mig_base 下
        with os.scandir(directory) as entries:
            for entry in entries:
                # 确保是文件、非链接且以 .lzo 结尾
                if entry.is_file() and not entry.is_symlink() and entry.name.endswith(".lzo"):
                    total_size += entry.stat().st_size
    except Exception as e:
        print(f"Error calculating LZO file size in {directory}: {e}")

    return total_size


# [新函数结束]
def final_sync_es_data(dest_ip, rootfs_path):
    """
    在 restore 之前，对 data 目录做一次“点名同步”，避免缺失 indices/*/index/*.lock 等深层文件。
    """
    import os
    import subprocess

    src_data = os.path.join(rootfs_path, "usr/share/elasticsearch/data") + "/"
    dst_data = f"root@{dest_ip}:{src_data}"

    # 确保目标端父目录存在
    subprocess.run(f"ssh {dest_ip} 'sudo mkdir -p {src_data}'", shell=True, check=False, text=True)

    # 做一次强同步（参数更稳健：权限/属性/uidgid 就位；inplace 避免重写；delete-delay 降低瞬时空窗）
    cmd = ["rsync", "-aHAX", "--numeric-ids", "--inplace", "--delete-delay", "-P", "--timeout=0", src_data, dst_data]
    print("[final_sync_es_data] running:", " ".join(cmd))
    subprocess.check_call(cmd)


# [tang change]定义全局变量用于累计预拷贝时间和大小
pre_dump_time_total = 0.0  # 毫秒
pre_dump_size_total = 0.0  # 字节
pre_dump_xfer_time_total = 0.0  # 毫秒

# [新] 定义全局变量用于累计压缩时间
total_compression_time = 0.0  # 毫秒
# 定义全局变量用于记录最后一次dump的时间和大小
dump_time = 0.0  # 毫秒
dump_size = 0.0  # 字节
dump_xfer_time = 0.0  # 毫秒
rst_time = 0.0
# post
total_uffd_copy = 0.0
rpf_handle_time = 0.0

# 预估最后一次dump的时间和各种大小
esti_dump_time = 0.0
esti_dump_size_pre = 0.0
esti_dump_size_post = 0.0
max_predump_size = 0.0  # 跟踪predump的最大大小

PAGE_SIZE = 4096  # 每页大小为4KB

# 初始化迭代和处理过的dirtymap文件
iter_dirtymaps = []
processed_files = set()

bandwidth_measurements = []  # List to store individual bandwidth measurements (Bytes/s)
average_bandwidth = 0.0  # Average bandwidth (Bytes/s)
bandwidth_stddev = 0.0  # Standard deviation of bandwidth (Bytes/s)

CONTROL_COMMAND_TIMEOUT = 30.0  # seconds


def _recv_control_json(sock, timeout):
    """Receive a JSON response (or simple status string) from the control channel."""
    deadline = time.monotonic() + timeout
    buffer = bytearray()

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        readable, _, _ = select.select([sock], [], [], remaining)
        if not readable:
            continue
        chunk = sock.recv(4096)
        if not chunk:
            break
        buffer.extend(chunk)

        try:
            text = buffer.decode("utf-8")
        except UnicodeDecodeError:
            # Wait for more data if partial multibyte sequence
            continue

        stripped = text.strip()
        if not stripped:
            continue

        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            status_upper = stripped.upper()
            if status_upper in {"OK", "ERROR"}:
                msg = None if status_upper == "OK" else stripped
                return {"status": status_upper, "message": msg}
            # Not a full JSON payload yet, continue reading
            continue

    if buffer:
        text = buffer.decode("utf-8", errors="replace").strip()
        if text:
            status_upper = text.upper()
            if status_upper in {"OK", "ERROR"}:
                msg = None if status_upper == "OK" else text
                return {"status": status_upper, "message": msg}
            return {"status": "ERROR", "message": text}

    raise TimeoutError("Timed out waiting for control channel response")


def _send_control_command(sock, payload, timeout=CONTROL_COMMAND_TIMEOUT):
    if isinstance(payload, (bytes, bytearray)):
        message_bytes = bytes(payload)
    elif isinstance(payload, str):
        message_bytes = payload.encode("utf-8")
    else:
        message_bytes = json.dumps(payload).encode("utf-8")
    sock.sendall(message_bytes)
    return _recv_control_json(sock, timeout)


def _await_transfer_completion(control_sock, token, timeout=180.0):
    request = {
        "transfer_status": {
            "token": token,
            "timeout_ms": int(max(timeout, 0) * 1000),
        }
    }
    response = _send_control_command(control_sock, request, timeout=max(timeout + 5.0, CONTROL_COMMAND_TIMEOUT))
    status = response.get("status")
    if status in ("OK", "N/A"):
        return response
    raise RuntimeError(f"transfer {token} failed: {response}")


def _stream_file_to_socket(file_path, host, port):
    total_sent = 0
    start = time.perf_counter()
    with socket.create_connection((host, port)) as data_sock, open(file_path, "rb") as fp:
        while True:
            chunk = fp.read(1024 * 1024)
            if not chunk:
                break
            data_sock.sendall(chunk)
            total_sent += len(chunk)
    duration_ms = (time.perf_counter() - start) * 1000.0
    return total_sent, duration_ms


# 通过ioctl设置脏页跟踪的目录路径
def ioctl_set_dirty_map_path(device_fd, path):
    if ioctl is None:
        raise RuntimeError("ioctl support is unavailable on this platform")
    # 路径字符串打包为定长字节数组
    buf = struct.pack(f"{len(path)}s", path.encode("utf-8"))
    # 调用 ioctl 传递路径给内核模块
    ioctl(device_fd, IOCTL_SET_DIRTY_MAP_PATH, buf)


# 通过ioctl启动指定进程的脏页跟踪
def ioctl_start_pid(device_fd, pid):
    if ioctl is None:
        raise RuntimeError("ioctl support is unavailable on this platform")
    # pid_t在Python中可以用struct.pack来打包
    buf = bytearray(struct.pack("I", pid))
    ioctl(device_fd, IOCTL_START_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]


# 通过ioctl停止指定进程的脏页跟踪
def ioctl_stop_pid(device_fd, pid):
    if ioctl is None:
        raise RuntimeError("ioctl support is unavailable on this platform")
    buf = bytearray(struct.pack("I", pid))
    ioctl(device_fd, IOCTL_STOP_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]


# 通过ioctl获取脏页跟踪的目录路径
def ioctl_get_dirty_map_path(device_fd):
    if ioctl is None:
        raise RuntimeError("ioctl support is unavailable on this platform")
    buf = bytearray(struct.pack("256s", b"\0" * 256))
    ioctl(device_fd, IOCTL_GET_DIRTY_MAP_PATH, buf)
    # 解包路径字符串
    path = struct.unpack_from(f"{len(buf)}s", buf)[0]
    return path.decode("utf-8").rstrip("\0")


# 获取runc容器进程树的PID
def get_runc_container_pidtree(container_name):
    if psutil is None:
        raise RuntimeError("psutil is required to inspect container process tree")
    container_pids.clear()  # 先清空pid列表
    container_pid_path = f"/run/runc/{container_name}/state.json"
    if not os.path.exists(container_pid_path):
        raise FileNotFoundError(f"runc容器 {container_name} 的状态文件不存在：{container_pid_path}")

    with open(container_pid_path, "r") as f:
        state = json.load(f)
        init_pid = state["init_process_pid"]

    try:
        init_process = psutil.Process(init_pid)
        container_pids.append(init_pid)
    except psutil.NoSuchProcess:
        print(f"Process with PID {init_pid} does not exist.")
        return
    # 获取子进程列表
    children = init_process.children(recursive=True)
    if not children:
        print(f"No child processes found for PID {init_pid}.")
    else:
        print(f"Child processes of PID {init_pid}:")
        for child in children:
            try:
                print(f"PID: {child.pid}, Name: {child.name()}, Status: {child.status()}")
                container_pids.append(child.pid)
            except psutil.NoSuchProcess:
                continue


def error():
    print("Something did not work. Exiting!", file=sys.stderr)
    # 确保在程序终止时停止 sync_rootfs 进程
    stop_sync_rootfs()

    if diskless:
        post_process(max_iter)
    sys.exit(-1)


def _run_command_checked(cmd, desc):
    """Run a subprocess command and dump stdout/stderr on failure for easier debugging."""
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        printable = " ".join(shlex.quote(part) for part in cmd)
        message = f"{desc} failed (exit {proc.returncode}): {printable}"
        print(message, file=sys.stderr)
        if proc.stdout:
            print("[stdout]", file=sys.stderr)
            print(proc.stdout.strip(), file=sys.stderr)
        if proc.stderr:
            print("[stderr]", file=sys.stderr)
            print(proc.stderr.strip(), file=sys.stderr)
        error()


# 迁移开始前，指定dirty-map的目录路径
def set_dirty_map_path(device_fd, path):
    """设置脏页跟踪的目录路径"""
    if not os.path.exists(path):
        os.mkdir(path)
    # 挂载到tmpfs
    mount_cmd = f"mount -t tmpfs none {path}"
    ret = os.system(mount_cmd)
    if ret != 0:
        raise SystemError(f"无法将{path}装载到tmpfs")
    print(f"设置脏页跟踪的目录路径为: {os.path.abspath(path)}")
    ioctl_set_dirty_map_path(device_fd, path)


# 启动所有容器进程的脏页跟踪
def start_dirty_track(device_fd):
    for pid in container_pids:
        ioctl_start_pid(device_fd, pid)
        print(f"启动对PID {pid}的脏页跟踪")


# 在pre-dump之间执行dirty-track并获取dirty-map
def execute_dirty_track(device_fd, first):
    """启动并停止脏页跟踪, 获取dirty-map"""
    # 启动所有容器进程的脏页跟踪
    # 启动暂时放入criu中
    if first:
        for pid in container_pids:
            ioctl_start_pid(device_fd, pid)
            print(f"启动对PID {pid}的脏页跟踪")

    # 等待一段时间以收集脏页数据
    time.sleep(0.2)  # 根据实际情况调整等待时间

    # 停止所有容器进程的脏页跟踪
    for pid in container_pids:
        ioctl_stop_pid(device_fd, pid)
        print(f"停止对PID {pid}的脏页跟踪")

    # 获取 dirty-map 路径
    # dirty_map_path = ioctl_get_dirty_map_path(device_fd)
    # print(f"脏页跟踪目录路径: {dirty_map_path}")

    # return dirty_map_path


def read_unsigned_long(file_path):
    """
    读取包含ulong64数字的二进制文件，返回一个列表
    """
    try:
        with open(file_path, "rb") as f:
            data = f.read()
            count = len(data) // 8  # sizeof(unsigned long)
            return list(struct.unpack("<" + "Q" * count, data))
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []


def read_dirtymap(file_path):
    """
    读取dirtymap文件，返回文件头（时间）和记录的脏页地址。
    """
    try:
        with open(file_path, "rb") as f:
            data = f.read()

            # 文件头大小（unsigned long）
            header_size = 8  # sizeof(unsigned long)

            # 验证文件长度是否足够
            if len(data) < header_size:
                print(f"Invalid dirtymap file: file size ({len(data)} bytes) is too small.")
                return None, []

            # 读取文件头（时间）
            time_header = struct.unpack("<Q", data[:header_size])[0]

            # 每个条目的大小
            entry_size = 12  # sizeof(unsigned long) + sizeof(unsigned int)

            # 剩余数据的长度
            data = data[header_size:]

            if len(data) % entry_size != 0:
                print(f"Invalid dirtymap file size after header: {len(data)} bytes")
                return time_header, []

            # 解析脏页地址和写次数
            count = len(data) // entry_size
            addresses = []
            for i in range(count):
                entry = data[i * entry_size: (i + 1) * entry_size]
                address, write_count = struct.unpack("<QI", entry)
                addresses.append(address)

            return time_header, addresses
    except Exception as e:
        print(f"Error reading dirtymap file {file_path}: {e}")
        return None, []


def merge_addresses(dirty_addresses, candidate_addresses):
    """
    去重合并脏页地址和候选页地址
    """
    merged = []
    i = j = 0
    len_dirty = len(dirty_addresses)
    len_candidate = len(candidate_addresses)

    while i < len_dirty and j < len_candidate:
        if dirty_addresses[i] < candidate_addresses[j]:
            merged.append(dirty_addresses[i])
            i += 1
        elif dirty_addresses[i] > candidate_addresses[j]:
            merged.append(candidate_addresses[j])
            j += 1
        else:
            merged.append(dirty_addresses[i])
            i += 1
            j += 1

    while i < len_dirty:
        merged.append(dirty_addresses[i])
        i += 1

    while j < len_candidate:
        merged.append(candidate_addresses[j])
        j += 1

    return merged


def pid_may_dump_size(addresses):
    """
    计算单个进程的内存大小（单位：字节）。
    """
    return len(addresses) * PAGE_SIZE


def container_may_dump_size(container_pids, dirtymap_path):
    """
    遍历container_pids, 计算每个pid的脏页列表, 合并去重, 计算总传输大小
    """
    total_transfer_size = 0

    for pid in container_pids:
        # 步骤1: 读取timestamp_list.pid
        timestamp_list_file = os.path.join(dirtymap_path, f"timestamp_list.{pid}")
        timestamps = read_unsigned_long(timestamp_list_file)
        if not timestamps:
            print(f"No timestamps found for pid {pid}. Skipping.")

        latest_timestamp = timestamps[-1] if timestamps else 0

        # 步骤2: 加载最新的dirtymap
        if latest_timestamp != 0:
            dirtymap_file = os.path.join(dirtymap_path, f"{pid}-{latest_timestamp}.dirtymap")
            track_time, dirty_addresses = read_dirtymap(dirtymap_file)
            print(f"[PID {pid}] Loaded {len(dirty_addresses)} dirty addresses")
        else:
            dirty_addresses = []
            print(f"[PID {pid}] No latest dirtymap found.")

        # 步骤3: 读取candidate_list.pid
        candidate_list_file = os.path.join(dirtymap_path, f"candidate_list.{pid}")
        candidate_addresses = read_unsigned_long(candidate_list_file)
        print(f"[PID {pid}] Loaded {len(candidate_addresses)} candidate addresses")

        # 步骤4: 合并并去重
        merged_addresses = merge_addresses(dirty_addresses, candidate_addresses)
        print(f"[PID {pid}] Merged {len(merged_addresses)} addresses that may be dumped")

        # 将地址添加到总集合中
        total_transfer_size += pid_may_dump_size(merged_addresses)

    return total_transfer_size


# 准备好迁移所需的镜像目录，同时要清除之前的迁移残留的镜像
# 需要先尝试删除image和parent的整个目录树
def prepare(base_path, image_path, parent_path, work_path):
    if os.path.exists(base_path):
        try:
            umount_cmd = "umount " + image_path
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
            shutil.rmtree(image_path)
            shutil.rmtree(base_path + "/d_log")
        except Exception:
            pass

        try:
            dir_list = os.listdir(base_path)
            for entry in dir_list:
                entry_path = os.path.join(base_path, entry)
                # print(entry)
                # print(entry_path)
                if os.path.isdir(entry_path) and entry.startswith("parent"):
                    umount_cmd = "umount " + entry_path
                    subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
                    shutil.rmtree(entry_path)
                elif os.path.isdir(entry_path) and entry.startswith("pd_log"):
                    shutil.rmtree(entry_path)
        except Exception:
            pass
    else:
        os.mkdir(base_path)
    if parent_path:
        for i in parent_path:
            os.mkdir(i)
    if work_path:
        for i in work_path:
            os.mkdir(i)
    os.mkdir(image_path)
    os.mkdir(base_path + "/d_log")


# 功能函数：获取目录下特定模式文件的总大小
# pattern: 文件名模式, e.g. "pages*.img"
def getdirsize(path, pattern=None):
    tsize = 0
    if not os.path.exists(path):
        return tsize

    # skip soft link file
    if os.path.islink(path):
        return tsize

    # avoid stat certain filename pattern
    if os.path.isfile(path):
        tsize = os.path.getsize(path)  # 5041481
        if pattern:
            if pattern not in path:
                return 0

        return tsize

    if os.path.isdir(path):
        with os.scandir(path) as dir_list:
            for sub_entry in dir_list:
                sub_entry_path = os.path.join(path, sub_entry.name)
                if sub_entry.is_symlink():
                    # print("current symbol link is {}".format(sub_entry.name))
                    continue
                if sub_entry.is_dir():
                    # print("current dir is {}".format(sub_entry.name))
                    subdir_size = getdirsize(sub_entry_path, pattern)  # 5800007
                    tsize += subdir_size
                elif sub_entry.is_file():
                    # file_size = os.path.getsize(sub_entry_path) # 1891
                    file_size = getdirsize(sub_entry_path, pattern)
                    # print("current file is {}".format(sub_entry.name))
                    # print("current filepath is {}".format(sub_entry_path))
                    # print("{}'s size is {}".format(sub_entry.name, file_size))
                    tsize += file_size
        # if pattern:
        #     print('the total size of {} with pattern {} is {}'.format(path, pattern, tsize))
        # else:
        #     print('the total size of {} is {}'.format(path, tsize))
        return tsize


# 带宽测量（使用异步或多线程）
def measure_bandwidth(dest_ip):
    print(f"开始测量到{dest_ip}的带宽")
    try:
        # 使用 iperf3 进行短时间带宽测量
        result = subprocess.run(
            ["iperf3", "-c", dest_ip, "-t", "3", "-", "m", "-J"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.returncode != 0:
            print("带宽测量失败:", result.stderr)
            return 0
        iperf_output = json.loads(result.stdout)
        bandwidth = iperf_output["end"]["sum_sent"]["bits_per_second"] / 8  # 转换为Bytes/s
        print(f"测得带宽: {bandwidth:.2f} Bytes/s")
        return bandwidth
    except Exception as e:
        print("带宽测量异常:", e)
        return 0


# 降低source的优先级以触发VIP迁移到dest
def transfer_vip(new_prior):
    """Set the local Keepalived priority to `new_prior` using vipctl.

    Returns 0 on success, non-zero otherwise.
    """
    try:
        try:
            import mig_scripts.vipctl as vipctl
        except Exception:
            import importlib.util as _il

            spec = _il.spec_from_file_location("vipctl_mod", os.path.join(os.path.dirname(__file__), "vipctl.py"))
            vipctl = _il.module_from_spec(spec)
            spec.loader.exec_module(vipctl)

        rc = vipctl.set_keepalived_priority(new_prior)
        return 0 if rc == 0 else 1
    except Exception as e:
        print(f"transfer_vip failed: {e}")
        return 1


# 通知dest提升优先级
def notify_transfer_vip(cs, inputs):
    vip_cmd = json.dumps({"transfer_vip": True})
    cs.send(bytes(vip_cmd, encoding="utf-8"))
    # print("send notify_transfer_vip")
    inputready, outputready, exceptready = select.select(inputs, [], [], 5)

    if inputready:
        for s in inputready:
            answer_bytes = s.recv(1024)
            answer = answer_bytes.decode("utf-8").strip()
            # print(answer)
            pattern = r"OK"
            match = re.search(pattern, answer)
            if not match:
                print(answer)
                return 1
            else:
                return 0
    else:
        print("can't confirm the VIP has been transfered")
        return 1


# 异步VIP迁移
def async_vip_migration(cs, inputs):
    """
    1. 降低优先级到30 (低于目标节点)
    2. 通知目标节点接管VIP
    3. 验证迁移成功，异常时自动恢复
    """
    migration_start = time.time()

    try:
        ret = transfer_vip("30")

        if ret == 0:
            ret = notify_transfer_vip(cs, inputs)
            if ret == 0:
                # 记录迁移时间
                migration_time = time.time() - migration_start
                print(f"VIP migration: {migration_time:.2f}s")
                return
            else:
                print("VIP notification confirmation failed, restoring priority")
                transfer_vip("70")  # 恢复到原优先级
                return
        else:
            print("VIP priority setting failed, migration aborted")
            return

    except Exception as e:
        print(f"VIP migration failed: {e}, restoring priority")
        transfer_vip("70")  # 恢复到原优先级


# 计算image目录下除pages-x.img外的文件总大小
def calculate_image(directory, exclude_pages=False):
    total_size = 0
    for root, dirs, files in os.walk(directory):
        for file in files:
            if exclude_pages and file.startswith("pages-") and file.endswith(".img"):
                continue
            # if not file.startswith("pages-") or not file.endswith(".img"):
            else:
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
    return total_size


# create the pre-dump, which is done in case of pre-copy and hybrid migrations.
# pre-dump contains the entire content of the container virtual memory
# pre-dump is stored in the parent directory
def pre_dump(mig_base, container, i, dirtymap):

    old_cwd = os.getcwd()
    os.chdir(mig_base)
    cmd = "runc checkpoint --pre-dump --work-path pd_log_{} --image-path parent_{}".format(i, i)
    cmd += " " + container
    if dirtymap:
        cmd += " --use-dirty-map --dirty-map-dir " + dirtymap_path
    # 只有 i>1 时才加上上一次的 parent_(i-1)
    if i > 1:
        cmd += f" --parent-path ../parent_{i-1}"
    # cmd += ' --parent-path ../parent_{}'.format(i)
    # print(cmd)
    # start = time.perf_counter() * 1000
    ret = os.system(cmd)
    # end = time.perf_counter() * 1000
    # print ("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    # pre_dump_time_total += (end - start)# 累计预拷贝时间
    os.chdir(old_cwd)
    if ret != 0:
        error()


def real_dump_0(mig_base, runc_args=None):

    old_cwd = os.getcwd()
    os.chdir(mig_base)

    cmd = "runc checkpoint --image-path parent_0 --work-path pd_log_0"

    if runc_args:
        cmd += " " + " ".join(runc_args)

    cmd += " --leave-running"
    cmd += " " + container

    p = subprocess.Popen(cmd, shell=True)
    ret = p.wait()
    # print("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    os.chdir(old_cwd)
    if ret != 0:
        error()
    directory_path = f"{mig_base}/parent_0"
    esti_dump_size_post = calculate_image(directory_path, False)
    # esti_dump_size_pre = calculate_image(directory_path, True)
    print(f"The total size of all files excluding 'pages-x.img' in {directory_path} is {esti_dump_size_post} bytes.")
    # print(f"The total size of all files including 'pages-x.img' in {directory_path} is {esti_dump_size_pre} bytes.")
    stats_dump_file = os.path.join(mig_base, "pd_log_0/stats-dump")
    parse_stats_dump(stats_dump_file, "dump", False)


# create the dump. This is done for any migration technique.
# The dump is stored in the image directory.
# If a pre-dump is present, it will be in the parent directory.
# When post-copy is not used, wait until the dump command ends (p.wait()).
# When post-copy is enabled, the dump procedure does not write memory pages into
# the image; instead it starts a page server to transfer faulted pages later.
# The page server will read the local memory dump and serve pages to the lazy-
# pages daemon running on the destination. The page server listens on a port.
# When using --status-fd, CRIU writes '\0' to the given pipe after finishing the
# checkpoint and starting the page server. See CRIU docs for --lazy-pages and
# --status-fd for details: https://criu.org/CLI/opt/--lazy-pages
def real_dump(mig_base, precopy, postcopy, last_iter, dirtymap, replay, cs, inputs, runc_args=None):
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    global dump_time

    # cmd = 'runc checkpoint --image-path image --leave-running'
    cmd = "runc checkpoint --image-path image --work-path d_log"

    if runc_args:
        cmd += " " + " ".join(runc_args)
    if precopy:
        cmd += " --parent-path ../parent_{}".format(last_iter)
    # if diskless:
    #     #send the page server command,
    #     #after the server's response, CRIU can directly transfer memory dump with network
    #     pageserver_cmd = '{ "pageserver" : { "path" : "image" } }'
    #     cs.send(bytes(pageserver_cmd, encoding='utf-8'))
    #     inputready, outputready, exceptready = select.select(input, [], [], 4)
    #     #If after 4 seconds there is something to read(e.g., error msg from the socket), then print it and exit
    #     if inputready:
    #         for s in inputready:
    #             answer = s.recv(1024)
    #             print(answer)
    #             error()
    #     cmd += ' --page-server {}:27'.format(dest)
    if postcopy:
        if fcntl is None:
            raise RuntimeError("post-copy mode requires fcntl support on this platform")
        cmd += " --lazy-pages"
        cmd += " --page-server localhost:27"
        read_fd, write_fd = os.pipe()
        fdflags = fcntl.fcntl(write_fd, fcntl.F_GETFD)  # type: ignore[attr-defined]
        fcntl.fcntl(write_fd, fcntl.F_SETFD, fdflags & ~fcntl.FD_CLOEXEC)  # type: ignore[attr-defined]
        cmd += " --status-fd " + str(write_fd)
    if dirtymap:
        cmd += " --use-dirty-map --dirty-map-dir " + dirtymap_path
    if replay:
        cmd += " --leave-running"

    cmd += " " + container

    # postcopy时stat-dump无法准确度量检查点时间，因此需要单独计算
    if postcopy:
        start = time.perf_counter() * 1000
        p = subprocess.Popen(cmd, pass_fds=(write_fd,), shell=True)
        ret = os.read(read_fd, 1)
        if ret == b"\0":
            print("Ready for lazy page transfer")
            os.close(read_fd)
            os.close(write_fd)
        ret = 0
        end = time.perf_counter() * 1000
        dump_time = end - start
    else:
        p = subprocess.Popen(cmd, shell=True)
        ret = p.wait()
    # print("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    os.chdir(old_cwd)
    if ret != 0:
        error()

    # '--tcp-established'迁移TCP连接
    if runc_args and "--tcp-established" in " ".join(runc_args):
        # VIP线程独立处理，不要共享inputs列表
        vip_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        vip_socket.connect((dest, 18863))
        vip_thread = threading.Thread(target=async_vip_migration, args=(vip_socket, [vip_socket]))
        vip_thread.start()


# 解析大小字符串，转换为以Byte为单位
def parse_size(size_str):
    size_multiplier = {"K": 1024, "M": 1024 * 1024, "G": 1024 * 1024 * 1024}
    unit = size_str[-1].upper()
    if unit in size_multiplier:
        size = float(size_str[:-1]) * size_multiplier[unit]
    else:
        size = float(size_str)  # 默认单位为字节，保持为原始值
    return size


def xfer_pre_dump(parent_path, dest, iteration, session, control_sock):
    global pre_dump_xfer_time_total, total_compression_time

    if not session:
        raise RuntimeError(f"missing transfer session for pre-dump iteration {iteration}")

    token = session.get("token")
    port = session.get("port")
    if not token or port is None:
        raise RuntimeError(f"invalid session descriptor for iteration {iteration}: {session}")

    port = int(port)

    # 创建压缩包
    if compress == 0:
        archive_name = os.path.join(mig_base, f"pre_dump_{iteration}.tar")
        cmd_tar = ["tar", "-cf", archive_name, "-C", parent_path, "."]
    elif compress >= 1 and compress <= 4:
        tar_name = os.path.join(mig_base, f"pre_dump_{iteration}.tar")
        archive_name = os.path.join(mig_base, f"pre_dump_{iteration}.tar.lzo")
        cmd_tar = ["tar", "-cf", tar_name, "-C", parent_path, "."]
        lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")
        cmd_compress = [lzo_gpu_path, f"-{compress}", tar_name, archive_name]
    else:
        raise ValueError(f"不支持的压缩等级: {compress}")

    start = time.perf_counter() * 1000

    if compress == 0:
        _run_command_checked(cmd_tar, f"Create tar pre_dump_{iteration}")
    else:
        _run_command_checked(cmd_tar, f"Create tar pre_dump_{iteration}")

        if not os.path.exists(tar_name):
            raise FileNotFoundError(f"TAR file {tar_name} not found")

        _run_command_checked(cmd_compress, f"lzo_gpu compress pre_dump_{iteration}")

        try:
            os.remove(tar_name)
        except OSError as e:
            print(f"警告：无法删除临时tar文件 {tar_name}: {e}")

    end = time.perf_counter() * 1000

    total_compression_time += end - start
    if not os.path.exists(archive_name):
        raise FileNotFoundError(f"Archive file {archive_name} not found")

    size = os.path.getsize(archive_name)
    if size == 0:
        raise ValueError(f"pre_dump_{iteration} archive size is 0")

    print(f"Pre-dump {iteration} archive: {size} Bytes, {(end - start):.3f} ms")

    try:
        sent_bytes, transfer_time = _stream_file_to_socket(archive_name, dest, port)
    except Exception as exc:
        raise RuntimeError(f"pre-dump {iteration} transfer failed: {exc}") from exc

    effective_mbps = 0.0
    if transfer_time > 0:
        effective_mbps = (sent_bytes * 8.0) / (transfer_time / 1000.0) / 1_000_000
    print(f"Pre-dump {iteration} xfer: {transfer_time:.3f} ms ({effective_mbps:.2f} Mbps) -> {dest}:{port}")

    try:
        ack = _await_transfer_completion(control_sock, token)
    except Exception as exc:
        raise RuntimeError(f"pre-dump {iteration} completion check failed: {exc}") from exc

    if ack.get("status") != "OK":
        print(f"Pre-dump {iteration} transfer reported failure: {ack}")
        error()

    ack_bytes = ack.get("bytes")
    ack_duration = ack.get("duration_ms")
    if ack_bytes is not None and ack_duration is not None:
        print("  Destination reported %.0f bytes, %.3f ms" % (float(ack_bytes), float(ack_duration)))

    if time_constraint > 0 and transfer_time > 0:
        bandwidth_measurements.append(1000.0 * sent_bytes / transfer_time)

    pre_dump_xfer_time_total += transfer_time


def xfer_final(image_path, dest, compress, session, control_sock):
    global dump_xfer_time, total_compression_time, final_archive_size_bytes

    if not session:
        raise RuntimeError("missing final transfer session descriptor")

    token = session.get("token")
    port = session.get("port")
    if not token or port is None:
        raise RuntimeError(f"invalid final session descriptor: {session}")

    port = int(port)

    final_archive_size_bytes = 0.0
    if compress == 0:
        tar_name = os.path.join(mig_base, "final_dump.tar")
        start_comp = time.perf_counter() * 1000
        cmd_create_tar = ["tar", "-cf", tar_name, "-C", image_path, "."]
        _run_command_checked(cmd_create_tar, "tar final dump")
        end_comp = time.perf_counter() * 1000
        total_compression_time += end_comp - start_comp
        if not os.path.exists(tar_name):
            raise FileNotFoundError(f"Final dump archive not found: {tar_name}")
        final_archive_size_bytes = os.path.getsize(tar_name)
        archive_path = tar_name
    elif compress >= 1 and compress <= 4:
        start = time.perf_counter() * 1000
        tar_name = os.path.join(mig_base, "final_dump.tar")
        lzo_name = os.path.join(mig_base, "final_dump.tar.lzo")
        lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")

        cmd_create_tar = ["tar", "-cf", tar_name, "-C", image_path, "."]
        _run_command_checked(cmd_create_tar, "tar final dump")

        cmd_compress = [lzo_gpu_path, f"-{compress}", tar_name, lzo_name]
        _run_command_checked(cmd_compress, "lzo_gpu compress final dump")
        end = time.perf_counter() * 1000
        total_compression_time += end - start

        try:
            os.remove(tar_name)
        except OSError as e:
            print(f"警告：无法删除临时tar文件 {tar_name}: {e}")

        if not os.path.exists(lzo_name):
            raise FileNotFoundError(f"Final dump archive not found: {lzo_name}")
        final_archive_size_bytes = os.path.getsize(lzo_name)
        archive_path = lzo_name
    else:
        raise ValueError(f"不支持的压缩等级: {compress}")

    try:
        sent_bytes, dump_xfer_time = _stream_file_to_socket(archive_path, dest, port)
    except Exception as exc:
        raise RuntimeError(f"final dump transfer failed: {exc}") from exc

    final_archive_size_bytes = float(sent_bytes)
    effective_mbps = 0.0
    if dump_xfer_time > 0:
        effective_mbps = (sent_bytes * 8.0) / (dump_xfer_time / 1000.0) / 1_000_000
    print(f"Final dump xfer: {dump_xfer_time:.3f} ms ({effective_mbps:.2f} Mbps) -> {dest}:{port}")

    try:
        ack = _await_transfer_completion(control_sock, token)
    except Exception as exc:
        raise RuntimeError(f"final dump completion check failed: {exc}") from exc

    if ack.get("status") != "OK":
        print(f"Final dump transfer reported failure: {ack}")
        error()

    ack_bytes = ack.get("bytes")
    ack_duration = ack.get("duration_ms")
    if ack_bytes is not None and ack_duration is not None:
        print("  Destination reported %.0f bytes, %.3f ms" % (float(ack_bytes), float(ack_duration)))

    # If destination provided a resource usage file path, expose it for orchestrators
    dest_res = ack.get("resource_path") or ack.get("resource_usage") or ack.get("dest_resource_usage")
    if dest_res:
        try:
            print(f"METRIC_PARAM\tdest_resource_usage\t{dest_res}")
        except Exception:
            pass

    if compress == 0:
        try:
            os.remove(tar_name)
        except OSError as e:
            print(f"警告：无法删除临时final tar文件 {tar_name}: {e}")
    elif 1 <= compress <= 4:
        try:
            os.remove(lzo_name)
        except OSError:
            pass


# Run the pre-dump iteration and transfer it to the destination

def parse_pred_stats_from_log(dump_log):
    """Parse ObsidianPred stats from a dump.log file.

    Returns: (pred_total, pred_hit, pred_miss, pred_acc)
    """
    pred_total = 0
    pred_hit = 0
    pred_miss = 0
    pred_acc = 0.0
    if not dump_log or not os.path.exists(dump_log):
        return pred_total, pred_hit, pred_miss, pred_acc

    try:
        with open(dump_log, "r", errors="ignore") as f:
            for line in f:
                if "[ObsidianPred]" not in line:
                    continue
                for token in line.strip().split():
                    if token.startswith("predicted_total="):
                        pred_total = int(token.split("=", 1)[1])
                    elif token.startswith("predicted_hit="):
                        pred_hit = int(token.split("=", 1)[1])
                    elif token.startswith("predicted_miss="):
                        pred_miss = int(token.split("=", 1)[1])
                    elif token.startswith("predicted_accuracy="):
                        pred_acc = float(token.split("=", 1)[1])
    except Exception:
        return 0, 0, 0, 0.0

    if pred_total > 0 and pred_acc == 0.0:
        pred_acc = (pred_hit * 100.0) / float(pred_total)
    return pred_total, pred_hit, pred_miss, pred_acc


def parse_def_total_from_log(dump_log):
    """Parse ObsidianDef deferred_total from a dump.log file (last value)."""
    def_total = 0
    if not dump_log or not os.path.exists(dump_log):
        return def_total

    try:
        with open(dump_log, "r", errors="ignore") as f:
            for line in f:
                if "[ObsidianDef]" not in line:
                    continue
                for token in line.strip().split():
                    if token.startswith("deferred_total="):
                        def_total = int(token.split("=", 1)[1])
    except Exception:
        return 0

    return def_total


def get_dm_stop_params():
    """Return DM adaptive stop parameters (aligned with checkpoint_run_impl.sh).

    DM-based stopping is enabled when dirtymap is active. The default stop policy is
    `balanced`. Environment variables are only used to micro-tune thresholds:
    `PREDUMP_STOP_POLICY` and `PREDUMP_DM_*` (e.g., `PREDUMP_DM_MIN_PRED_TOTAL`).
    """
    policy = os.getenv("PREDUMP_STOP_POLICY", "balanced").lower()
    if policy == "aggressive":
        min_pred_total = 128
        acc_converge = 60
        acc_diverge = 50
        def_growth_converge = 0.15
        def_growth_diverge = 0.25
    elif policy == "conservative":
        min_pred_total = 512
        acc_converge = 70
        acc_diverge = 55
        def_growth_converge = 0.08
        def_growth_diverge = 0.25
    else:
        min_pred_total = 256
        acc_converge = 65
        acc_diverge = 55
        def_growth_converge = 0.12
        def_growth_diverge = 0.25

    min_pred_total = int(os.getenv("PREDUMP_DM_MIN_PRED_TOTAL", min_pred_total))
    acc_converge = float(os.getenv("PREDUMP_DM_ACC_CONVERGE", acc_converge))
    acc_diverge = float(os.getenv("PREDUMP_DM_ACC_DIVERGE", acc_diverge))
    def_growth_converge = float(os.getenv("PREDUMP_DM_DEF_GROWTH_CONVERGE", def_growth_converge))
    def_growth_diverge = float(os.getenv("PREDUMP_DM_DEF_GROWTH_DIVERGE", def_growth_diverge))
    stop_consec = int(os.getenv("PREDUMP_DM_STOP_CONSEC", 2))

    return {
        "min_pred_total": min_pred_total,
        "acc_converge": acc_converge,
        "acc_diverge": acc_diverge,
        "def_growth_converge": def_growth_converge,
        "def_growth_diverge": def_growth_diverge,
        "stop_consec": stop_consec,
    }

def iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap, resolve_session):
    iter_terminate = False
    last_iter = 1
    # DM-based stopping is automatically active when dirtymap is enabled; environment variables only tune thresholds.
    dm_params = get_dm_stop_params() if dirtymap else None
    prev_def_total = 0
    dm_converge_count = 0
    dm_diverge_count = 0
    if dirtymap:
        # 在pre-copy开启前先启动对容器的dirty-track
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)
    while last_iter <= max_iter:
        last_path = parent_path[last_iter - 1]
        pre_dump(mig_base, container, last_iter, dirtymap)

        dir_size = float(getdirsize(last_path, "pages") or 0)
        less_last_path = parent_path[last_iter - 2] if last_iter > 1 else None

        # 更新最大predump大小
        global max_predump_size
        if dir_size > max_predump_size:
            max_predump_size = dir_size

        if last_iter == 1:
            if dir_size < 1024 * 64 or last_iter == max_iter:
                iter_terminate = True
        else:
            less_last_size = float(getdirsize(less_last_path, "pages") or 0) if less_last_path else 0.0
            if (
                abs(dir_size - less_last_size) < 1024 * 64
                or dir_size < 1024 * 64
                or last_iter == max_iter
            ):
                iter_terminate = True

        if dirtymap:
            dump_log = os.path.join(mig_base, f"pd_log_{last_iter}", "dump.log")
            pred_total, pred_hit, pred_miss, pred_acc = parse_pred_stats_from_log(dump_log)
            def_total = parse_def_total_from_log(dump_log)
            def_growth = (def_total - prev_def_total) / float(prev_def_total) if prev_def_total > 0 else 0.0
            dm_pred_valid = (pred_total >= dm_params["min_pred_total"]) and (prev_def_total > 0)

            print(
                f"Iteration {last_iter}: pred_total={pred_total} pred_acc={pred_acc:.2f}% "
                f"def_total={def_total} def_growth={def_growth:.4f} dm_valid={dm_pred_valid}"
            )

            if dm_pred_valid:
                if pred_acc >= dm_params["acc_converge"] and def_growth <= dm_params["def_growth_converge"]:
                    dm_converge_count += 1
                else:
                    dm_converge_count = 0

                if pred_acc <= dm_params["acc_diverge"] and def_growth >= dm_params["def_growth_diverge"]:
                    dm_diverge_count += 1
                else:
                    dm_diverge_count = 0

                if dm_converge_count >= dm_params["stop_consec"]:
                    print(f"Converged by dirtymap at iter {last_iter} (dm_converge_count={dm_converge_count})")
                    iter_terminate = True
                if dm_diverge_count >= dm_params["stop_consec"]:
                    print(f"Diverged by dirtymap at iter {last_iter} (dm_diverge_count={dm_diverge_count})")
                    iter_terminate = True
            else:
                dm_converge_count = 0
                dm_diverge_count = 0

            prev_def_total = def_total

        session = resolve_session(last_iter, last_path)
        if not session:
            raise RuntimeError(f"no transfer session available for pre-dump iteration {last_iter}")

        xfer_pre_dump(last_path, dest, last_iter, session, cs)
        if iter_terminate:
            break
        last_iter += 1
    print("last_iter:", last_iter)
    return last_iter


def parse_stats_dump(stats_dump_path, log_type, accumulate=True):
    """
    解析stats-dump文件并累加迁移时间

    :param stats_dump_path: stats-dump文件的路径
    :param log_type: 日志类型，'pre_dump' 或 'dump'
    :param accumulate: 布尔值，指定是否进行时间累加，默认为True
    """
    global pre_dump_time_total, dump_time, esti_dump_time

    try:
        # 执行 'crit decode' 命令并获取输出
        result = subprocess.run(
            ["crit", "show", stats_dump_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )

        # 解析 JSON 输出
        stat_data = json.loads(result.stdout)
        entries = stat_data.get("entries", [])

        for entry in entries:
            dump_info = entry.get("dump", {})
            # 提取特定时间字段并累加(us)
            time_keys = ["freezing_time", "frozen_time"]
            if log_type == "pre_dump":
                time_keys.extend(["memdump_time", "memwrite_time"])

            total_time = sum(float(dump_info.get(key, 0)) for key in time_keys if key in dump_info)

            if accumulate:  # 只有当accumulate为True时，才执行累加
                if log_type == "pre_dump":
                    pre_dump_time_total += total_time / 1000
                    print(f"stats-dump total_time for pre-dump: {total_time/1000}ms")
                elif log_type == "dump":
                    dump_time += total_time / 1000
                    print(f"stats-dump total_time for dump: {total_time/1000}ms")
            else:
                esti_dump_time = total_time / 1000
                print(f"stats-dump total_time for first-dump: {total_time/1000}ms")

    except subprocess.CalledProcessError as e:
        print(f"执行 crit decode 时出错: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"解析 JSON 时出错: {e}")
    except Exception as e:
        print(f"处理 stats-dump 文件时发生未知错误: {e}")


def determine_log_type(path):
    """
    根据路径名称确定日志类型

    :param path: 工作路径
    :return: 'pre_dump' 或 'dump'，若无法确定则返回 None
    """
    # 使用正则表达式匹配路径模式
    pre_dump_pattern = re.compile(r".*/pd_log_\d+$")
    dump_pattern = re.compile(r".*/d_log$")

    if pre_dump_pattern.match(path):
        return "pre_dump"
    elif dump_pattern.match(path):
        return "dump"
    else:
        return None


def get_dump_time(work_path_list):
    """
    遍历 work_path 列表，查找 stats-dump 文件并解析。
    根据路径名称自动确定日志类型。

    :param work_path_list: 包含工作路径的列表
    """
    for path in work_path_list:
        stats_dump_file = os.path.join(path, "stats-dump")
        if os.path.isfile(stats_dump_file):
            log_type = determine_log_type(path)
            if log_type:
                parse_stats_dump(stats_dump_file, log_type)
            else:
                print(f"无法确定日志类型的路径: {path}")
        else:
            print(f"未找到stats-dump文件: {stats_dump_file}")


def get_dump_size(image_path, pre_dump):
    """
    遍历image_path

    :param image_path: 镜像目录
    :param pre_dump: 是否为预拷贝
    """
    global dump_size, pre_dump_size_total
    cmd_du = ["du", "-bs", image_path]
    try:
        result = subprocess.run(cmd_du, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        # 提取大小部分

        xfer_size_str = result.stdout.strip().split("\t")[0]  # 例如 "124K"
        xfer_size = parse_size(xfer_size_str)
        if pre_dump:
            pre_dump_size_total += xfer_size  # 累计预拷贝大小
        else:
            dump_size = xfer_size  # 累计拷贝大小
    except subprocess.CalledProcessError as e:
        print(f"Error executing du command: {e.stderr}")
        if pre_dump:
            pre_dump_size_total += 0.0  # 累计预拷贝大小
        else:
            dump_size = 0.0  # 累计拷贝大小
            error()



def update_image_parent(mig_base: str, latest_parent: str):
    """
    更新 image 目录下的 parent 符号链接指向最新的 checkpoint。

    :param mig_base: 迁移基路径，例如 '/runc/containers/<container>/migrate'
    :param latest_parent: 最新的 parent 目录名称，例如 'parent_1'
    """
    image_parent_link = os.path.join(mig_base, "image", "parent")
    target = os.path.join(mig_base, latest_parent)

    # 计算相对路径
    relative_target = os.path.relpath(target, os.path.dirname(image_parent_link))

    if os.path.islink(image_parent_link) or os.path.exists(image_parent_link):
        try:
            os.remove(image_parent_link)
            # print(f"已移除旧的 image parent 符号链接: {image_parent_link}")
        except OSError as e:
            print(f"无法移除旧的 image parent 符号链接 {image_parent_link}: {e}")
            error()

    try:
        os.symlink(relative_target, image_parent_link)
        # print(f"已更新 image parent 符号链接: {image_parent_link} -> {relative_target}")
    except OSError as e:
        print(f"无法创建新的 image parent 符号链接 {image_parent_link} -> {relative_target}: {e}")
        error()


def migrate(container, dest, pre, post, replay, rootfs, max_iter, dirtymap, time_constraint, runc_args):
    global rst_time, dirtymap_path, device_fd, sync_rootfs_process, sync_rootfs_log_file

    # 注册退出处理器和信号处理器
    atexit.register(stop_sync_rootfs)

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    base_path = runc_base + container
    rootfs_path = base_path + "/rootfs"
    mig_base = base_path + "/migrate"
    image_path = mig_base + "/image"
    # parent_path = base_path + "/parent"
    parent_path = []
    work_path = []
    global dirtymap_path
    dirtymap_path = mig_base + "/dirty_map"

    if pre:
        for i in range(1, max_iter + 1):
            parent_dir = f"{mig_base}/parent_{i}"
            parent_path.append(parent_dir)
            work_path.append(f"{mig_base}/pd_log_{i}")

    print("parent_path:", parent_path)
    prepare(mig_base, image_path, parent_path, work_path)

    pre_sessions_by_path = {}
    pre_sessions_by_iter = {}
    pre_sessions_fallback = {}
    final_session = None

    real_dump_0(mig_base, runc_args=runc_args)

    # time.sleep(100000)
    # 测量初始带宽和状态传输最大值(Bytes)
    if time_constraint > 0:
        global max_xfer_size
        # Measure initial bandwidth
        # mea_bandwidth = measure_bandwidth(dest)
        # max_xfer_size = mea_bandwidth * (time_constraint / 1000.0)  # Convert ms to seconds
        # bandwidth_measurements.append(mea_bandwidth)
        # print(f"current bandwidth is {mea_bandwidth}")

    # 打开dirty-track设备
    if dirtymap:
        try:
            global device_fd
            global device_file
            device_file = open(DEVICE_PATH, "wb")
            device_fd = device_file.fileno()
        except FileNotFoundError:
            print(f"Light-DT not found in {DEVICE_PATH}, please load the dirty-track kernel module first.")
            sys.exit(1)

        # 迁移开始前配置dirty-map目录
        set_dirty_map_path(device_fd, dirtymap_path)

    socket.setdefaulttimeout(6)
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Connect to the migration server running on the destination to send the commands
    cs.connect((dest, 18863))

    inputs = [cs, sys.stdin]

    prepare_payload = {
        "prepare": {
            "path": mig_base,
            "image_path": image_path,
            "compress": compress,
        }
    }
    if pre:
        prepare_payload["prepare"]["parent_path"] = parent_path

    try:
        prepare_reply = _send_control_command(cs, prepare_payload, timeout=60.0)
    except TimeoutError as exc:
        print(f"Timeout waiting for prepare acknowledgement: {exc}")
        error()
    except Exception as exc:
        print(f"Failed to execute prepare command: {exc}")
        error()

    status = str(prepare_reply.get("status", "OK")).upper() if isinstance(prepare_reply, dict) else "OK"
    if status != "OK":
        print(f"Destination prepare failed: {prepare_reply}")
        error()

    session_catalog = prepare_reply.get("sessions", {}) if isinstance(prepare_reply, dict) else {}
    if not isinstance(session_catalog, dict):
        session_catalog = {}
    pre_sessions_raw = session_catalog.get("pre_dump", []) if session_catalog else []
    final_session = session_catalog.get("final") if session_catalog else None

    for idx, entry in enumerate(pre_sessions_raw, start=1):
        path = entry.get("path")
        if path:
            pre_sessions_by_path[path] = entry
        iteration_val = entry.get("iteration")
        if isinstance(iteration_val, str) and iteration_val.isdigit():
            iteration_val = int(iteration_val)
        if isinstance(iteration_val, int):
            pre_sessions_by_iter[iteration_val] = entry
        pre_sessions_fallback[idx] = entry

    if final_session is None:
        print("Destination did not provide final transfer session metadata")
        error()

    def resolve_pre_session(iteration, path):
        return (
            pre_sessions_by_path.get(path)
            or pre_sessions_by_iter.get(iteration)
            or pre_sessions_fallback.get(iteration)
        )

    if pre_sessions_raw:
        session_summary = []
        for entry in pre_sessions_raw:
            iter_label = entry.get("iteration")
            port_label = entry.get("port")
            session_summary.append(f"{iter_label}:{port_label}")
        print("Prepared pre-dump sessions:", ", ".join(session_summary))
    else:
        print("No pre-dump sessions required")

    if final_session:
        print("Final transfer session port:", final_session.get("port"))

    if rootfs:
        search_cmd = "runc list | grep " + container
        container_exist = subprocess.getstatusoutput(search_cmd)
        # (0, 'redis-test   7289        running     /runc/containers/redis-test   2024-04-21T07:13:10.98300754Z   root')

        # if the container is already running on the source, then we can transfer the rootfs
        # if the container is not running, then the script will exit
        if container_exist[0]:
            error()

        # init_xfer_cmd = 'rsync -aqz --delete --timeout=100 {0}/ root@{1}:{0}/'.format(rootfs_path, dest)
        # start = time.perf_counter() * 1000
        # ret = os.system(init_xfer_cmd)
        # end = time.perf_counter() * 1000
        # print("initial ROOTFS transfer time %.3f ms" % (end - start))
        # if ret != 0:
        #     error()

        # infinite rootfs sync
        # 确保脚本有执行权限
        if not os.access("./sync_rootfs.sh", os.X_OK):  # 检查是否有执行权限
            os.chmod("./sync_rootfs.sh", 0o755)  # 添加执行权限

        # 保存日志文件句柄到全局变量
        sync_rootfs_log_file = open(mig_base + "/d_log/sync_rootfs.log", "w")
        sync_cmd = "./sync_rootfs.sh " + dest + " " + rootfs_path

        # 保存进程对象到全局变量
        sync_rootfs_process = subprocess.Popen(
            sync_cmd, shell=True, stdout=sync_rootfs_log_file, stderr=sync_rootfs_log_file
        )
        print(f"sync_rootfs started: (PID = {sync_rootfs_process.pid})")

    if pre:
        if diskless:
            for i in range(0, max_iter):
                mount_cmd = "mount -t tmpfs none " + parent_path[i]
                ret = os.system(mount_cmd)
                if ret != 0:
                    error()

        last_iter = iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap, resolve_pre_session)
        global pre_dump_iters
        pre_dump_iters = last_iter
        # 使用同步传输，所有传输已在iterate_predump中完成，无需发送确认消息
        # if diskless:
        #   diskless_pre_dump(base_path, container, dest)
        # else:
        #   pre_dump(base_path, container)
        #   xfer_pre_dump(parent_path, dest, base_path)
    else:
        last_iter = 0

    if diskless:
        mount_cmd = "mount -t tmpfs none " + image_path
        ret = os.system(mount_cmd)
        if ret != 0:
            error()

    if dirtymap and not pre:
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)

    if time_constraint > 0 and bandwidth_measurements:
        # 如果时间约束低于criu C/R时间之和，则表明无法热迁移
        if time_constraint < 2 * esti_dump_time:
            print(f"Time constraint {time_constraint} is too strict to perform live-migration")
            error()

        # 计算迁移可用带宽
        average_bandwidth = statistics.mean(bandwidth_measurements)
        if len(bandwidth_measurements) > 1:
            bandwidth_stddev = statistics.stdev(bandwidth_measurements)
        else:
            bandwidth_stddev = 0.0
        print(f"Average bandwidth: {average_bandwidth:.2f} Bytes/s")
        print(f"Bandwidth standard deviation: {bandwidth_stddev:.2f} Bytes/s")

        # 可用于传输的时间=时间约束-2*C/R时间
        max_xfer_size = abs(average_bandwidth - bandwidth_stddev) * (
            (time_constraint - 2 * esti_dump_time) / 1000.0
        )  # Convert ms to seconds
        print(f"Max_transfer_size: {max_xfer_size:.2f} Bytes based on average bandwidth and time constraint")

        # 获取容器尚未传输的内存状态大小，判断是否post-copy
        # 读取timestamp_list.pid文件，获取最新的dirty-map
        # 读取dirty-map中的被跳过温页和热页
        # 读取candidate_list.pid文件维护的候选页
        # 将两者累计并预计最终传输的内存状态大小(*4KB)
        if dirtymap:
            # 计算传输大小
            esti_dump_page = container_may_dump_size(container_pids, dirtymap_path)
            esti_dump_size_pre = esti_dump_page + esti_dump_size_post
            print(f"Container may dump {esti_dump_page} bytes of memory pages")
        else:
            # 没有启用dirty-map时，使用最大predump大小进行估算
            esti_dump_size_pre = max_predump_size + esti_dump_size_post
            print(f"Estimated dump size from max predump: {esti_dump_size_pre} bytes")

        # 步骤6: 与max_xfer_size比较
        if esti_dump_size_post >= 0.95 * max_xfer_size:
            print(f"Time constraint {time_constraint} is too strict to perform live-migration")
            error()
        else:
            if esti_dump_size_pre >= 0.95 * max_xfer_size:
                print(f"Exceed max_xfer_size {max_xfer_size}, post-copy is needed")
                if not post:
                    # print("[Warning]post-copy is not enabled, pre-copy may failed")
                    post = True
            else:
                print("We can transfer within one-shot stop-and-copy")
                if post:
                    post = False

    # Start resource monitor (auto) and run real_dump
    resmon = None
    if getattr(args, "monitor", False):
        try:
            # Prefer package import when available (module can be run as package),
            # but fall back to a file-based import so `python source.py` still works.
            try:
                import mig_scripts.monitor as monitor_mod
            except Exception:
                spec = importlib.util.spec_from_file_location(
                    "monitor_mod", os.path.join(os.path.dirname(__file__), "monitor.py")
                )
                monitor_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(monitor_mod)

            ts = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            default_outdir = f"/runc/containers/{container}/migrate/d_log"
            os.makedirs(default_outdir, exist_ok=True)
            out_path = args.monitor_out or os.path.join(default_outdir, f"resource_usage.source.{ts}.tsv")
            resmon = monitor_mod.ContainerResourceMonitor(
                container,
                interval=args.monitor_interval,
                out_path=out_path,
                include_host=True,
                enable_net=True,
                host_iface=args.monitor_host_iface,
            )
            resmon.start()
            try:
                monitor_mod.set_phase("prepare")
            except Exception:
                pass
        except Exception as e:
            print(f"Warning: failed to start resource monitor: {e}")
            resmon = None

    try:
        real_dump(mig_base, pre, post, last_iter, dirtymap, replay, cs, inputs, runc_args)
    finally:
        if resmon:
            try:
                monitor_mod.set_phase("done")
            except Exception:
                pass
            try:
                resmon.stop()
            except Exception:
                pass
            try:
                print(f"METRIC_PARAM\tsource_resource_usage\t{resmon.out_path}")
            except Exception:
                pass

    # 更新 image/parent 符号链接指向最新的 parent_i
    # update_image_parent(mig_base, f"parent_{last_iter+1}")

    # 创建标记文件确保rsync同步一次
    if rootfs and sync_rootfs_process and sync_rootfs_process.poll() is None:
        try:
            # 创建标记文件触发rsync同步
            force_sync_marker = os.path.join(base_path, "force_sync.marker")
            with open(force_sync_marker, "w"):
                pass  # 创建空文件
            print("标记文件已创建：触发实转储后同步")
        except Exception as e:
            print(f"创建转储后同步标记失败: {e}")

    # 传输容器剩余状态
    xfer_final(image_path, dest, compress, final_session, cs)

    # 等待强制同步完成 - 检查标记文件是否已被删除
    if rootfs and sync_rootfs_process and sync_rootfs_process.poll() is None:
        print("等待强制rootfs同步完成...")
        force_sync_marker = os.path.join(base_path, "force_sync.marker")

        for attempt in range(50):
            try:
                # 如果标记文件不存在，说明强制同步已完成
                if not os.path.exists(force_sync_marker):
                    print("强制rootfs同步已完成")
                    break
            except Exception as e:
                print(f"检查同步状态时出错: {e}")
                break
            time.sleep(0.2)

        # 如果循环结束标记文件还存在，可能有问题
        if os.path.exists(force_sync_marker):
            print("警告：rootfs强制同步可能未完成，继续迁移流程")
            # 清理标记文件以避免后续问题
            try:
                os.remove(force_sync_marker)
                print("清理未完成的同步标记文件")
            except Exception:
                pass
    else:
        print("rootfs同步进程未运行，跳过同步检查")
    # dir_size = convert_byte(getdirsize(image_path))
    # print('the total size of {} is {}{}'.format(image_path, dir_size[0], dir_size[1]))

    # if replay:
    # todo: 创建转发路由
    # 只有elastisearch才需要
    # final_sync_es_data(dest, rootfs_path)
    # one-shot restore with post-copy
    # Build runc_args string for restore command
    runc_args_str = " ".join(runc_args) if runc_args else ""
    # final_sync_es_data(dest, rootfs_path)

    restore_cmd = (
        '{ "restore" : { "path" : "' + base_path + '", "name" : "' + container + '" , "image_path" : "' + image_path
    )
    restore_cmd += '" , "lazy" : "' + str(post) + '" , "runc_args" : "' + runc_args_str.replace('"', '\\"') + '" } }'
    cs.send(bytes(restore_cmd, encoding="utf-8"))

    # 等待恢复完成
    print("Wait for destination...")
    max_wait_time = 200 if post else 30  # post-copy使用更长的等待时间
    answer = None
    try:
        cs.settimeout(max_wait_time)
        data = cs.recv(1024)
        if data:
            answer = data.decode("utf-8")
    except socket.timeout:
        print(
            f"Warning: exceed {max_wait_time} seconds without receiving restore confirmation, "
            "live-migration may encountered issues"
        )
    except Exception as exc:
        print(f"[warn] failed to read restore reply: {exc}")

    # If there is something to read (e.g., from the socket), then print it
    global total_uffd_copy, rpf_handle_time
    if answer:
        print("answer:", answer)
        if "runc restored" in answer:
            # 使用正则表达式提取数据
            pattern = (
                r"runc restored .* successfully with (\d+\.\d+) ms"
                r"(?:, total_uffd_copy: (\d+\.\d+) KB, rpf_handle_time: (\d+\.\d+) ms)?"
            )
            match = re.search(pattern, answer)
            if match:
                rst_time = float(match.group(1))
                print("Restore time: {:.3f} ms".format(rst_time))
                # 检查是否匹配到了 total_uffd_copy 和 rpf_handle_time
                if match.group(2) and match.group(3):
                    total_uffd_copy = float(match.group(2))
                    rpf_handle_time = float(match.group(3))
                    print("Total uffd copy: {:.2f} KB".format(total_uffd_copy))
                    print("Error transfer time: {:.2f} ms".format(rpf_handle_time))
                else:
                    # 如果没有匹配到，说明这是预拷贝的回复
                    total_uffd_copy = None
                    rpf_handle_time = None
            else:
                print("Failed to parse reply:", answer)
        else:
            print("Received reply:", answer)

    # after migration, rootfs sync process and opened files will be closed
    if rootfs:
        stop_sync_rootfs()

    if dirtymap:
        device_file.close()

    # 读取stat-dump计算迁移时间
    # 注意后拷贝时dump_time不通过读取stat-dump获取
    if not post:
        work_path.append(mig_base + "/d_log")
    get_dump_time(work_path_list=work_path)

    # 计算迁移大小
    for path in parent_path:
        get_dump_size(path, pre_dump=True)
    get_dump_size(image_path, pre_dump=False)

    return True


def post_process(max_iter):
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    for i in range(0, max_iter):
        umount_cmd = "umount " + mig_base + "/parent_{}".format(i)
        try:
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
        except Exception:
            pass

    try:
        umount_cmd = "umount " + mig_base + "/image"
        subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    os.chdir(old_cwd)


def touch(fname):
    open(fname, "a").close()


parser = argparse.ArgumentParser(description="manual to migration script for source node")
parser.add_argument("container", help="container's name(identical to bundle name)")
parser.add_argument("dest", help="IP address of destination")
parser.add_argument("-pre", "--pre-copy", dest="pre", action="store_true", help="enable per-copy migration")
parser.add_argument("-post", "--post-copy", dest="post", action="store_true", help="enable post-copy migration")
parser.add_argument(
    "-d",
    "--disk-less",
    dest="diskless",
    action="store_true",
    help="enable disk-less migration(page-server, only effect pre-copy)",
)
parser.add_argument("--no-rootfs", dest="norootfs", action="store_true", help="avoid the synchronization of rootfs")
parser.add_argument("-i", "--iter", type=int, help="Max iterations of pre-dump")
parser.add_argument(
    "-dm",
    "--use-dirty-map",
    dest="dirtymap",
    action="store_true",
    help="use dirty-map to reduce the size of memory dump",
)
parser.add_argument("-tc", "--time-constraint", type=float, default=1000.0, help="max tranfer time constraint(ms)")
parser.add_argument("--replay", dest="replay", action="store_true", help="enable post packets replay")
parser.add_argument(
    "-z",
    "--compress",
    type=int,
    choices=[0, 1, 2, 3, 4],
    default=0,
    help="compression level: 0=off, 1=fastest(2K), 2=fast(4K), 3=standard(16K), 4=best(32K)",
)
parser.add_argument(
    "--bandwidth",
    "-b",
    dest="bandwidth",
    type=str,
    default=None,
    help="(optional) bandwidth limit used by wrappers; ignored by the migration script itself",
)

# 处理 --tcp-established 和 --shell-job 等criu参数
# 将这些参数排除在脚本参数解析之外
parser.add_argument(
    "--monitor",
    action=argparse.BooleanOptionalAction,
    default=True,
    help="Enable resource monitoring (cpu/mem/net) during migration",
)
parser.add_argument(
    "--monitor-interval",
    type=float,
    default=1.0,
    help="Sampling interval (seconds) for resource monitoring",
)
parser.add_argument(
    "--monitor-host-iface",
    type=str,
    default="ens33",
    help="Host network interface used for bandwidth measurements",
)
parser.add_argument(
    "--monitor-out",
    type=str,
    default=None,
    help="Optional output file path for resource monitor (overrides default)",
)

args, remaining = parser.parse_known_args()


def extract_positional_args():
    """从原始命令行中智能提取位置参数(container名和目标IP)"""

    # 定义所有已知的可带数值参数
    value_params = {"-tc", "--time-constraint", "-i", "--iter", "-z", "--compress", "--bandwidth", "-b"}

    i = 1  # 跳过脚本名称
    positional_args = []

    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg.startswith("-"):
            if arg in value_params:
                # 跳过参数名和它的值
                i += 2
                continue
            elif arg.startswith("--"):
                # 长选项，如果占用参数则跳过
                i += 1
                continue
        else:
            positional_args.append(arg)
        i += 1

    return positional_args


# 获取位置参数
positional = extract_positional_args()
container_name = None
runc_args = []

# 第一步：从remaining中提取criu参数（跳过脚本自身的带值参数，如 --bandwidth/-b）
skip_next = False
for arg in remaining:
    if skip_next:
        skip_next = False
        continue

    if arg in {"--bandwidth", "-b"}:
        # 跳过带宽参数本身和随后的数值（如 50mbit），这些不应传给 runc
        skip_next = True
        continue

    if arg.startswith("--") or arg.startswith("-"):
        # criu/runc 参数
        runc_args.append(arg)
    else:
        # 可能是位置参数，但我们基于原始命令行提取更好
        continue

# 第二步：从智能解析的positional参数中提取容器名
if len(positional) > 0:
    container_name = positional[0]  # 第一个位置参数是container名
    if len(positional) > 1 and positional[1] != args.dest:
        # 如果有其他位置参数，作为runc参数（IP地址等不应在这里）
        for extra_arg in positional[1:]:
            if extra_arg not in runc_args:
                runc_args.append(extra_arg)

# 如果仍然没找到，使用备用方法
if not container_name:
    for arg in sys.argv[1:]:
        if not arg.startswith("-") and arg != args.dest:
            container_name = arg
            break

if not container_name:
    parser.error("container name is required")

# print(f"Debug: container_name = '{container_name}'")
# print(f"Debug: criu_args = {runc_args}")

if __name__ == "__main__":

    runc_base = "/runc/containers/"

    pre = False
    post = False
    diskless = False
    replay = False
    rootfs = True
    dirtymap = False
    compress = args.compress

    # 检查用户是否确实提供了时间约束参数
    if "--time-constraint" in sys.argv or "-tc" in sys.argv:
        time_constraint = args.time_constraint
    else:
        time_constraint = -1  # 用户没有提供时间约束，禁用时间约束检查

    if args.iter and not args.pre:
        parser.error("Pre-copy is required when max_iter is provided.")

    if args.replay and args.post:
        parser.error("Post-copy conflicted with replay.")

    if args.pre:
        if args.iter:
            max_iter = args.iter
        else:
            max_iter = 8
    else:
        max_iter = 0  # 当未启用预拷贝时，将 max_iter 设为 0

    # The name of the container is the first argument
    # NOTE: for the way the code is currently written, it must be the same as the name of the OCI bundle
    container = container_name
    # destination IP is the second argument
    dest = args.dest
    # the Pre and Lazy flags, which are used to determine the migration techniques as follows:
    # Cold = False False
    # Pre-copy = True False
    # Post-copy = False True
    # Hybrid = True True
    pre = args.pre
    post = args.post
    replay = args.replay
    dirtymap = args.dirtymap

    # use CRIU's page server to directly transfer memory dump
    diskless = args.diskless
    if diskless and not (pre or post):
        parser.error("Diskless only supported to used in pre/post-copy")

    # rootfs_sync flag, which is used to enable synchronization of container's rootfs
    if args.norootfs:
        rootfs = False

    base_path = runc_base + container
    mig_base = base_path + "/migrate"

    # -h outputs numbers in human readable format
    # -a enables archive mode, which preserves permissions, ownership, and modification times, among other things
    # -z enables compression during transfer
    # -P reserves files which are not completely transferred to speed-up the following re-transferring
    # rsync_opts = "-haz --whole-file"
    rsync_opts = "-az --whole-file"
    ssh_opts = "-o TCPWindowSize=65536 -o SSHBufferSize=65536 -c aes128-ctr"

    # 开始热迁移
    migrate(container, dest, pre, post, replay, rootfs, max_iter, dirtymap, time_constraint, runc_args)

    print("-----------------------statistics---------------")
    transfer_vip("100")  # 把源端优先级恢复到 100s
    # 输出累计的预拷贝时间和大小
    if pre:
        print("Total pre-dump time: {:.0f} ms".format(pre_dump_time_total))
        print("Total pre-dump transfer time: {:.0f} ms".format(pre_dump_xfer_time_total))

    # 输出 dump 的时间和大小
    print("Total dump time: {:.0f} ms".format(dump_time))
    print("Total dump transfer time: {:.0f} ms".format(dump_xfer_time))
    print("resume time (pre dump can use):{:.0f} ms ".format(rst_time))
    if pre:
        print("Total pre-dump size: {:.3f} KB".format(pre_dump_size_total / 1024))  # 转换为 KB
    print("Total dump size:{:.3f} KB".format(dump_size / 1024))  # 转换为 KB
    print("Total compression time: {:.0f} ms".format(total_compression_time))

    compression_overhead = total_compression_time if total_compression_time > 0 else 0.0

    if pre and not post:
        total_time = (
            pre_dump_time_total
            + pre_dump_xfer_time_total
            + dump_time
            + dump_xfer_time
            + rst_time
            + compression_overhead
        )
    elif not pre and post:
        total_time = dump_time + dump_xfer_time + rst_time + rpf_handle_time + compression_overhead
    elif pre and post:
        total_time = (
            pre_dump_time_total
            + pre_dump_xfer_time_total
            + dump_time
            + dump_xfer_time
            + rst_time
            + rpf_handle_time
            + compression_overhead
        )
    else:
        total_time = dump_time + dump_xfer_time + rst_time + compression_overhead

    stop_time = dump_time + dump_xfer_time + rst_time

    print(f"total migrate time: {total_time:.0f} ms")
    print(f"down time: {stop_time:.0f} ms")

    if post:
        total_size = dump_size / 1024 + pre_dump_size_total / 1024 + total_uffd_copy
    else:
        total_size = dump_size / 1024 + pre_dump_size_total / 1024
    print("total migrate size: {:.3f} KB".format(total_size))
    if post:
        print("Faulted pages transfer time（ms）: {:.0f} ms".format(rpf_handle_time))
        print("Faulted pages size(KB): {:.2f} KB".format(total_uffd_copy))
    # input()
    # 迁移完成后，执行后处理
    # for excel

    # 计算压缩相关统计
    total_uncompressed_size = pre_dump_size_total + dump_size
    total_compressed_size = 0.0
    compression_ratio = 100.0

    if compress >= 0:
        total_compressed_size = get_compressed_files_size(mig_base, compress)
        if final_archive_size_bytes > 0:
            total_compressed_size += final_archive_size_bytes
        if compress == 0:
            compression_ratio = 100.0
        elif total_uncompressed_size > 0 and total_compressed_size > 0:
            compression_ratio = (total_compressed_size / total_uncompressed_size) * 100.0
        else:
            compression_ratio = 0.0

        if total_compressed_size > 0:
            print(f"Total compressed size: {total_compressed_size / 1024:.3f} KB")
        else:
            print("Total compressed size: N/A")

        if total_uncompressed_size > 0:
            print(f"Total original size: {total_uncompressed_size / 1024:.3f} KB")

        print(f"Compression Ratio: {compression_ratio:.2f} %")

    # 输出数据行
    output_metrics = []
    output_values = []

    if pre:
        output_metrics.extend(["pre_dump_time_total_ms", "pre_dump_xfer_time_total_ms"])
        output_values.extend([int(round(pre_dump_time_total)), int(round(pre_dump_xfer_time_total))])
    else:
        output_metrics.extend(["pre_dump_time_total_ms", "pre_dump_xfer_time_total_ms"])
        output_values.extend(["NA", "NA"])

    output_metrics.extend(["dump_time_ms", "dump_xfer_time_ms", "restore_time_ms"])
    output_values.extend([int(round(dump_time)), int(round(dump_xfer_time)), int(round(rst_time))])

    output_metrics.append("pre_dump_size_kb")
    if pre:
        output_values.append("{:.2f}".format(pre_dump_size_total / 1024))
    else:
        output_values.append("NA")

    output_metrics.append("dump_size_kb")
    output_values.append("{:.2f}".format(dump_size / 1024))

    output_metrics.extend(["total_time_ms", "downtime_ms", "total_migrate_size_kb"])
    output_values.extend([int(round(total_time)), int(round(stop_time)), int(round(total_size))])

    if post:
        output_metrics.extend(["faulted_pages_xfer_time_ms", "faulted_pages_size_kb"])
        output_values.extend([int(round(rpf_handle_time)), "{:.2f}".format(total_uffd_copy)])
    else:
        output_metrics.extend(["faulted_pages_xfer_time_ms", "faulted_pages_size_kb"])
        output_values.extend(["NA", "NA"])

    output_metrics.append("pre_dump_iterations")
    output_values.append(int(round(pre_dump_iters)) if pre else "NA")

    output_metrics.append("total_compression_time_ms")
    output_values.append(int(round(total_compression_time)))

    output_metrics.append("compression_ratio_percent")
    output_values.append("{:.2f}".format(compression_ratio))

    metrics_line = "\t".join(map(str, output_metrics))
    values_line = "\t".join(map(str, output_values))

    print(f"METRIC_HEADER\t{metrics_line}")
    print(f"METRIC_VALUES\t{values_line}")
    if pre:
        print(f"Pre-dump iterations: {pre_dump_iters}")

    # 将结果追加写入 results.txt 文件，包含指标与取值
    with open("results.txt", "a") as f:
        f.write(metrics_line + "\n")
        f.write(values_line + "\n")

    if diskless:
        post_process(max_iter)
