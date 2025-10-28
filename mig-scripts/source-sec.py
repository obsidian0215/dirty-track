#!/usr/bin/env python
# code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
# import distutils.util
import argparse
import atexit
import fcntl
import json
import os
import re
import select
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
from fcntl import ioctl

import psutil

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


# [修改] 重命名并修正函数
def get_compressed_files_size(directory, compress_level):
    """
    计算目录下 (仅限顶层) 的压缩文件总大小。
    根据 compress_level 决定是查找 .tar.gz 还是 .lzo。
    """
    total_size = 0
    if not os.path.isdir(directory):
        print(f"Warning: Directory not found, cannot calculate compressed size: {directory}")
        return 0

    # 根据压缩级别确定要查找的文件后缀
    if compress_level == 0:
        suffix_to_find = ".tar.gz"
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
                    kill_proc = subprocess.run(f"pkill -P {pid} || true", shell=True, capture_output=True, text=True)
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
                    subprocess.run(
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


# 通过ioctl设置脏页跟踪的目录路径
def ioctl_set_dirty_map_path(device_fd, path):
    # 路径字符串打包为定长字节数组
    buf = struct.pack(f"{len(path)}s", path.encode("utf-8"))
    # 调用 ioctl 传递路径给内核模块
    ioctl(device_fd, IOCTL_SET_DIRTY_MAP_PATH, buf)


# 通过ioctl启动指定进程的脏页跟踪
def ioctl_start_pid(device_fd, pid):
    # pid_t在Python中可以用struct.pack来打包
    buf = bytearray(struct.pack("I", pid))
    ioctl(device_fd, IOCTL_START_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]


# 通过ioctl停止指定进程的脏页跟踪
def ioctl_stop_pid(device_fd, pid):
    buf = bytearray(struct.pack("I", pid))
    ioctl(device_fd, IOCTL_STOP_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]


# 通过ioctl获取脏页跟踪的目录路径
def ioctl_get_dirty_map_path(device_fd):
    buf = bytearray(struct.pack("256s", b"\0" * 256))
    ioctl(device_fd, IOCTL_GET_DIRTY_MAP_PATH, buf)
    # 解包路径字符串
    path = struct.unpack_from(f"{len(buf)}s", buf)[0]
    return path.decode("utf-8").rstrip("\0")


# 获取runc容器进程树的PID
def get_runc_container_pidtree(container_name):
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
    print("Something did not work. Exiting!")
    # 确保在程序终止时停止 sync_rootfs 进程
    stop_sync_rootfs()

    if diskless:
        post_process(max_iter)
    sys.exit(-1)


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


def read_warmlist(file_path):
    """
    读取warmlist文件，返回其中选择次数(uchar)最大的脏页地址(ulong)和选择次数
    """
    try:
        with open(file_path, "rb") as f:
            data = f.read()

            # 每个条目的大小
            entry_size = 9  # sizeof(unsigned long) + sizeof(unsigned char)

            # 验证文件长度是否有效
            if len(data) % entry_size != 0:
                print(f"Invalid warmlist file size: {len(data)} bytes")
                return None, None

            count = len(data) // entry_size
            max_address = None
            max_s_count = 0  # 初始化最大选择次数

            for i in range(count):
                # 解析单个条目
                entry = data[i * entry_size: (i + 1) * entry_size]
                address, s_count = struct.unpack("<QB", entry)  # Q: unsigned long, B: unsigned char

                # 更新最大值
                if s_count > max_s_count:
                    max_address = address
                    max_s_count = s_count

            return max_address, max_s_count
    except Exception as e:
        print(f"Error reading warmlist file {file_path}: {e}")
        return None, None


def read_max_scount(container_pids, dirtymap_path):
    """
    遍历container_pids, 遍历每个pid的温页列表, 找到其中最大的选择次数

    参数:
    - container_pids: 包含容器的 PID 列表
    - dirtymap_path: 存放 warm_list.pid 文件的目录路径

    返回:
    - max_scount: 所有温页列表中最大的选择次数
    """
    max_scount = 0  # 初始化最大选择次数

    for pid in container_pids:
        # 拼接文件路径
        warm_list_file = os.path.join(dirtymap_path, f"warm_list.{pid}")

        # 调用 read_warmlist 函数读取温页列表
        max_address, max_scount_pid = read_warmlist(warm_list_file)

        if max_address is None or max_scount_pid is None:
            print(f"No valid data found for pid {pid}. Skipping.")
            continue

        # 更新全局最大选择次数
        if max_scount_pid > max_scount:
            max_scount = max_scount_pid

    return max_scount


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
    try:
        # 定义 Keepalived 配置文件路径和备份路径
        config_path = "/etc/keepalived/keepalived.conf"
        backup_path = "/etc/keepalived/keepalived.conf.bak"

        # 备份原始配置文件
        shutil.copy(config_path, backup_path)
        # print(f"已备份原始 Keepalived 配置文件到 {backup_path}")

        # 读取原始配置文件内容
        with open(config_path, "r") as f:
            config = f.read()

        # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
        pattern = r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})"

        # 定义替换函数，将 priority设置为比目标节点较低的值
        def repl(match):
            match.group(2)
            new_priority = new_prior  # 设置新的优先级
            # print(f"将 VIP 的优先级从 {original_priority} 更新为 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"

        # 使用正则表达式替换 priority
        new_config, count = re.subn(pattern, repl, config, flags=re.DOTALL)

        if count == 0:
            print("未能找到 vrrp_instance VI_1 中的 priority 配置。请检查配置文件格式。")
            return 1

        # 将修改后的配置写回配置文件
        with open(config_path, "w") as f:
            f.write(new_config)
        # print(f"已更新 Keepalived 配置文件 {config_path}，降低 VIP 优先级。")

        # 重新加载 Keepalived 服务以应用更改
        result = subprocess.run(
            ["sudo", "systemctl", "reload", "keepalived"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )

        if result.returncode != 0:
            print(f"重新加载 Keepalived 服务失败：{result.stderr}")
            # 如果重新加载失败，可以选择恢复备份配置
            shutil.copy(backup_path, config_path)
            subprocess.run(["sudo", "systemctl", "reload", "keepalived"])
            print("已恢复原始 Keepalived 配置文件并重新加载服务。")
            return 1
        else:
            # print("成功重新加载 Keepalived 服务，VIP 迁移已触发。")
            return 0

    except PermissionError:
        print("权限错误：请以具有足够权限的用户（如root）运行此脚本。")
        return 1
    except FileNotFoundError:
        print(f"配置文件 {config_path} 未找到，请确保 Keepalived 已正确安装。")
        return 1
    except Exception as e:
        print(f"发生错误：{e}")
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
    global pre_dump_time_total, pre_dump_size_total
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
    global esti_dump_time, esti_dump_size_pre, esti_dump_size_post
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


# create the dump. This is done for any migration technique. Content of the dump varies depending on the technique.
# dump is stored in the image directory.
# in case of pre-dump present, specify it is in the parent directory.
# When post-copy phase is not present, wait until dump command ends (with p.wait())
# If instead post-copy phase is present, the dump procedure does not write memory pages in image and starts the page server for later transfer of faulted pages.
# the page server will then read local memory dump and send memory pages upon request of the lazy-pages daemon running on the destination.
# The page server listens on port 27.
# Still in case of the post-copy phase, with the --status-fd option, CRIU writes '\0' to the specified pipe when it has finished with the checkpoint and start of the page server
# Read https://criu.org/CLI/opt/--lazy-pages and https://criu.org/CLI/opt/--status-fd for more information.
def real_dump(mig_base, precopy, postcopy, last_iter, dirtymap, replay, cs, inputs, runc_args=None):
    global dump_time, dump_size, dump_xfer_time
    old_cwd = os.getcwd()
    os.chdir(mig_base)

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
        cmd += " --lazy-pages"
        cmd += " --page-server localhost:27"
        read_fd, write_fd = os.pipe()
        fdflags = fcntl.fcntl(write_fd, fcntl.F_GETFD)
        fcntl.fcntl(write_fd, fcntl.F_SETFD, fdflags & ~fcntl.FD_CLOEXEC)
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


# Transfer the previously created pre-dump using nc (同步版本)
def xfer_pre_dump(cs, parent_path, dest, i, port):
    global pre_dump_xfer_time_total

    # print(f"开始传输 PRE-DUMP {i} 到 {dest}")
    # 创建压缩包
    if compress == 0:
        # 无压缩
        # archive_name = os.path.join(mig_base, f"pre_dump_{i}.tar")
        archive_name = os.path.join(mig_base, f"pre_dump_{i}.tar.gz")
        cmd_tar = f"tar -cf {archive_name} -C {parent_path} ."
    elif compress >= 1 and compress <= 4:
        # 使用lzo_gpu压缩
        tar_name = os.path.join(mig_base, f"pre_dump_{i}.tar")
        archive_name = os.path.join(mig_base, f"pre_dump_{i}.tar.lzo")
        # 先创建tar文件
        cmd_tar = f"tar -cf {tar_name} -C {parent_path} ."
        # 再使用lzo_gpu压缩
        lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")
        cmd_compress = f"{lzo_gpu_path} -{compress} {tar_name} {archive_name}"
    else:
        raise ValueError(f"不支持的压缩等级: {compress}")

    # print(cmd_tar)
    start = time.perf_counter() * 1000

    if compress == 0:
        # 无压缩，直接创建tar文件
        ret = os.system(cmd_tar)
        if ret != 0:
            exit_code = ret >> 8
            print(f"Create tar pre_dump_{i} failed, ExitCode: {exit_code}")
            raise RuntimeError(f"tar pre_dump_{i} failed")
    else:
        # 有压缩：先创建tar文件，再压缩
        ret = os.system(cmd_tar)
        if ret != 0:
            exit_code = ret >> 8
            print(f"Create tar pre_dump_{i} failed, ExitCode: {exit_code}")
            raise RuntimeError(f"tar pre_dump_{i} failed")

        # 检查tar文件是否存在
        if not os.path.exists(tar_name):
            raise FileNotFoundError(f"TAR file {tar_name} not found")

        # 再进行lzo压缩
        ret = os.system(cmd_compress)
        if ret != 0:
            exit_code = ret >> 8
            print(f"LZO compress pre_dump_{i} failed, ExitCode: {exit_code}")
            raise RuntimeError(f"lzo_gpu compress pre_dump_{i} failed")

        # 删除中间的tar文件
        try:
            os.remove(tar_name)
        except OSError as e:
            print(f"警告：无法删除临时tar文件 {tar_name}: {e}")

    end = time.perf_counter() * 1000

    global total_compression_time
    total_compression_time += end - start
    if not os.path.exists(archive_name):
        raise FileNotFoundError(f"Archive file {archive_name} not found")

    size = os.path.getsize(archive_name)
    if size == 0:
        raise ValueError(f"pre_dump_{i} archive size is 0")

    print(f"Pre-dump {i} archive: {size} Bytes, {(end - start):.3f} ms")

    # 传输到目标服务器：使用 scp/rsync（根据大小选择）
    remote_dir = f"root@{dest}:{parent_path}"

    # 选择传输工具：大文件使用 rsync（可续传+效率），小文件使用 scp
    transfer_cmd = None
    RSYNC_THRESHOLD = 10 * 1024 * 1024  # 10MB
    if size >= RSYNC_THRESHOLD:
        # rsync via ssh
        transfer_cmd = f"rsync -av --inplace -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' {archive_name} {remote_dir}/"
    else:
        transfer_cmd = (
            f"scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {archive_name} {remote_dir}/"
        )

    # 传输时增加重试机制
    max_retries = 3
    attempt = 0
    transfer_time = 0
    success = False
    while attempt < max_retries:
        attempt += 1
        print(f"Transferring pre-dump {i} to {dest} using: {transfer_cmd} (attempt {attempt})")
        start = time.perf_counter() * 1000
        ret = os.system(transfer_cmd)
        end = time.perf_counter() * 1000
        transfer_time = end - start
        print(f"Pre-dump {i} xfer attempt {attempt}: {transfer_time:.3f} ms")
        if ret == 0:
            success = True
            break
        else:
            print(f"Pre-dump {i} xfer failed (attempt {attempt}), ExitCode: {ret}")
            if attempt < max_retries:
                backoff = 2**attempt
                print(f"Retrying after {backoff}s...")
                time.sleep(backoff)

    if not success:
        error()

    # 通知目标端已经收到归档并请求目标端解包/处理（两阶段：立即 ACK，再异步轮询目标处理完成标记）
    try:
        notify = json.dumps(
            {
                "archive_ready": {
                    "path": parent_path,
                    "archive": os.path.basename(archive_name),
                    "compress": compress,
                    "iter": i,
                }
            }
        )

        # 发送通知并等待短时ACK
        cs.send(bytes(notify, encoding="utf-8"))
        inputready, _, _ = select.select([cs], [], [], 10)
        if inputready:
            resp = cs.recv(2048).decode("utf-8")
            # 期待目标端快速返回 'RECEIVED'
            if not resp or "RECEIVED" not in resp:
                print(f"Destination did not ACK archive upload: {resp}")
                error()
        else:
            print("No immediate ACK from destination (timeout)")
            error()

    except Exception as e:
        print(f"Error notifying destination about archive: {e}")
        error()

    # 异步轮询目标上是否写入了 .{archive}.processed 标记（不阻塞主流程）
    def _poll_processed():
        marker = os.path.join(parent_path, f".{os.path.basename(archive_name)}.processed")
        err_marker = os.path.join(parent_path, f".{os.path.basename(archive_name)}.processed.err")
        check_cmd = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{dest} 'test -f {marker} && echo OK || (test -f {err_marker} && echo ERR || echo NO)'"
        timeout = 600.0
        end = time.time() + timeout
        while time.time() < end:
            try:
                proc = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                out = proc.stdout.strip()
                if out == "OK":
                    print(f"Pre-dump {i} processed on destination")
                    return
                if out == "ERR":
                    print(f"Pre-dump {i} extraction failed on destination")
                    return
            except Exception as e:
                print(f"Error polling processed marker: {e}")
            time.sleep(1.0)

    poll_thread = threading.Thread(target=_poll_processed, daemon=True)
    poll_thread.start()

    if time_constraint > 0:
        bandwidth_measurements.append(1000.0 * size / transfer_time)

    pre_dump_xfer_time_total += transfer_time


# Transfer the previosuly created dump using rsync
def xfer_final(cs, image_path, dest, compress, port):
    global dump_xfer_time

    # print("xfer DUMP")
    # 创建压缩包并通过 SSH 传输
    if compress == 0:
        # 无压缩
        f"tar -cf - -C {image_path} . | nc -q 0 {dest} {port}"
        # 创建压缩包并通过 SSH 传输
    elif compress >= 1 and compress <= 4:
        # 使用lzo_gpu压缩：先创建tar文件，再压缩成本地lzo文件，最后传输
        start = time.perf_counter() * 1000
        tar_name = os.path.join(mig_base, "final_dump.tar")
        lzo_name = os.path.join(mig_base, "final_dump.tar.lzo")
        lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")

        # 先创建tar文件
        cmd_create_tar = f"tar -cf {tar_name} -C {image_path} ."
        ret = os.system(cmd_create_tar)
        if ret != 0:
            exit_code = ret >> 8
            print(f"Create final tar failed, ExitCode: {exit_code}")
            raise RuntimeError("tar final dump failed")

        # 再压缩为lzo
        cmd_compress = f"{lzo_gpu_path} -{compress} {tar_name} {lzo_name}"
        ret = os.system(cmd_compress)
        if ret != 0:
            exit_code = ret >> 8
            print(f"LZO compress final dump failed, ExitCode: {exit_code}")
            raise RuntimeError("lzo_gpu compress final dump failed")
        end = time.perf_counter() * 1000
        global total_compression_time
        total_compression_time += end - start  # 累加压缩时间
        # 删除临时tar文件
        try:
            os.remove(tar_name)
        except OSError as e:
            print(f"警告：无法删除临时tar文件 {tar_name}: {e}")

        # 通过nc传输lzo文件
        f"nc -q 0 {dest} {port} < {lzo_name}"
    else:
        raise ValueError(f"不支持的压缩等级: {compress}")
    # 传输到目标服务器：使用 scp/rsync 选择
    # 如果是已生成的文件(lzo或tar), 找到要传输的实际文件
    if compress == 0:
        # tar via stdout handled earlier as cmd_tar; but we now prefer to create a file and scp it
        # 使用 tar -cf - | ssh dest "cat > /path/to/image_archive.tar"
        archive_name = os.path.join(mig_base, "final_dump.tar.gz")
        # 生成压缩tar.gz
        cmd_make = f"tar -czf {archive_name} -C {image_path} ."
        ret = os.system(cmd_make)
        if ret != 0:
            print("Create final tar.gz failed")
            error()
    elif compress >= 1 and compress <= 4:
        lzo_name = os.path.join(mig_base, "final_dump.tar.lzo")
        archive_name = lzo_name
    else:
        raise ValueError(f"不支持的压缩等级: {compress}")

    # 选择传输工具
    RSYNC_THRESHOLD = 10 * 1024 * 1024
    size = os.path.getsize(archive_name)
    remote_dir = f"root@{dest}:{image_path}"
    if size >= RSYNC_THRESHOLD:
        transfer_cmd = f"rsync -av --inplace -e 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' {archive_name} {remote_dir}/"
    else:
        transfer_cmd = (
            f"scp -q -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null {archive_name} {remote_dir}/"
        )

    print(f"Transferring final dump to {dest} using: {transfer_cmd}")
    # final transfer with retries
    max_retries = 3
    attempt = 0
    dump_xfer_time = 0
    success = False
    while attempt < max_retries:
        attempt += 1
        start = time.perf_counter() * 1000
        ret = os.system(transfer_cmd)
        end = time.perf_counter() * 1000
        dump_xfer_time = end - start
        print(f"Final dump xfer attempt {attempt}: {dump_xfer_time:.3f} ms")
        if ret == 0:
            success = True
            break
        else:
            print(f"Final dump transfer failed (attempt {attempt}), ExitCode: {ret}")
            if attempt < max_retries:
                backoff = 2**attempt
                print(f"Retrying after {backoff}s...")
                time.sleep(backoff)

    if not success:
        error()

    # 通知目标端解包并处理，等待短时ACK
    try:
        notify = json.dumps(
            {
                "archive_ready": {
                    "path": image_path,
                    "archive": os.path.basename(archive_name),
                    "compress": compress,
                    "final": True,
                }
            }
        )
        cs.send(bytes(notify, encoding="utf-8"))
        inputready, _, _ = select.select([cs], [], [], 10)
        if inputready:
            resp = cs.recv(4096).decode("utf-8")
            if not resp or "RECEIVED" not in resp:
                print(f"Destination did not ACK final archive: {resp}")
                error()
        else:
            print("Timed out waiting for destination ACK for final archive")
            error()
    except Exception as e:
        print(f"Error notifying destination about final archive: {e}")
        error()

    # 对 final dump 在主流程中同步等待目标处理完成（最长等待600s）
    marker = os.path.join(image_path, f".{os.path.basename(archive_name)}.processed")
    err_marker = os.path.join(image_path, f".{os.path.basename(archive_name)}.processed.err")
    check_cmd = f"ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@{dest} 'test -f {marker} && echo OK || (test -f {err_marker} && echo ERR || echo NO)'"
    timeout = 600.0
    end_time = time.time() + timeout
    processed_ok = False
    while time.time() < end_time:
        try:
            proc = subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            out = proc.stdout.strip()
            if out == "OK":
                processed_ok = True
                break
            if out == "ERR":
                print("Final dump extraction failed on destination")
                error()
        except Exception as e:
            print(f"Error polling final processed marker: {e}")
        time.sleep(1.0)

    if not processed_ok:
        print("Timed out waiting for destination to process final archive")
        error()


# Run the pre-dump iteration and transfer it to the destination
def iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap):
    global port_list
    iter_terminate = False
    last_iter = 1
    if dirtymap:
        # 在pre-copy开启前先启动对容器的dirty-track
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)
    while last_iter <= max_iter:
        last_path = parent_path[last_iter - 1]
        # if diskless:
        # #send the page server command,
        # #after the server's response, CRIU can directly transfer memory dump with network
        # pageserver_cmd = '{ "pageserver" : { "path" : "' + last_path + '", "iter" : "' + str(last_iter) + '} }'
        # cs.send(bytes(pageserver_cmd, encoding='utf-8'))
        # inputready, outputready, exceptready = select.select(input, [], [], 4)
        # #If after 4 seconds there is something to read(e.g., error msg from the socket), then print it and exit
        # if inputready:
        #     for s in inputready:
        #         answer = s.recv(1024)
        #         print(answer)
        #         error()
        # diskless_pre_dump(mig_base, container, dest, last_iter, dirtymap)
        # else:
        pre_dump(mig_base, container, last_iter, dirtymap)

        dir_size = getdirsize(last_path, "pages")
        # print("parent_path:",parent_path)
        # print("last_iter:",last_iter)
        less_last_path = parent_path[last_iter - 2] if last_iter > 1 else None
        # print("less_last_path:",less_last_path)

        # 更新最大predump大小
        global max_predump_size
        if dir_size > max_predump_size:
            max_predump_size = dir_size
        # if abs(dir_size - getdirsize(less_last_path, 'pages')) < 1024 * 64 \
        #             or (dir_size < 1024 * 64) or last_iter == max_iter:     #64KB
        #     iter_terminate = True

        if last_iter == 1:
            # 第一次 pre-dump 不需要比较，直接判断目录大小
            if dir_size < 1024 * 64 or last_iter == max_iter:
                iter_terminate = True
        else:
            # 否则比较两次 pre-dump 目录大小
            less_last_path = parent_path[last_iter - 2]
            if (
                abs(dir_size - getdirsize(less_last_path, "pages")) < 1024 * 64
                or dir_size < 1024 * 64
                or last_iter == max_iter
            ):
                iter_terminate = True

        if dirtymap:
            if read_max_scount(container_pids, dirtymap_path) >= 3:
                iter_terminate = True

        # 传输当前迭代的 pre-dump
        xfer_pre_dump(cs, last_path, dest, last_iter, port_list[last_iter - 1])
        if iter_terminate:
            break
        last_iter += 1
    print("last_iter:", last_iter)
    print("less_last_path:", less_last_path)
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


INIT_PORT = 12345


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
    global port_list
    port_list = [INIT_PORT]

    if pre:
        for i in range(1, max_iter + 1):
            pathname = mig_base + "/parent_{}".format(i)
            parent_path.append(pathname)
            pathname = mig_base + "/pd_log_{}".format(i)
            work_path.append(pathname)
            port_list.append(INIT_PORT + i)  # del +1

    print("post_list: {0}", port_list)
    print("parent_path: {0}", parent_path)
    prepare(mig_base, image_path, parent_path, work_path)

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

    if pre:
        prepare_cmd = json.dumps(
            {
                "prepare": {
                    "path": mig_base,
                    "image_path": image_path,
                    "parent_path": parent_path,  # parent_path为列表
                    "compress": compress,
                }
            }
        )
    else:
        prepare_cmd = json.dumps(
            {
                "prepare": {
                    "path": mig_base,
                    "image_path": image_path,
                    "compress": compress,
                    # 不包含 parent_path
                }
            }
        )

    cs.send(bytes(prepare_cmd, encoding="utf-8"))
    inputready, outputready, exceptready = select.select(inputs, [], [], 4)
    # If after 4 seconds there is something to read(e.g., error msg from the socket), then print it and exit
    if inputready:
        for s in inputready:
            answer = s.recv(1024).decode("utf-8")
            # print(answer)
            pattern = r"OK"
            match = re.search(pattern, answer)
            if not match:
                print(answer)
                error()

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

        # iter pre-dump
        last_iter = iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap)
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
            global max_predump_size
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

    real_dump(mig_base, pre, post, last_iter, dirtymap, replay, cs, inputs, runc_args)
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
    xfer_final(cs, image_path, dest, compress, port_list[-1])

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
    time_left = max_wait_time
    polling_interval = 5  # 每5秒检测一次，防止占用过多CPU

    while time_left > 0:
        wait_time = min(polling_interval, time_left)
        inputready, outputready, exceptready = select.select(inputs, [], [], wait_time)

        if inputready:
            break  # 收到数据，跳出等待循环

        time_left -= polling_interval
        print(f"  Remaining {time_left} seconds...")

        if time_left <= 0:
            print(
                f"Warning: exceed {max_wait_time} seconds without receiving restore confirmation, live-migration may encountered issues"
            )
    # If there is something in input to read (e.g., from the socket), then print it
    global total_uffd_copy, rpf_handle_time
    for s in inputready:
        answer = s.recv(1024).decode("utf-8")
        print("answer:", answer)
        if "runc restored" in answer:
            # 使用正则表达式提取数据
            pattern = r"runc restored .* successfully with (\d+\.\d+) ms(?:, total_uffd_copy: (\d+\.\d+) KB, rpf_handle_time: (\d+\.\d+) ms)?"
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

# 处理 --tcp-established 和 --shell-job 等criu参数
# 将这些参数排除在脚本参数解析之外
args, remaining = parser.parse_known_args()


def extract_positional_args():
    """从原始命令行中智能提取位置参数(container名和目标IP)"""

    # 定义所有已知的可带数值参数
    value_params = {"-tc", "--time-constraint", "-i", "--iter", "-z", "--compress"}

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

# 第一步：从remaining中提取criu参数
for arg in remaining:
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

    if pre and not post:
        total_time = pre_dump_time_total + pre_dump_xfer_time_total + dump_time + dump_xfer_time + rst_time
    elif not pre and post:
        total_time = dump_time + dump_xfer_time + rst_time + rpf_handle_time
    elif pre and post:
        total_time = (
            pre_dump_time_total + pre_dump_xfer_time_total + dump_time + dump_xfer_time + rst_time + rpf_handle_time
        )

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

    # [修改] 计算压缩率并为 output_values 准备变量
    compression_ratio = 0.0  # 默认初始化
    # [新] 计算并打印压缩率
    if compress >= 0:
        # 1. 计算 LZO 文件总大小 (分子)
        # mig_base 在 __main__ 块的开头 (约 1756 行) 已经定义
        # total_lzo_size = get_lzo_files_size(mig_base)
        total_compressed_size = get_compressed_files_size(mig_base, compress)  # <-- 修正后的调用
        # 2. 计算未压缩数据总大小 (分母)
        # pre_dump_size_total 和 dump_size 是全局变量,
        # 并在 migrate() 函数末尾通过 get_dump_size() 填充
        total_uncompressed_size = pre_dump_size_total + dump_size

        compression_ratio = 0.0
        if total_uncompressed_size > 0:
            # 压缩率 = (压缩后大小 / 压缩前大小) * 100%
            compression_ratio = total_uncompressed_size / total_compressed_size

        print(f"Total LZO (compressed) size: {total_compressed_size / 1024:.3f} KB")
        # print(f'Total Original (uncompressed) size: {total_uncompressed_size / 1024:.3f} KB')
        print(f"Compression Ratio: {compression_ratio:.2f} %")
    # [新功能结束]

    # 输出数据行
    output_values = []

    if pre:
        output_values.extend([int(round(pre_dump_time_total)), int(round(pre_dump_xfer_time_total))])

    output_values.extend([int(round(dump_time)), int(round(dump_xfer_time)), int(round(rst_time))])

    if pre:
        output_values.append("{:.2f}".format(pre_dump_size_total / 1024))  # 预拷贝大小以KB为单位
    else:
        output_values.append("")

    output_values.append("{:.2f}".format(dump_size / 1024))  # dump_size以KB为单位

    output_values.extend([int(round(total_time)), int(round(stop_time))])
    output_values.append(int(round(total_size)))
    if post:
        output_values.extend([int(round(rpf_handle_time)), "{:.2f}".format(total_uffd_copy)])
    if pre:
        output_values.append(int(round(pre_dump_iters)))
    print("aaaaaaaaaaaaaaaa")
    if compress > 0:
        print("bbbbbbbbbbbb")
        output_values.append(int(round(total_compression_time)))
        # output_values.append(int(round(compression_ratio)))
        output_values.append("{:.2f}".format(compression_ratio))
    print("\t".join(map(str, output_values)))
    if pre:
        print(f"Pre-dump iterations: {pre_dump_iters}")

    output_line = "\t".join(map(str, output_values))
    # 将结果追加写入 results.txt 文件
    with open("results.txt", "a") as f:
        f.write(output_line + "\n")

    if diskless:
        post_process(max_iter)
