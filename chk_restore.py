#!/usr/bin/env python
#code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
import socket
import sys
import select
import time
import os
import shutil
import subprocess
#import distutils.util
import argparse
import json
from fcntl import ioctl
import psutil
import struct
import fcntl
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field
import statistics
import math
import bisect
import re

# 定义字符设备路径
DEVICE_PATH = '/dev/dirty-track'

# 定义ioctl命令相关参数
IOC_NRBITS = 8
IOC_TYPEBITS = 8
IOC_SIZEBITS = 14
IOC_DIRBITS = 2

IOC_NRSHIFT = 0
IOC_TYPESHIFT = IOC_NRSHIFT + IOC_NRBITS        # 8
IOC_SIZESHIFT = IOC_TYPESHIFT + IOC_TYPEBITS    # 16
IOC_DIRSHIFT = IOC_SIZESHIFT + IOC_SIZEBITS     # 30

IOC_NONE = 0
IOC_WRITE = 1
IOC_READ = 2

IOC_IN = IOC_WRITE << IOC_DIRSHIFT
IOC_OUT = IOC_READ << IOC_DIRSHIFT
IOC_IO = (IOC_WRITE | IOC_READ) << IOC_DIRSHIFT

def _IO(type, nr):
    return (IOC_NONE | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOR(type, nr, size):
    return (IOC_OUT | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOW(type, nr, size):
    return (IOC_IN | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOWR(type, nr, size):
    return (IOC_IO | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

# 定义 ioctl 命令
DIRTY_TRACK_MAGIC = ord('d')
IOCTL_SET_DIRTY_MAP_PATH = _IOW(DIRTY_TRACK_MAGIC, 1, 256)
IOCTL_START_PID = _IOW(DIRTY_TRACK_MAGIC, 2, 4)
IOCTL_STOP_PID = _IOW(DIRTY_TRACK_MAGIC, 3, 4)
IOCTL_STOP_ALL = _IO(DIRTY_TRACK_MAGIC, 4)
IOCTL_GET_DIRTY_MAP_PATH = _IOR(DIRTY_TRACK_MAGIC, 5, 256)

# 定义页面大小映射，根据 page_type 索引
PAGE_SIZES = [1 << 12, 1 << 21]     # 0: 4KB PTE, 1: 2MB PMD

"""
struct __((packed))__ {
    uint64_t address;
    uint32_t write_count;
    // uint8_t page_type;   //size = 13
    uint32_t size;  // size = 16
}
"""
@dataclass
class DirtyMapEntry:
    address: int
    write_count: float
    size: int

    @property
    def start(self) -> int:
        """
        返回页的起始地址
        """
        return self.address

    @property
    def end(self) -> int:
        """
        返回页的结束地址
        """
        return self.address + self.size

"""
struct __((packed))__ {
    uint64_t address;
    // uint32_t write_count;
    uint32_t size;

    // added field
    uint8_t heat_level;
    int8_t heat_trend;
    uint8_t selected;   //size = 15
}
"""
@dataclass
class DirtyHeatMapEntry:
    address: int
    # write_count: float
    size: int
    heat_level: int = field(default=10)
    heat_trend: int = field(default=0)
    selected: int = field(default=0)

    @property
    def start(self) -> int:
        """
        返回页的起始地址
        """
        return self.address

    @property
    def end(self) -> int:
        """
        返回页的结束地址
        """
        return self.address + self.size

# 定义容器进程树的 pid 列表
container_pids = []

mig_time = 0.0
chk_time = 0.0
rst_time = 0.0

# 表示pre-copy需要提前停止的标志
precopy_limit = False

# 初始化迭代和处理过的dirtymap文件
iter_dirtymaps = []
processed_files = set()

# 通过ioctl设置脏页跟踪的目录路径
def ioctl_set_dirty_map_path(device_fd, path):
    # 路径字符串打包为定长字节数组
    buf = struct.pack(f'{len(path)}s', path.encode('utf-8'))
    # 调用 ioctl 传递路径给内核模块
    ioctl(device_fd, IOCTL_SET_DIRTY_MAP_PATH, buf)

# 通过ioctl启动指定进程的脏页跟踪
def ioctl_start_pid(device_fd, pid):
    # pid_t在Python中可以用struct.pack来打包
    buf = bytearray(struct.pack('I', pid))
    ioctl(device_fd, IOCTL_START_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]

# 通过ioctl停止指定进程的脏页跟踪
def ioctl_stop_pid(device_fd, pid):
    buf = bytearray(struct.pack('I', pid))
    ioctl(device_fd, IOCTL_STOP_PID, buf)
    # ret = struct.unpack_from('I', buf)[0]

# 通过ioctl停止所有进程的脏页跟踪
def ioctl_stop_all(device_fd):
    ioctl(device_fd, IOCTL_STOP_ALL, None)

# 通过ioctl获取脏页跟踪的目录路径
def ioctl_get_dirty_map_path(device_fd):
    buf = bytearray(struct.pack('256s', b'\0' * 256))
    ioctl(device_fd, IOCTL_GET_DIRTY_MAP_PATH, buf)
    # 解包路径字符串
    path = struct.unpack_from(f'{len(buf)}s', buf)[0]
    return path.decode('utf-8').rstrip('\0')

# 获取runc容器进程树的PID
def get_runc_container_pidtree(container_name):
    container_pids.clear()      # 先清空pid列表
    container_pid_path = f'/run/runc/{container_name}/state.json'
    if not os.path.exists(container_pid_path):
        raise FileNotFoundError(f"runc容器{container_name}的状态文件不存在：{container_pid_path}")

    with open(container_pid_path, 'r') as f:
        state = json.load(f)
        init_pid = state['init_process_pid']

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
    if diskless:
        post_process(max_iter)
    sys.exit(-1)

# 迁移开始前，指定dirty-map的目录路径
def set_dirty_map_path(device_fd, path):
    """设置脏页跟踪的目录路径"""
    if not os.path.exists(path):
        os.mkdir(path)
    # 挂载到tmpfs
    mount_cmd = f'mount -t tmpfs none {path}'
    ret = os.system(mount_cmd)
    if ret != 0:
        raise SystemError(f"无法将{path}装载到tmpfs")
    print(f"设置脏页跟踪的目录路径为: {os.path.abspath(path)}")
    ioctl_set_dirty_map_path(device_fd, path)

# 在pre-dump之间执行dirty-track并获取dirty-map
def execute_dirty_track(device_fd, first):
    """启动并停止脏页跟踪，获取dirty-map"""
    # 启动所有容器进程的脏页跟踪
    # 启动暂时放入criu中
    if first:
        for pid in container_pids:
            ioctl_start_pid(device_fd, pid)
            print(f"启动对PID {pid}的脏页跟踪")

    # 等待一段时间以收集脏页数据
    time.sleep(1)  # 根据实际情况调整等待时间

    # 停止所有容器进程的脏页跟踪
    for pid in container_pids:
        ioctl_stop_pid(device_fd, pid)
        print(f"停止对PID {pid}的脏页跟踪")

    # 获取 dirty-map 路径
    # dirty_map_path = ioctl_get_dirty_map_path(device_fd)
    # print(f"脏页跟踪目录路径: {dirty_map_path}")

    # return dirty_map_path

# 准备好迁移所需的镜像目录，同时要清除之前的迁移残留的镜像
# 需要先尝试删除image和parent的整个目录树
def prepare(base_path, image_path, parent_path, work_path):
    if os.path.exists(base_path):
        try:
            umount_cmd = 'umount ' + image_path
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
            shutil.rmtree(image_path)
            shutil.rmtree(base_path + '/d_log')
        except:
            pass

        try:
            dir_list = os.listdir(base_path)
            for entry in dir_list:
                entry_path = os.path.join(base_path, entry)
                # print(entry)
                # print(entry_path)
                if os.path.isdir(entry_path) and entry.startswith('parent'):
                    umount_cmd = 'umount ' + entry_path
                    subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
                    shutil.rmtree(entry_path)
                elif os.path.isdir(entry_path) and entry.startswith('pd_log'):
                    shutil.rmtree(entry_path)
        except:     
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
    os.mkdir(base_path + '/d_log')

# 功能函数：获取目录下特定模式文件的总大小
# pattern: 文件名模式, e.g. "pages*.img"
def getdirsize(path, pattern=None):
    tsize = 0
    if not os.path.exists(path):
        return tsize
    
    #skip soft link file
    if os.path.islink(path):
        return tsize

    #avoid stat certain filename pattern
    if os.path.isfile(path):
        tsize = os.path.getsize(path) # 5041481
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
                    subdir_size = getdirsize(sub_entry_path, pattern) # 5800007
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
    
def convert_byte(tsize):
    if tsize < 1024:
        return(round(tsize,2),'Byte')
    else:
        KBX = tsize / 1024
        if KBX < 1024:
            return(round(KBX,2),'KB')
        else:
            MBX = KBX / 1024
            if MBX < 1024:
                return(round(MBX,2),'MB')
            else:
                return(round(MBX/1024,2),'GB')

# 带宽测量（使用异步或多线程）
def measure_bandwidth(dest_ip):
    print(f"开始测量到{dest_ip}的带宽")
    try:
        # 使用 iperf3 进行短时间带宽测量
        result = subprocess.run(['iperf3', '-c', dest_ip, '-t', '3', '-f', 'm', '-J'], 
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode != 0:
            print("带宽测量失败:", result.stderr)
            return 0
        iperf_output = json.loads(result.stdout)
        bandwidth = iperf_output['end']['sum_sent']['bits_per_second'] / 8  # 转换为Bytes/s
        print(f"测得带宽: {bandwidth:.2f} Bytes/s")
        return bandwidth
    except Exception as e:
        print("带宽测量异常:", e)
        return 0
    
# dirtymap/heatmap处理函数
def write_heatmap_to_file(heatmap: List[DirtyHeatMapEntry], output_file: str):
    """
    写入heatmap文件

    Args:
        heatmap (List[DirtyHeatMapEntry]): 假定地址已升序的heatmap
        output_file (str): 输出文件路径
    """
    
    # 合并地址连续且 heat_level 相同的条目
    if not heatmap:
        return

    # 确保heatmap按地址升序排序
    # heatmap = sorted(heatmap, key=lambda x: x.address)

    merged_heatmap = [heatmap[0]]

    for current_entry in heatmap[1:]:
        last_entry = merged_heatmap[-1]
        # 检查当前条目是否与最后一个合并条目连续且 heat_level 相同
        if (last_entry.address + last_entry.size == current_entry.address) and (last_entry.heat_level == current_entry.heat_level):
            # 合并条目：增加 size
            last_entry.size += current_entry.size
        else:
            # 不满足合并条件，直接添加到合并列表
            merged_heatmap.append(current_entry)

    with open(output_file, 'wb') as f:
        for entry in merged_heatmap:
            entry_packed = struct.pack('<QIBbB', entry.address, entry.size, entry.heat_level, entry.heat_trend, entry.selected)
            f.write(entry_packed)

def write_dirty_map_to_file(dirty_map: List[DirtyMapEntry], output_file: str):
    """
    写入dirtymap文件

    Args:
        dirty_map (List[DirtyMapEntry]): dirtymap
        output_file (str): 输出文件路径
    """
    with open(output_file, 'wb') as f:
        for entry in dirty_map:
            if entry.size in PAGE_SIZES:
                # 如果大小是4KB或2MB，按原方式处理
                page_type = PAGE_SIZES.index(entry.size)
                write_count = max(int(entry.write_count), 1)  # 确保不会出现0
                entry_packed = struct.pack('<QIB', entry.address, write_count, page_type)
                f.write(entry_packed)
            else:
                # 如果大小不是4KB或2MB，需要拆分为多个4KB条目
                num_4kb_pages = entry.size // PAGE_SIZES[0]
                remaining_size = entry.size % PAGE_SIZES[0]

                base_address = entry.address
                for _ in range(num_4kb_pages):
                    # 每个4KB条目
                    split_entry_packed = struct.pack('<QIB', base_address, max(int(entry.write_count), 1), 0)
                    f.write(split_entry_packed)
                    base_address += PAGE_SIZES[0]

                if remaining_size > 0:
                    # 处理最后剩余的部分，如果有的话，按照4KB对齐
                    split_entry_packed = struct.pack('<QIB', base_address, max(int(entry.write_count), 1), 0)
                    f.write(split_entry_packed)

def insert_entry_to_consolidated(consolidated: List[DirtyMapEntry], new_entry: DirtyMapEntry):
    """
    将新的 DirtyMapEntry 合并到 consolidated 列表中，处理与现有条目的重叠部分。
    
    Args:
        consolidated (List[DirtyMapEntry]): 已整合的脏页映射条目列表，按address升序排序。
        new_entry (DirtyMapEntry): 需要合并的新条目
    """
    # 如果 consolidated 为空，直接插入 new_entry
    if not consolidated:
        consolidated.append(new_entry)
        return

    # 使用二分查找快速定位 new_entry.address 在 consolidated 中的位置
    addresses = [entry.address for entry in consolidated]
    index = bisect.bisect_left(addresses, new_entry.address)
    if index < len(consolidated) and consolidated[index].address == new_entry.address and consolidated[index].size == new_entry.size:
        # 完全重叠，直接相加 write_count
        consolidated[index].write_count += new_entry.write_count
    elif index < len(consolidated) and consolidated[index].address == new_entry.address and consolidated[index].size != new_entry.size:
        # 不会发生，因为所有条目都是4KB，大小恒定
        pass
    else:
        # 无重叠，直接插入
        bisect.insort(consolidated, new_entry, key=lambda x: x.address)

def prehandle_dirtymap(dirty_map_path: str) -> List[Dict]:
    """
    预处理dirty_map_path中的dirtymap文件，返回包含各PID的最新dirtymap和整合后的旧dirtymap

    Args:
        dirty_map_path (str): dirty_map_path路径

    Returns:
        List[Dict]: 包含各pid的各类dirtymap
    """
    dirtymap_pids: List[Dict] = []
    per_pid_iter_files: Dict[int, List[Optional[List[str]]]] = {}

    num_iters = len(iter_dirtymaps)

 # 遍历每次迭代的 dirtymap 文件列表
    for iter_idx, iter_dirtymap in enumerate(iter_dirtymaps):
        for filename in iter_dirtymap:
            if not filename.endswith('.dirtymap'):
                continue
            parts = filename.split('-')
            if len(parts) < 2:
                print(f"无法解析文件名 {filename}，跳过")
                continue
            try:
                pid = int(parts[0])
                # timestamp_str = parts[1].split('.')[0]  # 如果需要使用 timestamp，可以保留
                # timestamp = int(timestamp_str)
            except ValueError:
                print(f"无法解析文件名 {filename} 中的 PID，跳过")
                continue

            # 初始化 PID 的 iter_dirtymap_pids 列表
            if pid not in per_pid_iter_files:
                per_pid_iter_files[pid] = [None] * num_iters

            # 添加文件到对应的迭代索引
            per_pid_iter_files[pid][iter_idx] = filename

    # 为每个 PID 生成 iter_dirtymap_pids
    for pid, iter_files in per_pid_iter_files.items():
        # 每个 pid 的 iter_dirtymap_pids 是一个列表，对应每次迭代的文件列表或 None
        # iter_dirtymap_pids: List[Optional[List[str]]] = iter_files  # 已经是对应的列表

        # 处理最新的dirtymap文件
        latest_dirtymap_file = iter_files[-1]

        latest_dirtymap: List[DirtyMapEntry] = []
        if latest_dirtymap_file:
            latest_file_path = os.path.join(dirty_map_path, latest_dirtymap_file)
            try:
                with open(latest_file_path, 'rb') as f:
                    while True:
                        data = f.read(13)  # sizeof(dirty_page) = 8 + 4 + 1 = 13 bytes
                        if not data or len(data) < 13:
                            break
                        address, write_count, page_type = struct.unpack('<QIB', data)
                        num_pages = PAGE_SIZES[page_type] >> 12
                        for _ in range(num_pages):
                            new_entry = DirtyMapEntry(
                                address=address,
                                write_count=max(float(write_count), 1.0),  # Ensure at least 1
                                size=1 << 12
                            )
                            latest_dirtymap.append(new_entry)
                            address += 1 << 12
            except IOError as e:
                print(f"无法读取最新 dirtymap 文件 {latest_file_path}，错误：{e}")

        # Consolidate old dirtymap from previous iterations
        consolidated_old_dirtymap: List[DirtyMapEntry] = []

        # Read all dirtymap files from earlier iterations
        for iter_idx, dirtymap_file in enumerate(iter_files):
            if dirtymap_file is None or iter_idx == num_iters - 1:
                continue
            if dirtymap_file == latest_dirtymap_file:
                continue  # Skip the latest dirtymap

            dirtymap_file_path = os.path.join(dirty_map_path, dirtymap_file)
            try:
                with open(dirtymap_file_path, 'rb') as f:
                    while True:
                        data = f.read(13)
                        if not data or len(data) < 13:
                            break
                        address, write_count, page_type = struct.unpack('<QIB', data)
                        num_pages = PAGE_SIZES[page_type] >> 12
                        weight = 1.0 / (2 ** (num_iters - iter_idx - 1))
                        for _ in range(num_pages):
                            weighted_write_count = weight * float(write_count)
                            new_entry = DirtyMapEntry(
                                address=address,
                                write_count=weighted_write_count,
                                size=1 << 12
                            )
                            insert_entry_to_consolidated(consolidated_old_dirtymap, new_entry)
                            address += 1 << 12
            except IOError as e:
                print(f"无法读取 dirtymap 文件 {dirtymap_file_path}，错误：{e}")

        # old_dirtymap = []
        # # Read existing old dirtymap if it exists
        # old_dirtymap_file = os.path.join(dirty_map_path, f'old-{pid}.dirtymap')
        # if os.path.exists(old_dirtymap_file):
        #     try:
        #         with open(old_dirtymap_file, 'rb') as f:
        #             while True:
        #                 data = f.read(13)
        #                 if not data or len(data) < 13:
        #                     break
        #                 address, write_count, page_type = struct.unpack('<QIB', data)
        #                 new_entry = DirtyMapEntry(
        #                     address=address,
        #                     write_count=float(write_count),
        #                     size=1 << 12
        #                 )
        #                 old_dirtymap.append(new_entry)
        #     except IOError as e:
        #         print(f"无法读取 old dirtymap 文件 {old_dirtymap_file}，错误：{e}")

        # Read transfered information from latest heatmap
        transfered = []
        latest_heatmap_file = os.path.join(dirty_map_path, f'latest-{pid}.heatmap')
        if os.path.exists(latest_heatmap_file):
            try:
                with open(latest_heatmap_file, 'rb') as f:
                    while True:
                        data = f.read(15)  # sizeof(heatmap_entry) = 8 + 4 + 1 + 1 + 1 = 15 bytes
                        if not data or len(data) < 15:
                            break
                        address, size, heat_level, heat_trend, selected = struct.unpack('<QIBbB', data)
                        if selected > 0:
                            transfered.append({'address': address, 'selected': selected})
                            # 存在被选中转储3次的脏页，提前停止预转储
                            if selected >= 3:
                                precopy_limit = True
            except IOError as e:
                print(f"无法读取 heatmap 文件 {latest_heatmap_file}，错误：{e}")

        # Build the PID's information dictionary
        dirtymap_pid_info = {
            'pid': pid,
            'latest_dirtymap': latest_dirtymap,
            'old_dirtymap': consolidated_old_dirtymap,
            # 'last_old_dirtymap': old_dirtymap,
            'transfered': transfered,
            'dirtymap_file': iter_files
        }

        dirtymap_pids.append(dirtymap_pid_info)

    return dirtymap_pids

def detect_extreme_high_wc(dirtymap: List[DirtyMapEntry]) -> float:
    """
    计算给定 dirtymap 中划分异常高 write_count 的阈值。
    Args: dirtymap (List[DirtyMapEntry]): DirtyMapEntry 列表。
    Returns: float: dirtymap 中异常高 write_count 的阈值。
    """
    if not dirtymap:
        return 0
    
    # write_counts = sorted(entry.write_count for entry in dirtymap)
    n = len(dirtymap)
    write_counts = [entry.write_count for entry in dirtymap]
    max_write_count = max(write_counts)
    if max_write_count <= 10:
        return max_write_count
    if n == 0:
        return 0  # 无数据
    elif n < 4096:
        # # 小规模脏内存(<=16MB)：使用四分位数方法
        # try:
        #     # 使用四分位数方法检测异常值
        #     q1 = statistics.quantiles(write_counts, n=4)[0]  # 第一四分位数
        #     q3 = statistics.quantiles(write_counts, n=4)[2]  # 第三四分位数
        #     iqr = q3 - q1
        #     threshold = q3 + 1.5 * iqr
        # except statistics.StatisticsError:
        #     threshold = max(write_counts) * 0.9
    # elif n < 32768:
        # 中等规模脏内存(<=128MB)：使用中位数和MAD
        try:
            median_wc = statistics.median(write_counts)
            mad = statistics.median([abs(wc - median_wc) for wc in write_counts])
            threshold = median_wc + 5 * mad  # 选择5倍MAD作为阈值
        except statistics.StatisticsError:
            median_wc = statistics.median(write_counts)
            threshold = max(write_counts) * 0.9
    else:
        # 大规模脏内存：使用99百分位数
        try:
            percentile_99 = statistics.quantiles(write_counts, n=100)[98]  # 95th 百分位
            threshold = percentile_99
        except statistics.StatisticsError:
            threshold = max(write_counts) * 0.9

    return threshold

def convert_dirtymap_to_heatmap(dirtymap: List[DirtyMapEntry]) -> List[DirtyHeatMapEntry]:
    """
    排除异常高 write_count 后将dirtymap转换为heatmap(并默认按地址升序)

    :param dirtymap: List[DirtyMapEntry]，DirtyMapEntry 实例的列表
    :return: List[DirtyHeatMapEntry]，处理后的 HeatMapEntry 列表
    """
    if not dirtymap:
        return []

    threshold = detect_extreme_high_wc(dirtymap)
    # print(f"阈值为: {threshold}")

    # 找出非异常高 write_count 的最大值，用于归一化
    non_extreme_wcs = [wc.write_count for wc in dirtymap if wc.write_count <= threshold]
    max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1  # 避免除零

    # 确保 dirtymap 按 address 升序排序
    sorted_dirtymap = sorted(dirtymap, key=lambda x: x.address)

    heatmap_entries = []
    for entry in sorted_dirtymap:
        write_count = entry.write_count
        # 判断是否为异常高 write_count
        if write_count >= threshold:
            heat_level = 10  # 最大 heat_level
        else:
            normalized_wc = write_count / max_write_count
            # 归一化后均分为10级
            heat_level = math.ceil(normalized_wc * 9) + 1  # 1到10
            heat_level = min(max(heat_level, 1), 10)  # 确保在范围内

        # 创建 HeatMapEntry 实例
        heatmap_entry = DirtyHeatMapEntry(
            address=entry.address,
            size=entry.size,
            heat_level=heat_level,
            heat_trend=0,
            selected=0
        )
        heatmap_entries.append(heatmap_entry)
    return heatmap_entries

def merge_sub_heat(
    A: List[DirtyMapEntry],
    B: List[DirtyHeatMapEntry]
) -> List[DirtyHeatMapEntry]:
    """
    将DirtyMapEntry列表 A和DirtyHeatMapEntry列表 B取并集
    并根据B中各地址范围的heat_level与A中相同地址范围的heat_level的差值更新heat_trend

    Args:
        A (List[DirtyMapEntry]): 基准DirtyMap
        B (List[DirtyHeatMapEntry]): 目标HeatMap

    Returns:
        List[DirtyHeatMapEntry]: 合并并更新了heat_trend的HeatMap
    """
    PAGE_SIZE = 1 << 12  # 4KB

    # 收集所有4KB对齐的地址
    addresses = set()

    for entry in A:
        for offset in range(0, entry.size, PAGE_SIZE):
            addr = entry.start + offset
            addresses.add(addr)

    for entry in B:
        for offset in range(0, entry.size, PAGE_SIZE):
            addr = entry.start + offset
            addresses.add(addr)

    sorted_addresses = sorted(addresses)

    result = []

    for addr in sorted_addresses:
        # 查找 A 和 B 中覆盖当前地址的条目
        a_entry = next((e for e in A if e.start <= addr < e.end), None)
        b_entry = next((e for e in B if e.start <= addr < e.end), None)

        # 计算 A 的 heat_level
        if a_entry:
            threshold = detect_extreme_high_wc([a_entry])  # 单个条目列表
            write_count = a_entry.write_count
            if write_count >= threshold:
                a_heat_level = 10
            else:
                non_extreme_wcs = [wc.write_count for wc in [a_entry] if wc.write_count < threshold]
                max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1
                normalized_wc = write_count / max_write_count
                a_heat_level = math.ceil(normalized_wc * 9) + 1
                a_heat_level = min(max(a_heat_level, 1), 10)
        else:
            a_heat_level = 0  # 没有对应的 A 条目

        # 获取 B 的 heat_level
        b_heat_level = b_entry.heat_level if b_entry else 0

        # 计算 heat_trend
        heat_trend = b_heat_level - a_heat_level

        # 合并逻辑：仅在地址连续且 heat_trend 相同时进行合并
        if result and (result[-1].address + result[-1].size == addr) and (result[-1].heat_trend == heat_trend):
            # print("合并与前一个条目")
            result[-1].size += PAGE_SIZE
        else:
            # print("添加新的条目到结果")
            new_entry = DirtyHeatMapEntry(
                address=addr,
                size=PAGE_SIZE,
                heat_level=b_heat_level,
                heat_trend=heat_trend,
                selected=0
            )
            result.append(new_entry)

    # # 打印最终结果
    # print("最终结果列表:")
    # for entry in result:
    #     print(entry)

    return result

def merge_update_selected(
    heatmap: List[DirtyHeatMapEntry],
    transfered: List[Dict[str, int]]
) -> None:
    """
    使用二分查找优化查找速度

    Args:
        heatmap (List[DirtyHeatMapEntry]): HeatMapEntry列表，已按地址排序
        transfered (List[Dict[str, int]]): {'address': int, 'selected': int}列表
    """
    heatmap_addresses = [entry.address for entry in heatmap]
    for item in transfered:
        addr = item['address']
        selected = item['selected']
        index = bisect.bisect_left(heatmap_addresses, addr)
        if index < len(heatmap_addresses) and heatmap_addresses[index] == addr:
            heatmap[index].selected = selected
        else:
            print(f"警告: transfered 中的地址 {hex(addr)} 未在 heatmap 中找到")

def generate_heatmap(dirty_map_path: str) -> None:
    # 确保输出目录存在
    os.makedirs(dirty_map_path, exist_ok=True)
    dirtymap_pids = prehandle_dirtymap(dirty_map_path)

    for pid_dirtymap in dirtymap_pids:
        # print(f"dirtymaps - PID: {pid_dirtymap['pid']}")
        pid = pid_dirtymap['pid']
        # print(f"-- 最新dirtymap条目数: {len(pid_dirtymap['latest_dirtymap'])}")
        latest_heatmap = convert_dirtymap_to_heatmap(pid_dirtymap['latest_dirtymap'])
        
        # 使用old dirtymap计算最新heatmap的热度变化
        old_dirtymap = pid_dirtymap['old_dirtymap']
        if old_dirtymap is not None:
            dirtymap_old_file = os.path.join(dirty_map_path, f'old-{pid}.dirtymap')
            write_dirty_map_to_file(old_dirtymap, dirtymap_old_file)    # 保存新的old dirtymap
            latest_heatmap = merge_sub_heat(old_dirtymap, latest_heatmap)
        
        # 将脏页的转储情况更新到heatmap中
        selected = pid_dirtymap['transfered']
        if selected is not None:
            merge_update_selected(latest_heatmap, selected)
        
        # 保存更新的heatmap
        heatmap_latest_file = os.path.join(dirty_map_path, f'latest-{pid}.heatmap')
        write_heatmap_to_file(latest_heatmap, heatmap_latest_file)


def restore(container_path, tty, netdump):
    global rst_time
    old_cwd = os.getcwd()
    os.chdir(container_path)
    #The following command is the restore command, which resotres execution of the container at destination
    if os.path.exists(container_path+'/console.sock'):
        cmd = 'runc restore --console-socket ' + container_path + '/console.sock -d '
    else:
        cmd = 'runc restore'
    cmd += ' --image-path migrate/image'
    cmd += ' --work-path migrate/r_log'
    if tty:
        cmd += ' --shell-job'
    if netdump:
        cmd += ' --tcp-established'
    #In case of a post-copy phase in the migration technique, the restore command restores the process without filling out the entire memory contents.
    #When the --lazy-pages option is used, restore registers the lazy virtual memory areas (VMAs) with the userfaultfd mechanism. The lazy pages are completely handled by dedicated lazy-pages daemon.
    #The daemon receives userfault file descriptors from restore via UNIX socket.
    cmd += ' ' + container
    # print("Running " +  cmd)
    start = time.perf_counter() * 1000
    p = subprocess.Popen(cmd, shell=True)
    ret = p.wait()
    end = time.perf_counter() * 1000
    print("%s finished after %.3fms with %d" % (cmd, end - start, ret))
    rst_time += end - start
    os.chdir(old_cwd)
    

#create the pre-dump, which is done in case of pre-copy and hybrid migrations.
#pre-dump contains the entire content of the container virtual memory
#pre-dump is stored in the parent directory
def pre_dump(mig_base, container, i, dirtymap):
    global chk_time
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    cmd = 'runc checkpoint --pre-dump --work-path pd_log_{} --image-path parent_{}'.format(i, i)
    cmd += ' ' + container
    if dirtymap:
        cmd += ' --use-dirty-map --dirty-map-dir dirty_map'
    if i > 0:
        cmd += ' --parent-path ../parent_{}'.format(i-1)
    # print(cmd)
    start = time.perf_counter() * 1000
    ret = os.system(cmd)
    end = time.perf_counter() * 1000
    print ("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    chk_time += end - start
    os.chdir(old_cwd)
    if ret != 0:
        error()

#create the dump. This is done for any migration technique. Content of the dump varies depending on the technique.
#dump is stored in the image directory.
#in case of pre-dump present, specify it is in the parent directory.
#When post-copy phase is not present, wait until dump command ends (with p.wait())
#If instead post-copy phase is present, the dump procedure does not write memory pages in image and starts the page server for later transfer of faulted pages.
#the page server will then read local memory dump and send memory pages upon request of the lazy-pages daemon running on the destination.
#The page server listens on port 27.
#Still in case of the post-copy phase, with the --status-fd option, CRIU writes '\0' to the specified pipe when it has finished with the checkpoint and start of the page server

#Read https://criu.org/CLI/opt/--lazy-pages and https://criu.org/CLI/opt/--status-fd for more information.
def real_dump(mig_base, precopy, postcopy, tty, netdump, last_iter, dirtymap, replay):
    global chk_time    
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    
    #cmd = 'runc checkpoint --image-path image --leave-running'
    cmd = 'runc checkpoint --image-path image --work-path d_log'

    if tty:
        cmd += ' --shell-job'
    if netdump:
        cmd += ' --tcp-established'
    if precopy:
        cmd += ' --parent-path ../parent_{}'.format(last_iter)
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
        cmd += ' --lazy-pages'
        cmd += ' --page-server localhost:27'
        read_fd, write_fd = os.pipe()
        fdflags = fcntl.fcntl(write_fd, fcntl.F_GETFD)
        fcntl.fcntl(write_fd, fcntl.F_SETFD, fdflags & ~fcntl.FD_CLOEXEC)
        cmd += ' --status-fd ' + str(write_fd)
    if dirtymap:
        cmd += ' --use-dirty-map --dirty-map-dir dirty_map'
    if replay:
        cmd += ' --leave-running'

    cmd += ' ' + container
    start = time.perf_counter() * 1000
    print(cmd)
    if postcopy:
        p = subprocess.Popen(cmd, pass_fds=(write_fd,), shell=True)
        ret = os.read(read_fd, 1)
        if ret == b'\0':
            print('Ready for lazy page transfer')
            os.close(read_fd)
            os.close(write_fd)
        ret = 0
    else:
        p = subprocess.Popen(cmd, shell=True)
        ret = p.wait()

    end = time.perf_counter() * 1000
    print("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    os.chdir(old_cwd)
    if ret != 0:
        error()

def iterate_predump(mig_base, parent_path, max_iter, dirtymap):
    last_iter = 0
    while last_iter < max_iter:
        if dirtymap:
            # 获取container进程树
            get_runc_container_pidtree(container)
            # 执行一次dirty-track
            execute_dirty_track(device_fd, True)

            # 收集此次迭代生成的 dirtymap 文件
            current_dirtymaps = []
            for filename in os.listdir(dirtymap_path):
                if not filename.endswith('.dirtymap'):
                    continue
                if filename.startswith('old-'):
                    continue
                if filename not in processed_files:
                    current_dirtymaps.append(filename)
                    processed_files.add(filename)
            
            # 将当前迭代生成的dirtymap文件添加到迭代列表中
            if current_dirtymaps:
                iter_dirtymaps.append(current_dirtymaps)

            # 更新dirty-map
            generate_heatmap(dirtymap_path)
        last_path = parent_path[last_iter]
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

        dir_size = convert_byte(getdirsize(parent_path[last_iter], 'pages'))
        print('the total size of {} with pattern {} is {}{}'\
                .format(last_path, 'pages', dir_size[0], dir_size[1]))
        # if getdirsize(last_path, 'pages') < max_xfer_size:
        #     break
        if last_iter > 0:
            less_last_path = parent_path[last_iter - 1]
            if precopy_limit or abs(getdirsize(last_path, 'pages') \
                    - getdirsize(less_last_path, 'pages')) < 102400:     #100KB
                break
        last_iter += 1
        if last_iter >= max_iter:
            last_iter = max_iter - 1
            break
    return last_iter

def migrate(container, pre, post, replay, tty, netdump, rootfs, max_iter, dirtymap, time_constraint):
    global rst_time
    base_path = runc_base + container
    mig_base = base_path + "/migrate"
    image_path = mig_base + "/image"
    # parent_path = base_path + "/parent"
    parent_path = []
    work_path = []
    global dirtymap_path
    dirtymap_path = mig_base + "/dirty_map"

    if pre:
        for i in range(0, max_iter):
            pathname = mig_base + "/parent_{}".format(i)
            parent_path.append(pathname)
            pathname = mig_base + "/pd_log_{}".format(i)
            work_path.append(pathname)

    prepare(mig_base, image_path, parent_path, work_path)

    # # 测量初始带宽和最大传输值
    # global mea_bandwidth, max_xfer_size
    # mea_bandwidth = measure_bandwidth(dest)
    # max_xfer_size = mea_bandwidth * time_constraint / 1000
    # # print(f"current bandwidth is {mea_bandwidth}")


    # 打开dirty-track设备
    if dirtymap:
        try:
            global device_fd
            device_fd = open(DEVICE_PATH, 'wb')
        except FileNotFoundError:
            print(f"设备文件{DEVICE_PATH}不存在。请先加载dirty-track内核模块。")
            sys.exit(1)
        
        # 迁移开始前配置dirty-map目录
        set_dirty_map_path(device_fd, dirtymap_path)


    # socket.setdefaulttimeout(6)
    # cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # #Connect to the migration server running on the destination to send the commands
    # cs.connect((dest, 18863))

    # input = [cs,sys.stdin]

    # if pre:
    #     prepare_cmd = json.dumps({
    #         "prepare": {
    #             "path": mig_base,
    #             "image_path": image_path,
    #             "parent_path": parent_path  # parent_path为列表
    #         }
    #     })
    # else:
    #     prepare_cmd = json.dumps({
    #         "prepare": {
    #             "path": mig_base,
    #             "image_path": image_path
    #             # 不包含 parent_path
    #         }
    #     })

    # cs.send(bytes(prepare_cmd, encoding='utf-8'))
    # inputready, outputready, exceptready = select.select(input, [], [], 4)
    # #If after 4 seconds there is something to read(e.g., error msg from the socket), then print it and exit
    # if inputready:
    #     for s in inputready:
    #         answer = s.recv(1024)
    #         print(answer)
    #         error()

    # if rootfs:
    #     search_cmd = 'runc list | grep ' + container
    #     container_exist = subprocess.getstatusoutput(search_cmd)
    #     #(0, 'redis-test   7289        running     /runc/containers/redis-test   2024-04-21T07:13:10.98300754Z   root')
        
    #     #if the container is already running on the source, then we can transfer the rootfs
    #     #if the container is not running, then the script will exit
    #     if container_exist[0]:
    #         error()

        # init_xfer_cmd = 'rsync -aqz --delete --timeout=100 {0}/ root@{1}:{0}/'.format(rootfs_path, dest)
        # start = time.perf_counter() * 1000
        # ret = os.system(init_xfer_cmd)
        # end = time.perf_counter() * 1000
        # print("initial ROOTFS transfer time %.3f ms" % (end - start))
        # if ret != 0:
        #     error()
        
        # #infinite sync loop
        # f = open("logs/rootfs_sync_progress.logs", 'w')
        # sync_cmd = './sync_rootfs.sh ' + dest + ' ' + rootfs_path
        # p = subprocess.Popen(sync_cmd, shell=True, stdout=f, stderr=f)
    
    if pre:
        if diskless:
            for i in range(0, max_iter):
                mount_cmd = 'mount -t tmpfs none '+ parent_path[i]
                ret = os.system(mount_cmd)
                if ret != 0:   
                    error()

        # iter pre-dump
        last_iter = iterate_predump(mig_base, parent_path, max_iter, dirtymap)
            # diskless_pre_dump(base_path, container, dest)
            # xfer_pre_dump(parent_path, dest, base_path)
        # else:
            # pre_dump(base_path, container)
            # xfer_pre_dump(parent_path, dest, base_path)
    else:
        last_iter = 0

    if dirtymap:
        get_runc_container_pidtree(container)
        execute_dirty_track(device_fd, True)
        
        # 收集此次迭代生成的 dirtymap 文件
        current_dirtymaps = []
        for filename in os.listdir(dirtymap_path):
            if not filename.endswith('.dirtymap'):
                continue
            if filename.startswith('old-'):
                continue
            if filename not in processed_files:
                current_dirtymaps.append(filename)
                processed_files.add(filename)
        
        # 将当前迭代生成的dirtymap文件添加到迭代列表中
        if current_dirtymaps:
            iter_dirtymaps.append(current_dirtymaps)
                
        generate_heatmap(dirtymap_path)
    if diskless:
        mount_cmd = 'mount -t tmpfs none '+ image_path
        ret = os.system(mount_cmd)
        if ret != 0:   
            error()
    # print(dirtymap)
    real_dump(mig_base, pre, post, tty, netdump, last_iter, dirtymap, replay)
    # if replay:
    #     ret = transfer_vip()
    #     if ret == 0:
    #         ret = notify_transfer_vip(cs)
    #     # 确认VIP漂移后再恢复
    #     if ret == 0:
    #         # todo: 创建转发路由
            
    #         # 最后传输容器剩余状态
    #         xfer_final(image_path, dest, mig_base)
    #         dir_size = convert_byte(getdirsize(image_path))
    #         print('the total size of {} is {}{}'.format(image_path, dir_size[0], dir_size[1]))

    #         #send the restore command
    #         restore_cmd = '{ "restore" : { "path" : "' + base_path + '", "name" : "' + container + '" , "image_path" : "' + image_path 
    #         restore_cmd += '" , "lazy" : "' + str(post) + '" , "shell-job" : "' + str(tty) + '" , "tcp-established" : "' + str(netdump) + '" , "pre" : "' + str(pre) + '" } }'
    #         cs.send(bytes(restore_cmd, encoding='utf-8'))

    #         while True:
    #             #select.select calls the Unix select() system call
    #             #the first three arguments are three waitable objects (a read list, a write list, and an exception list). The fourth argument is a timeout
    #             #After the timeout, select() returns the triple of lists of objects that are ready (subset of the three arguments)... or empty if not ready
    #             inputready, outputready, exceptready = select.select(input, [], [], 5)

    #             #If after 5 seconds there is nothing to read, then exit
    #             if not inputready:
    #                 break

    #             #If there is something in input to read (e.g., from the socket), then print it
    #             for s in inputready:
    #                 answer = s.recv(1024).decode("utf-8")
    #                 print(answer)
    #                 answer_list = answer.split()
    #                 rst_time = float(answer_list[-2])
    #     else:
    #         print("can't confirm VIP has been transfered, can't restore on destination")
    restore(base_path, tty, netdump)
    #after migration, rootfs sync process and opened files will be closed
    # if rootfs:
    #     p.terminate()
    #     f.close()
    
    if dirtymap:
        device_fd.close()

    return True

def post_process(max_iter):
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    for i in range(0, max_iter):
        umount_cmd = 'umount ' + mig_base + '/parent_{}'.format(i)
        try:
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
        except:
            pass
    
    try:
        umount_cmd = 'umount ' + mig_base + '/image'
        subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
    except:
        pass
    os.chdir(old_cwd)

def touch(fname):
    open(fname, 'a').close()

parser = argparse.ArgumentParser(description='manual to migration script for source node')
parser.add_argument('container', help="container's name(identical to bundle name)")
parser.add_argument('-pre', '--pre-copy', dest='pre', action='store_true', help="enable per-copy migration")
parser.add_argument('-post', '--post-copy', dest='post', action='store_true', help="enable post-copy migration")
parser.add_argument('-d', '--disk-less', dest='diskless', action='store_true', help="enable disk-less migration(page-server, only effect pre-copy)")
parser.add_argument('-t', '--tcp-established', dest='netdump', action='store_true', help="dump and restore the established connection")
parser.add_argument('-s', '--shell-job', dest='tty', action='store_true', help="dump and restore the tty device(opened shell job)")
parser.add_argument('--no-rootfs', dest='norootfs', action='store_true', help="avoid the synchronization of rootfs")
parser.add_argument('-i','--iter', type=int, help='Max iterations of pre-dump')
parser.add_argument('-dm', '--use-dirty-map', dest='dirtymap', action='store_true', help="use dirty-map to reduce the size of memory dump")
parser.add_argument('-tc', '--time-constraint', type=float, default=1000.0, help="max tranfer time constraint(ms)")
parser.add_argument('--replay', dest='replay', action='store_true', help="enable post packets replay")
args = parser.parse_args()

if __name__ == '__main__':

    runc_base = "/runc/containers/"
    
    pre = False
    post = False
    diskless = False
    tty = False
    netdump = False
    replay = False
    rootfs = True
    dirtymap = False
    if args.time_constraint:
        time_constraint = args.time_constraint
    else:
        time_constraint = 2000

    if args.iter and not args.pre:
        parser.error("Pre-copy is required when max_iter is provided.")
    
    if args.replay and args.post:
        parser.error("Post-copy conflicted with replay.")

    if args.pre and not args.iter:
        max_iter = 5
    else:
        max_iter = args.iter

    
    #The name of the container is the first argument
    #NOTE: for the way the code is currently written, it must be the same as the name of the OCI bundle
    container = args.container
    #destination IP is the second argument
    #the Pre and Lazy flags, which are used to determine the migration techniques as follows:
    #Cold = False False
    #Pre-copy = True False
    #Post-copy = False True
    #Hybrid = True True
    pre = args.pre
    post = args.post
    replay = args.replay
    dirtymap = args.dirtymap

    #use CRIU's page server to directly transfer memory dump
    diskless = args.diskless
    if diskless and not (pre or post):
        parser.error("Diskless only supported to used in pre/post-copy")

    #enable CRIU's --shell-job and --tcp-established flag to dump tty device and socket
    tty = args.tty
    netdump = args.netdump

    #rootfs_sync flag, which is used to enable synchronization of container's rootfs
    if args.norootfs:
        rootfs = False

    base_path = runc_base + container
    mig_base = base_path + "/migrate"
    # image_path = base_path + "/image"
    # parent_path = base_path + "/parent"
    # dirtymap_path = base_path + "/dirty_map"

    #-h outputs numbers in human readable format
    #-a enables archive mode, which preserves permissions, ownership, and modification times, among other things
    #-z enables compression during transfer
    #-P reserves files which are not completely transferred to speed-up the following re-transferring
    rsync_opts = "-haz"

    # 开始热迁移
    migrate(container, pre, post, replay, tty, netdump, rootfs, 
                    max_iter, dirtymap, time_constraint)

    if diskless:
        print('total checkpoint and transfer time is {:.3f}ms'.format(chk_time))
    else:
        print('total checkpoint time is {:.3f}ms'.format(chk_time))

    print('total restore time is {:.3f}ms'.format(rst_time))
    mig_time = chk_time + rst_time
    print('total migration time is {:.3f}ms'.format(mig_time))

    # 迁移完成后，执行后处理
    if diskless:
        post_process(max_iter)
