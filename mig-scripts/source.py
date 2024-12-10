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
IOCTL_CHECK_PID = _IOWR(DIRTY_TRACK_MAGIC, 4, 5)
IOCTL_GET_DIRTY_MAP_PATH = _IOR(DIRTY_TRACK_MAGIC, 5, 256)

# 定义容器进程树的 pid 列表
container_pids = []

mig_time = 0.0
chk_time = 0.0
rst_time = 0.0
xfer_time = 0.0
# [tang change]定义全局变量用于累计预拷贝时间和大小
pre_dump_time_total = 0.0  # 毫秒
pre_dump_size_total = 0.0  # 字节
pre_dump_transfer_time_total = 0.0  # 毫秒

# 定义全局变量用于记录最后一次 dump 的时间和大小
dump_time_total = 0.0                # 毫秒
dump_size_total = 0.0                # 字节
dump_transfer_time_total = 0.0        # 毫秒

# post
total_uffd_copy = 0.0
error_transfer_time = 0.0

PAGE_SIZE = 4096  # 每页大小为4KB

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

# 启动所有容器进程的脏页跟踪
def start_dirty_track(device_fd):
    for pid in container_pids:
        ioctl_start_pid(device_fd, pid)
        print(f"启动对PID {pid}的脏页跟踪")

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
        with open(file_path, 'rb') as f:
            data = f.read()
            count = len(data) // 8  # sizeof(unsigned long)
            return list(struct.unpack('<' + 'Q' * count, data))
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def read_dirtymap(file_path):
    """
    读取dirtymap文件，返回其中记录的脏页地址
    """
    try:
        with open(file_path, 'rb') as f:
            data = f.read()
            entry_size = 12  # sizeof(unsigned long) + sizeof(unsigned int)
            if len(data) % entry_size != 0:
                print(f"Invalid dirtymap file size: {len(data)} bytes")
                return []
            count = len(data) // entry_size
            addresses = []
            for i in range(count):
                entry = data[i*entry_size:(i+1)*entry_size]
                address, write_count = struct.unpack('<QI', entry)
                addresses.append(address)
            return addresses
    except Exception as e:
        print(f"Error reading dirtymap file {file_path}: {e}")
        return []

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
    遍历container_pids，计算每个pid的脏页列表，合并去重，计算总传输大小
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
            dirty_addresses = read_dirtymap(dirtymap_file)
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

# 流量控制函数
def transfer_vip():
    """
    降低源节点的优先级并触发 VIP 迁移到目标节点。
    """
    try:
        # 定义 Keepalived 配置文件路径和备份路径
        config_path = '/etc/keepalived/keepalived.conf'
        backup_path = '/etc/keepalived/keepalived.conf.bak'
        
        # 备份原始配置文件
        shutil.copy(config_path, backup_path)
        print(f"已备份原始 Keepalived 配置文件到 {backup_path}")
        
        # 读取原始配置文件内容
        with open(config_path, 'r') as f:
            config = f.read()
        
        # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
        pattern = r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})'
        
        # 定义替换函数，将 priority 设置为较低的值（例如：50）
        def repl(match):
            original_priority = match.group(2)
            new_priority = '50'  # 设置新的优先级
            print(f"将 VIP 的优先级从 {original_priority} 降低到 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"
        
        # 使用正则表达式替换 priority
        new_config, count = re.subn(pattern, repl, config, flags=re.DOTALL)
        
        if count == 0:
            print("未能找到 vrrp_instance VI_1 中的 priority 配置。请检查配置文件格式。")
            return 1
        
        # 将修改后的配置写回配置文件
        with open(config_path, 'w') as f:
            f.write(new_config)
        print(f"已更新 Keepalived 配置文件 {config_path}，降低 VIP 优先级。")
        
        # 重新加载 Keepalived 服务以应用更改
        result = subprocess.run(['sudo', 'systemctl', 'reload', 'keepalived'], 
                                stdout=subprocess.PIPE, 
                                stderr=subprocess.PIPE, 
                                text=True)
        
        if result.returncode != 0:
            print(f"重新加载 Keepalived 服务失败：{result.stderr}")
            # 如果重新加载失败，可以选择恢复备份配置
            shutil.copy(backup_path, config_path)
            subprocess.run(['sudo', 'systemctl', 'reload', 'keepalived'])
            print("已恢复原始 Keepalived 配置文件并重新加载服务。")
            return 1
        else:
            print("成功重新加载 Keepalived 服务，VIP 迁移已触发。")
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
    
# 向dest发送提升优先级的通知
def notify_transfer_vip(cs):
    vip_cmd = '{"transfer_vip"}'
    cs.send(bytes(vip_cmd, encoding='utf-8'))
    inputready, outputready, exceptready = select.select(input, [], [], 3)

    if inputready:
        for s in inputready:
            answer = s.recv(1024)
            print(answer)
            if answer == 'OK':
                return 0
            else:
                return 1     
    else:
        print("can't confirm the VIP has been transfered")
        return 1


#create the pre-dump, which is done in case of pre-copy and hybrid migrations.
#pre-dump contains the entire content of the container virtual memory
#pre-dump is stored in the parent directory
def pre_dump(mig_base, container, i, dirtymap):
    global chk_time, pre_dump_time_total, pre_dump_size_total,parent_path   #[change] 添加 pre_dump 的全局变量
    old_cwd = os.getcwd()
    os.chdir(mig_base)
    cmd = 'runc checkpoint --pre-dump --work-path pd_log_{} --image-path parent_{}'.format(i, i)
    cmd += ' ' + container
    if dirtymap:
        cmd += ' --use-dirty-map --dirty-map-dir ' + dirtymap_path
    if i > 0:
        cmd += ' --parent-path ../parent_{}'.format(i-1)
    # print(cmd)
    start = time.perf_counter() * 1000
    ret = os.system(cmd)
    end = time.perf_counter() * 1000
    print ("%s finished after %.3f ms with %d" % (cmd, end - start, ret))
    chk_time += end - start
    pre_dump_time_total += (end - start)# 累计预拷贝时间
    os.chdir(old_cwd)
    if ret != 0:
        error()
 # 计算并记录预拷贝的大小
    # pre_dump_size = convert_byte(getdirsize(parent_path[i], 'pages'))[0] * 1024 * 1024  # 转换为字节
    # pre_dump_size_total += pre_dump_size  # 累计预拷贝大小
    # print('PRE-DUMP size: {}M\t{}'.format(convert_byte(pre_dump_size)[0], parent_path[i]))
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
    global chk_time, dump_time_total, dump_size_total, dump_transfer_time_total
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
        cmd += ' --use-dirty-map --dirty-map-dir ' + dirtymap_path
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

    # 计算并记录 dump 的大小和时间
    dump_time_total = (end-start)
#Transfer the previously created pre-dump using rsync
def xfer_pre_dump(parent_path, dest, base_path, i):
    global xfer_time, pre_dump_transfer_time_total,pre_dump_size_total  # 添加 pre_dump_transfer_time_total
    sys.stdout.write('PRE-DUMP size: ')
    sys.stdout.flush()
    #cmd = 'du -hs %s' % parent_path
    #ret = os.system(cmd)
    cmd_du = ['du', '-hs', parent_path]
    try:
        result_du = subprocess.run(cmd_du, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        pre_dump_size_str = result_du.stdout.strip()  # 例如 "199M\t/path/to/parent_i"
        print(pre_dump_size_str)
        
        # 解析大小（可选）
        size_str, _ = pre_dump_size_str.split('\t')
        size_value, size_unit = size_str[:-1], size_str[-1]
        size_multiplier = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
        pre_dump_size = float(size_value) * size_multiplier.get(size_unit.upper(), 1)
        pre_dump_size_total += pre_dump_size  # 累计预拷贝大小
    except subprocess.CalledProcessError as e:
        print(f"Error executing du command: {e.stderr}")
        pre_dump_size_str = "0B\t/path/to/parent_i"  # 赋予默认值或根据需要处理
        print(pre_dump_size_str)
        pre_dump_size = 0.0
        pre_dump_size_total += pre_dump_size
        error()

    cmd = 'rsync %s --stats %s %s:%s/' % (rsync_opts, parent_path, dest, base_path)
    print("Transferring PRE-DUMP %d to %s" % (i, dest))
    start = time.perf_counter() * 1000
    ret = os.system(cmd)
    end = time.perf_counter() * 1000
    print("PRE-DUMP %d transfer time %.3f ms" % (i, end - start))
    # 累计传输时间
    pre_dump_transfer_time_total += (end -start)
    xfer_time += end -start
    if ret != 0:
        error()
# 解析大小字符串，转换为以 KB 为单位的浮点数
def parse_size(size_str):
    size_multiplier = {'K': 1, 'M': 1024, 'G': 1024 * 1024}
    unit = size_str[-1].upper()
    if unit in size_multiplier:
        size = float(size_str[:-1]) * size_multiplier[unit]
    else:
        size = float(size_str)  # 默认单位为字节，保持为原始值
        size = size / 1024  # 转换为 KB
    return size

#Transfer the previosuly created dump using rsync
def xfer_final(image_path, dest, base_path):
    global xfer_time,dump_size_total,dump_time_total,dump_transfer_time_total
    sys.stdout.write('DUMP size: ')
    sys.stdout.flush()
    #cmd = 'du -hs %s' % image_path
    #ret = os.system(cmd)
    cmd_du = ['du', '-hs', image_path]
    try:
        result = subprocess.run(cmd_du, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        # 提取大小部分
        dump_size_str = result.stdout.strip().split('\t')[0]  # 例如 "124K"
        dump_size_total = parse_size(dump_size_str)
        print(dump_size_total)  # 继续打印输出
    except subprocess.CalledProcessError as e:
        print(f"Error executing du command: {e.stderr}")
        dump_size_total = "0B\t/path/to/image"  # 赋予默认值或根据需要处理
        error()
    cmd = 'rsync %s --stats %s %s:%s/' % (rsync_opts, image_path, dest, base_path)
    print("Transferring DUMP to %s" % dest)
    start = time.perf_counter() * 1000
    ret = os.system(cmd)
    end = time.perf_counter() * 1000
    print("DUMP transfer time %.3f ms" % (end - start))
    xfer_time += end -start
    dump_transfer_time_total = end-start
    if ret != 0:
        error()

# Run the pre-dump iteration and transfer it to the destination
def iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap):
    last_iter = 0
    if dirtymap:
        # 在pre-copy开启前先启动对容器的dirty-track
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)
    while last_iter < max_iter:
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
        xfer_pre_dump(last_path, dest, mig_base, last_iter)        

        dir_size = convert_byte(getdirsize(parent_path[last_iter], 'pages'))
        print('the total size of {} with pattern {} is {}{}'\
                .format(last_path, 'pages', dir_size[0], dir_size[1]))
        if getdirsize(last_path, 'pages') < max_xfer_size:
            break
        if last_iter > 0:
            less_last_path = parent_path[last_iter - 1]
            if abs(getdirsize(last_path, 'pages') - getdirsize(less_last_path, 'pages')) < 102400 \
                    or (getdirsize(last_path, 'pages') < 102400):     #100KB
                break
        last_iter += 1
        if last_iter >= max_iter:
            last_iter = max_iter - 1
            break
    return last_iter

def migrate(container, dest, pre, post, replay, tty, netdump, rootfs, max_iter, dirtymap, time_constraint):
    global rst_time, dirtymap_path, device_fd
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
        for i in range(0, max_iter):
            pathname = mig_base + "/parent_{}".format(i)
            parent_path.append(pathname)
            pathname = mig_base + "/pd_log_{}".format(i)
            work_path.append(pathname)

    prepare(mig_base, image_path, parent_path, work_path)

    # 测量初始带宽和状态传输最大值(Bytes)
    global mea_bandwidth, max_xfer_size
    mea_bandwidth = measure_bandwidth(dest)
    max_xfer_size = mea_bandwidth * time_constraint
    # print(f"current bandwidth is {mea_bandwidth}")

    # 打开dirty-track设备
    if dirtymap:
        try:
            global device_fd
            global device_file
            device_file = open(DEVICE_PATH, 'wb')
            device_fd = device_file.fileno()
        except FileNotFoundError:
            print(f"设备文件{DEVICE_PATH}不存在。请先加载dirty-track内核模块。")
            sys.exit(1)
        
        # 迁移开始前配置dirty-map目录
        set_dirty_map_path(device_fd, dirtymap_path)


    socket.setdefaulttimeout(6)
    cs = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Connect to the migration server running on the destination to send the commands
    cs.connect((dest, 18863))

    input = [cs,sys.stdin]

    if pre:
        prepare_cmd = json.dumps({
            "prepare": {
                "path": mig_base,
                "image_path": image_path,
                "parent_path": parent_path  # parent_path为列表
            }
        })
    else:
        prepare_cmd = json.dumps({
            "prepare": {
                "path": mig_base,
                "image_path": image_path
                # 不包含 parent_path
            }
        })

    cs.send(bytes(prepare_cmd, encoding='utf-8'))
    inputready, outputready, exceptready = select.select(input, [], [], 4)
    #If after 4 seconds there is something to read(e.g., error msg from the socket), then print it and exit
    if inputready:
        for s in inputready:
            answer = s.recv(1024)
            print(answer)
            error()

    if rootfs:
        search_cmd = 'runc list | grep ' + container
        container_exist = subprocess.getstatusoutput(search_cmd)
        #(0, 'redis-test   7289        running     /runc/containers/redis-test   2024-04-21T07:13:10.98300754Z   root')
        
        #if the container is already running on the source, then we can transfer the rootfs
        #if the container is not running, then the script will exit
        if container_exist[0]:
            error()

        init_xfer_cmd = 'rsync -aqz --delete --timeout=100 {0}/ root@{1}:{0}/'.format(rootfs_path, dest)
        start = time.perf_counter() * 1000
        ret = os.system(init_xfer_cmd)
        end = time.perf_counter() * 1000
        print("initial ROOTFS transfer time %.3f ms" % (end - start))
        if ret != 0:
            error()
        
        #infinite sync loop
        f = open(mig_base + "/d_log/rootfs_sync_progress.logs", 'w')
        sync_cmd = './sync_rootfs.sh ' + dest + ' ' + rootfs_path
        p = subprocess.Popen(sync_cmd, shell=True, stdout=f, stderr=f)
    
    if pre:
        if diskless:
            for i in range(0, max_iter):
                mount_cmd = 'mount -t tmpfs none '+ parent_path[i]
                ret = os.system(mount_cmd)
                if ret != 0:   
                    error()

        # iter pre-dump
        last_iter = iterate_predump(cs, mig_base, parent_path, max_iter, dest, dirtymap)
        #if diskless:
        #   diskless_pre_dump(base_path, container, dest)
        #else:
        #   pre_dump(base_path, container)
        #   xfer_pre_dump(parent_path, dest, base_path)
    else:
        last_iter = 0

    if diskless:
        mount_cmd = 'mount -t tmpfs none '+ image_path
        ret = os.system(mount_cmd)
        if ret != 0:   
            error()

    if dirtymap and not pre:
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)

    # todo: 获取容器尚未传输的内存状态大小，判断是否post-copy
    # 读取timestamp_list.pid文件，获取最新的dirty-map
    # 读取dirty-map中的被跳过温页和热页
    # 读取candidate_list.pid文件维护的候选页
    # 将两者累计并预计最终传输的内存状态大小(*4KB)
    if dirtymap:
        # 计算传输大小
        total_transfer_size = container_may_dump_size(container_pids, dirtymap_path)
        print(f"Container may dump {total_transfer_size} bytes of memory")

        # 步骤6: 与max_xfer_size比较
        if total_transfer_size > 0.8 * max_xfer_size:
            print(f"Exceed max_xfer_size {max_xfer_size}, post-copy is needed")
            if not post:
                print("[Warning]post-copy is not enabled, pre-copy may failed")
        else:
            print(f"We can transfer within one-shot stop&dump")
            if post:
                post = False

    real_dump(mig_base, pre, post, tty, netdump, last_iter, dirtymap, replay)
    ret = transfer_vip()
    if ret == 0:
        ret = notify_transfer_vip(cs)
    # 确认VIP漂移后再恢复
    if ret != 0:
        print("can't confirm VIP has been transfered, can't restore on destination")
        error()
    
    # 传输容器剩余状态
    xfer_final(image_path, dest, mig_base)
    dir_size = convert_byte(getdirsize(image_path))
    print('the total size of {} is {}{}'.format(image_path, dir_size[0], dir_size[1]))

    # if replay:
    #     # todo: 创建转发路由

    # one-shot restore with post-copy
    restore_cmd = '{ "restore" : { "path" : "' + base_path + '", "name" : "' + container + '" , "image_path" : "' + image_path 
    restore_cmd += '" , "lazy" : "' + str(post) + '" , "shell-job" : "' + str(tty) + '" , "tcp-established" : "' + str(netdump) + '" , "pre" : "' + str(pre) + '" } }'
    cs.send(bytes(restore_cmd, encoding='utf-8'))

    # while True:
    #     #select.select calls the Unix select() system call
    #     #the first three arguments are three waitable objects (a read list, a write list, and an exception list). The fourth argument is a timeout
    #     #After the timeout, select() returns the triple of lists of objects that are ready (subset of the three arguments)... or empty if not ready
    #     inputready, outputready, exceptready = select.select(input, [], [], 5)

    #     #If after 5 seconds there is nothing to read, then exit
    #     if not inputready:
    #         break

    #     #If there is something in input to read (e.g., from the socket), then print it
    #     for s in inputready:
    #         answer = s.recv(1024).decode("utf-8")
    #         print("answer is here:",answer)
    #         answer_list = answer.split()
    #         rst_time = float(answer_list[-2])

    # post拷贝返回较慢，需要加大等待时间
    if post:
        inputready, outputready, exceptready = select.select(input, [], [], 200)
    else:
        inputready, outputready, exceptready = select.select(input, [], [], 5)
    #If there is something in input to read (e.g., from the socket), then print it
    global total_uffd_copy,error_transfer_time
    for s in inputready:
        answer = s.recv(1024).decode("utf-8")
        print(answer)
        if "runc restored" in answer:
            # 使用正则表达式提取数据
            pattern = r"runc restored .* successfully with (\d+\.\d+) ms(?:, total_uffd_copy: (\d+\.\d+) KB, error_transfer_time: (\d+\.\d+) ms)?"
            match = re.search(pattern, answer)
            if match:
                rst_time = float(match.group(1))
                print("Restore time: {:.3f} ms".format(rst_time))
                # 检查是否匹配到了 total_uffd_copy 和 error_transfer_time
                if match.group(2) and match.group(3):
                    total_uffd_copy = float(match.group(2))
                    error_transfer_time = float(match.group(3))
                    print("Total uffd copy: {:.2f} KB".format(total_uffd_copy))
                    print("Error transfer time: {:.2f} ms".format(error_transfer_time))
                else:
                    # 如果没有匹配到，说明这是预拷贝的回复
                    total_uffd_copy = None
                    error_transfer_time = None
            else:
                print("Failed to parse reply:", answer)
        else:
            print("Received reply:", answer)
  
    #after migration, rootfs sync process and opened files will be closed
    if rootfs:
        p.terminate()
        f.close()
    
    if dirtymap:
        device_file.close()

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
parser.add_argument('dest', help="IP address of destination")
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

    if args.pre:
        if args.iter:
            max_iter = args.iter
        else:
            max_iter = 5
    else:
        max_iter = 0  # 当未启用预拷贝时，将 max_iter 设为 0


    
    #The name of the container is the first argument
    #NOTE: for the way the code is currently written, it must be the same as the name of the OCI bundle
    container = args.container
    #destination IP is the second argument
    dest = args.dest
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
    migrate(container, dest, pre, post, replay, tty, netdump, rootfs, 
                    max_iter, dirtymap, time_constraint)

    if diskless:
        print('total checkpoint and transfer time is {:.3f}ms'.format(chk_time))
    else:
        print('total checkpoint time is {:.3f}ms'.format(chk_time))
        print('total transfer time is {:.3f}ms'.format(xfer_time))
        chk_time += xfer_time
    print('total restore time is {:.3f}ms'.format(rst_time))
    mig_time = chk_time + rst_time
    print('total migration time is {:.3f}ms'.format(mig_time))


    print("-----------------------for note------------")
# 输出累计的预拷贝时间和大小
    if pre:
        print('Total pre-dump time: {:.0f} ms'.format(pre_dump_time_total))
        print('Total pre-dump transfer time: {:.0f} ms'.format(pre_dump_transfer_time_total))
        

    # 输出 dump 的时间和大小
    print('Total dump time: {:.0f} ms'.format(dump_time_total))
    print('Total dump transfer time: {:.0f} ms'.format(dump_transfer_time_total))

    print('resume time (pre dump can use):{:.0f} ms '.format(rst_time))
    if pre:
        print('Total pre-dump size: {:.2f} MB'.format(pre_dump_size_total / (1024 * 1024)))  # 转换为 MB
    print('Total dump size:', dump_size_total,'KB')  # 直接输出字符串

    if pre and not post:
        total_time = pre_dump_time_total + pre_dump_transfer_time_total + dump_time_total + dump_transfer_time_total+rst_time
    elif not pre and post:
        total_time = dump_time_total + dump_transfer_time_total+rst_time + error_transfer_time
    elif pre and post:
        total_time = pre_dump_time_total + pre_dump_transfer_time_total + dump_time_total + dump_transfer_time_total+rst_time+error_transfer_time


    stop_time = dump_time_total + dump_transfer_time_total+rst_time
    
    print(f"total migrate time: {total_time:.0f} ms",)
    print(f"stop time: {stop_time:.0f} ms" )

    if post:
        print('Faulted pages transfer time（ms）: {:.0f} ms'.format(error_transfer_time))
        print('Faulted pages size(KB): {:.2f} KB'.format(total_uffd_copy))

    #input()
    # 迁移完成后，执行后处理


    # for excel

   # 输出数据行
    output_values = []

    if pre:
        output_values.extend([
            int(round(pre_dump_time_total)),
            int(round(pre_dump_transfer_time_total))
        ])

    output_values.extend([
        int(round(dump_time_total)),
        int(round(dump_transfer_time_total)),
        int(round(rst_time))
    ])

    if pre:
        output_values.append('{:.2f}'.format(pre_dump_size_total / (1024 * 1024)))  # 预拷贝大小仍以 MB 为单位
    else:
        output_values.append('')

    output_values.append('{:.2f}'.format(dump_size_total))  # dump_size_total 已经是以 KB 为单位的浮点数

    output_values.extend([
        int(round(total_time)),
        int(round(stop_time))
    ])

    if post:
        output_values.extend([
            '{:.2f}'.format(total_uffd_copy),
            int(round(error_transfer_time))
        ])

    print('\t'.join(map(str, output_values)))

    if diskless:
        post_process(max_iter)
