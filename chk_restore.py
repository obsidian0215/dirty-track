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
    time.sleep(0.3)  # 根据实际情况调整等待时间

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
    


def restore(container_path, tty, netdump, post):
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
    if post:
        cmd += ' --lazy-pages'
    #In case of a post-copy phase in the migration technique, the restore command restores the process without filling out the entire memory contents.
    #When the --lazy-pages option is used, restore registers the lazy virtual memory areas (VMAs) with the userfaultfd mechanism. The lazy pages are completely handled by dedicated lazy-pages daemon.
    #The daemon receives userfault file descriptors from restore via UNIX socket.
    cmd += ' ' + container
    # print("Running " +  cmd)
    start = time.perf_counter() * 1000
    p = subprocess.Popen(cmd, shell=True)
    if post:
        lazy_cmd = "criu lazy-pages --page-server --address 127.0.0.1"
        lazy_cmd += " --port 27 -v4 -D "
        lazy_cmd += base_path + "/migrate/image"
        lazy_cmd += " -W " + base_path + "/migrate/r_log"
        lazy_cmd += " -o " + base_path + "/migrate/logs/lp.log"
        print ("Running lazy-pages server: " + lazy_cmd)
        lp = subprocess.Popen(lazy_cmd, shell=True)
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
        cmd += ' --use-dirty-map --dirty-map-dir ' + dirtymap_path
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

def iterate_predump(mig_base, parent_path, max_iter, dirtymap):
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

        dir_size = convert_byte(getdirsize(parent_path[last_iter], 'pages'))
        print('the total size of {} with pattern {} is {}{}'\
                .format(last_path, 'pages', dir_size[0], dir_size[1]))
        # if getdirsize(last_path, 'pages') < max_xfer_size:
        #     break
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

    if dirtymap and not pre:
        get_runc_container_pidtree(container)
        start_dirty_track(device_fd)

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
    restore(base_path, tty, netdump, post)
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
