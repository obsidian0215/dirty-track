import os
import sys
import fcntl
import psutil
import struct
import time

# 定义字符设备路径
DEVICE_PATH = '/dev/dirty-track'

# 定义ioctl命令
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

MAJOR_NUM = 114

def _IO(type, nr):
    return (IOC_NONE | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOR(type, nr, size):
    return (IOC_OUT | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOW(type, nr, size):
    return (IOC_IN | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

def _IOWR(type, nr, size):
    return (IOC_IO | (size << IOC_SIZESHIFT) | (type << IOC_TYPESHIFT) | (nr << IOC_NRSHIFT))

IOCTL_SET_DIRTY_MAP_PATH = _IOW(MAJOR_NUM, 1, 256)
IOCTL_START_PID = _IOWR(MAJOR_NUM, 2, 4)
IOCTL_STOP_PID = _IOWR(MAJOR_NUM, 3, 4)
IOCTL_STOP_ALL = _IO(MAJOR_NUM, 4)
IOCTL_GET_DIRTY_MAP_PATH = _IOR(MAJOR_NUM, 5, 256)

# 定义容器进程树的 pid列表
container_pids = []

def ioctl_set_dirty_map_path(device_fd, path):
    """通过ioctl设置脏页跟踪的目录路径"""
    # 路径字符串打包为定长字节数组
    buf = struct.pack(f'{len(path)}s', path.encode('utf-8'))
    # 调用 ioctl 传递路径给内核模块
    fcntl.ioctl(device_fd, IOCTL_SET_DIRTY_MAP_PATH, buf)

def ioctl_start_pid(device_fd, pid):
    """通过ioctl启动指定进程的脏页跟踪"""
    # pid_t 在 Python 中可以用 struct.pack 来打包
    buf = bytearray(struct.pack('I', pid))
    fcntl.ioctl(device_fd, IOCTL_START_PID, buf)
    ret = struct.unpack_from('I', buf)[0]

def ioctl_stop_pid(device_fd, pid):
    """通过ioctl停止指定进程的脏页跟踪"""
    buf = bytearray(struct.pack('I', pid))
    fcntl.ioctl(device_fd, IOCTL_STOP_PID, buf)
    ret = struct.unpack_from('I', buf)[0]

def ioctl_stop_all(device_fd):
    """通过ioctl停止所有进程的脏页跟踪"""
    fcntl.ioctl(device_fd, IOCTL_STOP_ALL, None)

def ioctl_get_dirty_map_path(device_fd):
    """通过ioctl获取脏页跟踪的目录路径"""
    buf = bytearray(struct.pack('256s', b'\0' * 256))
    fcntl.ioctl(device_fd, IOCTL_GET_DIRTY_MAP_PATH, buf)
    # 解包路径字符串
    path = struct.unpack_from(f'{len(buf)}s', buf)[0]
    return path

def get_runc_container_pidtree(container_name):
    """获取runc容器进程树的PID"""
    container_pid_path = f'/run/runc/{container_name}/state.json'
    if not os.path.exists(container_pid_path):
        raise FileNotFoundError(f"runc容器{container_name}的状态文件不存在：{container_pid_path}")

    import json
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


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description="控制runc容器的脏页跟踪")
    parser.add_argument('action', choices=['start', 'stop', 'set_path' , 'stop_all'], help="操作类型：启动或停止")
    parser.add_argument('container', nargs='?', help="runc 容器的名称")
    parser.add_argument('--path', help="设置脏页跟踪的目录路径(optional)", required=False, default="./dirty_map")

    args = parser.parse_args()

    if args.action not in ['stop_all', 'set_path']:
        if not args.container:
            print("Error: 'start' and 'stop' actions require a container name.")
            sys.exit(1)
        else:
            get_runc_container_pidtree(args.container)

    with open(DEVICE_PATH, 'wb') as device_fd:
        # 设置脏页跟踪的目录路径
        if args.action == 'set_path':
            global tmpfs_path
            if not args.path:
                args.path = "./dirty_map"

            if not os.path.exists(args.path):
                if args.path == "./dirty_map":
                    os.mkdir(args.path)
                    tmpfs_path = os.path.abspath(args.path)
                    mount_cmd = 'mount -t tmpfs none '+ tmpfs_path
                    ret = os.system(mount_cmd)
                    if ret != 0:   
                        raise SystemError(f"无法将{tmpfs_path}装载到tmpfs")
                else:
                    raise FileNotFoundError(f"找不到设置脏页追踪的目录路径：{args.path}")
            else:
                tmpfs_path = os.path.abspath(args.path)
                mount_cmd = 'mount -t tmpfs none '+ tmpfs_path
                ret = os.system(mount_cmd)
            print(f"设置脏页跟踪的目录路径为: {tmpfs_path}")
            ioctl_set_dirty_map_path(device_fd.fileno(), tmpfs_path)
        else:
            path = ioctl_get_dirty_map_path(device_fd.fileno())
            # print(path)
            path = path.decode('utf-8').rstrip('\0')
            if not path:
                print("Error: 脏页跟踪的目录路径未设置")
                sys.exit(1)
            # 根据用户指定的操作进行跟踪控制
            elif args.action == 'stop_all':
                ioctl_stop_all(device_fd.fileno())
            elif args.action == 'start':
                print("dirty-map path: {0}".format(path))
                time.sleep(2)
                # 启动指定容器的脏页跟踪
                for pid in container_pids:
                    # ioctl_start_pid(device_fd.fileno(), pid)
                    buffer = bytearray(struct.pack('i', pid))
                    # print(type(buffer))
                    fcntl.ioctl(device_fd.fileno(), IOCTL_START_PID, buffer)
                    ret = struct.unpack_from('i', buffer)[0]
                    # print("start ret = {0}".format(ret))
                    while ret != 0:
                        print("start ret = {0}".format(ret))
                        time.sleep(1)
                        ret = struct.unpack('I', buffer)[0]
                    print(f"启动对容器 {args.container} (PID: {pid}) 的脏页跟踪")
                    # time.sleep(5)
            elif args.action == 'stop':
                # 停止指定容器的页面跟踪
                for pid in container_pids:
                    # ioctl_stop_pid(device_fd.fileno(), pid)
                    buffer = bytearray(4)
                    struct.pack_into('i', buffer, 0, pid)
                    fcntl.ioctl(device_fd.fileno(), IOCTL_STOP_PID, buffer)
                    ret = struct.unpack_from('I', buffer)[0]
                    while ret != 0:
                        print("stop ret = {0}".format(ret))
                        time.sleep(1)
                        ret = struct.unpack('I', buffer)[0]

                    print(f"停止对容器 {args.container} (PID: {pid}) 的脏页跟踪")
            else:
                print(f"无效的操作: {args.action}")
                raise ValueError("对容器的action取值仅接受：start/stop")

