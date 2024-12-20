#!/usr/bin/env python
#code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
import socket
import sys
from _thread import *
import json
import os
import shutil
import distutils.util
import time
import subprocess
import re
import iptc
import logging
from collections import deque
import threading
from typing import List, Dict

compress = False
restore_info = None

# 设置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to track port list
INIT_PORT = 12345
iteration_list: List[int] = []
port_list: List[int] = [INIT_PORT]
transfer_processes: Dict[int, subprocess.Popen] = {}
last_iter = 0

# Lock 以确保线程安全
process_lock = threading.Lock()


VIP = "192.168.2.100"

rst_time = 0.0

def handle_pre_xfer_complete(msg):
    """
    处理 pre_xfer_complete 命令，等待指定迭代及之前的传输完成。
    """
    global last_iter, iteration_list
    try:
        last_iter = msg["pre_xfer_complete"]
    except Exception as e:
        print("error;",e)
    logger.info(f"收到 pre_xfer_complete，等待迭代 {last_iter} 及之前的传输完成")
    # 等待指定迭代及之前的传输完成

    for iter_num in iteration_list:
        last_iter = int(last_iter)
        #print("iter_num:",iter_num)
        #print("last_iter:",last_iter)
        if iter_num < last_iter:
            port = INIT_PORT  + iter_num  #  -1
            with process_lock:
                process = transfer_processes.get(port)
            if process:
                logger.info(f"等待端口 {port} 的传输完成")
                process.wait()  # 阻塞直到进程完成
                logger.info(f"端口 {port} 的传输已完成")
                with process_lock:
                    del transfer_processes[port]
        else:
            # 对于大于last_iter的进程，终止它们
            # 最后一次迭代不包含在iteration_list，不会被终止
            port = INIT_PORT + iter_num - 1
            with process_lock:
                process = transfer_processes.get(port)
            if process:
                logger.info(f"终止端口 {port} 的传输进程{process}")
                #print_transfer_processes()
                process.terminate()  # 终止该进程
                process.wait()  # 等待进程终止
                with process_lock:
                    del transfer_processes[port]
                logger.info(f"端口 {port} 的传输进程已终止")

    logger.info(f"迭代 {last_iter} 及之前的传输均已完成，并且其他进程已关闭")

def prepare(base_path, image_path, parent_path):
    # parent_path为None时，仅准备image_path
    if os.path.exists(base_path):
        try:
            umount_cmd = 'umount ' + image_path
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
            shutil.rmtree(image_path)
            shutil.rmtree(base_path + '/r_log')
            # shutil.rmtree(base_path + '/lp_log')
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
        except:
            pass
    else:
        os.mkdir(base_path)
    if parent_path:
        for i in parent_path:
            os.mkdir(i)
    os.mkdir(image_path)
    os.mkdir(base_path + '/r_log')
    # os.mkdir(base_path + '/lp_log')

def handle_prepare(prepare_info):
    global compress, iteration_list, port_list
    print("port_list:",port_list)

    path = prepare_info['path']
    image_path = prepare_info['image_path']

    parent_paths = prepare_info.get('parent_path', [])
    compress = prepare_info.get('compress', False)

    # 初始化监听端口列表和迭代列表
    for parent in parent_paths:
        iter_suffix = parent.split('_')[-1]
        try:
            iter_num = int(iter_suffix)
        except ValueError:
            logger.error(f"无法解析迭代号，从 parent_path 中提取的迭代号为 {iter_suffix}")
            continue
        iteration_list.append(iter_num)
        port = INIT_PORT + iter_num
        port_list.append(port)
        #print(port_list)
    #input()
    path_exist = os.path.exists(path)
    if not path_exist and not os.path.exists(os.path.dirname(path)):
        reply = 'Cannot find corresponding container bundle'
        logger.error(reply)
    else:
        prepare(path, image_path, parent_paths)

        # 根据端口和迭代列表，启动ncat进程监听
        print(port_list)
        for parent, iter_num, port in zip(parent_paths, iteration_list, port_list):
            # 定义解压路径
            extract_path = parent
            # 启动 ncat 监听并解压的管道命令
            # 命令: nc -l {port} | tar -xzf - -C {extract_path}
            if compress:
                cmd = f"nc -lp {port} | tar -xzf - -C {extract_path}"
            else:
                cmd = f"nc -lp {port} | tar -xf - -C {extract_path}"
            logger.info(f"启动 ncat 监听端口 {port}，解压到 {extract_path}")
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            #print("process id:",process)
            # 将进程记录到字典中
            with process_lock:
                transfer_processes[port] = process

        # 最后一个端口用于解压到 image_path
        if port_list:
            last_port = port_list[-1]
            # os.makedirs(image_path, exist_ok=True)
            extract_path = image_path
            if compress:
                cmd = f"nc -lp {last_port} | tar -xzf - -C {extract_path}"
            else:
                cmd = f"nc -lp {last_port} | tar -xf - -C {extract_path}"
                #cmd = f"nc -lp {last_port} "
                #cmd1 = f"tar -xf {extract_path}.tar -C {extract_path}"
            logger.info(f"启动 ncat 监听端口 {last_port}，解压到 {extract_path}")
            process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            with process_lock:
                transfer_processes[last_port] = process
        #print_transfer_processes()
        reply = 'OK'

    return reply

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
        # print(f"已备份原始Keepalived配置文件到 {backup_path}")

        # 读取原始配置文件内容
        with open(config_path, 'r') as f:
            config = f.read()

        # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
        pattern = r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})'

        # 定义替换函数，将 priority 设置为较低的值（例如：50）
        def repl(match):
            original_priority = match.group(2)
            new_priority = '100'  # 设置新的优先级
            # print(f"将 VIP 的优先级从 {original_priority} 提高到 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"

        # 使用正则表达式替换 priority
        new_config, count = re.subn(pattern, repl, config, flags=re.DOTALL)

        if count == 0:
            print("未能找到 vrrp_instance VI_1 中的 priority 配置。请检查配置文件格式。")
            sys.exit(1)

        # 将修改后的配置写回配置文件
        with open(config_path, 'w') as f:
            f.write(new_config)
        # print(f"已更新 Keepalived 配置文件 {config_path}，降低 VIP 优先级。")

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

def calculate_uffd_copy(lp_log_file):
    """
    计算总的 UFFD 复制字节数。

    参数:
        lp_log_file (str): lp.log 文件的路径。

    返回:
        int: 总的 UFFD 复制字节数。
    """
    uffd_copy_pattern = re.compile(r'uffd_copy:\s+0x[0-9a-fA-F]+/(\d+)')
    total_uffd_copy = 0
    with open(lp_log_file, 'r') as f:
        for line in f:
            match = uffd_copy_pattern.search(line)
            if match:
                size = int(match.group(1))
                total_uffd_copy += size
                #print(f"UFFD copy: {size} bytes")
    return total_uffd_copy

def parse_stats_restore(stats_restore_path):
    """
    解析 stat-dump 文件并累加时间值到全局变量。

    :param stat_dump_path: stat-dump 文件的路径
    :param log_type: 日志类型，'pre_dump' 或 'dump'
    """
    global rst_time

    try:
        # 执行 'crit decode' 命令并获取输出
        result = subprocess.run(
            ['crit', 'show', stats_restore_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )

        # 解析 JSON 输出
        stat_data = json.loads(result.stdout)
        entries = stat_data.get('entries', [])

        for entry in entries:
            dump_info = entry.get('restore', {})
            # 提取所有 *_time 字段并累加(us)
            total_time = 0.0
            for key, value in dump_info.items():
                if key.endswith('_time'):
                    try:
                        total_time += float(value)
                    except ValueError:
                        (f"无法将{key}的值转换为浮点数: {value}")

            rst_time = total_time / 1000;
    except subprocess.CalledProcessError as e:
        print(f"执行 crit decode 时出错: {e.stderr}")
    except json.JSONDecodeError as e:
        print(f"解析 JSON 时出错: {e}")
    except Exception as e:
        print(f"处理 stats-restore 文件时发生未知错误: {e}")

def get_restore_time(work_path):
    """
    在work_path中查找stats-restore文件并解析

    :param work_path: 包含stats-restore文件的工作路径
    """
    stats_restore_file = os.path.join(work_path, 'stats-restore')
    if os.path.isfile(stats_restore_file):
        parse_stats_restore(stats_restore_file)
    else:
        print(f"未找到stats-restore文件: {stats_restore_file}")

def get_rpf_handle_time(lp_log_file):
    """
    计算错误页面传输的总时间，从 lp.log 中匹配 'Connecting to server' 到 'page-xfer: Disconnect from the page server' 的时间间隔。

    参数:
        lp_log_file (str): lp.log 文件的路径。

    返回:
        float: 错误页面传输的总持续时间（秒）。
    """
    connect_pattern = re.compile(r'\(([\d\.]+)\)\s+Connecting to server\s+[\d\.]+:\d+')
    disconnect_pattern = re.compile(r'\(([\d\.]+)\)\s+page-xfer:\s+Disconnect from the page server')

    transfer_durations = []
    connect_time = None

    with open(lp_log_file, 'r') as f:
        for line in f:
            # 匹配错误页面传输开始
            connect_match = connect_pattern.search(line)
            if connect_match:
                connect_time = float(connect_match.group(1))
                #print(f"Error Page Transfer Started at: {connect_time} seconds")
                continue

            # 匹配错误页面传输完成
            disconnect_match = disconnect_pattern.search(line)
            if disconnect_match and connect_time is not None:
                disconnect_time = float(disconnect_match.group(1))
                duration = disconnect_time - connect_time
                transfer_durations.append(duration)
                #print(f"Error Page Transfer Finished at: {disconnect_time} seconds, Duration: {duration} seconds")
                # 重置开始时间以便处理下一个传输
                connect_time = None

    total_error_transfer_time = sum(transfer_durations)
    return total_error_transfer_time*1000

def perform_restore(msg):
    try:
        lazy = bool(distutils.util.strtobool(msg['restore']['lazy']))
        tty = bool(distutils.util.strtobool(msg['restore']['shell-job']))
        netdump = bool(distutils.util.strtobool(msg['restore']['tcp-established']))
    except:
        lazy = False

    old_cwd = os.getcwd()
    os.chdir(msg['restore']['path'])
 #   input()
    # 构建恢复命令
    cmd = 'time -p runc restore --console-socket ' + msg['restore']['path']
    cmd += '/console.sock -d  --image-path ' + msg['restore']['image_path']
    cmd += ' --work-path ' + msg['restore']['path'] + "/migrate/r_log"
    if tty:
        cmd += ' --shell-job'
    if netdump:
        cmd += ' --tcp-established'
    if lazy:
        cmd += ' --lazy-pages'
    cmd += ' ' + msg['restore']['name']
    # print("Restore command: " + cmd)

    # 若启用post-copy，则先启动lazy-pages守护进程
    if lazy:
        lazy_cmd = "criu lazy-pages --page-server --address " + str(source_ip)
        lazy_cmd += " --port 27 -v4 -D "
        lazy_cmd += msg['restore']['image_path']
        lazy_cmd += " -W " + msg['restore']['path'] + "/migrate/r_log"
        lazy_cmd += " -o " + msg['restore']['path'] + "/migrate/r_log/lp.log"
        print("Running lazy-pages server: " + lazy_cmd)
        # 启动 lazy-pages 守护进程
        lp = subprocess.Popen(lazy_cmd, shell=True)
        # 为了确保 lazy-pages.socket 已经创建，等待片刻
        time.sleep(0.1)  # 等待0.1秒，可根据需要调整时间

    # 现在启动 runc restore 命令
    # print("Running restore command...")
    # start = time.perf_counter() * 1000
    p = subprocess.Popen(cmd, shell=True)
    ret = p.wait()
    # end = time.perf_counter() * 1000

    if lazy:
        # 等待 lazy-pages 守护进程结束
        lp.wait()

    if ret == 0:
        global rst_time
        restore_log_path =msg['restore']['path'] + "/migrate/r_log"
        get_restore_time(restore_log_path)
        # print(123)
        if lazy:
            # print(456)
            lp_log_file = msg['restore']['path'] + "/migrate/r_log/lp.log"

            total_uffd_copy = calculate_uffd_copy(lp_log_file)
            rpf_handle_time = get_rpf_handle_time(lp_log_file)
            # 将 total_uffd_copy 从字节转换为 KB，保留两位小数
            total_uffd_copy_kb = total_uffd_copy / 1024.0

            reply = "runc restored %s successfully with %.3f ms, total_uffd_copy: %.2f KB, rpf_handle_time: %.2f ms" % (
msg['restore']['name'], rst_time, total_uffd_copy_kb, rpf_handle_time)
        else:
            reply = "runc restored %s successfully with %.3f ms" % (msg['restore']['name'], rst_time)
    else:
        reply = "runc failed(%d)" % ret

    os.chdir(old_cwd)
    return reply

def print_transfer_processes():
    if not transfer_processes:
        print("transfer_processes 字典为空。")
    else:
        print("当前 transfer_processes 内容:")
        for port, process in transfer_processes.items():
            status = '运行中' if process.poll() is None else f'已结束 (退出码: {process.returncode})'
            print(f"  端口: {port}, PID: {process.pid}, 状态: {status}, 命令: {process.args}")

def handle_restore(msg):
    """
    处理 restore 命令，持续等待最后一个迭代传输和指定及其之前迭代传输都完成后再执行恢复操作。
    """
    global last_iter, iteration_list
    os.system('criu -V')  # 检查 CRIU 版本
    logger.info("收到 restore 指令")

    # 持续等待最后一个迭代传输及指定迭代及之前的传输完成
    while True:
        print_transfer_processes()
        with process_lock:
            all_transfers_complete = True
            # 检查所有传输进程是否已完成
            for iter_num in iteration_list:
                if iter_num <= last_iter:
                    port = INIT_PORT + 1 + iter_num
                    process = transfer_processes.get(port)
                    # if process and process.poll() is None:  # 如果进程尚未完成
                    #     all_transfers_complete = False
                    #     break
                    if process:
                        status = process.poll()
                        print(f"端口 {port} 对应的进程状态: {'运行中' if status is None else '已结束'}")
                    else:
                        print(f"端口 {port} 没有对应的传输进程")
                    if process and process.poll() is None:  # 如果进程尚未完成
                        print(f"发现端口 {port} 的传输进程仍在运行，设置 all_transfers_complete = False")
                        all_transfers_complete = False
                        break
            # 检查最后一个传输进程是否已完成
            if transfer_processes:
                last_port = port_list[-1]
                print(f"开始检查最后一个传输进程的端口号: {last_port}")
                print_transfer_processes()
                #time.sleep(2)
                last_process = transfer_processes.get(last_port)
                #print("last_process:",last_process)
                if last_process:
                    last_status = last_process.poll()
                    print(f"最后一个端口 {last_port} 对应的进程状态: {'运行中' if last_status is None else '已结束'}")
                else:
                    print(f"最后一个端口 {last_port} 没有对应的传输进程")
                   # input()
                if last_process and last_process.poll() is None:  # 如果最后一个传输进程尚未完成
                    print(f"发现最后一个端口 {last_port} 的传输进程仍在运行，设置 all_transfers_complete = False")
                    all_transfers_complete = False
            else:
                print("transfer_processes 字典为空，跳过最后一个传输进程的检查")
        if all_transfers_complete:
            logger.info("所有指定迭代和最后一个迭代的传输已完成，开始执行恢复操作")
            reply = perform_restore(msg)
            break
        else:
            logger.info("等待所有传输完成后再执行恢复操作")
            # 休眠一段时间后再次检查
            time.sleep(2)

    restore_info = None
    return reply

def migrate_server():
    HOST = ''   # Symbolic name meaning all available interfaces
    PORT = 18863

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')

    #Bind socket to local host and port
    try:
        s.bind((HOST, PORT))
    except socket.error as msg:
        print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()

    print('Socket bind complete')

    #Start listening on socket
    s.listen(10)
    print('Socket now listening')

    #Function for handling connections. This will be used to create threads
    def clientthread(conn, addr):
        global compress, iteration_list, last_iter
        #Sending message to connected client

        #infinite loop so that function does not terminate and thread does not end.
        while True:

            reply = ""
            #Receiving from client
            data = conn.recv(1024)
            #print(data)
            if not data:
                print(111)
                break
            # 解码数据
            decoded_data = data.decode('utf-8').strip()
            if decoded_data.lower() == 'exit':
                break
            print(decoded_data)


            if data == 'exit':
                break

            try:
                #Parse JSON string into Python dictionary
                msg = json.loads(decoded_data)
                print("clientthread msg:",msg)
                #print("msg keys:", list(msg.keys()), repr(list(msg.keys())[0]))

                old_cwd = os.getcwd()

                match msg:
                    case {'transfer_vip':_}:
                        # print(1111)
                        ret = transfer_vip()
                        # print(2222)
                        if ret == 0:
                            reply = 'OK'
                        else:
                            reply = 'Error'

                    case {'pre_xfer_complete':_}:
                        # 只需等待该次及之前迭代以及最后一次迭代的传输完成
                        # 中间的所有ncat线程全部可以退出，不会被用于传输
                        #print("============handle_pre_xfer_complete=============")
                        handle_pre_xfer_complete(msg)

                    case {'prepare': prepare_info}:
                        reply = handle_prepare(prepare_info)
                    case {'restore':_}:
                        # 如果所有传输已完成，立即执行恢复
                        # 所有传输指last_iter及之前的传输，和最大端口对应的传输
                        reply = handle_restore(msg)

                    case _:
                        print("Unknown request: " + msg)
                        reply = 'unknown request'
            except:
                continue

            print(reply)
            conn.sendall(bytes(reply, encoding='utf-8'))

        #came out of loop
        conn.close()

    #now keep talking with the client
    while 1:
        #wait to accept a connection - blocking call
        conn, addr = s.accept()
        print('Connected with ' + addr[0] + ':' + str(addr[1]))
        global source_ip
        source_ip = addr[0]

        #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
        start_new_thread(clientthread,(conn, str(addr[0]),))

    s.close()

if __name__ == '__main__':
    migrate_server()

# def configure_iptables_forward():
#     """
#     配置iptables规则，缓存并转发请求包至source
#     """
#     table = iptc.Table(iptc.Table.FILTER)
#     table.autocommit = False

#     # PREROUTING链中添加TEE转发规则
#     chain = iptc.Chain(table, "PREROUTING")

#     # 创建一个新的规则
#     rule = iptc.Rule()
#     rule.protocol = "tcp"
#     rule.dst = VIP
#     rule.dport = "80"

#     # 添加 TEE 目标，将流量复制到Source
#     target = iptc.Target(rule, "TEE")
#     target.extra = False
#     rule.target = "TEE"
#     rule.add_target(target)
#     rule.parameters = {"gateway": SOURCE_IP}

#     # 添加 DNAT 规则，将复制的流量目标IP改为Source的实际IP
#     nat_table = iptc.Table(iptc.Table.NAT)
#     nat_table.autocommit = False
#     nat_chain = iptc.Chain(nat_table, "PREROUTING")

#     nat_rule = iptc.Rule()
#     nat_rule.protocol = "tcp"
#     nat_rule.dst = SOURCE_IP
#     nat_rule.dport = "80"
#     nat_rule.target = "DNAT"
#     nat_rule.parameters = {"to_destination": "192.168.1.101:80"}
#     nat_chain.insert_rule(nat_rule)

#     # 允许转发到Source的流量
#     forward_table = iptc.Table(iptc.Table.FORWARD)
#     forward_table.autocommit = False
#     forward_chain = iptc.Chain(forward_table, "FORWARD")

#     forward_rule = iptc.Rule()
#     forward_rule.protocol = "tcp"
#     forward_rule.dst = "192.168.1.101"
#     forward_rule.dport = "80"
#     forward_rule.target = "ACCEPT"
#     forward_chain.insert_rule(forward_rule)

#     # 提交更改
#     table.commit()
#     nat_table.commit()
#     forward_table.commit()

#     print("已配置iptables规则，开始缓存并转发请求包至source。")

# def remove_iptables_forward():
#     """
#     移除iptables转发规则，允许destination直接响应客户端
#     """
#     # 移除 PREROUTING 链中的 TEE 规则
#     table = iptc.Table(iptc.Table.FILTER)
#     table.autocommit = False
#     chain = iptc.Chain(table, "PREROUTING")

#     for rule in chain.rules:
#         if rule.dst == VIP and rule.protocol == "tcp" and rule.dport == "80":
#             for target in rule.targets:
#                 if target.name == "TEE" and target.parameters.get("gateway") == SOURCE_IP:
#                     rule.delete_rule(target)
#                     print("已移除iptables的TEE转发规则。")

#     # 移除 NAT 表中的 DNAT 规则
#     nat_table = iptc.Table(iptc.Table.NAT)
#     nat_table.autocommit = False
#     nat_chain = iptc.Chain(nat_table, "PREROUTING")

#     for rule in nat_chain.rules:
#         if rule.protocol == "tcp" and rule.dst == SOURCE_IP and rule.dport == "80":
#             if rule.target == "DNAT" and rule.parameters.get("to_destination") == "192.168.1.101:80":
#                 nat_chain.delete_rule(rule)
#                 print("已移除iptables的DNAT转发规则。")

#     # 移除 FORWARD 表中的 ACCEPT 规则
#     forward_table = iptc.Table(iptc.Table.FORWARD)
#     forward_table.autocommit = False
#     forward_chain = iptc.Chain(forward_table, "FORWARD")

#     for rule in forward_chain.rules:
#         if rule.protocol == "tcp" and rule.dst == "192.168.1.101" and rule.dport == "80":
#             if rule.target == "ACCEPT":
#                 forward_chain.delete_rule(rule)
#                 print("已移除iptables的FORWARD ACCEPT规则。")

#     # 提交更改
#     table.commit()
#     nat_table.commit()
#     forward_table.commit()

#     print("已移除iptables规则，允许destination直接响应客户端。")
#                     case {'pageserver':_}:
#                         #os.system('criu -V')
#                         postcopy = 1
#                         mount_cmd = 'mount -t tmpfs none ' + msg['pageserver']['path']
#                         umount_cmd = 'umount ' + msg['pageserver']['path']

#                         if msg['pageserver']['iter']:
#                             i = msg['pageserver']['iter']

#                         print("start page server")
#                         os.system(mount_cmd)

#                         cmd = 'criu page-server --images-dir ' + msg['pageserver']['path']
#                         if not i is None:
#                             cmd += ' --port 27 --auto-dedup -v4 -o ' + msg['pageserver']['path'] + '../logs/ps_{}.log'.format(i)
#                         else:
#                             cmd += ' --port 27 --auto-dedup -v4 -o ' + msg['pageserver']['path'] + '../logs/ps.log'
#                         print ("Running page server for pre-copy: " + cmd)
#                         ps = subprocess.Popen(cmd, shell=True)
#                         exitcode = ps.poll()
#                         print(exitcode)
#                         if exitcode is not None:
#                             reply = 'remote criu page-server failed'
#                         else:
#                             continue