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
from collections import deque

VIP = "192.168.2.100"

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

def prepare(base_path, image_path, parent_path):
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
        print(f"已备份原始Keepalived配置文件到 {backup_path}")
        
        # 读取原始配置文件内容
        with open(config_path, 'r') as f:
            config = f.read()
        
        # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
        pattern = r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})'
        
        # 定义替换函数，将 priority 设置为较低的值（例如：50）
        def repl(match):
            original_priority = match.group(2)
            new_priority = '100'  # 设置新的优先级
            print(f"将 VIP 的优先级从 {original_priority} 提高到 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"
        
        # 使用正则表达式替换 priority
        new_config, count = re.subn(pattern, repl, config, flags=re.DOTALL)
        
        if count == 0:
            print("未能找到 vrrp_instance VI_1 中的 priority 配置。请检查配置文件格式。")
            sys.exit(1)
        
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


import re

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

def get_error_page_transfer_time(lp_log_file):
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




# 页面大小（通常为4KB）
page_size = 4096
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
        #Sending message to connected client

        #infinite loop so that function does not terminate and thread does not end.
        while True:

            reply = ""
            #Receiving from client
            data = conn.recv(1024)
            #print(data)
            if not data:
                break
            if data == 'exit':
                break

            try:
                #Parse JSON string into Python dictionary
                msg = json.loads(data)
                print(msg)
                
                old_cwd = os.getcwd()

                match msg:
                    case {'transfer_vip':_}:
                        ret = transfer_vip()
                        if ret == 0:
                            reply = 'OK'
                        else:
                            reply = 'Error'

                    case {'pageserver':_}:
                        #os.system('criu -V')
                        postcopy = 1
                        mount_cmd = 'mount -t tmpfs none ' + msg['pageserver']['path']
                        umount_cmd = 'umount ' + msg['pageserver']['path']

                        if msg['pageserver']['iter']:
                            i = msg['pageserver']['iter']
                        
                        print("start page server")
                        os.system(mount_cmd)

                        cmd = 'criu page-server --images-dir ' + msg['pageserver']['path']
                        if not i is None:
                            cmd += ' --port 27 --auto-dedup -v4 -o ' + msg['pageserver']['path'] + '../logs/ps_{}.log'.format(i)
                        else:
                            cmd += ' --port 27 --auto-dedup -v4 -o ' + msg['pageserver']['path'] + '../logs/ps.log'
                        print ("Running page server for pre-copy: " + cmd)
                        ps = subprocess.Popen(cmd, shell=True)
                        exitcode = ps.poll()
                        print(exitcode)
                        if exitcode is not None:
                            reply = 'remote criu page-server failed'
                        else:
                            continue
                
                    case {'prepare': prepare_info}:
                        path = prepare_info['path']
                        image_path = prepare_info['image_path']

                        if 'parent_path' in prepare_info:
                            parent_paths = prepare_info['parent_path']  # parent_path为列表
                        else:
                            parent_paths = []

                        path_exist = os.path.exists(path)
                        if not path_exist and not os.path.exists(path + '/..'):
                            reply = 'cannot find corresponding container bundle'
                        else:
                            prepare(path, image_path, parent_paths)
                            # parent_path为None时，仅准备image_path
                            continue

                    case {'restore':_}:
                        os.system('criu -V')

                        try:
                            lazy = bool(distutils.util.strtobool(msg['restore']['lazy']))
                            tty = bool(distutils.util.strtobool(msg['restore']['shell-job']))
                            netdump = bool(distutils.util.strtobool(msg['restore']['tcp-established']))
                        except:
                            lazy = False

                        old_cwd = os.getcwd()
                        os.chdir(msg['restore']['path'])

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
                        print("Restore command: " + cmd)

                        # 如果启用了懒恢复，先启动 lazy-pages 守护进程
                        if lazy:
                            lazy_cmd = "criu lazy-pages --page-server --address " + addr
                            lazy_cmd += " --port 27 -v4 -D "
                            lazy_cmd += msg['restore']['image_path']
                            lazy_cmd += " -W " + msg['restore']['path'] + "/migrate/r_log"
                            lazy_cmd += " -o " + msg['restore']['path'] + "/migrate/r_log/lp.log"
                            print("Running lazy-pages server: " + lazy_cmd)
                            # 启动 lazy-pages 守护进程
                            lp = subprocess.Popen(lazy_cmd, shell=True)
                            # 为了确保 lazy-pages.socket 已经创建，等待片刻
                            time.sleep(1)  # 等待 1 秒，您可以根据需要调整时间

                        # 现在启动 runc restore 命令
                        print("Running restore command...")
                        start = time.perf_counter() * 1000
                        p = subprocess.Popen(cmd, shell=True)
                        ret = p.wait()
                        end = time.perf_counter() * 1000

                        if lazy:
                            # 等待 lazy-pages 守护进程结束
                            lp.wait()

                        if ret == 0:
                            print(123)
                            if lazy:
                                print(456)
                                lp_log_file = msg['restore']['path'] + "/migrate/r_log/lp.log"
                                restore_log_file =msg['restore']['path'] + "/migrate/r_log/restore.log" 
                                total_uffd_copy = calculate_uffd_copy(lp_log_file)
                                error_transfer_time = get_error_page_transfer_time(lp_log_file)
                                # 将 total_uffd_copy 从字节转换为 KB，保留两位小数
                                total_uffd_copy_kb = total_uffd_copy / 1024.0
                          
                                reply = "runc restored %s successfully with %.3f ms, total_uffd_copy: %.2f KB, error_transfer_time: %.2f ms" % (
    msg['restore']['name'], end - start, total_uffd_copy_kb, error_transfer_time)
                            else:
                                reply = "runc restored %s successfully with %.3f ms" % (msg['restore']['name'], end - start)
                        else:
                            reply = "runc failed(%d)" % ret
                        os.chdir(old_cwd)

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
