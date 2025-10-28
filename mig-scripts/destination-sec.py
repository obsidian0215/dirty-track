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
import psutil
import logging
from collections import deque
import threading
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor, Future

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

# 确保线程安全
process_lock = threading.Lock()
VIP = "192.168.37.150"
rst_time = 0.0
vip_transfer_complete = False  # 标记VIP转移是否完成

PRIORITY_RE = re.compile(r'(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})', re.S)
KEEPALIVED_CONF = '/etc/keepalived/keepalived.conf'
KEEPALIVED_BAK  = '/etc/keepalived/keepalived.conf.bak'

def set_keepalived_priority(new_priority, config_path=KEEPALIVED_CONF, backup_path=KEEPALIVED_BAK):
    # 备份
    shutil.copy(config_path, backup_path)
    with open(config_path, 'r') as f:
        cfg = f.read()
    new_cfg, cnt = re.subn(PRIORITY_RE, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", cfg)
    if cnt == 0:
        raise RuntimeError("未找到 vrrp_instance VI_1 的 priority 配置段")
    with open(config_path, 'w') as f:
        f.write(new_cfg)
    res = subprocess.run(['sudo', 'systemctl', 'reload', 'keepalived'],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode != 0:
        # 回滚
        shutil.copy(backup_path, config_path)
        subprocess.run(['sudo', 'systemctl', 'reload', 'keepalived'])
        raise RuntimeError(f"reload keepalived 失败: {res.stderr}")
# 在 handle_restore(msg) 末尾，return reply 之前加：
def _restore_target_priority_later():
    try:
        # 给个缓冲时间，等源端先恢复到 100
        time.sleep(4.0)   # 可按需调整 1~5 秒
        set_keepalived_priority(50)  # 恢复到 50（或原值）
        print(f"目标端优先级已恢复到 50")
    except Exception as e:
        print(f"恢复目标端优先级失败: {e}")


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
    logger.info("开始处理prepare请求")
    prep_start = time.perf_counter()
    cpu_prep_start = psutil.cpu_percent(interval=None)

    path = prepare_info['path']
    image_path = prepare_info['image_path']

    parent_paths = prepare_info.get('parent_path', [])
    compress = prepare_info.get('compress', 0)

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
    print("port_list:",port_list)
    #input()
    path_exist = os.path.exists(path)
    if not path_exist and not os.path.exists(os.path.dirname(path)):
        reply = 'Cannot find corresponding container bundle'
        logger.error(reply)
    else:
        prepare(path, image_path, parent_paths)

        # 不再通过 nc 启动监听；改为源端通过 scp/rsync 将归档文件直接传输到这些目录，
        # 目标端将通过控制消息通知并在收到归档后执行解包。
        # prepare() 已经创建了需要的目录结构。
        # transfer_processes 保留以便兼容旧逻辑的清理；但此处不创建任何进程。

        prep_end = time.perf_counter()
        cpu_prep_end = psutil.cpu_percent(interval=None)
        prep_elapsed = (prep_end - prep_start) * 1000
        prep_cpu = cpu_prep_end - cpu_prep_start
        logger.info(f"handle_prepare completed in {prep_elapsed:.3f} ms, CPU usage change: {prep_cpu:.2f}%")

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
        # 获取runc_args，如果不存在则为空字符串
        runc_args_str = msg['restore'].get('runc_args', '')
    except Exception as e:
        print(f"Error parsing restore parameters: {e}")
        lazy = False
        runc_args_str = ''

    old_cwd = os.getcwd()
    os.chdir(msg['restore']['path'])
 #   input()
    # 构建恢复命令
    cmd = 'time -p runc restore --console-socket ' + msg['restore']['path']
    cmd += '/console.sock -d  --image-path ' + msg['restore']['image_path']
    cmd += ' --work-path ' + msg['restore']['path'] + "/migrate/r_log"

    # 添加runc_args参数
    if runc_args_str:
        cmd += ' ' + runc_args_str

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
        time.sleep(0.07)  # 等待0.07秒，可根据需要调整时间

    # 现在启动 runc restore 命令
    logger.info("Running restore command...")
    start_time = time.perf_counter()
    cpu_start = psutil.cpu_percent(interval=None)
    p = subprocess.Popen(cmd, shell=True)
    ret = p.wait()
    end_time = time.perf_counter()
    cpu_end = psutil.cpu_percent(interval=None)
    cpu_usage = cpu_end - cpu_start
    elapsed_ms = (end_time - start_time) * 1000
    logger.info(".3f")

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
def _wait_file_stable(path, timeout=10.0, interval=0.1):
    import os, time
    end = time.time() + timeout
    last = None
    while time.time() < end:
        if os.path.exists(path):
            sz = os.path.getsize(path)
            if last is not None and sz == last:
                return True
            last = sz
        time.sleep(interval)
    return False
def _wait_all_transfers_done(timeout=30.0, interval=0.1):
    end = time.time() + timeout
    while time.time() < end:
        with process_lock:
            alive = [p for p in transfer_processes.values() if p and p.poll() is None]
        if not alive:
            return True
        time.sleep(interval)
    return False

def handle_archive_ready(info):
    """
    处理源端发送的 archive_ready 控制消息：立即返回 ACK（RECEIVED），
    并在后台解包归档文件，完成后在目标目录写入处理标记文件 (.{archive}.processed)
    以供源端通过轮询确认处理结果。
    返回立即 ACK 字符串 'RECEIVED' 或错误信息。
    """
    try:
        path = info.get('path')
        archive = info.get('archive')
        compress_level = info.get('compress', 0)

        if not path or not archive:
            return 'Error: invalid archive_ready payload'

        full_archive = os.path.join(path, archive)

        # 确保目标路径存在
        os.makedirs(path, exist_ok=True)

        def _background_extract():
            try:
                # 等待文件写入稳定（最长300秒）
                if not _wait_file_stable(full_archive, timeout=300.0):
                    # 写入失败则写入错误标记
                    err_marker = os.path.join(path, f'.{archive}.processed.err')
                    with open(err_marker, 'w') as f:
                        f.write('timeout or not found')
                    return

                # 根据文件后缀选择解包命令
                if archive.endswith('.tar.gz') or archive.endswith('.tgz'):
                    cmd = f"tar -xzf {full_archive} -C {path}"
                elif archive.endswith('.tar.lzo') or archive.endswith('.lzo'):
                    lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")
                    cmd = f"{lzo_gpu_path} -d {full_archive} - | tar -xf - -C {path}"
                else:
                    cmd = f"tar -xf {full_archive} -C {path}"

                proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                if proc.returncode != 0:
                    err_marker = os.path.join(path, f'.{archive}.processed.err')
                    with open(err_marker, 'w') as f:
                        f.write(proc.stderr or 'extract failed')
                    return

                # 移除归档以节省空间（可选）
                try:
                    os.remove(full_archive)
                except Exception:
                    pass

                # 写入成功标记
                ok_marker = os.path.join(path, f'.{archive}.processed')
                with open(ok_marker, 'w') as f:
                    f.write('OK')

            except Exception:
                try:
                    err_marker = os.path.join(path, f'.{archive}.processed.err')
                    with open(err_marker, 'w') as f:
                        f.write('exception during extract')
                except Exception:
                    pass

        t = threading.Thread(target=_background_extract, daemon=True)
        t.start()

        # 立即 ACK，源端应该改为轮询目标上的 .{archive}.processed 文件以确认完成
        return 'RECEIVED'

    except Exception as e:
        return f'Error: {e}'
def handle_restore(msg):
    """
    处理 restore 命令，由于使用同步传输，传输在迁移过程中已完成，直接执行恢复操作。
    """
    # 检查是否启用了TCP连接迁移，若启用且VIP未迁移则主动迁移
    runc_args_str = msg['restore'].get('runc_args', '')
    needs_vip_transfer = '--tcp-established' in runc_args_str
    # print(1211111)
    image_path = msg['restore']['image_path']
    desc = os.path.join(image_path, "descriptors.json")
    # 2) 等待 descriptors.json 存在且大小稳定
    if not _wait_file_stable(desc, timeout=15.0):
        logger.error("descriptors.json not ready at %s", desc)
        return "descriptors.json not ready"

    if needs_vip_transfer:
        global vip_transfer_complete
        if not vip_transfer_complete:
            # logger.info("执行VIP转移")
            ret_code = transfer_vip()
            vip_transfer_complete = (ret_code == 0)
            if ret_code == 0:
                logger.debug("VIP迁移完成")
            else:
                logger.error("VIP迁移失败")

    # logger.info("开始执行恢复操作")
    reply = perform_restore(msg)

    # 异步启动进程清理任务，让主线程快速响应
    def _cleanup_worker():
        global transfer_processes, process_lock
        terminated_count = 0
        with process_lock:
            for port, process in list(transfer_processes.items()):
                if process and process.poll() is None:  # 进程仍在运行
                    try:
                        logger.debug(f"终止仍在运行的nc进程 (端口 {port}, PID {process.pid})")
                        process.terminate()

                        # 等待进程优雅退出，最多等待3秒
                        try:
                            process.wait(timeout=3.0)
                            logger.debug(f"进程 {process.pid} 已退出")
                        except subprocess.TimeoutExpired:
                            logger.warning(f"进程 {process.pid} 未退出，强制杀死")
                            process.kill()
                            process.wait()
                            logger.info(f"进程 {process.pid} 已被强制杀死")

                        terminated_count += 1
                    except Exception as e:
                        logger.error(f"清理进程 {process.pid} 时出错: {e}")

        # 清空进程字典
        transfer_processes.clear()

        if terminated_count > 0:
            logger.debug(f"共清理了 {terminated_count} 个nc进程")
        else:
            logger.debug("没有需要清理的nc进程")

    # 使用线程池异步执行清理
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cleanup")
    executor.submit(_cleanup_worker)
    executor.shutdown(wait=False)
    logger.debug("已启动异步进程清理任务")

    threading.Thread(target=_restore_target_priority_later, daemon=True).start()
    return reply

def migrate_server():
    HOST = ''   # Symbolic name meaning all available interfaces
    PORT = 18863

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')

    #Bind socket to local host and port
    try:
        s.bind((HOST, PORT))
    except socket.error as exc:
        print(f'Bind failed: {exc}')
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
            # print("data:",data)
            if not data:
                break  # 连接断开时退出循环
            # 解码数据
            decoded_data = data.decode('utf-8').strip()
            if decoded_data.lower() == 'exit':
                break
            print("received: ",decoded_data)

            if data == 'exit':
                break

            try:
                #Parse JSON string into Python dictionary
                msg = json.loads(decoded_data)
                # print("clientthread msg:",msg)
                #print("msg keys:", list(msg.keys()), repr(list(msg.keys())[0]))
                # old_cwd = os.getcwd()

                match msg:
                    case {'transfer_vip':_}:
                        # 检查并设置VIP转移完成状态
                        global vip_transfer_complete
                        if vip_transfer_complete:
                            logger.debug("VIP已迁移，通知source")
                            reply = 'OK'
                        else:
                            ret = transfer_vip()
                            vip_transfer_complete = (ret == 0)
                            if ret == 0:
                                logger.debug("VIP完成迁移，通知source")
                                reply = 'OK'
                            else:
                                reply = 'Error'


                    case {'prepare': prepare_info}:
                        reply = handle_prepare(prepare_info)
                    case {'archive_ready': info}:
                        # 源端已通过 scp/rsync 上传了归档文件，处理并解包
                        reply = handle_archive_ready(info)
                    case {'restore':_}:
                        # 如果所有传输已完成，立即执行恢复
                        # 所有传输指last_iter及之前的传输，和最大端口对应的传输
                        # time.sleep(1)
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
