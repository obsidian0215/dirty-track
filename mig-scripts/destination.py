#!/usr/bin/env python
# code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
import distutils.util
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from _thread import start_new_thread
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import psutil  # type: ignore[import-not-found]
from script_defaults import get_default_ips

compress = False
restore_info = None

# 设置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

last_iter = 0
rnd = os.path

# Transfer management is handled by TransferManager (defined below)


class TransferSession:
    """Single inbound transfer that streams data into tar extraction."""

    def __init__(
        self,
        desc: str,
        extract_path: str,
        compress: int,
        expected_bytes: Optional[int],
        on_finish: Optional[Callable[["TransferSession"], None]],
    ):
        self.desc = desc
        self.extract_path = extract_path
        self.compress = compress
        self.expected_bytes = expected_bytes
        self.token = uuid.uuid4().hex
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("", 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self.bytes_received = 0
        self.duration_ms = 0.0
        self.error: Optional[str] = None
        self.status: str = "pending"
        self.done = threading.Event()
        self.on_finish = on_finish
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        start = time.perf_counter()
        try:
            conn, addr = self.sock.accept()
            with conn:
                logger.info("transfer %s accepted connection from %s:%s", self.desc, addr[0], addr[1])
                self.bytes_received = self._receive_stream(conn)
                self.status = "OK"
                logger.info(
                    "transfer %s completed: %s bytes", self.desc, self.bytes_received
                )
        except Exception as exc:  # pragma: no cover - defensive
            self.status = "ERROR"
            self.error = str(exc)
            logger.error("transfer %s failed: %s", self.desc, exc)
        finally:
            self.duration_ms = (time.perf_counter() - start) * 1000.0
            try:
                self.sock.close()
            except Exception:
                pass
            self.done.set()
            if self.on_finish:
                try:
                    self.on_finish(self)
                except Exception as cb_exc:  # pragma: no cover - defensive
                    logger.error("transfer %s finalize callback failed: %s", self.desc, cb_exc)

    def _receive_stream(self, conn: socket.socket) -> int:
        total = 0
        if self.compress == 0:
            tar_proc = subprocess.Popen(["tar", "-xf", "-", "-C", self.extract_path], stdin=subprocess.PIPE)
            sink = tar_proc.stdin
            lzo_proc = None
        elif 1 <= self.compress <= 4:
            lzo_gpu_path = os.path.join(os.path.dirname(__file__), "../lzo_gpu/lzo_gpu")
            lzo_proc = subprocess.Popen([lzo_gpu_path, "-d", "-"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            tar_proc = subprocess.Popen(["tar", "-xf", "-", "-C", self.extract_path], stdin=lzo_proc.stdout)
            if lzo_proc.stdout:
                lzo_proc.stdout.close()
            sink = lzo_proc.stdin
        else:
            raise ValueError(f"Unsupported compress level: {self.compress}")

        try:
            while True:
                data = conn.recv(1024 * 1024)
                if not data:
                    break
                if sink:
                    sink.write(data)
                total += len(data)
        finally:
            if sink:
                try:
                    sink.close()
                except Exception:
                    pass

        tar_rc = tar_proc.wait()
        if tar_rc != 0:
            raise RuntimeError(f"tar extraction failed with code {tar_rc}")
        if self.compress >= 1 and lzo_proc is not None:
            lzo_rc = lzo_proc.wait()
            if lzo_rc != 0:
                raise RuntimeError(f"lzo_gpu decompress failed with code {lzo_rc}")

        return total

    def wait(self, timeout: float) -> bool:
        return self.done.wait(timeout)

    def close(self) -> None:
        try:
            self.sock.close()
        except Exception:
            pass
        self.done.set()


class TransferManager:
    def __init__(self) -> None:
        self.sessions: Dict[str, TransferSession] = {}
        self.lock = threading.Lock()
        self.final_token: Optional[str] = None
        self.final_info: Optional[Dict[str, object]] = None
        self.final_event = threading.Event()
        # keep a short-lived cache of recently completed non-final sessions
        # maps token -> (result_dict, completion_time)
        self._completed_sessions: Dict[str, tuple] = {}

    def reset(self) -> None:
        with self.lock:
            for session in self.sessions.values():
                session.close()
            self.sessions.clear()
            self.final_token = None
            self.final_info = None
            self.final_event.clear()
            self._completed_sessions.clear()

    def _finalize_session(self, session: TransferSession) -> None:
        result: Dict[str, object] = {
            "status": session.status,
            "bytes": session.bytes_received,
            "duration_ms": session.duration_ms,
        }
        if session.error:
            result["message"] = session.error

        with self.lock:
            # Remove the session if it is still tracked.
            self.sessions.pop(session.token, None)
            if self.final_token == session.token:
                self.final_info = result
                self.final_event.set()
            else:
                # store recently completed non-final session results for a short period
                try:
                    self._completed_sessions[session.token] = (result, time.time())
                except Exception:
                    pass

    def create_session(
        self,
        desc: str,
        extract_path: str,
        compress: int,
        expected_bytes: Optional[int] = None,
        is_final: bool = False,
    ) -> TransferSession:
        os.makedirs(extract_path, exist_ok=True)
        session = TransferSession(desc, extract_path, compress, expected_bytes, self._finalize_session)
        with self.lock:
            self.sessions[session.token] = session
            if is_final:
                self.final_token = session.token
                self.final_info = None
                self.final_event.clear()
        return session

    def complete_session(self, token: str, timeout: float = 180.0) -> Dict[str, object]:
        with self.lock:
            session = self.sessions.get(token)
            if not session:
                # maybe the session already finished and was moved into the
                # completed-sessions cache by _finalize_session; return that if
                # it's recent enough
                completed = self._completed_sessions.get(token)
                if completed is not None:
                    result, when = completed
                    # TTL: 60 seconds
                    if time.time() - when < 60.0:
                        return result
                    else:
                        # remove stale entry
                        try:
                            self._completed_sessions.pop(token, None)
                        except Exception:
                            pass
                if self.final_token == token and self.final_info is not None:
                    return self.final_info
                return {"status": "ERROR", "message": "unknown transfer token"}

        if not session.wait(timeout):
            session.close()
            with self.lock:
                self.sessions.pop(token, None)
                if self.final_token == token:
                    self.final_info = {"status": "TIMEOUT", "message": "transfer timed out"}
                    self.final_event.set()
            return {"status": "TIMEOUT", "message": "transfer timed out"}

        # Session has already finalized via callback; return stored info if available.
        with self.lock:
            if self.final_token == token and self.final_info is not None:
                return self.final_info

        result = {
            "status": session.status,
            "bytes": session.bytes_received,
            "duration_ms": session.duration_ms,
        }
        if session.error:
            result["message"] = session.error
        return result

    def wait_for_final(self, timeout: float) -> Optional[Dict[str, object]]:
        with self.lock:
            token = self.final_token
            info = self.final_info
        if token is None:
            return {"status": "N/A"}
        if info is not None:
            return info
        if not self.final_event.wait(timeout):
            return None
        with self.lock:
            return self.final_info


transfer_manager = TransferManager()
# VIP 默认从集中配置加载，可由上层脚本通过命令行参数覆盖
_, _, _, VIP = get_default_ips()
rst_time = 0.0
vip_transfer_complete = False  # 标记VIP转移是否完成

PRIORITY_RE = re.compile(r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})", re.S)
KEEPALIVED_CONF = "/etc/keepalived/keepalived.conf"
KEEPALIVED_BAK = "/etc/keepalived/keepalived.conf.bak"


def set_keepalived_priority(new_priority, config_path=KEEPALIVED_CONF, backup_path=KEEPALIVED_BAK):
    # 备份
    shutil.copy(config_path, backup_path)
    with open(config_path, "r") as f:
        cfg = f.read()
    new_cfg, cnt = re.subn(PRIORITY_RE, lambda m: f"{m.group(1)}{new_priority}{m.group(3)}", cfg)
    if cnt == 0:
        raise RuntimeError("未找到 vrrp_instance VI_1 的 priority 配置段")
    with open(config_path, "w") as f:
        f.write(new_cfg)
    res = subprocess.run(
        ["sudo", "systemctl", "reload", "keepalived"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    if res.returncode != 0:
        # 回滚
        shutil.copy(backup_path, config_path)
        subprocess.run(["sudo", "systemctl", "reload", "keepalived"])
        raise RuntimeError(f"reload keepalived 失败: {res.stderr}")


# 在 handle_restore(msg) 末尾，return reply 之前加：
def _restore_target_priority_later():
    try:
        # 给个缓冲时间，等源端先恢复到 100
        time.sleep(4.0)  # 可按需调整 1~5 秒
        set_keepalived_priority(50)  # 恢复到 50（或原值）
        print("目标端优先级已恢复到 50")
    except Exception as e:
        print(f"恢复目标端优先级失败: {e}")


def prepare(base_path, image_path, parent_path):
    # parent_path为None时，仅准备image_path
    if os.path.exists(base_path):
        try:
            umount_cmd = "umount " + image_path
            subprocess.run(umount_cmd, shell=True, stderr=subprocess.DEVNULL)
            shutil.rmtree(image_path)
            shutil.rmtree(base_path + "/r_log")
            # shutil.rmtree(base_path + '/lp_log')
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
        except Exception:
            pass
    else:
        os.mkdir(base_path)
    if parent_path:
        for i in parent_path:
            os.mkdir(i)
    os.mkdir(image_path)
    os.mkdir(base_path + "/r_log")
    # os.mkdir(base_path + '/lp_log')


def handle_prepare(prepare_info):
    logger.info("开始处理prepare请求")
    prep_start = time.perf_counter()
    cpu_prep_start = psutil.cpu_percent(interval=None)

    _reset_transfer_state()

    path = prepare_info["path"]
    image_path = prepare_info["image_path"]

    parent_paths = prepare_info.get("parent_path", [])
    compress = prepare_info.get("compress", 0)

    path_exist = os.path.exists(path)
    if not path_exist and not os.path.exists(os.path.dirname(path)):
        reply = json.dumps({"status": "ERROR", "message": "Cannot find corresponding container bundle"})
        logger.error("Cannot find corresponding container bundle")
    else:
        prepare(path, image_path, parent_paths)

        session_details: List[Dict[str, object]] = []
        for idx, parent in enumerate(parent_paths):
            if not parent:
                continue
            iter_num: Optional[int] = None
            match = re.search(r"(\d+)$", parent)
            if match:
                try:
                    iter_num = int(match.group(1))
                except ValueError:
                    iter_num = None

            session = transfer_manager.create_session(
                desc=f"pre-dump-{iter_num if iter_num is not None else idx}",
                extract_path=parent,
                compress=compress,
            )
            session_details.append(
                {
                    "token": session.token,
                    "port": session.port,
                    "path": parent,
                    "iteration": iter_num if iter_num is not None else idx,
                    "type": "pre_dump",
                }
            )

        final_session = transfer_manager.create_session(
            desc="final-dump",
            extract_path=image_path,
            compress=compress,
            is_final=True,
        )

        prep_end = time.perf_counter()
        cpu_prep_end = psutil.cpu_percent(interval=None)
        prep_elapsed = (prep_end - prep_start) * 1000
        prep_cpu = cpu_prep_end - cpu_prep_start
        logger.info(f"handle_prepare completed in {prep_elapsed:.3f} ms, CPU usage change: {prep_cpu:.2f}%")

        reply_payload = {
            "status": "OK",
            "compress": compress,
            "sessions": {
                "pre_dump": session_details,
                "final": {
                    "token": final_session.token,
                    "port": final_session.port,
                    "path": image_path,
                    "type": "final",
                },
            },
        }

        reply = json.dumps(reply_payload)

    return reply


def handle_transfer_status(request: Dict[str, Any]) -> str:
    token = request.get("token")
    if not token:
        return json.dumps({"status": "ERROR", "message": "missing transfer token"})

    timeout_ms = request.get("timeout_ms")
    timeout_sec = 180.0
    if timeout_ms is not None:
        try:
            timeout_sec = max(0.0, float(timeout_ms) / 1000.0)
        except (TypeError, ValueError):
            return json.dumps({"status": "ERROR", "message": "invalid timeout_ms"})

    result = transfer_manager.complete_session(str(token), timeout=timeout_sec)
    return json.dumps(result)


def transfer_vip():
    """
    降低源节点的优先级并触发 VIP 迁移到目标节点。
    """
    try:
        # 定义 Keepalived 配置文件路径和备份路径
        config_path = "/etc/keepalived/keepalived.conf"
        backup_path = "/etc/keepalived/keepalived.conf.bak"

        # 备份原始配置文件
        shutil.copy(config_path, backup_path)
        # print(f"已备份原始Keepalived配置文件到 {backup_path}")

        # 读取原始配置文件内容
        with open(config_path, "r") as f:
            config = f.read()

        # 定义正则表达式模式，匹配 vrrp_instance VI_1 块中的 priority
        pattern = r"(vrrp_instance\s+VI_1\s*\{[^}]*?priority\s+)(\d+)([^}]*?\})"

        # 定义替换函数，将 priority 设置为较低的值（例如：50）
        def repl(match):
            match.group(2)
            new_priority = "100"  # 设置新的优先级
            # print(f"将 VIP 的优先级从 {original_priority} 提高到 {new_priority}")
            return f"{match.group(1)}{new_priority}{match.group(3)}"

        # 使用正则表达式替换 priority
        new_config, count = re.subn(pattern, repl, config, flags=re.DOTALL)

        if count == 0:
            print("未能找到 vrrp_instance VI_1 中的 priority 配置。请检查配置文件格式。")
            sys.exit(1)

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


def calculate_uffd_copy(lp_log_file):
    """
    计算总的 UFFD 复制字节数。

    参数:
        lp_log_file (str): lp.log 文件的路径。

    返回:
        int: 总的 UFFD 复制字节数。
    """
    uffd_copy_pattern = re.compile(r"uffd_copy:\s+0x[0-9a-fA-F]+/(\d+)")
    total_uffd_copy = 0
    with open(lp_log_file, "r") as f:
        for line in f:
            match = uffd_copy_pattern.search(line)
            if match:
                size = int(match.group(1))
                total_uffd_copy += size
                # print(f"UFFD copy: {size} bytes")
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
            ["crit", "show", stats_restore_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
        )

        # 解析 JSON 输出
        stat_data = json.loads(result.stdout)
        entries = stat_data.get("entries", [])

        for entry in entries:
            dump_info = entry.get("restore", {})
            # 提取所有 *_time 字段并累加(us)
            total_time = 0.0
            for key, value in dump_info.items():
                if key.endswith("_time"):
                    try:
                        total_time += float(value)
                    except ValueError:
                        (f"无法将{key}的值转换为浮点数: {value}")

            rst_time = total_time / 1000
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
    stats_restore_file = os.path.join(work_path, "stats-restore")
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
    connect_pattern = re.compile(r"\(([\d\.]+)\)\s+Connecting to server\s+[\d\.]+:\d+")
    disconnect_pattern = re.compile(r"\(([\d\.]+)\)\s+page-xfer:\s+Disconnect from the page server")

    transfer_durations = []
    connect_time = None

    with open(lp_log_file, "r") as f:
        for line in f:
            # 匹配错误页面传输开始
            connect_match = connect_pattern.search(line)
            if connect_match:
                connect_time = float(connect_match.group(1))
                # print(f"Error Page Transfer Started at: {connect_time} seconds")
                continue

            # 匹配错误页面传输完成
            disconnect_match = disconnect_pattern.search(line)
            if disconnect_match and connect_time is not None:
                disconnect_time = float(disconnect_match.group(1))
                duration = disconnect_time - connect_time
                transfer_durations.append(duration)
                # print(f"Error Page Transfer Finished at: {disconnect_time} seconds, Duration: {duration} seconds")
                # 重置开始时间以便处理下一个传输
                connect_time = None

    total_error_transfer_time = sum(transfer_durations)
    return total_error_transfer_time * 1000


def perform_restore(msg):
    try:
        lazy = bool(distutils.util.strtobool(msg["restore"]["lazy"]))
        # 获取runc_args，如果不存在则为空字符串
        runc_args_str = msg["restore"].get("runc_args", "")
    except Exception as e:
        print(f"Error parsing restore parameters: {e}")
        lazy = False
        runc_args_str = ""

    old_cwd = os.getcwd()
    os.chdir(msg["restore"]["path"])
    #   input()
    # 构建恢复命令
    cmd = "time -p runc restore --console-socket " + msg["restore"]["path"]
    cmd += "/console.sock -d  --image-path " + msg["restore"]["image_path"]
    cmd += " --work-path " + msg["restore"]["path"] + "/migrate/r_log"

    # 添加runc_args参数
    if runc_args_str:
        cmd += " " + runc_args_str

    if lazy:
        cmd += " --lazy-pages"
    cmd += " " + msg["restore"]["name"]
    # print("Restore command: " + cmd)

    # 若启用post-copy，则先启动lazy-pages守护进程
    if lazy:
        lazy_cmd = "criu lazy-pages --page-server --address " + str(source_ip)
        lazy_cmd += " --port 27 -v4 -D "
        lazy_cmd += msg["restore"]["image_path"]
        lazy_cmd += " -W " + msg["restore"]["path"] + "/migrate/r_log"
        lazy_cmd += " -o " + msg["restore"]["path"] + "/migrate/r_log/lp.log"
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
    # 计算并记录恢复期间的 wall-clock 耗时（毫秒）和 CPU 使用变化。
    cpu_delta = cpu_end - cpu_start
    elapsed_ms = (end_time - start_time) * 1000
    logger.info(f"restore elapsed {elapsed_ms:.3f} ms, CPU change {cpu_delta:.2f}%")

    if lazy:
        # 等待 lazy-pages 守护进程结束
        lp.wait()

    if ret == 0:
        restore_log_path = msg["restore"]["path"] + "/migrate/r_log"
        get_restore_time(restore_log_path)
        # print(123)
        if lazy:
            # print(456)
            lp_log_file = msg["restore"]["path"] + "/migrate/r_log/lp.log"

            total_uffd_copy = calculate_uffd_copy(lp_log_file)
            rpf_handle_time = get_rpf_handle_time(lp_log_file)
            # 将 total_uffd_copy 从字节转换为 KB，保留两位小数
            total_uffd_copy_kb = total_uffd_copy / 1024.0

            reply = "runc restored %s successfully with %.3f ms, total_uffd_copy: %.2f KB, rpf_handle_time: %.2f ms" % (
                msg["restore"]["name"],
                rst_time,
                total_uffd_copy_kb,
                rpf_handle_time,
            )
        else:
            reply = "runc restored %s successfully with %.3f ms" % (msg["restore"]["name"], rst_time)
    else:
        reply = "runc failed(%d)" % ret

    os.chdir(old_cwd)
    return reply


def _wait_file_stable(path, timeout=10.0, interval=0.1):
    import os
    import time

    end = time.time() + timeout
    last = None
    while time.time() < end:
        if os.path.exists(path):
            sz = os.path.getsize(path)
            if last is not None and sz == last:
                return True
            last = sz
def _reset_transfer_state():
    """Clear previous transfer sessions and reset per-run flags."""
    transfer_manager.reset()
    global vip_transfer_complete, rst_time
    vip_transfer_complete = False
    rst_time = 0.0


def _wait_final_transfer_complete(timeout: float = 60.0) -> bool:
    """Block until the final dump session reports completion."""
    result = transfer_manager.wait_for_final(timeout)
    if result is None:
        logger.error("Timeout waiting for final transfer completion")
        return False

    status = result.get("status") if isinstance(result, dict) else None
    if status in ("OK", "N/A"):
        return True

    logger.error("Final transfer did not complete successfully: %s", result)
    return False


def handle_restore(msg):
    """
    处理 restore 命令，由于使用同步传输，传输在迁移过程中已完成，直接执行恢复操作。
    """
    # 检查是否启用了TCP连接迁移，若启用且VIP未迁移则主动迁移
    runc_args_str = msg["restore"].get("runc_args", "")
    needs_vip_transfer = "--tcp-established" in runc_args_str
    # print(1211111)
    image_path = msg["restore"]["image_path"]
    desc = os.path.join(image_path, "descriptors.json")
    # 2) 等待最终镜像解包完成
    if not _wait_final_transfer_complete(timeout=120.0):
        return "final dump transfer incomplete"

    # 3) 等待 descriptors.json 存在且大小稳定
    if not _wait_file_stable(desc, timeout=30.0):
        logger.error("descriptors.json not ready at %s", desc)
        return "descriptors.json not ready"

    if needs_vip_transfer:
        global vip_transfer_complete
        if not vip_transfer_complete:
            # logger.info("执行VIP转移")
            ret_code = transfer_vip()
            vip_transfer_complete = ret_code == 0
            if ret_code == 0:
                logger.debug("VIP迁移完成")
            else:
                logger.error("VIP迁移失败")

    # logger.info("开始执行恢复操作")
    reply = perform_restore(msg)

    # 异步启动进程清理任务，让主线程快速响应
    def _cleanup_worker():
        transfer_manager.reset()
        logger.debug("transfer sessions cleaned up after restore")

    # 使用线程池异步执行清理
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="cleanup")
    executor.submit(_cleanup_worker)
    executor.shutdown(wait=False)
    logger.debug("已启动异步进程清理任务")

    threading.Thread(target=_restore_target_priority_later, daemon=True).start()
    return reply


def migrate_server():
    HOST = ""  # Symbolic name meaning all available interfaces
    PORT = 18863

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print("Socket created")

    # Bind socket to local host and port
    try:
        s.bind((HOST, PORT))
    except socket.error as exc:
        # 使用异常对象的字符串表示而不是按索引访问（旧代码假定 msg 可索引，导致类型错误）
        print(f"Bind failed: {exc}")
        sys.exit()

    print("Socket bind complete")

    # Start listening on socket
    s.listen(10)
    print("Socket now listening")

    # Function for handling connections. This will be used to create threads
    def clientthread(conn, addr):
        # Sending message to connected client
        # infinite loop so that function does not terminate and thread does not end.
        while True:
            reply = ""
            # Receiving from client
            data = conn.recv(1024)
            # print("data:",data)
            if not data:
                break  # 连接断开时退出循环
            # 解码数据
            decoded_data = data.decode("utf-8").strip()
            if decoded_data.lower() == "exit":
                break
            print("received: ", decoded_data)

            if data == "exit":
                break

            try:
                # Parse JSON string into Python dictionary
                msg = json.loads(decoded_data)
                # print("clientthread msg:",msg)
                # print("msg keys:", list(msg.keys()), repr(list(msg.keys())[0]))
                # old_cwd = os.getcwd()

                match msg:
                    case {"transfer_vip": _}:
                        # 检查并设置VIP转移完成状态
                        global vip_transfer_complete
                        if vip_transfer_complete:
                            logger.debug("VIP已迁移，通知source")
                            reply = "OK"
                        else:
                            ret = transfer_vip()
                            vip_transfer_complete = ret == 0
                            if ret == 0:
                                logger.debug("VIP完成迁移，通知source")
                                reply = "OK"
                            else:
                                reply = "Error"

                    case {"prepare": prepare_info}:
                        reply = handle_prepare(prepare_info)
                    case {"transfer_status": status_info}:
                        reply = handle_transfer_status(status_info)
                    case {"restore": _}:
                        # 如果所有传输已完成，立即执行恢复
                        # 所有传输指last_iter及之前的传输，和最大端口对应的传输
                        # time.sleep(1)
                        reply = handle_restore(msg)
                    case _:
                        print("Unknown request: " + msg)
                        reply = "unknown request"
            except Exception:
                continue

            print(reply)
            conn.sendall(bytes(reply, encoding="utf-8"))

        # came out of loop
        conn.close()

    # now keep talking with the client
    while 1:
        # wait to accept a connection - blocking call
        conn, addr = s.accept()
        print("Connected with " + addr[0] + ":" + str(addr[1]))
        global source_ip
        source_ip = addr[0]

        # start new thread takes 1st argument as a function name to be run,
        # second is the tuple of arguments to the function.
        start_new_thread(
            clientthread,
            (
                conn,
                str(addr[0]),
            ),
        )

    s.close()


if __name__ == "__main__":
    migrate_server()
