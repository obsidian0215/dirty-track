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
import shlex
import signal
from _thread import start_new_thread
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

import psutil  # type: ignore[import-not-found]
from script_defaults import get_default_ips

# Import HostResourceMonitor from the centralized monitor module (fallback to file import
# so the script can still be run directly as a script).
try:
    from mig_scripts.monitor import HostResourceMonitor
except Exception:
    import importlib.util as _il

    spec = _il.spec_from_file_location("monitor_mod", os.path.join(os.path.dirname(__file__), "monitor.py"))
    monitor_mod = _il.module_from_spec(spec)
    spec.loader.exec_module(monitor_mod)
    HostResourceMonitor = monitor_mod.HostResourceMonitor

compress = False
restore_info = None
rst_time = 0.0

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


# HostResourceMonitor is provided by `mig-scripts/monitor.py` and will be imported where needed.
# (was previously an inline duplicate here; centralized implementation lives in monitor.py)


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
        # map token -> HostResourceMonitor for sessions we are monitoring (final session typically)
        self._session_monitors: Dict[str, "HostResourceMonitor"] = {}

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

        # If we started a destination monitor for this session, stop it and expose the path
        mon = None
        try:
            mon = self._session_monitors.pop(session.token, None)
        except Exception:
            mon = None
        if mon:
            try:
                mon.stop()
                result["resource_path"] = mon.out_path
            except Exception:
                pass

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

        # If this is the final transfer, start a host-side monitor that will record
        # destination resource usage during the transfer and save it into d_log
        if is_final:
            try:
                ts = time.strftime("%Y%m%d-%H%M%S")
                parent_dir = os.path.abspath(os.path.join(extract_path, os.pardir))
                out_dir = os.path.join(parent_dir, "d_log")
                os.makedirs(out_dir, exist_ok=True)
                out_path = os.path.join(out_dir, f"resource_usage.dest.{ts}.tsv")
                mon = HostResourceMonitor(out_path, interval=1.0, iface="ens33")
                mon.start()
                self._session_monitors[session.token] = mon
            except Exception as e:
                logger.warning(f"Failed to start destination resource monitor: {e}")

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

    local_dest = bool(prepare_info.get("local_dest", False))

    parent_paths = prepare_info.get("parent_path", [])
    compress = prepare_info.get("compress", 0)

    path_exist = os.path.exists(path)
    if not path_exist and not os.path.exists(os.path.dirname(path)):
        reply = json.dumps({"status": "ERROR", "message": "Cannot find corresponding container bundle"})
        logger.error("Cannot find corresponding container bundle")
    else:
        prepare(path, image_path, parent_paths)

        session_details: List[Dict[str, object]] = []

        if local_dest:
            # 本机模式：不启动网络传输会话，直接返回本地会话描述
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

                session_details.append(
                    {
                        "token": "LOCAL",
                        "port": None,
                        "path": parent,
                        "iteration": iter_num if iter_num is not None else idx,
                        "type": "pre_dump",
                        "local": True,
                    }
                )

            with transfer_manager.lock:
                transfer_manager.final_token = "LOCAL_FINAL"
                transfer_manager.final_info = {"status": "N/A"}
                transfer_manager.final_event.set()

            final_info = {
                "token": "LOCAL_FINAL",
                "port": None,
                "path": image_path,
                "type": "final",
                "local": True,
            }
        else:
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
            final_info = {
                "token": final_session.token,
                "port": final_session.port,
                "path": image_path,
                "type": "final",
            }

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
                "final": final_info,
            },
        }

        reply = json.dumps(reply_payload)

    return reply


def handle_transfer_status(request: Dict[str, Any]) -> str:
    token = request.get("token")
    if not token:
        return json.dumps({"status": "ERROR", "message": "missing transfer token"})

    if token in {"LOCAL", "LOCAL_FINAL"}:
        return json.dumps({"status": "N/A", "bytes": None, "duration_ms": 0})

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
    """Ask the local side to take over the VIP (set a high priority).

    Delegates to `mig-scripts/vipctl.py` implementation.
    """
    try:
        try:
            # prefer package import
            import mig_scripts.vipctl as vipctl
        except Exception:
            import importlib.util as _il

            spec = _il.spec_from_file_location("vipctl_mod", os.path.join(os.path.dirname(__file__), "vipctl.py"))
            vipctl = _il.module_from_spec(spec)
            spec.loader.exec_module(vipctl)

        rc = vipctl.set_keepalived_priority(100)
        return 0 if rc == 0 else 1
    except Exception as e:
        print(f"transfer_vip failed: {e}")
        return 1


def calculate_uffd_copy(lp_log_file):
    """
    解析 lp.log 中的页传输字节数并返回字节总数（best-effort）。

    目前支持的模式：
    - uffd_copy: 0x.../<bytes>
    - page-xfer: ... Received <bytes> bytes
    - page-xfer: ... p 0x... [<count>] （count 表示页数，使用 4KB 页大小计算）

    返回：
        int: 估算的总复制字节数（字节）
    """
    uffd_copy_pattern = re.compile(r"uffd_copy:\s+0x[0-9a-fA-F]+/(\d+)")
    page_bytes_pattern = re.compile(r"page-xfer:.*Received\s+(\d+)\s+bytes", re.IGNORECASE)
    p_count_pattern = re.compile(r"page-xfer:.*p\s+0x[0-9a-fA-F]+\s+\[(\d+)\]")
    PAGE_SIZE = 4096

    total_bytes = 0
    page_count = 0
    uffd_matches = 0
    try:
        with open(lp_log_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = uffd_copy_pattern.search(line)
                if m:
                    total_bytes += int(m.group(1))
                    uffd_matches += 1
                    continue
                m = page_bytes_pattern.search(line)
                if m:
                    total_bytes += int(m.group(1))
                    continue
                m = p_count_pattern.search(line)
                if m:
                    cnt = int(m.group(1))
                    page_count += cnt
                    total_bytes += cnt * PAGE_SIZE
    except Exception as e:
        logger.debug("calculate_uffd_copy: failed to read %s: %s", lp_log_file, e)
        return 0

    logger.debug(
        "calculate_uffd_copy: lp_log=%s uffd_matches=%d page_count=%d total_bytes=%d",
        lp_log_file,
        uffd_matches,
        page_count,
        total_bytes,
    )

    return total_bytes


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
    Best-effort 计算错误页面传输的总时间（毫秒），从 lp.log 中提取时间戳并尝试匹配开始/结束区间。

    算法：
    - 先尝试查找连接/断开对 (Connecting / Disconnect)，累加每次传输持续时间
    - 若未找到显式对，则回退到 page-xfer 事件的首尾时间差（span）作为估算

    返回：
        float: 错误页面传输的总持续时间（毫秒）
    """
    # 常见的时间戳形式出现在行首，如：(00.013805)
    timestamp_pattern = re.compile(r"\(([\d\.]+)\)")
    connect_pattern = re.compile(r"\(([\d\.]+)\)\s+(?:Connecting to(?: server)?|page-xfer: Transferring pages)\b", re.IGNORECASE)
    disconnect_pattern = re.compile(r"\(([\d\.]+)\)\s+page-xfer:.*Disconnect", re.IGNORECASE)
    page_xfer_event = re.compile(r"\(([\d\.]+)\)\s+page-xfer:", re.IGNORECASE)

    transfer_durations = []
    connect_time = None
    first_ts = None
    last_ts = None

    try:
        with open(lp_log_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                ts_match = timestamp_pattern.search(line)
                if not ts_match:
                    continue
                ts = float(ts_match.group(1))
                if first_ts is None:
                    first_ts = ts
                last_ts = ts

                if connect_pattern.search(line):
                    connect_time = ts
                    continue

                if disconnect_pattern.search(line) and connect_time is not None:
                    duration = ts - connect_time
                    transfer_durations.append(duration)
                    connect_time = None
                    continue

                # 若出现 page-xfer 行但没有显式 Connecting/Disconnect 对，记录时间用于后续 span 回退
                if page_xfer_event.search(line) and connect_time is None:
                    # treat this as an implicit start only if we haven't seen one
                    connect_time = connect_time or ts

    except Exception as e:
        logger.debug("get_rpf_handle_time: failed to read %s: %s", lp_log_file, e)
        return 0.0

    if transfer_durations:
        total = sum(transfer_durations)
        total_ms = total * 1000.0
        logger.debug("get_rpf_handle_time: matched %d transfers -> %.2f ms", len(transfer_durations), total_ms)
        return total_ms

    # 回退到 span（first->last）作为估算
    if first_ts is not None and last_ts is not None and last_ts >= first_ts:
        span_ms = (last_ts - first_ts) * 1000.0
        logger.debug("get_rpf_handle_time: fallback span estimate %.2f ms", span_ms)
        return span_ms

    return 0.0


def perform_restore(msg):
    try:
        lazy = bool(distutils.util.strtobool(msg["restore"]["lazy"]))
        # 获取runc_args，如果不存在则为空字符串
        runc_args_str = msg["restore"].get("runc_args", "")
    except Exception as e:
        logger.warning("Error parsing restore parameters: %s", e)
        lazy = False
        runc_args_str = ""

    old_cwd = os.getcwd()
    os.chdir(msg["restore"]["path"])
    #   input()
    # 构建恢复命令
    cfg_path = os.path.join(msg["restore"]["path"], "config.json")
    needs_console = False
    console_sock = os.path.join(msg["restore"]["path"], "console.sock")
    try:
        cfg = json.load(open(cfg_path))
        needs_console = bool(cfg.get("process", {}).get("terminal", False))
    except Exception:
        needs_console = False

    if needs_console:
        # recvtty 的生命周期应由 fog_test 管理；此处不再启动 recvtty。
        # 等待短时（最多 6s）以便 fog_test 创建 console.sock；若超时则返回明确错误以便上游处理。
        timeout_sec = 6.0
        check_interval = 0.1
        end_t = time.time() + timeout_sec
        while time.time() < end_t:
            if os.path.exists(console_sock):
                break
            time.sleep(check_interval)
        else:
            logger.warning("console socket %s not present after %.1f seconds; expecting fog_test to start recvtty", console_sock, timeout_sec)
            return f"missing console socket {console_sock} - recvtty not started on destination"

    cmd = "time -p runc restore"
    if needs_console:
        cmd += f" --console-socket {console_sock}"
    cmd += " -d --image-path " + msg["restore"]["image_path"]
    cmd += " --work-path " + msg["restore"]["path"] + "/migrate/r_log"

    # 添加runc_args参数
    if runc_args_str:
        cmd += " " + runc_args_str

    if lazy:
        cmd += " --lazy-pages"
    cmd += " " + msg["restore"]["name"]
    # print("Restore command: " + cmd)

    # 保证 lp 变量在任何分支都有定义，便于后续等待/清理
    lp = None

    # 若启用post-copy，则先启动lazy-pages守护进程
    if lazy:
        lp_log_file = os.path.join(msg["restore"]["path"], "migrate", "r_log", "lp.log")
        lp_cmd = [
            "criu",
            "lazy-pages",
            "--page-server",
            "--address",
            str(source_ip),
            "--port",
            "27",
            "-v4",
            "-D",
            msg["restore"]["image_path"],
            "-W",
            os.path.join(msg["restore"]["path"], "migrate", "r_log"),
            "-o",
            lp_log_file,
        ]

        logger.info("Starting lazy-pages server: %s", " ".join(lp_cmd))
        try:
            lp = subprocess.Popen(lp_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, preexec_fn=os.setsid)
        except Exception as e:
            logger.error("Failed to start lazy-pages server: %s", e)
            lp = None
        # 等待 lp 日志出现或进程保持运行，最多等待 ~5秒
        started = False
        for _ in range(50):
            if lp and lp.poll() is not None:
                logger.error("lazy-pages process exited early with code %s", lp.returncode)
                break
            if os.path.exists(lp_log_file):
                started = True
                break
            time.sleep(0.1)
        if not started:
            logger.warning("lazy-pages log not present after wait; monitor will still proceed and may detect failures from restore.log")

    # 现在启动 runc restore 命令
    logger.info("Running restore command...")
    start_time = time.perf_counter()
    cpu_start = psutil.cpu_percent(interval=None)

    restore_log_file = os.path.join(msg["restore"]["path"], "migrate", "r_log", "restore.log")
    os.makedirs(os.path.dirname(restore_log_file), exist_ok=True)

    # Safely build command args (avoid shell=True when possible)
    try:
        cmd_args = shlex.split(cmd)
    except Exception:
        cmd_args = cmd

    # Start runc restore in its own process group so we can cleanly terminate subtree if needed
    try:
        p = subprocess.Popen(cmd_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid)
    except Exception as e:
        logger.error("Failed to start restore command: %s", e)
        return f"failed to start restore: {e}"

    # Stream stdout/stderr to restore log and monitor for definitive failure markers
    detected_failure = {"flag": False, "snippet": ""}

    def _stream_reader(stream, collector, stream_name):
        try:
            for line in iter(stream.readline, ""):
                if not line:
                    break
                collector.append(line)
                try:
                    with open(restore_log_file, "a", encoding="utf-8", errors="replace") as rf:
                        rf.write(line)
                except Exception:
                    pass
                if stream_name == "stderr":
                    logger.warning("%s: %s", stream_name, line.rstrip())
                else:
                    logger.debug("%s: %s", stream_name, line.rstrip())
                # Only treat explicit 'Restoring FAILED' as definitive failure
                try:
                    if "restoring failed" in line.lower():
                        detected_failure["flag"] = True
                        detected_failure["snippet"] = line.strip()
                        logger.error("Detected definitive failure marker in restore output: %s", detected_failure["snippet"])
                except Exception:
                    pass
        except Exception as e:
            logger.error("Error streaming %s: %s", stream_name, e)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    stdout_lines: List[str] = []
    stderr_lines: List[str] = []
    t_out = threading.Thread(target=_stream_reader, args=(p.stdout, stdout_lines, "stdout"), daemon=True)
    t_err = threading.Thread(target=_stream_reader, args=(p.stderr, stderr_lines, "stderr"), daemon=True)
    t_out.start()
    t_err.start()

    # Monitor process and abort early if we see a definitive failure marker
    while True:
        if p.poll() is not None:
            break
        if detected_failure["flag"]:
            logger.error("Definitive failure detected in restore logs; killing restore process group")
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except Exception:
                pass
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except Exception:
                    pass
            break
        time.sleep(0.1)

    # Ensure streaming threads have drained
    t_out.join(timeout=2)
    t_err.join(timeout=2)

    ret = p.poll()
    if ret is None:
        try:
            ret = p.wait(timeout=2)
        except Exception:
            ret = p.returncode if p.returncode is not None else -1

    end_time = time.perf_counter()
    cpu_end = psutil.cpu_percent(interval=None)
    cpu_delta = cpu_end - cpu_start
    elapsed_ms = (end_time - start_time) * 1000
    logger.info("restore elapsed %.3f ms, CPU change %.2f%%", elapsed_ms, cpu_delta)

    out = "".join(stdout_lines)
    err = "".join(stderr_lines)

    # Log short snippets for diagnostics
    if out:
        logger.info("restore stdout (snippet):\n%s", out[-8192:])
    if err:
        logger.info("restore stderr (snippet):\n%s", err[-8192:])

    # If we detected definitive failure marker, ensure return code indicates failure and annotate error
    if detected_failure["flag"]:
        if ret == 0:
            ret = 1
        err = (err or "") + "\n" + f"detected failure in restore logs: {detected_failure['snippet']}"

    if lazy:
        # Wait for lazy-pages process to exit, but don't block indefinitely.
        lp_log_file = os.path.join(msg["restore"]["path"], "migrate", "r_log", "lp.log")
        restore_log_file = os.path.join(msg["restore"]["path"], "migrate", "r_log", "restore.log")

        def _scan_logs_for_failure():
            # Only treat an explicit 'Restoring FAILED' marker in restore.log as a definitive restore failure.
            # Avoid treating generic 'error' messages as a fatal restore indicator.
            try:
                if os.path.exists(restore_log_file):
                    with open(restore_log_file, "rb") as fh:
                        fh.seek(0, os.SEEK_END)
                        size = fh.tell()
                        start = max(0, size - 8192)
                        fh.seek(start)
                        tail = fh.read().decode("utf-8", errors="replace")
                    # Match case-insensitively for the explicit failure marker
                    if "restoring failed" in tail.lower():
                        return (restore_log_file, tail[-2048:])
            except Exception:
                # best-effort, ignore read errors
                pass
            return None

        detected_failure = False
        detected_log = None
        snippet = None

        # initial scan (perhaps restore already logged an error)
        initial = _scan_logs_for_failure()
        if initial:
            detected_failure = True
            detected_log, snippet = initial

        if detected_failure:
            logger.error("Detected restore/lazy failure in %s: %s", detected_log, (snippet or '')[:200])
            if lp:
                try:
                    os.killpg(os.getpgid(lp.pid), signal.SIGTERM)
                except Exception:
                    try:
                        lp.kill()
                    except Exception:
                        pass
                try:
                    lp.wait(timeout=5)
                except Exception:
                    pass
            # Mark as error and annotate stderr for reply
            if ret == 0:
                ret = 1
                err = (err or "") + "\n" + f"detected failure in {detected_log}: {(snippet or '')[:400]}"
            else:
                err = (err or "") + "\n" + f"detected failure in {detected_log}: {(snippet or '')[:400]}"

    if ret == 0:
        restore_log_path = msg["restore"]["path"] + "/migrate/r_log"
        get_restore_time(restore_log_path)
        if lazy:
            lp_log_file = msg["restore"]["path"] + "/migrate/r_log/lp.log"

            # 等待 lazy-pages 进程退出或 lp.log 稳定（best-effort）
            try:
                if 'lp' in locals() and lp:
                    try:
                        logger.debug("Waiting up to 15s for lazy-pages to exit")
                        lp.wait(timeout=15)
                        logger.debug("lazy-pages exited before timeout")
                    except subprocess.TimeoutExpired:
                        logger.debug("lazy-pages still running after timeout; waiting for lp.log to stabilize for 5s")
                        if not _wait_file_stable(lp_log_file, timeout=5.0):
                            logger.warning("lp.log did not stabilize within timeout; parsing current content for best-effort metrics")
                else:
                    if not _wait_file_stable(lp_log_file, timeout=10.0):
                        logger.warning("lp.log not present or not stable; parsing current content for best-effort metrics")
            except Exception as e:
                logger.debug("Exception while waiting for lazy-pages/log stabilization: %s", e)

            total_uffd_copy = calculate_uffd_copy(lp_log_file)
            rpf_handle_time = get_rpf_handle_time(lp_log_file)
            total_uffd_copy_kb = total_uffd_copy / 1024.0

            # 简要摘要便于排查（会写入 restore 日志）
            logger.info("lp.log summary: total_uffd_copy=%d bytes (%.2f KB), rpf_handle_time=%.2f ms", total_uffd_copy, total_uffd_copy_kb, rpf_handle_time)

            reply = "runc restored %s successfully with %.3f ms, total_uffd_copy: %.2f KB, rpf_handle_time: %.2f ms" % (
                msg["restore"]["name"],
                rst_time,
                total_uffd_copy_kb,
                rpf_handle_time,
            )
        else:
            reply = "runc restored %s successfully with %.3f ms" % (msg["restore"]["name"], rst_time)
    else:
        # Include brief stderr in reply to aid debugging
        brief_err = (err or "")[:1024].replace("\n", "\\n")
        reply = f"runc failed({ret}) stderr={brief_err}"

    os.chdir(old_cwd)
    return reply


def _wait_file_stable(path, timeout=10.0, interval=0.1):
    """Wait for a file to appear and its size to stabilize between checks.

    Returns True if the file size stabilizes before timeout, otherwise False.
    """
    import os
    import time

    end = time.time() + float(timeout)
    last = None
    while time.time() < end:
        if os.path.exists(path):
            try:
                sz = os.path.getsize(path)
            except Exception:
                sz = None
            if last is not None and sz == last:
                return True
            last = sz
        # avoid busy-looping
        time.sleep(float(interval))
    logger.debug("_wait_file_stable timed out waiting for %s after %.1f seconds", path, float(timeout))
    return False
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

    # Write pre-restore diagnostics to FOG_TMP_DIR (or /tmp if unset) to aid debugging
    # try:
    #     tmp_dir = os.environ.get('FOG_TMP_DIR', '/tmp')
    #     try:
    #         os.makedirs(tmp_dir, exist_ok=True)
    #     except Exception:
    #         pass
    #     ts = int(time.time())
    #     pre_path = os.path.join(tmp_dir, f"destination_pre_restore_{ts}.log")
    #     try:
    #         with open(pre_path, 'w', encoding='utf-8') as fo:
    #             fo.write(f"image_path: {image_path}\n")
    #             fo.write(f"desc: {desc}\n\n=== ip addr show ===\n")
    #             try:
    #                 p = subprocess.run("ip addr show", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    #                 fo.write(p.stdout or '')
    #             except Exception:
    #                 fo.write("ip addr show failed\n")
    #             fo.write("\n=== ss -tnp ===\n")
    #             try:
    #                 p = subprocess.run("ss -tnp", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    #                 fo.write(p.stdout or '')
    #             except Exception:
    #                 fo.write("ss -tnp failed\n")
    #             fo.write("\n=== descriptors.json snippet ===\n")
    #             try:
    #                 with open(desc, 'r', encoding='utf-8', errors='ignore') as df:
    #                     fo.write((df.read(4096) or '')[:4096])
    #             except Exception as e:
    #                 fo.write(f"read descriptors failed: {e}\n")
    #             fo.write("\n\n=== tail restore.log ===\n")
    #             rlog = os.path.join(image_path, 'r_log', 'restore.log')
    #             if os.path.exists(rlog):
    #                 try:
    #                     p = subprocess.run(f"tail -n 200 {shlex.quote(rlog)}", shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    #                     fo.write(p.stdout or '')
    #                 except Exception:
    #                     fo.write(f"tail {rlog} failed\n")
    #             else:
    #                 fo.write(f"restore.log not found: {rlog}\n")
    #         logger.info("pre-restore diagnostics written to %s", pre_path)
    #     except Exception as _e:
    #         logger.warning("failed to write pre-restore diagnostics: %s", _e)
    # except Exception:
    #     pass

    # logger.info("开始执行恢复操作")
    reply = perform_restore(msg)

    # Persist reply to FOG_TMP_DIR for auditing
    # try:
    #     tmp_dir = os.environ.get('FOG_TMP_DIR', '/tmp')
    #     try:
    #         os.makedirs(tmp_dir, exist_ok=True)
    #     except Exception:
    #         pass
    #     ts = int(time.time())
    #     reply_path = os.path.join(tmp_dir, f"destination_reply_{msg['restore']['name']}_{ts}.txt")
    #     try:
    #         with open(reply_path, 'w', encoding='utf-8') as rf:
    #             rf.write(reply)
    #         logger.info("persisted reply to %s", reply_path)
    #     except Exception as _e:
    #         logger.warning("failed to persist reply: %s", _e)
    # except Exception:
    #     pass

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
            except Exception as exc:
                print(f"[server] error handling message: {exc}")
                continue

            if not reply:
                reply = "OK"
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
