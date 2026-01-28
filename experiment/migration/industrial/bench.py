#!/usr/bin/env python3
# coding: utf-8
"""
bench_predictive_maintenance.py - 预测性维护边缘负载

模拟工业物联网中的预测性维护场景：
- 多设备并发监控（每设备多传感器）
- 高频数据采集 → 边缘聚合 → 定期持久化
- 异常检测触发告警
- 真实物理约束（传感器漂移、电池消耗、故障模式）

业界实例：
- GE Predix: 工业设备健康监测
- 西门子 MindSphere: 预测性维护平台
- 施耐德 EcoStruxure: 能源设备监控

USAGE:
    python3 bench_predictive_maintenance.py --redis-host 127.0.0.1 --threads 4 --duration 60 --devices 50
    python3 bench_predictive_maintenance.py --redis-host 127.0.0.1 --anomaly-rate 0.10 --sampling-rate 10
"""

import argparse
import os
import json
import logging
import random
import threading
import time
import math
import statistics
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# Dynamic bench_common import (searches up the tree for common/bench_common.py)
try:
    import importlib.util as _importlib_util, os as _os
    _cur = _os.path.abspath(_os.path.dirname(__file__))
    _bench_common = None
    for _ in range(6):
        _candidate = _os.path.join(_cur, 'common', 'bench_common.py')
        if _os.path.exists(_candidate):
            spec = _importlib_util.spec_from_file_location('bench_common', _candidate)
            _bench_common = _importlib_util.module_from_spec(spec)
            spec.loader.exec_module(_bench_common)
            break
        _cur = os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

IntervalMetrics = getattr(bench_common, 'IntervalMetrics', None) if bench_common else None

logger = logging.getLogger(__name__)


class SensorSimulator:
    """模拟真实工业传感器行为"""

    SENSOR_TYPES = {
        'vibration_x': {'range': (0.0, 50.0), 'unit': 'm/s²', 'normal_max': 10.0},
        'vibration_y': {'range': (0.0, 50.0), 'unit': 'm/s²', 'normal_max': 10.0},
        'vibration_z': {'range': (0.0, 50.0), 'unit': 'm/s²', 'normal_max': 10.0},
        'temperature': {'range': (20.0, 120.0), 'unit': '°C', 'normal_max': 85.0},
        'pressure': {'range': (0.0, 10.0), 'unit': 'bar', 'normal_max': 8.0},
        'current': {'range': (0.0, 100.0), 'unit': 'A', 'normal_max': 80.0},
        'rpm': {'range': (0, 3000), 'unit': 'RPM', 'normal_max': 2500}
    }

    FAULT_PATTERNS = {
        'bearing_wear': {
            'affected_sensors': ['vibration_x', 'vibration_y', 'temperature'],
            'pattern': 'gradual_increase',
            'duration_sec': 300  # 5分钟逐渐恶化
        },
        'overheating': {
            'affected_sensors': ['temperature', 'current'],
            'pattern': 'sudden_spike',
            'duration_sec': 60
        },
        'pressure_leak': {
            'affected_sensors': ['pressure'],
            'pattern': 'gradual_decrease',
            'duration_sec': 180
        },
        'electrical_fault': {
            'affected_sensors': ['current', 'rpm'],
            'pattern': 'oscillation',
            'duration_sec': 120
        }
    }

    def __init__(self, device_id: str, anomaly_rate: float = 0.05):
        self.device_id = device_id
        self.anomaly_rate = anomaly_rate
        self.base_values = {}
        self.drift_rates = {}
        self.fault_state = None
        self.fault_start = None
        self.battery_level = 100.0
        self.calibration_drift = 0.0

        # 初始化基线值（带设备个体差异）
        for sensor, config in self.SENSOR_TYPES.items():
            min_val, max_val = config['range']
            normal_max = config.get('normal_max', max_val * 0.8)
            self.base_values[sensor] = random.uniform(min_val, normal_max * 0.5)
            self.drift_rates[sensor] = random.gauss(0, 0.001)  # 缓慢漂移

    def read_sensors(self) -> Dict[str, float]:
        """读取所有传感器数据"""
        current_time = time.time()
        values = {}

        # 检查是否触发新故障
        if self.fault_state is None and random.random() < self.anomaly_rate / 100:
            self.fault_state = random.choice(list(self.FAULT_PATTERNS.keys()))
            self.fault_start = current_time
            logger.info(f"[{self.device_id}] 故障模式触发: {self.fault_state}")

        # 计算故障进度
        fault_progress = 0.0
        if self.fault_state:
            elapsed = current_time - self.fault_start
            duration = self.FAULT_PATTERNS[self.fault_state]['duration_sec']
            fault_progress = min(elapsed / duration, 1.0)

            if fault_progress >= 1.0:
                logger.info(f"[{self.device_id}] 故障模式结束: {self.fault_state}")
                self.fault_state = None

        # 生成传感器数据
        for sensor, config in self.SENSOR_TYPES.items():
            base_val = self.base_values[sensor]

            # 正常波动（白噪声 + 周期性）
            noise = random.gauss(0, base_val * 0.02)
            periodic = base_val * 0.05 * math.sin(current_time / 10)

            # 缓慢漂移（传感器老化）
            drift = self.drift_rates[sensor] * (current_time % 86400)

            # 校准误差
            calibration_error = self.calibration_drift * base_val

            value = base_val + noise + periodic + drift + calibration_error

            # 应用故障模式
            if self.fault_state:
                fault = self.FAULT_PATTERNS[self.fault_state]
                if sensor in fault['affected_sensors']:
                    value = self._apply_fault_pattern(value, fault['pattern'], fault_progress)

            # 限制范围
            min_val, max_val = config['range']
            values[sensor] = max(min_val, min(max_val, value))

        # 电池消耗（缓慢下降）
        self.battery_level = max(0, self.battery_level - 0.0001)
        values['battery_level'] = round(self.battery_level, 2)

        # 校准漂移累积
        self.calibration_drift += random.gauss(0, 0.00001)

        return values

    def _apply_fault_pattern(self, value: float, pattern: str, progress: float) -> float:
        """应用故障模式"""
        if pattern == 'gradual_increase':
            return value * (1 + progress * 3)  # 最终增加到3倍
        elif pattern == 'sudden_spike':
            if progress < 0.1:
                return value * 5  # 突然激增
            else:
                return value * (1 + 2 * (1 - progress))  # 缓慢恢复
        elif pattern == 'gradual_decrease':
            return value * (1 - progress * 0.8)  # 下降80%
        elif pattern == 'oscillation':
            return value * (1 + 0.5 * math.sin(progress * 20 * math.pi))
        return value

    def is_anomaly(self, values: Dict[str, float]) -> Tuple[bool, Optional[str]]:
        """检测异常"""
        for sensor, value in values.items():
            if sensor == 'battery_level':
                continue

            config = self.SENSOR_TYPES.get(sensor)
            if config and value > config.get('normal_max', config['range'][1]):
                return True, f"{sensor} 超过阈值: {value:.2f} > {config['normal_max']}"

        return False, None


class PredictiveMaintenanceBench:
    """预测性维护基准测试"""

    # LUA 脚本：计算滑动窗口内的健康得分 (均值 + 标准差)
    # 这模拟了复杂的内存计算状态，迁移时状态丢失会导致得分“跳变”
    HEALTH_ANALYSIS_LUA = """
    local hist_key = KEYS[1]
    local status_key = KEYS[2]
    local val = tonumber(ARGV[1])
    local max_hist = tonumber(ARGV[2])

    -- 1. 推入新值并保持窗口大小
    redis.call('LPUSH', hist_key, val)
    redis.call('LTRIM', hist_key, 0, max_hist - 1)

    -- 2. 获取历史并计算统计量
    local history = redis.call('LRANGE', hist_key, 0, -1)
    local sum = 0
    local count = #history
    if count == 0 then return 100 end

    for i=1, count do
        sum = sum + tonumber(history[i])
    end
    local avg = sum / count

    local sq_diff_sum = 0
    for i=1, count do
        local diff = tonumber(history[i]) - avg
        sq_diff_sum = sq_diff_sum + (diff * diff)
    end
    local std = math.sqrt(sq_diff_sum / count)

    -- 3. 计算健康分 (受波动率 std 和 均值 avg 共同影响)
    local health = 100 - (std * 2) - (math.abs(avg - 5) * 0.5)
    health = math.max(0, math.min(100, health))

    redis.call('HSET', status_key,
        'health_score', string.format("%.2f", health),
        'avg_vibration', string.format("%.2f", avg),
        'std_dev', string.format("%.2f", std)
    )
    return tostring(health)
    """

    def __init__(self, args):
        self.args = args
        self.redis_client = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5
        )
        # 注册 LUA 脚本
        self.health_script = self.redis_client.register_script(self.HEALTH_ANALYSIS_LUA)

        self.devices = [
            SensorSimulator(f"device-{i:03d}", args.anomaly_rate)
            for i in range(args.devices)
        ]

        self.stats = {
            'total_samples': 0,
            'anomalies_detected': 0,
            'latencies': [],
            'errors': 0
        }
        self.stop_flag = threading.Event()
        self.lock = threading.Lock()

        self.metrics = None
        if IntervalMetrics:
            self.metrics = IntervalMetrics(
                interval_sec=getattr(args, 'metrics_interval', 1.0),
                out_path=getattr(args, 'metrics_out', None),
                label='industrial',
                logger=logger,
            )
        if bench_common and self.metrics:
            try:
                bench_common.register_metrics_signal_handlers(self.metrics)
            except Exception:
                pass

    def worker(self, thread_id: int):
        """工作线程"""
        local_stats = {'samples': 0, 'anomalies': 0, 'errors': 0, 'latencies': []}
        device_indices = list(range(len(self.devices)))
        random.shuffle(device_indices)

        # 均匀分配设备给线程
        my_devices = [
            self.devices[i] for i in device_indices
            if i % self.args.threads == thread_id
        ]

        logger.info(f"线程 {thread_id}: 监控 {len(my_devices)} 个设备")

        interval = 1.0 / self.args.sampling_rate

        while not self.stop_flag.is_set():
            for device in my_devices:
                start = time.perf_counter()

                try:
                    # 读取传感器数据
                    sensor_data = device.read_sensors()
                    timestamp = int(time.time() * 1000)

                    # 检测异常
                    is_anomaly, reason = device.is_anomaly(sensor_data)

                    # 构建数据记录
                    record = {
                        'device_id': device.device_id,
                        'timestamp': timestamp,
                        'sensors': sensor_data,
                        'anomaly': is_anomaly,
                        'reason': reason
                    }

                    # 写入Redis Stream（实时数据流）
                    stream_key = f"maintenance:stream:{device.device_id}"
                    self.redis_client.xadd(
                        stream_key,
                        {'data': json.dumps(record)},
                        maxlen=1000  # 保留最近1000条
                    )

                    # 如果异常，写入告警队列
                    if is_anomaly:
                        alert_key = "maintenance:alerts"
                        self.redis_client.zadd(
                            alert_key,
                            {json.dumps(record): timestamp}
                        )
                        local_stats['anomalies'] += 1

                    # 更新设备状态（Hash）
                    status_key = f"maintenance:status:{device.device_id}"
                    self.redis_client.hset(status_key, mapping={
                        'last_update': timestamp,
                        'battery': sensor_data.get('battery_level', 100),
                        'anomaly': str(is_anomaly).lower(),
                        'temperature': f"{sensor_data.get('temperature', 0):.2f}",
                        'vibration_max': f"{max(sensor_data.get('vibration_x', 0), sensor_data.get('vibration_y', 0), sensor_data.get('vibration_z', 0)):.2f}"
                    })

                    # 执行 LUA 聚合分析 (状态依赖逻辑)
                    history_key = f"maintenance:history:{device.device_id}"
                    vibration = max(sensor_data.get('vibration_x', 0),
                                  sensor_data.get('vibration_y', 0),
                                  sensor_data.get('vibration_z', 0))
                    self.health_script(keys=[history_key, status_key], args=[vibration, 20])

                    latency = (time.perf_counter() - start) * 1000
                    local_stats['latencies'].append(latency)
                    local_stats['samples'] += 1
                    if self.metrics:
                        self.metrics.record(True, latency)

                except Exception as e:
                    logger.debug(f"线程 {thread_id} 错误: {e}")
                    local_stats['errors'] += 1
                    if self.metrics:
                        self.metrics.record(False, None)

                time.sleep(interval)

        # 合并统计
        with self.lock:
            self.stats['total_samples'] += local_stats['samples']
            self.stats['anomalies_detected'] += local_stats['anomalies']
            self.stats['errors'] += local_stats['errors']
            self.stats['latencies'].extend(local_stats['latencies'])

    def run(self):
        """运行基准测试"""
        logger.info(f"启动预测性维护基准测试:")
        logger.info(f"  设备数量: {self.args.devices}")
        logger.info(f"  采样率: {self.args.sampling_rate} Hz")
        logger.info(f"  线程数: {self.args.threads}")
        logger.info(f"  运行时长: {self.args.duration} 秒")
        logger.info(f"  异常率: {self.args.anomaly_rate * 100:.2f}%")

        # 清理旧数据
        try:
            for key in self.redis_client.scan_iter("maintenance:*"):
                self.redis_client.delete(key)
            logger.info("清理旧数据完成")
        except Exception as e:
            logger.warning(f"清理失败: {e}")

        # 启动工作线程
        threads = []
        start_time = time.time()

        if self.metrics:
            self.metrics.start()

        for i in range(self.args.threads):
            t = threading.Thread(target=self.worker, args=(i,))
            t.start()
            threads.append(t)

        # 定期打印进度
        try:
            while time.time() - start_time < self.args.duration:
                time.sleep(5)
                elapsed = time.time() - start_time
                with self.lock:
                    qps = self.stats['total_samples'] / elapsed if elapsed > 0 else 0
                    logger.info(f"进度: {elapsed:.1f}s | 采样数: {self.stats['total_samples']} | "
                              f"QPS: {qps:.1f} | 异常: {self.stats['anomalies_detected']}")
        except KeyboardInterrupt:
            logger.info("用户中断")

        # 停止线程
        self.stop_flag.set()
        for t in threads:
            t.join()

        if self.metrics:
            self.metrics.stop()
            self.metrics.write()

        # 打印结果
        self.print_results(time.time() - start_time)

    def print_results(self, elapsed: float):
        """打印测试结果"""
        print("\n" + "="*60)
        print("预测性维护基准测试结果")
        print("="*60)

        total = self.stats['total_samples']
        anomalies = self.stats['anomalies_detected']
        errors = self.stats['errors']
        latencies = self.stats['latencies']

        print(f"运行时长:      {elapsed:.2f} 秒")
        print(f"总采样数:      {total:,}")
        print(f"成功率:        {(total/(total+errors)*100):.2f}%" if total+errors > 0 else "N/A")
        print(f"异常检测数:    {anomalies:,} ({anomalies/total*100:.2f}%)" if total > 0 else "N/A")
        print(f"吞吐量:        {total/elapsed:.2f} samples/sec")

        if latencies:
            print(f"\n延迟统计 (ms):")
            print(f"  平均值:      {statistics.mean(latencies):.3f}")
            print(f"  中位数:      {statistics.median(latencies):.3f}")
            sorted_lat = sorted(latencies)
            print(f"  P90:         {sorted_lat[int(len(sorted_lat)*0.9)]:.3f}")
            print(f"  P99:         {sorted_lat[int(len(sorted_lat)*0.99)]:.3f}")
            print(f"  最大值:      {max(latencies):.3f}")

        print("="*60)


def main():
    parser = argparse.ArgumentParser(description="预测性维护边缘负载")
    parser.add_argument('--redis-host', default='127.0.0.1', help='Redis主机')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis端口')
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=4, help='工作线程数')
    parser.add_argument('--duration', type=int, default=60, help='运行时长（秒）')
    parser.add_argument('--frontend-url', dest='frontend_url', default=None, help='Optional HTTP frontend URL to route requests through')
    parser.add_argument('--dataset', default=None, help='Path to dataset directory (default: repo datasets/)')
    parser.add_argument('--devices', type=int, default=50, help='设备数量')
    parser.add_argument('--sampling-rate', type=float, default=50.0, help='采样率（Hz）')
    parser.add_argument('--anomaly-rate', type=float, default=0.05, help='异常触发概率（0-1）')
    parser.add_argument('--metrics-out', default=None, help='Output path for interval metrics (JSON)')
    parser.add_argument('--metrics-interval', type=float, default=1.0, help='Sampling interval seconds (default: 1.0)')

    args = parser.parse_args()

    # Common post-parse adjustments and dataset resolution
    if bench_common:
        if getattr(args, 'dataset', None) is None:
            args.dataset = bench_common.DEFAULT_DATASET_DIR
        args.dataset = bench_common.get_dataset_path(args)
        bench_common.configure_logging()
    else:
        if getattr(args, 'dataset', None) is None:
            args.dataset = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'datasets'))
        args.dataset = os.path.abspath(args.dataset)

    bench = PredictiveMaintenanceBench(args)
    bench.run()


if __name__ == '__main__':
    main()
