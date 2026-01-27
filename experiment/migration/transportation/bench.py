#!/usr/bin/env python3
# coding: utf-8
"""
bench_v2x_communication.py - V2X车联网通信负载

模拟车路协同（V2X）场景：
- 车辆实时位置更新（高频）
- 地理位置查询（GEORADIUS）
- 交通事件广播（Pub/Sub）
- 超低延迟要求（<10ms）

业界实例：
- 华为 C-V2X: 车路协同解决方案
- 百度 Apollo: 自动驾驶V2X通信
- 移动 5G MEC: 车联网边缘计算
- Qualcomm C-V2X: 芯片级V2X支持

USAGE:
    python3 bench_v2x_communication.py --redis-host 127.0.0.1 --vehicles 500 --duration 60
    python3 bench_v2x_communication.py --redis-host 127.0.0.1 --query-radius 500 --update-freq 10
"""

import argparse
import json
import logging
import random
import threading
import time
import math
import statistics
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import os
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


class Vehicle:
    """车辆运动模拟器"""

    # 上海市中心区域（示例）
    CENTER_LAT = 31.2304
    CENTER_LON = 121.4737
    AREA_SIZE_KM = 5.0  # 5km x 5km区域

    def __init__(self, vehicle_id: str):
        self.vehicle_id = vehicle_id

        # 初始化随机位置
        self.lat = self.CENTER_LAT + random.uniform(-0.05, 0.05)
        self.lon = self.CENTER_LON + random.uniform(-0.05, 0.05)

        # 运动参数
        self.speed_kmh = random.uniform(20, 80)  # 20-80 km/h
        self.heading = random.uniform(0, 360)  # 方向角度

        # 车辆状态
        self.emergency = False
        self.last_emergency_time = 0

    def update(self, dt: float):
        """更新车辆位置（dt为时间间隔，秒）"""
        # 速度变化（模拟加减速）
        speed_change = random.gauss(0, 5)
        self.speed_kmh = max(0, min(120, self.speed_kmh + speed_change))

        # 方向变化（模拟转向）
        heading_change = random.gauss(0, 10)
        self.heading = (self.heading + heading_change) % 360

        # 计算位移（简化版，忽略地球曲率）
        distance_km = (self.speed_kmh / 3600) * dt

        # 1度纬度约111km，1度经度约111*cos(lat)km
        dlat = distance_km * math.cos(math.radians(self.heading)) / 111.0
        dlon = distance_km * math.sin(math.radians(self.heading)) / (111.0 * math.cos(math.radians(self.lat)))

        self.lat += dlat
        self.lon += dlon

        # 边界检查（保持在区域内）
        lat_range = self.AREA_SIZE_KM / 222.0
        lon_range = self.AREA_SIZE_KM / (222.0 * math.cos(math.radians(self.CENTER_LAT)))

        if abs(self.lat - self.CENTER_LAT) > lat_range:
            self.lat = self.CENTER_LAT + random.uniform(-lat_range, lat_range)
            self.heading = (self.heading + 180) % 360  # 掉头

        if abs(self.lon - self.CENTER_LON) > lon_range:
            self.lon = self.CENTER_LON + random.uniform(-lon_range, lon_range)
            self.heading = (self.heading + 180) % 360

        # 随机触发紧急事件（急刹车、碰撞风险）
        current_time = time.time()
        if not self.emergency and random.random() < 0.001:  # 0.1%概率
            self.emergency = True
            self.last_emergency_time = current_time

        # 紧急状态持续5秒
        if self.emergency and current_time - self.last_emergency_time > 5:
            self.emergency = False

    def get_position(self) -> Tuple[float, float]:
        """获取当前位置"""
        return self.lon, self.lat  # 注意：Redis Geo使用(经度, 纬度)顺序


class V2XCommunicationBench:
    """V2X通信基准测试"""

    def __init__(self, args):
        self.args = args
        self.redis_client = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            decode_responses=True,
            socket_timeout=2,  # V2X要求低延迟
            socket_connect_timeout=2
        )

        # 创建车辆
        self.vehicles = [Vehicle(f"vehicle-{i:04d}") for i in range(args.vehicles)]

        # Pub/Sub连接（单独的连接用于订阅）
        self.pubsub_client = self.redis_client.pubsub()
        self.pubsub_client.subscribe('v2x:emergency', 'v2x:traffic')

        self.stats = {
            'position_updates': 0,
            'proximity_queries': 0,
            'emergency_events': 0,
            'messages_received': 0,
            'update_latencies': [],
            'query_latencies': [],
            'errors': 0
        }
        self.stop_flag = threading.Event()
        self.lock = threading.Lock()

        self.metrics = None
        if IntervalMetrics:
            self.metrics = IntervalMetrics(
                interval_sec=getattr(args, 'metrics_interval', 1.0),
                out_path=getattr(args, 'metrics_out', None),
                label='transportation',
                logger=logger,
            )
        if bench_common and self.metrics:
            try:
                bench_common.register_metrics_signal_handlers(self.metrics)
            except Exception:
                pass

    def pubsub_listener(self):
        """Pub/Sub消息监听线程"""
        logger.info("Pub/Sub监听线程启动")

        while not self.stop_flag.is_set():
            try:
                message = self.pubsub_client.get_message(timeout=0.1)
                if message and message['type'] == 'message':
                    with self.lock:
                        self.stats['messages_received'] += 1

                    # 解析消息
                    try:
                        data = json.loads(message['data'])
                        logger.debug(f"收到紧急消息: {data['vehicle_id']} - {data['event_type']}")
                    except:
                        pass
            except Exception as e:
                logger.debug(f"Pub/Sub错误: {e}")
                time.sleep(0.1)

    def worker(self, thread_id: int):
        """工作线程"""
        local_stats = {
            'updates': 0, 'queries': 0, 'emergencies': 0,
            'errors': 0, 'update_lat': [], 'query_lat': []
        }

        # 分配车辆给线程
        vehicle_indices = list(range(len(self.vehicles)))
        random.shuffle(vehicle_indices)
        my_vehicles = [
            self.vehicles[i] for i in vehicle_indices
            if i % self.args.threads == thread_id
        ]

        logger.info(f"线程 {thread_id}: 处理 {len(my_vehicles)} 辆车")

        update_interval = 1.0 / self.args.update_freq
        last_update = time.time()

        while not self.stop_flag.is_set():
            current_time = time.time()
            dt = current_time - last_update

            for vehicle in my_vehicles:
                try:
                    # 更新车辆位置
                    vehicle.update(dt)
                    lon, lat = vehicle.get_position()

                    # 写入Redis Geo
                    start = time.perf_counter()
                    self.redis_client.geoadd(
                        'v2x:positions',
                        (lon, lat, vehicle.vehicle_id)
                    )
                    update_lat = (time.perf_counter() - start) * 1000
                    local_stats['update_lat'].append(update_lat)
                    local_stats['updates'] += 1
                    if self.metrics:
                        self.metrics.record(True, update_lat)

                    # 查询附近车辆（周边感知）
                    start = time.perf_counter()
                    nearby = self.redis_client.georadius(
                        'v2x:positions',
                        lon, lat,
                        self.args.query_radius,
                        unit='m',
                        withdist=True,
                        count=50  # 最多返回50辆车
                    )
                    query_lat = (time.perf_counter() - start) * 1000
                    local_stats['query_lat'].append(query_lat)
                    local_stats['queries'] += 1
                    if self.metrics:
                        self.metrics.record(True, query_lat)

                    # 碰撞风险检测（距离<50m且相对速度高）
                    for other_id, distance in nearby:
                        if other_id != vehicle.vehicle_id and distance < 50:
                            # 简化版：仅基于距离判断
                            if random.random() < 0.01:  # 1%触发告警
                                self.redis_client.publish(
                                    'v2x:traffic',
                                    json.dumps({
                                        'type': 'collision_risk',
                                        'vehicle_id': vehicle.vehicle_id,
                                        'other_vehicle': other_id,
                                        'distance': distance,
                                        'timestamp': int(time.time() * 1000)
                                    })
                                )

                    # 紧急事件广播
                    if vehicle.emergency:
                        self.redis_client.publish(
                            'v2x:emergency',
                            json.dumps({
                                'vehicle_id': vehicle.vehicle_id,
                                'event_type': 'hard_brake',
                                'position': {'lat': lat, 'lon': lon},
                                'speed': vehicle.speed_kmh,
                                'timestamp': int(time.time() * 1000)
                            })
                        )
                        local_stats['emergencies'] += 1

                    # 更新车辆详细状态（Hash）
                    status_key = f"v2x:status:{vehicle.vehicle_id}"
                    self.redis_client.hset(status_key, mapping={
                        'lat': f"{lat:.6f}",
                        'lon': f"{lon:.6f}",
                        'speed': f"{vehicle.speed_kmh:.1f}",
                        'heading': f"{vehicle.heading:.1f}",
                        'emergency': str(vehicle.emergency).lower(),
                        'last_update': int(time.time() * 1000)
                    })
                    self.redis_client.expire(status_key, 10)  # 10秒过期

                except Exception as e:
                    logger.debug(f"线程 {thread_id} 错误: {e}")
                    local_stats['errors'] += 1
                    if self.metrics:
                        self.metrics.record(False, None)

            last_update = current_time
            time.sleep(update_interval)

        # 合并统计
        with self.lock:
            self.stats['position_updates'] += local_stats['updates']
            self.stats['proximity_queries'] += local_stats['queries']
            self.stats['emergency_events'] += local_stats['emergencies']
            self.stats['errors'] += local_stats['errors']
            self.stats['update_latencies'].extend(local_stats['update_lat'])
            self.stats['query_latencies'].extend(local_stats['query_lat'])

    def run(self):
        """运行基准测试"""
        logger.info(f"启动V2X通信基准测试:")
        logger.info(f"  车辆数量: {self.args.vehicles}")
        logger.info(f"  更新频率: {self.args.update_freq} Hz")
        logger.info(f"  查询半径: {self.args.query_radius} m")
        logger.info(f"  线程数: {self.args.threads}")
        logger.info(f"  运行时长: {self.args.duration} 秒")

        # 清理旧数据
        try:
            for key in self.redis_client.scan_iter("v2x:*"):
                self.redis_client.delete(key)
            logger.info("清理旧数据完成")
        except Exception as e:
            logger.warning(f"清理失败: {e}")

        # 启动Pub/Sub监听线程
        pubsub_thread = threading.Thread(target=self.pubsub_listener)
        pubsub_thread.start()

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
                    update_qps = self.stats['position_updates'] / elapsed if elapsed > 0 else 0
                    query_qps = self.stats['proximity_queries'] / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"进度: {elapsed:.1f}s | 位置更新: {self.stats['position_updates']} ({update_qps:.1f} QPS) | "
                        f"查询: {self.stats['proximity_queries']} ({query_qps:.1f} QPS) | "
                        f"紧急事件: {self.stats['emergency_events']} | "
                        f"收到消息: {self.stats['messages_received']}"
                    )
        except KeyboardInterrupt:
            logger.info("用户中断")

        # 停止线程
        self.stop_flag.set()
        for t in threads:
            t.join()
        pubsub_thread.join()

        if self.metrics:
            self.metrics.stop()
            self.metrics.write()

        # 打印结果
        self.print_results(time.time() - start_time)

    def print_results(self, elapsed: float):
        """打印测试结果"""
        print("\n" + "="*60)
        print("V2X通信基准测试结果")
        print("="*60)

        updates = self.stats['position_updates']
        queries = self.stats['proximity_queries']
        emergencies = self.stats['emergency_events']
        messages = self.stats['messages_received']
        errors = self.stats['errors']
        update_lat = self.stats['update_latencies']
        query_lat = self.stats['query_latencies']

        print(f"运行时长:            {elapsed:.2f} 秒")
        print(f"位置更新数:          {updates:,} ({updates/elapsed:.2f} QPS)")
        print(f"邻近查询数:          {queries:,} ({queries/elapsed:.2f} QPS)")
        print(f"紧急事件数:          {emergencies:,}")
        print(f"收到消息数:          {messages:,}")
        print(f"错误数:              {errors}")

        if update_lat:
            print(f"\n位置更新延迟 (ms):")
            print(f"  平均值:            {statistics.mean(update_lat):.3f}")
            print(f"  中位数:            {statistics.median(update_lat):.3f}")
            sorted_lat = sorted(update_lat)
            print(f"  P90:               {sorted_lat[int(len(sorted_lat)*0.9)]:.3f}")
            print(f"  P99:               {sorted_lat[int(len(sorted_lat)*0.99)]:.3f}")
            print(f"  最大值:            {max(update_lat):.3f}")

        if query_lat:
            print(f"\n邻近查询延迟 (ms):")
            print(f"  平均值:            {statistics.mean(query_lat):.3f}")
            print(f"  中位数:            {statistics.median(query_lat):.3f}")
            sorted_lat = sorted(query_lat)
            print(f"  P90:               {sorted_lat[int(len(sorted_lat)*0.9)]:.3f}")
            print(f"  P99:               {sorted_lat[int(len(sorted_lat)*0.99)]:.3f}")
            print(f"  最大值:            {max(query_lat):.3f}")

        print("="*60)


def main():
    parser = argparse.ArgumentParser(description="V2X车联网通信边缘负载")
    parser.add_argument('--redis-host', default='127.0.0.1', help='Redis主机')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis端口')
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=4, help='工作线程数')
    parser.add_argument('--duration', type=int, default=60, help='运行时长（秒）')
    parser.add_argument('--frontend-url', dest='frontend_url', default=None, help='Optional HTTP frontend URL to route requests through')
    parser.add_argument('--dataset', default=None, help='Path to dataset directory (default: repo datasets/)')
    parser.add_argument('--vehicles', type=int, default=500, help='车辆数量')
    parser.add_argument('--update-freq', type=float, default=10.0, help='位置更新频率（Hz）')
    parser.add_argument('--query-radius', type=int, default=500, help='查询半径（米）')
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

    bench = V2XCommunicationBench(args)
    bench.run()


if __name__ == '__main__':
    main()
