#!/usr/bin/env python3
# coding: utf-8
"""
bench_surveillance_analytics.py - 智能监控视频分析负载

模拟边缘视频分析场景：
- 多路摄像头并发（16-64路）
- 目标检测与追踪（人、车、物）
- 帧级结果缓存（避免重复推理）
- 重要事件持久化

业界实例：
- 海康威视 AI Cloud: 边缘智能监控
- 大华 DeepHub: 视频结构化分析
- 商汤 SenseFoundry: 边缘视频分析平台
- AWS Panorama: 边缘计算机视觉

USAGE:
    python3 bench_surveillance_analytics.py --redis-host 127.0.0.1 --cameras 32 --fps 15 --duration 60
    python3 bench_surveillance_analytics.py --redis-host 127.0.0.1 --hotspot-ratio 0.3 --track-duration 10
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
import hashlib
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Set
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class ObjectTracker:
    """目标追踪器"""

    OBJECT_TYPES = ['person', 'car', 'bicycle', 'motorbike', 'truck']

    def __init__(self, camera_id: str, is_hotspot: bool = False):
        self.camera_id = camera_id
        self.is_hotspot = is_hotspot  # 热点摄像头（如出入口）
        self.active_tracks: Dict[str, Dict] = {}
        # 模拟 Re-ID 特征向量缓存 (增加内存压力和状态依赖)
        self.feature_vectors: Dict[str, List[float]] = {}
        self.track_counter = 0

        # 热点摄像头有更多目标
        self.base_object_count = random.randint(5, 15) if is_hotspot else random.randint(0, 5)

    def generate_frame_detections(self) -> List[Dict]:
        """生成一帧的检测结果"""
        detections = []
        current_time = time.time()

        # 更新现有目标
        to_remove = []
        for track_id, track_info in self.active_tracks.items():
            # 目标可能离开画面
            if random.random() < 0.05:  # 5%概率离开
                to_remove.append(track_id)
                continue

            # 更新位置（模拟运动）
            track_info['x'] += random.gauss(0, 10)
            track_info['y'] += random.gauss(0, 5)
            # ... (保持原位置更新逻辑)

            # 模拟特征向量波动 (产生大量脏页)
            if track_id in self.feature_vectors:
                self.feature_vectors[track_id] = [v + random.gauss(0, 0.01) for v in self.feature_vectors[track_id]]

            detections.append({
                'track_id': track_id,
                'object_type': track_info['object_type'],
                'bbox': [
                    int(track_info['x']),
                    int(track_info['y']),
                    int(track_info.get('width', 100)),
                    int(track_info.get('height', 200))
                ],
                'confidence': min(0.99, track_info['confidence'] + random.gauss(0, 0.05)),
                'age': track_info['frame_count']
            })

        # 移除离开的目标
        for track_id in to_remove:
            del self.active_tracks[track_id]
            if track_id in self.feature_vectors:
                del self.feature_vectors[track_id]

        # 新目标进入
        new_object_prob = 0.15 if self.is_hotspot else 0.05
        if len(self.active_tracks) < self.base_object_count * 2 and random.random() < new_object_prob:
            track_id = f"{self.camera_id}-T{self.track_counter:06d}"
            self.track_counter += 1
            obj_type = random.choice(self.OBJECT_TYPES)

            # 初始化 Re-ID 特征向量 (512维浮点数)
            self.feature_vectors[track_id] = [random.random() for _ in range(512)]

            self.active_tracks[track_id] = {
                'object_type': obj_type,
                'x': random.uniform(0, 1920),
                'y': random.uniform(0, 1080),
                'width': random.uniform(50, 200),
                'height': random.uniform(100, 300),
                'confidence': random.uniform(0.7, 0.95),
                'first_seen': current_time,
                'last_seen': current_time,
                'frame_count': 1
            }
            # ... (保持原输出逻辑)

            detections.append({
                'track_id': track_id,
                'object_type': obj_type,
                'bbox': [
                    int(self.active_tracks[track_id]['x']),
                    int(self.active_tracks[track_id]['y']),
                    int(self.active_tracks[track_id]['width']),
                    int(self.active_tracks[track_id]['height'])
                ],
                'confidence': self.active_tracks[track_id]['confidence'],
                'age': 1
            })

        return detections

    def detect_anomaly(self, detections: List[Dict]) -> Optional[Dict]:
        """检测异常事件"""
        # 人群聚集
        persons = [d for d in detections if d['object_type'] == 'person']
        if len(persons) > 10:
            return {
                'type': 'crowd_detected',
                'severity': 'medium',
                'count': len(persons)
            }

        # 停留时间过长（可疑）
        for track_id, track_info in self.active_tracks.items():
            if track_info['frame_count'] > 300:  # ~20秒 @ 15fps
                velocity = (
                    (track_info['x'] - track_info.get('init_x', track_info['x'])) ** 2 +
                    (track_info['y'] - track_info.get('init_y', track_info['y'])) ** 2
                ) ** 0.5

                if velocity < 50:  # 几乎没移动
                    return {
                        'type': 'loitering_detected',
                        'severity': 'low',
                        'track_id': track_id,
                        'duration_sec': track_info['frame_count'] / 15
                    }

        # 非法停车（车辆长时间静止）
        cars = [d for d in detections if d['object_type'] in ['car', 'truck']]
        for car in cars:
            track_id = car['track_id']
            if track_id in self.active_tracks:
                track_info = self.active_tracks[track_id]
                if track_info['frame_count'] > 600:  # ~40秒
                    return {
                        'type': 'illegal_parking',
                        'severity': 'high',
                        'track_id': track_id
                    }

        return None


class SurveillanceAnalyticsBench:
    """视频监控分析基准测试"""

    def __init__(self, args):
        self.args = args
        self.redis_client = redis.Redis(
            host=args.redis_host,
            port=args.redis_port,
            decode_responses=False,  # 二进制数据
            socket_timeout=5,
            socket_connect_timeout=5
        )

        # 创建摄像头追踪器
        num_hotspots = max(1, int(args.cameras * args.hotspot_ratio))
        self.trackers = []

        for i in range(args.cameras):
            is_hotspot = i < num_hotspots
            camera_id = f"cam-{i:03d}-{'hotspot' if is_hotspot else 'normal'}"
            self.trackers.append(ObjectTracker(camera_id, is_hotspot))

        self.stats = {
            'frames_processed': 0,
            'detections_total': 0,
            'anomalies_detected': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'latencies': [],
            'errors': 0
        }
        self.stop_flag = threading.Event()
        self.lock = threading.Lock()

    def _frame_hash(self, camera_id: str, detections: List[Dict]) -> str:
        """计算帧的哈希（用于缓存查询）"""
        # 简化版：基于目标位置和类型
        content = f"{camera_id}:"
        for det in sorted(detections, key=lambda x: x['track_id']):
            content += f"{det['object_type']}-{det['bbox']}"

        return hashlib.md5(content.encode()).hexdigest()

    def worker(self, thread_id: int):
        """工作线程"""
        local_stats = {
            'frames': 0, 'detections': 0, 'anomalies': 0,
            'cache_hits': 0, 'cache_misses': 0,
            'errors': 0, 'latencies': []
        }

        # 分配摄像头给线程
        tracker_indices = list(range(len(self.trackers)))
        random.shuffle(tracker_indices)
        my_trackers = [
            self.trackers[i] for i in tracker_indices
            if i % self.args.threads == thread_id
        ]

        logger.info(f"线程 {thread_id}: 处理 {len(my_trackers)} 路摄像头")

        frame_interval = 1.0 / self.args.fps

        while not self.stop_flag.is_set():
            for tracker in my_trackers:
                start = time.perf_counter()

                try:
                    # 生成检测结果
                    detections = tracker.generate_frame_detections()
                    frame_id = f"{tracker.camera_id}-{int(time.time()*1000)}"

                    # 计算帧哈希
                    frame_hash = self._frame_hash(tracker.camera_id, detections)

                    # 查询缓存（避免重复推理）
                    cache_key = f"surveillance:cache:{frame_hash}"
                    cached = self.redis_client.get(cache_key)

                    if cached:
                        local_stats['cache_hits'] += 1
                        # 使用缓存结果
                        detections = json.loads(cached.decode())
                    else:
                        local_stats['cache_misses'] += 1
                        # 写入缓存（TTL 10秒）
                        self.redis_client.setex(
                            cache_key,
                            self.args.cache_ttl,
                            json.dumps(detections)
                        )

                    # 更新目标追踪状态（Hash）
                    for det in detections:
                        track_key = f"surveillance:track:{det['track_id']}"
                        self.redis_client.hset(track_key, mapping={
                            'camera': tracker.camera_id,
                            'object_type': det['object_type'],
                            'bbox': json.dumps(det['bbox']),
                            'confidence': f"{det['confidence']:.3f}",
                            'last_update': int(time.time() * 1000),
                            'age': det['age']
                        })
                        # 设置过期时间
                        self.redis_client.expire(track_key, self.args.track_duration)

                    # 更新近期追踪历史 (用于计算追踪连续性, 模拟 100 帧状态依赖)
                    history_key = f"surveillance:history:{tracker.camera_id}"
                    track_ids = [det['track_id'] for det in detections]
                    if track_ids:
                        self.redis_client.lpush(history_key, json.dumps({
                            'ts': int(time.time() * 1000),
                            'ids': track_ids
                        }))
                        self.redis_client.ltrim(history_key, 0, 99)

                    # 检测异常事件
                    anomaly = tracker.detect_anomaly(detections)
                    if anomaly:
                        # 写入告警队列（Sorted Set）
                        alert_key = "surveillance:alerts"
                        alert_data = {
                            'camera': tracker.camera_id,
                            'frame_id': frame_id,
                            'timestamp': int(time.time() * 1000),
                            'anomaly': anomaly,
                            'detections': detections
                        }
                        self.redis_client.zadd(
                            alert_key,
                            {json.dumps(alert_data): time.time()}
                        )
                        local_stats['anomalies'] += 1

                    # 保留最近N帧检测结果（List）
                    recent_key = f"surveillance:recent:{tracker.camera_id}"
                    self.redis_client.lpush(
                        recent_key,
                        json.dumps({
                            'frame_id': frame_id,
                            'timestamp': int(time.time() * 1000),
                            'detections': detections
                        })
                    )
                    self.redis_client.ltrim(recent_key, 0, 99)  # 保留最近100帧

                    latency = (time.perf_counter() - start) * 1000
                    local_stats['latencies'].append(latency)
                    local_stats['frames'] += 1
                    local_stats['detections'] += len(detections)

                except Exception as e:
                    logger.debug(f"线程 {thread_id} 错误: {e}")
                    local_stats['errors'] += 1

                time.sleep(frame_interval)

        # 合并统计
        with self.lock:
            self.stats['frames_processed'] += local_stats['frames']
            self.stats['detections_total'] += local_stats['detections']
            self.stats['anomalies_detected'] += local_stats['anomalies']
            self.stats['cache_hits'] += local_stats['cache_hits']
            self.stats['cache_misses'] += local_stats['cache_misses']
            self.stats['errors'] += local_stats['errors']
            self.stats['latencies'].extend(local_stats['latencies'])

    def run(self):
        """运行基准测试"""
        logger.info(f"启动视频监控分析基准测试:")
        logger.info(f"  摄像头数量: {self.args.cameras}")
        logger.info(f"  帧率: {self.args.fps} FPS")
        logger.info(f"  热点比例: {self.args.hotspot_ratio * 100:.0f}%")
        logger.info(f"  线程数: {self.args.threads}")
        logger.info(f"  运行时长: {self.args.duration} 秒")

        # 清理旧数据
        try:
            for key in self.redis_client.scan_iter(b"surveillance:*"):
                self.redis_client.delete(key)
            logger.info("清理旧数据完成")
        except Exception as e:
            logger.warning(f"清理失败: {e}")

        # 启动工作线程
        threads = []
        start_time = time.time()

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
                    fps_total = self.stats['frames_processed'] / elapsed if elapsed > 0 else 0
                    cache_hit_rate = (
                        self.stats['cache_hits'] /
                        (self.stats['cache_hits'] + self.stats['cache_misses']) * 100
                        if self.stats['cache_hits'] + self.stats['cache_misses'] > 0 else 0
                    )
                    logger.info(
                        f"进度: {elapsed:.1f}s | 帧数: {self.stats['frames_processed']} | "
                        f"FPS: {fps_total:.1f} | 缓存命中率: {cache_hit_rate:.1f}% | "
                        f"异常: {self.stats['anomalies_detected']}"
                    )
        except KeyboardInterrupt:
            logger.info("用户中断")

        # 停止线程
        self.stop_flag.set()
        for t in threads:
            t.join()

        # 打印结果
        self.print_results(time.time() - start_time)

    def print_results(self, elapsed: float):
        """打印测试结果"""
        print("\n" + "="*60)
        print("视频监控分析基准测试结果")
        print("="*60)

        frames = self.stats['frames_processed']
        detections = self.stats['detections_total']
        anomalies = self.stats['anomalies_detected']
        cache_hits = self.stats['cache_hits']
        cache_misses = self.stats['cache_misses']
        errors = self.stats['errors']
        latencies = self.stats['latencies']

        print(f"运行时长:          {elapsed:.2f} 秒")
        print(f"处理帧数:          {frames:,}")
        print(f"检测目标总数:      {detections:,}")
        print(f"平均目标/帧:       {detections/frames:.2f}" if frames > 0 else "N/A")
        print(f"异常事件数:        {anomalies:,}")
        print(f"总帧率 (FPS):      {frames/elapsed:.2f}")
        print(f"缓存命中率:        {cache_hits/(cache_hits+cache_misses)*100:.2f}%" if cache_hits+cache_misses > 0 else "N/A")
        print(f"错误数:            {errors}")

        if latencies:
            print(f"\n延迟统计 (ms):")
            print(f"  平均值:          {statistics.mean(latencies):.3f}")
            print(f"  中位数:          {statistics.median(latencies):.3f}")
            sorted_lat = sorted(latencies)
            print(f"  P90:             {sorted_lat[int(len(sorted_lat)*0.9)]:.3f}")
            print(f"  P99:             {sorted_lat[int(len(sorted_lat)*0.99)]:.3f}")
            print(f"  最大值:          {max(latencies):.3f}")

        print("="*60)


def main():
    parser = argparse.ArgumentParser(description="视频监控分析边缘负载")
    parser.add_argument('--redis-host', default='127.0.0.1', help='Redis主机')
    parser.add_argument('--redis-port', type=int, default=6379, help='Redis端口')
    parser.add_argument('--threads', type=int, default=4, help='工作线程数')
    parser.add_argument('--duration', type=int, default=60, help='运行时长（秒）')
    parser.add_argument('--cameras', type=int, default=32, help='摄像头数量')
    parser.add_argument('--fps', type=int, default=15, help='每路帧率')
    parser.add_argument('--hotspot-ratio', type=float, default=0.2, help='热点摄像头比例（0-1）')
    parser.add_argument('--cache-ttl', type=int, default=10, help='缓存TTL（秒）')
    parser.add_argument('--track-duration', type=int, default=10, help='追踪状态保留时长（秒）')

    args = parser.parse_args()

    bench = SurveillanceAnalyticsBench(args)
    bench.run()


if __name__ == '__main__':
    main()
