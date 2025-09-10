#!/usr/bin/env python3
# coding: utf-8
"""
bench_sensoragg_distributed.py - Distributed Sensor Network Benchmark for InfluxDB

全新的分布式传感器网络基准测试，完全不同的数据模型和访问模式
提供与Vehicle Telematics脚本更大的差异性

FEATURES:
   - 数据模型完全重新设计: 分布式传感器网络，网状拓扑结构
   - 访问模式转型: 从单一设备传感器转向网络拓扑关系查询
   - 复合测量支持: 网络通讯状态，邻居关系，网络分析等多种测量类型

USAGE:
   python3 bench_sensoragg_distributed.py --influx-url http://localhost:8181 --threads 8 --duration 30
   python3 bench_sensoragg_distributed.py --influx-url http://localhost:8181 --network-topology mesh --sensor-density 10

EXTENDED USAGE:
   --network-topology: mesh/star/tree/hierarchical (default: mesh)
   --sensor-density: 传感网密度因子 (default: 5)
   --communication-intervals: 通讯间隔秒数 (default: 30)
   --data-propagation-depth: 数据传播深度 (default: 2)
"""

import argparse
import json
import logging
import random
import threading
import time
import statistics
from typing import List, Optional, Dict, Any, Set
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(handler)


class SensorNetworkBench:
    """分布式传感器网络InfluxDB基准测试"""
    def __init__(self, influx_url: str, token: str, org: str, bucket: str = "sensor-network",
                 # 网络结构配置
                 network_topology: str = "mesh",
                 sensor_density: int = 5,
                 # 数据传播配置
                 communication_intervals: int = 30,
                 data_propagation_depth: int = 2,
                 # 数据规模扩展
                 payload_size_kb: float = 1.0):
        self.influx_url = influx_url
        self.token = token
        self.org = org
        self.bucket = bucket
        self._stop = threading.Event()

        # 初始化统计相关属性
        self.lock = threading.Lock()
        self.latencies_ms = []
        self.success = 0
        self.fail = 0

        # 网络结构配置
        self.network_topology = network_topology
        self.sensor_density = sensor_density
        self.communication_intervals = communication_intervals
        self.data_propagation_depth = data_propagation_depth

        # 数据规模
        self.payload_size_kb = payload_size_kb

        # 网络状态
        self.network_nodes: Dict[str, Dict[str, Any]] = {}
        self.node_connections: Dict[str, Set[str]] = {}
        self.propagation_queues: Dict[str, List[Dict]] = {}

        # 初始化基础网络结构
        self._initialize_network_topology()

        # InfluxDB客户端
        self.client = InfluxDBClient(url=influx_url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
        self.query_api = self.client.query_api()

    def _initialize_network_topology(self):
        """初始化网络拓扑结构"""
        if self.network_topology == "mesh":
            # 网状网络：每个节点与多个邻居连接
            nodes_count = self.sensor_density * 10
            for i in range(nodes_count):
                node_id = f"sensor_node_{i:03d}"
                self.network_nodes[node_id] = {
                    "location": [random.uniform(30.0, 32.0), random.uniform(120.0, 122.0)],
                    "energy_level": random.uniform(70, 100),
                    "last_broadcast": 0,
                    "role": random.choice(["coordinator", "router", "sensor"])
                }
                # 每个节点连接3-8个随机邻居
                neighbors = set()
                for _ in range(random.randint(3, 8)):
                    neighbor = random.choice(list(self.network_nodes.keys() - {node_id, *neighbors}))
                    neighbors.add(neighbor)
                self.node_connections[node_id] = neighbors

        elif self.network_topology == "tree":
            # 树状网络：层级结构
            nodes_count = self.sensor_density * 8
            coordinators = []
            # 第一层：协调器
            for i in range(max(1, self.sensor_density // 2)):
                node_id = f"coord_{i:02d}"
                self.network_nodes[node_id] = {
                    "location": [31.0 + random.uniform(-0.5, 0.5), 121.0 + random.uniform(-0.5, 0.5)],
                    "energy_level": random.uniform(85, 100),
                    "last_broadcast": 0,
                    "role": "coordinator"
                }
                coordinators.append(node_id)

            # 第二层：路由器
            for i in range(max(2, self.sensor_density * 2)):
                node_id = f"router_{i:02d}"
                coordinator = random.choice(coordinators)
                self.network_nodes[node_id] = {
                    "location": [31.0 + random.uniform(-0.3, 0.3), 121.0 + random.uniform(-0.3, 0.3)],
                    "energy_level": random.uniform(80, 95),
                    "last_broadcast": 0,
                    "role": "router",
                    "parent": coordinator
                }
                self.node_connections[node_id] = {coordinator}

            # 第三层：传感器
            for i in range(max(5, (nodes_count - len(self.network_nodes)))):
                node_id = f"sensor_{i:02d}"
                router = random.choice([n for n in self.network_nodes if self.network_nodes[n]["role"] == "router"])
                self.network_nodes[node_id] = {
                    "location": [31.0 + random.uniform(-0.2, 0.2), 121.0 + random.uniform(-0.2, 0.2)],
                    "energy_level": random.uniform(70, 90),
                    "last_broadcast": 0,
                    "role": "sensor",
                    "parent": router
                }
                self.node_connections[node_id] = {router}

        # 初始化传播队列
        for node_id in self.network_nodes:
            self.propagation_queues[node_id] = []

    def _simulate_sensor_reading(self, node_id: str, sensor_type: str) -> Dict[str, Any]:
        """生成传感器网络读数"""
        node = self.network_nodes[node_id]

        # 传感器状态影响读数质量
        accuracy_factor = min(1.0, node["energy_level"] / 100.0)
        noise_level = (1.0 - accuracy_factor) * 5.0

        sensor_configs = {
            "temperature": {"range": (15, 35), "unit": "celsius"},
            "humidity": {"range": (30, 80), "unit": "percent"},
            "pressure": {"range": (980, 1020), "unit": "hPa"},
            "vibration": {"range": (0, 15), "unit": "mm/s"},
            "light": {"range": (0, 100), "unit": "lux"},
            "soil_moisture": {"range": (0, 100), "unit": "percent"},
        }

        config = sensor_configs.get(sensor_type, sensor_configs["temperature"])
        base_value = config["range"][0] + random.random() * (config["range"][1] - config["range"][0])
        noise = random.gauss(0, noise_level * abs(base_value) * 0.05)
        actual_value = base_value + noise

        # 模拟传输信号强度
        signal_strength = max(0, min(100, random.gauss(85, 10)))
        transmission_quality = min(1.0, signal_strength / 100.0)

        return {
            "node_id": node_id,
            "sensor_type": sensor_type,
            "value": round(actual_value, 2),
            "unit": config["unit"],
            "signal_strength": round(signal_strength, 1),
            "transmission_quality": round(transmission_quality, 3),
            "energy_consumed": round(random.uniform(0.1, 0.5), 2),
            "location": node["location"]
        }

    def _generate_network_communication(self) -> List[Point]:
        """生成网络通讯测量数据"""
        points = []
        timestamp = int(time.time() * 1000000000)

        # 所有节点的通讯状态
        for node_id, node_state in self.network_nodes.items():
            if time.time() - node_state["last_broadcast"] >= self.communication_intervals or random.random() < 0.1:
                # 网络状态测量
                network_point = Point("network_status") \
                    .tag("node_id", node_id) \
                    .tag("role", node_state["role"]) \
                    .field("energy_level", round(node_state["energy_level"], 1)) \
                    .field("neighbor_count", len(self.node_connections.get(node_id, set()))) \
                    .time(timestamp, write_precision=WritePrecision.NS)
                points.append(network_point)

                # 数据传播跟踪
                propagation_point = Point("data_propagation") \
                    .tag("source_node", node_id) \
                    .field("propagate_depth", len(self.propagation_queues.get(node_id, []))) \
                    .time(timestamp, write_precision=WritePrecision.NS)
                points.append(propagation_point)

                node_state["last_broadcast"] = time.time()

        return points

    def _generate_sensor_measurements(self) -> List[Point]:
        """生成传感器测量数据"""
        points = []
        timestamp = int(time.time() * 1000000000)

        for node_id in self.network_nodes:
            if self.network_nodes[node_id]["role"] == "sensor":
                # 随机选择1-3种传感器类型
                active_sensors = random.sample(["temperature", "humidity", "pressure", "vibration"],
                                             random.randint(1, 3))

                for sensor_type in active_sensors:
                    reading = self._simulate_sensor_reading(node_id, sensor_type)

                    sensor_point = Point("sensor_measurement") \
                        .tag("node_id", node_id) \
                        .tag("sensor_type", sensor_type) \
                        .tag("role", "sensor") \
                        .field("value", reading["value"]) \
                        .field("signal_strength", reading["signal_strength"]) \
                        .field("transmission_quality", reading["transmission_quality"]) \
                        .field("energy_consumed", reading["energy_consumed"]) \
                        .time(timestamp, write_precision=WritePrecision.NS)

                    points.append(sensor_point)

                # 更新节点能量水平
                energy_used = sum(reading["energy_consumed"] for reading in [
                    self._simulate_sensor_reading(node_id, sensor_type) for sensor_type in active_sensors
                ])
                self.network_nodes[node_id]["energy_level"] = max(0,
                    self.network_nodes[node_id]["energy_level"] - energy_used)

        return points

    def _generate_network_flow(self) -> List[Point]:
        """生成网络数据流测量"""
        points = []
        timestamp = int(time.time() * 1000000000)

        # 模拟路由器节点的网络流量
        for node_id, node_state in self.network_nodes.items():
            if node_state["role"] in ["coordinator", "router"]:
                # 出站流量
                outgoing_point = Point("network_flow") \
                    .tag("src_node", node_id) \
                    .tag("direction", "outgoing") \
                    .field("data_rate", round(random.uniform(10, 100) * self.sensor_density, 2)) \
                    .field("packets_sent", random.randint(5, 50)) \
                    .time(timestamp, write_precision=WritePrecision.NS)

                # 入站流量
                incoming_point = Point("network_flow") \
                    .tag("dst_node", node_id) \
                    .tag("direction", "incoming") \
                    .field("data_rate", round(random.uniform(5, 80) * self.sensor_density, 2)) \
                    .field("packets_received", random.randint(3, 40)) \
                    .time(timestamp, write_precision=WritePrecision.NS)

                points.extend([outgoing_point, incoming_point])

        return points

    def _generate_network_event(self) -> List[Point]:
        """生成网络事件（故障、异常、邻居关系变化等）"""
        points = []
        timestamp = int(time.time() * 1000000000)

        # 模拟网络事件（1%的概率）
        if random.random() < 0.01:
            node_id = random.choice(list(self.network_nodes.keys()))
            event_types = ["link_failure", "node_reboot", "low_energy", "high_traffic"]

            event_point = Point("network_event") \
                .tag("affected_node", node_id) \
                .tag("event_type", random.choice(event_types)) \
                .tag("severity", "warning" if random.random() < 0.7 else "critical") \
                .field("impact_score", round(random.uniform(0.1, 1.0), 2)) \
                .time(timestamp, write_precision=WritePrecision.NS)

            points.append(event_point)

        # 邻居关系变化（更低的概率）
        if random.random() < 0.005:
            node_id = random.choice(list(self.network_nodes.keys()))
            if self.node_connections.get(node_id):
                old_neighbor = random.choice(list(self.node_connections[node_id]))
                self.node_connections[node_id].remove(old_neighbor)

                # 寻找新邻居
                possible_new = set(self.network_nodes.keys()) - {node_id} - self.node_connections[node_id]
                if possible_new:
                    new_neighbor = random.choice(list(possible_new))
                    self.node_connections[node_id].add(new_neighbor)

                    neighbor_change_point = Point("topology_change") \
                        .tag("node_id", node_id) \
                        .tag("change_type", "neighbor_update") \
                        .field("old_neighbor", old_neighbor) \
                        .field("new_neighbor", new_neighbor) \
                        .time(timestamp, write_precision=WritePrecision.NS)

                    points.append(neighbor_change_point)

        return points

    def _generate_distributed_data(self) -> List[Point]:
        """生成完整的分布式网络数据"""
        points = []

        # 通讯状态数据
        points.extend(self._generate_network_communication())

        # 传感器测量数据
        points.extend(self._generate_sensor_measurements())

        # 网络流数据（路由节点）
        points.extend(self._generate_network_flow())

        # 网络事件数据（低频）
        points.extend(self._generate_network_event())

        return points

    def _execute_network_query(self):
        """执行网络状态查询（完全不同于Vehicle的地理查询）"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -10m)
            |> filter(fn: (r) => r["_measurement"] == "network_status")
            |> group(columns: ["role"])
            |> count()
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        # 统计
        with self.lock if hasattr(self, 'lock') else threading.Lock():
            if not hasattr(self, 'latencies_ms'):
                self.latencies_ms = []
                self.success = 0
                self.fail = 0
            self.latencies_ms.append(latency)
            self.success += 1

        return len(result)

    def _execute_propagation_query(self):
        """执行数据传播查询"""
        query = f"""
            from(bucket: "{self.bucket}")
            |> range(start: -5m)
            |> filter(fn: (r) => r["_measurement"] == "data_propagation")
            |> yield(name: "propagate")
        """

        start_time = time.perf_counter()
        result = self.query_api.query(query, self.org)
        latency = (time.perf_counter() - start_time) * 1000

        with self.lock if hasattr(self, 'lock') else threading.Lock():
            if not hasattr(self, 'latencies_ms'):
                self.latencies_ms = []
                self.success = 0
                self.fail = 0
            self.latencies_ms.append(latency)
            self.success += 1

        return len(result)

    def _worker(self, duration: float, read_pct: int):
        """工作线程，支持混合读写操作"""
        end_time = time.time() + duration

        while time.time() < end_time and not self._stop.is_set():
            do_read = random.randint(1, 100) <= read_pct
            start = time.perf_counter()

            try:
                if do_read:
                    # 网络分析查询（完全不同于Vehicle的地理查询）
                    query_type = random.choice(["network_status", "propagation"])
                    if query_type == "network_status":
                        self._execute_network_query()
                    else:
                        self._execute_propagation_query()
                else:
                    # 写入分布式网络数据
                    points = self._generate_distributed_data()
                    if points:
                        self.write_api.write(bucket=self.bucket, org=self.org, record=points)
                        lat = (time.perf_counter() - start) * 1000.0

                        with self.lock if hasattr(self, 'lock') else threading.Lock():
                            if not hasattr(self, 'latencies_ms'):
                                self.latencies_ms = []
                                self.success = 0
                                self.fail = 0
                            self.latencies_ms.append(lat)
                            self.success += 1
            except Exception as e:
                logger.debug("Operation failed: %s", e)
                with self.lock if hasattr(self, 'lock') else threading.Lock():
                    if not hasattr(self, 'latencies_ms'):
                        self.latencies_ms = []
                        self.success = 0
                        self.fail = 0
                    self.fail += 1
                time.sleep(0.01)

                # 定期监控输出
                current_time = time.time()
                if current_time - getattr(self, 'last_report_time', 0) >= getattr(self, 'monitor_interval', 1.0):
                    elapsed = getattr(self, 'elapsed', 0)
                    success_count = getattr(self, 'success', 0)
                    new_operations = success_count - getattr(self, 'last_success_count', 0)
                    throughput = new_operations / getattr(self, 'monitor_interval', 1.0)

                    logger.info(f"[{elapsed:.1f}s] TPS: {throughput:.1f}")
                    setattr(self, 'last_report_time', current_time)
                    setattr(self, 'last_success_count', success_count)

    def run(self, threads: int = 4, duration: int = 10, read_pct: int = 5):
        """运行基准测试"""
        start_time = time.time()

        # 初始化监控属性
        self.monitor_interval = 1.0
        self.last_report_time = start_time
        self.last_success_count = 0
        self.elapsed = 0

        threads_list = []

        for _ in range(threads):
            t = threading.Thread(target=self._worker, args=(duration, read_pct), daemon=True)
            t.start()
            threads_list.append(t)

        logger.info("分布式传感器网络基准测试启动: %d个线程，%d秒，读取比例%d%%",
                   threads, duration, read_pct)
        logger.info("网络拓扑: %s, 传感器密度: %d",
                   self.network_topology, self.sensor_density)

        for t in threads_list:
            t.join()

        self._print_summary(duration)
        self.client.close()

    def _print_summary(self, duration: int):
        """打印测试总结"""
        total = getattr(self, 'success', 0) + getattr(self, 'fail', 0)
        ops_per_sec = getattr(self, 'success', 0) / max(1e-9, duration)
        logger.info("总操作数: %d 成功=%d 失败=%d ops/sec=%.2f",
                   total, getattr(self, 'success', 0), getattr(self, 'fail', 0), ops_per_sec)

        if hasattr(self, 'latencies_ms') and self.latencies_ms:
            lat = sorted(self.latencies_ms)
            p50 = lat[int(len(lat) * 0.5)]
            p90 = lat[int(len(lat) * 0.9)]
            p99 = lat[int(len(lat) * 0.99)]
            logger.info("延迟(ms) - 平均=%.3f p50=%.3f p90=%.3f p99=%.3f 最大=%.3f",
                       statistics.mean(lat), p50, p90, p99, lat[-1])


def main():
    parser = argparse.ArgumentParser(description="Distributed Sensor Network InfluxDB Benchmark")

    # InfluxDB连接
    parser.add_argument("--influx-url", default="http://localhost:8181", help="InfluxDB URL")
    parser.add_argument("--token", default="my-super-secret-auth-token", help="InfluxDB token")
    parser.add_argument("--org", default="my-org", help="InfluxDB org")
    parser.add_argument("--bucket", default="sensor-network", help="InfluxDB bucket")

    # 网络结构配置
    parser.add_argument("--network-topology", default="mesh", type=str,
                       choices=["mesh", "tree", "star"], help="网络拓扑类型")
    parser.add_argument("--sensor-density", default=5, type=int, help="传感网络密度因子")

    # 数据规模和通讯配置
    parser.add_argument("--payload-size-kb", default=1.0, type=float, help="目标负载大小(KB)")
    parser.add_argument("--communication-intervals", default=20, type=int, help="通讯间隔(秒)")
    parser.add_argument("--data-propagation-depth", default=2, type=int, help="数据传播深度")

    # 工作负载参数
    parser.add_argument("--threads", default=4, type=int, help="工作线程数")
    parser.add_argument("--duration", default=120, type=int, help="测试持续时间(秒)")
    parser.add_argument("--read-pct", default=10, type=int, help="读取操作比例(%)")

    args = parser.parse_args()

    bench = SensorNetworkBench(
        influx_url=args.influx_url,
        token=args.token,
        org=args.org,
        bucket=args.bucket,
        # 网络结构配置
        network_topology=args.network_topology,
        sensor_density=args.sensor_density,
        # 数据规模配置
        payload_size_kb=args.payload_size_kb,
        communication_intervals=args.communication_intervals,
        data_propagation_depth=args.data_propagation_depth
    )

    bench.run(threads=args.threads, duration=args.duration, read_pct=args.read_pct)


if __name__ == "__main__":
    main()