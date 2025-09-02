#!/usr/bin/env python3

# InfluxDB基准测试客户端脚本
# 支持并发客户端测试、延迟测量和吞吐量评估
# 与chk_restore.py迁移测试结合使用

import random
import time
from argparse import ArgumentParser
import concurrent.futures
import statistics
from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.domain.write_precision import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

def write_worker(tid, url, token, org, bucket, points_per_thread, field_count, tag_count, latencies):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    local_latencies = []

    points = []
    for i in range(points_per_thread):
        point = f"benchmark_measurement,thread={tid}"

        # 添加标签
        tags = []
        for j in range(tag_count):
            tags.append(f"tag{j}={random.randint(1, 1000)}")
        if tags:
            point += "," + ",".join(tags)

        # 添加字段
        fields = []
        for j in range(field_count):
            fields.append(f"field{j}={random.uniform(0, 100)}")
        point += " " + ",".join(fields)

        # 添加时间戳
        point += f" {int(time.time() * 1000000000)}"
        points.append(point)

    # 批量写入测量延迟
    start_time = time.time()
    write_api.write(bucket, org, "\n".join(points))
    end_time = time.time()

    total_time = end_time - start_time
    avg_latency = total_time / len(points) if points else 0
    throughput = len(points) / total_time if total_time > 0 else 0

    latencies.append(avg_latency)
    return len(points), total_time, avg_latency, throughput

def read_worker(tid, url, token, org, bucket, queries_per_thread, latencies):
    client = InfluxDBClient(url=url, token=token, org=org)
    query_api = client.query_api()

    local_latencies = []
    query = f'''
    from(bucket: "{bucket}")
    |> range(start: -1h)
    |> filter(fn: (r) => r["_measurement"] == "benchmark_measurement")
    |> filter(fn: (r) => r["thread"] == "{tid}")
    |> limit(n: 100)
    '''

    for i in range(queries_per_thread):
        start_time = time.time()
        result = query_api.query(query, org)
        end_time = time.time()

        latency = end_time - start_time
        local_latencies.append(latency)

    latencies.extend(local_latencies)

def main():
    parser = ArgumentParser(description='InfluxDB 基准测试客户端')
    parser.add_argument('--threads', type=int, default=10, help='并发线程数')
    parser.add_argument('--operations', type=int, default=1000, help='每个线程操作数')
    parser.add_argument('--url', default='http://localhost:8086', help='InfluxDB URL')
    parser.add_argument('--token', default='my-super-secret-auth-token', help='认证token')
    parser.add_argument('--org', default='my-org', help='组织名')
    parser.add_argument('--bucket', default='benchmark', help='bucket名')
    parser.add_argument('--field-count', type=int, default=5, help='每点字段数')
    parser.add_argument('--tag-count', type=int, default=3, help='每点标签数')
    parser.add_argument('--test-mode', choices=['write', 'read', 'mixed'], default='write', help='测试模式：write 只写，read 只读，mixed 混合')

    args = parser.parse_args()

    print(f'开始InfluxDB基准测试: threads={args.threads} operations/thread={args.operations} mode={args.test_mode}')

    latencies = []
    total_operations = 0
    total_time = 0

    if args.test_mode in ['write', 'mixed']:
        print("执行写入基准测试...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(write_worker, tid, args.url, args.token, args.org, args.bucket,
                                       args.operations, args.field_count, args.tag_count, latencies)
                futures.append(future)

            for future in futures:
                try:
                    ops, time_taken, avg_lat, throughput = future.result()
                    total_operations += ops
                    total_time += time_taken
                    print('.3f', '.1f')
                except Exception as e:
                    print(f'线程错误: {e}')

    if args.test_mode in ['read', 'mixed']:
        print("执行读取基准测试...")
        latencies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(read_worker, tid, args.url, args.token, args.org, args.bucket,
                                       args.operations, latencies)
                futures.append(future)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f'读取线程错误: {e}')

        total_operations += args.operations * args.threads

    # 计算整体统计
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = statistics.median(latencies + [latencies[0]])  # 近似p95

        throughput = total_operations / total_time if total_time > 0 else 0

        print("\n=== InfluxDB基准测试结果 ===")
        print(".3f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".3f")
    else:
        print("无延迟数据，无法计算统计信息")

    print("测试完成，可与chk_restore.py迁移测试结合分析迁移期间性能影响。")

if __name__ == '__main__':
    main()