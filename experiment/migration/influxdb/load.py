#!/usr/bin/env python3

# InfluxDB负载生成客户端
# 使用Python influxdb-client库生成可配置的时间序列负载

import random
import time
from argparse import ArgumentParser
import concurrent.futures
from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.domain.write_precision import WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

def worker(tid, url, token, org, bucket, points_per_thread, field_count, tag_count, delay, measurement_name):
    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for i in range(points_per_thread):
        point = f"{measurement_name}"

        # 添加标签
        tags = []
        for j in range(tag_count):
            tags.append(f"tag{j}={tid}_{random.randint(1, 1000)}")
        if tags:
            point += "," + ",".join(tags)

        # 添加字段
        fields = []
        for j in range(field_count):
            fields.append(f"field{j}={random.uniform(0, 100)}")
        point += " " + ",".join(fields)

        # 添加时间戳
        point += f" {int(time.time() * 1000000000)}"

        write_api.write(bucket, org, point)

        if delay > 0:
            time.sleep(delay / 1000.0)  # ms to s

def main():
    parser = ArgumentParser(description='InfluxDB 可配置时间序列负载生成客户端')
    parser.add_argument('--threads', type=int, default=10, help='并发线程数')
    parser.add_argument('--points', type=int, default=1000, help='每个线程写入的点数')
    parser.add_argument('--url', default='http://localhost:8086', help='InfluxDB URL')
    parser.add_argument('--token', default='my-super-secret-auth-token', help='认证token')
    parser.add_argument('--org', default='my-org', help='组织名')
    parser.add_argument('--bucket', default='benchmark', help='bucket名')
    parser.add_argument('--field-count', type=int, default=5, help='每点字段数（内容复杂性）')
    parser.add_argument('--tag-count', type=int, default=3, help='每点标签数')
    parser.add_argument('--delay', type=int, default=0, help='写入间延迟(ms)')
    parser.add_argument('--measurement', default='test_measurement', help='测量名')
    parser.add_argument('--continuous', action='store_true', help='连续运行模式')

    args = parser.parse_args()

    print(f'开始InfluxDB负载生成: threads={args.threads} points/thread={args.points} fields={args.field_count}')

    while True:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(worker, tid, args.url, args.token, args.org, args.bucket, args.points, args.field_count, args.tag_count, args.delay, args.measurement)
                futures.append(future)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f'线程错误: {e}')

        print(f'完成一轮 {args.threads} 线程 x {args.points} 点')

        if not args.continuous:
            break

if __name__ == '__main__':
    main()