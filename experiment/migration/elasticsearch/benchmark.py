#!/usr/bin/env python3

# Elasticsearch基准测试客户端脚本
# 支持并发客户端测试、延迟测量和吞吐量评估
# 与chk_restore.py迁移测试结合使用

import random
import time
import string
from argparse import ArgumentParser
import concurrent.futures
import statistics
from elasticsearch import Elasticsearch

def generate_random_text(size):
    """生成随机文本"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def random_title():
    """生成随机标题"""
    words = ['test', 'document', 'index', 'search', 'benchmark', 'performance', 'data', 'query']
    return ' '.join(random.sample(words, 3))

def index_worker(tid, es_client, index_name, documents_per_thread, field_count, latencies):
    """索引文档worker"""
    operations = []

    for i in range(documents_per_thread):
        doc_id = f"{tid}-{i}"
        document = {
            'title': random_title(),
            'content': generate_random_text(512),
            'thread': tid,
            'timestamp': int(time.time() * 1000)
        }

        # 添加额外字段
        for j in range(field_count):
            document[f'field{j}'] = random.randint(0, 1000)

        operations.append({
            '_index': index_name,
            '_id': doc_id,
            '_source': document
        })

    # 批量索引
    start_time = time.time()

    from elasticsearch.helpers import bulk
    bulk(es_client, operations)

    end_time = time.time()

    total_time = end_time - start_time
    avg_latency = total_time / len(operations) if operations else 0
    throughput = len(operations) / total_time if total_time > 0 else 0

    latencies.append(avg_latency)
    return len(operations), total_time, avg_latency, throughput

def search_worker(tid, es_client, index_name, searches_per_thread, latencies, field_count):
    """搜索worker"""
    for i in range(searches_per_thread):
        # 构建随机查询
        query_type = random.choice(['wildcard', 'term', 'match_phrase'])

        if query_type == 'wildcard':
            query = {
                'wildcard': {
                    'title': {
                        'value': 'test*'
                    }
                }
            }
        elif query_type == 'term':
            query = {
                'term': {
                    'thread': tid
                }
            }
        else:
            query = {
                'match_phrase': {
                    'content': random_title()
                }
            }

        search_query = {
            "query": {
                "bool": {
                    "must": [
                        query,
                        {"range": {"timestamp": {"gte": int(time.time() * 1000) - 36000000}}}  # 最近1小时
                    ]
                }
            }
        }

        start_time = time.time()
        response = es_client.search(index=index_name, body=search_query)
        end_time = time.time()

        latency = end_time - start_time
        latencies.append(latency)

def main():
    parser = ArgumentParser(description='Elasticsearch 基准测试客户端')
    parser.add_argument('--threads', type=int, default=10, help='并发线程数')
    parser.add_argument('--operations', type=int, default=1000, help='每个线程操作数')
    parser.add_argument('--es-host', default='localhost', help='Elasticsearch主机')
    parser.add_argument('--es-port', type=int, default=9200, help='Elasticsearch端口')
    parser.add_argument('--index-name', default='benchmark-test', help='索引名称')
    parser.add_argument('--field-count', type=int, default=5, help='每个文档额外字段数')
    parser.add_argument('--test-mode', choices=['index', 'search', 'mixed'], default='index', help='测试模式：index 只索引，search 只搜索，mixed 混合')

    args = parser.parse_args()

    # 连接ES
    es = Elasticsearch([{'host': args.es_host, 'port': args.es_port, 'scheme': 'http'}])

    # 检查连接
    if not es.ping():
        print(f"错误: 无法连接到Elasticsearch {args.es_host}:{args.es_port}")
        exit(1)

    # 创建索引（如果不存在）
    if not es.indices.exists(index=args.index_name):
        es.indices.create(index=args.index_name)

    print(f'开始Elasticsearch基准测试: threads={args.threads} operations/thread={args.operations} mode={args.test_mode}')

    latencies = []
    total_operations = 0
    total_time = 0

    if args.test_mode in ['index', 'mixed']:
        print("执行索引基准测试...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(index_worker, tid, es, args.index_name, args.operations,
                                       args.field_count, latencies)
                futures.append(future)

            for future in futures:
                try:
                    ops, time_taken, avg_lat, throughput = future.result()
                    total_operations += ops
                    total_time += time_taken
                    print('.3f', '.1f')
                except Exception as e:
                    print(f'索引线程错误: {e}')

    if args.test_mode in ['search', 'mixed']:
        print("执行搜索基准测试...")
        latencies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(search_worker, tid, es, args.index_name, args.operations,
                                       latencies, args.field_count)
                futures.append(future)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f'搜索线程错误: {e}')

        total_operations += args.operations * args.threads

    # 计算统计
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 10 else max(latencies)  # 95th percentile

        print("\n=== Elasticsearch基准测试结果 ===")
        print(".3f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".6f")
        print(".3f")
    else:
        print("无延迟数据，无法计算统计信息")

    # 清理索引（可选）
    if input("是否删除测试索引? (y/N): ").lower() == 'y':
        es.indices.delete(index=args.index_name)
        print("测试索引已删除")

    print("测试完成，可与chk_restore.py迁移测试结合分析迁移期间性能影响。")

if __name__ == '__main__':
    main()