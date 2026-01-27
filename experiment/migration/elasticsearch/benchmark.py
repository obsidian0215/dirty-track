#!/usr/bin/env python3

# Elasticsearch基准测试客户端脚本
# 支持并发客户端测试、延迟测量和吞吐量评估
# 与chk_restore.py迁移测试结合使用

import random
import time
import string
import os
from argparse import ArgumentParser
import concurrent.futures
import statistics
from elasticsearch import Elasticsearch

# Dynamic bench_common import (for IntervalMetrics)
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
        _cur = _os.path.dirname(_cur)
    bench_common = _bench_common
except Exception:
    bench_common = None

IntervalMetrics = getattr(bench_common, 'IntervalMetrics', None) if bench_common else None

PAYLOAD_SIZE = 512

def generate_random_text(size=None):
    """生成随机文本（默认使用全局 PAYLOAD_SIZE）"""
    if size is None:
        size = PAYLOAD_SIZE
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

def random_title():
    """生成随机标题"""
    words = ['test', 'document', 'index', 'search', 'benchmark', 'performance', 'data', 'query']
    return ' '.join(random.sample(words, 3))

def index_worker(tid, es_client, index_name, documents_per_thread, field_count, latencies, metrics=None):
    """索引文档worker：按批次提交并做速率限制。

    参数说明：
    - documents_per_thread: 脚本层面的“操作数”参数（可能等于 requests_per_thread*bulk_size 或直接由 --operations 指定）
    为了支持把 --rps 解释为 HTTP requests/sec，我们将 documents_per_thread 当作线程总文档数，
    并根据 --bulk-size 将其分成多次 bulk 请求；每次 bulk 后按每线程目标间隔节流。
    """
    from elasticsearch.helpers import bulk
    import statistics as _statistics

    # Determine actual bulk size from main args if available
    actual_bulk = getattr(__import__('sys').modules[__name__], 'ARG_BULK_SIZE', None)
    if actual_bulk is None:
        actual_bulk = getattr(__import__('sys').modules[__name__], 'DEFAULT_BULK_SIZE', 100)
    try:
        actual_bulk = int(actual_bulk)
    except Exception:
        actual_bulk = 100

    docs = int(documents_per_thread)
    if docs <= 0:
        return 0, 0, 0, 0

    bulk_count = (docs + actual_bulk - 1) // actual_bulk

    # Determine per-thread rps interval if available (main stored in MAIN_ARGS)
    per_thread_interval = None
    try:
        main_args = getattr(__import__('sys').modules[__name__], 'MAIN_ARGS', None)
        if main_args and getattr(main_args, '_requests_per_thread', None):
            per_thread_rps = float(main_args.rps) / max(1.0, float(main_args.threads)) if getattr(main_args, 'rps', None) else None
            if per_thread_rps and per_thread_rps > 0:
                per_thread_interval = 1.0 / per_thread_rps
    except Exception:
        per_thread_interval = None

    total_docs_indexed = 0
    total_time = 0.0
    lat_samples = []

    for b in range(bulk_count):
        this_batch_size = actual_bulk if (b < bulk_count - 1) else (docs - actual_bulk * (bulk_count - 1))
        operations = []
        for i in range(this_batch_size):
            idx = b * actual_bulk + i
            doc_id = f"{tid}-{idx}"
            document = {
                'title': random_title(),
                'content': generate_random_text(),
                'thread': tid,
                'timestamp': int(time.time() * 1000)
            }
            for j in range(field_count):
                document[f'field{j}'] = random.randint(0, 1000)
            operations.append({'_index': index_name, '_id': doc_id, '_source': document})

        start_time = time.time()
        success = True
        try:
            bulk(es_client, operations)
        except Exception:
            # ignore individual bulk failures
            success = False
        end_time = time.time()

        elapsed = end_time - start_time
        total_time += elapsed
        total_docs_indexed += len(operations)
        if len(operations) > 0:
            lat_samples.append(elapsed / len(operations))
        if metrics:
            metrics.record(success, elapsed * 1000.0)

        # Rate limiting: sleep to maintain per-thread request interval (if available)
        if per_thread_interval:
            to_sleep = per_thread_interval - elapsed
            if to_sleep > 0:
                time.sleep(to_sleep)

    avg_latency = _statistics.mean(lat_samples) if lat_samples else 0
    throughput = (total_docs_indexed / total_time) if total_time > 0 else 0
    latencies.append(avg_latency)
    return total_docs_indexed, total_time, avg_latency, throughput

def search_worker(tid, es_client, index_name, searches_per_thread, latencies, field_count, metrics=None):
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
        success = True
        try:
            response = es_client.search(index=index_name, body=search_query)
        except Exception:
            success = False
        end_time = time.time()

        latency = end_time - start_time
        latencies.append(latency)
        if metrics:
            metrics.record(success, latency * 1000.0)

def main():
    parser = ArgumentParser(description='Elasticsearch 基准测试客户端')
    parser.add_argument('--threads', type=int, default=10, help='并发线程数')
    parser.add_argument('--operations', type=int, default=1000, help='每个线程操作数')
    # Compatibility flags used by the matrix harness
    parser.add_argument('--rps', type=int, default=None, help='目标 HTTP 请求数/秒（全局, optional; 与 --duration 一起使用）')
    parser.add_argument('--duration', type=int, default=None, help='运行时长（秒），与 --rps 一起使用以计算请求总数）')
    parser.add_argument('--payload-size', dest='payload_size', default=None, help='每个文档的 payload 大小（字节，兼容调用）')
    parser.add_argument('--bulk-size', dest='bulk_size', type=int, default=100, help='索引时每个 bulk 请求包含的文档数')
    parser.add_argument('--es-host', default='localhost', help='Elasticsearch主机')
    parser.add_argument('--es-port', type=int, default=9200, help='Elasticsearch端口')
    parser.add_argument('--index-name', default='benchmark-test', help='索引名称')
    parser.add_argument('--field-count', type=int, default=5, help='每个文档额外字段数')
    parser.add_argument('--test-mode', choices=['index', 'search', 'mixed'], default='index', help='测试模式：index 只索引，search 只搜索，mixed 混合')
    parser.add_argument('--metrics-out', default=None, help='Output path for interval metrics (JSON)')
    parser.add_argument('--metrics-interval', type=float, default=1.0, help='Sampling interval seconds (default: 1.0)')

    args = parser.parse_args()

    # Expose args to workers via module-level reference
    # MAIN_ARGS used by workers to compute per-thread intervals; ARG_BULK_SIZE used to set bulk size
    import sys as _sys
    _sys.modules[__name__].MAIN_ARGS = args
    _sys.modules[__name__].ARG_BULK_SIZE = args.bulk_size
    _sys.modules[__name__].DEFAULT_BULK_SIZE = 100

    # Map compatibility flags to internal parameters
    global PAYLOAD_SIZE
    try:
        if args.rps is not None and args.duration is not None:
            # Interpret rps as HTTP requests/sec (global). Compute total requests
            total_requests = int(args.rps) * int(args.duration)
            # Per-thread requests (integer division)
            requests_per_thread = max(1, total_requests // max(1, args.threads))
            # Map to operations per thread depending on test mode:
            # - index: each HTTP request is a bulk of --bulk-size docs -> operations = requests_per_thread * bulk_size
            # - search: each HTTP request is one search -> operations = requests_per_thread
            if args.test_mode == 'search':
                args.operations = requests_per_thread
            else:
                # index or mixed: compute docs per thread according to bulk size
                args.operations = requests_per_thread * max(1, int(args.bulk_size or 100))
            # Save derived per-thread requests for use by workers
            args._requests_per_thread = requests_per_thread
    except Exception:
        args._requests_per_thread = None

    if args.payload_size is not None:
        try:
            # Accept values like '256', '256B'
            ps = str(args.payload_size).strip()
            if ps.lower().endswith('b'):
                ps = ps[:-1]
            PAYLOAD_SIZE = max(1, int(ps))
        except Exception:
            pass

    # 连接ES
    es = Elasticsearch([{'host': args.es_host, 'port': args.es_port, 'scheme': 'http'}])

    # 检查连接（带重试以避免短暂启动延迟导致的失败）
    # 首先尝试 es.ping(); 若 ping 返回 False 或抛出异常，则回退到对 HTTP 根节点的原生 GET 检查。
    import urllib.request as _urllib_request
    import urllib.error as _urllib_error

    max_retries = 30
    connected = False
    probe_url = f'http://{args.es_host}:{args.es_port}/'
    import json as _json
    import re as _re

    for attempt in range(1, max_retries + 1):
        try:
            # 优先使用官方客户端的 ping
            try:
                if es.ping():
                    connected = True
                    break
            except Exception:
                # 忽略客户端 ping 的偶发异常，尝试回退方法
                pass

            # 回退：直接对 HTTP 根路径做简单 GET（避免添加任何额外查询参数）
            try:
                req = _urllib_request.Request(probe_url, method='GET')
                with _urllib_request.urlopen(req, timeout=2) as resp:
                    status = getattr(resp, 'status', None)
                    body = resp.read()
                    if status == 200 and body:
                        # 解析 JSON 并验证 version.number 字段
                        try:
                            parsed = _json.loads(body.decode('utf-8', errors='replace'))
                            ver = parsed.get('version', {}).get('number')
                            if ver and _re.match(r'^\d+\.\d+(?:\.\d+)?$', str(ver)):
                                print(f"Elasticsearch root OK, version={ver}")
                                connected = True
                                break
                            else:
                                print(f"Root returned JSON but missing/invalid version.number: {ver}")
                        except Exception as e:
                            print(f"Failed to parse root JSON: {e}")
            except (_urllib_error.URLError, _urllib_error.HTTPError, Exception):
                # 继续重试
                pass

        finally:
            if not connected and attempt < max_retries:
                print(f"Waiting for Elasticsearch to be ready... attempt {attempt}/{max_retries}")
                time.sleep(1)

    if not connected:
        print(f"ERROR: Unable to connect to Elasticsearch {args.es_host}:{args.es_port} after {max_retries} attempts")
        exit(1)

    # 创建索引（如果不存在）
    # 为兼容性：如果官方 client 抛出 UnsupportedProductError，使用 HTTP 回退实现索引检查/创建和 bulk
    from elasticsearch import UnsupportedProductError as _UnsupportedProductError
    CLIENT_INCOMPATIBLE = False

    def http_request(path, method='GET', body=None, timeout=5, headers=None):
        url = f'http://{args.es_host}:{args.es_port}{path}'
        req = _urllib_request.Request(url, data=body, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        return _urllib_request.urlopen(req, timeout=timeout)

    def http_index_exists(index_name):
        try:
            with http_request(f'/{index_name}', method='GET', timeout=2) as resp:
                status = getattr(resp, 'status', None)
                return status == 200
        except _urllib_error.HTTPError as he:
            if getattr(he, 'code', None) == 404:
                return False
            return False
        except Exception:
            return False

    def http_create_index(index_name):
        try:
            body = b'{}'
            with http_request(f'/{index_name}', method='PUT', body=body, headers={'Content-Type': 'application/json'}, timeout=5) as resp:
                return getattr(resp, 'status', None) in (200, 201)
        except Exception:
            return False

    def http_bulk(ops):
        # ops: list of {'_index':..., '_id':..., '_source': {...}}
        try:
            lines = []
            for op in ops:
                meta = {'index': {'_index': op.get('_index')}}
                if op.get('_id') is not None:
                    meta['index']['_id'] = op.get('_id')
                lines.append(_json.dumps(meta))
                lines.append(_json.dumps(op.get('_source', {})))
            payload = '\n'.join(lines) + '\n'
            body = payload.encode('utf-8')
            with http_request('/_bulk', method='POST', body=body, headers={'Content-Type': 'application/x-ndjson'}, timeout=10) as resp:
                txt = resp.read()
                try:
                    parsed = _json.loads(txt.decode('utf-8', errors='replace'))
                    # ignore bulk errors for now
                    return parsed
                except Exception:
                    return None
        except Exception:
            return None

    def perform_indices_exists(index_name):
        nonlocal CLIENT_INCOMPATIBLE
        try:
            return es.indices.exists(index=index_name)
        except _UnsupportedProductError:
            CLIENT_INCOMPATIBLE = True
            return http_index_exists(index_name)

    def perform_create_index(index_name):
        nonlocal CLIENT_INCOMPATIBLE
        try:
            return es.indices.create(index=index_name)
        except _UnsupportedProductError:
            CLIENT_INCOMPATIBLE = True
            return http_create_index(index_name)

    def perform_bulk(es_client, operations):
        nonlocal CLIENT_INCOMPATIBLE
        try:
            from elasticsearch.helpers import bulk as _bulk_helper
            return _bulk_helper(es_client, operations)
        except _UnsupportedProductError:
            CLIENT_INCOMPATIBLE = True
            return http_bulk(operations)

    if not perform_indices_exists(args.index_name):
        perform_create_index(args.index_name)

    print(f'Starting Elasticsearch benchmark: threads={args.threads} operations/thread={args.operations} mode={args.test_mode}')

    latencies = []
    total_operations = 0
    total_time = 0

    metrics = None
    if IntervalMetrics:
        metrics = IntervalMetrics(
            interval_sec=getattr(args, 'metrics_interval', 1.0),
            out_path=getattr(args, 'metrics_out', None),
            label='elasticsearch',
        )
        if bench_common:
            try:
                bench_common.register_metrics_signal_handlers(metrics)
            except Exception:
                pass
        metrics.start()

    if args.test_mode in ['index', 'mixed']:
        print("Running index benchmark...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(index_worker, tid, es, args.index_name, args.operations,
                                       args.field_count, latencies, metrics)
                futures.append(future)

            for future in futures:
                try:
                    ops, time_taken, avg_lat, throughput = future.result()
                    total_operations += ops
                    total_time += time_taken
                    # report latency in milliseconds for readability
                    avg_lat_ms = avg_lat * 1000.0
                    print(f'Thread result: ops={ops} time={time_taken:.3f}s avg_lat={avg_lat_ms:.3f}ms throughput={throughput:.3f} ops/s')
                except Exception as e:
                    print(f'Index thread error: {e}')

    if args.test_mode in ['search', 'mixed']:
        print("Running search benchmark...")
        latencies = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = []
            for tid in range(args.threads):
                future = executor.submit(search_worker, tid, es, args.index_name, args.operations,
                                       latencies, args.field_count, metrics)
                futures.append(future)

            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f'Search thread error: {e}')

        total_operations += args.operations * args.threads

    if metrics:
        metrics.stop()
        metrics.write()

    # 计算统计
    if latencies:
        avg_latency = statistics.mean(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        p50_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) > 10 else max(latencies)  # 95th percentile

        overall_throughput = (total_operations / total_time) if total_time > 0 else 0

        # convert seconds -> milliseconds for latency numbers
        avg_ms = avg_latency * 1000.0
        p50_ms = p50_latency * 1000.0
        p95_ms = p95_latency * 1000.0
        min_ms = min_latency * 1000.0
        max_ms = max_latency * 1000.0

        print("\n=== Elasticsearch Benchmark ===")
        print(f"Total operations: {total_operations}")
        print(f"Total time (s): {total_time:.3f}")
        print(f"Overall throughput (ops/s): {overall_throughput:.3f}")
        print(f"Avg latency (ms): {avg_ms:.3f}")
        print(f"p50 latency (ms): {p50_ms:.3f}")
        print(f"p95 latency (ms): {p95_ms:.3f}")
        print(f"Min latency (ms): {min_ms:.3f}")
        print(f"Max latency (ms): {max_ms:.3f}")

        # Emit single-line metric header and values for harness parsing
        # Format matches other benches: METRIC_HEADER\ttotal_ops\tops_per_sec
        try:
            ops = int(total_operations)
        except Exception:
            ops = 0
        try:
            overall_ops = float(overall_throughput)
        except Exception:
            overall_ops = 0.0
        print(f"METRIC_HEADER\ttotal_ops\tops_per_sec")
        print(f"METRIC_VALUES\t{ops}\t{overall_ops:.3f}")
    else:
        print("No latency data available; cannot compute statistics")
        # Still print metric header/values if possible
        try:
            ops = int(total_operations)
        except Exception:
            ops = 0
        overall_ops = (total_operations / total_time) if total_time > 0 else 0.0
        print(f"METRIC_HEADER\ttotal_ops\tops_per_sec")
        print(f"METRIC_VALUES\t{ops}\t{overall_ops:.3f}")

if __name__ == '__main__':
    main()