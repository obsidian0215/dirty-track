#!/usr/bin/env python3
"""Audit current bench implementations for 'edge realism'.

This script samples payload generation from benches (without sending
traffic to real services) and computes basic distribution statistics:
  - payload size distribution
  - device id cardinality and top devices
  - presence of realism fields (battery_level, drift, etc.)
  - inter-arrival timing summary (if applicable)

Results are written to `config_tests/results/realism_audit_<bench>.json` and
a short combined markdown summary `config_tests/results/realism_audit_summary.md`.
"""
from __future__ import annotations

import argparse
import importlib
import json
import math
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# Ensure repo root is on sys.path so package imports work when executed directly
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
OUT_DIR = os.path.join(ROOT, 'results')
os.makedirs(OUT_DIR, exist_ok=True)


def ensure_influx_stub() -> None:
    """Inject a minimal influxdb_client stub if real package is unavailable."""
    try:
        import influxdb_client  # type: ignore
        return
    except Exception:
        pass

    import types

    mod = types.ModuleType('influxdb_client')

    class DummyPoint:
        def __init__(self, measurement=None):
            self._measurement = measurement
            self._tags = {}
            self._fields = {}
            self._time = None

        def tag(self, k, v):
            self._tags[k] = v
            return self

        def field(self, k, v):
            self._fields[k] = v
            return self

        def time(self, t, write_precision=None):
            self._time = t
            return self

        def to_dict(self):
            return {'measurement': self._measurement, 'tags': self._tags, 'fields': self._fields, 'time': self._time}

        def __repr__(self):
            return f"DummyPoint({self._measurement},{self._tags},{self._fields})"

    mod.Point = DummyPoint
    mod.WritePrecision = type('WP', (), {'NS': 'ns'})

    # minimal client submodules
    client_pkg = types.ModuleType('influxdb_client.client')
    write_api_mod = types.ModuleType('influxdb_client.client.write_api')
    write_api_mod.SYNCHRONOUS = 'synchronous'
    write_api_mod.ASYNCHRONOUS = 'asynchronous'
    query_api_mod = types.ModuleType('influxdb_client.client.query_api')
    query_api_mod.QueryApi = lambda *a, **k: None
    bucket_api_mod = types.ModuleType('influxdb_client.client.bucket_api')
    bucket_api_mod.BucketsApi = lambda *a, **k: None

    sys.modules['influxdb_client'] = mod
    sys.modules['influxdb_client.client'] = client_pkg
    sys.modules['influxdb_client.client.write_api'] = write_api_mod
    sys.modules['influxdb_client.client.query_api'] = query_api_mod
    sys.modules['influxdb_client.client.bucket_api'] = bucket_api_mod

    class DummyClient:
        def __init__(self, *a, **k):
            pass

        def write_api(self, write_options=None):
            return type('W', (), {'write': lambda self, bucket, org, record: None})()

        def query_api(self):
            return type('Q', (), {'query': lambda self, q, org=None: []})()

        def buckets_api(self):
            return type('B', (), {})()

        def organizations_api(self):
            return type('O', (), {})()

    mod.InfluxDBClient = DummyClient


def ensure_redis_stub() -> None:
    try:
        import redis  # type: ignore
        return
    except Exception:
        pass

    import types
    rmod = types.ModuleType('redis')

    class DummyRedis:
        def __init__(self, *a, **k):
            pass

        def xadd(self, stream, data):
            return None

        def expire(self, key, seconds):
            return None

        def xtrim(self, stream, maxlen=None, approximate=True):
            return None

    rmod.Redis = DummyRedis
    rmod.ConnectionPool = lambda *a, **k: None
    sys.modules['redis'] = rmod


def summarize_numeric(vals: Iterable[float]) -> Dict[str, Any]:
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return {'count': 0}
    return {
        'count': len(vals),
        'min': min(vals),
        'max': max(vals),
        'mean': statistics.mean(vals),
        'median': statistics.median(vals),
        'p90': percentile(vals, 90),
        'p95': percentile(vals, 95),
        'p99': percentile(vals, 99),
    }


def percentile(a: Iterable[float], p: float) -> float:
    a = sorted(a)
    if not a:
        return float('nan')
    i = max(0, min(len(a) - 1, int(round((p / 100.0) * (len(a) - 1)))))
    return a[i]


def extract_point_payload_size(point) -> int:
    # try common attr shapes
    try:
        if hasattr(point, 'to_dict'):
            return len(json.dumps(point.to_dict()))
        if hasattr(point, '_tags') or hasattr(point, '_fields'):
            d = {}
            if hasattr(point, '_tags'):
                d['tags'] = getattr(point, '_tags')
            if hasattr(point, '_fields'):
                d['fields'] = getattr(point, '_fields')
            return len(json.dumps(d))
    except Exception:
        pass
    try:
        s = str(point)
        return len(s)
    except Exception:
        return 0


def audit_influx_sensoragg(samples: int = 500) -> Dict[str, Any]:
    ensure_influx_stub()
    from experiment.migration.influxdb import bench_sensoragg as bs

    bench = bs.SensorInfluxBench(influx_url='http://nohost', token='t', org='o', bucket='b')
    bench.payload_mode = getattr(bench, 'payload_mode', 'json')
    bench.size_distribution = getattr(bench, 'size_distribution', 'uniform')

    sizes = []
    devices = Counter()
    sensor_types = Counter()
    field_presence = Counter()
    interarrivals = []

    # If the bench exposes a pacing sampler, collect pacing-based interarrival samples
    pacing_intervals = []
    if hasattr(bench, 'sample_interarrival'):
        # enable pacing sampling using poisson model for audit
        bench.pacing = 'poisson'
        # ensure a sensible rate for sampling
        if not getattr(bench, 'max_requests_per_second', None):
            bench.max_requests_per_second = 50
        for _ in range(min(300, samples)):
            try:
                iv = bench.sample_interarrival()
                pacing_intervals.append(iv)
            except Exception:
                pass

    last_t = None
    for i in range(samples):
        t0 = time.perf_counter()
        pts = bench._generate_sensor_data()
        t1 = time.perf_counter()
        if last_t is not None:
            interarrivals.append(t0 - last_t)
        last_t = t1

        est_size = 0
        for p in pts:
            s = extract_point_payload_size(p)
            est_size += s
            # extract device id if available
            dev = None
            if hasattr(p, '_tags'):
                dev = p._tags.get('device_id')
                st = p._tags.get('sensor_type')
                if st:
                    sensor_types[st] += 1
            # inspect fields
            if hasattr(p, '_fields'):
                for k in ('battery_level', 'readings_count', 'calibration_status'):
                    if k in p._fields:
                        field_presence[k] += 1

            if dev:
                devices[dev] += 1

        sizes.append(est_size)

    interarrival_summary = summarize_numeric(pacing_intervals) if pacing_intervals else summarize_numeric(interarrivals)

    report = {
        'bench': 'influx_sensoragg',
        'samples': samples,
        'payload_size': summarize_numeric(sizes),
        'devices': {'unique': len(devices), 'top': devices.most_common(10)},
        'sensor_types': sensor_types.most_common(10),
        'field_presence': dict(field_presence),
        'interarrival': interarrival_summary,
        'payload_mode': getattr(bench, 'payload_mode', None),
        'size_distribution': getattr(bench, 'size_distribution', None),
    }
    return report


def audit_redis_cartelem(samples: int = 500) -> Dict[str, Any]:
    ensure_redis_stub()
    from experiment.migration.redis import bench_cartelem as rc

    bench = rc.CarTelematicsBench(redis_host='127.0.0.1', redis_port=6379, stream_name='s')

    sizes = []
    vehicles = Counter()
    field_presence = Counter()
    interarrivals = []

    # pacing sample if available
    pacing_intervals = []
    if hasattr(bench, 'sample_interarrival'):
        # enable pacing sampling (poisson) for audit
        bench.pacing = 'poisson'
        if not getattr(bench, 'max_requests_per_second', None):
            bench.max_requests_per_second = 200
        for _ in range(min(300, samples)):
            try:
                iv = bench.sample_interarrival()
                pacing_intervals.append(iv)
            except Exception:
                pass

    last_t = None
    for i in range(samples):
        t0 = time.perf_counter()
        payload = bench._make_payload()
        t1 = time.perf_counter()
        if last_t is not None:
            interarrivals.append(t0 - last_t)
        last_t = t1
        s = len(json.dumps(payload))
        sizes.append(s)
        vid = payload.get('vehicle_id')
        if vid:
            vehicles[vid] += 1
        for k in ('obd_codes', 'fuel_level', 'engine_temp'):
            if k in payload:
                field_presence[k] += 1

    interarrival_summary = summarize_numeric(pacing_intervals) if pacing_intervals else summarize_numeric(interarrivals)

    report = {
        'bench': 'redis_cartelem',
        'samples': samples,
        'payload_size': summarize_numeric(sizes),
        'vehicles': {'unique': len(vehicles), 'top': vehicles.most_common(10)},
        'field_presence': dict(field_presence),
        'interarrival': interarrival_summary,
        'vehicle_pattern': getattr(bench, 'vehicle_pattern', None),
    }
    return report


def audit_redis_sensoragg(samples: int = 500) -> Dict[str, Any]:
    ensure_redis_stub()
    from experiment.migration.redis import bench_sensoragg as rs

    bench = rs.SensorAggBench(redis_host='127.0.0.1', redis_port=6379) if hasattr(rs, 'SensorAggBench') else None
    if bench is None:
        # Fallback to a simpler module variant
        try:
            bench = rs.SensorAggBench(redis_host='127.0.0.1', redis_port=6379)
        except Exception:
            return {'bench': 'redis_sensoragg', 'error': 'bench class not found'}

    sizes = []
    devices = Counter()
    field_presence = Counter()
    interarrivals = []

    # Pacing
    pacing_intervals = []
    if hasattr(bench, 'sample_interarrival'):
        bench.pacing = 'poisson'
        if hasattr(bench, 'rate_limiter') and not bench.rate_limiter.rate:
             bench.rate_limiter.rate = 50
        for _ in range(min(300, samples)):
            try:
                iv = bench.sample_interarrival()
                pacing_intervals.append(iv)
            except Exception:
                pass

    last_t = None
    for i in range(samples):
        t0 = time.perf_counter()
        # SensorAgg uses _make_reading() to produce a single reading dict
        if hasattr(bench, '_make_reading'):
            payload = bench._make_reading()
        elif hasattr(bench, '_generate_sensor_data'):
            payload = bench._generate_sensor_data(int(time.time() * 1000))
        else:
            payload = None
        t1 = time.perf_counter()
        if last_t is not None:
            interarrivals.append(t0 - last_t)
        last_t = t1
        if payload is None:
            continue
        # payload may be dict
        if isinstance(payload, dict):
            sizes.append(len(json.dumps(payload)))
            # device id may be under various keys: device_id / sensor_id / vehicle_id
            dev = None
            for dk in ('device_id', 'sensor_id', 'vehicle_id', 'dev_id'):
                if dk in payload:
                    dev = payload.get(dk)
                    break
            if dev:
                devices[dev] += 1
            for k in ('battery_level', 'readings_count'):
                if k in payload:
                    field_presence[k] += 1
        else:
            sizes.append(len(str(payload)))

    report = {
        'bench': 'redis_sensoragg',
        'samples': samples,
        'payload_size': summarize_numeric(sizes),
        'devices': {'unique': len(devices), 'top': devices.most_common(10)},
        'field_presence': dict(field_presence),
        'interarrival': summarize_numeric(pacing_intervals if pacing_intervals else interarrivals),
    }
    return report


def audit_influx_cartelem(samples: int = 500) -> Dict[str, Any]:
    ensure_influx_stub()
    from experiment.migration.influxdb import bench_cartelem as bc

    bench = bc.VehicleInfluxBench(influx_url='http://nohost', token='t', org='o', bucket='b')
    bench.payload_mode = getattr(bench, 'payload_mode', 'json')
    bench.size_distribution = getattr(bench, 'size_distribution', 'uniform')

    sizes = []
    devices = Counter()
    field_presence = Counter()
    interarrivals = []

    # Pacing
    pacing_intervals = []
    if hasattr(bench, 'sample_interarrival'):
        bench.pacing = 'poisson'
        if not getattr(bench, 'max_requests_per_second', None):
            # Manually set rate for sampling if needed, though sample_interarrival handles default
            if hasattr(bench, '_rate_limiter'):
                bench._rate_limiter.rate = 50
        for _ in range(min(300, samples)):
            try:
                iv = bench.sample_interarrival()
                pacing_intervals.append(iv)
            except Exception:
                pass

    last_t = None
    for i in range(samples):
        t0 = time.perf_counter()
        # _generate_vehicle_data returns list of points
        pts = bench._generate_vehicle_data()
        t1 = time.perf_counter()
        if last_t is not None:
            interarrivals.append(t0 - last_t)
        last_t = t1

        if not pts:
            continue

        # Estimate size
        total_size = sum(extract_point_payload_size(p) for p in pts)
        sizes.append(total_size)

        # Extract device ID from tags
        for p in pts:
            tags = getattr(p, '_tags', {})
            if 'vehicle_id' in tags:
                devices[tags['vehicle_id']] += 1
            fields = getattr(p, '_fields', {})
            for k in fields:
                field_presence[k] += 1

    report = {
        'bench': 'influx_cartelem',
        'samples': samples,
        'payload_size': summarize_numeric(sizes),
        'devices': {'unique': len(devices), 'top': devices.most_common(10)},
        'field_presence': dict(field_presence),
        'interarrival': summarize_numeric(pacing_intervals if pacing_intervals else interarrivals),
    }
    return report


def audit_redis_video_cache_realistic(samples: int = 500) -> Dict[str, Any]:
    ensure_redis_stub()
    from experiment.migration.redis import bench_video_cache_realistic as bv

    bench = bv.VideoCacheRealisticBench(redis_host='nohost', redis_port=0)

    sizes = []
    devices = Counter()
    field_presence = Counter()
    interarrivals = []

    # Pacing
    pacing_intervals = []
    if hasattr(bench, 'sample_interarrival'):
        bench.pacing = 'poisson'
        # framerate is default 30
        for _ in range(min(300, samples)):
            try:
                iv = bench.sample_interarrival()
                pacing_intervals.append(iv)
            except Exception:
                pass

    last_t = None
    for i in range(samples):
        t0 = time.perf_counter()
        res = bench._make_result()
        t1 = time.perf_counter()
        if last_t is not None:
            interarrivals.append(t0 - last_t)
        last_t = t1

        payload = json.dumps(res)
        sizes.append(len(payload))

        if 'camera_id' in res:
            devices[res['camera_id']] += 1

        for k in res:
            field_presence[k] += 1

    report = {
        'bench': 'redis_video_cache_realistic',
        'samples': samples,
        'payload_size': summarize_numeric(sizes),
        'devices': {'unique': len(devices), 'top': devices.most_common(10)},
        'field_presence': dict(field_presence),
        'interarrival': summarize_numeric(pacing_intervals if pacing_intervals else interarrivals),
    }
    return report


def run_audits(benches: List[str], samples: int = 500) -> Dict[str, Any]:
    results = {}
    for b in benches:
        if b == 'influx_sensoragg':
            report = audit_influx_sensoragg(samples)
        elif b == 'influx_cartelem':
            report = audit_influx_cartelem(samples)
        elif b == 'redis_cartelem':
            report = audit_redis_cartelem(samples)
        elif b == 'redis_sensoragg':
            report = audit_redis_sensoragg(samples)
        elif b == 'redis_video_cache_realistic':
            report = audit_redis_video_cache_realistic(samples)
        else:
            report = {'bench': b, 'error': 'unknown bench'}
        results[b] = report
        # write per-bench JSON
        fn = os.path.join(OUT_DIR, f'realism_audit_{b}.json')
        with open(fn, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
    return results


def write_summary(results: Dict[str, Any], out_md: str) -> None:
    lines = ['# Realism Audit Summary', '']
    def fmt(v, prec=1):
        try:
            if v is None:
                return 'N/A'
            return f"{v:.{prec}f}"
        except Exception:
            try:
                return str(v)
            except Exception:
                return 'N/A'

    for k, r in results.items():
        lines.append(f'## {k}')
        if 'error' in r:
            lines.append(f'- Error: {r.get("error")}')
            lines.append('')
            continue
        ps = r.get('payload_size', {}) or {}
        lines.append(f'- samples: {r.get("samples")}, payload mean={fmt(ps.get("mean"))} median={fmt(ps.get("median"))} p95={fmt(ps.get("p95"))}')
        devs = r.get('devices') or r.get('vehicles') or {}
        if devs:
            top = devs.get('top') if isinstance(devs.get('top'), list) else []
            lines.append(f'- unique devices: {devs.get("unique")} top: {top[:5]}')
        if r.get('field_presence'):
            lines.append(f'- field_presence: {r.get("field_presence")}')
        ia = r.get('interarrival', {}) or {}
        if ia and ia.get('count', 0) > 0:
            lines.append(f'- interarrival mean={fmt(ia.get("mean"), prec=4)}s median={fmt(ia.get("median"), prec=4)}s p95={fmt(ia.get("p95"), prec=4)}s')
        else:
            lines.append('- interarrival: no pacing detected (generation is CPU-bound or untimed)')
        lines.append('')

    with open(out_md, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--benches', default='influx_sensoragg,redis_cartelem,redis_sensoragg', help='Comma-separated bench keys to audit')
    p.add_argument('--samples', default=500, type=int, help='Samples per bench')
    args = p.parse_args()

    benches = [b.strip() for b in args.benches.split(',') if b.strip()]
    results = run_audits(benches, samples=args.samples)
    out_md = os.path.join(OUT_DIR, 'realism_audit_summary.md')
    write_summary(results, out_md)
    print('Wrote', out_md)


if __name__ == '__main__':
    main()
