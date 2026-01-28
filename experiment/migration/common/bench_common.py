# coding: utf-8
"""
bench_common.py

Provide shared CLI arguments and utilities for all migration bench scripts:
- add_common_args(parser)
- RateLimiter
- parse_size
- get_dataset_path

Default dataset directory: repo root `datasets/` (computed at runtime).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import atexit
import sys
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Default dataset dir relative to this file: ../../../.. -> /runc/datasets
DEFAULT_DATASET_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'datasets'))


def add_common_args(parser: argparse.ArgumentParser, *, include_dataset: bool = True) -> None:
    """Add standard benchmarking arguments to an ArgumentParser.

    Args:
        parser: The argparse.ArgumentParser to extend.
        include_dataset: Whether to add the --dataset option.
    """
    parser.add_argument('--duration', type=int, default=60,
                        help='Benchmark duration in seconds (default: 60)')
    # rps alias supports both --rps and --qps
    parser.add_argument('--rps', '--qps', dest='rps', type=int, default=0,
                        help='Requests per second / QPS cap (0 = no cap)')
    # threads alias supports --threads and --concurrency
    parser.add_argument('--threads', '--concurrency', dest='threads', type=int, default=1,
                        help='Number of worker threads/concurrency (default: 1)')
    parser.add_argument('--frontend-url', dest='frontend_url', default=None,
                        help='Optional HTTP frontend URL to route requests through')
    parser.add_argument('--metrics-out', default=None,
                        help='Output path for interval metrics (JSON)')
    parser.add_argument('--metrics-interval', type=float, default=1.0,
                        help='Sampling interval in seconds for metrics (default: 1.0)')

    if include_dataset:
        parser.add_argument('--dataset', default=DEFAULT_DATASET_DIR,
                            help=f'Path to datasets directory (default: {DEFAULT_DATASET_DIR})')


def parse_size(size: str) -> int:
    """Parse human-friendly size strings like '1KB', '256B', '2MB' into bytes."""
    if isinstance(size, int):
        return size
    s = size.strip().upper()
    if s.endswith('KB'):
        return int(float(s[:-2]) * 1024)
    if s.endswith('MB'):
        return int(float(s[:-2]) * 1024 * 1024)
    if s.endswith('B'):
        return int(float(s[:-1]))
    return int(float(s))


class RateLimiter:
    """Simple token bucket rate limiter for cross-thread control."""

    def __init__(self, rps: Optional[int]):
        self.rps = int(rps) if rps else 0
        if self.rps > 0:
            self.capacity = float(self.rps)
            self.tokens = float(self.rps)
            self.last = time.monotonic()
            self.lock = threading.Lock()

    def acquire(self) -> None:
        if not self.rps:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last
                if elapsed > 0:
                    refill = elapsed * self.capacity
                    self.tokens = min(self.capacity, self.tokens + refill)
                    self.last = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                deficit = 1.0 - self.tokens
                wait_time = deficit / self.capacity if self.capacity > 0 else 0.01
            time.sleep(wait_time)


def get_dataset_path(args_or_path) -> str:
    """Return absolute dataset path from either args.dataset or a path string.

    Non-fatal: logs a warning if dataset path does not exist.
    """
    if hasattr(args_or_path, 'dataset'):
        path = args_or_path.dataset
    else:
        path = args_or_path

    path = os.path.abspath(path)
    if not os.path.exists(path):
        logger.warning('Dataset path does not exist: %s', path)
    return path


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(level=level, format='%(asctime)s %(levelname)s %(message)s')


class IntervalMetrics:
    """Track interval throughput/latency/loss and persist to a JSON file."""

    def __init__(self, interval_sec: float = 1.0, out_path: Optional[str] = None, label: Optional[str] = None, logger=None):
        self.interval_sec = float(interval_sec) if interval_sec else 1.0
        self.out_path = out_path
        self.label = label or 'bench'
        self.logger = logger
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

        self._start_mono = None
        self._last_mono = None

        self._total_count = 0
        self._total_success = 0
        self._total_latency_sum = 0.0
        self._total_latency_count = 0

        self._win_count = 0
        self._win_success = 0
        self._win_latency_sum = 0.0
        self._win_latency_count = 0

        self._samples = []

    def start(self) -> None:
        if self._thread is not None:
            return
        self._start_mono = time.monotonic()
        self._last_mono = self._start_mono
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def record(self, success: bool, latency_ms: Optional[float]) -> None:
        with self._lock:
            self._total_count += 1
            if success:
                self._total_success += 1
            if latency_ms is not None:
                try:
                    self._total_latency_sum += float(latency_ms)
                    self._total_latency_count += 1
                except Exception:
                    pass

            self._win_count += 1
            if success:
                self._win_success += 1
            if latency_ms is not None:
                try:
                    self._win_latency_sum += float(latency_ms)
                    self._win_latency_count += 1
                except Exception:
                    pass

    def _emit(self, sample: dict) -> None:
        msg = (
            f"[sample] {self.label} t={sample['elapsed_end_sec']:.1f}s "
            f"throughput={sample['throughput_ops_sec']:.2f}ops/s "
            f"avg_latency={sample['avg_latency_ms']:.3f}ms "
            f"loss_rate={sample['loss_rate']:.3f}"
        )
        if self.logger is not None:
            try:
                self.logger.info(msg)
                return
            except Exception:
                pass
        print(msg)

    def _sample(self, now_mono: Optional[float] = None) -> None:
        if self._start_mono is None or self._last_mono is None:
            return
        now_mono = now_mono or time.monotonic()
        duration = now_mono - self._last_mono
        if duration <= 0:
            return

        with self._lock:
            count = self._win_count
            success = self._win_success
            lat_sum = self._win_latency_sum
            lat_cnt = self._win_latency_count

            self._win_count = 0
            self._win_success = 0
            self._win_latency_sum = 0.0
            self._win_latency_count = 0

        throughput = count / duration if count > 0 else 0.0
        avg_latency = lat_sum / lat_cnt if lat_cnt > 0 else 0.0
        loss_rate = (count - success) / count if count > 0 else 0.0

        sample = {
            "elapsed_start_sec": self._last_mono - self._start_mono,
            "elapsed_end_sec": now_mono - self._start_mono,
            "interval_sec": duration,
            "total_ops": count,
            "success_ops": success,
            "throughput_ops_sec": throughput,
            "avg_latency_ms": avg_latency,
            "loss_rate": loss_rate,
        }
        self._samples.append(sample)
        self._emit(sample)
        self._last_mono = now_mono

    def _run(self) -> None:
        while not self._stop.wait(self.interval_sec):
            self._sample()

    def stop(self) -> None:
        if self._thread is None:
            return
        self._stop.set()
        try:
            self._thread.join(timeout=self.interval_sec + 1.0)
        except Exception:
            pass
        self._sample()

    def to_dict(self) -> dict:
        if self._start_mono is None:
            total_duration = 0.0
        else:
            total_duration = max(0.0, time.monotonic() - self._start_mono)

        with self._lock:
            total_count = self._total_count
            total_success = self._total_success
            total_latency_sum = self._total_latency_sum
            total_latency_count = self._total_latency_count

        overall_throughput = total_count / total_duration if total_duration > 0 else 0.0
        overall_avg_latency = total_latency_sum / total_latency_count if total_latency_count > 0 else 0.0
        overall_loss = (total_count - total_success) / total_count if total_count > 0 else 0.0

        return {
            "label": self.label,
            "interval_sec": self.interval_sec,
            "total_duration_sec": total_duration,
            "samples": list(self._samples),
            "overall": {
                "total_ops": total_count,
                "success_ops": total_success,
                "throughput_ops_sec": overall_throughput,
                "avg_latency_ms": overall_avg_latency,
                "loss_rate": overall_loss,
            },
        }

    def write(self, out_path: Optional[str] = None) -> Optional[str]:
        path = out_path or self.out_path
        if not path:
            return None
        out_dir = os.path.dirname(path)
        try:
            if out_dir:
                os.makedirs(out_dir, exist_ok=True)
            data = self.to_dict()
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            return path
        except Exception as e:
            # Best-effort logging; avoid raising from metrics write
            try:
                if self.logger:
                    self.logger.warning("IntervalMetrics write failed for %s: %s", path, e)
                else:
                    logger.warning("IntervalMetrics write failed for %s: %s", path, e)
            except Exception:
                pass
            return None


def install_signal_handlers(cleanup_fn) -> None:
    def _handler(signum, frame):
        try:
            cleanup_fn()
        finally:
            raise SystemExit(0)

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def register_metrics_signal_handlers(metrics: Optional[IntervalMetrics]) -> None:
    if not metrics:
        return

    def _cleanup():
        try:
            metrics.stop()
        except Exception:
            pass
        try:
            metrics.write()
        except Exception:
            pass

    # Ensure metrics are flushed on signals (SIGINT/SIGTERM), on normal exit, and on uncaught exceptions
    try:
        install_signal_handlers(_cleanup)
    except Exception:
        # best-effort only
        pass

    try:
        atexit.register(_cleanup)
    except Exception:
        pass

    # Ensure uncaught exceptions trigger cleanup as well (best-effort)
    try:
        _old_exch = sys.excepthook

        def _excepthook(exc_type, exc_value, exc_tb):
            try:
                _cleanup()
            finally:
                _old_exch(exc_type, exc_value, exc_tb)

        sys.excepthook = _excepthook
    except Exception:
        pass


__all__ = [
    'add_common_args',
    'RateLimiter',
    'parse_size',
    'get_dataset_path',
    'configure_logging',
    'DEFAULT_DATASET_DIR',
    'IntervalMetrics',
    'install_signal_handlers',
    'register_metrics_signal_handlers',
]
