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
import logging
import os
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


__all__ = [
    'add_common_args',
    'RateLimiter',
    'parse_size',
    'get_dataset_path',
    'configure_logging',
    'DEFAULT_DATASET_DIR',
]
