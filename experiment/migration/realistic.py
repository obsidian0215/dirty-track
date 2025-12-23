"""Realism profiles for bench scripts (simple prototype).

Provides a tiny RealismProfile class that supplies inter-arrival times,
payload size samples and device id choices to emulate edge-like flows.

This is intentionally small so it can be prototyped and iterated on.
"""
import random
import time
from typing import List, Optional, Tuple


class RealismProfile:
    def __init__(self,
                 name: str = 'edge_basic',
                 mean_rps: Optional[float] = None,
                 burst_chance: float = 0.01,
                 burst_multiplier: float = 3.0,
                 burst_mean_duration: float = 5.0,
                 device_count: int = 1000,
                 payload_mixture: Optional[List[Tuple[float,int,int]]] = None):
        """Initialize profile.

        payload_mixture: list of (weight, min_bytes, max_bytes)
        """
        self.name = name
        self.mean_rps = mean_rps
        self.burst_chance = burst_chance
        self.burst_multiplier = burst_multiplier
        self.burst_mean_duration = burst_mean_duration
        self.device_count = max(1, int(device_count))
        self.payload_mixture = payload_mixture or [(0.7, 50, 200), (0.25, 512, 1024), (0.05, 3500, 8000)]

        # burst state
        self._in_burst = False
        self._burst_remaining = 0.0

    def _maybe_start_burst(self):
        if self._in_burst:
            return
        if random.random() < self.burst_chance:
            # start burst
            self._in_burst = True
            # draw duration from exponential with mean burst_mean_duration
            self._burst_remaining = random.expovariate(1.0 / max(0.1, self.burst_mean_duration))

    def next_interarrival(self, upper_rps: Optional[float] = None) -> float:
        """Return seconds to sleep until next event (exponential interarrival).

        If no effective rate available, returns 0 (no sleep).
        """
        # decide effective base rate
        effective = self.mean_rps or upper_rps
        if not effective or effective <= 0:
            return 0.0

        # maybe start burst
        self._maybe_start_burst()
        if self._in_burst:
            rate = effective * self.burst_multiplier
            # decrement remaining
            # draw an interarrival; then reduce remaining time
            interval = random.expovariate(rate)
            self._burst_remaining -= interval
            if self._burst_remaining <= 0:
                self._in_burst = False
                self._burst_remaining = 0.0
            return max(0.0, interval)
        else:
            interval = random.expovariate(effective)
            return max(0.0, interval)

    def sample_payload_size(self) -> int:
        """Sample a payload size (bytes) based on mixture model."""
        p = random.random()
        acc = 0.0
        for w, lo, hi in self.payload_mixture:
            acc += w
            if p <= acc:
                return random.randint(lo, hi)
        # fallback
        w, lo, hi = self.payload_mixture[-1]
        return random.randint(lo, hi)

    def choose_device_id(self) -> str:
        """Choose a device id from the synthetic population."""
        return f"dev-{random.randint(1, self.device_count)}"


def load_profile(name: str) -> RealismProfile:
    # minimal named profiles (can extend to load JSON files later)
    if name == 'edge_bursty':
        return RealismProfile(name=name, mean_rps=None, burst_chance=0.02, burst_multiplier=4.0, burst_mean_duration=8.0, device_count=5000)
    # default
    return RealismProfile(name='edge_basic', mean_rps=None, burst_chance=0.01, burst_multiplier=3.0, burst_mean_duration=5.0, device_count=1000)
