#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous Memory Mapping Test - Repeated execution version
Tests light-dt module with repeated memory operations
"""

import mmap
import os
import sys
import time
import psutil
import gc
import numpy as np
import signal

class ContinuousMemoryTest:
    def __init__(self, size_mb=25, max_cycles=None):
        self.size_mb = size_mb
        self.max_cycles = max_cycles  # None = unlimited
        self.process = psutil.Process()

        # Statistics
        self.cycles_completed = 0
        self.total_mappings = 0
        self.total_bytes_written = 0
        self.start_time = None
        self.running = True

        # Control
        self.memory_mappings = []

    def create_memory_mapping(self, size):
        """Create a memory mapping and track it"""
        try:
            mapping = mmap.mmap(-1, size, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS)
            self.memory_mappings.append(mapping)
            self.total_mappings += 1
            return mapping
        except Exception as e:
            print(f"Failed to create mapping: {e}")
            return None

    def memory_write_cycle(self):
        """Execute one memory write cycle"""
        print(f"[Memory {self.cycles_completed}] Creating and writing...")

        # Create mapping
        mapping = self.create_memory_mapping(128 * 1024)  # 128KB
        if not mapping:
            return

        try:
            mapping.seek(0)

            # Write data in safe chunks
            data_chunk = b"TEST_BLOCK_" * 32  # 352 bytes
            bytes_written = 0

            # Write up to 100 chunks max
            for i in range(100):
                try:
                    mapping.write(data_chunk)
                    bytes_written += len(data_chunk)
                    self.total_bytes_written += len(data_chunk)

                    # Flush every 25 chunks
                    if (i + 1) % 25 == 0:
                        mapping.flush()
                        print(f"  Written {bytes_written} bytes...")

                except Exception:
                    break

            print(f"✓ Successfully wrote {bytes_written} bytes")

        except Exception as e:
            print(f"Write cycle failed: {e}")

        # Read sample for dirty pages
        try:
            mapping.seek(0)
            sample = mapping.read(2048)
            if sample:
                print(f"✓ Generated dirty pages: {len(sample)} bytes")
        except Exception as e:
            print(f"Read failed: {e}")

    def numpy_computation_cycle(self):
        """Execute NumPy computation cycle"""
        print(f"[NumPy {self.cycles_completed}] Computing matrices...")

        created_arrays = 0
        try:
            # Create and process arrays
            for i in range(10):
                arr = np.random.rand(150, 150)  # Smaller arrays
                arr = np.sin(arr) + np.cos(arr)
                arr = np.fft.fft2(arr).real

                created_arrays += 1

                if created_arrays % 5 == 0:
                    print(f"  Processed {created_arrays} arrays...")

            # Force garbage collection
            gc.collect()

            print(f"✓ NumPy cycle: {created_arrays} arrays processed")

        except Exception as e:
            print(f"NumPy cycle failed: {e}")

    def cleanup_mappings(self):
        """Clean up old memory mappings"""
        old_count = len(self.memory_mappings)

        # Keep only last 5 mappings
        while len(self.memory_mappings) > 5:
            try:
                old_mapping = self.memory_mappings.pop(0)
                old_mapping.close()
            except:
                pass

        cleaned = old_count - len(self.memory_mappings)
        if cleaned > 0:
            print(f"🧹 Cleaned up {cleaned} old mappings")

    def print_progress_stats(self):
        """Print current progress and statistics"""
        elapsed_seconds = time.time() - self.start_time
        elapsed_minutes = elapsed_seconds / 60

        print("\n" + "="*50)
        print("CONTINUOUS MEMORY TEST PROGRESS")
        print("="*50)
        print(f"Cycles completed: {self.cycles_completed}")
        print(f"Total mappings created: {self.total_mappings}")
        print(f"Total bytes written: {self.total_bytes_written:,} ({self.total_bytes_written/1024/1024:.2f} MB)")
        print(f"Active mappings: {len(self.memory_mappings)}")
        print(f"Runtime: {elapsed_seconds:.1f} seconds")
        print(f"Average cycles/min: {self.cycles_completed / max(elapsed_minutes, 0.01):.1f}")
        print(f"Write rate: {self.total_bytes_written / max(elapsed_seconds, 1):.0f} bytes/s")
        print("="*50)

    def run_continuous_test(self):
        """Run the continuous repeated test"""
        self.start_time = time.time()

        print("="*60)
        print("CONTINUOUS MEMORY MAPPING TEST - REPEATED")
        print(f"Process PID: {os.getpid()}")
        print(f"Memory size per test: {self.size_mb}MB")
        print(f"Max cycles: {self.max_cycles if self.max_cycles else 'Unlimited'}")
        print("Press Ctrl+C to stop at any time")
        print("="*60)

        try:
            cycle_counter = 0

            while self.running:
                cycle_counter += 1
                self.cycles_completed = cycle_counter

                print(f"\n{'='*25} CYCLE {cycle_counter} {'='*25}")

                # Check maximum cycles limit
                if self.max_cycles and cycle_counter >= self.max_cycles:
                    print(f"⚠️ Reached maximum {self.max_cycles} cycles. Stopping...")
                    break

                # Execute test cycles
                self.memory_write_cycle()

                # Every other cycle also run NumPy test
                if cycle_counter % 2 == 0:
                    time.sleep(0.5)  # Brief pause
                    self.numpy_computation_cycle()

                # Periodic cleanup
                if cycle_counter % 20 == 0:
                    self.cleanup_mappings()

                # Progress reporting
                if cycle_counter % 10 == 0:
                    self.print_progress_stats()

                # Brief pause between cycles
                if cycle_counter < 50 or cycle_counter % 25 != 0:
                    print("⏳ Processing next cycle...")
                    time.sleep(1.0)

        except KeyboardInterrupt:
            print(f"\n⚠️  Interrupted after {self.cycles_completed} cycles")
        except Exception as e:
            print(f"\n❌ Error after {self.cycles_completed} cycles: {e}")
        finally:
            # Final cleanup and statistics
            print(f"\n{'='*60}")
            print("FINALIZING TEST...")
            self.cleanup_mappings()

            # Force garbage collection
            gc.collect()

            # Final statistics
            self.print_progress_stats()

        print("="*60)
        print("CONTINUOUS MEMORY TEST COMPLETED! 🎉")
        print("="*60)

def signal_handler(signum, frame):
    """Signal handler for graceful exit"""
    print(f"\n🛑 Signal {signum} received - shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Install signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Parse command line arguments
    test_size_mb = 25  # Default test size
    max_test_cycles = None  # Unlimited cycles by default

    if len(sys.argv) >= 2:
        test_size_mb = int(sys.argv[1])

    if len(sys.argv) >= 3:
        max_test_cycles = int(sys.argv[2])

    print("⚙️  TEST CONFIGURATION:")
    print(f"   Memory size per mapping: {test_size_mb}MB")
    print(f"   Maximum cycles: {max_test_cycles if max_test_cycles else 'Unlimited'}")

    # Start the continuous test
    test_runner = ContinuousMemoryTest(test_size_mb, max_test_cycles)
    test_runner.run_continuous_test()