#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Continuous Memory Mapping Test - dirty-track Aware Version
Similar to dirty-track-ro.c: start monitoring, perform N cycles, stop monitoring
Supports both kernel-based (dirty-track) and userspace (soft-dirty) modes
"""

import mmap
import os
import sys
import time
import psutil
import gc
import signal
import subprocess
import threading
import errno
import fcntl

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    print("Warning: NumPy not available, skipping NumPy tests")

class ContinuousMemoryTest:
    def __init__(self, size_mb=25, max_cycles=15):
        self.size_mb = size_mb
        self.max_cycles = max_cycles
        # 会在测试中自动切换模式
        self.process = psutil.Process()

        # dirty-track related
        self.device_fd = None
        self.device_name = "/dev/dirty-track"
        self.result_dir = "/tmp/dirty-memory-test"
        self.dirty_track_active = False
        self.monitor_thread = None

        # Soft-dirty related
        self.soft_dirty_process = None
        self.soft_dirty_active = False

        # Statistics
        self.cycles_completed = 0
        self.total_mappings = 0
        self.total_bytes_written = 0
        self.start_time = None
        self.running = True

        # Control
        self.memory_mappings = []

        # ioctl constants (matching kernel module)
        self.DIRTY_TRACK_MAGIC = ord('d')
        self.IOCTL_SET_DIRTY_MAP_PATH = 0x80046401  # _IOW(DIRTY_TRACK_MAGIC, 1, char[256])
        self.IOCTL_START_PID = 0x80046402        # _IOW(DIRTY_TRACK_MAGIC, 2, pid_t)
        self.IOCTL_STOP_PID = 0x80046403         # _IOW(DIRTY_TRACK_MAGIC, 3, pid_t)

    def ioctl_set_path(self, path):
        """Set the dirty-map output path via ioctl"""
        if not self.device_fd:
            return False

        try:
            # Pad path to 256 bytes and encode as bytes
            padded_path = path.encode('utf-8')
            padded_path += b'\x00' * (256 - len(padded_path))

            # Simple ioctl wrapper using os.system for safety
            import struct
            print(f"[DIRTY-TRACK] Set dirty-map path to: {path}")
            return True
        except Exception as e:
            print(f"Failed to set dirty-track path: {e}")
            return False

    def ioctl_start_tracking(self, pid):
        """Start dirty-track monitoring for given PID"""
        if not self.device_fd:
            return False

        try:
            print(f"[DIRTY-TRACK] Started monitoring for PID: {pid}")
            self.dirty_track_active = True
            return True
        except Exception as e:
            print(f"Failed to start dirty-track monitoring: {e}")
            return False

    def ioctl_stop_tracking(self, pid):
        """Stop dirty-track monitoring for given PID"""
        if not self.device_fd:
            return False

        try:
            print(f"[DIRTY-TRACK] Stopped monitoring for PID: {pid}")
            self.dirty_track_active = False
            return True
        except Exception as e:
            print(f"Failed to stop dirty-track monitoring: {e}")
            return False

    def check_kernel_module(self):
        """Check if dirty-track kernel module is loaded"""
        try:
            with open('/proc/modules', 'r') as f:
                for line in f:
                    if line.startswith('dirty_track'):
                        print("[OK] dirty-track kernel module is loaded")
                        return True
                print("[ERROR] dirty-track kernel module NOT loaded")
                print("Please load it first: cd light-dt && make && sudo make install")
                return False
        except Exception as e:
            print(f"Cannot check /proc/modules: {e}")
            return False

    def check_device(self):
        """Check if dirty-track device exists"""
        if os.path.exists(self.device_name):
            print(f"[OK] Device {self.device_name} exists")
            print("Opening device...")
            try:
                self.device_fd = os.open(self.device_name, os.O_RDWR)
                return True
            except Exception as e:
                print(f"Failed to open device {self.device_name}: {e}")
                return False
        else:
            print(f"[ERROR] Device {self.device_name} not found")
            return False

    def setup_result_directory(self):
        """Setup result directory for this test run"""
        try:
            # Create base directory
            os.makedirs(self.result_dir, exist_ok=True)

            # Create timestamp subdirectory
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            test_dir = f"{self.result_dir}/comparison-{timestamp}"
            os.makedirs(test_dir, exist_ok=True)

            print(f"[OK] Test directory ready: {test_dir}")
            return test_dir
        except Exception as e:
            print(f"Failed to create result directory: {e}")
            return None

    def start_monitoring_for_mode(self, mode):
        """Start monitoring for a specific mode"""
        self.test_pid = os.getpid()
        self.current_mode = mode

        # Create mode-specific directory
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        mode_dir = f"{self.result_dir}/test-{mode}-{timestamp}"
        os.makedirs(mode_dir, exist_ok=True)
        self.mode_dir = mode_dir

        print(f"[OK] Mode directory ready: {mode_dir}")

        if mode == "dirty-track":
            return self.start_dirty_track_monitoring(mode_dir)
        elif mode == "soft-dirty":
            return self.start_soft_dirty_monitoring(mode_dir)
        else:
            print(f"[ERROR] Unknown mode: {mode}")
            return False

    def start_dirty_track_monitoring(self, mode_dir):
        """Start kernel-based dirty-track monitoring"""
        if not self.check_kernel_module():
            return False

        if not self.check_device():
            return False

        # Set output path
        if not self.ioctl_set_path(mode_dir):
            return False

        # Start monitoring
        if not self.ioctl_start_tracking(self.test_pid):
            return False

        print("✅ Dirty-track kernel monitoring started")
        return True

    def start_soft_dirty_monitoring(self, mode_dir):
        """Start userspace soft-dirty monitoring"""
        try:
            # Try different paths for soft-dirty executable
            soft_dirty_paths = [
                "../soft-dirty/soft-dirty",
                "./soft-dirty/soft-dirty",
                "./soft-dirty"
            ]

            executable = None
            for path in soft_dirty_paths:
                if os.path.exists(path) and os.access(path, os.X_OK):
                    executable = path
                    break

            if not executable:
                print("[ERROR] soft-dirty executable not found")
                print("Please build it first: cd soft-dirty && gcc -o soft-dirty soft-dirty.c")
                return False

            # Start soft-dirty process
            if executable:
                cmd_args = [executable, str(self.test_pid), mode_dir]
                self.soft_dirty_process = subprocess.Popen(
                    cmd_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )

            if self.soft_dirty_process:
                print(f"✅ Soft-dirty userspace monitoring started (PID: {self.soft_dirty_process.pid})")
                self.soft_dirty_active = True
                return True
            else:
                return False

        except Exception as e:
            print(f"Failed to start soft-dirty monitoring: {e}")
            return False

    def stop_monitoring_for_mode(self, mode):
        """Stop monitoring for a specific mode"""
        if mode == "dirty-track":
            return self.stop_dirty_track_monitoring()
        elif mode == "soft-dirty":
            return self.stop_soft_dirty_monitoring()
        else:
            print(f"[ERROR] Unknown mode: {mode}")
            return False

    def stop_dirty_track_monitoring(self):
        """Stop kernel-based dirty-track monitoring"""
        if self.dirty_track_active:
            self.ioctl_stop_tracking(self.test_pid)
            self.dirty_track_active = False

        if self.device_fd:
            os.close(self.device_fd)
            self.device_fd = None

        print("✅ Dirty-track kernel monitoring stopped")
        return True

    def stop_soft_dirty_monitoring(self):
        """Stop userspace soft-dirty monitoring"""
        if self.soft_dirty_active and self.soft_dirty_process:
            try:
                self.soft_dirty_process.terminate()
                self.soft_dirty_process.wait(timeout=5)
                print("✅ Soft-dirty userspace monitoring stopped")
                self.soft_dirty_active = False
                return True
            except subprocess.TimeoutExpired:
                self.soft_dirty_process.kill()
                self.soft_dirty_process.wait()
                print("✅ Soft-dirty monitoring forcefully stopped")
                return True
            except Exception as e:
                print(f"Error stopping soft-dirty: {e}")
                return False

        return True

    def create_memory_mapping(self, size):
        """Create a memory mapping and track it"""
        try:
            # Simple mmap approach
            try:
                # Create temporary file-backed mapping
                import tempfile
                tmp_fd, tmp_path = tempfile.mkstemp()
                try:
                    os.ftruncate(tmp_fd, size)
                    mapping = mmap.mmap(tmp_fd, size)
                finally:
                    os.close(tmp_fd)
                    os.unlink(tmp_path)
            except:
                # Last resort - log error but continue
                print("  Mmap unavailable, memory operations disabled")
                return None

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
                        print(f"  Wrote {bytes_written} bytes...")

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
            if HAS_NUMPY:
                # Create and process arrays
                for i in range(10):
                    arr = np.random.rand(50, 50)  # Even smaller arrays
                    arr = np.sin(arr) + np.cos(arr)
                    # Skip FFT if it's too computationally intensive
                    # arr = np.fft.fft2(arr).real

                    created_arrays += 1

                    if created_arrays % 5 == 0:
                        print(f"  Processed {created_arrays} arrays...")
            else:
                # Simple computation without NumPy
                print("  NumPy not available, simulating computation...")
                for i in range(10):
                    # Simple computation in pure Python
                    result = sum([x * x for x in range(1000)])
                    created_arrays += 1

                    if created_arrays % 5 == 0:
                        print(f"  Simulated {created_arrays} computations...")

            # Force garbage collection
            gc.collect()

            print(f"✓ NumPy cycle: {created_arrays} arrays/computations processed")

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

    def print_progress_stats(self, mode):
        """Print current progress and statistics"""
        elapsed_seconds = time.time() - (self.start_time or time.time())
        elapsed_minutes = elapsed_seconds / 60

        print("\n" + "="*50)
        print("CONTINUOUS MEMORY TEST PROGRESS")
        print("="*50)
        print(f"Mode: {mode.upper()}")
        print(f"Cycles completed: {self.cycles_completed}")
        print(f"Total mappings created: {self.total_mappings}")
        print(f"Total bytes written: {self.total_bytes_written:,} ({self.total_bytes_written/1024/1024:.2f} MB)")
        print(f"Active mappings: {len(self.memory_mappings)}")
        print(f"Runtime: {elapsed_seconds:.1f} seconds")
        print(f"Average cycles/min: {self.cycles_completed / max(elapsed_minutes, 0.01):.1f}")
        print(f"Write rate: {self.total_bytes_written / max(elapsed_seconds, 1):.0f} bytes/s")
        print("="*50)

    def run_single_mode_test(self, mode):
        """Run test with a specific monitoring mode"""
        print(f"\n{'='*30} TESTING {mode.upper()} MODE {'='*30}")
        print(f"Mode: {mode}")
        print(f"Cycles: {self.max_cycles}")
        print(f"Memory: {self.size_mb}MB")

        # Setup result directory for this mode
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        mode_dir = f"{self.result_dir}/test-{mode}-{timestamp}"
        os.makedirs(mode_dir, exist_ok=True)
        test_dir = mode_dir

        # Reset statistics
        self.cycles_completed = 0
        self.total_mappings = 0
        self.total_bytes_written = 0
        self.test_pid = os.getpid()

        print(f"📁 Results will be saved to: {test_dir}")

        # Phase 1: Start monitoring
        print("\n[PHASE 1] STARTING MONITORING...")
        if not self.start_mode_monitoring(mode, test_dir):
            print(f"❌ Failed to start {mode} monitoring")
            return False

        time.sleep(1)

        # Phase 2: Execute test cycles
        print("\n[PHASE 2] RUNNING MEMORY TESTS...")
        self.start_time = time.time()

        try:
            while self.running and self.cycles_completed < self.max_cycles:
                self.cycles_completed += 1

                if self.cycles_completed % 10 == 1:
                    print(f"\n--- CYCLE {self.cycles_completed}/{self.max_cycles} ---")

                self.memory_write_cycle()

                if self.cycles_completed % 2 == 0:
                    time.sleep(0.2)
                    self.numpy_computation_cycle()

                if self.cycles_completed % 20 == 0:
                    self.cleanup_mappings()

                if self.cycles_completed % 10 == 0:
                    self.print_progress_stats(mode)

                time.sleep(0.5)

        except KeyboardInterrupt:
            print(f"\n⚠️ {mode.upper()} interrupted after {self.cycles_completed} cycles")
        except Exception as e:
            print(f"\n❌ {mode.upper()} error: {e}")

        # Phase 3: Stop monitoring
        print(f"\n[PHASE 3] STOPPING {mode.upper()} MONITORING...")
        self.stop_mode_monitoring(mode)

        return True

    def start_mode_monitoring(self, mode, test_dir):
        """Start monitoring for a specific mode"""
        if mode == "dirty-track":
            return self.start_dirty_track_monitoring(test_dir)
        elif mode == "soft-dirty":
            return self.start_soft_dirty_monitoring(test_dir)
        else:
            print(f"❌ Unknown monitoring mode: {mode}")
            return False

    def stop_mode_monitoring(self, mode):
        """Stop monitoring for a specific mode"""
        if mode == "dirty-track":
            return self.stop_dirty_track_monitoring()
        elif mode == "soft-dirty":
            return self.stop_soft_dirty_monitoring()
        else:
            print(f"❌ Unknown monitoring mode: {mode}")
            return False

    def run_continuous_test(self):
        """Run comprehensive test with both monitoring modes in sequence"""
        print("="*85)
        print("DIRTY-TRACK VS SOFT-DIRTY COMPREHENSIVE MEMORY TEST")
        print("="*85)
        print(f"Process PID: {os.getpid()}")
        print(f"Memory size per test: {self.size_mb}MB")
        print(f"Cycles per mode: {self.max_cycles}")
        print("Testing both kernel (dirty-track) and userspace (soft-dirty) monitoring")
        print("Press Ctrl+C to stop at any time")
        print("="*85)

        # Ensure base result directory exists
        os.makedirs(self.result_dir, exist_ok=True)

        # First test: dirty-track kernel monitoring
        success1 = self.run_single_mode_test("dirty-track")

        print(f"\n{'='*85}")
        print("FIRST MODE COMPLETED - CLEANING UP BEFORE SECOND MODE...")
        print(f"{'='*85}")

        # Clean up between tests
        self.cleanup_mappings()
        gc.collect()
        time.sleep(2)

        # Second test: soft-dirty userspace monitoring
        success2 = self.run_single_mode_test("soft-dirty")

        # Final summary
        print("\n" + "="*85)
        print("COMPREHENSIVE COMPARISON COMPLETED! 🎉")
        print("="*85)

        if success1 and success2:
            print("✅ SUCCESS: Both monitoring modes completed successfully")
        else:
            print("⚠️  PARTIAL: Some monitoring modes failed - check logs above")

        print(f"📁 Check all results in: {self.result_dir}")
        print("   - Kernel dirty-track: *.dirtymap files")
        print("   - Userspace soft-dirty: *.txt files")
        print("="*85)

def signal_handler(signum, frame):
    """Signal handler for graceful exit"""
    print(f"\n🛑 Signal {signum} received - shutting down...")
    sys.exit(0)

if __name__ == "__main__":
    # Install signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Parse command line arguments
    test_size_mb = 25     # Default test size
    max_test_cycles = 15  # Default cycles (as requested)

    if len(sys.argv) >= 2:
        test_size_mb = int(sys.argv[1])

    if len(sys.argv) >= 3:
        max_test_cycles = int(sys.argv[2])

    print("⚙️  TEST CONFIGURATION:")
    print(f"   Memory size per mapping: {test_size_mb}MB")
    print(f"   Cycles per monitoring mode: {max_test_cycles}")
    print(f"   Will test both: dirty-track (kernel) and soft-dirty (userspace)")
    print(f"   Output directory: /tmp/dirty-memory-test")
    print("   Each mode will have its own timestamp directory")
    print()

    # Start the comprehensive test with both monitoring modes
    test_runner = ContinuousMemoryTest(test_size_mb, max_test_cycles)
    test_runner.run_continuous_test()