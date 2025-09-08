# Makefile for dirty-track testing tools and scripts
# Comprehensive build system for dirty-track kernel module testing framework

# Build settings - 使用标准的GNU C编译选项以确保兼容性
CC=gcc
CFLAGS=-Wall -Wextra -O2 -g
LDFLAGS=
PV=python3

# Target programs
C_TARGETS=user-read-dirtymap user-read-timestamp user-read-warmlist dirty-track-ro
PYTHON_SCRIPTS=dirty-track-comp.py python-jit-emu.py

# External dependencies
SOFT_DIRTY_TARGET=../soft-dirty/soft-dirty
KERNEL_MODULE_TARGET=/lib/modules/$(shell uname -r)/extra/dirty-track.ko

# Default target
.PHONY: all
all: c-targets python-check dependencies

# Build all C programs
.PHONY: c-targets
c-targets: $(C_TARGETS)

.PHONY: c-help
c-help:
	@echo "Available C programs to build:"
	@echo "  user-read-dirtymap    - Read and analyze .dirtymap files from kernel"
	@echo "  user-read-timestamp   - Parse timestamp data from dirty-track output"
	@echo "  user-read-warmlist    - Analyze hot page lists and statistics"
	@echo "  dirty-track-ro        - Original comparison test program (C version)"

# Individual C program builds
user-read-dirtymap: user-read-dirtymap.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built $@ - Tool for reading dirty-map binary files"

user-read-timestamp: user-read-timestamp.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built $@ - Tool for analyzing timestamp data"

user-read-warmlist: user-read-warmlist.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built $@ - Tool for analyzing warm/hot page statistics"

dirty-track-ro: dirty-track-ro.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Built $@ - C version of kernel-based dirty tracking test"

# Python script validation
.PHONY: python-check
python-check:
	@echo "Checking Python environment..."
	@if command -v $(PV) > /dev/null 2>&1; then \
		echo "[OK] Python3 available: $$(python3 --version)"; \
	else \
		echo "[ERROR] Python3 not found"; exit 1; \
	fi
	@if $(PV) -c "import psutil, mmap, numpy; print('[OK] Required Python libraries available')" >/dev/null 2>&1; then \
		echo "[OK] Required Python libraries (psutil, mmap, numpy) available"; \
	else \
		echo "[WARN] Some Python libraries might be missing (psutil, mmap, numpy)"; \
	fi
	@echo "Python scripts: $(PYTHON_SCRIPTS)"

# External dependencies check
.PHONY: dependencies
dependencies: check-soft-dirty check-kernel-module

.PHONY: check-soft-dirty
check-soft-dirty:
	@if [ ! -f $(SOFT_DIRTY_TARGET) ]; then \
		echo "Searching for soft-dirty executable or source..."; \
		if [ -f ../soft-dirty/soft-dirty.c ] || [ -f ../../soft-dirty/soft-dirty.c ]; then \
			if [ -f ../soft-dirty/soft-dirty.c ]; then \
				$(CC) $(CFLAGS) -o $(SOFT_DIRTY_TARGET) ../soft-dirty/soft-dirty.c $(LDFLAGS); \
			else \
				$(CC) $(CFLAGS) -o $(SOFT_DIRTY_TARGET) ../../soft-dirty/soft-dirty.c $(LDFLAGS); \
			fi; \
			echo "[OK] Built soft-dirty program from source"; \
		elif [ -x ../soft-dirty/soft-dirty ] && [ -f ../soft-dirty/soft-dirty ]; then \
			cp ../soft-dirty/soft-dirty $(SOFT_DIRTY_TARGET); \
			echo "[OK] Copied soft-dirty executable"; \
		else \
			echo "[WARN] Soft-dirty not found in expected locations:"; \
			echo "   - ../soft-dirty/soft-dirty.c"; \
			echo "   - ../../soft-dirty/soft-dirty.c"; \
			echo "   - ../soft-dirty/soft-dirty"; \
			echo "   Build it manually: cd /path/to/soft-dirty && gcc -o soft-dirty soft-dirty.c"; \
		fi \
	else \
		echo "[OK] Soft-dirty program already exists at $(SOFT_DIRTY_TARGET)"; \
	fi

.PHONY: check-kernel-module
check-kernel-module:
	@lsmod | grep -q dirty_track && \
		echo "[OK] Dirty-track kernel module is loaded" || \
		(echo "[WARN] Dirty-track kernel module not loaded"; \
		 echo "   To load: cd light-dt && sudo make install"; \
		 echo "   To build: cd light-dt && make clean && make && sudo make install")

# Test targets
.PHONY: test-basic test-comprehensive test-jit
test-basic: c-targets check-soft-dirty
	@echo "Running basic functionality test..."
	@if [ ! -c /dev/dirty-track ]; then \
		echo "[ERROR] /dev/dirty-track device not found - kernel module not loaded"; \
		echo "   Try: cd light-dt && sudo make install"; \
		exit 1; \
	fi
	./dirty-track-ro

test-comprehensive: all
	@echo "Running comprehensive test with both kernel and userspace monitoring..."
	@if [ ! -c /dev/dirty-track ]; then \
		echo "[WARN] /dev/dirty-track device not found - only userspace test will run"; \
	fi
	$(PV) dirty-track-comp.py 50 20

test-jit: python-check
	@echo "Running JIT-style code modification test..."
	$(PV) python-jit-emu.py
	$(PV) python-jit-emu.py --actual-test

# Comparison test between kernel and userspace approaches
.PHONY: compare-modes
compare-modes: test-comprehensive
	@echo "Analyzing test results..."
	@if [ -d /tmp/dirty-memory-test ]; then \
		find /tmp/dirty-memory-test -name "*.dirtymap" -o -name "*.txt" | \
		xargs ls -lh; \
		echo " "; \
		echo "Analysis:"; \
		echo "  - Kernel mode (.dirtymap): Real-time page fault tracking"; \
		echo "  - Userspace mode (.txt): Periodic soft-dirty bit scanning"; \
		echo "  - Compare file sizes and detected dirty pages"; \
	else \
		echo "[ERROR] No test results found"; \
	fi

# Documentation and help
.PHONY: help help-build help-run help-clean
help:
	@echo "Dirty-Track Testing Framework - Build and Run Guide"
	@echo "=================================================="
	@echo ""
	@echo "BUILD TARGETS:"
	@echo "  make all             - Build everything and check dependencies"
	@echo "  make c-targets       - Build all C programs"
	@echo "  make python-check    - Validate Python environment"
	@echo "  make dependencies    - Check and build external dependencies"
	@echo "  make help            - Show this help message"
	@echo ""
	@echo "TEST TARGETS:"
	@echo "  make test-basic      - Run basic C program test"
	@echo "  make test-comprehensive - Full test (kernel + userspace)"
	@echo "  make test-jit        - JIT-style code modification tests"
	@echo "  make compare-modes   - Compare kernel vs userspace results"
	@echo ""
	@echo "PROGRAMS:"
	@echo "  - dirty-track-comp.py  - Python comprehensive comparison tool"
	@echo "  - dirty-track-ro       - C kernel-based dirty tracking test"
	@echo "  - user-read-dirtymap   - Analyze .dirtymap binary files"
	@echo "  - user-read-timestamp  - Parse timestamp monitoring data"
	@echo "  - user-read-warmlist   - Analyze hot page statistics"
	@echo "  - python-jit-emu.py     - JIT code modification simulation"
	@echo ""
	@echo "QUICK START:"
	@echo "  1. make all                    # Build everything"
	@echo "  2. make test-comprehensive     # Run comprehensive test"
	@echo "  3. make compare-modes         # Analyze results"

help-build:
	@echo "BUILD HELP:"
	@echo "==========="
	@echo "## Prerequisites:"
	@echo "  - Linux system with build tools (gcc, make)"
	@echo "  - Python 3.6+ with psutil, mmap, numpy"
	@echo "  - Root access for kernel module loading"
	@echo ""
	@echo "## External Dependencies:"
	@echo "  - dirty-track kernel module (in ../light-dt/)"
	@echo "  - soft-dirty userspace tool (in ../soft-dirty/)"
	@echo ""
	@echo "## Build Process:"
	@echo "  1. make dependencies     # Check and build external tools"
	@echo "  2. make c-targets       # Build local C programs"
	@echo "  3. make python-check    # Validate Python setup"

help-run:
	@echo "RUNTIME HELP:"
	@echo "============="
	@echo "## Before Running Tests:"
	@echo "  sudo make -C ../light-dt install    # Load kernel module"
	@echo "  lsmod | grep dirty_track            # Confirm module loaded"
	@echo "  ls -la /dev/dirty-track             # Confirm device file"
	@echo ""
	@echo "## Test Execution:"
	@echo "  make test-comprehensive             # Full comparison test"
	@echo "  make test-basic                     # Basic kernel test only"
	@echo "  make test-jit                       # JIT modification tests"
	@echo ""
	@echo "## Result Analysis:"
	@echo "  make compare-modes                  # Compare results"
	@echo "  find /tmp/dirty-memory-test -name \"*\"  # List all output files"

help-clean:
	@echo "CLEAN HELP:"
	@echo "==========="
	@echo "## Cleaning Build Artifacts:"
	@echo "  make clean                      # Remove built programs"
	@echo "  sudo make -C ../light-dt clean # Clean kernel module"
	@echo "  rm -rf /tmp/dirty-memory-test   # Remove test results"
	@echo ""
	@echo "## Full Cleanup:"
	@echo "  make distclean                 # Remove everything"

# Enhanced cleaning targets
.PHONY: clean clean-results clean-kernel clean-all distclean
clean:
	rm -f $(C_TARGETS)
	@echo "Cleaned C program binaries"

clean-results:
	rm -rf /tmp/dirty-memory-test
	@echo "Cleaned test result directories"

clean-kernel:
	@if lsmod | grep -q dirty_track; then \
		sudo rmmod dirty_track; \
		echo "Unloaded kernel module"; \
	else \
		echo "Kernel module not loaded"; \
	fi
	@echo "Cleaned kernel module (unload if loaded)"

clean-all: clean clean-results clean-kernel

distclean: clean-all
	@echo "Full cleanup completed"
	@echo "Note: External tools in ../soft-dirty/ and ../light-dt/ not cleaned"

# Status and information
.PHONY: status info version
status:
	@echo "Dirty-Track Testing Framework Status"
	@echo "===================================="
	@echo "C Programs:"
	@for prog in $(C_TARGETS); do \
		if [ -f $$prog ]; then echo "  ✓ $$prog (built)"; \
		else echo "  [NOT BUILT] $$prog (not built)"; fi; \
	done
	@echo "Python Scripts:"
	@for script in $(PYTHON_SCRIPTS); do \
		if [ -f $$script ]; then echo "  [OK] $$script (present)"; \
		else echo "  [MISSING] $$script (missing)"; fi; \
	done
	@echo "Dependencies:"
	@if [ -f $(SOFT_DIRTY_TARGET) ]; then echo "  [OK] Soft-dirty program"; \
	else echo "  [MISSING] Soft-dirty program"; fi
	@if [ -c /dev/dirty-track ]; then echo "  [OK] Kernel module loaded"; \
	else echo "  [MISSING] Kernel module not available"; fi
	@echo "Output Directory: /tmp/dirty-memory-test"

info:
	@echo "Directory Contents:"
	@ls -la
	@echo ""
	@echo "Disk Usage:"
	@du -h . 2>/dev/null || echo "du command not available"

version:
	@echo "Dirty-Track Testing Framework v1.0"
	@echo "=================================="
	@echo "Build Date: $(shell date)"
	@echo "System: $(shell uname -s) $(shell uname -r)"
	@echo "Architecture: $(shell uname -m)"

# Default target alias
.PHONY: build
build: all

# Test shortcuts for different configurations
.PHONY: test-tiny test-small test-medium test-large
test-tiny: all
	$(PV) dirty-track-comp.py 10 5

test-small: all
	$(PV) dirty-track-comp.py 25 10

test-medium: all
	$(PV) dirty-track-comp.py 50 20

test-large: all
	$(PV) dirty-track-comp.py 100 30