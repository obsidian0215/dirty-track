#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <signal.h>

#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS 0x20  // Anonymous mapping flag
#endif

// dirty-track ioctl定义
#define DIRTY_TRACK_MAGIC 'd'
#define IOCTL_SET_DIRTY_MAP_PATH _IOW(DIRTY_TRACK_MAGIC, 1, char[256])
#define IOCTL_START_PID _IOW(DIRTY_TRACK_MAGIC, 2, pid_t)
#define IOCTL_STOP_PID _IOW(DIRTY_TRACK_MAGIC, 3, pid_t)

#define DEVICE_NAME "/dev/dirty-track"
#define TMP_DIR "/tmp/dirty-maps"

// ioctl 函数定义
int ioctl_set_path(int fd, const char *path) {
    char padded_path[256];
    memset(padded_path, 0, sizeof(padded_path));
    strncpy(padded_path, path, sizeof(padded_path) - 1);

    if (ioctl(fd, IOCTL_SET_DIRTY_MAP_PATH, padded_path) < 0) {
        perror("ioctl SET_DIRTY_MAP_PATH failed");
        return -1;
    }
    printf("Set dirty-map path to: %s\n", path);
    return 0;
}

int ioctl_start_tracking(int fd, pid_t pid) {
    if (ioctl(fd, IOCTL_START_PID, &pid) < 0) {
        perror("ioctl START_PID failed");
        return -1;
    }
    printf("[START] Started dirty-track monitoring for PID: %d\n", pid);
    return 0;
}

int ioctl_stop_tracking(int fd, pid_t pid) {
    if (ioctl(fd, IOCTL_STOP_PID, &pid) < 0) {
        perror("ioctl STOP_PID failed");
        return -1;
    }
    printf("[STOP] Stopped dirty-track monitoring for PID: %d\n", pid);
    return 0;
}

// 测试场景结构体
typedef struct {
    char test_name[64];
    char description[256];
    int cycles;
    void (*test_func)(int cycles);
} test_scenario_t;

// 结果统计
typedef struct {
    char scenario[64];
    int device_fd;
    pid_t test_pid;
    time_t start_time;
    time_t end_time;
    char result_dir[256];
    int success;
} test_result_t;

// 测试场景1：权限变化测试
void run_permission_change_test(int cycles) {
    printf("\n[Test] EXECUTING PERMISSION CHANGE TEST\n");

    void *test_mapping = mmap(NULL, 4096, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (test_mapping == MAP_FAILED) {
        perror("Failed to create mapping");
        printf("  [WARN] mmap failed, skipping write operations\n");
        return;
    }

    printf("  Created readonly mapping at %p\n", test_mapping);
    printf("  Running with %d cycles for dirty page generation\n", cycles);

    // 第一阶段：将权限改为可写以建立页表项
    printf("  Phase 1: Change to writable to establish page table entries\n");
    if (mprotect(test_mapping, 4096, PROT_READ | PROT_WRITE) != 0) {
        perror("    Failed to change permission to read-write for PTE setup");
        munmap(test_mapping, 4096);
        return;
    }

    // 建立页表项的写操作
    char *ptr = (char *)test_mapping;
    for (int page = 0; page < 1; page++) {  // 只写第一页以建立页表项
        ptr[page] = 'A';  // 写入一个字节
        printf("    Wrote byte 0x%02x at offset %d\n", ptr[page], page);
    }

    // 改变权限为只读 (模拟实时权限变化)
    printf("  Phase 2: Changing permissions to read-only\n");
    if (mprotect(test_mapping, 4096, PROT_READ) == 0) {
        printf("    Changed permission to readonly\n");
    } else {
        perror("    Failed to change permission to readonly");
        munmap(test_mapping, 4096);
        return;
    }

    // 第二阶段：大量循环写入以触发dirty-tracking
    printf("  Phase 3: Cyclic write operations to trigger dirty tracking\n");
    if (mprotect(test_mapping, 4096, PROT_READ | PROT_WRITE) == 0) {
        printf("    Changed permission back to read-write for dirty-tracking\n");

        // 执行指定次数（cycles）的写入循环
        for (int cycle = 0; cycle < cycles; cycle++) {
            int offset = cycle % 4096;
            ptr[offset] = (char)(cycle % 256);
            if (cycle < 10 || cycle % (cycles / 10 + 1) == 0) {  // 自适应显示进度
                printf("    Cycle %d: Wrote 0x%02x at offset %d\n", cycle + 1, ptr[offset], offset);
            }
        }
        printf("    Completed %d write cycles to generate dirty pages\n", cycles);
    } else {
        perror("    Failed to change permission to read-write");
        munmap(test_mapping, 4096);
        return;
    }

    sleep(1);  // 短暂等待确保light-dt处理完成

    munmap(test_mapping, 4096);
    printf("  Permission change test completed with %d dirty writes\n", cycles);
}

// 测试场景2：JIT代码修改测试
void run_jit_code_test(int cycles) {
    printf("\n[Test] EXECUTING JIT CODE MODIFICATION TEST\n");

    // 创建RWX内存（JIT样式）
    void *code_mapping = mmap(NULL, 4096, PROT_READ | PROT_WRITE | PROT_EXEC,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (code_mapping == MAP_FAILED) {
        perror("Failed to create code mapping");
        printf("  [WARN] RWX mmap failed, skipping JIT simulation\n");
        return;
    }

    printf("  Created RWX mapping at %p\n", code_mapping);
    printf("  Running JIT test with %d optimization cycles\n", cycles);

    // 构建简单的x86-64指令序列
    unsigned char machine_code[] = {
        0x48, 0xC7, 0xC0, 0x2A, 0x00, 0x00, 0x00,  // mov rax, 42
        0xC3                                            // ret
    };

    // 第一阶段：多代码段写入
    printf("  Phase 1: Writing multiple code segments\n");
    unsigned char *code_ptr = (unsigned char *)code_mapping;
    const int NUM_SEGMENTS = 10;

    // 确保页表项建立 - 先写入一个字节
    code_ptr[0] = machine_code[0];
    printf("    Initial byte written to establish PTE\n");

    // 写入初始代码段
    for (int i = 0; i < NUM_SEGMENTS; i++) {
        memcpy(code_ptr + i * sizeof(machine_code), machine_code, sizeof(machine_code));
        if (i < 5) {  // 只打印前5个
            printf("    Copied machine code segment %d\n", i + 1);
        }
    }
    printf("    Total: %d code segments prepared\n", NUM_SEGMENTS);

    // 第二阶段：JIT优化循环
    printf("  Phase 2: JIT optimization loops\n");

    for (int cycle = 0; cycle < cycles; cycle++) {
        // 选择一个代码段进行修改
        int segment_idx = cycle % NUM_SEGMENTS;
        unsigned char *segment_start = code_ptr + segment_idx * sizeof(machine_code);
        unsigned long new_value = cycle + 42;  // 从42开始递增

        // 修改指令中的立即数
        segment_start[0] = 0x48;  // mov rax, imm64
        segment_start[1] = 0xC7;
        segment_start[2] = 0xC0;
        segment_start[3] = (new_value >> 0) & 0xFF;   // 低8位
        segment_start[4] = (new_value >> 8) & 0xFF;   // 8-15位
        segment_start[5] = (new_value >> 16) & 0xFF;  // 16-23位
        segment_start[6] = (new_value >> 24) & 0xFF;  // 24-31位
        segment_start[7] = 0xC3;  // ret

        if (cycle < 5 || cycle % (cycles / 10 + 1) == 0) {  // 自适应显示进度
            printf("    JIT Cycle %d: Modified segment %d to mov rax, %lu\n",
                   cycle + 1, segment_idx, new_value);
        }
    }

    printf("  Completed %d JIT optimization loops across %d code segments\n",
           cycles, NUM_SEGMENTS);

    // 第三阶段：最终权限调整模拟JIT完成
    printf("  Phase 3: Final permission adjustments\n");
    if (mprotect(code_mapping, 4096, PROT_READ | PROT_WRITE) == 0) {
        printf("    Changed to read-write only (JIT completion)\n");

        // 最后几轮只读修改，观察权限变化对dirty-tracking的影响
        const int FINAL_WRITES = 10;
        for (int final_write = 0; final_write < FINAL_WRITES; final_write++) {
            code_ptr[final_write] = (unsigned char)final_write;
            printf("    Final modification %d: Wrote 0x%02x at position %d\n",
                   final_write + 1, final_write, final_write);
        }
        printf("    Added %d final modifications\n", FINAL_WRITES);
    }

    sleep(1);  // 短暂等待确保light-dt处理完成

    munmap(code_mapping, 4096);
    printf("  JIT code modification test completed with %d optimizations\n",
           cycles + 10);
}

// 测试场景定义
test_scenario_t test_scenarios[] = {
    {
        "permission-change",
        "Testing permission changes with cyclic writes",
        0,  // cycles will be set dynamically
        NULL // test_func will be set dynamically
    },
    {
        "jit-code-modification",
        "Testing JIT-style code generation with optimization loops",
        0,  // cycles will be set dynamically
        NULL // test_func will be set dynamically
    }
};

#define NUM_SCENARIOS ((size_t)(sizeof(test_scenarios) / sizeof(test_scenario_t)))

int setup_test_environment(char *result_dir, size_t dir_size) {
    struct stat st = {0};

    strcpy(result_dir, TMP_DIR);

    // 检查基础目录
    if (stat(result_dir, &st) == -1) {
        printf("[INFO] Creating base directory: %s\n", result_dir);
        if (mkdir(result_dir, 0755) < 0) {
            perror("mkdir failed for base path");
            return -1;
        }
    }

    // 添加时间戳子目录
    struct timeval tv;
    gettimeofday(&tv, NULL);
    char time_suffix[32];
    struct tm *time_info = localtime(&tv.tv_sec);
    strftime(time_suffix, sizeof(time_suffix), "%Y%m%d_%H%M%S", time_info);

    snprintf(result_dir, dir_size, "%s/test-comparison-%s", TMP_DIR, time_suffix);

    if (mkdir(result_dir, 0755) < 0) {
        perror("mkdir failed for test directory");
        return -1;
    }

    printf("[OK] Test directory ready: %s\n", result_dir);
    return 0;
}

int check_module_loaded(void) {
    FILE *proc_modules = fopen("/proc/modules", "r");
    if (!proc_modules) {
        perror("Cannot check /proc/modules");
        return -1;
    }

    char line[512];
    int found = 0;

    while (fgets(line, sizeof(line), proc_modules)) {
        if (strstr(line, "dirty_track") == line) {
            found = 1;
            break;
        }
    }

    fclose(proc_modules);

    if (found) {
        printf("[OK] dirty-track kernel module is loaded\n");
        return 0;
    } else {
        printf("[ERROR] dirty-track kernel module is NOT loaded\n");
        printf("   You need to load the kernel module first:\n");
        printf("   cd light-dt && make && sudo make install\n");
        return -1;
    }
}

int check_device_exists(void) {
    if (access(DEVICE_NAME, F_OK) == 0) {
        printf("[OK] Device %s exists\n", DEVICE_NAME);
        return 0;
    } else {
        printf("[ERROR] Device %s not found\n", DEVICE_NAME);
        printf("   Check if kernel module is loaded and device created\n");
        return -1;
    }
}

int run_test_scenario(test_scenario_t *scenario, int device_fd, pid_t pid, char *result_dir) {
    printf("\n%s\n", scenario->description);
    printf("--------------------------------------------------\n");
    printf("Scenario: %s, PID: %d\n", scenario->test_name, pid);

    // 为每个场景创建独立的目录
    char scenario_result_dir[512];
    snprintf(scenario_result_dir, sizeof(scenario_result_dir), "%s/%s", result_dir, scenario->test_name);

    if (mkdir(scenario_result_dir, 0755) < 0 && errno != EEXIST) {
        perror("Failed to create scenario directory");
        return -1;
    }

    // 执行测试时分别使用light-dt和soft-dirty

    // Phase 1: 使用内核脏页跟踪 (light-dt kernel module)
    printf("  Running with KERNEL-BASED dirty tracking (light-dt module)...\n");

    // 设置light-dt输出路径
    if (ioctl_set_path(device_fd, scenario_result_dir) < 0) {
        return -1;
    }

    // 启动内核脏页跟踪监控
    if (ioctl_start_tracking(device_fd, pid) < 0) {
        return -1;
    }

    // 执行测试
    scenario->test_func(scenario->cycles);

    // 等待light-dt处理
    sleep(2);

    // 停止内核脏页跟踪监控
    if (ioctl_stop_tracking(device_fd, pid) < 0) {
        return -1;
    }

    printf("  Kernel dirty-tracking completed for scenario '%s'\n", scenario->test_name);

    // Phase 2: 使用soft-dirty用户空间监控（重新执行相同的测试）
    printf("  Running with USERSPACE dirty tracking (soft-dirty tool)...\n");

    // 创建soft-dirty输出文件路径
    char soft_dirty_output[1024];
    snprintf(soft_dirty_output, sizeof(soft_dirty_output), "%s/soft-dirty-%d-%s.txt",
             scenario_result_dir, pid, scenario->test_name);

    // 启动soft-dirty监控
    pid_t soft_dirty_pid = start_soft_dirty_monitoring(pid, scenario_result_dir, soft_dirty_output);
    if (soft_dirty_pid < 0) {
        printf("  [WARN] Failed to start soft-dirty monitoring\n");
    } else {

        // 重新执行相同的测试
        scenario->test_func(scenario->cycles);

        // 等待soft-dirty处理（给更多时间来完成监控和写入）
        sleep(2);

        // 停止soft-dirty监控
        stop_soft_dirty_monitoring(soft_dirty_pid);
        printf("  Soft-dirty monitoring completed for scenario '%s'\n", scenario->test_name);
    }

    printf(" Test scenario '%s' completed\n", scenario->test_name);
    return 0;
}

int analyze_comparison_results(char *result_dir) {
    printf("\n=======================================================\n");
    printf("\n[ANALYSIS] COMPARISON ANALYSIS\n");
    printf("=======================================================\n");

    for (int i = 0; i < NUM_SCENARIOS; i++) {
        test_scenario_t *scenario = &test_scenarios[i];
        printf("\n%s:", scenario->test_name);

        char scenario_dir[1024];
        snprintf(scenario_dir, sizeof(scenario_dir), "%s/%s", result_dir, scenario->test_name);

        // 统计light-dt结果
        char cmd[1024];
        sprintf(cmd, "find %s -name '*.dirtymap' | wc -l", scenario_dir);
        FILE *pipe = popen(cmd, "r");
        int light_dt_files = 0;
        if (pipe) {
            char line[128];
            fgets(line, sizeof(line), pipe);
            light_dt_files = atoi(line);
            pclose(pipe);
        }

        // 统计soft-dirty结果
        sprintf(cmd, "find %s -name '*.txt' | wc -l", scenario_dir);
        pipe = popen(cmd, "r");
        int soft_dirty_files = 0;
        if (pipe) {
            char line[128];
            fgets(line, sizeof(line), pipe);
            soft_dirty_files = atoi(line);
            pclose(pipe);
        }

        printf(" Light-dt files: %d", light_dt_files);
        printf(" | Soft-dirty files: %d", soft_dirty_files);
        printf(" | Cycles used: %d", scenario->cycles);
        printf(" | %s", scenario->description);
    }

    printf("\n\n RECOMMENDATIONS:\n");
    printf("   - Compare actual dirty page counts vs expected high-volume writes\n");
    printf("   - Check if cyclic write patterns are properly tracked\n");
    printf("   - Verify JIT-style sequential modifications are captured\n");
    printf("   - Compare light-dt vs soft-dirty detection accuracy with real memory access\n");
    printf("   - Analyze the impact of page table entry establishment timing\n");

    return 0;
}

// 启动soft-dirty监控进程
pid_t start_soft_dirty_monitoring(pid_t target_pid, const char *output_dir, const char *output_file) {
    char pid_str[32];
    snprintf(pid_str, sizeof(pid_str), "%d", target_pid);

    pid_t child_pid = fork();

    if (child_pid == 0) {
        // 子进程：启动soft-dirty监控

        // 设置进程组，以便可以被杀死
        setpgid(0, 0);

        // 尝试调用soft-dirty程序的不同路径
        // 从experiment/dirty-track目录向上查找soft-dirty目录
        const char *soft_dirty_paths[] = {
            "../soft-dirty/soft-dirty",              // 从dirty-track上级目录查找
            "../../soft-dirty/soft-dirty",           // 从experiment上级目录查找
            "../soft-dirty",                         // 程序名在当前soft-dirty目录
            NULL
        };

        int i = 0;
        while (soft_dirty_paths[i]) {
            if (access(soft_dirty_paths[i], X_OK) == 0) {
                printf("Starting soft-dirty for PID %d with output dir: %s\n", target_pid, output_dir);
                execl(soft_dirty_paths[i], "soft-dirty", pid_str, output_dir, (char *)NULL);

                // 如果execl失败但程序存在
                if (errno != ENOENT) {
                    perror("[SOFT-DIRTY] execl failed");
                }
                break;
            }
            i++;
        }

        // 如果没有找到可执行文件，等待5秒后退出
        if (!soft_dirty_paths[i]) {
            printf("soft-dirty Executable not found, waiting 5 seconds and exiting...\n");
            sleep(2);
            printf("soft-dirty Monitoring completed (executable not found)\n");
        }

        exit(0);
    } else if (child_pid > 0) {
        printf("soft-dirty Monitor process started with PID: %d\n", child_pid);
        return child_pid;
    } else {
        perror("fork failed for soft-dirty");
        return -1;
    }
}

// 停止soft-dirty监控
int stop_soft_dirty_monitoring(pid_t monitor_pid) {
    if (monitor_pid <= 0) {
        return 0;
    }

    // 发送SIGTERM到进程组
    if (killpg(monitor_pid, SIGTERM) == 0) {
        // 等待进程终止
        int status;
        waitpid(monitor_pid, &status, 0);
        printf("soft-dirty Monitor process stopped\n");
        return 0;
    } else {
        perror("Failed to stop soft-dirty process");
        return -1;
    }
}

int main(int argc, char *argv[]) {
    int device_fd;
    pid_t test_pid = getpid();
    char result_dir[512];

    // 解析命令行参数，设置循环次数
    int cycles = 20;  // 默认循环次数

    if (argc >= 2) {
        cycles = atoi(argv[1]);
        if (cycles <= 0) {
            printf("Invalid cycle count: %s, using default: %d\n", argv[1], 50);
            cycles = 20;
        }
    }

    // 动态设置测试场景参数
    test_scenarios[0].cycles = cycles;
    test_scenarios[0].test_func = run_permission_change_test;
    test_scenarios[1].cycles = cycles;
    test_scenarios[1].test_func = run_jit_code_test;

    printf("[DIRTY-TRACK] MONITORING METHOD COMPARISON TEST\n");
    printf("Comparing kernel-based (light-dt) vs userspace (soft-dirty) dirty tracking\n");
    printf("Test cycles per scenario: %d (default: %d)\n", cycles, 50);
    printf("Usage: %s [cycles]\n", argv[0]);
    printf("Test scenarios: permission-change vs JIT code modification\n");
    printf("======================================================================\n");
    printf("\n");

    // Phase 0: 初始化检查
    printf("--- PHASE 0: ENVIRONMENT CHECKS ---\n");

    // 检查内核模块状态
    if (check_module_loaded() < 0) {
        printf("ERROR: Cannot proceed - kernel module required\n");
        return 1;
    }

    // 检查设备可用性
    if (check_device_exists() < 0) {
        printf("[ERROR] Cannot proceed - device not available\n");
        return 1;
    }

    // 创建测试结果目录
    if (setup_test_environment(result_dir, sizeof(result_dir)) < 0) {
        printf("[ERROR] Cannot create test directory\n");
        return 1;
    }

    // 打开dirty-track设备
    device_fd = open(DEVICE_NAME, O_RDWR);
    if (device_fd < 0) {
        perror("Cannot open dirty-track device");
        return 1;
    }

    printf("[OK] Environment setup complete\n");

    // 执行第一个测试场景（分别用light-dt和soft-dirty）
    printf("\n--- PHASE 1: FIRST SCENARIO (%s) ---\n", test_scenarios[0].test_name);
    printf("Executing %s with both monitoring methods\n", test_scenarios[0].description);
    test_scenario_t *first_scenario = &test_scenarios[0];
    if (run_test_scenario(first_scenario, device_fd, test_pid, result_dir) < 0) {
        printf("[ERROR] First test scenario failed\n");
        close(device_fd);
        return 1;
    }

    // 执行第二个测试场景（分别用light-dt和soft-dirty）
    printf("\n--- PHASE 2: SECOND SCENARIO (%s) ---\n", test_scenarios[1].test_name);
    printf("Executing %s with both monitoring methods\n", test_scenarios[1].description);
    test_scenario_t *second_scenario = &test_scenarios[1];
    if (run_test_scenario(second_scenario, device_fd, test_pid, result_dir) < 0) {
        printf("[ERROR] Second test scenario failed\n");
        close(device_fd);
        return 1;
    }

    close(device_fd);

    // Phase 3: 分析和比较结果
    printf("\n--- PHASE 3: RESULT ANALYSIS ---\n");
    analyze_comparison_results(result_dir);

    printf("\n======================================================================\n");
    printf("\n[SUCCESS] COMPREHENSIVE COMPARISON COMPLETED\n");
    printf("[RESULT] Light-dt and Soft-dirty results stored in: %s\n", result_dir);
    printf("[ANALYSIS] Compare both monitoring methods across different scenarios\n");
    printf("======================================================================\n");
    printf("\n");

    printf("SUMMARY OF MONITORING METHODS:\n");
    printf("----------------------------------------------------------------------\n");
    printf("[LIGHT-DT] Kernel module, real-time page fault tracking\n");
    printf("[SOFT-DIRTY] Userspace, periodic soft-dirty bit scanning\n");
    printf("Both methods output to unified directory structure for comparison\n");
    printf("\n");

    return 0;
}