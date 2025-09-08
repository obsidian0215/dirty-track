#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/ioctl.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>

// ioctl 常量定义，与内核模块匹配
#define DIRTY_TRACK_MAGIC 'd'
#define IOCTL_SET_DIRTY_MAP_PATH _IOW(DIRTY_TRACK_MAGIC, 1, char[256])
#define IOCTL_START_PID _IOW(DIRTY_TRACK_MAGIC, 2, pid_t)
#define IOCTL_STOP_PID _IOW(DIRTY_TRACK_MAGIC, 3, pid_t)

#define DEVICE_NAME "/dev/dirty-track"
#define TEST_SIZE (4 * 1024)  // 4KB 测试
#define CODE_SIZE (1024)      // 1KB 代码段

typedef void (*func_ptr_t)(void);

int ioctl_start_tracking(int fd, pid_t pid) {
    if (ioctl(fd, IOCTL_START_PID, &pid) < 0) {
        perror("ioctl START_PID failed");
        return -1;
    }
    printf("Started tracking PID: %d\n", pid);
    return 0;
}

int ioctl_stop_tracking(int fd, pid_t pid) {
    if (ioctl(fd, IOCTL_STOP_PID, &pid) < 0) {
        perror("ioctl STOP_PID failed");
        return -1;
    }
    printf("Stopped tracking PID: %d\n", pid);
    return 0;
}

// 模拟代码段
__attribute__((aligned(4096))) char simulated_code[CODE_SIZE] = {
    // 简单的 x86-64 指令：mov rax, 42; ret;
    0x48, 0xC7, 0xC0, 0x2A, 0x00, 0x00, 0x00,  // mov rax, 42
    0xC3                                      // ret
};

int main(int argc, char *argv[]) {
    int device_fd;
    pid_t pid = getpid();
    void *code_mapping;
    func_ptr_t func;
    int result;

    setuid(getuid());  // 确保不是 root 权限

    printf("=== JIT/动态代码修改测试 ===\n");
    printf("PID: %d\n", pid);

    // 打开设备
    device_fd = open(DEVICE_NAME, O_RDWR);
    if (device_fd < 0) {
        perror("Cannot open dirty-track device");
        printf("Make sure the kernel module is loaded\n");
        return 1;
    }

    // 启动跟踪 - 先启动跟踪来监控映射创建
    if (ioctl_start_tracking(device_fd, pid) < 0) {
        close(device_fd);
        return 1;
    }

    printf("\n--- PHASE 1: 创建可执行内存映射 ---\n");

    // 创建可执行内存段 (JIT 风格)
    code_mapping = mmap(NULL, CODE_SIZE, PROT_READ | PROT_WRITE | PROT_EXEC,
                       MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (code_mapping == MAP_FAILED) {
        perror("mmap for code failed");
        ioctl_stop_tracking(device_fd, pid);
        close(device_fd);
        return 1;
    }

    printf("Created executable mapping at %p\n", code_mapping);

    printf("\n--- PHASE 2: 初始代码写入 (JIT 编译) ---\n");

    // 复制模拟代码 (像 JIT 生成代码)
    memcpy(code_mapping, simulated_code, sizeof(simulated_code));

    // 测试执行
    func = (func_ptr_t)code_mapping;
    result = func();
    printf("Initial function executed, returned: %d\n", result);

    printf("\n--- PHASE 3: 动态代码修改 ---\n");

    // 模拟 JIT 优化或运行时代码修改
    char *code_ptr = (char *)code_mapping;

    // 修改返回值为 100 (原本返回 42)
    code_ptr[3] = 0x64;  // 修改立即数为 100

    printf("Modified code at %p (JIT-style code update)\n", code_ptr);

    // 测试修改后的执行
    result = func();
    printf("Modified function executed, returned: %d\n", result);

    printf("\n--- PHASE 4: 权限调整模拟 ---\n");

    // 模拟 JIT 的权限调整：写入后改为执行
    if (mprotect(code_mapping, CODE_SIZE, PROT_READ | PROT_EXEC) < 0) {
        perror("mprotect to read-exec failed");
    } else {
        printf("Changed permission to read-exec only\n");
    }

    // 测试最终执行
    result = func();
    printf("Final function executed, returned: %d\n", result);

    printf("\n--- PHASE 5: CLEANUP ---\n");

    // 清理映射
    munmap(code_mapping, CODE_SIZE);

    sleep(2);  // 等待 dirty-track 处理

    // 停止跟踪
    ioctl_stop_tracking(device_fd, pid);

    close(device_fd);

    printf("\nJIT/动态代码修改测试完成!\n");
    printf("检查 /tmp/dirty-maps 目录中的结果文件\n");

    return 0;
}