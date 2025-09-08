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
#define IOCTL_CHECK_PID _IOWR(DIRTY_TRACK_MAGIC, 4, struct pid_check)

struct pid_check {
    pid_t pid;
    bool is_tracked;
};

#define DEVICE_NAME "/dev/dirty-track"
#define TEST_SIZE (4 * 1024)  // 4KB 测试
#define TMP_DIR "/tmp/dirty-maps"

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

void *create_readonly_mapping(void) {
    char *mapping;

    // 创建匿名只读映射
    mapping = mmap(NULL, TEST_SIZE, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        perror("mmap PROT_READ failed");
        return NULL;
    }

    // 预填充数据
    memset(mapping, 'R', TEST_SIZE);

    printf("Created readonly mapping at %p, size %d\n", mapping, TEST_SIZE);
    return mapping;
}

int change_permission(void *addr, size_t size, int new_prot) {
    if (mprotect(addr, size, new_prot) < 0) {
        perror("mprotect failed");
        return -1;
    }

    printf("Changed permission to %s at %p\n",
           new_prot == PROT_READ ? "readonly" :
           new_prot == (PROT_READ | PROT_WRITE) ? "read-write" : "unknown",
           addr);
    return 0;
}

int test_write_to_mapping(void *addr, char value) {
    char *ptr = (char *)addr;

    // 尝试写入
    *ptr = value;

    printf("Write succeeded: wrote '%c' to %p\n", value, ptr);
    return 0;
}

void print_vma_info(pid_t pid) {
    char maps_path[256];
    FILE *maps_file;
    char line[512];

    printf("Current VMA (Virtual Memory Areas) for PID %d:\n", pid);

    snprintf(maps_path, sizeof(maps_path), "/proc/%d/maps", pid);
    maps_file = fopen(maps_path, "r");

    if (!maps_file) {
        perror("Failed to open /proc/pid/maps");
        return;
    }

    while (fgets(line, sizeof(line), maps_file)) {
        // 只显示包含堆栈和映射的行
        if (strstr(line, "rw-p") || strstr(line, "r--p") || strstr(line, "r-xp") ||
            strstr(line, "rwxp")) {
            printf("  %s", line);
        }
    }

    fclose(maps_file);
    printf("VMA information displayed above.\n");
}

unsigned long get_mapping_start_addr(void *mapping_addr) {
    char maps_path[256];
    FILE *maps_file;
    char line[512];
    void *start_addr = NULL;

    pid_t pid = getpid();
    snprintf(maps_path, sizeof(maps_path), "/proc/%d/maps", pid);
    maps_file = fopen(maps_path, "r");

    if (!maps_file) {
        perror("Failed to open /proc/pid/maps");
        return 0;
    }

    while (fgets(line, sizeof(line), maps_file)) {
        unsigned long addr_start, addr_end;
        if (sscanf(line, "%lx-%lx", &addr_start, &addr_end) == 2) {
            void *addr_start_ptr = (void *)addr_start;
            if (mapping_addr >= addr_start_ptr && mapping_addr < (void *)addr_end) {
                start_addr = addr_start_ptr;
                break;
            }
        }
    }

    fclose(maps_file);
    return (unsigned long)start_addr;
}

int main(int argc, char *argv[]) {
    int device_fd;
    pid_t pid = getpid();
    void *test_mapping;
    char tmp_dir[256];

    setuid(getuid());  // 确保不是 root 权限

    printf("=== DIRTY-TRACK PERMISSION CHANGE TEST ===\n");
    printf("PID: %d\n", pid);

    // 设置测试目录
    snprintf(tmp_dir, sizeof(tmp_dir), "%s/permission-test", TMP_DIR);

    // 创建目录
    if (mkdir(tmp_dir, 0755) < 0 && errno != EEXIST) {
        perror("mkdir failed");
        return 1;
    }

    // 打开设备
    device_fd = open(DEVICE_NAME, O_RDWR);
    if (device_fd < 0) {
        perror("Cannot open dirty-track device");
        printf("Make sure the kernel module is loaded\n");
        return 1;
    }

    // 设置 dirty-map 保存路径
    if (ioctl_set_path(device_fd, tmp_dir) < 0) {
        close(device_fd);
        return 1;
    }

    // 启动跟踪
    if (ioctl_start_tracking(device_fd, pid) < 0) {
        close(device_fd);
        return 1;
    }

    printf("\n--- PHASE 1: INITIAL READONLY MAPPING ---\n");

    // 创建只读映射
    test_mapping = create_readonly_mapping();
    if (!test_mapping) {
        ioctl_stop_tracking(device_fd, pid);
        close(device_fd);
        return 1;
    }

    // 输出映射的VMA起始地址
    unsigned long vma_start = get_mapping_start_addr(test_mapping);
    if (vma_start) {
        printf("✅ Mapping VMA start address: 0x%lx\n", vma_start);
    }

    // 显示当前VMA状态
    printf("Current VMA status:\n");
    print_vma_info(pid);

    // 验证只读映射 - 尝试写入应该失败
    printf("Testing write to readonly mapping (expect failure): ");
    if (test_write_to_mapping(test_mapping, 'W') < 0) {
        printf("Write failed as expected (permission denied)\n");
    } else {
        printf("WARNING: Write succeeded unexpectedly!\n");
    }

    printf("\n--- PHASE 2: PERMISSION CHANGE ---\n");

    // 改变权限为读写
    if (change_permission(test_mapping, TEST_SIZE, PROT_READ | PROT_WRITE) < 0) {
        munmap(test_mapping, TEST_SIZE);
        ioctl_stop_tracking(device_fd, pid);
        close(device_fd);
        return 1;
    }

    // 显示权限改变后的VMA状态
    printf("VMA status after permission change:\n");
    print_vma_info(pid);

    sleep(2);  // 给内核时间响应权限变化，测试权限变化是否被监控

    printf("\n--- PHASE 3: PERMISSION CHANGED, NO WRITE OPERATION ---\n");
    printf("🔍 Testing whether permission change alone triggers dirty-track monitoring...\n");
    printf("   (No write operation performed - only permission change)\n");
    printf("   If dirty-track detects this, it would be very interesting!\n");

    // 改变回只读
    printf("\n--- PHASE 4: CHANGE BACK TO READONLY ---\n");
    if (change_permission(test_mapping, TEST_SIZE, PROT_READ) < 0) {
        munmap(test_mapping, TEST_SIZE);
        ioctl_stop_tracking(device_fd, pid);
        close(device_fd);
        return 1;
    }

    printf("Now mapping is readonly again\n");

    // 显示权限变回只读后的VMA状态
    printf("VMA status after changing back to readonly:\n");
    print_vma_info(pid);

    sleep(2);  // 等待 dirty-track 处理

    printf("\n--- PHASE 5: CLEANUP ---\n");

    // 清理
    munmap(test_mapping, TEST_SIZE);

    // 停止跟踪
    ioctl_stop_tracking(device_fd, pid);

    close(device_fd);

    printf("\nTest completed!\n");
    printf("Check dirty-map files in: %s\n", tmp_dir);
    printf("Use ../../dirtymap/user-read-dirtymap to analyze results\n");
    printf("\n🔍 Key Test Result:\n");
    printf("   Did dirty-track detect permission changes without writes?\n");
    printf("   Check the dirty-map - if empty, permission changes alone are NOT monitored\n");

    return 0;
}