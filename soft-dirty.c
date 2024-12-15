#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <search.h>

#define MAX_FILENAME_LENGTH 256
// 页大小
#define PAGE_SIZE_4K 4096
#define PAGE_SIZE_2M (2 * 1024 * 1024)

// 数据结构存储脏页信息
typedef struct dirty_page {
    unsigned long address;
    unsigned int write_count;
} dirty_page_t;

// 全局变量：存储平衡树的根节点
void *root = NULL;

// 全局变量：存储上一次 track_dirty_pages 运行的时间（纳秒）
long last_run_duration_ns = 0;

// 标志位，用于捕获 Ctrl+C
volatile sig_atomic_t stop = 0;

// 全局文件指针
FILE *global_output_file = NULL;

// 信号处理器，用于捕获 Ctrl+C
void handle_sigint(int sig) {
    stop = 1;
}

/**
 * 函数：生成格式为 <pid>-<timestamp>.txt 的文件名
 *
 * @param pid 进程 ID
 * @param filename 输出的文件名字符串缓冲区
 * @param size 缓冲区大小
 * @return 如果成功生成文件名，返回 0；否则返回 -1
 */
int generate_dirty_map_filename(pid_t pid, char *filename, size_t size) {
    if (filename == NULL) {
        fprintf(stderr, "Filename buffer is NULL.\n");
        return -1;
    }

    // 获取当前时间
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) {
        perror("gettimeofday");
        return -1;
    }

    // 转换为本地时间
    struct tm *local_time = localtime(&tv.tv_sec);
    if (local_time == NULL) {
        perror("localtime");
        return -1;
    }

    // 格式化时间戳，例如 "20240427_153045_123"
    char timestamp[30];
    if (strftime(timestamp, sizeof(timestamp), "%Y%m%d_%H%M%S", local_time) == 0) {
        fprintf(stderr, "strftime failed to format time.\n");
        return -1;
    }

    // 添加毫秒
    char timestamp_with_ms[35];
    if (snprintf(timestamp_with_ms, sizeof(timestamp_with_ms), "%s_%03ld", timestamp, tv.tv_usec / 1000) >= sizeof(timestamp_with_ms)) {
        fprintf(stderr, "Timestamp buffer too small.\n");
        return -1;
    }

    // 生成文件名 "<pid>-<timestamp>.txt"
    if (snprintf(filename, size, "%d-%s.txt", pid, timestamp_with_ms) >= size) {
        fprintf(stderr, "Filename buffer too small.\n");
        return -1;
    }

    return 0;
}

// 比较函数
int compare_dirty_pages(const void *a, const void *b) {
    const dirty_page_t *page_a = (const dirty_page_t *)a;
    const dirty_page_t *page_b = (const dirty_page_t *)b;

    if (page_a->address < page_b->address)
        return -1;
    else if (page_a->address > page_b->address)
        return 1;
    else
        return 0;
}

// 添加或更新脏页到平衡树
void add_or_update_dirty_page(unsigned long address) {
    dirty_page_t key;
    key.address = address;
    key.write_count = 0; // 初始化为0，实际值将在查找后设置

    // 查找节点
    dirty_page_t **result = (dirty_page_t **)tfind(&key, &root, compare_dirty_pages);

    if (result == NULL) {
        // 节点不存在，创建新节点
        dirty_page_t *new_page = malloc(sizeof(dirty_page_t));
        if (!new_page) {
            perror("malloc dirty_page");
            exit(EXIT_FAILURE);
        }
        new_page->address = address;
        new_page->write_count = 1;

        // 插入新节点
        void *node = tsearch(new_page, &root, compare_dirty_pages);
        if (node == NULL) {
            fprintf(stderr, "tsearch failed to insert node\n");
            free(new_page);
            exit(EXIT_FAILURE);
        }
    } else {
        // 节点已存在，更新写入计数
        dirty_page_t *existing_page = *result;
        existing_page->write_count++;
    }
}

// 释放平衡树的节点
void free_dirty_page(void *nodep) {
    dirty_page_t *page = *(dirty_page_t **)nodep;
    free(page);
}

// 释放平衡树
void free_dirty_pages_tree() {
    tdestroy(root, free_dirty_page);
}

// 启用 soft-dirty tracking（清除 soft-dirty 位）
int enable_soft_dirty_tracking(pid_t pid) {
    char clear_refs_path[256];
    snprintf(clear_refs_path, sizeof(clear_refs_path), "/proc/%d/clear_refs", pid);

    int fd = open(clear_refs_path, O_WRONLY);
    if (fd < 0) {
        perror("open clear_refs");
        return -1;
    }

    // 写入 "4" 以清除 soft-dirty 位
    if (write(fd, "4", 1) != 1) {
        perror("write clear_refs");
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

// 批量读取 pagemap 数据
int is_soft_dirty_bulk(pid_t pid, unsigned long start_addr, unsigned long end_addr) {
    char pagemap_path[256];
    snprintf(pagemap_path, sizeof(pagemap_path), "/proc/%d/pagemap", pid);

    int fd = open(pagemap_path, O_RDONLY);
    if (fd < 0) {
        perror("open pagemap");
        return -1;
    }

    unsigned long num_pages = (end_addr - start_addr) / PAGE_SIZE_4K;
    uint64_t *pagemap_entries = malloc(num_pages * sizeof(uint64_t));
    if (!pagemap_entries) {
        perror("malloc pagemap_entries");
        close(fd);
        return -1;
    }

    off_t offset = (start_addr / PAGE_SIZE_4K) * sizeof(uint64_t);
    ssize_t bytes_read = pread(fd, pagemap_entries, num_pages * sizeof(uint64_t), offset);
    if (bytes_read != (ssize_t)(num_pages * sizeof(uint64_t))) {
        perror("pread pagemap");
        free(pagemap_entries);
        close(fd);
        return -1;
    }

    for (unsigned long i = 0; i < num_pages; i++) {
        if (pagemap_entries[i] & ((uint64_t)1 << 55)) {
            unsigned long addr = start_addr + (i * PAGE_SIZE_4K);
            add_or_update_dirty_page(addr);
        }
    }

    free(pagemap_entries);
    close(fd);
    return 0;
}

// 辅助函数，用于 twalk 的回调，写入文件
void write_to_file_callback(const void *nodep, const VISIT which, const int depth) {
    if (which == preorder || which == leaf) {
        const dirty_page_t *page = *(const dirty_page_t **)nodep;
        fprintf(global_output_file, "Page address: 0x%lx, Write count: %u\n", page->address, page->write_count);
    }
}

// 遍历 /proc/[pid]/maps 并检查 Soft-Dirty 位
int track_dirty_pages(pid_t pid) {
    char maps_path[256];
    snprintf(maps_path, sizeof(maps_path), "/proc/%d/maps", pid);

    FILE *maps = fopen(maps_path, "r");
    if (!maps) {
        perror("fopen maps");
        return -1;
    }

    char line[1024];
    while (fgets(line, sizeof(line), maps)) {
        unsigned long start, end;
        char perms[5];
        char pathname[256] = {0};
        // 解析每一行的地址范围和权限，并尝试获取路径名
        // 示例行：
        // address           perms offset  dev   inode   pathname
        // 00400000-0040b000 r-xp 00000000 08:02 131073 /usr/bin/cat
        int num_fields = sscanf(line, "%lx-%lx %4s %*s %*s %*s %s", &start, &end, perms, pathname);
        if (num_fields < 4) {
            pathname[0] = '\0'; // 没有路径名
        }

        // 仅处理可写的内存区域
        if (strchr(perms, 'w') == NULL)
            continue;

        // 批量读取 soft-dirty 位
        if (is_soft_dirty_bulk(pid, start, end) != 0) {
            // 出错处理，可选择记录日志或忽略
            continue;
        }
    }

    fclose(maps);
    return 0;
}

// 将脏页信息写入文件
int write_dirty_pages_to_file(const char *filepath) {
    FILE *file = fopen(filepath, "w");
    if (!file) {
        perror("fopen output file");
        return -1;
    }

    // 设置全局文件指针
    global_output_file = file;

    // 使用 twalk 进行中序遍历并写入文件
    twalk(root, write_to_file_callback);

    // 重置全局文件指针
    global_output_file = NULL;

    fclose(file);
    return 0;
}

int main(int argc, char *argv[]) {
    pid_t pid;
    char output_file[MAX_FILENAME_LENGTH];

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <pid>\n", argv[0]);
        return EXIT_FAILURE;
    }

    pid = atoi(argv[1]);
    if (pid <= 0) {
        fprintf(stderr, "Invalid PID: %s\n", argv[1]);
        return EXIT_FAILURE;
    }

    if (generate_dirty_map_filename(pid, output_file, sizeof(output_file)) != 0) {
        fprintf(stderr, "Failed to generate filename.\n");
        return EXIT_FAILURE;
    }

    printf("Generated filename: %s\n", output_file);

    // 启用 soft-dirty tracking（初始清除）
    if (enable_soft_dirty_tracking(pid) != 0) {
        fprintf(stderr, "Failed to enable soft-dirty tracking for PID %d\n", pid);
        return EXIT_FAILURE;
    }

    // 设置信号处理器
    struct sigaction sa;
    sa.sa_handler = handle_sigint;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("sigaction");
        return EXIT_FAILURE;
    }

    printf("Starting dirty page tracking for PID %d. Press Ctrl+C to stop.\n", pid);

    // 持续追踪脏页，直到用户中断
    while (!stop) {
        struct timespec start_time, end_time;

        // 获取开始时间
        if (clock_gettime(CLOCK_MONOTONIC, &start_time) != 0) {
            perror("clock_gettime");
            break;
        }

        // 追踪脏页
        if (track_dirty_pages(pid) != 0) {
            fprintf(stderr, "Failed to track dirty pages for PID %d\n", pid);
            break;
        }

        // 清除 soft-dirty 位
        if (enable_soft_dirty_tracking(pid) != 0) {
            fprintf(stderr, "Failed to clear soft-dirty bits for PID %d\n", pid);
            // 继续循环
        }

        // 获取结束时间
        if (clock_gettime(CLOCK_MONOTONIC, &end_time) != 0) {
            perror("clock_gettime");
            break;
        }

        // 计算运行时间（ns）
        last_run_duration_ns = (end_time.tv_sec - start_time.tv_sec) * 1000000000L +
                               (end_time.tv_nsec - start_time.tv_nsec);

        // 输出运行时间
        printf("Dirty-track run time: %ld ns\n", last_run_duration_ns);

        // 计算sleep时间, 默认1ms
        unsigned long sleep_time_us = 1000;
        if ((last_run_duration_ns) / 1000 > 3 * sleep_time_us) {
            sleep_time_us += (last_run_duration_ns) / 1000;
        } else if (sleep_time_us >= (last_run_duration_ns) / 1000 * 10) {
            sleep_time_us = 9 * (last_run_duration_ns) / 1000;
        }

        if (sleep_time_us > 4000) {
            sleep_time_us = 4000; // 最大睡眠时间为4ms
        }
        // 休眠
        usleep(sleep_time_us);
    }

    printf("\nStopping dirty page tracking for PID %d.\n", pid);

    // 将结果写入文件
    if (write_dirty_pages_to_file(output_file) != 0) {
        fprintf(stderr, "Failed to write dirty pages to file.\n");
    } else {
        printf("Dirty pages written to %s\n", output_file);
    }

    // 清理
    free_dirty_pages_tree();

    return EXIT_SUCCESS;
}
