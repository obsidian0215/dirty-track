#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>

// 全局变量，用于控制程序的运行
volatile sig_atomic_t keep_running = 1;

// 信号处理函数，用于优雅地终止程序
void handle_sigint(int sig) {
    keep_running = 0;
}

// 获取页面大小
size_t get_page_size() {
    long sz = sysconf(_SC_PAGESIZE);
    if (sz == -1) {
        perror("sysconf");
        exit(EXIT_FAILURE);
    }
    return (size_t)sz;
}

typedef struct {
    volatile unsigned char *mem;    // 分配的内存，声明为 volatile 防止优化
    size_t total_bytes;             // 总字节数
    size_t page_size;               // 单个页面大小
    size_t total_pages;             // 总页面数
    size_t target_pages;            // 目标脏页面数
    size_t *selected_dirty_pages;   // 预选的脏页索引
} mem_info_t;

// 写入线程：持续随机选择脏页进行写入，并在写入之间加入随机延迟
void* writer_thread(void* arg) {
    mem_info_t *info = (mem_info_t*)arg;
    const useconds_t max_delay_per_write_us = 500000; // 最大每次写入延迟 500 毫秒

    while (keep_running) {
        // 随机选择一个脏页
        size_t random_index = rand() % info->target_pages;
        size_t page_num = info->selected_dirty_pages[random_index];

        // 直接写入页面的基地址一个随机值
        volatile unsigned char *page = info->mem + page_num * info->page_size;
        uint32_t value = rand(); // 写入一个32位的随机值
        *((volatile uint32_t*)page) = value;

        // 可选：添加调试日志
        printf("写入页面地址: 0x%p, 编号: %zu, 值: %u\n", page, page_num, value);

        // 加入随机延迟，最大不超过每次写入的5毫秒
        useconds_t delay = rand() % max_delay_per_write_us;
        usleep(delay);
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        printf("用法: %s <memory_size_MB> <proportion_percent>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 解析参数
    size_t memory_size_mb = atoi(argv[1]);
    double proportion = atof(argv[2]);

    if (memory_size_mb <= 0 || proportion <= 0 || proportion > 100) {
        fprintf(stderr, "参数无效。\n");
        return EXIT_FAILURE;
    }

    // 设置信号处理
    struct sigaction sa;
    sa.sa_handler = handle_sigint;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    if (sigaction(SIGINT, &sa, NULL) == -1) {
        perror("sigaction");
        exit(EXIT_FAILURE);
    }

    // 获取页面大小和总内存信息
    size_t page_size = get_page_size();
    size_t total_bytes = memory_size_mb * 1024 * 1024;
    size_t total_pages = total_bytes / page_size;

    // 分配内存
    volatile unsigned char *mem = (volatile unsigned char*)mmap(NULL, total_bytes, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mem == MAP_FAILED) {
        perror("mmap");
        return EXIT_FAILURE;
    }

    printf("已分配 %zu MB (%zu 页) 内存\n", memory_size_mb, total_pages);

    // 初始化内存为零
    memset((void*)mem, 0, total_bytes);

    // 计算目标脏页数量
    size_t target_pages = (size_t)(total_pages * (proportion / 100.0));
    if (target_pages == 0) target_pages = 1; // 至少1页

    printf("将预选 %zu 页内存作为脏页池\n", target_pages);

    // 初始化 mem_info_t
    mem_info_t info = {
        .mem = mem,
        .total_bytes = total_bytes,
        .page_size = page_size,
        .total_pages = total_pages,
        .target_pages = target_pages,
        .selected_dirty_pages = NULL
    };

    // 分配并初始化脏页池
    info.selected_dirty_pages = malloc(info.target_pages * sizeof(size_t));
    if (!info.selected_dirty_pages) {
        perror("malloc for selected_dirty_pages");
        munmap((void*)mem, total_bytes);
        return EXIT_FAILURE;
    }

    // 随机选择目标脏页，确保唯一性
    srand(time(NULL)); // 初始化随机数种子
    for (size_t i = 0; i < info.target_pages; i++) {
        size_t page_num;
        int unique;
        do {
            unique = 1;
            page_num = rand() % info.total_pages;
            // 确保选择的页是唯一的
            for (size_t j = 0; j < i; j++) {
                if (info.selected_dirty_pages[j] == page_num) {
                    unique = 0;
                    break;
                }
            }
        } while (!unique);
        info.selected_dirty_pages[i] = page_num;
    }

    // 可选：打印选择的脏页池
    /*
    printf("选择的脏页池:\n");
    for (size_t i = 0; i < info.target_pages; i++) {
        printf("%zu ", info.selected_dirty_pages[i]);
        if ((i + 1) % 10 == 0) printf("\n");
    }
    printf("\n");
    */

    // 创建写入线程
    pthread_t writer;
    if (pthread_create(&writer, NULL, writer_thread, &info) != 0) {
        perror("pthread_create");
        munmap((void*)mem, total_bytes);
        free(info.selected_dirty_pages);
        return EXIT_FAILURE;
    }

    printf("开始持续随机写入。按 Ctrl+C 终止程序。\n");

    // 主线程等待，直到收到中断信号
    while (keep_running) {
        pause(); // 等待信号
    }

    // 等待写入线程结束
    if (pthread_join(writer, NULL) != 0) {
        perror("pthread_join");
    }

    printf("已停止随机写入。\n");

    // 统计实际被写入的脏页数量
    size_t actual_dirty_pages = 0;
    for (size_t i = 0; i < info.target_pages; i++) {
        size_t page_num = info.selected_dirty_pages[i];
        // 检查页面的第一个字节是否非零
        if (info.mem[page_num * info.page_size] != 0) {
            actual_dirty_pages++;
        }
    }

    printf("最终脏页数量: %zu\n", actual_dirty_pages);

    // 释放资源
    free(info.selected_dirty_pages);
    munmap((void*)mem, total_bytes);

    return EXIT_SUCCESS;
}

