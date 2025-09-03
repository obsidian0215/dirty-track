#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/mman.h>
#include <signal.h>
#include <time.h>
#include <math.h>

// 测试配置参数
#define MEMORY_POOL_SIZE (300 * 1024 * 1024)  // 300MB内存池
#define PAGE_SIZE 4096
#define HOT_SPOT_RATIO 0.1                     // 热区比例
#define COLD_ACCESS_RATIO 0.05                 // 冷区访问比例
#define SIMULTANEOUS_THREADS 2                 // 并发线程数

// 全局测试状态
volatile sig_atomic_t running = 1;
pthread_mutex_t stats_mutex;
pthread_barrier_t barrier;

// 性能统计结构
typedef struct {
    unsigned long long pages_accessed;
    unsigned long long hot_spot_hits;
    unsigned long long cold_area_hits;
    unsigned long long writes_performed;
    time_t start_time;
} test_stats_t;

test_stats_t global_stats;

// 内存工作区结构
typedef struct {
    void *memory_pool;
    char *access_pattern_map;    // 记录页面访问模式
    unsigned long total_pages;
    unsigned long hot_spot_pages;
    unsigned long hot_spot_start;
    unsigned long hot_spot_end;
    size_t memory_pool_size;
} memory_zone_t;

// 线程工作函数类型
typedef void *(*worker_func_t)(void *);

// 信号处理函数
void signal_handler(int sig) {
    running = 0;
}

// 初始化内存区域
int init_memory_zone(memory_zone_t *zone, size_t memory_pool_size_mb) {
    if (!zone) {
        fprintf(stderr, "Invalid zone pointer\n");
        return -1;
    }

    // 计算内存池大小（MB到字节）
    zone->memory_pool_size = memory_pool_size_mb * 1024 * 1024;

    // 分配大的内存池
    zone->memory_pool = mmap(NULL, zone->memory_pool_size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (zone->memory_pool == MAP_FAILED) {
        perror("Failed to allocate memory pool");
        return -1;
    }

    zone->total_pages = zone->memory_pool_size / PAGE_SIZE;
    zone->hot_spot_pages = (unsigned long)(zone->total_pages * HOT_SPOT_RATIO);

    // 确保热区页面数量合理
    if (zone->hot_spot_pages == 0) {
        zone->hot_spot_pages = 1;  // 最少1页
    } else if (zone->hot_spot_pages >= zone->total_pages) {
        zone->hot_spot_pages = zone->total_pages - 1;  // 最多比总页数少1
    }

    // 初始化为冷区
    zone->hot_spot_start = 0;
    zone->hot_spot_end = zone->hot_spot_pages;

    // 分配访问模式映射
    zone->access_pattern_map = calloc(zone->total_pages, 1);
    if (!zone->access_pattern_map) {
        perror("Failed to allocate access pattern map");
        munmap(zone->memory_pool, zone->memory_pool_size);
        return -1;
    }

    // 初始化内存内容
    memset(zone->memory_pool, 0, zone->memory_pool_size);

    printf("Initialized memory zone: %zu MB, %lu pages, hot spot: %lu pages\n",
           zone->memory_pool_size / (1024*1024), zone->total_pages, zone->hot_spot_pages);

    return 0;
}

// 释放内存区域
void cleanup_memory_zone(memory_zone_t *zone) {
    if (zone->memory_pool) {
        munmap(zone->memory_pool, zone->memory_pool_size);
    }
    if (zone->access_pattern_map) {
        free(zone->access_pattern_map);
    }
}

// 更新统计信息
void update_stats(unsigned long page_idx, int is_write, int is_hot_spot) {
    pthread_mutex_lock(&stats_mutex);
    global_stats.pages_accessed++;

    if (is_write) {
        global_stats.writes_performed++;
    }

    if (is_hot_spot) {
        global_stats.hot_spot_hits++;
    } else {
        global_stats.cold_area_hits++;
    }
    pthread_mutex_unlock(&stats_mutex);
}

// 计算下一访问页面（模拟真实应用访问模式）
unsigned long calculate_next_access(memory_zone_t *zone, unsigned long current_page, int access_type) {
    unsigned long next_page = 0;

    // 根据访问类型选择不同的访问模式
    if (access_type == 0) {  // 热区密集访问
        // 在热区内随机选择，但有局部性倾向
        unsigned long local_offset = (unsigned long)(random() % (zone->hot_spot_pages / 4));
        next_page = zone->hot_spot_start + ((current_page - zone->hot_spot_start + local_offset) % zone->hot_spot_pages);
    } else if (access_type == 1) {  // 冷区稀疏访问
        // 在冷区随机选择，避免热区
        unsigned long cold_start = zone->hot_spot_end;
        unsigned long cold_pages = zone->total_pages - zone->hot_spot_pages;
        if (cold_pages > 0) {
            next_page = cold_start + (random() % cold_pages);
        } else {
            // 如果没有冷区，回到热区
            next_page = zone->hot_spot_start + (random() % zone->hot_spot_pages);
        }
    } else {  // 混合访问
        // 80%热区，20%冷区
        unsigned long cold_start = zone->hot_spot_end;
        unsigned long cold_pages = zone->total_pages - zone->hot_spot_pages;

        if (random() % 100 < 80 && zone->hot_spot_pages > 0) {
            next_page = zone->hot_spot_start + (random() % zone->hot_spot_pages);
        } else {
            if (cold_pages > 0) {
                next_page = cold_start + (random() % cold_pages);
            } else {
                next_page = zone->hot_spot_start + (random() % zone->hot_spot_pages);
            }
        }
    }

    // 确保页索引在有效范围内
    if (next_page >= zone->total_pages) {
        next_page = zone->total_pages - 1;
    }

    return next_page;
}

// 执行内存访问操作
void perform_memory_operation(memory_zone_t *zone, unsigned long page_idx, int operation_type) {
    // 安全检查：确保页面索引有效
    if (page_idx >= zone->total_pages || !zone->memory_pool) {
        fprintf(stderr, "Warning: Invalid page_idx %lu, total_pages %lu\n", page_idx, zone->total_pages);
        return;
    }

    char *page_addr = (char *)zone->memory_pool + page_idx * PAGE_SIZE;
    int is_hot_spot = (page_idx >= zone->hot_spot_start && page_idx < zone->hot_spot_end);

    // 更新访问模式映射
    if (zone->access_pattern_map) {
        if (page_idx < zone->total_pages) {
            zone->access_pattern_map[page_idx] = 1;  // 标记为已访问
        }
    }

    switch (operation_type) {
        case 0: {  // 读取操作
            volatile char value = *page_addr;
            (void)value;  // 防止优化
            update_stats(page_idx, 0, is_hot_spot);
            break;
        }
        case 1: {  // 写入操作
            char new_value = (char)(random() % 256);
            *page_addr = new_value;
            update_stats(page_idx, 1, is_hot_spot);
            break;
        }
        case 2: {  // 修改操作（读-改-写）
            char current_value = *page_addr;
            char new_value = current_value + 1;
            *page_addr = new_value;
            update_stats(page_idx, 1, is_hot_spot);
            break;
        }
    }
}

// 热区访问模式线程
void *hot_spot_worker(void *arg) {
    memory_zone_t *zone = (memory_zone_t *)arg;
    unsigned long current_page = zone->hot_spot_start;

    // 确保初始页面在有效范围内
    if (current_page >= zone->total_pages) {
        current_page = zone->hot_spot_start;
    }

    pthread_barrier_wait(&barrier);

    while (running) {
        // 高频访问热区
        for (int i = 0; i < 100 && running; i++) {
            current_page = calculate_next_access(zone, current_page, 0);
            if (current_page < zone->total_pages) {
                perform_memory_operation(zone, current_page, 1);  // 写入操作
            }
            usleep(100);  // 短暂延迟
        }

        // 偶尔访问冷区
        if (random() % 100 < 5) {
            current_page = calculate_next_access(zone, current_page, 1);
            if (current_page < zone->total_pages) {
                perform_memory_operation(zone, current_page, 0);  // 读取操作
            }
        }

        usleep(1000);  // 线程间延迟
    }

    return NULL;
}

// 冷区访问模式线程
void *cold_area_worker(void *arg) {
    memory_zone_t *zone = (memory_zone_t *)arg;
    unsigned long current_page = zone->hot_spot_end;

    // 确保初始页面在有效范围内
    if (current_page >= zone->total_pages) {
        current_page = zone->total_pages - 1;
    }

    pthread_barrier_wait(&barrier);

    while (running) {
        // 低频访问冷区
        current_page = calculate_next_access(zone, current_page, 1);
        if (current_page < zone->total_pages) {
            perform_memory_operation(zone, current_page, 2);  // 修改操作
        }

        // 较长的延迟，模拟冷数据访问
        usleep(random() % 10000 + 5000);
    }

    return NULL;
}

// 混合访问模式线程
void *mixed_access_worker(void *arg) {
    memory_zone_t *zone = (memory_zone_t *)arg;
    unsigned long current_page = random() % zone->total_pages;

    // 确保初始页面在有效范围内
    if (current_page >= zone->total_pages) {
        current_page = zone->total_pages - 1;
    }

    pthread_barrier_wait(&barrier);

    while (running) {
        // 混合访问模式
        current_page = calculate_next_access(zone, current_page, 2);
        int operation = random() % 3;  // 随机选择操作类型
        if (current_page < zone->total_pages) {
            perform_memory_operation(zone, current_page, operation);
        }

        usleep(random() % 2000 + 500);  // 中等延迟
    }

    return NULL;
}

// 周期性工作集变化线程
void *workload_changer(void *arg) {
    memory_zone_t *zone = (memory_zone_t *)arg;

    pthread_barrier_wait(&barrier);

    while (running) {
        sleep(30);  // 每30秒改变一次工作集

        // 移动热区位置，模拟工作集变化
        unsigned long available_range = zone->total_pages - zone->hot_spot_pages;
        if (available_range > 0) {
            unsigned long new_start = random() % available_range;
            zone->hot_spot_start = new_start;
            zone->hot_spot_end = new_start + zone->hot_spot_pages;
        } else {
            // 如果热区大小等于总内存大小，保持原样
            zone->hot_spot_start = 0;
            zone->hot_spot_end = zone->total_pages;
        }

        printf("Workload changed: hot spot moved to [%lu, %lu]\n",
               zone->hot_spot_start, zone->hot_spot_end);
    }

    return NULL;
}

// 统计线程
void *stats_reporter(void *arg) {
    (void)arg;

    pthread_barrier_wait(&barrier);

    while (running) {
        sleep(10);  // 每10秒报告一次统计信息

        pthread_mutex_lock(&stats_mutex);
        time_t current_time = time(NULL);
        double elapsed = difftime(current_time, global_stats.start_time);

        printf("\n=== Performance Statistics (%.0fs) ===\n", elapsed);
        printf("Total pages accessed: %llu\n", global_stats.pages_accessed);
        printf("Hot spot hits: %llu (%.2f%%)\n", global_stats.hot_spot_hits,
               global_stats.hot_spot_hits * 100.0 / (global_stats.pages_accessed ?: 1));
        printf("Cold area hits: %llu (%.2f%%)\n", global_stats.cold_area_hits,
               global_stats.cold_area_hits * 100.0 / (global_stats.pages_accessed ?: 1));
        printf("Write operations: %llu (%.2f%%)\n", global_stats.writes_performed,
               global_stats.writes_performed * 100.0 / (global_stats.pages_accessed ?: 1));
        printf("Access rate: %.1f pages/sec\n",
               global_stats.pages_accessed / (elapsed ?: 1));

        pthread_mutex_unlock(&stats_mutex);
    }

    return NULL;
}

// 主函数
int main(int argc, char *argv[]) {
    memory_zone_t zone;
    pthread_t threads[SIMULTANEOUS_THREADS + 2];  // 工作线程 + 统计线程 + 工作集变化线程
    worker_func_t worker_functions[SIMULTANEOUS_THREADS];
    int thread_count = 0;

    // 解析命令行参数
    int memory_size_mb = MEMORY_POOL_SIZE / (1024 * 1024);  // 默认300MB
    int test_type = 0;

    if (argc == 2) {
        // 只提供了一个参数
        test_type = atoi(argv[1]);
        printf("使用默认大小300MB，\n");
    } else if (argc >= 3) {
        memory_size_mb = atoi(argv[1]);
        test_type = atoi(argv[2]);
    } else {
        printf("Usage: %s <memory_size_mb> <test_type>\n", argv[0]);
        printf("Or for backward compatibility: %s <test_type>\n", argv[0]);
        printf("  memory_size_mb: 内存大小(MB), 默认: %d\n", memory_size_mb);
        printf("  test_type:\n");
        printf("    0: Hot spot intensive\n");
        printf("    1: Cold area sparse\n");
        printf("    2: Mixed access\n");
        return 1;
    }

    if (memory_size_mb <= 0) {
        fprintf(stderr, "Error: memory_size_mb must be greater than 0\n");
        return 1;
    }

    printf("=== CRIU阈值算法测试程序 ===\n");
    printf("内存池大小: %d MB\n", memory_size_mb);
    printf("并发线程数: %d\n", SIMULTANEOUS_THREADS);
    printf("测试类型: %d\n\n", test_type);

    // 初始化随机种子
    srand(time(NULL) ^ getpid());

    // 注册信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    // 初始化同步原语
    pthread_mutex_init(&stats_mutex, NULL);
    pthread_barrier_init(&barrier, NULL, SIMULTANEOUS_THREADS + 2);

    // 初始化内存区域
    if (init_memory_zone(&zone, memory_size_mb) != 0) {
        fprintf(stderr, "Failed to initialize memory zone\n");
        return 1;
    }

    // 初始化全局统计
    memset(&global_stats, 0, sizeof(global_stats));
    global_stats.start_time = time(NULL);

    // 设置工作线程函数
    switch (test_type) {
        case 0:  // 热区密集测试
            for (int i = 0; i < SIMULTANEOUS_THREADS; i++) {
                worker_functions[i] = hot_spot_worker;
            }
            printf("Running hot spot intensive test...\n");
            break;
        case 1:  // 冷区稀疏测试
            for (int i = 0; i < SIMULTANEOUS_THREADS; i++) {
                worker_functions[i] = cold_area_worker;
            }
            printf("Running cold area sparse test...\n");
            break;
        case 2:  // 混合测试
            for (int i = 0; i < SIMULTANEOUS_THREADS; i++) {
                worker_functions[i] = mixed_access_worker;
            }
            printf("Running mixed access test...\n");
            break;
        default:
            printf("Invalid test type: %d\n", test_type);
            cleanup_memory_zone(&zone);
            return 1;
    }

    // 创建工作线程
    for (int i = 0; i < SIMULTANEOUS_THREADS; i++) {
        if (pthread_create(&threads[thread_count++], NULL,
                          worker_functions[i], &zone) != 0) {
            perror("Failed to create worker thread");
            running = 0;
            break;
        }
    }

    // 创建统计报告线程
    if (pthread_create(&threads[thread_count++], NULL, stats_reporter, NULL) != 0) {
        perror("Failed to create stats thread");
        running = 0;
    }

    // 创建工作集变化线程
    if (pthread_create(&threads[thread_count++], NULL, workload_changer, &zone) != 0) {
        perror("Failed to create workload changer thread");
        running = 0;
    }

    printf("测试程序启动完成，PID: %d\n", getpid());
    printf("使用 Ctrl+C 停止测试程序\n\n");

    // 等待所有线程完成
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], NULL);
    }

    // 输出最终统计
    printf("\n=== Final Statistics ===\n");
    printf("Total runtime: %.0fs\n", difftime(time(NULL), global_stats.start_time));
    printf("Total pages accessed: %llu\n", global_stats.pages_accessed);
    printf("Total writes: %llu\n", global_stats.writes_performed);

    // 清理资源
    pthread_barrier_destroy(&barrier);
    pthread_mutex_destroy(&stats_mutex);
    cleanup_memory_zone(&zone);

    printf("测试程序完成\n");
    return 0;
}