#include <stdio.h>
#include <stdlib.h>

// 脏页信息(8 + 4 + 1 * 3 = 15 Btyes)
struct __attribute__((__packed__)) dirty_heat{
    unsigned long address;
    // uint32_t write_count;
    unsigned int size;
    unsigned char heat_level;
    char heat_trend;
};

int main(int argc, char *argv[]) {
    FILE *file;
    unsigned long index;
    struct dirty_heat dirty_page;
    int ret;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dirty_heatmap_file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    file = fopen(argv[1], "rb");
    if (!file) {
        perror("Failed to open file");
        return 1;
    }

    // 逐个读取文件中的索引（页地址）和结构体数据
    index = 0;
    while (ret = fread(&dirty_page, sizeof(struct dirty_heat), 1, file)) {
        if (ret != 1) {
            perror("Error reading dirty_heat data");
            break;
        }

        // 读取结构体数据,打印出索引和结构体内容
        // 验证 page_type 是否有效
        
        printf("Address: 0x%lx, Size: %u, Heat Level: %d, Heat Trend: %d\n", 
                   dirty_page.address, dirty_page.size, dirty_page.heat_level, dirty_page.heat_trend);

        index++;
    }
    if (ferror(file)) {
        perror("Error reading dirty_heat data");
    } else if (!feof(file)) {
        fprintf(stderr, "Unexpected end of file.\n");
    }
    
    fclose(file);
    return 0;
}