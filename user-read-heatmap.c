#include <stdio.h>
#include <stdlib.h>

// 页面类型
#define PAGE_PTE 0                  // 4KB
#define PAGE_PMD 1                  // 2MB
#define PAGE_PUD 2                  // 1GB(当前设计下不会被使用)

const char* page_type_names [] = {
    "PAGE_PTE",
    "PAGE_PMD",
    "PAGE_PUD",
};

// 脏页信息(8 + 4 + 1 * 3 = 15 Btyes)
struct __attribute__((__packed__)) dirty_page{
    unsigned long address;
    // uint32_t write_count;
    unsigned int size;
    unsigned char heat_level;
    char heat_trend;
    unsigned char selected;
};

int main(int argc, char *argv[]) {
    FILE *file;
    unsigned long index;
    struct dirty_page dirty_page;
    int ret;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s <dirty_map_file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    file = fopen(argv[1], "rb");
    if (!file) {
        perror("Failed to open file");
        return 1;
    }

    // 逐个读取文件中的索引（页地址）和结构体数据
    index = 0;
    while (ret = fread(&dirty_page, sizeof(struct dirty_page), 1, file)) {
        if (ret != 1) {
            perror("Error reading dirty_page data");
            break;
        }

        // 读取结构体数据,打印出索引和结构体内容
        // 验证 page_type 是否有效
        
        printf("Address: 0x%lx, Size: %u, Heat Level: %d, Heat Trend: %d, Selected: %d\n", 
                   dirty_page.address, dirty_page.size, dirty_page.heat_level, dirty_page.heat_trend, dirty_page.selected);

        index++;
    }
    if (ferror(file)) {
        perror("Error reading dirty_page data");
    } else if (!feof(file)) {
        fprintf(stderr, "Unexpected end of file.\n");
    }
    
    fclose(file);
    return 0;
}