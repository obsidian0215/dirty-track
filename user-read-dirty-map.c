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

// 脏页信息
struct __attribute__((__packed__)) dirty_page{
	unsigned long address;
    unsigned long write_count;
    unsigned int page_type;
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
        if (dirty_page.page_type >= 0 && dirty_page.page_type < (sizeof(page_type_names)/sizeof(page_type_names[0]))) {
            printf("Page address: 0x%lx, Write count: %lu, Page type: %s\n", 
                   dirty_page.address, dirty_page.write_count, page_type_names[dirty_page.page_type]);
        } else {
            printf("Page address: 0x%lx, Write count: %lu, Page type: Unknown(%d)\n", 
                   dirty_page.address, dirty_page.write_count, dirty_page.page_type);
        }
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