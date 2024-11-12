#include <stdio.h>
#include <stdlib.h>

// 脏页信息(8 + 4 + 1 = 13 Btyes)
struct __attribute__((__packed__)) dirty_page{
	unsigned long address;
    unsigned int write_count;
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

        // 读取结构体数据, 打印出地址和写入次数
        printf("Page address: 0x%lx, Write count: %lu\n", 
                   dirty_page.address, dirty_page.write_count);
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