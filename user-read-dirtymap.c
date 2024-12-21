#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <endian.h> // 用于字节序转换

// 假设内核使用小端字节序
typedef struct {
    uint64_t track_duration_ns;
} file_header_t;

// 脏页信息(8 + 4 + 1 = 13 Btyes)
struct __attribute__((__packed__)) dirty_page{
	unsigned long address;
    unsigned int write_count;
};

int main(int argc, char *argv[]) {
    FILE *file;
    unsigned long index;
    file_header_t file_header;
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
    ret = fread(&file_header, sizeof(file_header_t), 1, file);
    if (ret != 1) {
        perror("Error reading file_header_t data");
        fclose(file);
        return 1;
    }
    printf("Track duration: 0x%lu ns\n",
                   file_header.track_duration_ns);

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
        perror("Error reading dirty_map file");
    } else if (!feof(file)) {
        fprintf(stderr, "Unexpected end of file.\n");
    }

    fclose(file);
    return 0;
}