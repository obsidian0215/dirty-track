#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <endian.h> // 用于字节序转换

// 脏页信息(8 + 1 = 9 Btyes)
struct __attribute__((__packed__)) warm_page{
	unsigned long address;
    unsigned char scount;
};

int main(int argc, char *argv[]) {
    FILE *file;
    unsigned long index;
    struct warm_page warm_page;
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

    while (ret = fread(&warm_page, sizeof(struct warm_page), 1, file)) {
        if (ret != 1) {
            perror("Error reading dirty_page data");
            break;
        }

        // 读取结构体数据, 打印出地址和写入次数
        printf("Warm-Page address: 0x%lx, scount: %d\n",
                   warm_page.address, warm_page.scount);
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