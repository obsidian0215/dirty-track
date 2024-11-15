#include <stdio.h>
#include <stdlib.h>

int main(int argc, char *argv[]) {
    FILE *file;
    unsigned long index;
    int timestamp, ret;

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
    while (ret = fread(&timestamp, sizeof(int), 1, file)) {
        if (ret != 1) {
            perror("Error reading dirty_heat data");
            break;
        }

        // 读取结构体数据,打印出索引和结构体内容
        // 验证 page_type 是否有效
        
        printf("%d's timestamp: %d\n", 
                   index, timestamp);

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