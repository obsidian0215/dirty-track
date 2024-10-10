#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/mount.h>  // 用于 mount 系统调用
#include <limits.h>     // 用于 PATH_MAX

// 定义与 ioctl 通信的操作码
#define DIRTY_TRACK_MAGIC 'd'
#define IOCTL_SET_DIRTY_MAP_PATH _IOW(DIRTY_TRACK_MAGIC, 1, char[256])
#define IOCTL_START_PID _IOW(DIRTY_TRACK_MAGIC, 2, pid_t)
#define IOCTL_STOP_PID _IOW(DIRTY_TRACK_MAGIC, 3, pid_t)
#define IOCTL_CLEAR_SOFT_DIRTY _IO(DIRTY_TRACK_MAGIC, 4)
#define IOCTL_GET_DIRTY_MAP_PATH _IOR(DIRTY_TRACK_MAGIC, 5, char[256])

#define DEVICE_PATH "/dev/dirty-track"
#define CMD_BUFFER_SIZE 512

// 打印错误并退出
void error_exit(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

// 获取绝对路径，处理相对路径并自动创建不存在的目录
void get_absolute_path(const char *input_path, char *abs_path, size_t size) {
    // 如果是绝对路径，直接拷贝
    if (input_path[0] == '/') {
        strncpy(abs_path, input_path, size - 1);
        abs_path[size - 1] = '\0'; // 确保末尾有 '\0'
    } else {
        // 检查路径是否存在
        struct stat st;
        if (stat(input_path, &st) != 0) {
            // 如果路径不存在，则创建目录
            if (mkdir(input_path, 0755) != 0) {
                error_exit("无法创建目录");
            }
        }
        if (realpath(input_path, abs_path) == NULL) {
            error_exit("无法转换为绝对路径");
        }
    }

    // 检查路径是否存在
    struct stat st;
    if (stat(abs_path, &st) != 0) {
        // 如果路径不存在，则创建目录
        if (mkdir(abs_path, 0755) != 0) {
            error_exit("无法创建目录");
        }
    }
}

// 检查并挂载 tmpfs 到指定目录
void mount_tmpfs(const char *path) {
    struct stat st;

    // 检查路径是否存在
    if (stat(path, &st) != 0) {
        // 如果路径不存在，则创建目录
        if (mkdir(path, 0755) != 0) {
            error_exit("无法创建目录");
        }
    }
    // 挂载 tmpfs 到该目录
    if (mount("none", path, "tmpfs", 0, NULL) != 0) {
        error_exit("无法挂载 tmpfs");
    }
    printf("成功将 %s 挂载到 tmpfs。\n", path);
}

// 检查 dirty_map_path 是否被定义
int is_dirty_map_path_set(int fd) {
    char dirty_map_path[256] = {'\0'};
    if (ioctl(fd, IOCTL_GET_DIRTY_MAP_PATH, dirty_map_path) < 0) {
        error_exit("无法获取脏页跟踪路径");
    }

    // 检查路径是否为全 '\0' 或者未设置的默认值
    for (int i = 0; i < sizeof(dirty_map_path); i++) {
        if (dirty_map_path[i] != '\0') {
            return 1;  // dirty_map_path 已设置
        }
    }
    return 0;  // dirty_map_path 未设置
}


// 处理设置路径的逻辑
void handle_set_path(int fd, char *path) {
    char abs_path[256];
    
    // 将路径转换为绝对路径
    get_absolute_path(path, abs_path, sizeof(abs_path));

    // printf("length of path %s: %lu\n", abs_path, strlen(abs_path));
    // 检查路径是否为空
    if (strlen(abs_path) == 0) {
        error_exit("路径不能为空");
    }

    // 检查路径是否已设置
    if (is_dirty_map_path_set(fd)) {
        error_exit("脏页跟踪路径已设置");
    }
    // 挂载 tmpfs 到该路径
    if (mount("none", abs_path, "tmpfs", 0, NULL) != 0) {
        error_exit("无法挂载 tmpfs");
    }

    // 挂载 tmpfs 到该路径
    mount_tmpfs(abs_path);

    // 通过 ioctl 设置路径
    if (ioctl(fd, IOCTL_SET_DIRTY_MAP_PATH, abs_path) < 0) {
        error_exit("无法设置脏页跟踪路径");
    }
    printf("脏页跟踪路径设置为: %s\n", abs_path);
}

// 处理开始跟踪进程的逻辑
void handle_start_pid(int fd, pid_t pid) {
    if (!is_dirty_map_path_set(fd)) {
        printf("脏页跟踪路径未设置\n");
        return;
    }
    if (ioctl(fd, IOCTL_START_PID, &pid) < 0) {
        error_exit("无法启动对进程的脏页跟踪");
    }
    printf("进程 %d 的脏页跟踪启动\n", pid);
}

// 处理停止跟踪进程的逻辑
void handle_stop_pid(int fd, pid_t pid) {
    if (ioctl(fd, IOCTL_STOP_PID, &pid) < 0) {
        error_exit("无法停止对进程的脏页跟踪");
    }
    printf("进程 %d 的脏页跟踪停止\n", pid);
}

// 处理停止所有进程跟踪的逻辑
void handle_stop_all(int fd) {
    if (ioctl(fd, IOCTL_CLEAR_SOFT_DIRTY) < 0) {
        error_exit("无法停止所有进程的脏页跟踪");
    }
    printf("所有进程的脏页跟踪停止\n");
}

void run_loop(int fd) {
    char command[CMD_BUFFER_SIZE];
    char cmd[32], arg[256];
    pid_t pid;

    while (1) {
        printf("请输入命令(set_path <path>, start <pid>, stop <pid>, stop_all, get_path, exit):\n> ");
        // 读取用户输入
        if (fgets(command, sizeof(command), stdin) == NULL) {
            error_exit("读取命令失败");
        }

        // 处理输入，去掉末尾的换行符
        command[strcspn(command, "\n")] = '\0';

        // 分析输入的命令
        int num_args = sscanf(command, "%s %s", cmd, arg);

        // 处理不同的命令
        if (strcmp(cmd, "set_path") == 0 && num_args == 2) {
            // 设置路径
            handle_set_path(fd, arg);
        } else if (strcmp(cmd, "start") == 0 && num_args == 2) {
            // 开始跟踪进程
            pid = atoi(arg);
            printf("开始跟踪进程 %d\n", pid);
            handle_start_pid(fd, pid);
        } else if (strcmp(cmd, "stop") == 0 && num_args == 2) {
            // 停止跟踪进程
            pid = atoi(arg);
            printf("停止跟踪进程 %d\n", pid);
            handle_stop_pid(fd, pid);
        } else if (strcmp(cmd, "stop_all") == 0 && num_args == 1) {
            // 停止所有跟踪
            handle_stop_all(fd);
        } else if (strcmp(cmd, "get_path") == 0 && num_args == 1) {
            // 获取路径
            char dirty_map_path[256] = {'\0'};
            if (ioctl(fd, IOCTL_GET_DIRTY_MAP_PATH, dirty_map_path) < 0) {
                perror("无法获取脏页跟踪路径");
            } else {
                printf("当前脏页跟踪dirty-map路径: %s\n", dirty_map_path);
            }
        } else if (strcmp(cmd, "exit") == 0) {
            // 退出循环
            printf("退出程序\n");
            break;
        } else {
            printf("未知命令或参数错误: %s\n", command);
            // printf("可用操作: set_path, start_pid, stop_pid, stop_all, get_path\n");
        }
    }
}

int main(int argc, char *argv[]) {
    int fd;

    // 打开设备文件
    if (argc > 1) {
        fd = open(argv[1], O_RDWR);
    } else {     
        fd = open(DEVICE_PATH, O_RDWR);
    }

    if (fd < 0) {
        error_exit("无法打开设备文件");
    }

    printf("IOCTL_SET_DIRTY_MAP_PATH = 0x%o\n", IOCTL_SET_DIRTY_MAP_PATH);
    printf("IOCTL_START_PID = 0x%o\n", IOCTL_START_PID);
    printf("IOCTL_STOP_PID = 0x%o\n", IOCTL_STOP_PID);
    printf("IOCTL_GET_DIRTY_MAP_PATH = 0x%o\n", IOCTL_GET_DIRTY_MAP_PATH);
    // 进入命令处理循环
    run_loop(fd);

    // 关闭设备文件
    close(fd);
    return 0;
}