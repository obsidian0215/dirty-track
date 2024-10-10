# 模块名称
MODULE_NAME = dirty-track

# 内核源码路径
KERNEL_SRC := /lib/modules/$(shell uname -r)/build

# 模块源文件
obj-m := $(MODULE_NAME).o

# 默认目标
all:
	make -C $(KERNEL_SRC) M=$(PWD) modules

# 清理生成的文件
clean:
    # rm -f $(MODULE_NAME).ko $(MODULE_NAME).o
	make -C $(KERNEL_SRC) M=$(PWD) clean

# 安装模块
install:
    sudo insmod $(MODULE_NAME).ko

# 卸载模块
uninstall:
    sudo rmmod $(MODULE_NAME)