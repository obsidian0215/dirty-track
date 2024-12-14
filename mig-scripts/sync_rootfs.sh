#!/usr/bin/bash
#user=root

# 检查参数数量
if [ $# -eq 3 ]; then
    host=$1
    rootfs=$2
    log_path=$3

    # 确保日志目录存在
    mkdir -p "$log_path" || { echo "无法创建日志目录 $log_path"; exit 1; }

    # 设置日志目标为指定的日志文件
    LOG_TARGET="$log_path/rsync_rootfs.log"
elif [ $# -eq 2 ]; then
    host=$1
    rootfs=$2

    # 设置日志目标为终端输出
    LOG_TARGET="/dev/stdout"
else
    echo "Usage:"
    echo "  $0 <host> <path-to-rootfs> [path-to-log]"
    exit 1
fi

# 切换到rootfs目录
cd "$rootfs" || { echo "无法切换到目录 $rootfs"; exit 1; }


cd $rootfs  # rsync同步的特性，这里必须要先cd到源目录，inotify再监听./ 才能rsync同步后目录结构一致
rsync -ahvzP --delete --timeout=100 $rootfs/ root@$host:$rootfs/ >> "$LOG_TARGET" 2>&1   # 首先执行一次rsync，保证目录结构一致
if [ $? -ne 0 ]; then
    echo "初始 rsync 失败" >> "$LOG_TARGET"
    exit 1
fi

inotifywait -mrq --timefmt '%Y%m%d %H:%M' --format  '%Xe %w%f' -e modify,create,delete,attrib,close_write,move ./ \
| while read line;
do
        INO_EVENT=$(echo $line | awk '{print $1}')              # 把inotify输出切割 把事件类型部分赋值给INO_EVENT
        INO_FILE=$(echo $line | awk '{print $2}')               # 把inotify输出切割 把文件路径部分赋值给INO_FILE
        echo "-------------------------------$(date)------------------------------------" >> "$LOG_TARGET"
        echo ${line}  >> "$LOG_TARGET"

        # 增加、修改、写入完成、移动到事件的处理
        # 都是针对文件的操作，新建目录同步的也只是一个空目录，不会影响速度
        if [[ $INO_EVENT =~ 'CREATE' ]] || [[ $INO_EVENT =~ 'MODIFY' ]] || [[ $INO_EVENT =~ 'CLOSE_WRITE' ]] || [[ $INO_EVENT =~ 'MOVED_TO' ]]
        then
            echo 'CREATE or MODIFY or CLOSE_WRITE or MOVED_TO' >> "$LOG_TARGET"
            # 同步源使用$(dirname ${INO_FILE})变量，即每次只同步发生改变的文件的目录
            # 避免只同步目标文件时漏文件的可能并平衡同步性能
            # -R参数把源的目录结构递归到目标后面，保证目录结构一致性
            # rsync -avzcR $(dirname ${INO_FILE}) root@$host::rootfs
            rsync -avzcR $(dirname ${INO_FILE}) root@$host:$rootfs/
        fi

        # 删除、移动出事件
        if [[ $INO_EVENT =~ 'DELETE' ]] || [[ $INO_EVENT =~ 'MOVED_FROM' ]]
        then
            echo 'DELETE or MOVED_FROM' >> "$LOG_TARGET"
            # 直接同步已删除的路径${INO_FILE}会报no such or directory错误，rsync不能删除远程目标的指定文件
            # 同步的源是被删文件或目录的上一级路径，并加上--delete来删除目标上有而源中没有的文件
            # 缺点：如果删除的路径越靠近根，则同步的目录越多，同步删除的操作就越花时间
            rsync -avzR --delete $(dirname ${INO_FILE}) root@$host:$rootfs/
        fi

        # 修改属性(touch, ch{grp,mod,own})事件
        if [[ $INO_EVENT =~ 'ATTRIB' ]]
        then
            echo 'ATTRIB' >> "$LOG_TARGET"
            # 不同步修改属性的目录，避免递归扫描
            # 等此目录下的文件发生同步时，rsync会同时更新此目录的属性
            if [ ! -d "$INO_FILE" ]
            then
                rsync -avzcR $(dirname ${INO_FILE}) root@$host:$rootfs/

            fi
        fi
done

# inotifywait -mrq --timefmt '%Y%m%d %H:%M' --format '%T %w%f%e' -e modify,delete,create,attrib,move $rootfs \
# | while read files;do
#     rsync -ahvzP --delete --timeout=100 $rootfs/ root@$host:$rootfs/
#     echo "${files} was rsynced" >& /runc//logs/rsync_filename.log
# done