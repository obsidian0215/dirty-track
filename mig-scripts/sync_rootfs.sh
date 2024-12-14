#!/usr/bin/bash
#user=root

# 设置防抖时间间隔（秒）
DEBOUNCE_INTERVAL=1

# 声明关联数组，用于记录每个目录的最后同步时间
declare -A LAST_SYNC_TIME
# 声明关联数组，用于标记每个目录是否有定时器在运行
declare -A TIMER_PID

# 检查参数数量
if [ $# -eq 2 ]; then
    host=$1
    rootfs=$2
else
    echo "Usage:"
    echo "  $0 <host> <path-to-rootfs> [path-to-log]"
    exit 1
fi

# 切换到rootfs目录
cd "$rootfs" || { echo "无法切换到目录 $rootfs"; exit 1; }

cd $rootfs  # rsync同步的特性，这里必须要先cd到源目录，inotify再监听./ 才能rsync同步后目录结构一致
rsync -ahvzP --delete --timeout=100 "$rootfs/" "root@$host:$rootfs/"   # 首先执行一次rsync，保证目录结构一致
if [ $? -ne 0 ]; then
    echo "初始 rsync 失败"
    exit 1
fi

inotifywait -mrq --timefmt '%Y%m%d %H:%M' --format  '%Xe %w%f' -e modify,create,delete,attrib,close_write,move ./ \
| while read line;
do
    INO_EVENT=$(echo $line | awk '{print $1}')              # 把inotify输出切割 把事件类型部分赋值给INO_EVENT
    INO_FILE=$(echo $line | awk '{print $2}')               # 把inotify输出切割 把文件路径部分赋值给INO_FILE

    # 提取文件所在目录
    FILE_DIR=$(dirname "$INO_FILE")

    # 获取当前时间（秒）
    CURRENT_TIME=$(date +%s)

    # 模拟第一次事件，未初始化
    LAST_TIME=${LAST_SYNC_TIME["$FILE_DIR"]}

    # if [[ ! "$LAST_TIME" =~ ^[0-9]+$ ]]; then
    #     echo "LAST_TIME 未初始化或无效，初始化为 0."
    #     LAST_TIME=0
    # fi

    # 计算时间差
    if [ -n "$LAST_TIME" ]; then
        TIME_DIFF=$(( CURRENT_TIME - LAST_TIME ))
    else
        TIME_DIFF=$DEBOUNCE_INTERVAL
    fi

    # 更新最新事件时间
    LAST_SYNC_TIME["$FILE_DIR"]=$CURRENT_TIME

    # 判断是否需要同步（时间差大于防抖间隔）
    if [ -z "$LAST_TIME" ] || [ "$TIME_DIFF" -ge "$DEBOUNCE_INTERVAL" ]; then
        echo "-------------------------------$(date)------------------------------------"
        echo ${line}

        # 增加、修改、写入完成、移动到事件的处理
        # 都是针对文件的操作，新建目录同步的也只是一个空目录，不会影响速度
        if [[ $INO_EVENT =~ 'CREATE' ]] || [[ $INO_EVENT =~ 'MODIFY' ]] || [[ $INO_EVENT =~ 'CLOSE_WRITE' ]] || [[ $INO_EVENT =~ 'MOVED_TO' ]]
        then
            echo 'CREATE or MODIFY or CLOSE_WRITE or MOVED_TO'
            # 同步源使用$(dirname ${INO_FILE})变量，即每次只同步发生改变的文件的目录
            # 避免只同步目标文件时漏文件的可能并平衡同步性能
            # -R参数把源的目录结构递归到目标后面，保证目录结构一致性
            # rsync -avzcR $(dirname ${INO_FILE}) root@$host::rootfs
            rsync -avzcR "$FILE_DIR" "root@$host:$rootfs/"
        fi

        # 删除、移动出事件
        if [[ $INO_EVENT =~ 'DELETE' ]] || [[ $INO_EVENT =~ 'MOVED_FROM' ]]
        then
            echo 'DELETE or MOVED_FROM'
            # 直接同步已删除的路径${INO_FILE}会报no such or directory错误，rsync不能删除远程目标的指定文件
            # 同步的源是被删文件或目录的上一级路径，并加上--delete来删除目标上有而源中没有的文件
            # 缺点：如果删除的路径越靠近根，则同步的目录越多，同步删除的操作就越花时间
            rsync -avzR --delete "$FILE_DIR" "root@$host:$rootfs/"
        fi

        # 修改属性(touch, ch{grp,mod,own})事件
        if [[ $INO_EVENT =~ 'ATTRIB' ]]; then
            echo 'ATTRIB'
            # 不同步修改属性的目录，避免递归扫描
            # 等此目录下的文件发生同步时，rsync会同时更新此目录的属性
            if [ ! -d "$INO_FILE" ]; then
                rsync -avzcR "$FILE_DIR" "root@$host:$rootfs/"

            fi
        fi

        # 更新最后同步时间
        LAST_SYNC_TIME["$FILE_DIR"]=$CURRENT_TIME
    else
        echo "触发防抖机制: ${line}"
        # 如果有定时器在运行，则杀死旧的定时器
        if [ -n "${TIMER_PID["$FILE_DIR"]}" ]; then
            echo "杀死旧的定时器: ${TIMER_PID["$FILE_DIR"]}"
            kill "${TIMER_PID["$FILE_DIR"]}" 2>/dev/null
        fi

        # 启动一个新的定时器
        (
            sleep "$DEBOUNCE_INTERVAL"
            NEW_TIME=${LAST_SYNC_TIME["$FILE_DIR"]}
            FINAL_TIME_DIFF=$(( $(date +%s) - NEW_TIME ))
            if [ "$FINAL_TIME_DIFF" -ge "$DEBOUNCE_INTERVAL" ]; then
                echo "-------------------------------$(date)------------------------------------"
                echo "批量处理事件: '$FILE_DIR'"

                # 同步源使用 -R 选项保持相对路径
                rsync -avzcR --delete "$FILE_DIR" "root@$host:$rootfs/"

                # 清除最后事件时间
                unset LAST_SYNC_TIME["$FILE_DIR"]
            fi
            # 清除定时器 PID
            unset TIMER_PID["$FILE_DIR"]
        ) &

        # 保存定时器的 PID
        TIMER_PID["$FILE_DIR"]=$!
        fi
    fi
done

# inotifywait -mrq --timefmt '%Y%m%d %H:%M' --format '%T %w%f%e' -e modify,delete,create,attrib,move $rootfs \
# | while read files;do
#     rsync -ahvzP --delete --timeout=100 $rootfs/ root@$host:$rootfs/
#     echo "${files} was rsynced" >& /runc//logs/rsync_filename.log
# done