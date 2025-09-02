#!/usr/bin/bash
#user=root

# 设置检查时间间隔（秒）
CHECK_INTERVAL=5

# 检查参数数量
if [ $# -eq 2 ]; then
    host=$1
    rootfs=$2
else
    echo "Usage:"
    echo "  $0 <host> <path-to-rootfs>"
    exit 1
fi

# 切换到rootfs目录
cd "$rootfs" || { echo "无法切换到目录 $rootfs"; exit 1; }

cd $rootfs

# 同步config.json
rsync -avz --timeout=100 "$rootfs/../config.json" "root@$host:$rootfs/../config.json"
RET=$?
# rsync 退出码24是"文件在传输前消失"的警告，不是实际错误
if [ $RET -ne 0 ] && [ $RET -ne 24 ]; then
    echo "同步config.json失败 (exit code: $RET)"
    exit 1
fi
echo "config.json同步完成 (returned: $RET)"

# 执行初始全量同步
echo "Performing initial full sync..."
rsync -ahvzP --delete --timeout=100 "$rootfs/" "root@$host:$rootfs/"
RET=$?
# rsync 退出码24是"文件在传输前消失"的警告，不是实际错误
if [ $RET -ne 0 ] && [ $RET -ne 24 ]; then
    echo "Initial rsync failed (exit code: $RET)"
    exit 1
fi
echo "Initial sync completed (returned: $RET)"

# 初始化最后同步时间
LAST_SYNC_TIME=$(date +%s)

echo "Starting periodic check every $CHECK_INTERVAL seconds"

# 开始周期性检查循环
while true; do
    # 检查是否存在强制同步标记文件
    FORCE_SYNC_FILE="$rootfs/../force_sync.marker"
    if [ -f "$FORCE_SYNC_FILE" ]; then
        echo "检测到强制同步请求，开始执行最终同步..."
        # 执行实际同步
        rsync -ahvzP --delete --timeout=100 "$rootfs/" "root@$host:$rootfs/"
        RET=$?
        # rsync 退出码24是"文件在传输前消失"的警告，不是实际错误
        if [ $RET -eq 0 ] || [ $RET -eq 24 ]; then
            echo "强制同步完成 at $(date) (returned: $RET)"
            echo "同步完成后退出sync_rootfs进程"
        else
            echo "强制同步失败 at $(date) (exit code: $RET)"
        fi
        # 删除同步请求标记文件，表示同步已完成
        rm -f "$FORCE_SYNC_FILE"
        exit 0
    fi

    sleep "$CHECK_INTERVAL"

    # 使用rsync --dry-run检查是否有变化
    CHANGES=$(rsync -avu --dry-run --timeout=10 "$rootfs/" "root@$host:$rootfs/" | grep -c "^\.")
    CURRENT_TIME=$(date +%s)

    if [ "$CHANGES" -gt 0 ]; then
        echo "-------------------------------$(date)------------------------------------"
        echo "Changes detected, executing full sync"

        # 执行实际同步
        rsync -ahvzP --delete --timeout=100 "$rootfs/" "root@$host:$rootfs/"
        RET=$?
        # rsync 退出码24是"文件在传输前消失"的警告，不是实际错误
        if [ $RET -eq 0 ] || [ $RET -eq 24 ]; then
            echo "Full sync completed at $(date) (returned: $RET)"
            LAST_SYNC_TIME=$CURRENT_TIME
        else
            echo "Full sync failed at $(date) (exit code: $RET)"
        fi
    else
        TIME_AGO=$(( CURRENT_TIME - LAST_SYNC_TIME ))
        echo "No changes detected - Last sync $TIME_AGO seconds ago"
    fi
done