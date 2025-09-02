#!/bin/bash

# Redis基准测试脚本，使用redis-benchmark工具
# 支持不同并发客户端、延迟测量和吞吐量评估
# 与chk_restore.py迁移测试集成

set -e

# 配置参数，环境变量或默认值
REDIS_HOST=${REDIS_HOST:-localhost}
REDIS_PORT=${REDIS_PORT:-6379}
CLIENTS=${CLIENTS:-50}          # 并发出连接数
REQUESTS=${REQUESTS:-10000}     # 每个客户端请求数
KEYSPACE_LEN=${KEYSPACE_LEN:-1000000}  # 操作的键空间大小
DATA_SIZE=${DATA_SIZE:-2048}    # 数据大小（字节）
TEST_TYPES=${TEST_TYPES:-"SET,GET,GETSET"}  # 测试类型，以逗号分隔
DELAY=${DELAY:-0}               # 请求间延迟（微秒）
JSON_OUTPUT=${JSON_OUTPUT:-0}   # 是否输出JSON格式
QUIET=${QUIET:-1}              # 安静模式

echo "Redis基准测试配置:"
echo "主机: $REDIS_HOST:$REDIS_PORT"
echo "客户端数: $CLIENTS"
echo "请求数/客户端: $REQUESTS"
echo "键空间大小: $KEYSPACE_LEN"
echo "数据大小: $DATA_SIZE 字节"
echo "测试类型: $TEST_TYPES"
echo "延迟: $DELAY 微秒"

# 检查redis是否运行
if ! nc -z $REDIS_HOST $REDIS_PORT; then
    echo "错误: Redis服务器 $REDIS_HOST:$REDIS_PORT 不响应"
    exit 1
fi

# 设置JSON输出选项
JSON_OPT=""
if [ "$JSON_OUTPUT" -eq 1 ]; then
    JSON_OPT="-json"
fi

# 为每个测试类型运行benchmark
for TEST_TYPE in $(echo $TEST_TYPES | sed "s/,/ /g"); do
    echo ""
    echo "运行 $TEST_TYPE 测试..."

    # 构建redis-benchmark命令
    CMD="redis-benchmark -h $REDIS_HOST -p $REDIS_PORT -c $CLIENTS -n $REQUESTS \
          -d $DATA_SIZE -k $KEYSPACE_LEN -t $TEST_TYPE \
          --latency $JSON_OPT"

    if [ "$DELAY" -gt 0 ]; then
        CMD="$CMD --intrinsic-latency 100000"  # 内部延迟测试
    fi

    if [ "$QUIET" -eq 1 ]; then
        CMD="$CMD -q"
    fi

    echo "执行: $CMD"
    eval $CMD

    sleep 2  # 测试间停顿
done

# 输出总结
echo ""
echo "Redis基准测试完成。平均吞吐量和延迟信息已在上面输出。"
echo "可将此脚本与chk_restore.py结合，用于迁移期间性能分析。"