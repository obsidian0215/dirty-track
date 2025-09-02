#!/bin/bash

# Redis复杂负载生成脚本，使用YCSB外部基准测试工具
# YCSB可以高度配置负载大小、类型、分布等

set -e

# YCSB配置，通过环境变量或默认值
YCSB_HOME=${YCSB_HOME:-/usr/local/ycsb}
HOST=${HOST:-localhost}
PORT=${PORT:-6379}
RECORDCOUNT=${RECORDCOUNT:-10000}        # 记录数（负载大小）
OPERATIONCOUNT=${OPERATIONCOUNT:-50000}  # 操作数（负载总量）
READPROP=${READPROP:-0.50}                # 读取成分比例
UPDATEPROP=${UPDATEPROP:-0.40}           # 更新比例
INSERTPROP=${INSERTPROP:-0.10}           # 插入比例
FIELDC=${FIELDC:-10}                     # 字段数（内容复杂度）
FIELDL=${FIELDL:-100}                    # 字段长度（字节）
REQDIST=${REQDIST:-zipfian}             # 请求分布类型
DELAY=${DELAY:-5}                         # 每次run间延时（秒）
THREADS=${THREADS:-10}                   # 并发线程数

echo "使用YCSB配置Redis负载:"
echo "记录数: $RECORDCOUNT"
echo "操作数: $OPERATIONCOUNT"
echo "读取比例: $READPROP"
echo "更新比例: $UPDATEPROP"
echo "插入比例: $INSERTPROP"
echo "字段数: $FIELDC"
echo "字段长度: $FIELDL"
echo "请求分布: $REQDIST"

if [ ! -d "$YCSB_HOME" ]; then
  echo "错误: YCSB未安装，请设置YCSB_HOME环境变量或下载YCSB到 $YCSB_HOME"
  echo "下载地址: https://github.com/brianfrankcooper/YCSB"
  exit 1
fi

cd "$YCSB_HOME"

# 生成工作负载配置文件
cat > /tmp/redis-workload.properties <<EOF
recordcount=$RECORDCOUNT
operationcount=$OPERATIONCOUNT
readproportion=$READPROP
updateproportion=$UPDATEPROP
insertproportion=$INSERTPROP
readmodifywriteproportion=0.0
scanproportion=0.0
workload=com.yahoo.ycsb.workloads.CoreWorkload
fieldcount=$FIELDC
fieldlength=$FIELDL
requestdistribution=$REQDIST
EOF

echo "正在预加载数据..."
./bin/ycsb load redis -P /tmp/redis-workload.properties -p redis.host=$HOST -p redis.port=$PORT -threads $THREADS

echo "开始运行持续负载生成..."
while true; do
  echo "$(date): 执行负载测试..."
  ./bin/ycsb run redis -P /tmp/redis-workload.properties -p redis.host=$HOST -p redis.port=$PORT -threads $THREADS | tail -5
  sleep $DELAY
done