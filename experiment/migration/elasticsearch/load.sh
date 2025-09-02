#!/bin/bash

# Elasticsearch复杂负载生成脚本，使用YCSB外部基准测试工具
# YCSB可以高度配置负载大小、类型、分布等

set -e

# YCSB配置，通过环境变量或默认值
YCSB_HOME=${YCSB_HOME:-/usr/local/ycsb}
HOST=${HOST:-localhost}
PORT=${PORT:-9200}
CLUSTER_NAME=${CLUSTER_NAME:-elasticsearch}
RECORDCOUNT=${RECORDCOUNT:-10000}
OPERATIONCOUNT=${OPERATIONCOUNT:-50000}
READPROP=${READPROP:-0.50}
UPDATEPROP=${UPDATEPROP:-0.40}
INSERTPROP=${INSERTPROP:-0.10}
FIELDC=${FIELDC:-10}
FIELDL=${FIELDL:-100}
REQDIST=${REQDIST:-zipfian}
DELAY=${DELAY:-5}
THREADS=${THREADS:-10}

echo "使用YCSB配置Elasticsearch负载:"

if [ ! -d "$YCSB_HOME" ]; then
  echo "错误: YCSB未安装，请设置YCSB_HOME环境变量或下载YCSB到 $YCSB_HOME"
  exit 1
fi

cd "$YCSB_HOME"

# 生成工作负载配置文件
cat > /tmp/elasticsearch-workload.properties <<EOF
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
./bin/ycsb load elasticsearch5 -P /tmp/elasticsearch-workload.properties -p elasticsearch.cluster.name=$CLUSTER_NAME -p elasticsearch.host=$HOST -p elasticsearch.port=$PORT -threads $THREADS

echo "开始运行持续负载生成..."
while true; do
  echo "$(date): 执行负载测试..."
  ./bin/ycsb run elasticsearch5 -P /tmp/elasticsearch-workload.properties -p elasticsearch.cluster.name=$CLUSTER_NAME -p elasticsearch.host=$HOST -p elasticsearch.port=$PORT -threads $THREADS | tail -5
  sleep $DELAY
done