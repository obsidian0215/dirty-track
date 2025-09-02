#!/bin/bash

# 构建InfluxDB OCI bundle脚本
# 将Docker镜像转换为runc OCI bundle，便于chk_restore.py集成

set -e

# 设置参数
SERVICE_NAME="influxdb"
IMAGE="docker.io/library/influxdb:latest"
BUNDLE_DIR="$SERVICE_NAME"

# 创建bundle目录
mkdir -p "$BUNDLE_DIR/rootfs"
mkdir -p "$BUNDLE_DIR/oci"

echo "正在拉取Docker镜像: $IMAGE"
# 拉取Docker镜像到OCI格式
skopeo copy "docker://$IMAGE" "oci:$BUNDLE_DIR/oci"

echo "正在解压到rootfs"
# 使用umoci解压layer到rootfs
umoci unpack --image "$BUNDLE_DIR/oci" "$BUNDLE_DIR/rootfs"

echo "正在生成config.json"
# 生成config.json
cd "$BUNDLE_DIR"
runc spec --rootfs "./rootfs"
cd ..

echo "InfluxDB OCI bundle构建完成: $BUNDLE_DIR"
echo "注意: 确保已安装skopeo和umoci，如果未安装请先安装。"