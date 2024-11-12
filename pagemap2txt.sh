#!/usr/bin/bash
# 该文件需要在runc容器热迁移根目录下运行
# 检查是否提供了参数
if [ $# -ne 2 ]; then
  echo "Usage: $0 <N> <container-name>"
  exit 1
fi

N=$1
CONTAINER_NAME=$2

TARGET_DIR="/runc/containers/$CONTAINER_NAME/migrate"

# 确保TARGET_DIR存在
if [ ! -d "$TARGET_DIR" ]; then
  echo "Error: Target directory '$TARGET_DIR' does not exist."
  exit 1
fi

# 遍历所有pd_i文件夹
for i in $(seq 0 "$N")
do
  PD_PARENT_DIR="$TARGET_DIR/parent_$i"
  LOG_DIR="$TARGET_DIR/pd_log_$i"

  # 检查parent_i目录是否存在
  if [ ! -d "$PD_PARENT_DIR" ]; then
    echo "Warning: Directory '$PD_PARENT_DIR' does not exist. Skipping."
    continue
  fi

  # 创建日志目录
  mkdir -p "$LOG_DIR"

  # 处理pd_i/pagemap-*.img
  shopt -s nullglob
  pagemap_files=("$PD_PARENT_DIR"/pagemap-*.img)
  shopt -u nullglob

  if [ ${#pagemap_files[@]} -eq 0 ]; then
    echo "Warning: No pagemap-*.img files found in '$PD_PARENT_DIR'."
    continue
  fi

  for img_file in "${pagemap_files[@]}"
  do
    if [ -f "$img_file" ]; then
      base_name=$(basename "$img_file")
      crit show "$img_file" > "$LOG_DIR/${base_name}.txt"
      if [ $? -ne 0 ]; then
        echo "Error: 'crit show' failed for '$img_file'."
      fi
    fi
  done
done

# 处理image/pagemap-*.img
IMAGE_DIR="$TARGET_DIR/image"
D_LOG_DIR="$TARGET_DIR/d_log"

# 检查image目录是否存在
if [ ! -d "$IMAGE_DIR" ]; then
  echo "Warning: Image directory '$IMAGE_DIR' does not exist. Skipping image pagemap processing."
else
  # 创建d_log目录
  mkdir -p "$D_LOG_DIR"

  shopt -s nullglob
  image_pagemap_files=("$IMAGE_DIR"/pagemap-*.img)
  shopt -u nullglob

  if [ ${#image_pagemap_files[@]} -eq 0 ]; then
    echo "Warning: No pagemap-*.img files found in '$IMAGE_DIR'."
  else
    for img_file in "${image_pagemap_files[@]}"
    do
      if [ -f "$img_file" ]; then
        base_name=$(basename "$img_file")
        crit show "$img_file" > "$D_LOG_DIR/${base_name}.txt"
        if [ $? -ne 0 ]; then
          echo "Error: 'crit show' failed for '$img_file'."
        fi
      fi
    done
  fi
fi

# 处理dirty_map的.dirtymap和.heatmap文件
DIRTY_MAP_DIR="$TARGET_DIR/dirty_map"

# 检查dirty_map目录是否存在
if [ ! -d "$DIRTY_MAP_DIR" ]; then
  echo "Warning: Dirty map directory '$DIRTY_MAP_DIR' does not exist. Skipping dirty_map processing."
else
  shopt -s nullglob
  dirtymap_files=("$DIRTY_MAP_DIR"/*.dirtymap)
  heatmap_files=("$DIRTY_MAP_DIR"/*.heatmap)
  shopt -u nullglob

  for file in "${dirtymap_files[@]}"
  do
    if [ -f "$file" ]; then
      ./read_dirtymap "$file" > "${file%.dirtymap}.txt"
      if [ $? -ne 0 ]; then
        echo "Error: 'read_dirtymap' failed for '$file'."
      fi
    fi
  done

  for file in "${heatmap_files[@]}"
  do
    if [ -f "$file" ]; then
      ./read_heatmap "$file" > "${file%.heatmap}.txt"
      if [ $? -ne 0 ]; then
        echo "Error: 'read_heatmap' failed for '$file'."
      fi
    fi
  done
fi