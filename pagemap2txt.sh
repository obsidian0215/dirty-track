#!/usr/bin/bash
# 该文件需要在runc容器热迁移根目录下运行
# 检查是否提供了参数
if [ $# -ne 2 ]; then
  echo "Usage: $0 <N> <container-name>"
  exit 1
fi

# set -e  # 当命令失败时，脚本会立即退出
# set -x  # 打印每一个执行的命令

N=$1
CONTAINER_NAME=$2

TARGET_DIR="/runc/containers/$CONTAINER_NAME/migrate"

# 确保TARGET_DIR存在
if [ ! -d "$TARGET_DIR" ]; then
  echo "Error: Target directory '$TARGET_DIR' does not exist."
  exit 1
fi

# 函数：处理 pagemap-*.img 文件
process_pagemap() {
  local parent_dir="$1"
  local log_dir="$2"

  echo "Processing pagemap files in '$parent_dir'..."

  # 检查 parent_i 目录是否存在
  if [ ! -d "$parent_dir" ]; then
    echo "Warning: Directory '$parent_dir' does not exist. Skipping."
    return
  fi

  # 创建日志目录
  mkdir -p "$log_dir"

  # 使用 find 查找文件
  find "$parent_dir" -maxdepth 1 -type f -name "pagemap-*.img" | while IFS= read -r img_file; do
    if [ -f "$img_file" ]; then
      base_name=$(basename "$img_file")
      echo "Running 'crit show' on '$img_file'..."
      crit show "$img_file" > "$log_dir/${base_name}.txt" || {
        echo "Error: 'crit show' failed for '$img_file'."
        # 继续处理下一个文件
      }
    fi
  done

  # 检查是否有文件被处理
  processed_files=$(find "$parent_dir" -maxdepth 1 -type f -name "pagemap-*.img" | wc -l)
  if [ "$processed_files" -eq 0 ]; then
    echo "Warning: No pagemap-*.img files found in '$parent_dir'."
  fi
}

# 函数：处理 image/pagemap-*.img 文件
process_image_pagemap() {
  local image_dir="$1"
  local d_log_dir="$2"

  echo "Processing image pagemap files in '$image_dir'..."

  # 检查 image 目录是否存在
  if [ ! -d "$image_dir" ]; then
    echo "Warning: Image directory '$image_dir' does not exist. Skipping image pagemap processing."
    return
  fi

  # 创建 d_log 目录
  mkdir -p "$d_log_dir"

  # 使用 find 查找文件
  find "$image_dir" -maxdepth 1 -type f -name "pagemap-*.img" | while IFS= read -r img_file; do
    if [ -f "$img_file" ]; then
      base_name=$(basename "$img_file")
      echo "Running 'crit show' on '$img_file'..."
      crit show "$img_file" > "$d_log_dir/${base_name}.txt" || {
        echo "Error: 'crit show' failed for '$img_file'."
        # 继续处理下一个文件
      }
    fi
  done

  # 检查是否有文件被处理
  processed_files=$(find "$image_dir" -maxdepth 1 -type f -name "pagemap-*.img" | wc -l)
  if [ "$processed_files" -eq 0 ]; then
    echo "Warning: No pagemap-*.img files found in '$image_dir'."
  fi
}

# 函数：处理 dirty_map 文件
process_dirty_map() {
  local dirty_map_dir="$1"

  echo "Processing dirty_map files in '$dirty_map_dir'..."

  # 检查 dirty_map 目录是否存在
  if [ ! -d "$dirty_map_dir" ]; then
    echo "Warning: Dirty map directory '$dirty_map_dir' does not exist. Skipping dirty_map processing."
    return
  fi

  echo "开始处理 dirty_map 目录中的文件..."

  # 使用 find 查找 .dirtymap 文件
  find "$dirty_map_dir" -type f -name "*.dirtymap" | while IFS= read -r file; do
    echo "Processing file: $file"
    # 确认 read_dirtymap 脚本存在且可执行
    if [ ! -x "./read_dirtymap" ]; then
      echo "Error: Script './read_dirtymap' does not exist or is not executable."
      continue
    fi

    ./read_dirtymap "$file" > "${file%.dirtymap}.txt" || {
      echo "Error: 'read_dirtymap' failed for '$file'."
      # 继续处理下一个文件
    }
  done

  # # 使用 find 查找 .heatmap 文件
  # find "$dirty_map_dir" -type f -name "*.heatmap" | while IFS= read -r file; do
  #   echo "Processing file: $file"
  #   # 确认 read_heatmap 脚本存在且可执行
  #   if [ ! -x "./read_heatmap" ]; then
  #     echo "Error: Script './read_heatmap' does not exist or is not executable."
  #     continue
  #   fi

  #   ./read_heatmap "$file" > "${file%.heatmap}.txt" || {
  #     echo "Error: 'read_heatmap' failed for '$file'."
  #     # 继续处理下一个文件
  #   }
  # done
}

# 函数：处理 timestamp_list.pid 文件
process_timestamp_list() {
  local dirty_map_dir="$1"

  echo "Processing timestamp_list.pid files in '$dirty_map_dir'..."

  # 检查 dirty_map 目录是否存在
  if [ ! -d "$dirty_map_dir" ]; then
    echo "Warning: Dirty map directory '$dirty_map_dir' does not exist. Skipping dirty_map processing."
    return
  fi

  echo "开始处理 dirty_map 目录中的文件..."

  # 使用 find 查找 .dirtymap 文件
  find "$dirty_map_dir" -type f -name "timestamp_list.*" | while IFS= read -r file; do
    echo "Processing file: $file"
    # 确认 read_timestamp 脚本存在且可执行
    if [ ! -x "./read_timestamp" ]; then
      echo "Error: Script './read_timestamp' does not exist or is not executable."
      continue
    fi

    ./read_timestamp "$file" > "${file}.txt" || {
      echo "Error: 'read_timestamp' failed for '$file'."
      # 继续处理下一个文件
    }
  done
}

# 开始处理所有 pd_i 文件夹
for i in $(seq 0 "$N"); do
  PD_PARENT_DIR="$TARGET_DIR/parent_$i"
  LOG_DIR="$TARGET_DIR/pd_log_$i"

  process_pagemap "$PD_PARENT_DIR" "$LOG_DIR"
done

# 开始处理 image/pagemap-*.img 文件
IMAGE_DIR="$TARGET_DIR/image"
D_LOG_DIR="$TARGET_DIR/d_log"

process_image_pagemap "$IMAGE_DIR" "$D_LOG_DIR"

# 最后处理 dirty_map 文件
DIRTY_MAP_DIR="$TARGET_DIR/dirty_map"
process_dirty_map "$DIRTY_MAP_DIR"
process_timestamp_list "$DIRTY_MAP_DIR"