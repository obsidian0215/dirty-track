#!/usr/bin/bash
# 该文件需要在runc容器热迁移根目录下运行
# 检查是否提供了参数
if [ $# -ne 2 ]; then
  echo "Usage: $0 <N> <container-name>"
  exit 1
fi

N=$1
CONTAINER_NAME = $2

TARGET_DIR="/runc/containers/$CONTAINER_NAME/migrate"

# 遍历所有pd_i文件夹
for i in {0..N} # 将N替换为实际的最大值
do
  # 处理pd_i/pagemap-*.img
  for img_file in "$SCRIPT_DIR/parent_$i/pagemap-*.img"; do
    [ -f "$img_file" ] || continue
    base_name=$(basename "$img_file")
    crit show "$img_file" > "$TARGET_DIR/pd_log_$i/$base_name"
  done
done

# 处理image/pagemap-*.img
for img_file in "$SCRIPT_DIR/image/pagemap-*.img"; do
  [ -f "$img_file" ] || continue
  base_name=$(basename "$img_file")
  crit show "$img_file" > "$TARGET_DIR/d_log/$base_name"
done

# 处理dirty_map的.dirtymap和.heatmap文件
DIRTY_MAP_DIR="$TARGET_DIR/dirty_map"

for file in "$DIRTY_MAP_DIR"/*.dirtymap
do
  [ -e "$file" ] || continue
  ./read_dirtymap "$file" > "${file%.dirtymap}.txt"
done

for file in "$DIRTY_MAP_DIR"/*.heatmap
do
  [ -e "$file" ] || continue
  ./read_heatmap "$file" > "${file%.heatmap}.txt"
done