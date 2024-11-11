#!/usr/bin/bash
# 该文件需要在runc容器热迁移根目录下运行
# 检查是否提供了参数
if [ -z "$1" ]; then
  echo "Usage: $0 <N>"
  exit 1
fi

N=$1

# 遍历所有pd_i文件夹
for i in {0..N} # 将N替换为实际的最大值
do
  # 处理pd_i/pagemap-8.img
  if [ -f "parent_$i/pagemap-8.img" ]; then
    crit show "pd_$i/pagemap-8.img" > "pd_log_$i/pagemap-8"
  fi

  # 处理pd_i/pagemap-9.img
  if [ -f "parent_$i/pagemap-9.img" ]; then
    crit show "pd_$i/pagemap-9.img" > "pd_log_$i/pagemap-9"
  fi
done

# 处理image/pagemap-8.img
if [ -f "image/pagemap-8.img" ]; then
  crit show "image/pagemap-8.img" > "d_log/pagemap-8"
fi

# 处理image/pagemap-9.img
if [ -f "image/pagemap-9.img" ]; then
  crit show "image/pagemap-9.img" > "d_log/pagemap-9"
fi
