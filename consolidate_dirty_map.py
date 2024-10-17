import os
import sys
from fcntl import ioctl
import psutil
import struct
import time

# 整合同一PID的dirty-map文件为newest-<pid>.img，同时添加时间相关的权重
def consolidate_dirty_maps_weighted(dirty_map_path):
    pid_files = {}
    # 遍历dirty_map_path目录下的所有文件，排除以newest-开头的img文件
    for filename in os.listdir(dirty_map_path):
        if filename.endswith('.img') and not filename.startswith('newest-'):
            parts = filename.split('-')
            if len(parts) < 2:
                continue
            pid_str, timestamp_str = parts[0], parts[1].split('.')[0]
            try:
                pid = int(pid_str)
                timestamp = int(timestamp_str)
            except ValueError:
                print(f"文件 {filename} 的 PID 或时间戳无效，跳过。")
                continue
            if pid not in pid_files:
                pid_files[pid] = []
            pid_files[pid].append((timestamp, os.path.join(dirty_map_path, filename)))

    # 对每个 PID 的文件进行整合
    for pid, files in pid_files.items():
        if not files:
            continue
        # 按时间戳排序（升序）
        sorted_files = sorted(files, key=lambda x: x[0])
        N = len(sorted_files)
        timestamps = [ts for ts, _ in sorted_files]
        min_ts = min(timestamps)
        max_ts = max(timestamps)
        
        # 防止分母为零
        if max_ts == min_ts:
            weight_factors = [1.0 for _ in sorted_files]
        else:
            weight_factors = [(ts - min_ts) / (max_ts - min_ts) for ts in timestamps]
            # 确保最大权重不超过1
            weight_factors = [min(0.3 * w + 0.7 / (2 ** (N - i - 1)), 1.0) for i, w in enumerate(weight_factors)]

        consolidated = {}

        for (timestamp, file_path), weight in zip(sorted_files, weight_factors):
            print(f"open file:{file_path}, weight_factor:{weight}")
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(20)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
                    if not data or len(data) < 20:
                        break
                    address, write_count, page_type = struct.unpack('<QQI', data)
                    if address in consolidated:
                        consolidated[address]['write_count'] += write_count * weight
                    else:
                        consolidated[address] = {
                            'write_count': write_count * weight,
                            'page_type': page_type
                        }
                    print(f"print address:{hex(address)}, write_count:{write_count}, page_type:{page_type}")
        
        # 对consolidated中的address进行排序
        sorted_addresses = sorted(consolidated.keys())

        # 写入 consolidated 的数据到 newest-<pid>.img，覆盖已有文件
        newest_img_path = os.path.join(dirty_map_path, f'newest-{pid}.img')
        with open(newest_img_path, 'wb') as f:
            for address in sorted_addresses:
                info = consolidated[address]
                # write_count为浮点数，需要转换为整数
                # 需要避免write_count加权和小于1
                write_count_weighted = max(int(info['write_count']), 1)
                packed = struct.pack('<QQI', address, write_count_weighted, info['page_type'])
                f.write(packed)
        print(f"已生成整合后的脏页映射文件: {newest_img_path}")

path = "./dirty_map"
consolidate_dirty_maps_weighted(path)