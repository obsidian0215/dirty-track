import struct
import os
import sys
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm

def plot_smooth_histogram(dirty_map_file_path, max_ticks=10):
    addresses = []
    write_counts = []

    # 验证文件是否存在
    if not os.path.exists(dirty_map_file_path):
        print(f"错误: dirtymap文件 '{dirty_map_file_path}' 不存在")
        return

    with open(dirty_map_file_path, 'rb') as f:
        # 读取8字节的时间戳（little-endian格式的持续时间ns）
        tracker_duration_data = f.read(8)
        if len(tracker_duration_data) < 8:
            print("错误: dirtymap文件格式不正确，无法读取时间戳")
            return

        tracker_duration_ns = struct.unpack('<Q', tracker_duration_data)[0]
        print(f"dirty-track追踪持续时间: {tracker_duration_ns} ns ({tracker_duration_ns / 1000000:.2f} ms)")

        # 读取地址-计数对，每个记录12字节（8字节地址 + 4字节计数）
        while True:
            data = f.read(12)  # light-dt格式：8字节地址 + 4字节计数
            if not data or len(data) < 12:
                break

            address, write_count = struct.unpack('<QI', data)
            addresses.append(address)
            write_counts.append(write_count)

    # 将address和write_count转换为numpy数组，方便处理
    addresses = np.array(addresses)
    write_counts = np.array(write_counts)

    # # 对数据进行排序 (根据地址排序)
    # sorted_indices = np.argsort(addresses)
    # addresses = addresses[sorted_indices]
    # write_counts = write_counts[sorted_indices]

    # 计算前后地址的差值，划分区间
    intervals = []
    start_idx = 0
    for i in range(1, len(addresses)):
        if addresses[i] - addresses[i - 1] > 0x40000000:  # 1GB = 0x40000000
            # 如果地址差值大于1GB，结束当前区间，开始新的区间
            intervals.append((start_idx, i - 1))
            start_idx = i
    # 添加最后一个区间
    intervals.append((start_idx, len(addresses) - 1))

    # 分段绘制
    for idx, (start, end) in enumerate(intervals):
        addresses_in_range = addresses[start:end+1]
        write_counts_in_range = write_counts[start:end+1]

        if len(addresses_in_range) == 0:
            continue  # 跳过没有数据的区间

        plt.figure(figsize=(12, 6))
        plt.scatter(addresses_in_range, write_counts_in_range, s=10, c='b', alpha=0.7, label=f'Address Range {idx + 1}: {hex(addresses_in_range[0])} - {hex(addresses_in_range[-1])}')
        plt.xlabel('Address (Hex)')
        plt.ylabel('Write Count')
        plt.title(f'Write Count in Address Range {idx + 1}: {hex(addresses_in_range[0])} - {hex(addresses_in_range[-1])}')
        plt.grid(True)

        # 获取当前区间的最小地址和最大地址
        min_address = addresses_in_range.min()
        max_address = addresses_in_range.max()

       # 计算地址范围
        address_range = max_address - min_address

        # 生成关键刻度，显示区间的起点、终点和一些中间值
        if max_ticks >= 2:
            xticks = np.linspace(min_address, max_address, min(max_ticks, len(addresses_in_range), 10))  # 至少2个刻度，最多max_ticks个
        else:
            xticks = [min_address, max_address]  # 如果只允许2个刻度，显示起点和终点

        # 将刻度对齐到4KB边界
        xticks = np.array([x - (x % 0x1000) for x in xticks], dtype=int)  # 4KB对齐
        xtick_labels = [hex(x) for x in xticks]
        plt.xticks(xticks, xtick_labels, rotation=45)

    plt.legend()
    plt.tight_layout()
    plt.show()
# 命令行参数处理
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("用法: python draw_dirtymap.py <dirtymap_file_path>")
        print("示例:")
        print("  python draw_dirtymap.py /tmp/dir/6313-1672584567203.dirtymap")
        print("  python draw_dirtymap.py ./output.dirtymap")
        sys.exit(1)

    dirtymap_file = sys.argv[1]

    # 调用函数绘制图形
    plot_smooth_histogram(dirtymap_file)