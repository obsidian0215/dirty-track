import struct
import os
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.api as sm

def plot_smooth_histogram(dirty_map_path, max_ticks=10):
    addresses = []
    write_counts = []

    # 假设文件名为 newest-<pid>.img
    newest_img_path = os.path.join(dirty_map_path, 'newest-6313.img')  # 替换为实际文件路径

    with open(newest_img_path, 'rb') as f:
        while True:
            data = f.read(20)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
            if not data or len(data) < 20:
                break
            address, write_count, page_type = struct.unpack('<QQI', data)
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
# 调用函数绘制图形
plot_smooth_histogram('.')  # 替换为实际路径