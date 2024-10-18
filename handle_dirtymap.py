import os
import struct
from typing import List, Dict, Tuple
from dataclasses import dataclass, field

@dataclass
class DirtyMapEntry:
    address: int
    write_count: float
    page_type: int
    heat_level: int = field(default=100)

def prehandle_dirtymap(dirty_map_path: str) -> Dict[str, List[Dict]]:
    """
    预处理 dirty_map 文件夹中的 dirtymap 文件，返回包含每个 PID 最新 dirtymap 和整合后的旧 dirtymap 的列表。

    Args:
        dirty_map_path (str): dirty_map 文件夹的路径。

    Returns:
        Dict[str, List[Dict]]: 包含 'latest_dirtymaps' 和 'consolidated_dirtymaps' 两个列表的字典。
    """
    pid_files: Dict[int, List[Tuple[int, str]]] = {}
    latest_dirtymaps: List[Dict] = []
    consolidated_dirtymaps: List[Dict] = []

    # 遍历 dirty_map_path 目录下的所有文件，排除以 newest- 开头的 img 文件
    for filename in os.listdir(dirty_map_path):
        if filename.endswith('.img') and not filename.startswith('newest-') and not filename.startswith('consolidated-'):
            parts = filename.split('-')
            if len(parts) < 2:
                continue
            pid_str, timestamp_str = parts[0], parts[1].split('.')[0]
            try:
                pid = int(pid_str)
                timestamp = int(timestamp_str)
            except ValueError:
                print(f"{filename} 的 PID 或时间戳无效，跳过")
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

        # 分离最新的 dirtymap
        latest_timestamp, latest_file_path = sorted_files[-1]
        older_files = sorted_files[:-1]

        # 读取最新 dirtymap
        latest_dirtymap_entries = []
        try:
            with open(latest_file_path, 'rb') as f:
                while True:
                    data = f.read(20)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
                    if not data or len(data) < 20:
                        break
                    address, write_count, page_type = struct.unpack('<QQI', data)
                    entry = DirtyMapEntry(
                        address=address,
                        write_count=float(write_count),
                        page_type=page_type
                    )
                    latest_dirtymap_entries.append(entry)
        except IOError as e:
            print(f"无法读取最新文件 {latest_file_path}，错误：{e}")
            continue

        latest_dirtymaps.append({
            'pid': pid,
            'timestamp': latest_timestamp,
            'dirtymap': latest_dirtymap_entries
        })

        if not older_files:
            # 如果只有一个文件，复制为 old-<pid>.img 并添加到 consolidated_dirtymaps
            oldest_img_path = os.path.join(dirty_map_path, f'old-{pid}.img')
            try:
                os.replace(latest_file_path, oldest_img_path)
                print(f"PID：{pid} 只有一个脏页映射文件，直接复制为 old-{pid}.img")
                # 将最新 dirtymap 作为整合后的 dirtymap
                consolidated_dirtymaps.append({
                    'pid': pid,
                    'dirtymap': latest_dirtymap_entries,
                    'source_files': [oldest_img_path]
                })
            except OSError as e:
                print(f"无法重命名文件 {latest_file_path} 为 {oldest_img_path}，错误：{e}")
            continue

        N = len(older_files)
        timestamps = [ts for ts, _ in older_files]
        min_ts = min(timestamps)
        max_ts = max(timestamps)

        # 防止分母为零
        if max_ts == min_ts:
            weight_factors = [1.0 for _ in older_files]
        else:
            weight_factors = [(ts - min_ts) / (max_ts - min_ts) for ts in timestamps]
            # 确保最大权重不超过1，并根据文件顺序调整权重
            weight_factors = [
                min(0.3 * w + 0.7 / (2 ** (N - i - 1)), 1.0) 
                for i, w in enumerate(weight_factors)
            ]

        # 整合旧的 dirtymap
        consolidated = {}
        global_max_write_count = 0.0

        for (timestamp, file_path), weight in zip(older_files, weight_factors):
            try:
                with open(file_path, 'rb') as f:
                    while True:
                        data = f.read(20)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
                        if not data or len(data) < 20:
                            break
                        address, write_count, page_type = struct.unpack('<QQI', data)
                        if address in consolidated:
                            consolidated[address].write_count += write_count * weight
                            # 假设 page_type 取最新的类型
                            consolidated[address].page_type = page_type
                        else:
                            consolidated[address] = DirtyMapEntry(
                                address=address,
                                write_count=write_count * weight,
                                page_type=page_type
                            )
                        # 更新全局最大 write_count
                        if consolidated[address].write_count > global_max_write_count:
                            global_max_write_count = consolidated[address].write_count
            except IOError as e:
                print(f"无法读取文件 {file_path}，错误：{e}")
                continue

        if global_max_write_count == 0:
            print(f"PID：{pid} 所有地址的 dirty-track 无效，跳过")
            continue

        # 转换 consolidated 字典为 DirtyMapEntry 列表，并设置 heat_level
        consolidated_dirtymap_entries = [
            DirtyMapEntry(
                address=addr,
                write_count=entry.write_count,
                page_type=entry.page_type,
                heat_level=100  # 初始化为100
            )
            for addr, entry in consolidated.items()
        ]

        consolidated_dirtymaps.append({
            'pid': pid,
            'dirtymap': consolidated_dirtymap_entries,
            'source_files': [fp for _, fp in older_files]
        })
        print(f"PID：{pid} 的旧 dirtymap 已整合，包含 {len(consolidated_dirtymap_entries)} 个条目")

    return {
        'latest_dirtymaps': latest_dirtymaps,
        'consolidated_dirtymaps': consolidated_dirtymaps
    }

def compute_write_count_distribution(dirtymap: List[DirtyMapEntry]) -> Dict[str, float]:
    """
    计算给定 dirtymap 中 write_count 的统计分布数据。
    
    Args:
        dirtymap (List[DirtyMapEntry]): DirtyMapEntry 实例的列表。
    
    Returns:
        Dict[str, float]: 包含最小值、最大值、平均值、中位数、标准差等统计信息的字典。
    """
    if not dirtymap:
        return {}
    
    write_counts = sorted(entry.write_count for entry in dirtymap)
    n = len(write_counts)
    min_write = write_counts[0]
    max_write = write_counts[-1]
    sum_write = sum(write_counts)
    mean_write = sum_write / n
    
    # 计算中位数
    if n % 2 == 1:
        median_write = write_counts[n // 2]
    else:
        median_write = (write_counts[n // 2 - 1] + write_counts[n // 2]) / 2
    
    # 计算标准差
    variance = sum((wc - mean_write) ** 2 for wc in write_counts) / n
    std_dev = math.sqrt(variance)
    
    distribution = {
        'count': n,
        'min': min_write,
        'max': max_write,
        'mean': mean_write,
        'median': median_write,
        'std_dev': std_dev
    }
    
    return distribution

def find_exceptionally_high_write_counts(dirtymap: List[DirtyMapEntry], multiplier: float = 3.0) -> List[DirtyMapEntry]:
    """
    识别 dirtymap 中 write_count 异常高的条目。
    异常高的定义为 write_count > mean + multiplier * std_dev。
    
    Args:
        dirtymap (List[DirtyMapEntry]): DirtyMapEntry 实例的列表。
        multiplier (float): 用于确定异常阈值的倍数。
    
    Returns:
        List[DirtyMapEntry]: 异常高 write_count 的 DirtyMapEntry 列表。
    """
    distribution = compute_write_count_distribution(dirtymap)
    if not distribution:
        return []
    
    mean = distribution['mean']
    std_dev = distribution['std_dev']
    threshold = mean + multiplier * std_dev
    
    exceptional_entries = [entry for entry in dirtymap if entry.write_count > threshold]
    
    print(f"识别出 {len(exceptional_entries)} 个异常高的 write_count 条目（阈值 > {threshold:.2f}）")
    
    return exceptional_entries

def assign_heat_level(dirtymap: List[DirtyMapEntry], num_intervals: int = 5) -> None:
    """
    根据 write_count 大小为 dirtymap 中的每个条目分配 heat_level。
    将除去异常高 write_count 的条目分为 num_intervals 个区间，并根据区间赋值 heat_level。
    
    Args:
        dirtymap (List[DirtyMapEntry]): DirtyMapEntry 实例的列表。
        num_intervals (int): 将 write_count 分为的区间数（默认为5）。
    
    Returns:
        None: 直接修改 dirtymap 中每个 DirtyMapEntry 的 heat_level 属性。
    """
    if not dirtymap:
        return
    
    # 首先计算分布
    distribution = compute_write_count_distribution(dirtymap)
    mean = distribution.get('mean', 0)
    std_dev = distribution.get('std_dev', 0)
    threshold = mean + 3 * std_dev  # 使用3倍标准差作为异常高的阈值
    
    # 分为异常高和正常
    normal_entries = [entry for entry in dirtymap if entry.write_count <= threshold]
    exceptional_entries = [entry for entry in dirtymap if entry.write_count > threshold]
    
    if not normal_entries:
        print("没有足够的正常 write_count 条目进行 heat_level 分配。")
        return
    
    # 获取正常条目的 write_count 范围
    write_counts = sorted(entry.write_count for entry in normal_entries)
    min_write = write_counts[0]
    max_write = write_counts[-1]
    
    # 定义区间边界
    interval_size = (max_write - min_write) / num_intervals if num_intervals > 0 else 1
    if interval_size == 0:
        interval_size = 1  # 防止除以零
    
    # 创建区间边界列表
    boundaries = [min_write + i * interval_size for i in range(1, num_intervals)]
    
    # 辅助函数：根据 write_count 找到所属区间
    def find_interval(write_count: float) -> int:
        for i, boundary in enumerate(boundaries):
            if write_count <= boundary:
                return i
        return num_intervals - 1  # 最后一个区间
    
    # 分配 heat_level
    for entry in normal_entries:
        interval = find_interval(entry.write_count)
        # 定义 heat_level 的分配策略，例如：较高的区间对应较高的 heat_level
        # 这里假设 heat_level 低于100，根据区间增加
        entry.heat_level = 50 + int((interval + 1) * (50 / num_intervals))
    
    # 对于异常高的条目，可以赋予最高的 heat_level或特殊标记
    for entry in exceptional_entries:
        entry.heat_level = 100  # 保持初始化值，或根据需要调整
    
    print(f"为 {len(normal_entries)} 个正常条目分配了 heat_level，{len(exceptional_entries)} 个异常条目已标记为最高 heat_level。")

def process_dirtymap_entries(dirtymap: List[DirtyMapEntry], num_intervals: int = 5) -> None:
    """
    对整个 dirtymap 的 DirtyMapEntry 列表进行统计、异常检测和 heat_level 分配。
    
    Args:
        dirtymap (List[DirtyMapEntry]): DirtyMapEntry 实例的列表。
        num_intervals (int): heat_level 分配的区间数（默认为5）。
    
    Returns:
        None: 直接修改每个 DirtyMapEntry 的 heat_level 属性。
    """
    distribution = compute_write_count_distribution(dirtymap)
    print(f"DirtyMap 统计信息: {distribution}")
    
    exceptional_entries = find_exceptionally_high_write_counts(dirtymap, multiplier=3.0)
    
    assign_heat_level(dirtymap, num_intervals=num_intervals)
    
    # 如果需要单独处理异常高的条目，可以在这里进行
    # 例如，将异常高的条目记录到日志或进行特殊处理
    # 在当前实现中，异常高的条目已被赋予最高的 heat_level

# 示例调用
if __name__ == "__main__":
    dirty_map_path = os.path.abspath('./dirty_map')
    result = prehandle_dirtymap(dirty_map_path)
    
    # 打印部分结果以验证
    for latest in result['latest_dirtymaps']:
        print(f"最新 dirtymap - PID: {latest['pid']}, Timestamp: {latest['timestamp']}, 条目数: {len(latest['dirtymap'])}")
        print(latest['dirtymap'][0])
    
    for consolidated in result['consolidated_dirtymaps']:
        print(f"整合后的旧 dirtymap - PID: {consolidated['pid']}, 来源文件数: {len(consolidated['source_files'])}, 条目数: {len(consolidated['dirtymap'])}")
        print(consolidated['dirtymap'][0])
    
