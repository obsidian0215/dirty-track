import os
import struct
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
import statistics
import math

# 定义页面大小映射，根据 page_type 索引
PAGE_SIZES = [1 << 12, 1 << 21]  # 0: 4KB PTE, 1: 2MB PMD

"""
struct __((packed))__ {
    uint64_t address;
    uint32_t write_count;

    uint8_t page_type;
}
sizeof() = 26
"""
@dataclass
class DirtyMapEntry:
    address: int
    write_count: float
    page_type: int
    heat_level: int = field(default=100)

"""
struct __((packed))__ {
    uint64_t start;
    uint64_t end;
    uint64_t page_size;

    uint8_t heat_level;
    int8_t heat_trend;
}
sizeof() = 26
"""
@dataclass
class DirtyHeatMapEntry:
    start: int
    end: int
    page_size: int
    heat_level: int
    heat_trend: int

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
                    data = f.read(13)  # sizeof(dirty_page) = 8 + 4 + 1 = 13 bytes
                    if not data or len(data) < 13:
                        break
                    address, write_count, page_type = struct.unpack('<QIB', data)
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
                # os.replace(latest_file_path, oldest_img_path)
                print(f"PID：{pid} 只有一个脏页映射文件，直接复制为 old-{pid}.img")
                # 将最新 dirtymap 作为整合后的 dirtymap
                consolidated_dirtymaps.append({
                    'pid': pid,
                    'dirtymap': latest_dirtymap_entries,
                    'source_files': [latest_file_path]
                    # 'source_files': [oldest_img_path]
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
                        data = f.read(13)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
                        if not data or len(data) < 13:
                            break
                        address, write_count, page_type = struct.unpack('<QIB', data)
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

def detect_extreme_high_wc(write_counts: List[float]) -> float:
    """
    计算给定 dirtymap 中划分异常高 write_count 的阈值。
    
    Args:
        dirtymap (List[DirtyMapEntry]): DirtyMapEntry 列表。
    
    Returns:
        float: dirtymap 中异常高 write_count 的阈值。
    """
    if not write_counts:
        return 0
    
    # write_counts = sorted(entry.write_count for entry in dirtymap)
    n = len(write_counts)
    if n == 0:
        return 0  # 无数据
    elif n < 4096:
        # 小规模脏内存(<=16MB)：使用中位数和MAD
        try:
            median_wc = statistics.median(write_counts)
            mad = statistics.median([abs(wc - median_wc) for wc in write_counts])
            threshold = median_wc + 3 * mad  # 任意选择3倍MAD作为阈值
        except statistics.StatisticsError:
            median_wc = statistics.median(write_counts)
            threshold = max(write_counts) * 0.9
    elif n < 32768:
        # 中等规模脏内存(<=128MB)：使用四分位数方法
        try:
            # 使用四分位数方法检测异常值
            q1 = statistics.quantiles(write_counts, n=4)[0]  # 第一四分位数
            q3 = statistics.quantiles(write_counts, n=4)[2]  # 第三四分位数
            iqr = q3 - q1
            threshold = q3 + 1.5 * iqr  # 常用的异常高值检测阈值
        except statistics.StatisticsError:
            threshold = max(write_counts) * 0.9
    else:
        # 大规模脏内存：使用95百分位数
        try:
            percentile_95 = statistics.quantiles(write_counts, n=100)[94]  # 95th 百分位
            threshold = percentile_95
        except statistics.StatisticsError:
            threshold = max(write_counts) * 0.9

    return threshold


def convert_dirtymap_to_heatmap(dirtymap: List[DirtyMapEntry])-> List[DirtyHeatMapEntry]:
    """
    排除异常高write_count后将dirtymap转换为heatmap
    :param dirtymap: dict, key=address, value=DirtyMapEntry
    :return: list of HeatmapEntry
    """
    if not dirtymap:
        return []
    
    write_counts = [entry.write_count for entry in dirtymap]
    threshold = detect_extreme_high_wc(write_counts)
    
    # 找出非异常高write_count的最大值，用于归一化
    non_extreme_wcs = [wc for wc in write_counts if wc < threshold]
    max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1  # 避免除零
    
    # 排序地址
    sorted_entries = sorted(dirtymap, key=lambda x: x.address)
    
    heatmap_entries = []
    current_start = None
    current_end = None
    current_size = None
    current_heat_level = None
    
    for entry in sorted_entries:
        address = entry.address
        write_count = entry.write_count
        page_type = entry.page_type
        
        # 判断是否为异常高write_count
        if write_count >= threshold:
            heat_level = 100  # 最大heat_level
        else:
            normalized_wc = write_count / max_write_count
            # 归一化后均分为10级
            heat_level = math.ceil(normalized_wc * 9) + 1  # 1到10
            heat_level = min(max(heat_level, 1), 10)  # 确保在范围内
        
        # 判断是否可以与当前heatmap_entry合并
        if (current_heat_level == heat_level and 
            current_end == address):
            # 合并范围
            current_end = address + PAGE_SIZES[page_type]
            current_size += PAGE_SIZES[page_type]
        else:
            # 保存当前heatmap_entry
            if current_start is not None:
                heatmap_entries.append(DirtyHeatMapEntry(start=current_start, end=current_end, 
                        page_size=current_size, heat_level=current_heat_level, heat_trend=0))
            # 开始新的heatmap_entry
            current_start = address
            current_end = address + PAGE_SIZES[page_type]
            current_heat_level = heat_level
            current_size = PAGE_SIZES[page_type]
    
    # 添加最后一个heatmap_entry
    if current_start is not None:
        heatmap_entries.append(DirtyHeatMapEntry(start=current_start, end=current_end, 
                heat_level=current_heat_level, page_size=current_size, heat_trend=0))
    
    return heatmap_entries

# 示例调用
if __name__ == "__main__":
    dirty_map_path = os.path.abspath('./dirty_map')
    result = prehandle_dirtymap(dirty_map_path)
    
    # 打印部分结果以验证
    for latest in result['latest_dirtymaps']:
        print(f"最新 dirtymap - PID: {latest['pid']}, Timestamp: {latest['timestamp']}, 条目数: {len(latest['dirtymap'])}")
        # print(f"latest{latest['pid']}异常高值阈值: {detect_extreme_high_wc(latest['dirtymap'])}")
        latest['dirtymap'] = convert_dirtymap_to_heatmap(latest['dirtymap'])
        print(latest['dirtymap'])
        print(f"最新 heatmap 大小{len(latest['dirtymap'])}")
    
    for consolidated in result['consolidated_dirtymaps']:
        print(f"整合后的旧 dirtymap - PID: {consolidated['pid']}, 来源文件数: {len(consolidated['source_files'])}, 条目数: {len(consolidated['dirtymap'])}")
        # print(f"consolidated{consolidated['pid']}异常高值阈值: {detect_extreme_high_wc(consolidated['dirtymap'])}")
        consolidated['dirtymap'] = convert_dirtymap_to_heatmap(consolidated['dirtymap'])
        print(consolidated['dirtymap'])
        print(f"整合 heatmap 大小{len(consolidated['dirtymap'])}")
    
