import os
import struct
from typing import List, Dict, Tuple
from dataclasses import dataclass, field
import statistics
import math
import bisect

PAGE_SIZES = [1 << 12, 1 << 21]     # 0: 4KB PTE, 1: 2MB PMD

"""
struct __((packed))__ {
    uint64_t address;
    uint32_t write_count;
    // uint8_t page_type;   //size = 13
    uint32_t size;  // size = 16
}
"""
@dataclass
class DirtyMapEntry:
    address: int
    write_count: float
    size: int

"""
struct __((packed))__ {
    uint64_t address;
    // uint32_t write_count;
    uint32_t size;

    // added field
    uint8_t heat_level;
    int8_t heat_trend;
    uint8_t selected;   //size = 15
}
"""
@dataclass
class DirtyHeatMapEntry:
    address: int
    # write_count: float
    size: int
    heat_level: int = field(default=100)
    heat_trend: int = field(default=0)
    selected: int = field(default=0)

    @property
    def start(self) -> int:
        """
        返回页的起始地址
        """
        return self.address

    @property
    def end(self) -> int:
        """
        返回页的结束地址
        """
        return self.address + self.size

def write_heatmap_to_file(heatmap: List[DirtyHeatMapEntry], output_file: str):
    """
    将热图数据写入到文件中。

    Args:
        heatmap (List[DirtyHeatMapEntry]): 热图列表
        output_file (str): 输出文件路径
    """
    with open(output_file, 'wb') as f:
        for entry in heatmap:
            entry_packed = struct.pack('<QIBbB', entry.address, entry.size, entry.heat_level, entry.heat_trend, entry.selected)
            f.write(entry_packed)

def insert_entry_to_consolidated(consolidated: List[DirtyMapEntry], new_entry: DirtyMapEntry):
    """
    将新的 DirtyMapEntry 合并到 consolidated 列表中，处理与现有条目的重叠部分。
    
    Args:
        consolidated (List[DirtyMapEntry]): 已整合的脏页映射条目列表，按address升序排序。
        new_entry (DirtyMapEntry): 需要合并的新条目
    """
    # 如果 consolidated 为空，直接插入 new_entry
    if not consolidated:
        consolidated.append(new_entry)
        return

    # 使用二分查找快速定位 new_entry.address 在 consolidated 中的位置
    addresses = [entry.address for entry in consolidated]
    index = bisect.bisect_left(addresses, new_entry.address)

    if index < len(consolidated) and consolidated[index].address == new_entry.address:
        existing_entry = consolidated[index]
        if existing_entry.size == new_entry.size:
            # 大小一致，直接相加 write_count
            existing_entry.write_count += new_entry.write_count
        else:
            # 大小不一致，拆分较长的条目
            if existing_entry.size > new_entry.size:
                # 拆分 existing_entry
                # 1. 重叠部分
                overlap_entry = DirtyMapEntry(
                    address=new_entry.address,
                    write_count=existing_entry.write_count + new_entry.write_count,
                    size=new_entry.size
                )
                # 2. existing_entry 右侧不重叠部分
                right_size = existing_entry.size - new_entry.size
                right_entry = DirtyMapEntry(
                    address=new_entry.address + new_entry.size,
                    write_count=existing_entry.write_count,  # 根据需求决定是否需要调整
                    size=right_size
                )
                # 替换 existing_entry 为 overlap_entry 和 right_entry
                consolidated[index] = overlap_entry
                consolidated.insert(index + 1, right_entry)
            else:
                # 拆分 new_entry
                # 1. 重叠部分
                overlap_entry = DirtyMapEntry(
                    address=new_entry.address,
                    write_count=existing_entry.write_count + new_entry.write_count,
                    size=existing_entry.size
                )
                # 2. new_entry 右侧不重叠部分
                right_size = new_entry.size - existing_entry.size
                right_entry = DirtyMapEntry(
                    address=new_entry.address + existing_entry.size,
                    write_count=new_entry.write_count,  # 剩余的 write_count
                    size=right_size
                )
                # 替换 existing_entry 为 overlap_entry 并插入 right_entry
                consolidated[index] = overlap_entry
                consolidated.insert(index + 1, right_entry)
    else:
        # 找不到相同 address 的条目
        # 需要检查周边是否有重叠
        # 上一个条目
        if index > 0:
            prev_entry = consolidated[index - 1]
            prev_end = prev_entry.address + prev_entry.size
            if new_entry.address < prev_end:
                # 有重叠，需要拆分
                overlap_start = new_entry.address
                overlap_end = min(prev_end, new_entry.address + new_entry.size)
                overlap_size = overlap_end - overlap_start
                overlap_write_count = new_entry.write_count  # 根据需求决定如何计算

                # 创建重叠部分
                overlap_entry = DirtyMapEntry(
                    address=overlap_start,
                    write_count=overlap_write_count,
                    size=overlap_size
                )
                # 更新前一个条目的大小
                prev_entry.size = overlap_start - prev_entry.address
                # 插入重叠部分
                consolidated.insert(index, overlap_entry)

                # 处理 new_entry 右侧不重叠部分
                remaining_size = new_entry.size - overlap_size
                if remaining_size > 0:
                    remaining_entry = DirtyMapEntry(
                        address=overlap_end,
                        write_count=new_entry.write_count,  # 剩余的 write_count
                        size=remaining_size
                    )
                    bisect.insort(consolidated, remaining_entry, key=lambda x: x.address)
        # 下一个条目
        if index < len(consolidated):
            next_entry = consolidated[index]
            new_end = new_entry.address + new_entry.size
            next_start = next_entry.address
            if new_end > next_start:
                # 有重叠，需要拆分
                overlap_start = new_end - (new_end - next_start)
                overlap_size = new_end - next_start
                overlap_write_count = new_entry.write_count  # 根据需求决定如何计算

                # 创建重叠部分
                overlap_entry = DirtyMapEntry(
                    address=next_start,
                    write_count=overlap_write_count,
                    size=overlap_size
                )
                # 更新 next_entry 的地址和大小
                next_entry.address = overlap_start
                next_entry.size -= overlap_size
                # 插入重叠部分
                consolidated.insert(index, overlap_entry)

                # 处理 new_entry 覆盖部分
                bisect.insort(consolidated, overlap_entry, key=lambda x: x.address)

        # 最终插入 new_entry
        bisect.insort(consolidated, new_entry, key=lambda x: x.address)

def prehandle_dirtymap(dirty_map_path: str) -> List[Dict]:
    """
    预处理 dirty_map 文件夹中的 dirtymap 文件，返回包含每个 PID 最新 dirtymap 和整合后的旧 dirtymap 的列表。

    Args:
        dirty_map_path (str): dirty_map 文件夹的路径。

    Returns:
        List[Dict]: 包含各'pid'的'latest_dirtymap'和'consolidated_dirtymap'两个列表的字典。
    """
    dirtymap_pids: List[Dict] = []
    pid_files: Dict[int, List[Tuple[int, str]]] = {}

    # 遍历 dirty_map_path 目录下的所有文件，排除以 newest- 开头的 img 文件
    for filename in os.listdir(dirty_map_path):
        if filename.endswith('.img') and not filename.startswith('latest-') and not filename.startswith('old-'):
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
        latest_dirtymap = []
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
                        size=PAGE_SIZES[page_type]
                    )
                    latest_dirtymap.append(entry)
        except IOError as e:
            print(f"无法读取最新文件 {latest_file_path}，错误：{e}")
            continue

        if not older_files:
            print(f"PID：{pid}只有一个脏页映射文件")
            # consolidated_dirtymap_entries = latest_dirtymap_entries
            dirtymap_pids.append({
                'pid': pid,
                'latest_dirtymap': latest_dirtymap,
                'latest_sourcefile': latest_file_path,
                'old_dirtymap': latest_dirtymap,
                'old_sourcefiles': latest_file_path
            })
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
        consolidated_dirtymap = []

        for (timestamp, file_path), weight in zip(older_files, weight_factors):
            try:
                with open(file_path, 'rb') as f:
                    while True:
                        data = f.read(13)  # sizeof(dirty_page) = 8 + 8 + 4 = 20 bytes
                        if not data or len(data) < 13:
                            break
                        address, write_count, page_type = struct.unpack('<QIB', data)
                        weighted_write_count = write_count * weight
                        new_entry = DirtyMapEntry(
                            address=address,
                            write_count=weighted_write_count,
                            size=PAGE_SIZES[page_type]
                        )
                        insert_entry_to_consolidated(consolidated_dirtymap, new_entry)
            except IOError as e:
                print(f"无法读取文件 {file_path}，错误：{e}")
                continue

        # 转换 consolidated 字典为 DirtyMapEntry 列表，并设置 heat_level

        # consolidated_dirtymaps.append({
        #     'pid': pid,
        #     'dirtymap': consolidated_dirtymap_entries,
        #     'source_files': [fp for _, fp in older_files]
        # })
        print(f"PID：{pid} 的旧 dirtymap 已整合，包含 {len(consolidated_dirtymap)} 个条目")

        dirtymap_pids.append({
            'pid': pid,
            'latest_dirtymap': latest_dirtymap,
            'latest_sourcefile': latest_file_path,
            'old_dirtymap': consolidated_dirtymap,
            'old_sourcefiles': [fp for _, fp in older_files]
        })
    return dirtymap_pids

def detect_extreme_high_wc(write_counts: List[float]) -> float:
    """
    计算给定 dirtymap 中划分异常高 write_count 的阈值。
    Args: dirtymap (List[DirtyMapEntry]): DirtyMapEntry 列表。
    Returns: float: dirtymap 中异常高 write_count 的阈值。
    """
    if not write_counts:
        return 0
    
    # write_counts = sorted(entry.write_count for entry in dirtymap)
    n = len(write_counts)
    if n == 0:
        return 0  # 无数据
    elif n < 4096:
        # # 小规模脏内存(<=16MB)：使用四分位数方法
        # try:
        #     # 使用四分位数方法检测异常值
        #     q1 = statistics.quantiles(write_counts, n=4)[0]  # 第一四分位数
        #     q3 = statistics.quantiles(write_counts, n=4)[2]  # 第三四分位数
        #     iqr = q3 - q1
        #     threshold = q3 + 1.5 * iqr
        # except statistics.StatisticsError:
        #     threshold = max(write_counts) * 0.9
    # elif n < 32768:
        # 中等规模脏内存(<=128MB)：使用中位数和MAD
        try:
            median_wc = statistics.median(write_counts)
            mad = statistics.median([abs(wc - median_wc) for wc in write_counts])
            threshold = median_wc + 5 * mad  # 选择5倍MAD作为阈值
        except statistics.StatisticsError:
            median_wc = statistics.median(write_counts)
            threshold = max(write_counts) * 0.9
    else:
        # 大规模脏内存：使用99百分位数
        try:
            percentile_99 = statistics.quantiles(write_counts, n=100)[98]  # 95th 百分位
            threshold = percentile_99
        except statistics.StatisticsError:
            threshold = max(write_counts) * 0.9

    return threshold

def convert_dirtymap_to_heatmap(dirtymap: List[DirtyMapEntry]) -> List[DirtyHeatMapEntry]:
    """
    排除异常高 write_count 后将 dirtymap 转换为 heatmap。
    并确保地址连续，且相同 heat_level 的连续条目合并为一个条目。

    :param dirtymap: List[DirtyMapEntry]，DirtyMapEntry 实例的列表
    :return: List[DirtyHeatMapEntry]，处理后的 HeatMapEntry 列表
    """
    if not dirtymap:
        return []

    write_counts = [entry.write_count for entry in dirtymap]
    threshold = detect_extreme_high_wc(write_counts)
    print(f"阈值为: {threshold}")

    # 找出非异常高 write_count 的最大值，用于归一化
    non_extreme_wcs = [wc for wc in write_counts if wc < threshold]
    max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1  # 避免除零

    # 确保 dirtymap 按 address 升序排序
    sorted_dirtymap = sorted(dirtymap, key=lambda x: x.address)

    heatmap_entries = []
    for entry in sorted_dirtymap:
        write_count = entry.write_count
        # 判断是否为异常高 write_count
        if write_count >= threshold:
            heat_level = 100  # 最大 heat_level
        else:
            normalized_wc = write_count / max_write_count
            # 归一化后均分为10级
            heat_level = math.ceil(normalized_wc * 9) + 1  # 1到10
            heat_level = min(max(heat_level, 1), 10)  # 确保在范围内

        # 创建 HeatMapEntry 实例
        heatmap_entry = DirtyHeatMapEntry(
            address=entry.address,
            size=entry.size,
            heat_level=heat_level,
            heat_trend=0,
            selected=0
        )
        heatmap_entries.append(heatmap_entry)

    # 合并地址连续且 heat_level 相同的条目
    if not heatmap_entries:
        return []

    merged_heatmap = [heatmap_entries[0]]

    for current_entry in heatmap_entries[1:]:
        last_entry = merged_heatmap[-1]
        # 检查当前条目是否与最后一个合并条目连续且 heat_level 相同
        if (last_entry.address + last_entry.size == current_entry.address) and (last_entry.heat_level == current_entry.heat_level):
            # 合并条目：增加 size
            last_entry.size += current_entry.size
        else:
            # 不满足合并条件，直接添加到合并列表
            merged_heatmap.append(current_entry)

    return merged_heatmap

def subtract_heatmap_entries(
    A: List[DirtyHeatMapEntry],
    B: List[DirtyHeatMapEntry]
) -> List[DirtyHeatMapEntry]:
    """
    对两个 DirtyHeatMapEntry 列表 A 和 B 进行差集操作，计算 heat_trend。
    
    Args:
        A (List[DirtyHeatMapEntry]): 基准 HeatMap 列表
        B (List[DirtyHeatMapEntry]): 要比较的 HeatMap 列表
    
    Returns:
        List[DirtyHeatMapEntry]: 差集后的 HeatMap 列表，包含 heat_trend
    """
    # 定义事件类型及其优先级
    EVENT_PRIORITY = {
        'start_A': 0,
        'start_B': 1,
        'end_A': 2,
        'end_B': 3
    }
    
    # 收集所有事件
    # 每个事件是一个元组 (address, event_type, heat_level)
    events = []
    
    for entry in A:
        events.append((entry.address, 'start_A', entry.heat_level))
        events.append((entry.end, 'end_A', entry.heat_level))
    
    for entry in B:
        events.append((entry.address, 'start_B', entry.heat_level))
        events.append((entry.end, 'end_B', entry.heat_level))
    
    # 按地址排序，若地址相同，按照 EVENT_PRIORITY 优先级排序
    events.sort(key=lambda x: (x[0], EVENT_PRIORITY[x[1]]))
    
    result = []
    current_a_heat = 0
    current_b_heat = 0
    prev_address = None
    
    for addr, event_type, heat in events:
        if prev_address is not None and prev_address < addr:
            heat_trend = current_b_heat - current_a_heat
            if heat_trend != 0:
                new_heat_level = current_b_heat
                # 合并连续且 heat_trend 相同的子区间
                if (result and 
                    (result[-1].address + result[-1].size == prev_address) and 
                    (result[-1].heat_trend == heat_trend)):
                    result[-1].size += addr - prev_address
                else:
                    new_entry = DirtyHeatMapEntry(
                        address=prev_address,
                        size=addr - prev_address,
                        heat_level=new_heat_level,
                        heat_trend=heat_trend,
                        selected=0
                    )
                    result.append(new_entry)
        
        # 更新当前的 heat_level
        if event_type == 'start_A':
            current_a_heat += heat
        elif event_type == 'end_A':
            current_a_heat -= heat
        elif event_type == 'start_B':
            current_b_heat += heat
        elif event_type == 'end_B':
            current_b_heat -= heat
        
        prev_address = addr
    
    # 验证条目的有效性
    for entry in result:
        if entry.size <= 0 or entry.size > 4294967295:
            print(f"无效条目 - 地址: {hex(entry.address)}, 大小: {hex(entry.size)}, 热度趋势: {entry.heat_trend}")
    
    return result
# 示例调用
if __name__ == "__main__":
    dirty_map_path = os.path.abspath('./dirty_map')
    result = prehandle_dirtymap(dirty_map_path)
    
    # 打印部分结果以验证
    for pid_dirtymap in result:
        print(f"dirtymaps - PID: {pid_dirtymap['pid']}")
        pid = pid_dirtymap['pid']
        print(f"-- 最新dirtymap条目数: {len(pid_dirtymap['latest_dirtymap'])}")
        # print(f"latest{latest['pid']}异常高值阈值: {detect_extreme_high_wc(latest['dirtymap'])}")
        latest_heatmap = convert_dirtymap_to_heatmap(pid_dirtymap['latest_dirtymap'])
        print(f"-- 最新heatmap条目数: {len(latest_heatmap)}")

        # 确保输出目录存在
        os.makedirs(dirty_map_path, exist_ok=True)
        
        # 定义输出文件路径
        heatmap_old_file = os.path.join(dirty_map_path, f'old-{pid}.img')
        heatmap_latest_file = os.path.join(dirty_map_path, f'latest-{pid}.img')
        heatmap_file = os.path.join(dirty_map_path, f'latest-old-{pid}.img')

        # 写入热度图文件
        if pid_dirtymap['old_sourcefiles'] == pid_dirtymap['latest_sourcefile']:
            print(f"-- 整合旧dirtymap与最新dirtymap内容一致")
            write_heatmap_to_file(latest_heatmap, heatmap_old_file)
        else:
            old_heatmap = convert_dirtymap_to_heatmap(pid_dirtymap['old_dirtymap'])
            print(f"-- 整合旧heatmap条目数: {len(old_heatmap)}")
            write_heatmap_to_file(old_heatmap, heatmap_old_file)
            heatmap = subtract_heatmap_entries(old_heatmap, latest_heatmap)
            write_heatmap_to_file(heatmap, heatmap_file)
        write_heatmap_to_file(latest_heatmap, heatmap_latest_file)
