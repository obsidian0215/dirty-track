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
    uint8_t page_type;   //size = 13
    # uint32_t size;  // size = 16
}
"""
@dataclass
class DirtyMapEntry:
    address: int
    write_count: float
    size: int

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
    heat_level: int = field(default=10)
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
    写入heatmap文件

    Args:
        heatmap (List[DirtyHeatMapEntry]): 假定地址已升序的heatmap
        output_file (str): 输出文件路径
    """
    
    # 合并地址连续且 heat_level 相同的条目
    if not heatmap:
        return

    # 确保heatmap按地址升序排序
    # heatmap = sorted(heatmap, key=lambda x: x.address)

    merged_heatmap = [heatmap[0]]

    for current_entry in heatmap[1:]:
        last_entry = merged_heatmap[-1]
        # 检查当前条目是否与最后一个合并条目连续且 heat_level 相同
        if (last_entry.address + last_entry.size == current_entry.address) and (last_entry.heat_level == current_entry.heat_level):
            # 合并条目：增加 size
            last_entry.size += current_entry.size
        else:
            # 不满足合并条件，直接添加到合并列表
            merged_heatmap.append(current_entry)

    with open(output_file, 'wb') as f:
        for entry in merged_heatmap:
            entry_packed = struct.pack('<QIBbB', entry.address, entry.size, entry.heat_level, entry.heat_trend, entry.selected)
            f.write(entry_packed)

def write_dirty_map_to_file(dirty_map: List[DirtyMapEntry], output_file: str):
    """
    写入dirtymap文件

    Args:
        dirty_map (List[DirtyMapEntry]): dirtymap
        output_file (str): 输出文件路径
    """
    with open(output_file, 'wb') as f:
        for entry in dirty_map:
            if entry.size in PAGE_SIZES:
                # 如果大小是4KB或2MB，按原方式处理
                page_type = PAGE_SIZES.index(entry.size)
                write_count = max(int(entry.write_count), 1)  # 确保不会出现0
                entry_packed = struct.pack('<QIB', entry.address, write_count, page_type)
                f.write(entry_packed)
            else:
                # 如果大小不是4KB或2MB，需要拆分为多个4KB条目
                num_4kb_pages = entry.size // PAGE_SIZES[0]
                remaining_size = entry.size % PAGE_SIZES[0]

                base_address = entry.address
                for _ in range(num_4kb_pages):
                    # 每个4KB条目
                    split_entry_packed = struct.pack('<QIB', base_address, max(int(entry.write_count), 1), 0)
                    f.write(split_entry_packed)
                    base_address += PAGE_SIZES[0]

                if remaining_size > 0:
                    # 处理最后剩余的部分，如果有的话，按照4KB对齐
                    split_entry_packed = struct.pack('<QIB', base_address, max(int(entry.write_count), 1), 0)
                    f.write(split_entry_packed)

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
    if index < len(consolidated) and consolidated[index].address == new_entry.address and consolidated[index].size == new_entry.size:
        # 完全重叠，直接相加 write_count
        consolidated[index].write_count += new_entry.write_count
    elif index < len(consolidated) and consolidated[index].address == new_entry.address and consolidated[index].size != new_entry.size:
        # 不会发生，因为所有条目都是4KB，大小恒定
        pass
    else:
        # 无重叠，直接插入
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

    # 遍历dirty_map_path目录下的所有.dirtymap和.heatmap文件
    for filename in os.listdir(dirty_map_path):
        if not filename.endswith('.dirtymap') and not filename.endswith('.heatmap'):
            print(f"{filename} 不是 dirtymap 或 heatmap 文件，跳过")
            continue
        filepath = os.path.join(dirty_map_path, filename)

        if filename.startswith('latest-') and filename.endswith('.heatmap'):
            # 处理最新的heatmap文件，格式：latest-<pid>.heatmap
            parts = filename.split('-')
            if len(parts) < 2:
                continue
            try:
                pid = int(parts[0])
                timestamp = int(parts[1].split('.')[0])
            except ValueError:
                print(f"{filename} 的 PID 或时间戳无效，跳过")
                continue
            pid_files.setdefault(pid, {'old': None, 'latest': None, 'dirtymaps': []})
            pid_files[pid]['latest'] = filepath
        elif filename.startswith('old-') and filename.endswith('.dirtymap'):
            # 处理old dirtymap，格式：old-<pid>.dirtymap
            parts = filename.split('-')
            if len(parts) < 2:
                continue
            try:
                pid = int(parts[1].split('.')[0])
            except ValueError:
                print(f"{filename}的PID无效，跳过")
                continue
            pid_files.setdefault(pid, {'old': None, 'latest': None, 'dirtymaps': []})
            pid_files[pid]['old'] = filepath
        else:
            # 处理其他timestamp文件，格式：<pid>-<timestamp>.dirtymap
            if not filename.endswith('.dirtymap'):
                print(f"{filename} 不是有效的 dirtymap 文件，跳过")
                continue
            parts = filename.split('-')
            if len(parts) < 2:
                continue
            try:
                pid = int(parts[0])
                timestamp = int(parts[1].split('.')[0])
            except ValueError:
                print(f"{filename} 的PID或时间戳无效，跳过")
                continue
            pid_files.setdefault(pid, {'old': None, 'latest': None, 'dirtymaps': []})
            pid_files[pid]['dirtymaps'].append((timestamp, filepath))

   # 对每个 PID 的文件进行整合
    for pid, files in pid_files.items():
        dirtymaps = files['dirtymaps']
        if not dirtymaps:
            continue

        # 按时间戳排序（升序）
        sorted_dirtymaps = sorted(dirtymaps, key=lambda x: x[0])
        n = len(sorted_dirtymaps)

        latest_timestamp, latest_file_path = sorted_dirtymaps[-1]
        older_files = sorted_dirtymaps[:-1]

        # 读取最新dirtymap
        latest_dirtymap = []
        try:
            with open(latest_file_path, 'rb') as f:
                while True:
                    data = f.read(13)  # sizeof(dirty_page) = 8 + 4 + 1 = 13 bytes
                    if not data or len(data) < 13:
                        break
                    address, write_count, page_type = struct.unpack('<QIB', data)
                    # 拆分为4KB条目
                    num_pages = PAGE_SIZES[page_type] >> 12
                    for _ in range(num_pages):
                        new_entry = DirtyMapEntry(
                            address=address,
                            write_count=write_count,
                            size=1 << 12
                        )
                        latest_dirtymap.append(new_entry)
                        address += 1 << 12
        except IOError as e:
            print(f"无法读取最新dirtymap {latest_file_path}，错误：{e}")
            continue

        if not older_files and not files['old']:
            print(f"PID：{pid} 只有一个dirtymap文件")
            dirtymap_pids.append({
                'pid': pid,
                'latest_dirtymap': latest_dirtymap,
                'old_dirtymap': None,
                'transfered': None
            })
            continue

        # 处理 old dirtymap
        if files['old']:
            # 读取 existing old dirtymap
            consolidated_dirtymap = []
            try:
                with open(files['old'], 'rb') as f:
                    while True:
                        data = f.read(13)
                        if not data or len(data) < 13:
                            break
                        address, write_count, page_type = struct.unpack('<QIB', data)
                        # 拆分为4KB条目
                        num_pages = PAGE_SIZES[page_type] >> 12
                        for _ in range(num_pages):
                            new_entry = DirtyMapEntry(
                                address=address,
                                write_count=0.5 * float(write_count),
                                size=1 << 12
                            )
                            insert_entry_to_consolidated(consolidated_dirtymap, new_entry)
                            address += 1 << 12
            except IOError as e:
                print(f"无法读取old dirtymap {files['old']}，错误：{e}")
                consolidated_dirtymap = []

            if older_files:
                # 获取次最新的dirtymap（即 sorted_dirtymaps[-2]）
                latest_old_timestamp, latest_old_file_path = sorted_dirtymaps[-2]
                new_dirtymap = []
                try:
                    with open(latest_old_file_path, 'rb') as f:
                        while True:
                            data = f.read(13)
                            if not data or len(data) < 13:
                                break
                            address, write_count, page_type = struct.unpack('<QIB', data)
                            # 拆分为4KB条目
                            num_pages = PAGE_SIZES[page_type] >> 12
                            for _ in range(num_pages):
                                new_entry = DirtyMapEntry(
                                    address=address,
                                    write_count=0.5 * float(write_count),
                                    size=1 << 12
                                )
                                new_dirtymap.append(new_entry)
                                address += 1 << 12
                except IOError as e:
                    print(f"无法读取文件 {latest_old_file_path}，错误：{e}")
                    new_dirtymap = []

                # 将已有的old dirtymap与次最新的dirtymap相加
                for entry in new_dirtymap:
                    insert_entry_to_consolidated(consolidated_dirtymap, entry)

            print(f"PID：{pid} 的旧 dirtymap 已整合，包含 {len(consolidated_dirtymap)} 个条目")

            # 保存更新后的old dirtymap
            dirtymap_pid = {
                'pid': pid,
                'latest_dirtymap': latest_dirtymap,
                'old_dirtymap': consolidated_dirtymap
            }
        else:
            # 没有old dirtymap，加权合并除最新以外的所有dirtymap
            consolidated_dirtymap = []
            for i, (timestamp, file_path) in enumerate(older_files):
                weight = 1 / (2 ** (n - 1 - i))
                try:
                    with open(file_path, 'rb') as f:
                        while True:
                            data = f.read(13)
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
                    print(f"无法读取dirtymap {file_path}，错误：{e}")
                    continue

            print(f"PID：{pid} 的旧dirtymap已整合，包含{len(consolidated_dirtymap)}个条目")

            # 保存新的old dirtymap
            dirtymap_pid = {
                'pid': pid,
                'latest_dirtymap': latest_dirtymap,
                'old_dirtymap': consolidated_dirtymap,
                'transfered': None
            }
        
        # 获取各页的被转储情况
        if files['latest']:   
            transfered = []
            try:
                with open(files['latest'], 'rb') as f:
                    while True:
                        data = f.read(15)  # sizeof(dirty_page) = 8 + 4 + 1 + 1 + 1 = 13 bytes
                        if not data or len(data) < 15:
                            break
                        address, size, heat_level, heat_trend, selected = struct.unpack('<QIBbB', data)
                        if selected > 0:
                            transfered.append({'address': address, 'selected': selected})

            except IOError as e:
                print(f"无法读取上次的heatmap {files['latest']}，错误：{e}")
                continue
            dirtymap_pid['transfered'] = transfered
        
        dirtymap_pids.append(dirtymap_pid)

    return dirtymap_pids

def detect_extreme_high_wc(dirtymap: List[DirtyMapEntry]) -> float:
    """
    计算给定 dirtymap 中划分异常高 write_count 的阈值。
    Args: dirtymap (List[DirtyMapEntry]): DirtyMapEntry 列表。
    Returns: float: dirtymap 中异常高 write_count 的阈值。
    """
    if not dirtymap:
        return 0
    
    # write_counts = sorted(entry.write_count for entry in dirtymap)
    n = len(dirtymap)
    write_counts = [entry.write_count for entry in dirtymap]
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
    排除异常高 write_count 后将dirtymap转换为heatmap(并默认按地址升序)

    :param dirtymap: List[DirtyMapEntry]，DirtyMapEntry 实例的列表
    :return: List[DirtyHeatMapEntry]，处理后的 HeatMapEntry 列表
    """
    if not dirtymap:
        return []

    threshold = detect_extreme_high_wc(dirtymap)
    # print(f"阈值为: {threshold}")

    # 找出非异常高 write_count 的最大值，用于归一化
    non_extreme_wcs = [wc.write_count for wc in dirtymap if wc.write_count < threshold]
    max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1  # 避免除零

    # 确保 dirtymap 按 address 升序排序
    sorted_dirtymap = sorted(dirtymap, key=lambda x: x.address)

    heatmap_entries = []
    for entry in sorted_dirtymap:
        write_count = entry.write_count
        # 判断是否为异常高 write_count
        if write_count >= threshold:
            heat_level = 10  # 最大 heat_level
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
    return heatmap_entries

def merge_sub_heat(
    A: List[DirtyMapEntry],
    B: List[DirtyHeatMapEntry]
) -> List[DirtyHeatMapEntry]:
    """
    将DirtyMapEntry列表 A和DirtyHeatMapEntry列表 B取并集
    并根据B中各地址范围的heat_level与A中相同地址范围的heat_level的差值更新heat_trend

    Args:
        A (List[DirtyMapEntry]): 基准DirtyMap
        B (List[DirtyHeatMapEntry]): 目标HeatMap

    Returns:
        List[DirtyHeatMapEntry]: 合并并更新了heat_trend的HeatMap
    """
    PAGE_SIZE = 1 << 12  # 4KB

    # 收集所有4KB对齐的地址
    addresses = set()

    for entry in A:
        for offset in range(0, entry.size, PAGE_SIZE):
            addr = entry.start + offset
            addresses.add(addr)

    for entry in B:
        for offset in range(0, entry.size, PAGE_SIZE):
            addr = entry.start + offset
            addresses.add(addr)

    sorted_addresses = sorted(addresses)

    result = []

    for addr in sorted_addresses:
        # 查找 A 和 B 中覆盖当前地址的条目
        a_entry = next((e for e in A if e.start <= addr < e.end), None)
        b_entry = next((e for e in B if e.start <= addr < e.end), None)

        # 计算 A 的 heat_level
        if a_entry:
            threshold = detect_extreme_high_wc([a_entry])  # 单个条目列表
            write_count = a_entry.write_count
            if write_count >= threshold:
                a_heat_level = 10
            else:
                non_extreme_wcs = [wc.write_count for wc in [a_entry] if wc.write_count < threshold]
                max_write_count = max(non_extreme_wcs) if non_extreme_wcs else 1
                normalized_wc = write_count / max_write_count
                a_heat_level = math.ceil(normalized_wc * 9) + 1
                a_heat_level = min(max(a_heat_level, 1), 10)
        else:
            a_heat_level = 0  # 没有对应的 A 条目

        # 获取 B 的 heat_level
        b_heat_level = b_entry.heat_level if b_entry else 0

        # 计算 heat_trend
        heat_trend = b_heat_level - a_heat_level

        # 合并逻辑：仅在地址连续且 heat_trend 相同时进行合并
        if result and (result[-1].address + result[-1].size == addr) and (result[-1].heat_trend == heat_trend):
            # print("合并与前一个条目")
            result[-1].size += PAGE_SIZE
        else:
            # print("添加新的条目到结果")
            new_entry = DirtyHeatMapEntry(
                address=addr,
                size=PAGE_SIZE,
                heat_level=b_heat_level,
                heat_trend=heat_trend,
                selected=0
            )
            result.append(new_entry)

    # # 打印最终结果
    # print("最终结果列表:")
    # for entry in result:
    #     print(entry)

    return result

def merge_update_selected(
    heatmap: List[DirtyHeatMapEntry],
    transfered: List[Dict[str, int]]
) -> None:
    """
    使用二分查找优化查找速度

    Args:
        heatmap (List[DirtyHeatMapEntry]): HeatMapEntry列表，已按地址排序
        transfered (List[Dict[str, int]]): {'address': int, 'selected': int}列表
    """
    heatmap_addresses = [entry.address for entry in heatmap]
    for item in transfered:
        addr = item['address']
        selected = item['selected']
        index = bisect.bisect_left(heatmap_addresses, addr)
        if index < len(heatmap_addresses) and heatmap_addresses[index] == addr:
            heatmap[index].selected = selected
        else:
            print(f"警告: transfered 中的地址 {hex(addr)} 未在 heatmap 中找到")

def generate_heatmap(dirty_map_path: str) -> None:
    # 确保输出目录存在
    os.makedirs(dirty_map_path, exist_ok=True)
    dirtymap_pids = prehandle_dirtymap(dirty_map_path)

    for pid_dirtymap in dirtymap_pids:
        # print(f"dirtymaps - PID: {pid_dirtymap['pid']}")
        pid = pid_dirtymap['pid']
        # print(f"-- 最新dirtymap条目数: {len(pid_dirtymap['latest_dirtymap'])}")
        latest_heatmap = convert_dirtymap_to_heatmap(pid_dirtymap['latest_dirtymap'])
        
        # 使用old dirtymap计算最新heatmap的热度变化
        old_dirtymap = pid_dirtymap['old_dirtymap']
        if old_dirtymap is not None:
            dirtymap_old_file = os.path.join(dirty_map_path, f'old-{pid}.dirtymap')
            write_dirty_map_to_file(old_dirtymap, dirtymap_old_file)    # 保存新的old dirtymap
            latest_heatmap = merge_sub_heat(old_dirtymap, latest_heatmap)
        
        # 将脏页的转储情况更新到heatmap中
        selected = pid_dirtymap['transfered']
        if selected is not None:
            merge_update_selected(latest_heatmap, selected)
        
        # 保存更新的heatmap
        heatmap_latest_file = os.path.join(dirty_map_path, f'latest-{pid}.heatmap')
        write_heatmap_to_file(latest_heatmap, heatmap_latest_file)

if __name__ == "__main__":
    dirty_map_path = os.path.abspath('./dirty_map')
    # print(dirty_map_path)

    generate_heatmap(dirty_map_path)