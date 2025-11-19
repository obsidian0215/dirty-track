"""Decode CRIU image files (pagemap.img, mm.img) using provided proto schemas.

The binary layout of CRIU image files is simple: after an 8-byte file header, the
stream is a sequence of entries encoded as `[uint32_le size][payload bytes...]`. The
payload itself is a protobuf message whose shape is described by the accompanying
proto files. We implement just enough protobuf decoding logic here (varints and
length-delimited nested messages) to extract the fields we care about without
needing `protoc` or the CRIU tooling.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple, TextIO, cast
import argparse
import bisect
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import Counter, defaultdict


CRIU_MAGIC = b"\x19CVT"  # Little-endian 0x54435619
PAGE_SIZE = 4096


class DecodeError(RuntimeError):
    """Raised when we fail to interpret the CRIU image stream."""


@dataclass
class PagemapHead:
    pages_id: int


@dataclass
class PagemapEntry:
    vaddr: int
    nr_pages: int
    in_parent: Optional[bool] = None
    flags: Optional[int] = None


@dataclass
class AioRingEntry:
    id: int
    nr_req: int
    ring_len: int


@dataclass
class VmaEntry:
    start: int
    end: int
    pgoff: int
    shmid: int
    prot: int
    flags: int
    status: int
    fd: int
    madv: Optional[int] = None
    fdflags: Optional[int] = None


@dataclass
class MmEntry:
    mm_start_code: int
    mm_end_code: int
    mm_start_data: int
    mm_end_data: int
    mm_start_stack: int
    mm_start_brk: int
    mm_brk: int
    mm_arg_start: int
    mm_arg_end: int
    mm_env_start: int
    mm_env_end: int
    exe_file_id: int
    mm_saved_auxv: List[int] = field(default_factory=list)
    vmas: List[VmaEntry] = field(default_factory=list)
    dumpable: Optional[int] = None
    aios: List[AioRingEntry] = field(default_factory=list)
    thp_disabled: Optional[bool] = None


@dataclass
class PagemapImage:
    head: PagemapHead
    entries: List[PagemapEntry]


@dataclass
class PageMetrics:
    digest: Optional[str]
    zero_ratio: float
    entropy: float
    is_zero: bool
    is_constant: bool
    has_data: bool


@dataclass
class PageHistory:
    first_iter: int
    last_iter: int
    writes: int


@dataclass
class VmaAggregate:
    pages: int = 0
    data_pages: int = 0
    missing_pages: int = 0
    zeros: int = 0
    modified: int = 0
    entropy_sum: float = 0.0
    zero_ratio_sum: float = 0.0


# --- Helper utilities ---------------------------------------------------------


def format_hex(value: int) -> str:
    return f"0x{value:016x}"


def maybe_hex(value: Optional[int]) -> Optional[str]:
    if value is None:
        return None
    return format_hex(value)


def compute_entropy(data: bytes) -> float:
    if not data:
        return 0.0
    length = len(data)
    counts = Counter(data)
    entropy = 0.0
    for count in counts.values():
        probability = count / length
        entropy -= probability * math.log2(probability)
    return entropy


def page_metrics_from_data(data: bytes) -> PageMetrics:
    digest = hashlib.sha1(data).hexdigest()
    zero_count = data.count(0)
    zero_ratio = zero_count / len(data) if data else 0.0
    unique_bytes = len(set(data))
    is_zero = zero_count == len(data)
    is_constant = unique_bytes == 1
    entropy = compute_entropy(data)
    return PageMetrics(
        digest=digest,
        zero_ratio=zero_ratio,
        entropy=entropy,
        is_zero=is_zero,
        is_constant=is_constant,
        has_data=True,
    )


def missing_page_metrics() -> PageMetrics:
    return PageMetrics(
        digest=None,
        zero_ratio=0.0,
        entropy=0.0,
        is_zero=False,
        is_constant=False,
        has_data=False,
    )


# --- Protobuf decoding helpers -------------------------------------------------

def read_varint(buf: bytes, offset: int) -> Tuple[int, int]:
    """Decode an unsigned varint starting at *offset* in *buf*."""
    shift = 0
    value = 0
    idx = offset
    while True:
        if idx >= len(buf):
            raise DecodeError("Unexpected end of buffer while parsing varint")
        byte = buf[idx]
        idx += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
        if shift >= 64:
            raise DecodeError("Varint exceeds 64 bits")
    return value, idx


def read_length_delimited(buf: bytes, offset: int) -> Tuple[bytes, int]:
    length, next_offset = read_varint(buf, offset)
    end = next_offset + length
    if end > len(buf):
        raise DecodeError("Length-delimited field extends past buffer end")
    return buf[next_offset:end], end


def decode_zigzag64(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def skip_field(buf: bytes, offset: int, wire_type: int) -> int:
    """Advance *offset* past an unknown protobuf field."""
    if wire_type == 0:  # varint
        _, offset = read_varint(buf, offset)
        return offset
    if wire_type == 1:  # 64-bit
        end = offset + 8
        if end > len(buf):
            raise DecodeError("Malformed fixed64 field")
        return end
    if wire_type == 2:  # length-delimited
        _, offset = read_length_delimited(buf, offset)
        return offset
    if wire_type == 5:  # 32-bit
        end = offset + 4
        if end > len(buf):
            raise DecodeError("Malformed fixed32 field")
        return end
    raise DecodeError(f"Unsupported wire type: {wire_type}")


# --- Message decoders ---------------------------------------------------------

def parse_pagemap_head(payload: bytes) -> PagemapHead:
    idx = 0
    pages_id: Optional[int] = None
    while idx < len(payload):
        tag, idx = read_varint(payload, idx)
        field_no = tag >> 3
        wire_type = tag & 0x7
        if field_no == 1 and wire_type == 0:
            pages_id, idx = read_varint(payload, idx)
        else:
            idx = skip_field(payload, idx, wire_type)
    if pages_id is None:
        raise DecodeError("pagemap_head missing pages_id field")
    return PagemapHead(pages_id=pages_id)


def parse_pagemap_entry(payload: bytes) -> PagemapEntry:
    idx = 0
    vaddr = nr_pages = None
    in_parent: Optional[bool] = None
    flags: Optional[int] = None
    while idx < len(payload):
        tag, idx = read_varint(payload, idx)
        field_no = tag >> 3
        wire_type = tag & 0x7
        if field_no == 1 and wire_type == 0:
            vaddr, idx = read_varint(payload, idx)
        elif field_no == 2 and wire_type == 0:
            nr_pages, idx = read_varint(payload, idx)
        elif field_no == 3 and wire_type == 0:
            value, idx = read_varint(payload, idx)
            in_parent = bool(value)
        elif field_no == 4 and wire_type == 0:
            flags, idx = read_varint(payload, idx)
        else:
            idx = skip_field(payload, idx, wire_type)
    if vaddr is None or nr_pages is None:
        raise DecodeError("pagemap_entry missing required fields")
    return PagemapEntry(vaddr=vaddr, nr_pages=nr_pages, in_parent=in_parent, flags=flags)


def parse_aio_ring_entry(payload: bytes) -> AioRingEntry:
    idx = 0
    entry_id = nr_req = ring_len = None
    while idx < len(payload):
        tag, idx = read_varint(payload, idx)
        field_no = tag >> 3
        wire_type = tag & 0x7
        if field_no == 1 and wire_type == 0:
            entry_id, idx = read_varint(payload, idx)
        elif field_no == 2 and wire_type == 0:
            nr_req, idx = read_varint(payload, idx)
        elif field_no == 3 and wire_type == 0:
            ring_len, idx = read_varint(payload, idx)
        else:
            idx = skip_field(payload, idx, wire_type)
    if entry_id is None or nr_req is None or ring_len is None:
        raise DecodeError("aio_ring_entry missing required fields")
    return AioRingEntry(id=entry_id, nr_req=nr_req, ring_len=ring_len)


def parse_vma_entry(payload: bytes) -> VmaEntry:
    idx = 0
    kwargs: Dict[str, Optional[int]] = {
        "start": None,
        "end": None,
        "pgoff": None,
        "shmid": None,
        "prot": None,
        "flags": None,
        "status": None,
        "fd": None,
        "madv": None,
        "fdflags": None,
    }
    while idx < len(payload):
        tag, idx = read_varint(payload, idx)
        field_no = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:
            value, idx = read_varint(payload, idx)
            if field_no == 8:  # sint64 fd uses zigzag encoding
                kwargs["fd"] = decode_zigzag64(value)
            elif field_no == 9:
                kwargs["madv"] = value
            elif field_no == 10:
                kwargs["fdflags"] = value
            elif field_no == 1:
                kwargs["start"] = value
            elif field_no == 2:
                kwargs["end"] = value
            elif field_no == 3:
                kwargs["pgoff"] = value
            elif field_no == 4:
                kwargs["shmid"] = value
            elif field_no == 5:
                kwargs["prot"] = value
            elif field_no == 6:
                kwargs["flags"] = value
            elif field_no == 7:
                kwargs["status"] = value
            else:
                # Unknown varint field, ignore
                pass
        else:
            idx = skip_field(payload, idx, wire_type)
    missing = [name for name in ("start", "end", "pgoff", "shmid", "prot", "flags", "status", "fd") if kwargs[name] is None]
    if missing:
        raise DecodeError(f"vma_entry missing fields: {missing}")
    return VmaEntry(**kwargs)  # type: ignore[arg-type]


def parse_mm_entry(payload: bytes) -> MmEntry:
    idx = 0
    numeric_fields: Dict[int, str] = {
        1: "mm_start_code",
        2: "mm_end_code",
        3: "mm_start_data",
        4: "mm_end_data",
        5: "mm_start_stack",
        6: "mm_start_brk",
        7: "mm_brk",
        8: "mm_arg_start",
        9: "mm_arg_end",
        10: "mm_env_start",
        11: "mm_env_end",
        12: "exe_file_id",
    }
    values: Dict[str, Optional[int]] = {name: None for name in numeric_fields.values()}
    dumpable: Optional[int] = None
    thp_disabled: Optional[bool] = None
    auxv: List[int] = []
    vmas: List[VmaEntry] = []
    aios: List[AioRingEntry] = []
    while idx < len(payload):
        tag, idx = read_varint(payload, idx)
        field_no = tag >> 3
        wire_type = tag & 0x7
        if wire_type == 0:
            value, idx = read_varint(payload, idx)
            if field_no == 13:
                auxv.append(value)
            elif field_no == 15:
                dumpable = value
            elif field_no == 17:
                thp_disabled = bool(value)
            else:
                name = numeric_fields.get(field_no)
                if name is not None:
                    values[name] = value
        elif wire_type == 2:
            blob, idx = read_length_delimited(payload, idx)
            if field_no == 14:
                vmas.append(parse_vma_entry(blob))
            elif field_no == 16:
                aios.append(parse_aio_ring_entry(blob))
        else:
            idx = skip_field(payload, idx, wire_type)
    missing = [name for name, val in values.items() if val is None]
    if missing:
        raise DecodeError(f"mm_entry missing fields: {missing}")
    resolved = {name: cast(int, value) for name, value in values.items()}
    entry = MmEntry(
        mm_start_code=resolved["mm_start_code"],
        mm_end_code=resolved["mm_end_code"],
        mm_start_data=resolved["mm_start_data"],
        mm_end_data=resolved["mm_end_data"],
        mm_start_stack=resolved["mm_start_stack"],
        mm_start_brk=resolved["mm_start_brk"],
        mm_brk=resolved["mm_brk"],
        mm_arg_start=resolved["mm_arg_start"],
        mm_arg_end=resolved["mm_arg_end"],
        mm_env_start=resolved["mm_env_start"],
        mm_env_end=resolved["mm_env_end"],
        exe_file_id=resolved["exe_file_id"],
        dumpable=dumpable,
        thp_disabled=thp_disabled,
    )
    entry.mm_saved_auxv.extend(auxv)
    entry.vmas.extend(vmas)
    entry.aios.extend(aios)
    return entry


class VmaIndex:
    def __init__(self, vmas: List[VmaEntry]):
        self._vmas = sorted(vmas, key=lambda v: v.start)
        self._starts = [v.start for v in self._vmas]

    def lookup(self, address: int) -> Optional[VmaEntry]:
        if not self._vmas:
            return None
        idx = bisect.bisect_right(self._starts, address) - 1
        if idx < 0:
            return None
        candidate = self._vmas[idx]
        if candidate.start <= address < candidate.end:
            return candidate
        return None

    def describe(self, address: int) -> str:
        vma = self.lookup(address)
        if vma is None:
            return "unknown"
        return (
            f"{format_hex(vma.start)}-{format_hex(vma.end)} "
            f"prot={vma.prot:#x} flags={vma.flags:#x} fd={vma.fd}"
        )


# --- Image readers -------------------------------------------------------------

def iter_criu_entries(path: Path) -> Iterator[bytes]:
    with path.open("rb") as fh:
        header = fh.read(8)
        if len(header) != 8:
            raise DecodeError("File shorter than CRIU header")
        if header[:4] != CRIU_MAGIC:
            raise DecodeError("Unexpected CRIU magic header")
        while True:
            size_raw = fh.read(4)
            if not size_raw:
                break
            if len(size_raw) != 4:
                raise DecodeError("Truncated entry size field")
            size = int.from_bytes(size_raw, "little")
            if size == 0:
                # Zero-sized sections occasionally show up as padding
                continue
            payload = fh.read(size)
            if len(payload) != size:
                raise DecodeError("Truncated entry payload")
            yield payload


def decode_pagemap(path: Path) -> PagemapImage:
    iterator = iter_criu_entries(path)
    try:
        head_payload = next(iterator)
    except StopIteration as exc:
        raise DecodeError("Empty pagemap image") from exc
    head = parse_pagemap_head(head_payload)
    entries = [parse_pagemap_entry(chunk) for chunk in iterator]
    return PagemapImage(head=head, entries=entries)


def decode_mm(path: Path) -> MmEntry:
    iterator = iter_criu_entries(path)
    payloads = list(iterator)
    if not payloads:
        raise DecodeError("Empty mm image")
    if len(payloads) > 1:
        raise DecodeError("Unexpected multiple mm entries")
    mm_entry = parse_mm_entry(payloads[0])
    return mm_entry


def stream_pagemap_pages(
    image: PagemapImage, pages_path: Path
) -> Iterator[Tuple[int, Optional[bytes], PagemapEntry]]:
    data_pages_expected = 0
    for entry in image.entries:
        flags = entry.flags if entry.flags is not None else 0
        if flags & 0x4 or entry.flags is None:
            data_pages_expected += entry.nr_pages
    truncated = False
    with pages_path.open("rb") as fh:
        data_pages_observed = 0
        for entry in image.entries:
            base_address = entry.vaddr
            flags = entry.flags if entry.flags is not None else 0
            has_data = (entry.flags is None) or (flags & 0x4) != 0
            for offset in range(entry.nr_pages):
                data: Optional[bytes]
                if has_data:
                    data = fh.read(PAGE_SIZE)
                    if len(data) != PAGE_SIZE:
                        truncated = True
                        data = None
                    else:
                        data_pages_observed += 1
                else:
                    data = None
                address = base_address + offset * PAGE_SIZE
                yield address, data, entry
        leftover = fh.read(1)
        if leftover and not truncated:
            raise DecodeError("pages.img contains extra data beyond pagemap entries")
        if not truncated and data_pages_observed != data_pages_expected:
            raise DecodeError(
                f"pages.img delivered {data_pages_observed} pages but pagemap expected {data_pages_expected}"
            )


def pagemap_entry_to_dict(entry: PagemapEntry) -> Dict[str, object]:
    start = entry.vaddr
    end = start + entry.nr_pages * PAGE_SIZE
    return {
        "start": format_hex(start),
        "end": format_hex(end),
        "pages": entry.nr_pages,
        "in_parent": entry.in_parent,
        "flags": entry.flags,
    }


def pagemap_to_dict(image: PagemapImage, limit: Optional[int] = None) -> Dict[str, object]:
    entries = image.entries
    if limit is not None and limit >= 0:
        entries = entries[:limit]
    return {
        "head": {"pages_id": image.head.pages_id},
        "entries": [pagemap_entry_to_dict(entry) for entry in entries],
    }


def write_pagemap_csv(entries: Iterable[PagemapEntry], dest: TextIO) -> None:
    fieldnames = ["start", "end", "pages", "in_parent", "flags"]
    writer = csv.DictWriter(dest, fieldnames=fieldnames)
    writer.writeheader()
    for entry in entries:
        start = entry.vaddr
        end = start + entry.nr_pages * PAGE_SIZE
        writer.writerow(
            {
                "start": format_hex(start),
                "end": format_hex(end),
                "pages": entry.nr_pages,
                "in_parent": "" if entry.in_parent is None else int(entry.in_parent),
                "flags": "" if entry.flags is None else entry.flags,
            }
        )


def mm_entry_to_dict(mm_entry: MmEntry) -> Dict[str, object]:
    result: Dict[str, object] = {
        "mm_start_code": format_hex(mm_entry.mm_start_code),
        "mm_end_code": format_hex(mm_entry.mm_end_code),
        "mm_start_data": format_hex(mm_entry.mm_start_data),
        "mm_end_data": format_hex(mm_entry.mm_end_data),
        "mm_start_stack": format_hex(mm_entry.mm_start_stack),
        "mm_start_brk": format_hex(mm_entry.mm_start_brk),
        "mm_brk": format_hex(mm_entry.mm_brk),
        "mm_arg_start": format_hex(mm_entry.mm_arg_start),
        "mm_arg_end": format_hex(mm_entry.mm_arg_end),
        "mm_env_start": format_hex(mm_entry.mm_env_start),
        "mm_env_end": format_hex(mm_entry.mm_env_end),
        "exe_file_id": mm_entry.exe_file_id,
        "mm_saved_auxv": [format_hex(value) for value in mm_entry.mm_saved_auxv],
        "dumpable": mm_entry.dumpable,
        "thp_disabled": mm_entry.thp_disabled,
    }

    vma_dicts: List[Dict[str, object]] = []
    for vma in mm_entry.vmas:
        vma_dicts.append(
            {
                "start": format_hex(vma.start),
                "end": format_hex(vma.end),
                "pgoff": format_hex(vma.pgoff),
                "shmid": vma.shmid,
                "prot": vma.prot,
                "flags": vma.flags,
                "status": vma.status,
                "fd": vma.fd,
                "madv": maybe_hex(vma.madv),
                "fdflags": maybe_hex(vma.fdflags),
            }
        )
    result["vmas"] = vma_dicts
    result["aios"] = [asdict(aio) for aio in mm_entry.aios]
    return result


def summarize_patterns(counter: Counter[str], meta: Dict[str, PageMetrics], top: int) -> List[Dict[str, object]]:
    if top >= 0:
        items = counter.most_common(top)
    else:
        items = counter.most_common()
    result: List[Dict[str, object]] = []
    for digest, count in items:
        metrics = meta.get(digest)
        entry = {
            "hash": digest,
            "count": count,
        }
        if metrics is not None:
            entry.update(
                {
                    "is_zero": metrics.is_zero,
                    "is_constant": metrics.is_constant,
                    "avg_zero_ratio": metrics.zero_ratio,
                    "entropy": metrics.entropy,
                }
            )
        result.append(entry)
    return result


def summarize_vma_counts(counter: Dict[str, VmaAggregate], top: int) -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []
    for description, agg in counter.items():
        entries.append(
            {
                "vma": description,
                "pages": agg.pages,
                "data_pages": agg.data_pages,
                "missing_pages": agg.missing_pages,
                "zeros": agg.zeros,
                "modified": agg.modified,
                "write_density": (agg.modified / agg.pages) if agg.pages else 0.0,
                "zero_ratio": (agg.zero_ratio_sum / agg.data_pages) if agg.data_pages else 0.0,
                "avg_entropy": (agg.entropy_sum / agg.data_pages) if agg.data_pages else 0.0,
            }
        )
    entries.sort(key=lambda item: cast(int, item["pages"]), reverse=True)
    return entries[:top] if top >= 0 else entries


def collect_iteration_stats(
    name: str,
    pagemap_image: PagemapImage,
    pages_path: Path,
    vma_index: VmaIndex,
    baseline: Dict[int, PageMetrics],
    *,
    iteration_idx: int,
    track_changes: bool,
    global_patterns: Optional[Counter[str]] = None,
    global_pattern_meta: Optional[Dict[str, PageMetrics]] = None,
    page_history: Optional[Dict[int, PageHistory]] = None,
) -> Dict[str, object]:
    total_pages = 0
    data_pages = 0
    missing_pages = 0
    zero_pages = 0
    constant_pages = 0
    entropies: List[float] = []
    zero_ratio_total = 0.0
    pattern_counter: Counter[str] = Counter()
    pattern_meta: Dict[str, PageMetrics] = {}
    vma_counter: Dict[str, VmaAggregate] = defaultdict(VmaAggregate)
    new_pages = 0
    modified_pages = 0
    unchanged_retransmits = 0

    for address, data, entry in stream_pagemap_pages(pagemap_image, pages_path):
        if data is None:
            metrics = missing_page_metrics()
            missing_pages += 1
        else:
            metrics = page_metrics_from_data(data)
            data_pages += 1
            zero_ratio_total += metrics.zero_ratio
            entropies.append(metrics.entropy)
            digest = cast(str, metrics.digest)
            pattern_counter[digest] += 1
            pattern_meta.setdefault(digest, metrics)
        total_pages += 1
        if metrics.is_zero:
            zero_pages += 1
        if metrics.is_constant and not metrics.is_zero:
            constant_pages += 1

        vma_description = vma_index.describe(address)
        bucket = vma_counter[vma_description]
        bucket.pages += 1
        if metrics.has_data:
            bucket.data_pages += 1
            bucket.entropy_sum += metrics.entropy
            bucket.zero_ratio_sum += metrics.zero_ratio
        else:
            bucket.missing_pages += 1
        if metrics.is_zero:
            bucket.zeros += 1

        if track_changes:
            previous = baseline.get(address)
            is_new_page = previous is None
            changed = False
            if metrics.has_data:
                if previous is None or not previous.has_data or previous.digest != metrics.digest:
                    changed = True
            else:
                changed = False

            if page_history is not None and is_new_page:
                page_history[address] = PageHistory(first_iter=iteration_idx, last_iter=iteration_idx, writes=0)

            if is_new_page:
                new_pages += 1

            if changed:
                modified_pages += 1
                bucket.modified += 1
                if page_history is not None and metrics.has_data:
                    hist = page_history.setdefault(
                        address, PageHistory(first_iter=iteration_idx, last_iter=iteration_idx, writes=0)
                    )
                    hist.last_iter = iteration_idx
                    hist.writes += 1
            elif metrics.has_data and previous is not None and previous.has_data:
                unchanged_retransmits += 1
            baseline[address] = metrics
        else:
            if page_history is not None and address not in page_history:
                page_history[address] = PageHistory(first_iter=iteration_idx, last_iter=iteration_idx, writes=0)
            baseline[address] = metrics

    if data_pages:
        avg_entropy = statistics.fmean(entropies) if entropies else 0.0
        min_entropy = min(entropies) if entropies else 0.0
        max_entropy = max(entropies) if entropies else 0.0
        avg_zero_ratio = zero_ratio_total / data_pages
    else:
        avg_entropy = 0.0
        min_entropy = 0.0
        max_entropy = 0.0
        avg_zero_ratio = 0.0

    unique_hashes = len(pattern_counter)
    duplicate_pages = data_pages - unique_hashes

    top_patterns: List[Dict[str, object]] = []
    for digest, count in pattern_counter.most_common(5):
        meta = pattern_meta[digest]
        top_patterns.append(
            {
                "hash": digest,
                "count": count,
                "is_zero": meta.is_zero,
                "is_constant": meta.is_constant,
                "avg_zero_ratio": meta.zero_ratio,
                "entropy": meta.entropy,
            }
        )
    iteration_stats: Dict[str, object] = {
        "iteration": name,
        "total_pages": total_pages,
        "data_pages": data_pages,
        "missing_pages": missing_pages,
        "zero_pages": zero_pages,
        "constant_pages": constant_pages,
        "avg_entropy": avg_entropy,
        "min_entropy": min_entropy,
        "max_entropy": max_entropy,
        "avg_zero_ratio": avg_zero_ratio,
        "unique_hashes": unique_hashes,
        "duplicate_pages": duplicate_pages,
        "top_patterns": top_patterns,
        "vma_hotspots": summarize_vma_counts(vma_counter, top=10),
    }

    if track_changes:
        iteration_stats.update(
            {
                "modified_pages": modified_pages,
                "new_pages": new_pages,
                "unchanged_retransmits": unchanged_retransmits,
            }
        )

    if global_patterns is not None:
        for digest, count in pattern_counter.items():
            global_patterns[digest] += count
            if global_pattern_meta is not None:
                global_pattern_meta.setdefault(digest, pattern_meta[digest])

    return iteration_stats


def analyze_precopy(root: Path, hot_limit: Optional[int] = None) -> Dict[str, object]:
    migrate_dir = root
    base_dir = migrate_dir / "image"
    pagemap_path = base_dir / "pagemap-1.img"
    pages_path = base_dir / "pages-1.img"
    mm_path = base_dir / "mm-1.img"

    for path in (base_dir, pagemap_path, pages_path, mm_path):
        if not path.exists():
            raise DecodeError(f"Required file for analysis not found: {path}")

    mm_entry = decode_mm(mm_path)
    vma_index = VmaIndex(mm_entry.vmas)
    baseline: Dict[int, PageMetrics] = {}
    page_history: Dict[int, PageHistory] = {}
    global_patterns: Counter[str] = Counter()
    global_pattern_meta: Dict[str, PageMetrics] = {}

    base_pagemap = decode_pagemap(pagemap_path)
    base_stats = collect_iteration_stats(
        "image",
        base_pagemap,
        pages_path,
        vma_index,
        baseline,
        iteration_idx=0,
        track_changes=False,
        global_patterns=global_patterns,
        global_pattern_meta=global_pattern_meta,
        page_history=page_history,
    )

    for address, metrics in baseline.items():
        if address not in page_history:
            page_history[address] = PageHistory(first_iter=0, last_iter=0, writes=0)

    iteration_stats: List[Dict[str, object]] = []
    parent_dirs = [p for p in migrate_dir.iterdir() if p.is_dir() and p.name.startswith("parent_")]
    parent_dirs.sort(key=lambda path: int(path.name.split("_")[1]))

    for idx, parent_dir in enumerate(parent_dirs, start=1):
        pagemap_file = parent_dir / "pagemap-1.img"
        pages_file = parent_dir / "pages-1.img"
        if not pagemap_file.exists() or not pages_file.exists():
            continue
        pagemap_image = decode_pagemap(pagemap_file)
        stats = collect_iteration_stats(
            parent_dir.name,
            pagemap_image,
            pages_file,
            vma_index,
            baseline,
            iteration_idx=idx,
            track_changes=True,
            global_patterns=global_patterns,
            global_pattern_meta=global_pattern_meta,
            page_history=page_history,
        )
        iteration_stats.append(stats)

    total_writes = sum(history.writes for history in page_history.values())
    pages_with_writes = [history for history in page_history.values() if history.writes > 0]
    lifespans = [history.last_iter - history.first_iter for history in pages_with_writes]
    history_summary = {
        "tracked_pages": len(page_history),
        "pages_with_writes": len(pages_with_writes),
        "pages_with_multiple_writes": sum(1 for h in pages_with_writes if h.writes > 1),
        "avg_writes_per_page": (total_writes / len(page_history)) if page_history else 0.0,
        "avg_writes_per_hot_page": (sum(h.writes for h in pages_with_writes) / len(pages_with_writes)) if pages_with_writes else 0.0,
        "max_writes": max((h.writes for h in pages_with_writes), default=0),
        "avg_lifespan": (sum(lifespans) / len(lifespans)) if lifespans else 0.0,
        "max_lifespan": max(lifespans, default=0),
    }

    hot_cap = hot_limit if hot_limit is not None and hot_limit >= 0 else 20
    hot_pages: List[Dict[str, object]] = []
    for address, history in sorted(
        page_history.items(), key=lambda item: item[1].writes, reverse=True
    ):
        if history.writes <= 0:
            continue
        hot_pages.append(
            {
                "address": format_hex(address),
                "writes": history.writes,
                "first_iter": history.first_iter,
                "last_iter": history.last_iter,
                "vma": vma_index.describe(address),
            }
        )
        if len(hot_pages) >= hot_cap:
            break

    pattern_summary = {
        "total_unique_patterns": len(global_patterns),
        "top_patterns": summarize_patterns(global_patterns, global_pattern_meta, top=10),
    }

    result = {
        "page_size": PAGE_SIZE,
        "total_tracked_pages": len(baseline),
        "total_writes": total_writes,
        "base": base_stats,
        "iterations": iteration_stats,
        "hot_pages": hot_pages,
        "page_history": history_summary,
        "pattern_summary": pattern_summary,
        "mm": mm_entry_to_dict(mm_entry),
    }
    return result


# --- CLI ----------------------------------------------------------------------

def main(argv: Optional[Iterable[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Decode CRIU pagemap.img or mm.img without CRIU tooling")
    parser.add_argument("kind", choices=["pagemap", "mm", "analyze"], help="Select the operation to perform")
    parser.add_argument("image", type=Path, help="Path to the target file or migrate directory")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="For pagemap: maximum entries to include; for analyze: number of hottest pages",
    )
    parser.add_argument("--indent", type=int, default=2, help="JSON indentation level")
    parser.add_argument("--format", choices=["json", "csv"], default="json", help="Output format")
    parser.add_argument("--output", type=Path, default=None, help="Optional output file path")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.image.exists():
        raise SystemExit(f"Image not found: {args.image}")

    if args.kind == "pagemap":
        pagemap_image = decode_pagemap(args.image)
        limited_entries = pagemap_image.entries
        if args.limit is not None and args.limit >= 0:
            limited_entries = limited_entries[: args.limit]
        if args.format == "csv":
            if args.output:
                with args.output.open("w", newline="") as fh:
                    write_pagemap_csv(limited_entries, fh)
            else:
                write_pagemap_csv(limited_entries, sys.stdout)
            return
        payload = pagemap_to_dict(pagemap_image, limit=args.limit)
    elif args.kind == "mm":
        if args.format == "csv":
            raise SystemExit("mm image decoding only supports JSON output")
        mm_entry = decode_mm(args.image)
        payload = {"mm": mm_entry_to_dict(mm_entry)}
    else:  # analyze
        if args.format == "csv":
            raise SystemExit("analysis output is available in JSON only")
        hot_limit = args.limit
        payload = analyze_precopy(args.image, hot_limit=hot_limit)

    rendered = json.dumps(payload, indent=args.indent)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
