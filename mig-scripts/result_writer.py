import os
from datetime import datetime
from typing import Optional


def _ensure_results_dir(results_dir: Optional[str] = None):
    if results_dir:
        results_dir = os.path.abspath(results_dir)
        os.makedirs(results_dir, exist_ok=True)
        return results_dir
    results_dir = "/runc/results"
    os.makedirs(results_dir, exist_ok=True)
    return results_dir


def extract_stats_from_output(output_text: str):
    """从 source 脚本 stdout 中提取结构化指标标题与数值行。

    额外返回解析到的参数行（METRIC_PARAM\tkey\tvalue -> "key: value"），以便汇总写入 results 的 params 区块。
    返回 (header_line, values_line, params_list)
    """
    if not output_text:
        return None, None, []

    header_line = None
    values_line = None
    params = []
    for line in output_text.splitlines():
        if line.startswith("METRIC_HEADER\t"):
            header_line = line.split("\t", 1)[1].strip()
        elif line.startswith("METRIC_VALUES\t"):
            values_line = line.split("\t", 1)[1].strip()
        elif line.startswith("METRIC_PARAM\t"):
            parts = line.split("\t")
            # METRIC_PARAM\tkey\tvalue (value may contain tabs)
            if len(parts) >= 3:
                key = parts[1]
                val = "\t".join(parts[2:]).strip()
                params.append(f"{key}: {val}")
            elif len(parts) == 2:
                params.append(parts[1].strip())

    return header_line, values_line, params


def append_result(
    exp_name: str,
    container: str,
    run_index: int,
    stats_line: Optional[str],
    header_line: Optional[str] = None,
    exp_params: Optional[str] = None,
    is_secure: Optional[bool] = None,
    extra_param_lines: Optional[list] = None,
    first_in_run: bool = False,
    results_dir: Optional[str] = None,
):
    results_dir = _ensure_results_dir(results_dir)
    fname = os.path.join(results_dir, f"{exp_name}.tsv")
    is_new = not os.path.exists(fname)
    ts = datetime.utcnow().isoformat() + "Z"
    header_columns = ["timestamp", "container", "run"]

    if header_line:
        header_columns.extend(header_line.split("\t"))
    else:
        header_columns.append("stats")

    row_values = [ts, container, str(run_index)]
    if stats_line:
        row_values.extend(stats_line.split("\t"))
    else:
        row_values.append("")

    with open(fname, "a", encoding="utf-8") as f:
        if is_new:
            f.write("\t".join(header_columns) + "\n")
        f.write("\t".join(row_values) + "\n")


def summarize_results(exp_name: str, out_suffix: str = "_summary.tsv"):
    """Read <exp_name>.tsv and produce a summary table (mean/std/min/max/count) for numeric columns.
    Summary is written to results/<exp_name>_summary.tsv and returned as a dict.
    """
    import csv
    import statistics

    results_dir = _ensure_results_dir()
    fname = os.path.join(results_dir, f"{exp_name}.tsv")
    if not os.path.exists(fname):
        return None

    # Read file, skip comment lines
    rows = []
    header = None
    with open(fname, "r", encoding="utf-8") as fr:
        for line in fr:
            if line.startswith("#"):
                continue
            line = line.rstrip("\n")
            if header is None:
                header = line.split("\t")
                continue
            parts = line.split("\t")
            rows.append(parts)

    if not header or not rows:
        return None

    # columns after the first three (timestamp, container, run) are numeric stats
    numeric_cols = list(range(3, len(header)))
    metrics = {}
    for col in numeric_cols:
        col_values = []
        for r in rows:
            if col < len(r):
                v = r[col].strip()
                try:
                    val = float(v)
                    col_values.append(val)
                except Exception:
                    # skip non-numeric
                    pass
        if not col_values:
            continue
        metrics[header[col]] = {
            "mean": statistics.mean(col_values),
            "stdev": statistics.stdev(col_values) if len(col_values) > 1 else 0.0,
            "min": min(col_values),
            "max": max(col_values),
            "count": len(col_values),
        }

    # write summary
    out_path = os.path.join(results_dir, f"{exp_name}{out_suffix}")
    with open(out_path, "w", encoding="utf-8") as fo:
        fo.write("metric\tmean\tstdev\tmin\tmax\tcount\n")
        for metric, stats in metrics.items():
            fo.write(
                f"{metric}\t{stats['mean']:.6f}\t{stats['stdev']:.6f}\t{stats['min']:.6f}\t{stats['max']:.6f}\t{stats['count']}\n"
            )

    return metrics
