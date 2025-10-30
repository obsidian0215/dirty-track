import os
from datetime import datetime
from typing import Optional


def _ensure_results_dir():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    results_dir = os.path.join(base, "results")
    os.makedirs(results_dir, exist_ok=True)
    return results_dir


def extract_stats_from_output(output_text: str):
    """从 source 脚本 stdout 中提取结构化指标标题与数值行。"""
    if not output_text:
        return None, None

    header_line = None
    values_line = None
    for line in output_text.splitlines():
        if line.startswith("METRIC_HEADER\t"):
            header_line = line.split("\t", 1)[1].strip()
        elif line.startswith("METRIC_VALUES\t"):
            values_line = line.split("\t", 1)[1].strip()

    return header_line, values_line


def append_result(
    exp_name: str,
    container: str,
    run_index: int,
    stats_line: Optional[str],
    header_line: Optional[str] = None,
    exp_params: Optional[str] = None,
):
    results_dir = _ensure_results_dir()
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
            f.write(f"# experiment: {exp_name}\n")
        if exp_params:
            f.write(f"# params: {exp_params}\n")
        if is_new:
            f.write("\t".join(header_columns) + "\n")
        f.write("\t".join(row_values) + "\n")
