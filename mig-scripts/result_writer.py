import os
import re
from datetime import datetime


def _ensure_results_dir():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    results_dir = os.path.join(base, "results")
    os.makedirs(results_dir, exist_ok=True)
    return results_dir


def extract_stats_from_output(output_text: str):
    """从 source 脚本的 stdout 中提取最后一行带有 tab 分隔的统计值行。
    返回统计行字符串或 None。
    """
    stats_line = None
    if not output_text:
        return None
    for line in output_text.splitlines():
        if "\t" in line and re.search(r"\d", line):
            stats_line = line.strip()
    return stats_line


def append_result(exp_name: str, container: str, run_index: int, stats_line: str):
    results_dir = _ensure_results_dir()
    fname = os.path.join(results_dir, f"{exp_name}.tsv")
    is_new = not os.path.exists(fname)
    ts = datetime.utcnow().isoformat() + "Z"
    with open(fname, "a", encoding="utf-8") as f:
        if is_new:
            f.write("timestamp\tcontainer\trun\tstats\n")
        f.write(f"{ts}\t{container}\t{run_index}\t{stats_line}\n")
