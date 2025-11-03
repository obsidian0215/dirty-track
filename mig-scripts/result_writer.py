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
    is_secure: Optional[bool] = None,
    extra_param_lines: Optional[list] = None,
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
        # If file is new, write experiment header + params + optional extra lines + header row
        if is_new:
            f.write(f"# experiment: {exp_name}\n")
            comment_parts = []
            if exp_params:
                comment_parts.append(exp_params)
            if is_secure is not None:
                comment_parts.append(f"secure={'yes' if is_secure else 'no'}")
            if comment_parts:
                f.write(f"# params: {' | '.join(comment_parts)}\n")
            if extra_param_lines:
                for line in extra_param_lines:
                    if line:
                        f.write(f"# params: {line}\n")
            f.write("\t".join(header_columns) + "\n")
            f.write("\t".join(row_values) + "\n")
            return

        # If file already exists, check whether an equivalent params block is present.
        # If not, append a params block (main params + extra lines) once before the row.
        need_params = True
        try:
            with open(fname, "r", encoding="utf-8") as fr:
                contents = fr.read()
                # Look for a params line that contains both exp_params and secure flag
                if exp_params:
                    if f"# params: {exp_params}" in contents:
                        # if secure provided, verify secure string present near it
                        if is_secure is None or f"secure={'yes' if is_secure else 'no'}" in contents:
                            need_params = False
                else:
                    # no exp_params passed; if any params line exists, assume present
                    if "# params:" in contents:
                        need_params = False
        except Exception:
            need_params = True

        if need_params:
            comment_parts = []
            if exp_params:
                comment_parts.append(exp_params)
            if is_secure is not None:
                comment_parts.append(f"secure={'yes' if is_secure else 'no'}")
            if comment_parts:
                f.write(f"# params: {' | '.join(comment_parts)}\n")
            if extra_param_lines:
                for line in extra_param_lines:
                    if line:
                        f.write(f"# params: {line}\n")

        # Finally append the row
        f.write("\t".join(row_values) + "\n")
