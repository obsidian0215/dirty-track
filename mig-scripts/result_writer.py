import os
from datetime import datetime
from typing import Optional


def _ensure_results_dir():
    base = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    results_dir = os.path.join(base, "results")
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
        # We'll construct the desired params block (main line + extra lines) and compare
        # it against the last params block in the file. Only append if they differ.
        # However, if this call is the first loop in the current script run (first_in_run=True),
        # we explicitly write the params block once at the start of this run.
        need_params = True
        try:
            with open(fname, "r", encoding="utf-8") as fr:
                lines = fr.readlines()

            # Build desired params block as list of strings (without the leading marker)
            desired_block = []
            comment_parts = []
            if exp_params:
                comment_parts.append(exp_params)
            if is_secure is not None:
                comment_parts.append(f"secure={'yes' if is_secure else 'no'}")
            if comment_parts:
                desired_block.append(" | ".join(comment_parts))
            if extra_param_lines:
                for line in extra_param_lines:
                    if line:
                        desired_block.append(line.strip())

            # If file has no params at all, we need to write them (if we have any)
            params_indices = [i for i, ln in enumerate(lines) if ln.strip().startswith("# params:")]
            if not params_indices:
                # If there's nothing desired to write, mark no need
                need_params = bool(desired_block)
            else:
                # Find the start index of the last contiguous params block
                last_idx = params_indices[-1]
                start = last_idx
                # walk backwards to find the beginning of that block
                while start > 0 and lines[start - 1].strip().startswith("# params:"):
                    start -= 1

                existing_block = []
                i = start
                while i < len(lines) and lines[i].strip().startswith("# params:"):
                    existing_block.append(lines[i].strip()[len("# params:"):].strip())
                    i += 1

                # Compare desired block to existing block exactly (order matters)
                if desired_block and existing_block == desired_block:
                    # identical to last written block
                    need_params = False
                else:
                    # If no desired block (nothing to write), but file already has params, don't add
                    if not desired_block:
                        need_params = False
                    else:
                        need_params = True
            # If caller requested this to be the first call in the current run, force write
            if first_in_run:
                # Only write when we have something to write
                need_params = bool(desired_block)
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
