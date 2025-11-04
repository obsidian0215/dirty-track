#!/usr/bin/env python3
"""tsv_to_csv.py

Safely convert one or more TSV files to CSV.

Features:
- Accept multiple input paths or glob patterns.
- Preserve comment lines (starting with '#') by copying them to the top of the CSV file.
- Use Python csv module to properly quote fields with commas/quotes/newlines.
- By default write output files alongside inputs with `.csv` extension; support --out-dir and --overwrite.

Usage examples:
  python3 mig-scripts/tsv_to_csv.py results/*.tsv
  python3 mig-scripts/tsv_to_csv.py --out-dir csv_out results/*.tsv

"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from typing import List


def find_input_files(patterns: List[str]) -> List[str]:
    files = []
    for p in patterns:
        # If pattern contains wildcard, expand; otherwise treat as literal path
        if any(ch in p for ch in "*?[]"):
            files.extend(sorted(glob.glob(p)))
        else:
            files.append(p)
    # Remove duplicates while preserving order
    seen = set()
    out = []
    for f in files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def convert_file(inpath: str, outpath: str, encoding: str = "utf-8", comment_char: str = "#") -> None:
    comments = []
    data_lines = []

    with open(inpath, "r", encoding=encoding, errors="surrogateescape") as fr:
        for raw in fr:
            line = raw.rstrip("\n\r")
            if line.startswith(comment_char):
                comments.append(line)
            elif line.strip() == "":
                # skip purely empty lines
                continue
            else:
                data_lines.append(line)

    if not data_lines:
        # nothing to write except comments — write comments only
        with open(outpath, "w", encoding=encoding, newline="") as fo:
            for c in comments:
                fo.write(c + "\n")
        return

    # First non-comment line is treated as header
    header = data_lines[0].split("\t")
    rows = [row.split("\t") for row in data_lines[1:]]

    # Ensure output directory exists
    os.makedirs(os.path.dirname(outpath) or ".", exist_ok=True)

    # Write CSV with proper quoting
    with open(outpath, "w", encoding=encoding, newline="") as fo:
        # write comment lines at top (CSV readers that support comment can skip them)
        for c in comments:
            fo.write(c + "\n")

        writer = csv.writer(fo, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Batch-convert TSV files to CSV safely.")
    parser.add_argument("inputs", nargs="+", help="Input files or glob patterns (e.g. results/*.tsv)")
    parser.add_argument("--out-dir", help="Directory to write CSV files into (default: source file directory)")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output files if they exist")
    parser.add_argument("--dry-run", action="store_true", help="Show actions without writing files")
    parser.add_argument("--comment-char", default="#", help="Comment character to preserve (default: '#')")

    args = parser.parse_args(argv)

    inputs = find_input_files(args.inputs)
    if not inputs:
        print("No input files found.", file=sys.stderr)
        return 2

    for inp in inputs:
        if not os.path.isfile(inp):
            print(f"Skipping non-file: {inp}", file=sys.stderr)
            continue

        base = os.path.splitext(os.path.basename(inp))[0]
        if args.out_dir:
            outdir = args.out_dir
        else:
            outdir = os.path.dirname(inp) or "."
        outpath = os.path.join(outdir, base + ".csv")

        if os.path.exists(outpath) and not args.overwrite:
            print(f"Skipping existing file (use --overwrite to replace): {outpath}", file=sys.stderr)
            continue

        print(f"Converting: {inp} -> {outpath}")
        if args.dry_run:
            continue

        try:
            convert_file(inp, outpath, encoding=args.encoding, comment_char=args.comment_char)
        except Exception as e:
            print(f"Failed to convert {inp}: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
