# mig-scripts

This folder contains orchestrator and benchmark scripts used for migration experiments.

## Default runs

Most orchestrator scripts in this folder accept a `--runs` command-line option that controls how many times each experiment is executed. The default has been changed to `5` to produce more stable experiment results. You can override the default at runtime, for example:

```bash
# run each experiment 3 times
python3 redis-ycsb.py --runs 3 --source-ip 10.0.0.1 --dest-ip 10.0.0.2
```

## Notes

- The `--sec` flag is available in many scripts to switch to secure transfer helpers (`source-sec.py`/`destination-sec.py`).
- Results are written via `result_writer` when orchestrators can extract stats lines from source output; see `result_writer.py` for details.
- If you need the project-wide README updated instead, tell me and I'll add a short note there as well.

## TSV -> CSV conversion tool

A small utility `tsv_to_csv.py` is included to safely convert TSV result files into CSV. Features:

- Accepts one or more input files or glob patterns (e.g. `results/*.tsv`).
- Preserves comment lines (lines starting with `#`) by copying them to the top of the CSV output.
- Uses Python's `csv` module to ensure fields containing commas/quotes/newlines are properly quoted.
- Options: `--out-dir` (write CSVs to a single directory), `--overwrite`, `--dry-run`, `--encoding`.

Examples:

```bash
# Convert all TSVs in results/ to CSVs next to the originals
python3 tsv_to_csv.py results/*.tsv

# Convert and place CSVs in csv_out/, overwriting existing outputs
python3 tsv_to_csv.py --out-dir csv_out --overwrite results/*.tsv

# Dry run (no files written)
python3 tsv_to_csv.py --dry-run results/*.tsv
```

The tool is conservative about tabs inside fields: it treats tabs as column separators and does not attempt to unescape them. If your `results/*.tsv` contains embedded tabs inside a logical field, consider post-processing those fields to remove or replace internal tabs before conversion.
# mig-scripts

This folder contains orchestrator and benchmark scripts used for migration experiments.

Default runs
------------
Most orchestrator scripts in this folder accept a `--runs` command-line option that controls how many times each experiment is executed. The default has been changed to `5` to produce more stable experiment results. You can override the default at runtime, for example:

```bash
# run each experiment 3 times
python3 redis-ycsb.py --runs 3 --source-ip 10.0.0.1 --dest-ip 10.0.0.2
```

Notes
-----
- The `--sec` flag is available in many scripts to switch to secure transfer helpers (`source-sec.py`/`destination-sec.py`).
- Results are written via `result_writer` when orchestrators can extract stats lines from source output; see `result_writer.py` for details.
- If you need the project-wide README updated instead, tell me and I'll add a short note there as well.

TSV -> CSV conversion tool
-------------------------

A small utility `tsv_to_csv.py` is included to safely convert TSV result files into CSV. Features:

- Accepts one or more input files or glob patterns (e.g. `results/*.tsv`).
- Preserves comment lines (lines starting with `#`) by copying them to the top of the CSV output.
- Uses Python's `csv` module to ensure fields containing commas/quotes/newlines are properly quoted.
- Options: `--out-dir` (write CSVs to a single directory), `--overwrite`, `--dry-run`, `--encoding`.

Examples:

```bash
# Convert all TSVs in results/ to CSVs next to the originals
python3 tsv_to_csv.py results/*.tsv

# Convert and place CSVs in csv_out/, overwriting existing outputs
python3 tsv_to_csv.py --out-dir csv_out --overwrite results/*.tsv

# Dry run (no files written)
python3 tsv_to_csv.py --dry-run results/*.tsv
```

The tool is conservative about tabs inside fields: it treats tabs as column separators and does not attempt to unescape them. If your `results/*.tsv` contains embedded tabs inside a logical field, consider post-processing those fields to remove or replace internal tabs before conversion.
