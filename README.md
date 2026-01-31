# dirty-track

This repository contains experiments, kernel modules, and orchestrator scripts for tracking "dirty" pages and evaluating migration strategies.

See the `mig-scripts/README.md` for orchestrator usage and benchmarks. A few highlights:

- Orchestrators and benchmark scripts live under `mig-scripts/`.
- Results are written to the `results/` directory by `mig-scripts/result_writer.py` in CSV format.
- A helper to convert legacy TSV files to CSV is provided: `mig-scripts/tsv_to_csv.py`.

Quick note for legacy TSV -> CSV conversion
----------------------------------

To convert legacy TSV result files into CSV files next to the originals:

```bash
python3 mig-scripts/tsv_to_csv.py results/*.tsv
```

To convert and place CSV outputs under a single directory (creating it if needed):

```bash
python3 mig-scripts/tsv_to_csv.py --out-dir csv_out --overwrite results/*.tsv
```

For more details and examples, see `mig-scripts/README.md`.
