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
