GZIP bench and datasets

- Generate payloads (host):
  python3 datasets/generate_payloads.py --outdir ./payloads --count 500 --size 65536

- Start server (in container):
  runc exec defog-gzip sh -c '/root/scripts/execute.sh --server'
  (or run ./fog_workloads/GZIP/run.sh --keep-alive and then runc exec into the running container)

- Notes on connectivity:
  - If the container is not reachable from the host network, run the client inside the container with `runc exec <container> python3 /runc/dirty-track/experiment/migration/gzip/client_bench.py --server http://127.0.0.1:8080 ...`
  - Otherwise use host to container address (container IP) for the `--server` URL.

- Run client bench (host or inside container):
  python3 client_bench.py --server http://<container_ip_or_host>:8080 --concurrency 8 --requests 1000 --dataset ./payloads

Notes:
- The bench prints METRIC_HEADER / METRIC_VALUES lines suitable for `fog_test.py` capture.