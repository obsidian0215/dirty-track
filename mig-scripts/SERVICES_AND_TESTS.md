# Services & Tests — DeFog / dirty-track 🔧

## 目标 (TL;DR) ✅
- 清晰说明仓库中每个主要服务的用途与常用测试方法。
- 指出可复用的测试入口与建议的运行流程（smoke / bench / migration）。
- 标注已发现的冗余脚本并采取清理动作（见下文）。

---

## 1) DeFog workloads (located under `/runc/fog_workloads`) 🚀

- YOLO
  - 用途：图像目标检测（edge inference）。
  - 运行/测试：
    - 构建/准备：`chroot_build.sh`（在 bundle 目录里）用于 chroot 安装依赖。
    - 启动：`./run.sh [--foreground] <application> <pipeline>` 或加 `--keep-alive` 保持容器驻留供迁移测试。示例：
      - `cd /runc && ./fog_workloads/YOLO/run.sh --foreground 0 0`（应用 0, pipeline 0）
    - 验证：查看 `/runc/results/arrresult.txt` 和 `/runc/results/cloudresult.txt`，recvtty 日志在 `/tmp/recvtty-YOLO.log`。
    - 数据：默认把结果写到容器内 `/mnt/results`，host 会 bind-mount 到 `/runc/results`（优先保留 bind-mount，不使用 tmpfs）。

- GOCR (new)
  - 用途：轻量 OCR（通过 `gocr`），用于文本识别的迁移测试用例（边缘光学字符识别场景）。
  - 运行/测试：
    - 构建/准备：`sudo /runc/fog_workloads/GOCR/chroot_build.sh`（会安装 `gocr`、`imagemagick`、`Flask` 与 `Pillow`）。
    - 服务器模式：`runc exec <container> sh -c '/root/scripts/execute.sh --server'` 将在容器内启动 HTTP API（`/ocr`）。
      - 客户端基准位于：`/runc/dirty-track/experiment/migration/gocr/`（`client_bench.py` 与 `datasets/generate_images.py`）。
      - 生成样例图像：`python3 dirty-track/experiment/migration/gocr/datasets/generate_images.py --outdir ./images --count 500`
    - 启动（单次）：`cd /runc && ./fog_workloads/GOCR/run.sh --foreground 0`（或加 `--keep-alive` 供迁移测试循环）。
    - 验证：`/runc/fog_workloads/GOCR/results/ocr_output.txt`、`arrresult.txt`，以及客户端 `METRIC_HEADER/METRIC_VALUES` 输出。

- GZIP (new)
  - 用途：压缩服务基线（使用 `gzip`），用于测试服务在高 I/O / CPU 压缩负载下的迁移表现。
  - 运行/测试：
    - 构建/准备：`sudo /runc/fog_workloads/GZIP/chroot_build.sh`（会安装 Python3、`Flask`）。
    - 服务器模式：`runc exec <container> sh -c '/root/scripts/execute.sh --server'` 将在容器内启动 HTTP API（`/compress`）。
      - 客户端基准位于：`/runc/dirty-track/experiment/migration/gzip/`（`client_bench.py` 与 `datasets/generate_payloads.py`）。
      - 生成样例负载：`python3 dirty-track/experiment/migration/gzip/datasets/generate_payloads.py --outdir ./payloads --count 500 --size 65536`
    - 启动（单次）：`cd /runc && ./fog_workloads/GZIP/run.sh --foreground`（或 `--keep-alive` 用作持续负载）。
    - 验证：查看 `/runc/fog_workloads/GZIP/results/compress_meta.txt`，并观察 `METRIC_HEADER/METRIC_VALUES` 打印的压缩时间与比率。

- GZIP (new)
  - 用途：压缩服务基线（使用 `gzip`），用于测试服务在高 I/O / CPU 压缩负载下的迁移表现。
  - 运行/测试：
    - 构建/准备：`sudo /runc/fog_workloads/GZIP/chroot_build.sh`（会安装 Python3 环境以便运行脚本）。
    - 启动：`cd /runc && ./fog_workloads/GZIP/run.sh --foreground`（或 `--keep-alive` 用作持续负载）。
    - 验证：查看 `/runc/fog_workloads/GZIP/results/compress_meta.txt`，并观察 `METRIC_HEADER/METRIC_VALUES` 打印的压缩时间与比率。


- PocketSphinx / Aeneas / FogLAMP
  - 用途：语音识别、音频/文本处理、IoT 服务模拟。
  - 运行/测试：与 YOLO 类似（`run.sh` + `execute.sh`），需要 `assets`（音频、模型）在 `/runc/assets` 或 bundle 的 `assets/` 下。

- iPokeMon
  - 用途：简单 microservice 使用 Redis（演示服务），适合作为迁移时保持连接/状态的测试用例。
  - 运行/测试：`run.sh` 同样支持 `--keep-alive`；确认容器内 `redis-server` 可执行且配置（参见下面 Redis 部分）。

---

## 2) Redis（位于 `/runc/containers/redis`）💾

- 用途：运行时数据存储，多个 bench（video/sensor/vehicle）依赖 Redis。
- 当前默认：**持久化（RDB）已禁用**（`save ""`），以避免把数据写入 tmpfs、造成迁移后数据不确定。
- 可选持久化：已为容器增加 host bind-mount `/runc/containers/redis/data → /var/lib/redis`；要启用持久化：在容器的 `/etc/redis/redis.conf` 中恢复 `save` 指令并重启容器，数据就会写入主机路径并在迁移后可保留。
- 测试脚本：
  - `redis_test.py`：场景化视频/传感器/车辆基准
  - `ycsb.py`：YCSB 驱动的 Redis 负载测试（**推荐**，更通用）
  - `redis-memtier.py`：使用 memtier 负载发生器的测试场景
  - 实验脚本：`/runc/dirty-track/experiment/migration/redis/*.py`（bench_cartelem.py, bench_sensoragg.py, bench_video_cache.py）

---

## 3) InfluxDB / Elasticsearch / Nginx / Postgres 等 🧩

- 用途：分别作为时序/搜索/反向代理/关系型数据库的后端。部分迁移/基准脚本依赖它们。
- 测试入口：
  - Influx: `influxdb_test.py` + `experiment/migration/influxdb/*` benches
  - Elasticsearch: `elasticsearch-ycsb.py`（YCSB 风格）

---

## 4) 辅助工具与常见流程 🛠

- recvtty：用于 `run.sh` 启动时将 container 控制台写到 socket（`--console-socket`），迁移测试可以监听该 socket 获取输出。
- result_writer.py：统一 `append_result()`、`summarize_results()`；所有 orchestrator/bench 应输出可解析的指标行（METRIC_HEADER/METRIC_VALUES），并使用 `append_result` 写入 `results/<exp>.tsv`。
- **资源监控（新增）**：`source.py` 与 `destination.py` 在迁移过程中会自动启动资源监控器（默认每 1s 采样），分别写入按 run 区分的文件，例如 `migrate/d_log/resource_usage.source.<ts>.tsv` 与 `migrate/d_log/resource_usage.dest.<ts>.tsv`。脚本会打印 `METRIC_PARAM\tsource_resource_usage\t<path>` / `METRIC_PARAM\tdest_resource_usage\t<path>`，上层 orchestrator 会将这些路径作为 `extra_param_lines` 写入 `results/<exp>.tsv`（便于后处理与绘图）。
- **VIP 控制器（新增）**：快速 VIP 切换逻辑已集中为 `mig-scripts/vipctl.py`，提供：
  - `set_keepalived_priority()`（编辑 keepalived 配置并 reload）
  - `ip_addr_add()` / `ip_addr_del()`（快速本地主机 ip add/del）
  - `arping_announce()`（gratuitous ARP 发包）
  - CLI：`vipctl set-priority|ip-add|ip-del|announce|switch-local`（支持 `--dry-run`）

  `destination.py` 与 `source.py` 已改为调用 `vipctl`，并且 `destination` 在收到迁移开始通知时会调用 `vipctl` 来触发 VIP 切换（可被替换为更快的 ip+arp 方案）。
- **测试运行时长与持续测试建议**：fog workloads 现在在 `fog_test.py` 支持 `--warmup`（默认 30s）与 `--duration`（默认 120s），并支持 `--continuous` 以持续执行测试（直到用户中断）。建议：`warmup >= 10s`，`duration >= 60s`，并在关键实验上执行多次（>=3 runs）。
- 典型迁移测试流程（简版）:
  1. 确保 assets 与 config 准备就绪（`/runc/assets`, `/runc/configs/config.sh`）。
  2. 在 source 上运行 workload 容器（`run.sh --keep-alive` 或 wrapper 启用循环执行 `execute.sh`）。
  3. 在 client 启动基准工具（YCSB / memtier / bench scripts）。
  4. 通过 mig-scripts（`ycsb.py` / `redis_test.py` / `fog_test.py`）启动迁移流程（source/destination prepare -> run migration -> collect metrics）。
  5. 汇总：`result_writer.summarize_results(exp_name)` 输出 `<exp>_summary.tsv`。

---

## 5) 冗余脚本说明与处理（已处理） 🧹

- 已归档：通过本次清理，以下脚本已从主目录移入 `mig-scripts/archived/` 以便回滚（非破坏性）：
  - `redis-ycsb.py` (archived)
  - `ycsb.py` (archived)
  - `start.py` (archived)
  - `source-cpu-mem.py` (archived)
  - `source-cpu-mem-net.py` (archived)

- 说明：
  - 归档是非破坏性操作，若需要可以从 `mig-scripts/archived/` 恢复原始脚本。
  - **监控逻辑已集中到 `mig-scripts/monitor.py`**，`source.py` 与 `destination.py` 会自动启用该模块进行迁移期间资源采样（每 1s）；采样文件路径会以 `METRIC_PARAM` 的形式输出供上层 orchestrator 使用。
  - 推荐使用更专用的 orchestrator 作为当前入口：`redis_test.py` / `redis-memtier.py` / `fog_test.py` 等，而不是旧的 `start.py` / `ycsb.py`。

---

## 6) 快速命令备忘（最常用）
- YOLO smoke: `cd /runc && ./fog_workloads/YOLO/run.sh --foreground 0 0`
- Run fog workloads migration suite: `python3 fog_test.py --scene yolo --runs 3 --experiment-types pre-copy`
- Redis YCSB: `python3 ycsb.py --runs 3 --source-ip <src> --dest-ip <dst>`
- Redis memtier: `python3 redis-memtier.py --runs 3 --client-ip <client> --mt-n 150000`
- Summarize results: in python REPL `from result_writer import summarize_results; summarize_results('your_exp')`

---