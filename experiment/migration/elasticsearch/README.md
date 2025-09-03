# ElasticSearch 基准测试脚本

该目录包含用于ElasticSearch的全文搜索和文档索引基准测试脚本。

## 依赖安装

```bash
pip install elasticsearch
```

## 脚本概览

### benchmark.py
**ElasticSearch基础基准测试**

支持文档索引和搜索的并发测试，包括批量写入、范围查询和分面搜索。

#### 基本用法
```bash
python benchmark.py --threads 10 --operations 1000 --es-host localhost \
    --es-port 9200 --index-name benchmark-test --test-mode index
```

#### 参数配置

##### ElasticSearch连接参数
- `--es-host`: ES主机地址 (默认: localhost)
- `--es-port`: ES端口 (默认: 9200)
- `--index-name`: 索引名称 (默认: benchmark-test)

##### 负载参数
- `--threads`: 并发线程数 (默认: 10)
- `--operations`: 每个线程操作数 (默认: 1000)
- `--field-count`: 文档额外字段数 (默认: 5)
- `--test-mode`: 测试模式 (index/search/mixed) (默认: index)

##### 高级选项
- `--query-type`: 查询类型 (wildcard/term/match_phrase/all) (默认: all)

#### 使用示例
```bash
# 索引性能测试
python benchmark.py --threads 16 --operations 5000 --es-host localhost \
    --index-name indexing-test --field-count 10 --test-mode index

# 搜索性能测试
python benchmark.py --threads 20 --operations 5000 --es-host localhost \
    --index-name search-test --test-mode search

# 混合负载测试
python benchmark.py --threads 12 --operations 3000 --es-host localhost \
    --index-name mixed-test --test-mode mixed --field-count 8

# 大规模文档测试
python benchmark.py --threads 8 --operations 10000 --es-host localhost \
    --index-name large-docs --field-count 50 --test-mode index
```

---

## 输出统计

ElasticSearch脚本提供标准性能指标：

### 实时监控输出格式
```
Thread 3-5: ops=1280, total_time=2.34s, avg_latency=1.83ms, throughput=547.0 ops/s
```

### 统计摘要
- 每个线程的操作数和性能指标
- 总体延迟分布统计
- 百分位延迟 (P50, P95等)
- 平均吞吐量

---

## 架构特性

### 索引优化
- 自动索引创建和管理
- 动态字段扩展
- 时间戳自动生成

### 查询能力
- 通配符查询支持
- 词项精确查询
- 短语匹配查询
- 时间范围过滤

### 批量操作
- 使用ElasticSearch bulk API
- 并发生成文档
- 错误重试机制

---

## 适用场景

- **日志分析平台**: 海量日志数据存储和全文检索
- **内容管理系统**: 文档索引和搜索
- **电商平台**: 商品搜索和推荐系统
- **社交媒体分析**: 社交数据存储和查询
- **文档管理系统**: 文件内容索引和元数据搜索

---

## 配置建议

### ElasticSearch配置
```yaml
# elasticsearch.yml
cluster.name: benchmark-cluster

# 内存配置
bootstrap.memory_lock: true
ES_JAVA_OPTS: "-Xmx8g -Xms8g"

# 路径配置
path.data: /var/lib/elasticsearch/data
path.logs: /var/log/elasticsearch

# 网络配置
network.host: 0.0.0.0
http.port: 9200

# 性能调优
indices.memory.index_buffer_size: 10%
indices.query.bool.max_clause_count: 1024
```

### JVM调优
```bash
# JVM配置
export ES_JAVA_OPTS="$ES_JAVA_OPTS -XX:+UnlockExperimentalVMOptions"
export ES_JAVA_OPTS="$ES_JAVA_OPTS -XX:+UseCGroupMemoryLimitForHeap"
export ES_JAVA_OPTS="$ES_JAVA_OPTS -XX:MaxRAMPercentage=75"
```

---

## 性能优化建议

### 索引优化
1. **分片设置**: 根据数据量和查询模式设置合适的分片数
2. **刷新间隔**: 批量导入时增加刷新间隔
3. **映射优化**: 使用适当的字段类型和分析器

### 查询优化
1. **查询缓存**: 启用查询结果缓存
2. **字段预热**: 预热搜索字段
3. **过滤器缓存**: 使用过滤器缓存复杂查询

### 系统级优化
1. **文件系统**: 使用EXT4或XFS文件系统
2. **虚拟内存**: 禁用swap或设置合理SWAPINESS
3. **内核参数**: 调整文件句柄限制和内存映射

---

## 使用注意事项

### 数据清理
脚本运行完成后，可以选择清理测试索引：

```bash
# 会询问是否删除测试索引
python benchmark.py --threads 1 --operations 1
# 之后回答 'y' 删除索引
```

### 内存监控
```bash
# 监控ElasticSearch内存使用
curl -X GET "localhost:9200/_cat/nodes?v&h=name,heap.percent"
```

### 索引监控
```bash
# 查看索引状态
curl -X GET "localhost:9200/_cat/indices?v"

# 集群健康检查
curl -X GET "localhost:9200/_cluster/health?pretty"
```

---

## 与迁移测试结合

ElasticSearch基准测试也可与容器迁移测试结合：

### 单一节点迁移测试
```bash
# 在后台运行ES基准测试
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 10 \
    --operations 10000 \
    --es-host localhost \
    --index-name migration-test \
    --test-mode mixed \
    --field-count 10 &

# 同时运行迁移测试
python3 chk_restore.py elasticsearch [options]
```

### 分布式迁移测试
```bash
# 源节点 - 索引负载测试
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 16 \
    --operations 5000 \
    --es-host source-node \
    --test-mode index \
    --field-count 20

# 目标节点 - 搜索验证
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 8 \
    --operations 2000 \
    --es-host dest-node \
    --test-mode search
```

这有助于评估迁移过程对搜索性能的影响，以及确保数据一致性和索引完整性。

---

## 故障排除

### 连接问题
```bash
# 检查ES服务状态
curl -X GET "localhost:9200/_cluster/health?pretty"

# 测试连接
curl -X GET "localhost:9200/"
```

### 性能问题
1. 增加JVM堆内存
2. 调整索引缓冲区大小
3. 优化查询语句
4. 检查磁盘I/O性能

### 内存不足
1. 减少并发线程数
2. 降低文档复杂度
3. 增加ESJVM堆大小
4. 考虑分批处理

### 集群同步问题
1. 验证集群健康状态
2. 检查分片分配状态
3. 监控主从节点同步

---

## 快速开始指南

### 完整测试流程示例
```bash
# 1. 启动Elasticsearch容器
cd /runc/containers && \
runc run --console-socket elasticsearch/console.sock -d -b elasticsearch elasticsearch

# 2. 等待服务就绪 (约30秒)
curl -f http://localhost:9200/_cluster/health?pretty

# 3. 运行快速索引测试
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 4 \
    --operations 100 \
    --test-mode index

# 4. 运行混合负载测试
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 8 \
    --operations 1000 \
    --test-mode mixed \
    --field-count 15 \
    --duration 30

# 5. 清理测试索引 (可选)
python3 ./experiment/migration/elasticsearch/benchmark.py \
    --threads 1 \
    --operations 1
# 然后输入 'y' 确认删除
```

建议：从小到大逐步增加测试规模，确保系统稳定后再进行大规模测试。