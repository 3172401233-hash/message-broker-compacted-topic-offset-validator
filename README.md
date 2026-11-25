# message-broker-compacted-topic-offset-validator
验证压缩 topic offset、墓碑和消费者位置

> **注意：本项目中所有日志段、key、offset、墓碑和消费者数据均为模拟数据，
> 不连接任何真实 Kafka 集群，仅用于在本地复现和验证压缩 topic 的 offset 可见性问题。**

## 问题背景

topic 开启日志压缩（log compaction）后：

- 重复 key 的旧记录被清理，消费者 offset 可能仍指向已删除记录；
- tombstone（删除标记）有独立的保留窗口，到期后被移除，形成 offset 空洞；
- 重试消费从失效 offset 跳转时，可能跳过某 key 真正的最新值或丢失删除语义；
- 慢消费者的 offset 可能落后于压缩后的 log start offset。

## 环境

- 目标版本：Python 3.8.2（代码仅使用 3.8 兼容语法，无第三方依赖）

## 使用方法

```bash
python compacted_topic.py scenario.example.json
```

输入 JSON 字段：

- `now`：当前时间戳（毫秒），用于判定墓碑是否到期
- `tombstone_retention_ms`：墓碑保留窗口
- `segments`：模拟日志段列表，每段含 `records`（`offset` / `key` / `value` / `timestamp`，`value` 为 `null` 表示墓碑）
- `consumers`：消费者组列表（`group` / `offset`，可选 `retry_offset` 表示重试引用位置）

输出报告：

- `visibility`：每个 offset 的可见状态（`RETAINED` / `TOMBSTONE_RETAINED` / `COMPACTED_AWAY` / `TOMBSTONE_EXPIRED` / `HOLE`）
- `skippable_ranges`：可跳过的连续 offset 闭区间
- `retained_records`：压缩后仍需保留的记录
- `affected_consumers`：受影响消费者及其风险（`SKIPS_SUPERSEDED_VALUE` / `SKIPS_LATEST_VALUE` / `SKIPS_TOMBSTONE`）

## 运行测试

```bash
python -m unittest
```

测试覆盖：墓碑清理（窗口内保留 / 到期移除）、offset 空洞（段间空洞、越过日志末端）、
慢消费者（落后于 log start offset）、重复 key 压缩。
