# stream-window-engine

本地实时事件聚合服务：事件按**事件时间**进入有界滚动窗口，在乱序、重复与进程重启的情况下给出稳定、可解释、可复现的聚合结论。仅依赖 Spring Boot 与本地文件系统，不依赖任何外部中间件、凭据或网络服务。

## 语义模型

### 事件时间与窗口

- 每条事件携带 `eventId`（全局唯一，去重依据）、`key`（分组键）、`eventTime`（事件时间，毫秒）、`value`（聚合值）。
- 窗口为事件时间**滚动窗口** `[start, end)`，大小由 `stream-engine.window-size-ms` 决定，按 `key` 分组聚合 `count` 与 `sum`。
- 同一事件集合在**任意到达顺序**下，水位推进后的窗口结果完全一致（结果中的 `eventIds` 已排序，聚合为交换结合运算）。

### 水位（Watermark）

- 水位由外部通过 `POST /api/watermark` **显式推进**，且必须**严格单调递增**，否则请求被拒绝（`IllegalArgumentException`）。
- 水位 `W` 的语义：引擎认为事件时间 `<= W` 的事件"基本到齐"。
- 窗口结果只在 `watermark >= windowEnd` 后**定版**（version=1）；定版后状态仍保留一个"宽限期"，用于迟到订正。
- 当 `watermark >= windowEnd + allowedLatenessMs` 时，窗口状态（含该窗口的去重依据）被**回收**，内存不随事件总数无界增长。

### 迟到事件

按事件时间与当前水位的关系，每条事件得到**可区分**的结论（`IngestStatus`），绝不静默计入或丢弃：

| 状态 | 条件 | 效果 |
|---|---|---|
| `ACCEPTED` | `eventTime > watermark` | 计入活跃窗口 |
| `ACCEPTED_LATE` | `eventTime <= watermark` 且 `windowEnd + allowedLateness > watermark` | 计入宽限窗口，**订正**已定版结果（version 递增） |
| `REJECTED_TOO_LATE` | `windowEnd + allowedLateness <= watermark` | 拒绝，进入有界死信日志（`GET /api/stats` 可观测），不影响任何窗口结果 |
| `DUPLICATE` | `eventId` 已见过（含重启恢复后） | 不重复计数 |
| `BACKPRESSURE` | 有界缓冲达到上限 | 事件未计入，调用方应稍后重试 |

### 背压与有界状态

- 三类有界资源：活跃/宽限窗口数（`max-buffered-windows`）、去重索引容量（`max-seen-event-ids`）、死信日志容量（`max-dead-letters`，满时淘汰最老并计数）。
- 前两者达到上限时摄取返回 `BACKPRESSURE`：**不丢数据、不崩溃**，只是拒绝计入；水位推进回收状态后重试即可成功。
- 已定版结果保留数量由 `max-stored-results` 限制，超出时淘汰最老窗口结果。

### 检查点与恢复

- 检查点写入本地文件（`stream-engine.checkpoint-dir/checkpoint.json`），内容为：水位、活跃/宽限窗口状态、已定版结果、窗口版本计数器、去重索引。
- 写入为**原子操作**（临时文件 + rename），不会留下半截文件；每次水位推进后必写，每次成功摄取后默认也写（`checkpoint-on-ingest`）。
- 文件带 `magic`、格式版本号与 CRC32 校验和。启动时：
  - 无检查点：默认全新启动；严格模式（`checkpoint-required-on-startup=true`）下抛 `CheckpointMissingException`；
  - 校验和/格式损坏：抛 `CheckpointCorruptedException`；
  - 版本不兼容：抛 `CheckpointVersionException`；
  - 任何异常都**明确拒绝**，绝不部分应用。
- 恢复后重复提交的事件依据恢复的 `eventId` 去重索引判为 `DUPLICATE`，**不重复计数**。

## REST API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/events` | 摄取事件：`{"eventId","key","eventTime","value"}`，返回 `IngestResult{status, windowStart, reason}` |
| `POST` | `/api/watermark` | 推进水位：`{"watermark": <ms>}`，返回当前水位 |
| `GET` | `/api/windows?key=<key>` | 查询某 key 的全部已定版窗口结果 |
| `GET` | `/api/stats` | 状态观测：水位、活跃窗口数、去重索引大小、死信计数等 |

## 配置项（`application.properties`，前缀 `stream-engine.`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `window-size-ms` | 60000 | 滚动窗口大小 |
| `allowed-lateness-ms` | 30000 | 允许迟到时长（宽限期） |
| `max-buffered-windows` | 10000 | 活跃+宽限窗口上限（背压阈值） |
| `max-seen-event-ids` | 1000000 | 去重索引容量（背压阈值） |
| `max-stored-results` | 10000 | 已定版结果保留上限 |
| `max-dead-letters` | 10000 | 死信日志容量 |
| `checkpoint-dir` | `data/checkpoints` | 检查点目录 |
| `checkpoint-required-on-startup` | false | 启动时是否必须存在检查点 |
| `checkpoint-on-ingest` | true | 每次成功摄取后是否落检查点 |

## 运行与测试

```bash
mvn test          # 运行全部测试
mvn spring-boot:run   # 本地启动服务（8080 端口）
```

测试覆盖：

- `OutOfOrderAndLateEventTest`：乱序到达结果一致、允许迟到内订正、超迟到拒绝且可见、重复事件去重；
- `CheckpointRecoveryTest`：重启后续传、重启后重复提交不重复计数、缺失/损坏/版本不兼容检查点明确拒绝；
- `ConcurrentIngestionTest`：并发摄取与串行结果一致、并发重复提交恰好计入一次；
- `BoundedStateTest`：缓冲/去重索引背压、状态随水位回收、水位单调性；
- `WindowAssignerTest`：窗口分配边界。

测试日志会打印水位推进、窗口定版/订正结果与去重依据（`dedupBasis=eventId`），便于复核结论的可复现性。
