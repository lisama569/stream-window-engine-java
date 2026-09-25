# stream-window-engine-java

本地实时事件聚合服务：事件按**事件时间**进入有界滚动窗口，在乱序、重复与进程重启的
情况下给出稳定、可解释、可复现的聚合结论。仅依赖 Spring Boot 与本地文件系统，
不依赖任何外部中间件、凭据或网络服务。

## 事件时间与水位语义

- 每个事件携带全局唯一的 `eventId`（去重依据）、业务键 `key`、事件时间
  `eventTimeMillis`（epoch 毫秒）与数值 `value`。
- 窗口为固定长度 `engine.window-size-millis` 的滚动窗口，事件按事件时间归属窗口：
  `windowStart = floor(eventTime / windowSize) * windowSize`。
- **水位（watermark）= 已见最大事件时间 − `engine.allowed-lateness-millis`**，单调不减。
  水位表示“事件时间早于该值的事件被认为已全部到达”。
- 窗口结束时间 `<=` 水位时窗口**确定（finalize）**，结果通过监听器/REST 输出。
  窗口结果只取决于被接受事件的集合：在满足有界乱序前提（同一批事件两两时间差
  不超过允许迟到时长）时，**任意到达顺序下同一事件集合的窗口结果完全一致**。

## 摄取结论（可区分，不静默）

每次摄取返回四种互相可区分的状态之一，并附人可读原因：

| 状态 | 含义 |
|---|---|
| `ACCEPTED` | 已计入对应窗口 |
| `DUPLICATE` | `eventId` 已登记过（含重启后重复提交），不重复计数 |
| `LATE_REJECTED` | `eventTime <= watermark`，超出允许迟到范围，拒绝计入（不静默丢弃，也不静默计入） |
| `BACKPRESSURE` | 打开窗口数或去重表达到上限，事件未被计入，调用方应稍后重试 |

## 迟到与背压规则

- **迟到**：事件时间 `<=` 当前水位的事件被 `LATE_REJECTED`；介于水位与已见最大事件
  时间之间的乱序事件仍被接受。允许迟到时长由 `engine.allowed-lateness-millis` 控制。
- **背压**：打开窗口数达到 `engine.max-open-windows` 或去重表达到
  `engine.max-dedup-entries` 时返回 `BACKPRESSURE`——不丢数据、不崩溃。
  水位推进会回收已确定窗口与过期去重条目，之后仍处于迟到视野内的被背压事件
  **重试可以成功**；若重试时水位已越过其事件时间，则得到可区分的 `LATE_REJECTED`。
- **有界状态**：窗口确定后其状态立即回收；去重条目在事件时间被水位越过后回收
  （此类事件若再次到达只会被判迟到，不可能重复计入，故回收安全）。因此长时间
  运行下内存不随事件总数无界增长。

## 检查点与恢复保证

- 检查点内容：格式版本、当前水位、去重表、未关闭窗口状态、已确定窗口结果。
- 写入：序列化为 JSON 并计算 CRC32，先写临时文件再**原子重命名**，避免半截文件。
- 恢复：启动时从最近有效检查点整体恢复；恢复后重复提交的事件因去重表随检查点
  持久化而被判 `DUPLICATE`，**不会造成重复计数**。
- 明确拒绝（抛 `CheckpointException`，不做部分应用）：
  - 文件存在但无法解析或 CRC 校验不符（损坏）；
  - 格式版本不兼容；
  - 配置 `engine.checkpoint-required=true` 但文件缺失。
  文件缺失且 `checkpoint-required=false` 时全新启动。
- 正常关停（`@PreDestroy`）自动落盘一次检查点；也可通过 `POST /api/checkpoint`
  手动触发。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/events` | 摄取事件，body 为 `{eventId, key, eventTimeMillis, value}` |
| `GET` | `/api/watermark` | 查询当前水位 |
| `GET` | `/api/windows` | 查询已确定的窗口结果 |
| `POST` | `/api/checkpoint` | 手动触发检查点落盘 |

## 配置项（`engine.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `engine.window-size-millis` | `60000` | 滚动窗口长度（毫秒） |
| `engine.allowed-lateness-millis` | `30000` | 允许迟到时长，决定水位位置 |
| `engine.max-open-windows` | `1024` | 同时打开的最大窗口数，超限背压 |
| `engine.max-dedup-entries` | `100000` | 去重表最大条目数，超限背压 |
| `engine.max-retained-results` | `10000` | 保留的已确定窗口结果最大条数 |
| `engine.checkpoint-path` | `data/checkpoint.json` | 本地检查点文件路径 |
| `engine.checkpoint-required` | `false` | 启动时是否必须存在有效检查点 |

## 测试

```bash
mvn test
```

覆盖：乱序与迟到事件（多随机种子下结果一致）、背压触发与重试、状态随水位回收、
重启后重复提交不重复计数、损坏/版本不兼容检查点拒绝、并发摄取与顺序执行结果一致。
单测日志会打印水位、窗口结果与去重依据（`eventId`），结论可复现、失败原因可区分。
