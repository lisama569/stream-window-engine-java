# stream-window-engine-java

本地实时事件聚合服务：事件按**事件时间**进入有界滚动窗口，在乱序、重复与进程重启的
情况下给出稳定、可解释、可复现的聚合结论。仅依赖 Spring Boot 与本地文件系统，
不依赖任何外部中间件、凭据或网络服务。

## 事件时间与水位语义

- 每个事件携带全局唯一的 `eventId`（去重依据）、业务键 `key`、事件时间
  `eventTimeMillis`（epoch 毫秒）与数值 `value`；多路接入时可选携带 `sourceId`
  标识来源，不携带时归入默认来源。
- 窗口为固定长度 `engine.window-size-millis` 的滚动窗口，事件按事件时间归属窗口：
  `windowStart = floor(eventTime / windowSize) * windowSize`。
- 每个已注册来源维护单调不减的进度（已见最大事件时间与已生效标记进度取大）。
  **水位（watermark）= min(各来源进度) − `engine.allowed-lateness-millis`**，单调不减。
  水位表示“事件时间早于该值的事件被认为已全部到达”。只有被事件或标记注册过的
  来源才参与水位计算，因此单来源（含不带来源信息的旧用法）语义与旧版完全一致：
  水位 = 已见最大事件时间 − 允许迟到时长。
- 窗口结束时间 `<=` 水位时窗口**确定（finalize）**，结果通过监听器/REST 输出。
  窗口结果只取决于被接受事件的集合：在满足有界乱序前提（同一批事件两两时间差
  不超过允许迟到时长）时，**任意到达顺序下同一事件集合的窗口结果完全一致**。

## 多来源与空闲/进度标记

上游拆成多路来源并发接入时，长时间无数据的来源会把水位拖住，导致其它来源
已满足条件的窗口无法定版。为此引入来源进度/空闲标记：

- 标记 `SourceMarker{markerId, sourceId, progressTimeMillis}` 表示“该来源在
  `progressTimeMillis`（事件时间）之前的数据已经到齐、当前空闲”。`markerId`
  全局唯一，是标记的去重依据。
- 标记生效后该来源以 `progressTimeMillis` 参与全局水位计算，不再拖住其它来源
  的窗口结论；来源恢复发数据时自然接回：事件时间仍大于水位的正常计入，
  已被水位越过的得到可区分的 `LATE_REJECTED`，已定版结果不会被改坏。
- 标记处理结论互相可区分且幂等（`MarkerStatus`）：

| 状态 | 含义 |
|---|---|
| `APPLIED` | 标记已生效，来源进度推进（可能触发窗口确定） |
| `DUPLICATE` | 与已处理标记 `markerId` 重复，未重复推进 |
| `STALE` | 进度不领先于该来源已知进度，或不领先于当前全局水位（过期或互相矛盾），或标记非法，未生效 |
| `BACKPRESSURE` | 来源数量或去重表达到上限，未生效，调用方应稍后重试 |

- 未生效的标记（`DUPLICATE`/`STALE`/`BACKPRESSURE`）不改变任何内部状态；
  重复事件与重复标记在任意交错、任意并发下都不会重复计数或重复触发结论。
- **未知来源的过期标记**：一个从未出现过的来源发来明显落后于当前水位的
  空闲标记（`progressTimeMillis <=` 当前水位）时，判 `STALE`——不生效、
  不登记该来源、不把水位钉回低位。水位单调不减，这种标记不可能再推进任何
  窗口结论；若放行，该来源会以过期进度参与 min 运算，把其它所有来源永远
  拖住。已知来源的标记另有“不领先于该来源已知进度”的过期判定。
- **事件与标记各自独立去重**：事件按 `eventId`、标记按 `markerId` 分别登记在
  两张互不影响的去重表里。各路来源的编号各自分配，事件编号与标记编号即使
  完全相同（包括 `M:` 开头的编号），正常标记照常生效、正常事件也不会被误判
  成重复。两张表都随水位回收、都进入检查点，重启后重复标记仍判 `DUPLICATE`。
- 兼容范围：不携带 `sourceId` 的事件与旧调用方行为完全一致（默认来源、
  单路水位语义）；旧格式检查点按清晰规则兼容恢复（见下节），
  其它不兼容版本整体拒绝，不做部分应用。


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
- **背压**：打开窗口数达到 `engine.max-open-windows`、去重表达到
  `engine.max-dedup-entries` 或来源数达到 `engine.max-sources` 时返回
  `BACKPRESSURE`——不丢数据、不崩溃。
  水位推进会回收已确定窗口与过期去重条目，之后仍处于迟到视野内的被背压事件
  **重试可以成功**；若重试时水位已越过其事件时间，则得到可区分的 `LATE_REJECTED`。
- **有界状态**：窗口确定后其状态立即回收；去重条目在事件时间被水位越过后回收
  （此类事件若再次到达只会被判迟到，不可能重复计入，故回收安全）。因此长时间
  运行下内存不随事件总数无界增长。

## 检查点与恢复保证

- 检查点内容：格式版本、当前水位、事件去重表、标记去重表（v3 起分表存储）、
  未关闭窗口状态、已确定窗口结果与各来源进度表。
- 写入：序列化为 JSON 并计算 CRC32，先写临时文件再**原子重命名**，避免半截文件。
- 恢复：启动时从最近有效检查点整体恢复；恢复后重复提交的事件因去重表随检查点
  持久化而被判 `DUPLICATE`，**不会造成重复计数**。
- **格式版本兼容规则**（当前支持 v1..v3，按格式严格区分，不越界套用）：
  - **v1**（无来源进度表，`sourceProgress` 字段缺失）：按单路语义兼容——恢复时以
    `watermark + allowedLateness` 重建默认来源进度，恢复后仍按单路语义工作；
    去重表中的 `M:` 前缀条目（v2 写入的标记去重条目）拆分到独立的标记去重表。
  - **v2**（有来源进度表，标记去重条目以 `M:` 前缀混在事件去重表）：来源进度表
    直接恢复；混合去重表按 `M:` 前缀拆分成事件/标记两张独立的表，拆分后两类
    编号互不影响。
  - **v3**（当前格式）：事件去重表与标记去重表分表存储，直接恢复。
  - **空检查点**（进程空跑后正常关停落盘，任何格式）：水位为 `Long.MIN_VALUE`、
    没有任何来源进度。恢复时**不虚构任何来源**——v2/v3 空检查点本身就没有来源
    记录；v1 空检查点因水位无效，单路兼容规则同样不适用。重启后无论上游带来源
    标识还是走老的单路用法，来源都随事件/标记到来重新登记，水位正常推进、
    窗口按时定版。
- 明确拒绝（抛 `CheckpointException`，不做部分应用）：
  - 文件存在但无法解析或 CRC 校验不符（损坏）；
  - 格式版本不兼容（低于 v1 或高于当前版本）；
  - 配置 `engine.checkpoint-required=true` 但文件缺失。
  文件缺失且 `checkpoint-required=false` 时全新启动。
- 正常关停（`@PreDestroy`）自动落盘一次检查点；也可通过 `POST /api/checkpoint`
  手动触发。

## REST 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/api/events` | 摄取事件，body 为 `{eventId, key, eventTimeMillis, value[, sourceId]}` |
| `POST` | `/api/markers` | 提交来源进度/空闲标记，body 为 `{markerId, sourceId, progressTimeMillis}` |
| `GET` | `/api/watermark` | 查询当前水位 |
| `GET` | `/api/sources/{sourceId}/progress` | 查询指定来源当前进度 |
| `GET` | `/api/windows` | 查询已确定的窗口结果 |
| `POST` | `/api/checkpoint` | 手动触发检查点落盘 |

## 配置项（`engine.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `engine.window-size-millis` | `60000` | 滚动窗口长度（毫秒） |
| `engine.allowed-lateness-millis` | `30000` | 允许迟到时长，决定水位位置 |
| `engine.max-open-windows` | `1024` | 同时打开的最大窗口数，超限背压 |
| `engine.max-dedup-entries` | `100000` | 去重表最大条目数，超限背压 |
| `engine.max-sources` | `4096` | 同时跟踪的最大来源数，超限背压 |
| `engine.max-retained-results` | `10000` | 保留的已确定窗口结果最大条数 |
| `engine.checkpoint-path` | `data/checkpoint.json` | 本地检查点文件路径 |
| `engine.checkpoint-required` | `false` | 启动时是否必须存在有效检查点 |

## 测试

```bash
mvn test
```

覆盖：乱序与迟到事件（多随机种子下结果一致）、背压触发与重试、状态随水位回收、
重启后重复提交不重复计数、损坏/版本不兼容检查点拒绝、并发摄取与顺序执行结果一致；
多来源场景：来源停更不阻塞其它来源窗口定版、事件与标记任意交错及并发下结论一致、
重复/过期标记幂等且可区分、来源数上限背压、多来源重启恢复不重不漏；
本次修复的回归场景：空检查点重启后带来源接入与老单路用法都能正常推进水位并定版、
事件编号与标记编号互撞（含 `M:` 开头的极端编号）时各自按自身语义去重、
未知来源的过期空闲标记判 `STALE` 且不拖住其它来源、
v1/v2 旧格式检查点按兼容规则恢复（v2 混合去重表拆分）。
单测日志会打印来源、当前进度、水位、窗口结果与去重依据（`eventId`/`markerId`），
结论可复现、失败原因可区分。

只跑多来源与恢复相关测试：

```bash
mvn test -Dtest=MultiSourceWindowEngineTest,MultiSourceCheckpointRecoveryTest
```

只跑本次修复的三个回归场景：

```bash
mvn test -Dtest='MultiSourceCheckpointRecoveryTest#emptyCheckpointRestartDoesNotBlockSourcedIngestion+emptyCheckpointRestartKeepsLegacySingleSourceSemantics,MultiSourceWindowEngineTest#eventAndMarkerDedupAreIndependentEvenWithCollidingIds+staleMarkerFromUnknownSourceIsRejectedAndDoesNotBlockOthers'
```
