# perf-benchmark 开发指南

## 项目使命

构建高性能、测试结果准确、扩展性好的 LLM 性能测试工具。三个一级目标是：

- **工具本身高性能**：降低压测客户端的 CPU、内存和调度开销，减少工具对被测结果的干扰；高并发与大数据集下的能力必须通过实际测量验证。
- **测试结果准确**：请求事实、计时边界、指标公式、聚合窗口和统计分母可追溯；区分实测、估算和观测缺失，保留失败、取消与重试事实。
- **扩展性好**：通过 Stage Plugin 完整替换阶段能力，通过 Hook 进行受控拦截和观察；扩展复用公共执行与资源管理能力，保持职责独立。

精简内核、流式处理、有界队列和 SQLite 结果存储是实现这些目标的工程手段。产品能力覆盖负载准备、压测执行、统计分析与结果比较；数据集兼容、压测模型、协议、统计指标、可观测性、报告及 Web/TUI 分别通过 Plugin、Hook 和应用层实现。

设计变更同时评估性能、测量准确性和扩展边界；出现冲突时先保证测试结果准确。优先设计接口窄、能力深的模块，保持主执行路径明确且易于追踪。

## 能力扩展边界

- 数据集兼容由 Loader / Normalization / Selector Plugin 承担；协议适配由 Executor Plugin 承担；压测模型、速率、准入、并发与重试由 Scheduler Plugin 承担。
- 指标公式、统计窗口与聚合由 Metrics Plugin 承担；结果持久化与读取由 Saver / ResultReader 承担；结果展示与报告由 Exporter 承担。
- 可观测性、诊断和外部集成通过 Hook 扩展，遵守下文的观察与拦截边界。
- `dataset stats/generate/compare` 是独立离线入口，实现在 `perf_benchmark/corpus/`，复用 Loader、Normalization、公共 tokenizer 和 ResultReader。
- Web/TUI 属于应用层，复用运行入口、受控事件和 ResultReader 结果视图；展示与交互使用独立处理路径，通过公开契约访问冻结事实，保持请求收发和计时路径独立。

## 设计与实现约束

- API、路径和配置遵循当前契约；替换接口时同步清理废弃别名、fallback、migration 代码和调用点。
- 采用满足当前需求的最简单完整实现，抽象对应实际复用场景。
- 能力实现以最小端到端闭环为基础，保持已支持路径可运行。架构按实际能力划分可维护的责任边界。
- 核心按职责统计：领域模型与事实、Stage 契约和注册、公共 Host、执行/调度、测量、资源生命周期、可靠结果交接均计入，主要位于 core/pipeline/runtime；公共能力按职责归类。具体 Plugin/Hook、CLI 与展示应用、离线 corpus、具体外部集成单独统计。
- 源码按职责统计物理行数，包含空行、注释和 docstring，用于观察模块规模。新文件或目录默认归入核心；单独分类须说明职责。`tests/core/test_code_size.py` 校验分类清单并报告各组总量及相对 HEAD 增量。
- 领域层级固定为 `Run -> Trial -> Session -> Turn`；Attempt、测量事实和场景判定作为关联事实保存。
- 添加依赖或自行实现功能前，优先使用项目中已有且持续维护的库。
- 设计新能力前先查看成熟工具对同类问题的解决方式，结合本项目的测量语义和扩展契约取舍。
- 每次 Run 执行期间由单 writer 将权威事实写入 SQLite；结束后原子封存为同身份 Parquet，summary 长期保留，SQLite 与完整 details 从 Run.finished_at 起保留七天。统一 ResultReader 校验身份并路由已封存结果。Run 权威结果格式为 SQLite 和同身份 Parquet；报告从读取器中的事实和正式指标派生。
- 离线命令生成的数据集、画像、manifest，以及内部 HF cache / 临时 BlobStore 由所属模块管理格式与生命周期。DuckDB 和 PyArrow 用于内部处理与聚合。

## 稳定 Pipeline

八个 Stage Plugin 的职责如下：

```text
loader
-> normalization
-> selector
-> executor
-> scheduler
-> metrics
-> saver
-> exporter
```

运行时采用流式组装、执行与收尾拓扑：

```text
组装：
  loader -> normalization -> selector -> SessionStream
  SaverRuntime.create + MetricsRuntime.create(publish_results=saver.write_results)
  每个 Trial：executor.create_runtime

执行：
  scheduler -> SessionRunner -> runtime -> metrics -> saver

收尾：
  metrics.finalize -> saver.finalize -> exporter
```

## 核心模块所有权

### Coordinator

- 在创建 Run、Hook dispatcher、BlobStore 和外部资源前，选择并解析、冻结全部八个 Stage 配置，再统一创建 `StageBinding`。
- 管理 Run 和 Stage 的生命周期转换。
- 组装流式输入和执行资源。
- 完成 metrics、saver、exporter、hooks 和 Run 状态的收尾。
- 使用结构化清理机制，确保每个资源只关闭一次。

### PreparedCorpus

- 组装 `DatasetSource`、normalization、selector 和 sealed BlobStore，将 selected metadata 解码为冻结的 Session/Turn plans。
- 区分 normalized 与 selected Arrow schema；选择后释放 normalized batches，plans 构造完成后释放 selected batches。
- 将 selected rows 解码为 O(N) 轻量冻结 plans；每个 Trial 的 `SessionStream` 按 Scheduler 消费进度从 plan 构造当前 Session/Turn，并提供 mmap-backed payload view。
- 统一清理 preparation 的临时正文资源。

### ExecutionEngine / SessionRunner

- 统一管理 runtime prepare、Turn 执行、取消、metrics 记录、增量持久化和 runtime close。
- 同一个 Session 中的 Turn 必须串行执行。
- FIFO 请求准入只通知下一张可放行票据；大活跃池下不能对每次入队/出队广播唤醒全部等待者。取消队首须通知后继；取消、重试、过载及并发额度回收保持原有语义。
- Scheduler 通过 TurnControl.send/retry/cancel 和 next_turn 决定下一步操作。待执行的 Turn 保持 pending；已准入或执行的 Turn 按真实终态持久化。
- Attempt 是 (turn_id, attempt_no) 标识的执行事实；最终 Attempt 决定 Turn 主指标，所有 Attempt 独立记录。预期场景结果与原始请求事实分别记录。
- 在并发执行时安全聚合 Session 和 Trial 的终态。

### Scheduler Plugin

- 组合 typed `TrialPlan`、负载模型和 Turn 决策；公共 `pipeline.scheduling` 管理绝对时间时钟、admission、并发、worker、retry、stop/drain 和有界预取。
- 调度入口与 `TrialController` 交互；每个打开的 Trial 提供可重放的 `SessionStream` 和 `SessionRunner`。`TurnStrategy` 只通过 `TurnControl.send/retry/cancel` 和 `next_turn` 操作当前 Turn。
- 可以创建一个 Trial，或创建严格串行执行的多个 comparison/repetition Trial。
- 通过 `TrialController`、`SessionRunner` 和 `TurnControl` 的公开契约访问执行能力。

## Stage Plugin 规则

- 一个 Stage Plugin 完整替换一个 Stage 的能力。
- Plugin 接口按 Stage 定义在 `perf_benchmark/pipeline/contracts/<stage>.py`，跨阶段事实定义在 `facts.py`，并由 `perf_benchmark.pipeline.contracts` 正式导出，各 Stage 使用专属接口。
- 每个 Stage 是 Host + Plugin 深模块：Host 固定该阶段的生命周期、测量、资源和数据约束；Plugin 只替换业务策略。
- Metrics Plugin 在构造时接收解析设置并实现 `create_backend()`；公共 MetricsRuntime 接收冻结事实、管理批处理和结果发布，Backend 只计算指标与聚合。修改 Metrics 时阅读 `docs/development-guide.md` 的计算接口。
- Plugin 实现在 `perf_benchmark/plugins/<stage>/<plugin>/` 下。
- 各来源的 Plugin 均遵循相同的 Stage 能力契约。
- Plugin 使用 `from perf_benchmark.plugins import register_plugin` 和 `@register_plugin(stage=..., name=..., description=..., settings_parser=...)` 登记 class/factory。`settings_parser` 必须拒绝未知字段并返回类型化或规范化设置；Registry 深度冻结结果并把它传给 factory。实现通过 `perf_benchmark/plugins/__init__.py` 的 `create_registry()` 显式导入，注册入口统一为该目录。
- 默认目录只保存包含 `settings_parser + factory` 的冻结的 `PluginSpec`；`create_registry()` 返回独立字典副本，Coordinator 一次性创建八个冻结的 `StageBinding`。测试替换通过注入独立 `StageRegistry` 完成，默认目录保持稳定。
- Stage 名称、版本等元数据集中保存在 `PluginSpec`。
- 协议请求构造和响应解析保留在 Executor Plugin 内；Python 公共请求层拥有正文准备与释放，Rust 公共 Host 拥有 HTTP 传输、计时、SSE framing、取消和资源释放。
- 协议、调度和 Hook 的扩展遵循本指南定义的入口、取消恢复与资源释放契约。
- Scheduler Plugin 定义 retry、并发、速率和 admission 的策略与参数；绝对时间时钟、有界队列、gate、worker 和 drain 的通用执行机制统一复用 `pipeline.scheduling`。
- 公共生命周期事件对应明确的领域语义，辅助函数保留模块内部职责。
- 核心通过契约依赖 Plugin，各具体 Plugin 独立实现；两个以上实现复用，或承载生命周期/测量约束的代码归入 `pipeline` 或 `runtime` 并计入核心行数。
- 同一公开契约下扩展仓内实现，只修改插件目录、catalog 的一行显式导入、插件测试和可选示例。只有改变领域事实、生命周期状态机，或测量时间点、公式、单位、窗口、统计分母和 unavailable 语义时，才允许 contracts -> runtime -> metrics -> schema/reader 的受控纵向改动。

## Hook 规则

Hook 提供可选的受控拦截与观察能力。仅 `run.before_start` 和 `trial.before_start` 可以 inline 拦截；其余事件只能后台观察。

- 具体 Hook 放在 `perf_benchmark/hooks/`；契约、注册和加载集中在 `pipeline/hooks.py`，事件分发与后台 worker 放在 `runtime/events.py`。
- `run.before_start` 只允许修改 name 和 metadata。
- `trial.before_start` 允许修改 name、metadata 和 request_parameters；请求参数保存到 Trial.parameters.request_parameters，在创建 Executor runtime 前完成校验和隔离。并发、重试等调度参数仍由 Scheduler 管理。
- Session/Turn metadata 在准备阶段由 Normalization / Selector Plugin 生成；请求 URL、headers 和参数由 Executor Plugin 构造，可读取当前 Turn、Attempt 和 Trial 请求参数。具体协议负责应用 Trial 请求参数并校验输出约定；执行期间这些数据由对应 Stage 管理。
- inline 权限按固定事件白名单校验，非法模式和未知事件配置启动时立即失败。
- ID、parent ID、ordinal、status、measurements、并发和取消决策始终由底座或 Stage Plugin 管理。
- Fact Hook 通过后台 worker 观察冻结 snapshot。
- Background Hook 失败通过日志和计数报告，Run 状态继续由核心执行结果决定。
- 高频 Fact 队列过载时可丢弃 Hook 观察副本，核心 MeasurementFact 始终通过可靠队列完整交接。
- 全局 logging 由固定内核基础设施提供。
- 调度、retry、取消、持久化和指标公式分别由所属 Stage 与公共 Host 管理。

## 流式处理与性能规则

- Loader / Normalization / Selector 的批数据边界使用 `pyarrow.RecordBatch`；执行侧使用 `SessionStream`、`AttemptInput` / `PreparedAttempt` / `AttemptResult` 和 `MeasurementFact`，结果发布使用 `ComputedResultBatch` / `ResultReader`，具体契约见 `pipeline/contracts/`。Selector 可全量物化轻量 metadata，正文通过引用访问。
- Loader 负责加载 raw Arrow batch 和追加稳定 `source_order`；会话、轮次、时间语义和排序由 Normalization / Selector 管理。
- Loader 的批读取粒度只由 `batch_rows` 控制；数据源、split 和 cache 等配置仍由 Loader Plugin 管理。同步 Reader 每次通过一次 `asyncio.to_thread` 拉取一个完整 batch，读取粒度由 `batch_rows` 统一控制。
- 逐记录 Normalization 默认用 4 个 spawn 进程独立构造请求与画像，有界在途记录数等于 `PERF_BENCHMARK_PREPARATION_WORKERS`，按来源顺序回收；取消或失败终止并 join 全部进程。自定义整批 Normalization 在线程中保留其批状态。公共 Host 仍按序解析 token 参考值、序列化正文并直接追加到 Run 生命周期内的临时 BlobStore。
- 包入口在原生分词池初始化前默认设置 `RAYON_NUM_THREADS=4`，显式环境变量优先。批量编码/解码复用分词库的进程内 Rayon 池；`TOKENIZERS_PARALLELISM` 沿用显式环境变量和分词库的 fork 安全行为。聊天模板的 Python 渲染仍串行；调整池大小须重启进程。
- Normalization 定义 `session_id`、`turn_order_key` 和 `created_at`；Selector 执行排序、过滤、limit 和最终 ordinal。
- Normalized/Selected Arrow schema 只保存类型化 `PayloadRef`、画像与 metadata；正文统一保存在 BlobStore。
- PreparedCorpus 持有 sealed BlobStore 和冻结 plans，并为每个 Trial 重放 Session。
- Executor 将 BlobStore 的 read-only mmap slice 直接放入 `PreparedBody.segments`，PyO3 导出缓冲与 Bytes owner 保持只读正文存活，HTTP 发送服从 socket 反压。五协议由进程内 Rust 扩展执行；取消时等待连接任务及正文引用释放后再释放 Python view；修改该边界时阅读 docs/development-guide.md 的 Rust HTTP 执行契约。
- ExecutionEngine 独占 Attempt prepare/execute/release、Session/runtime close 和 continuation 生命周期，确保 release/close exactly once。
- Scheduler 只保留 `TrialPlan.resources.ready_capacity` 指定的有界 ready window；内置有并发上限的模式令其等于并发数，无并发上限的 open-loop 令其等于 pending request 上限。正文准备统一遵循该窗口容量。
- 接受 metadata 内存随 Turn 数线性增长；有并发上限 C 时，执行中及清理中最多 2C 个正文，加上最多 C 个 ready 正文，发送工作集约 `3 × C × request body`。
- Live continuation 只保存在当前 SessionRunner 私有内存，Session 结束立即释放；实测回复由当前 SessionRunner 管理，数据集提供的 recorded response 通过 `recorded_response_payload_ref` 引用。
- 取消时持久化已 admitted/active 的部分结果，预建的待执行对象保持 pending。
- DuckDB 和 SQLite 操作由后台线程执行，asyncio event loop 专注请求调度与收发。
- Metrics 与 Saver 的输入队列各固定为 8,192 条记录，按实际记录计数，容量独立于 Scheduler 配置；每批最多 100 条，首条到达后最多等待 100 ms 组批。
- 数据库 writer 必须由单线程独占；SQLite 每批提交事务。队列容量包含正在处理的记录；余量等待由完成、故障或关闭事件通知。`flush()` 等待排队、处理和结果转交；Saver 的 flush 还确认提交完成。
- 队列过载或 writer 故障必须使 Run 失败，保留已提交结果，并如实报告事实交接失败。
- 用户明确选择 `saver.settings.summary_only=true` 的容量测试只保留聚合指标、实际计数和有界错误分类，不保存 Session/Turn/Attempt 明细；逐请求批次不执行 SQLite 事务。关键汇总保存在小型结果库和 `.summary.json`，实时计数每10秒原子更新，最终分位数仍由标准 Metrics 计算。普通模式默认完整保留；汇总模式必须标明无逐请求回溯，读取器与封存计数使用 Trial 的真实完成计数。
- Session 启动持久化在请求发送前等待 Saver 队列余量，然后不再挂起地提交；A 大于8192时也不能仅因启动突发溢出。请求终态事实仍采用原有的非阻塞提交与过载失败语义。等待通知须先清除事件再检查队列条件，避免丢失后台线程的完成通知。
- Metrics writer 向 Saver writer 发布结果时，在 Metrics 后台线程等待足够容量，以最多100条的批次可靠交接；不阻塞事件循环、不增加队列上限、不丢弃记录。调度队列与并发占用变化在HTTP计时区间之外等待两级writer余量，保留变化发生时的原始时间戳；完成通知、故障与关闭都必须唤醒跨线程等待者。
- `PERF_BENCHMARK_PREPARED_CACHE` 可为固定本地 SWE JSON/Parquet、open-swe-traces、all 组合启用 sealed 准备复用。缓存身份覆盖源文件SHA256、准备设置、seed、实现和依赖版本；C/A、结果路径与Run身份不影响正文复用。正文直接写入命名文件，Arrow索引和manifest在准备及校验完成后原子发布，复用时只读mmap正文、校验文件状态和索引SHA256并重绑定Run身份。失败或取消的未完成准备不发布；缓存独立于结果和模型KV缓存，不自动清除旧版本，磁盘预算由部署入口检查。
- HTTP 接收时间戳在 transport 从客户端库取得原始 bytes 时立即记录，早于 JSON/SSE 解析和 Hook 分发；该时间戳表示应用从客户端库读取原始 bytes 的时刻。
- 压测正文由临时 BlobStore、当前请求的准备/传输/解码资源及当前 live Session 私有内存按生命周期持有并释放；领域对象、Hook、日志和 SQLite 只保存元数据或测量事实。离线命令的正文处理与输出遵循其独立契约。
- TTFT、ITL、TPOT 和 E2E 使用传输层原始时间戳计算，解析、Hook 和数据库在测量区间外处理。
- 全局日志的格式化、写流与 flush 统一通过后台线程异步执行。
- 进度日志按固定间隔读取已计算最终Turn的有界计数和标量累积，位于请求计时之外；完成量速率按实际日志间隔计算，均值保留有效样本数和单位，缺失数据保持unknown。正式结果仍由完整测量事实生成。
- SSE chunk、occupancy 等高频性能事件通过专属观察路径处理；关闭订阅时直接走省略 Event 分配的快速路径。
- Run 收尾等待已入队日志完整输出，日志 flush 位于请求测量区间之外。

## 指标与结果

- Runtime 和 transport 返回 AttemptResult 事实；Engine 收口状态，Metrics 负责 Turn 测量、公式和聚合语义。
- `TrialPlan` 明确保存 `StopPlan`、`ResourceLimits`、`LoadProfile`、`ComparisonPoint` / `RepetitionPlan`、request parameters 和 metadata；`trial.before_start` 只生成允许变更 name、metadata 和 request parameters 的 `EffectiveTrial`。
- Trial 收尾生成 typed `TrialFact`，显式保存 load profile、resource limits、comparison/repetition、offered/admitted/completed、drain、demand exhaustion 和终止原因；Metrics、Saver、Exporter 直接读取这些类型化事实。
- SQLite 写入 typed Trial facts、Attempt、Turn、画像、分组统计及场景/诊断事实，Parquet 封存保持相同身份和测量语义；schema 与 measurement contract 版本以 `perf_benchmark/plugins/saver/sqlite/schema.py` 为准，版本校验采用严格匹配。封存和清理须遵守 reader/writer 租约，跨容器共享协调目录。
- 修改请求画像、分组统计或离线 HTML 报告时，阅读 `docs/development-guide.md`，保持实测/估算/未知和最终 Turn/Attempt 的边界。
- 修改 Trace 传播、Observation 子进程、增量读取或运行中指标展示时，阅读 `docs/development-guide.md`。请求事实走 SQLite 可靠路径，Hook/IPC 只转交低频通知；可选 SDK 仅在启用对应 Observation 能力时加载。
- Attempt/Turn/Session 终态事实由公共 MetricsRuntime 隔离，Backend 返回按身份关联的指标，公共层组装 `ComputedResultBatch` 并发布给 Saver；Engine 保存 Session 启动状态，终态指标由 MetricsRuntime 发布。
- Saver Plugin 在构造时接收解析设置并实现 `create_backend(RunRecord, SaverContext)`；公共 `SaverRuntime` 统一 snapshot、8192 队列、批处理、单 writer、事务、flush/finalize/close、过载失败和部分结果保留，Backend 只同步处理 immutable record batches。
- 原始 response samples 在批内派生后释放；Trial 收尾聚合已有 Turn 结果的窗口与汇总。结果转交复用 writer 的异步消费循环。
- 八个 Stage 各自接收只包含必需服务的冻结窄 Context；Plugin settings 在构造时独立注入，原始 Run config 由 Coordinator 管理。
- Run 级别的分布指标和时间加权指标通过 DuckDB 完成最终计算。
- prefill/decode 的 `time_wm` 使用同一有效成功请求队列的 token 总量除以时长总量；prefill 使用 prompt/TTFT，decode 使用 `(completion_tokens - 1)/(last_ns - first_ns)`，聚合时统一秒单位。正 token 权重、正时长和可用的单请求速度共同限定资格，统计保存独立有效/合格/缺失样本数。`twm` 继续表示 token 加权算术平均，`am` 和分位数保留客户端接收分布。
- 时间加权统计的公式版本为 `client-speed-time-weighted-v1`；maxTPM 完成队列使用 `dataset-turn-maxtpm-speed-time-weighted-v1-completion-cohort`。整体窗口、实时桶和 maxTPM 复用分布聚合；summary 的速度主列使用 `*_time_weighted_mean`，进度日志使用 `decode_time_weighted_tok_s`。旧结果缺少该统计时，导出保持空白；原始事实可用于独立重算。
- 观测缺失的指标保留在指标目录中，并明确记录 availability 和 reason。
- SSE chunk / 输出事件按传输事实记录，token 数以响应 usage 为准；非流式响应的 TTFT、TPOT、ITL、prefill/decode throughput 标记为 `non_streaming_response`，非文本输出遵循 `non_text_output` 契约，并保留 availability 和 reason。
- Chat流的协议完成状态与内容长度分别判定：结构有效的终止choice及完整`[DONE]`允许空内容成功，JSON/API 错误与流完整性校验错误保留失败状态。零输出事件的首输出及decode时间点保持缺失，E2E与usage沿用已有观测；协议成功与内容质量分别记录。
- 指标定义、单位、权重或 measured duration 边界的变更须同步契约、schema、读取器及行为测试。
- `ResultReader` 是 Exporter 的读取边界，提供通用 Run/Trial/Metric/Comparison 视图和 `comparison_points(run_id)`。Exporter 从该读取器获取已保存结果并返回 `ExportResult`，由 Host 统一登记 artifact 和发送生命周期事件。
- SQLite 及派生结果保存脱敏的配置、请求事实和指标；凭据仅用于发送，原始 SSE chunk 在处理后释放。
- 事实字符串的 4,096 字符上限逐字段检查；多个合法字段组成的 JSON 证据按结构保存，序列化文本长度与单个事实字符串分别校验。
- SQLite schema 严格校验版本，版本冲突时立即失败。

## 仓库结构

```text
perf_benchmark/
  app/          CLI 和应用入口
  core/         领域模型、生命周期、日志和错误
  corpus/       离线负载分析、生成和比较
  hooks/        具体 Hook 实现
  pipeline/     契约、Coordinator、Context 和流式输入
  plugins/      按 Stage 组织的 Plugin 实现和显式能力注册表
  runtime/      取消、执行引擎、transport/runtime 支持
tests/
  <stage>/      各 Stage Plugin 的独立测试
  core/         领域模型和生命周期行为测试
  corpus/       离线负载工具行为测试
  pipeline/     Coordinator 和流式边界测试
  runtime/      执行、transport、取消和 writer 测试
  e2e/          基于临时小样本的端到端测试
examples/       静态、动态并发、多轮各一份配置，共用小样本
frontend/       React 工作台源码、依赖锁与测试
scripts/        前端构建及 Python 包发布入口
```

## 修改纪律

- 契约、实现、配置示例和行为测试必须同步更新。
- 每个 Stage Plugin 使用该阶段的 mock 输入进行独立测试。
- Scheduler Plugin 使用 mock `SessionRunner` 测试策略，HTTP 和数据库由各自模块的测试覆盖。
- Exporter 通过 `ResultReader` 测试已有事实的导出行为。
- 保持准确的 Stage 终态语义：每个 Stage 只能是 completed、failed 或 cancelled 之一。
- 性能变更以相同负载、相同测量口径的对照结果验收；涉及收流热路径或并发能力时检查客户端开销、尾延迟、完整事实和多次测量结果，沿用相同验收门槛。
- 取消时保留部分结果。
- 移动或替换接口时同步更新所有调用点，并清理被替换实现。
- commit、push、重写历史和删除分支须有用户明确授权。

## 验证

日常修改执行关键链路验收：

```bash
uv lock --check
PYTHON=.venv/bin/python bash scripts/test-smoke.sh
```

精简源码包保留核心 Python 回归测试及其公共夹具，另保留 Rust 与前端构建验证所需的测试。源码行数用于分类统计，执行链覆盖率门槛为 90%。完整开发仓库另含真实服务、外部数据集和容量验收测试；具备相应前置条件时，在完整开发仓库启用这些专项检查。
公共校验逻辑集中验证，各协议保留接入与协议特有行为测试。参数组合按独立行为选取代表场景。
合并前执行完整的 `uv run pytest`；远端同步使用上述关键链路验收。

修改 Executor 或公共请求准备/原生扩展生命周期时，执行 `uv run pytest --cov --cov-config=tests/executor/coverage.ini`；Python 执行链语句与分支综合覆盖率至少 90%，统计范围覆盖全部执行链生产文件和错误分支。

修改 Rust 执行扩展时，另外在 `perf_benchmark/` 下执行 `cargo test --workspace --locked`、`cargo test --workspace --release --locked`、`cargo fmt --all --check` 和 `cargo clippy --workspace --all-targets --locked -- -D warnings`。

wheel CI 将 PYO3_PYTHON 固定为当前虚拟环境解释器。Linux/macOS 的链接器使用该解释器基础目录下的 lib，动态加载路径限定在 Rust 测试步骤；Windows 将基础目录加入 PATH。TLS 测试使用独立证书配置。CI 覆盖 Python 3.11～3.14 及五个平台，个人分支通过 PR 执行矩阵校验，同一分支的新提交替换旧校验任务。

完成标准为相关行为测试通过、warning 检查通过，以及废弃接口和路径清理完成。


## 接口与测量速查

| 扩展 | 入口与公共边界 |
|---|---|
| Normalization | normalize_batch 接收 RecordBatch，生成 TurnDraft；正文入 BlobStore，metadata 保存引用 |
| Selector | select 接收轻量 Arrow metadata，返回 SelectionResult；排序与最终 ordinal 在这里完成 |
| Executor | create_runtime 接收 ExecutorTrial/ExecutorContext；prepare/execute/release、close_session/close 的生命周期由公共 Host 管理 |
| Scheduler | schedule 与 TrialController 交互；typed TrialPlan 声明负载、资源、停止条件及比较点 |
| Metrics | create_backend 计算冻结事实；公共 runtime 管理队列、隔离、发布；exporter 读取正式指标 |
| Saver | create_backend(RunRecord, SaverContext)；单 writer、事务、flush 与故障关闭由公共层负责 |
| Exporter | 从 ResultReader 派生 ExportResult；每个输出文件使用独立 artifact kind |

准确签名以 pipeline/contracts 和插件行为测试为准。插件需显式注册、拒绝未知 settings，
测试正常、失败、取消与资源释放；同一公共能力集中在公共 Host 中实现。
Rust transport 在接收 bytes 时记录时间，JSON/SSE 解析和数据库操作在请求计时区间之外处理。
保留 connect/send/body 原始异常 cause；空闲连接复用最多4秒，重试由 Scheduler 显式控制。
Chat timeout_seconds 接受 null 或有限正数，默认300秒；Python bridge 将 null 转为 Rust 的0秒哨兵，表示请求总时限由响应结束或主动取消决定。连接建立保留独立30秒时限，清理保留独立100ms边界，思考能力探测最多30秒。Rust 测试覆盖延迟流、有限总超时及主动取消。
执行异常与取消在返回前等待所属原生请求完成资源回收；正常结果继续使用后台收尾路径。
TTFT/TPOT/ITL 只有可观测的流式文本响应才能计算，chunk 表示传输事件，token 数以 usage 为准。

maxtpm 的 dataset_turn_count 取选中数据集的首遍轮数；活跃用户池由独立参数配置。
省略 metrics.settings.maxtpm_start_percent 时，dataset_session_count × 2 = target_concurrency × 3 则取53，其余情况取40；会话数来自最终选中数据，与活跃用户池分别记录。显式配置接受 [0, 100) 内的有限数值百分比并优先采用。按十进制比例精确计算 ceil(dataset_turn_count × P / 100)，通过首遍 terminal TURN 完成数定位起点，P=0 使用测试观测起点。布尔值、字符串和越界值在设置解析时拒绝。
从起点开始，in_flight 连续60秒低于 load_profile.target_concurrency 时，终点回溯到该段下降的起点，恢复到目标值即重新计时。同时间戳的并发变化按记录顺序取最终值。首遍完成或 Trial 结束限定观测上限；缺少目标请求并发或起点处的并发事实保持 unavailable。
成功最终 Turn 的 response_completed_at 归入连续60秒桶。maxTPM 公式版本为 dataset-turn-maxtpm-v8-auto-start-1p5x-underfill-60s-first-pass；诊断保存实际百分比、比例及 maxtpm_start_percent_source（configured / samples_1_5x_concurrency / default），现有 schema 和 ResultReader 按保存的窗口、诊断和公式版本读写，历史结果保留其原有统计边界。
重试增加 Attempt，每个最终 Turn 计一次轮次进度；吞吐取首遍 Session 的实际用量，完整零分钟保留，尾部窗口仅统计完整 60 秒。
maxtpm 的延迟、prefill/decode 分布使用完全相同的首遍成功完成队列和完整分钟边界；跨边界请求按完成时间归属，保留完整请求计时。样本合并计算分位数，沿用标准指标的公式、单位、有效/合格/缺失样本数与 availability 语义，独立公式版本标明完成队列；overall等窗口继续按请求开始时间选取延迟队列。
原始缓存字段缺失时保留未知，prompt 总量包含 cached 分项，二者按包含关系统计。收费使用 Decimal 与同窗实际 usage。
缓存总量和费用汇总已有可用数据，部分缺失时数值标记 availability=partial，报告显示 `*`；
数据全部缺失时保持 unavailable。费用总量相加已有可计费的输入、输出分项，缺失缓存保留未知；
observed_cost 仍只相加可完整计费的 Attempt。
summary 与 minutes 同时导出 maxTPM 窗口的缓存输入、未缓存输入、输出三项收入及总收入，单位为 currency/min。
三项共享成功最终 Turn 的完成时间队列、完整分钟边界与 Decimal 单价运算；有效分项独立保留数值和缺失原因。
summary 的三档单价、计价基数、币种和可选来源直接读取 Trial diagnostics.pricing；source 为冻结的非空文本元数据，费用计算继续使用原有单价与实际 usage。离线导出保留原价格，旧结果缺失的来源保持空白。
表格导出保留缺失数值空白及部分数据的星号，maxTPM 命中率、缓存输入收入和总收入列仅输出数值及单位。summary 展示成功请求数；失败/取消计数、成功率、流式设置和 Trial 身份由底层结果保留。其余 reason 列沿用缺失原因，availability 元数据由底层指标和 ResultReader 保存，用于正确显示缺失和 partial；Excel 星号使用数字格式，单元格保持数值类型。
应用Run配置必须显式提供单行case说明，作为Run证据保存并列在summary首列；低层 Coordinator 及历史库缺此证据时，描述列保留空白。Excel冻结首行与首列，首列按中英文显示宽度适配。静态与tabular导出共用文件锁和原子发布：同名子表先验证首列身份、唯一非空标题及行列上限，再按完整列名对齐追加。原有列和历史行保持原位，新指标列追加到末尾，缺失指标留空，单位属于列身份。缺少的子表创建，其余子表保留；校验失败保留原文件。含公式的追加数据要求列顺序保持原位。静态checkpoint只导出新增测试点，每次运行使用独立事实目录，支持共用工作簿及重复运行。
每个 CSV 与 XLSX 的 artifact kind 唯一。
默认 console 与 tabular（仅 XLSX），显式 tabular 设置替换默认实例。summary 直接读取 Trial overall
已存统计值，前置运行参数，末尾保留核心 maxtpm；全局分位数按正式指标导出。生成请求默认temperature=0.1、最多4,096 token并保留自然 EOS，显式参数优先（包括temperature=0）；Chat 显式配置 max_tokens=null 时省略默认输出上限，逐轮正整数目标继续优先，实际请求事实与画像的目标可为 null。固定输出仍要求显式目标。静态Prefill/Decode同样默认temperature=0.1，保留各自显式输出目标。
导出失败单独报告，已提交请求状态保持原值；导出命令可再次读取结果生成表格。

Executor 的 api_keys 从环境变量读取，通过 uniform/weighted 采样；数据集额外 headers 经公共校验、
BlobStore 与请求准备传递，鉴权值只用于发送，请求事实保存脱敏信息。鉴权配置统一使用 api_keys 字段。
Observation 使用 benchmark/observe/debug 三种 trace_mode 与独立 trace_export；请求执行链仅在启用 Trace
时加载对应可选 SDK。离线 Span 标注重建来源和传播状态，使用已保存的客户端时间点。
React 工作台从统一 ResultReader 读取正式指标；前端静态资源由构建命令生成，
源码交付保留 frontend 与 scripts，前端静态资源、依赖、wheel 和原生扩展由使用者在运行环境构建。
前端验证执行 npm run typecheck、npm test 与 npm run build；需要冻结结果或浏览器的 E2E 单独报告前置条件。

## 思考能力准备

示例 YAML 显式开启 `chat_template_kwargs.enable_thinking`，同时用 `reasoning_effort: auto` 选择档位。显式开启开关与 `auto` 组合时继续协商；关闭开关与单独指定开关保留调用方设置。Chat/Responses 的探测正文携带与正式请求相同的思考开关及模板参数，准备阶段和测量阶段保持同一思考配置。

应用入口在正式 Run 创建前为 Chat/Responses 协商默认思考档位，顺序为 max、xhigh、high；协议插件构造最小探测正文，应用层管理有界响应、鉴权、取消和错误分类。明确的思考字段或取值校验错误触发下一档，其他错误停止准备。显式配置优先。全部服务端点共同接受的档位写回配置后，再冻结条件、质量身份与原生运行证据。正式测量期间请求参数保持稳定，原生 Attempt、重试及计时边界沿用已有契约。

静态Decode在首个工作簿checkpoint前完成一次协商，整个矩阵共用选定档位。Prefill保持单token测量配置。探测状态和选定值保存在运行证据或workload.json中，准备请求的用量与费用由部署侧单独观察，正式指标仅聚合原生测试事实。

## 性能验证工具

大型性能实验、跨工具对比、诊断及真实平台专项测试由完整开发仓库管理。交付包的 `benchmarks/` 保留静态矩阵入口 `static_benchmark.py`，以及存储回归测试所依赖的四份辅助源码：`result_pipeline.py`、`result_storage.py`、`result_diagnostics.py`、`sqlite_parquet_comparison.py`。这四份辅助源码用于构造可复算的小型存储样本，由存储回归测试调用。

在完整开发仓库执行容量验收时，选择独占时段，构建和压测分开运行；保持相同正文、窗口、身份、资源采样及原有阈值。仅有本地 fixture 时如实报告该验证范围；完整容量验收须包含结果持久化和真实平台测量。

## 仓库交付范围

提交功能源码、行为测试、可运行的小型示例和这两份指南。实际模型测试结果、下载数据集、
生成包、进度记录、连接凭据、模型权重和构建缓存由本地或外部资源目录管理。
删除过时说明前将仍有效的契约或操作合并到指南；第三方许可证和来源声明保留。

完整开发仓库保留全部测试，源码交付用 `git archive` 并遵守 `.gitattributes` 的 `export-ignore` 清单，仅同步核心回归测试及其依赖。收件人获得 Python/Rust、前端源码和构建配置，在实际运行环境构建原生扩展、前端资源和安装包。

调度器的`SessionRunner.record_queue`为异步接口，所有生产、消费、stop和因果预留路径必须等待指标队列余量。队列变化的时间和值在等待前捕获，保留原始调度事实；该等待不位于HTTP计时区间。高活跃会话验收必须覆盖真实producer/worker队列事件，不能只测试Saver会话记录突发。
