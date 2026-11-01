# perf-benchmark 使用与部署指南

本文件介绍工具运行、测试模式、数据集配置、指标统计和结果导出。配置统一使用 **YAML**。
从第 2 节复制完整配置，再按服务地址、数据集和测试模式修改对应部分。

## 1. 安装、启动、指定配置

需要 Python 3.11+、uv 和 Rust；通过 rustup 安装仓库指定的 Rust toolchain。在仓库根目录运行：

```bash
uv sync --locked
uv run perf-benchmark --help
```

仓库提供静态固定并发、动态并发扫描和多轮对话三份通用 YAML 示例，共用一份小型 ShareGPT 样本；另有[静态 Prefill/Decode 矩阵示例](../examples/static-benchmark.yaml)，使用本地 tokenizer 构造输入。准备实际模型服务与匹配的 tokenizer 后，修改示例中的路径和服务参数：

```bash
uv run perf-benchmark run examples/source-only-openai.yaml
```

检查通过后，填入模型服务地址，指定 YAML 启动正式测试：

```bash
uv run perf-benchmark run /absolute/configs/run.yaml
```

相对路径以 **YAML 所在目录** 为基准，家目录路径可以使用 `~`。
每次运行填写一个新的 SQLite 文件路径。按 Ctrl-C 取消测试并等待结果收尾。

各类测试默认在请求测量前，用最长2秒的只读HTTP探测记录服务KV的token容量；Excel的 `kv_capacity` 表保留每个独立GPU/CPU池、采集时间、来源及未知原因，summary列出可完整统计的总容量。每个独立逻辑缓存池计一次容量。SGLang支持server-info和HiCache总量；新版 `attn_dp_size` 表示独立attention池，`dp_size` 表示完整模型副本；当前自动采集支持单模型副本与独立attention池，多副本部署使用实测快照记录完整布局。vLLM的block预算单独标记，完整模型及LMCache容量缺少布局时保留未知。部署端实测快照可通过 `application.service_capacity.snapshot` 引用，格式见 [静态测试说明](static-benchmark-guide.md)。多服务路由应提供覆盖独立池与共享CPU池的部署快照。`application.service_capacity.enabled: false` 可关闭采集；`timeout_seconds` 的取值范围为 `(0, 10]` 秒。

容量快照包含 `model`、带时区的 `observed_at` 和 `pools`，可选 `engine` 取值为 `sglang`、`vllm` 或 `unknown`；省略时记录为 `unknown`。导入快照沿用其中的引擎标识、采集时间与池容量，直接用于报告。

SGLang的HiCache容量按server-info中的attention-DP拓扑归并；同一DP内的TP与PP观测代表同一个逻辑token池。不同DP分别计入容量，各副本报告冲突或拓扑缺失时保留未知状态。

## 2. 一份可以直接修改的配置

保存为仓库根目录 `my-run.yaml`，替换服务地址、模型和 tokenizer。`case` 必填，用一行描述测试场景；它保存在原生事实中，并始终位于Excel首列。Excel冻结首列与标题行，首列按中英文字符宽度自适应（Excel列宽上限255）：

```yaml
name: my-benchmark
case: GLM5.3单轮16并发吞吐与延迟测试
seed: 42
pipeline:
  loader:
    plugin: hf-datasets
    settings:
      dataset: json
      data_files: examples/data/sharegpt.jsonl
      split: train
  normalization:
    plugin: sharegpt
    settings:
      tokenizer_path: /absolute/path/to/model-tokenizer
      multi_turn: false
  selector:
    plugin: all
    settings: {}
  executor:
    plugin: openai-chat
    settings:
      base_url: http://127.0.0.1:30000
      model: your-served-model-name
      stream: true
      timeout_seconds: 300
      extra_body:
        temperature: 0.1
  scheduler:
    plugin: single-turn-closed-loop
    settings:
      max_concurrency: 16
  metrics:
    plugin: standard
    settings:
      maxtpm: true
      # maxtpm_start_percent: 45 # 显式覆盖自动起点。
  saver:
    plugin: sqlite
    settings:
      path: results/run-001.db
  exporter: {}
```

```bash
uv run perf-benchmark run my-run.yaml
```

此配置读取全部数据集会话，每会话只取第一轮，在途请求上限为 16，数据跑一遍后结束。实际并发取决于可用会话数。
先用仓库样例跑通流程，正式性能测试换成足够大的数据集，并填写被测模型对应的 tokenizer。

### 必填字段与可选配置

| 字段 | 要求 / 默认 |
| --- | --- |
| `pipeline` 的八个 stage | `loader/normalization/selector/executor/scheduler/metrics/saver/exporter` 都要提供；`exporter: {}` 默认输出终端摘要并导出 Excel |
| 各 stage 的 `plugin` | 除使用默认 exporter 外，显式选择上例的插件 |
| `loader.settings.dataset` | 必填；本地 JSON/JSONL 用 `json`，Parquet 用 `parquet` |
| `loader.settings.data_files` | 本地文件 builder 必填；单路径、路径列表、目录或 glob |
| `loader.settings.split` | 必填；本地 JSON 通常填 `train`，Hub 数据集填实际 split，例如 `test` |
| `normalization.settings.tokenizer_path` | 用于计算 token 长度；已声明完整 token 信息的数据可以省略 |
| `selector.settings` | `all` 用 `{}` 保留全部；`token-length` 必须同时提供 `input_tokens_gt/output_tokens_lt/limit`，见第 5 节 |
| `executor.settings.base_url` 或 `base_urls` | 二选一；填写协议、IP/DNS 和端口，单实例用 `base_url`，多实例用 `base_urls` |
| `executor.settings.model` | 必填，填写服务的 served model name，可通过 `/v1/models` 查询 |
| `scheduler.settings` | 当前模式所需的并发数或速率，见下文各模式 |
| `saver.settings.path` | 必填，每次运行使用新的 SQLite 文件路径 |
| `name`, `seed` | 选填；建议写清测试名和固定 seed（seed 默认 0） |
| `stream` / `timeout_seconds` | 选填，默认 `true` / 300 秒；Chat 的 `timeout_seconds: null` 使请求持续到响应结束，正数设置一次 HTTP 请求尝试的总超时秒数；使用 `stream: true` 测量 TTFT/ITL |
| `extra_body` | 填 `temperature/top_p` 等请求参数；生成默认 `temperature: 0.1`、最多4,096 token并保留自然 EOS；生成方式见 3.1 |
| `metrics.settings.maxtpm` | 选填，默认 `false`，启用首遍达到指定进度至实际并发持续低于目标值前的完整分钟均值 |
| `metrics.settings.maxtpm_start_percent` | 选填；选中会话数恰为请求并发的 1.5 倍时自动取 `53`，其余情况取 `40`；显式数值优先，范围 `[0, 100)`，支持小数，`0` 表示从测试观测起点开始 |
| `metrics.settings.pricing` | 选填，填写后启用费用统计 |
| `observation`, `logging`, `hooks` | 选填，按需启用监控、日志和扩展 |

### 准备与分词并行度

逐记录 Normalization 默认使用 4 个进程构造请求和画像，适用于带 token 参考计数和需要分词的数据。按来源顺序收回结果，最多保留 4 个在途记录，保持会话、轮次及抽样规则一致。`PERF_BENCHMARK_PREPARATION_WORKERS` 可设置进程数，例如 `PERF_BENCHMARK_PREPARATION_WORKERS=8 uv run perf-benchmark run /absolute/configs/run.yaml`。正文按序追加到 Run 生命周期内的临时 BlobStore，收尾时释放。自定义整批 Normalization 保留其有状态批处理契约。

数据准备阶段的批量分词默认使用 **4 个原生 CPU 线程**，共用进程内的分词线程池。批量文本计数、聊天模板渲染后的批量编码和批量解码复用同一池；返回顺序、token 计数和样本选择规则保持一致。聊天模板的 Python 渲染部分仍串行执行，实际 CPU 核心使用由操作系统调度。

需要调整并行度时，在启动命令前设置 `RAYON_NUM_THREADS`：

```bash
# 默认 4 个分词线程
uv run perf-benchmark run examples/source-only-openai.yaml

# 改为 8 个分词线程
RAYON_NUM_THREADS=8 uv run perf-benchmark run examples/source-only-openai.yaml

# 单线程分词
RAYON_NUM_THREADS=1 uv run perf-benchmark run examples/source-only-openai.yaml
```

该设置也适用于 `dataset` 离线命令，并沿用显式环境变量设置。原生线程池在首次批量分词时初始化，调整并行度需要重新启动进程；将工具嵌入已有 Python 进程时，在该进程首次使用原生分词前设置环境变量。`TOKENIZERS_PARALLELISM=false` 可显式禁用并行，默认保留分词库自身的 fork 安全行为。

分词并行度用于准备输入，与 YAML 中的模型请求并发及活跃用户数分别配置。数据集已提供有效 token 计数时，继续沿用数据计数，直接复用已有 token 计数。

## 3. IP、端口、模型、多实例与鉴权

```yaml
pipeline:
  executor:
    plugin: openai-chat
    settings:
      base_url: http://10.0.0.8:30001
      endpoint: /v1/chat/completions
      model: replace-with-served-model-name
      api_keys:
      - env: PERF_TARGET_API_KEY
      api_key_sampling: uniform
      extra_headers:
        X-Data-Parallel-Rank: '0'
```

以上是替换 `executor` 的片段。请求地址由 `base_url` 与 `endpoint` 拼接：
上例得到 `http://10.0.0.8:30001/v1/chat/completions`。
若 `base_url` 填为 `http://10.0.0.8:30001/v1`，对应的 `endpoint` 填 `/chat/completions`。
模型名可先查询服务的 `/v1/models`。有鉴权时把 Key 放入 `PERF_TARGET_API_KEY` 环境变量。
Docker 内 `127.0.0.1` 指容器自身；远程服务填容器实际可达的 IP 或域名。

多个 Key 用 `api_keys` 列表提供不同环境变量名，`api_key_sampling: uniform` 均匀抽样；
使用 `weighted` 时，每项填写非负 `weight`，所有权重之和必须大于零。数据集可以提供 `headers` 对象作为本轮额外 HTTP Header；
Host、Content-Type 和 Content-Length 由传输层统一管理。鉴权值只在环境变量和请求发送阶段使用，结果、日志与报表保存脱敏信息。

多个实例用下面片段替换 `executor.settings`，保留其它请求配置：

```yaml
base_urls:
- http://10.0.0.8:30001
- http://10.0.0.8:31001
base_url_session_block_size: 16
model: replace-with-served-model-name
dp_rank_count: 8
```

`base_url_session_block_size` 将每连续 16 个会话分到同一实例；省略时轮转分配。
自动分配 DP rank 时填写 `dp_rank_count`；固定 DP rank 时在 `extra_headers` 填写 `X-Data-Parallel-Rank`，两种方式选一种。

### 3.1 自然生成、限制输出与忽略 EOS

| 方式 | 请求设置 | 结束方式 |
| --- | --- | --- |
| 自然输出，默认限长 | 省略输出上限配置时，工具发送4,096 token上限 | 遇到 EOS 提前结束，最多输出4,096 token |
| Chat 使用服务端输出设置 | 显式填写 `executor.settings.max_tokens: null`，样本仅提供参考输出长度 | 请求省略输出上限，按服务端上下文与 EOS 设置结束 |
| 主动限制最多输出 token 数 | 显式配置 `max_tokens: N` 或逐轮 `requested_output_tokens: N` | 遇到 EOS 可以提前结束，最多输出 N token |
| 主动忽略 EOS 强制输出 | 指定正整数目标，并配置 `extra_body.ignore_eos: true`，或使用 `output_policy: fixed` 数据 | 忽略 EOS，生成到指定目标；仍受服务端上下文与其他停止条件影响 |

默认生成上限为4,096 token，逐轮显式上限优先于配置上限，配置上限优先于默认值；实际输出数量以响应 usage 为准。参考输出长度用于数据画像，生成目标由请求参数指定。
Chat 的显式 `max_tokens: null` 省略工具的默认输出上限；逐轮正整数 `requested_output_tokens` 继续优先。对应请求事实和画像中的输出目标为 null，响应 usage 记录实际生成数量。
Chat、Completions、Responses的默认采样温度为 `temperature: 0.1`；显式 `extra_body.temperature` 和 Trial 请求参数优先，显式0同样保留。采样温度、输出上限和停止条件分别配置。采样温度用于 Chat、Completions 和 Responses 的生成请求，Embedding/Rerank 使用各自协议参数。

**自然生成（默认）**：在 `executor.settings` 中填写采样参数即可：

```yaml
extra_body:
  temperature: 0.1
  top_p: 0.9
```

三份示例均采用自然生成：Chat/Completions请求默认发送 `max_tokens: 4096`，Responses请求默认发送 `max_output_tokens: 4096`。
默认保留EOS，模型可以提前结束；固定输出和忽略EOS仍要求显式目标。
例如模型在 180 token 时结束，实际输出就是 180 token。
普通 ShareGPT、GSM8K、OpenQA 数据集的参考输出长度用于数据画像。
需要主动选择限长测试时，可使用下面两种显式设置：

| 上限来源 | 填写方式 | 生效范围 |
| --- | --- | --- |
| 配置统一上限 | `executor.settings.max_tokens: 4096` | 使用配置上限的各轮请求 |
| 数据集逐轮上限 | 数据记录填写 `requested_output_tokens: 256` | 该轮优先使用 256 作为上限 |

`requested_output_tokens` 同样表示上限；保留 EOS 时，模型可以提前结束。
统一采用 YAML 中的上限时，使用仅包含参考长度信息的数据记录。
LongBench 的 `normalization.settings.output_tokens` 也是可选的显式逐轮输出上限；省略时使用配置或默认上限并保留自然EOS。静态prefill的一token测量及显式固定输出目标保持原有设置。

**固定输出长度压测**：对支持 `ignore_eos` 扩展的服务，在 `executor.settings` 中填写：

```yaml
max_tokens: 256
extra_body:
  temperature: 0
  ignore_eos: true
```

服务忽略 EOS，按请求的 token 上限继续生成。测试后用响应 usage 查看实际输出 token 数。
离线生成测试数据时，填写 `output_policy: fixed`，生成的数据携带逐轮目标长度与固定输出标记，
执行器自动设置 `ignore_eos: true`；填写 `output_policy: natural` 则保留自然结束行为。
离线生成命令与配置入口见第 9 节。

### 3.2 默认思考档位

`perf-benchmark run` 的 Chat 与 Responses，以及静态 Decode 测试，默认在正式测量前按 `max → xhigh → high` 协商思考档位。服务明确返回当前思考字段或取值校验错误时，依次尝试下一档；鉴权、限流、超时、连接及其他错误直接报告。三档均被拒绝时停止准备。多个 Chat 服务地址共同选择全部端点接受的最高档位。
协商依据服务返回的字段校验和支持值信息，适用于各类模型及模板；显式配置的档位保持原值。

准备探测使用独立短请求，可能产生少量服务用量；正式统计和计费仅包含测试请求。选定档位冻结在运行配置中，运行证据保存各次探测的档位与HTTP状态，Excel的生成参数保存实际选择值。服务接受参数表示协议校验通过，模型执行效果由部署实现决定。正式测量期间沿用已冻结的请求设置和正常失败处理。

Chat在 `executor.settings.extra_body` 中配置 `reasoning_effort`；省略或 `auto` 使用上述顺序。示例 YAML 同时配置 `chat_template_kwargs.enable_thinking: true` 和 `reasoning_effort: auto`，开启思考模式并协商最高可用档位。探测和正式请求携带相同的思考开关及模板参数。

```yaml
extra_body:
  reasoning_effort: auto
  chat_template_kwargs:
    enable_thinking: true
```

显式字符串（例如 `high`、`low`）固定发送该值，`null` 省略该字段并沿用服务端设置。显式关闭开关优先于 `auto`；单独配置思考开关时沿用该设置。`thinking`、`enable_thinking`、`chat_template_kwargs.thinking` 与 `chat_template_kwargs.enable_thinking` 均参与开关识别。服务采用其他思考协议时，按服务约定替换开关，例如 `thinking: {type: enabled}` 或 `chat_template_kwargs: {thinking: true}`；标准 API 的思考模式可由 `reasoning_effort` 控制。实际思考行为以服务端实现及响应为准。

Responses使用 `extra_body.reasoning.effort`，同样支持 `auto`、显式字符串及 `null`。静态测试对应 `decode.reasoning_effort`。Embedding、Rerank和Completions继续沿用各自的协议配置。

## 4. 选择测试方法

下面的 `scheduler` 段替换最小配置中 **`pipeline.scheduler`**，其它 stage 保留。

| 模式 | 插件 | 必填设置 |
| --- | --- | --- |
| 单轮固定并发 | `single-turn-closed-loop` | `max_concurrency` |
| 多轮固定并发 | `multi-turn-closed-loop` | `max_active_sessions`, `max_concurrency` |
| 单轮固定到达率 | `single-turn-open-loop` | `request_rate`, `max_pending_requests` |
| 多轮双到达率 | `multi-turn-open-loop` | `session_rate`, `request_rate`, `max_pending_sessions`, `max_pending_requests` |
| 逐档扫描 | 上面任一插件 + `sweep.values` | 一个可扫描轴、档位列表、其余模式必填设置 |
| 自动 SLO 搜索 | 上面任一插件 + `sweep.search` | 搜索范围/预算、SLO 规则、其余模式必填设置 |
| 按时长持续压测 | 上面任一插件 + `stop.duration_s` | 明确数据耗尽策略 |

### 4.1 单轮：指定某个并发

```yaml
scheduler:
  plugin: single-turn-closed-loop
  settings:
    max_concurrency: 32
```

最多同时 32 个请求；请求结束才释放名额。省略 `stop` 时跑完选中的数据集一次。
使用多轮原始数据做单轮测试时，填写 `multi_turn: false`，每个对话取第一轮。

### 4.2 多轮：用户池与实际请求并发

```yaml
normalization:
  plugin: sharegpt
  settings:
    tokenizer_path: /absolute/tokenizer
    multi_turn: true
    live_continuation: true
selector:
  plugin: all
  settings: {}
scheduler:
  plugin: multi-turn-closed-loop
  settings:
    max_active_sessions: 128
    max_concurrency: 32
    stop:
      duration_s: 1200
    on_exhausted: cycle
```

| 概念 | 上例 | 含义 |
| --- | --- | --- |
| 数据来源 | `selector.plugin: all` | 所有数据集对话模板，供运行时持续补充用户池 |
| 用户池 / 同时激活会话上限 | `max_active_sessions: 128` | 池中最多 128 个未结束对话，包括等候请求名额的会话 |
| 同时在途请求上限 | `max_concurrency: 32` | 同时处于执行中的 HTTP 请求 |
| 运行时长 | `duration_s: 1200` | 到时停止准入，等待已发请求收尾；单请求超时另填 `timeout_seconds` |

上例从数据集补入最多 128 个活跃对话，其中最多 32 个请求同时发送；
某个对话结束后（轮次用完，或请求失败等原因使对话提前结束），释放其活跃名额，
从数据集取下一个补入池中，直到停止条件或数据耗尽。
每个请求结束就释放一个请求名额，调度器持续安排下一个待发请求。
同一会话的轮次始终串行，所以真实在途请求数还受活跃会话和轮间依赖限制。
闭环填写 `max_active_sessions >= max_concurrency`；实际请求并发随待发请求数量变化。
`cycle` 在数据集全部用尽后重用模板，每次创建独立的新会话。
跑完整个数据集一次时，省略 `stop` 与 `on_exhausted`。

- `live_continuation: true`：下一轮上下文接上模型实际生成的回答。
- `false`：下一轮上下文用数据集里的 recorded 回答，适合可复现回放。
- `multi_turn: false`：只测试每个对话的第一轮。

### 4.3 单轮：固定请求到达率（开环）

```yaml
scheduler:
  plugin: single-turn-open-loop
  settings:
    request_rate: 20
    arrival_distribution: constant
    max_concurrency: 64
    max_pending_requests: 256
    stop:
      duration_s: 600
    on_exhausted: cycle
```

目标每秒到达 20 个请求，`constant` 等间隔，`poisson` 为泊松到达。
`max_concurrency` 选填，用于指定 HTTP 并发上限；省略时由到达率和 pending 容量控制负载。
`max_pending_requests` 是待执行请求容量，必须填；服务慢或客户端受限时实际发送率可能低于目标。

### 4.4 多轮：分别指定会话和请求到达率

```yaml
scheduler:
  plugin: multi-turn-open-loop
  settings:
    session_rate: 2
    request_rate: 20
    arrival_distribution: poisson
    max_active_sessions: 128
    max_concurrency: 32
    max_pending_sessions: 256
    max_pending_requests: 512
    stop:
      duration_s: 1200
    on_exhausted: cycle
```

目标每秒新进 2 个会话、每秒 20 个请求，两套时钟独立；同一会话仍串行。
`max_active_sessions/max_concurrency` 选填；两种 pending 容量必填。
配上 4.2 的 `normalization` 开启多轮；`max_active_sessions` 指定运行时用户池。

### 4.5 并发扫描（每档独立、串行测试）

```yaml
scheduler:
  plugin: single-turn-closed-loop
  settings:
    sweep:
      axis: max_concurrency
      values:
      - 1
      - 4
      - 8
      - 16
      - 32
    repetitions: 2
    stop:
      session_count: 1000
    on_exhausted: cycle
```

每档跑 1000 个会话、重复两次，总计 10 个 Trial。
扫描的字段填在 `sweep.axis`，其余固定参数填在 `settings`。
固定档位使用 `values`，自动搜索使用 `search`，两种方式选一种。
各插件支持的扫描轴：

| 插件 | 可扫描轴 |
| --- | --- |
| 单轮闭环 | `max_concurrency` |
| 多轮闭环 | `max_concurrency`, `max_active_sessions` |
| 单轮开环 | `request_rate`, `max_concurrency` |
| 多轮开环 | `session_rate`, `request_rate`, `max_active_sessions`, `max_concurrency` |

多轮扫 `max_concurrency` 时，固定 `max_active_sessions >= 最高请求并发`；
多轮扫 `max_active_sessions` 时，固定 `max_concurrency <= 最低活跃会话档位`。
开环扫速率仍需填 pending 容量和其它固定速率。

### 4.6 自动找到满足 SLO 的并发边界

```yaml
scheduler:
  plugin: single-turn-closed-loop
  settings:
    sweep:
      axis: max_concurrency
      search:
        min: 1
        max: 128
        initial: 8
        resolution: 1
        max_trials: 30
        timeout_s: null
      slo:
        window: overall
        min_samples: 100
        rules:
        - metric: ttft
          statistic: p99
          op: le
          value: 60s
        - metric: success_rate
          statistic: value
          op: ge
          value: 0.99
    repetitions: 1
    stop:
      session_count: 1000
    on_exhausted: cycle
```

六个 `search` 字段都必填；`min <= initial <= max`。并发轴为整数，速率轴可用小数。

| 字段 | 填写内容 | 上例 |
| --- | --- | --- |
| `min` | 搜索的最低压力值 | 并发 1 |
| `max` | 搜索的最高压力值 | 并发 128 |
| `initial` | 第一个测试档位 | 并发 8 |
| `resolution` | 通过与失败边界的目标间距 | 相差 1 个并发 |
| `max_trials` | 总测试次数预算，包含重复与最终确认 | 30 次 |
| `timeout_s` | 填正数设置整次搜索的秒数预算；填 `null` 由 SLO 边界或次数预算决定结束 | 无总时限 |

通过时增加压力，失败时缩小区间，接近 `resolution` 后复测确认。
`max_trials` 包含重复与最终确认；`timeout_s: 7200` 设置整次搜索最多 2 小时，
`timeout_s: null` 让搜索按 SLO 边界或 `max_trials` 结束。单档时长和请求超时分别填写。
SLO 可以组合 `ttft/tpot/e2e/decode_throughput/success_rate/workload_total` 等已支持指标，规则全部通过才算通过。
`op` 是 `le`（≤）或 `ge`（≥）；延迟建议带单位，如 `60s`、`50ms`。
动态示例组合 mean TTFT ≤ 1 秒、P90 TTFT ≤ 2 秒、最小请求 decode 速度 ≥ 20 token/s
和成功率 ≥ 0.99。`decode_throughput` 的 `statistic: min` 检查有有效 decode 测量的请求下界；
完整判定要求达到 `min_samples`，失败和缺失情况结合成功率及指标 availability/reason 查看。
`window` 默认 `overall`，也可用 `drop_first_20pct`、`steady_occupancy` 等指标窗口；
启用 `maxtpm` 后可选 `maxtpm` 窗口的 `maxtpm.value`（单位 tok/min，阈值用数值）。

多轮搜索把插件换成 `multi-turn-closed-loop`，增加固定 `max_active_sessions: 128`，
配上 4.2 的多轮数据段。若要搜索活跃用户数量，改轴为 `max_active_sessions`，
将活跃用户数配置移到 `sweep`，并填写固定 `max_concurrency`；最低搜索用户数填写为大于或等于此并发。

多轮搜索请求并发时，在 `settings.max_active_sessions` 填写用户池大小。
例如填写 128，搜索并发 16、32、64 时，各档都使用 128 个活跃会话名额。

搜索结束后查看 SLO 结果：

| 终态 | 含义 |
| --- | --- |
| `boundary_confirmed` | 通过与失败边界已复测确认 |
| `upper_limit_passed` | 配置的最高档复测通过 |
| `indeterminate` | 判定所需的指标或样本量条件尚待满足 |
| `unstable` | 复测结果波动或单调性冲突 |
| `budget_exhausted` | 时间或 Trial 预算用尽 |

### 4.7 长时间持续测试、停止条件

| 运行方式 | scheduler `settings` 填法 | 结束时机 |
| --- | --- | --- |
| 数据集跑完即停 | 省略 `stop` 和 `on_exhausted` | 所有选中会话运行一次并收尾 |
| 按时长持续压测 | `stop.duration_s: 3600` + `on_exhausted: cycle` | 数据耗尽后循环补入新会话，到时停止发新请求 |
| 到时或耗尽即停 | `stop.duration_s: 3600` + `on_exhausted: stop` | 时长到达或数据耗尽，取先发生者 |

在所选模式 `settings` 中增加：

```yaml
stop:
  duration_s: 3600
on_exhausted: cycle
```

或改为固定会话数量：

```yaml
stop:
  session_count: 1000
on_exhausted: stop
```

停止条件在 `duration_s` 和 `session_count` 中选一个；`session_count` 按对话会话数计数。
时长到达后停止准入新会话和发送后续请求，等待已发出的请求收尾；实际墙钟时长可能超过此值。
显式配置 `stop` 时必须配置 `on_exhausted`：`cycle` 循环数据，`stop` 耗尽就提前结束，
`error` 耗尽视为错误。省略两者默认跑完整个选中数据集一次。
缓存隔离由 `executor.settings.cache_salt_enabled` 控制，默认 `false`。
填 `true` 时，每个 Trial 正式首轮和后续 cycle 使用独立 cache_salt，同一 cycle 的会话共享；
填 `false` 保留服务端前缀缓存复用。对比测试采用相同的缓存配置。
长压测只是延长 `duration_s`，仍使用上面的四种模式。

### 4.8 配置时间范围

按需要分别填写以下字段：

| 字段 | 时间范围 | 到时行为 |
| --- | --- | --- |
| `executor.settings.timeout_seconds` | 单次 HTTP 请求尝试，从发起到响应结束，包含连接、等待首 token 和流式输出 | 该次请求记为超时失败；启用重试时，每次重试重新计时 |
| `scheduler.settings.stop.duration_s` | 一个测试档位（一次 Trial）的发压时长；固定并发、固定速率、单轮和多轮模式都适用 | 停止准入新会话及发送后续请求，等待已经发出的请求收尾 |
| `scheduler.settings.sweep.search.timeout_s` | 填正数限制整次自动 SLO 搜索，累计所有档位、重复与边界确认；填 `null` 由 SLO 边界或次数预算决定结束 | 秒数预算到时取消搜索及当前测试，结果标记为 `budget_exhausted` / `timeout` |
| SLO 规则中的 `value: 60s` | 指标通过阈值，例如 TTFT p99 ≤ 60 秒 | 用于判定该档的 SLO 状态；请求执行时间由 `timeout_seconds` 控制 |

例如，填写请求超时 300 秒、单档时长 600 秒、搜索预算 7200 秒：每个档位发压 10 分钟，
一次请求最多执行 5 分钟，自动搜索累计最多运行 2 小时后进入取消收尾。
单档发压结束后的请求收尾、结果保存和监控退出仍需要时间，所以进程总时长可能超过所填预算。
扫多个档位时，每个档位及每次重复各自重新计算 `stop.duration_s`；多轮会话中的每一轮请求各自重新计算 `timeout_seconds`。

Chat Completions 可配置 `executor.settings.timeout_seconds: null`，单次请求持续到响应结束。
连接建立保留 30 秒超时，主动取消继续生效；正式测量前的思考能力探测最多等待 30 秒。
填正数时按该秒数限制单次请求总耗时，省略时为 300 秒。发压时长、搜索预算与单请求超时分别配置。

## 5. 切换数据集

切换数据集时，依次填写 **loader 文件格式、normalization 数据结构、scheduler 单/多轮模式**。

| 输入结构 | normalization.plugin | 必需数据字段 / 专用配置 |
| --- | --- | --- |
| ShareGPT | `sharegpt` | `id`, `conversations: [{from: human, value: ...}, {from: gpt, value: ...}]`，交替成对；支持单轮/recorded/live |
| GSM8K | `gsm8k` | `question`, `answer`；单轮 |
| OpenQA | `openqa` | `question`, `chatgpt_answers: [答案]`；单轮 |
| 项目 LongBench 格式 | `longbench_v1` | `question`；`output_tokens` 可选，省略时采用配置或4,096 token默认上限并保留自然 EOS；单轮；将原始数据整理为此字段结构 |
| OpenAI 请求快照 | `openai-messages` | `id`, `token_name`, `created`, `messages`；填写 `multi_turn: true` 按 token_name 做 recorded 多轮回放 |
| LongAlpaca | `longalpaca` | `instruction`, `output`；单轮 |

JSON/JSONL 示例：

```yaml
loader:
  plugin: hf-datasets
  settings:
    dataset: json
    data_files: /absolute/data/gsm8k-test.jsonl
    split: train
normalization:
  plugin: gsm8k
  settings:
    tokenizer_path: /absolute/tokenizer
```

Parquet 换 `dataset: parquet`、`data_files: /absolute/data/*.parquet`。
多个文件可写列表。支持本地目录和 Hub dataset：Hub 使用 `dataset: openai/gsm8k`、
`split: test`、`load_kwargs: {name: main, revision: <固定版本>}`，省略 `data_files`。
正式可复现测量使用提前下载并固定版本的本地文件。
CSV/text/Arrow 文件填写对应的 loader 格式，并将列名整理为所选 normalization 的字段结构。

自己的多轮最小 JSONL（每行一个完整 JSON 对象）：

```json
{"id":"user-1","conversations":[{"from":"human","value":"介绍缓存的作用。"},{"from":"gpt","value":"缓存用于复用数据。"},{"from":"human","value":"举个例子。"},{"from":"gpt","value":"复用已经计算的前缀。"}]}
```

在每个 `gpt` 消息填写 `requested_output_tokens` 可逐轮指定输出目标，例如 270、300、330；
记录顶层的 `requested_output_tokens` 则统一指定整条会话。

用 `sharegpt + multi_turn: true`。数据集里有多个 id 才形成多个用户模板；
运行时用户池由 `max_active_sessions` 指定。一次回放的实际池大小取配置上限与可用会话数量中的较小值；
`on_exhausted: cycle` 则可重用模板补充新会话。需要生成匹配 token 长度的随机/真实数据，
用 `perf-benchmark dataset generate --help` 查询离线生成入口。


### 限制选中的数据集总量

在 `selector` 中填写长度条件与 `limit`，选择指定数量的对话模板：

```yaml
selector:
  plugin: token-length
  settings:
    input_tokens_gt: 0
    output_tokens_lt: 1000
    limit: 128
```

三个字段都必填。每轮估计输入长度必须大于 `input_tokens_gt`，会话至少有一个已知参考输出长度，
且所有已知参考输出长度的最大值小于 `output_tokens_lt`。`limit` 填写为合格会话数量以内的正整数。
保留全部数据用 `all`。
在 `scheduler.settings` 中分别填写 `max_active_sessions` 设置用户池、`max_concurrency` 设置请求并发。

## 6. maxtpm 与每分钟收入

统计参数位于 `pipeline.metrics.settings`，在现有 YAML 的 `pipeline` 中配置：

```yaml
pipeline:
  metrics:
    plugin: standard
    settings:
      maxtpm: true
      # maxtpm_start_percent: 45 # 显式覆盖自动起点。
      pricing:
        currency: CNY
        per_tokens: 1000000
        input_price: 2.0
        output_price: 8.0
        cached_input_price: 0.2
```

按服务价格填写 `pricing`，上例为演示价格。同时填写 `maxtpm: true` 和 `pricing` 开启每分钟收入；省略整个 `pricing` 段关闭价格统计。
三个价格必填且可为 0；`currency` 默认 USD，`per_tokens` 默认每百万 token。
可选 `source` 填写非空的价目表来源或价格说明，作为冻结的计价依据保存到结果中。

### maxTPM 统计区间

省略 `maxtpm_start_percent` 时，选中会话数恰为预设请求并发的 1.5 倍，自动取 `53`，其余情况取 `40`。
例如选中 384 个会话、请求并发 256 时取 53%；选中 384 个会话、请求并发 192 时取 40%。
会话数由 loader 的 `split` 和 selector 的最终选择共同决定；`max_active_sessions` 单独控制活跃用户池。
显式配置 `maxtpm_start_percent` 时按该值统计，范围 `[0, 100)`，支持小数。
例如 `25` 表示 25%，`12.5` 表示 12.5%。选中数据集共有 T 轮请求，配置值为 P 时，
首遍第 `ceil(T×P/100)` 轮完成时开始，按完成时间排序；`P=0` 从测试观测起点开始。
从起点开始，使用调度器记录的实际在途请求数与 `target_concurrency` 比较。实际并发连续 60 秒低于预设并发时，
终点回溯到这段下降的起点；期间恢复到预设并发会重新计时。判定使用预设并发值本身。
首遍完成或 Trial 提前结束时，实际观测终点作为区间上限。缺少目标请求并发或起点处的并发记录时，窗口保留 unavailable。
每个实际结束的轮次计一次进度（包括失败/取消），每个轮次的重试合并计一次进度；吞吐仅累计区间内首遍成功最终 Turn 的实际用量。
在选定区间连续切分 60 秒窗口，
按响应完成时刻归桶，TPM = 输入 token（包含缓存）+ 输出 token，均值是所有完整分钟的算术平均。
零吞吐分钟也计入；尾部按实际时长记录，均值采用完整 60 秒窗口。
Trial 收尾时按已记录的响应时间计算，SQLite 保存每分钟 `maxtpm:minute:000001` 等窗口及
`maxtpm` 汇总，终端显示均值、分钟数和逐分钟表。
同一 `maxtpm` 窗口也统计 TTFT、TPOT、E2E、ITL、prefill/decode 单请求速度，以及整体输出吞吐和成功请求数。
这些延迟和速度只采用同窗内完成的首遍成功最终 Turn，包含窗口前发出、窗口内完成的请求，
排除窗口内发出但窗口后完成的请求、失败请求、重试 Attempt 和循环 Session。
延迟取完整请求的真实时间；均值和分位数从同窗全部合格请求的完整测量样本合并计算。
Excel summary 的 `maxtpm_ttft_*`、`maxtpm_tpot_*`、`maxtpm_e2e_*`、`maxtpm_prefill_throughput_*`、
`maxtpm_decode_throughput_*` 等列列出 mean/P50/P90/P99、有效样本 n 和合格请求数 eligible_n；
`maxtpm_workload_completion` 为同窗总输出 token/s，区别于单请求 decode tok/s。完整统计及缺失原因保存在原生 SQLite / 同身份 Parquet 中。
缺少完整分钟时，这些窗口指标保留 unavailable；非流式、单 token 或缺少输出事件等情况沿用各指标的有效样本与缺失原因。
运行结果在首遍达到配置的起点比例、并发观测可用、区间内包含完整分钟且 usage 有效时显示数值，其余情况显示 `unavailable`。
进度按首遍全部轮次完成数量计算；循环测试沿用首遍轮次定位起点及限定统计队列。
测试执行由 `scheduler.settings.stop` 及数据耗尽规则控制；maxTPM 区间用于吞吐、收入和同窗性能统计，全局 summary 指标使用整体窗口。
SQLite / 同身份 Parquet 保存 `maxtpm_start_percent`、`maxtpm_start_percent_source`、起点比例、实际边界轮数和时间戳、目标并发、持续下降的确认时刻及结束原因；来源分别为显式配置 `configured`、1.5 倍规则 `samples_1_5x_concurrency` 和常规默认 `default`。CSV/Excel 分钟表保存实际完整窗口。

### 每分钟收入

对上面每个完整分钟的成功请求，按服务返回的实际 token 用量计算：

```text
每分钟收入 = ((输入 token − 缓存命中 token) × input_price
            + 缓存命中 token × cached_input_price
            + 输出 token × output_price) / per_tokens
```

`revenue_per_minute` 是这些完整分钟收入的算术平均，单位为 `currency/min`；
例如 `CNY/min`。收入与 maxtpm 使用相同的请求、完成时间和分钟窗口，零请求分钟的收入为 0。

summary 在总收入旁列出三项计费贡献：`cached_input_revenue_per_minute` 为缓存输入收入，
`input_revenue_per_minute` 为未缓存输入收入，`output_revenue_per_minute` 为输出收入，
单位均为 `currency/min`。三项分别使用缓存、输入、输出单价；用量完整时三项之和等于总收入。
输入按服务 usage 的 `prompt_tokens - cached_tokens` 计算，缓存按 `cached_tokens` 计算，
输出按 `completion_tokens` 计算。这些数值表示计费贡献，GPU实际prefill和decode工作由性能指标与引擎采样描述。
这里的收入表示配置销售单价对应的 token 收入。
收入数值列始终保留，观测缺失的用量以空白及原因表示，实际观测到的零用量以数值 0 表示。
带 `*` 的数值表示部分观测，或经过独立验收的有效请求子集；Excel 沿用现有收入列与底层数值类型。
子集收入使用原 maxTPM 窗口、时长和同一有效请求集合，数值表示该子集的收入。
覆盖率、剔除原因与原始 SHA 保存在独立验收证据中，原生结果按原记录保存。

| 字段 | 结果含义 |
|---|---|
| `maxtpm` | 每分钟输入（含缓存）加输出 token 的均值 |
| `maxtpm_input_tpm` / `maxtpm_output_tpm` | 同窗每分钟输入 / 输出 token 均值 |
| `maxtpm_cached_tpm` / `maxtpm_uncached_tpm` | 同窗每分钟缓存命中 / 未命中输入 token 均值 |
| `maxtpm_cache_hit_rate` | 同窗缓存命中 token 总量 ÷ 输入 token 总量，0–1 |
| `revenue_per_minute` | 同窗平均每分钟收入 |
| `input_revenue_per_minute` | 未命中输入的平均每分钟收入 |
| `cached_input_revenue_per_minute` | 缓存命中输入的平均每分钟收入 |
| `output_revenue_per_minute` | 输出的平均每分钟收入 |
| `maxtpm_minutes` | 纳入均值的完整分钟数 |

Excel/CSV 中的 `maxtpm` 按 **百万 token/分钟** 展示（单位 `百万token/M`，M 指分钟），
数值为原生 maxTPM 除以 1,000,000。数据库保留原生 token/分钟值，窗口和收入按原生规则计算；
其他输入/输出速率继续按各列明确标注的单位读取。

**vLLM / SGLang 用量采集**：`openai-chat` 读取每次响应的 `usage.prompt_tokens`、
`usage.completion_tokens` 和 `usage.prompt_tokens_details.cached_tokens`；
兼容网关的 `usage.cached_tokens`，嵌套字段优先。流式请求默认发送
`stream_options: {include_usage: true}`；填写 `stream: true` 即自动请求 usage：

```yaml
executor:
  plugin: openai-chat
  settings:
    base_url: http://127.0.0.1:8000
    model: your-served-model
    stream: true
```

非流式填写 `stream: false`，用量从 JSON 响应读取。
在服务启动命令中追加缓存用量报告开关：

- vLLM：`--enable-prompt-tokens-details`。
- SGLang：`--enable-cache-report`。

服务端开启缓存用量报告后，
缓存命中随 usage 保存到每个 Attempt 和最终 Turn，独立于价格配置。
缓存总量汇总已有的服务端 usage；部分请求缺失缓存字段时仍显示已知总量，并在报告数值后标 `*`。
所有请求都缺失该字段时显示未知。命中率及每分钟收入仍要求对应窗口数据完整。
输入价格和缓存价格相同时，可以计算总收入；缓存分项仍保留未知。

另有 `overall` 窗口的 `total_cost` 保存全测试所有 Attempt 的金额，包含重试及有有效 usage 的失败/取消请求；
`total_cost` 有部分用量缺失时汇总已有可计费的输入、输出分项，报告数值后标 `*`，缓存缺失状态保留为未知。
`observed_cost` 另保存可完整计费的 Attempt 金额。每分钟收入采用成功的最终 Turn，用于对应 maxtpm 的吞吐产出。

将本节的 `metrics` 与 `exporter` 片段填入第 2 节的完整 YAML 即可启用计费与 maxtpm。

## 7. 查看结果与更多参数

### 运行进度日志

普通单轮与多轮测试默认每30秒输出一行进度，运行和Trial开始、结束时另输出摘要。
日志记录已计算的最终Turn成功/失败/取消数、已完成会话与目标会话数、活跃会话、在途请求，
以及累计成功请求的TTFT均值（秒）、TPOT均值（毫秒）、E2E均值（秒）与时间加权decode速度（token/s，字段为 `decode_time_weighted_tok_s`）。
`latency_samples`依次给出这四项的有效样本数，缺失观测显示`unknown`。
`completed_output_tok_s`按两次日志之间完成的成功最终Turn输出量除以实际间隔计算；
长请求输出中的token在请求完成、服务端usage到达后归入该预览。重试只按最终Turn计一次。
日志标注`preview=true`，正式结果由SQLite/Parquet中的完整事实生成。

```yaml
logging:
  level: INFO
  stream: stderr
  progress_interval_seconds: 30
```

间隔使用正秒数，`0`关闭进度摘要；`WARNING`日志级别也可只保留告警。
交互终端的`--tui`提供原地刷新的实时面板，后台运行可直接保存上述低频文本日志。

测试结束终端输出指标和 SQLite 位置。退出码 0 成功、1 Run 失败/取消、2 配置/启动错误。
读取已有 SQLite 结果生成 HTML：

```bash
uv run perf-benchmark report /absolute/results/run-001.db --output /absolute/results/run-001.html
```

### CSV / Excel 导出

每次运行默认输出终端摘要并生成一份 `results.xlsx`，`exporter: {}` 即可。CSV 通过 `formats: [csv, xlsx]` 显式开启。
输出目录自动取 SQLite 同级的 `<数据库文件名去掉扩展名>-tables/`，例如
`/absolute/results/run-001.db` 对应 `/absolute/results/run-001-tables/results.xlsx`。
需要自定义目录或额外 CSV 时，在 YAML 的 `pipeline.exporter` 中填写：

```yaml
exporter:
  plugins:
  - plugin: tabular
    settings:
      output: results/tables
      formats:
      - xlsx
```

`output` 填输出目录，相对路径按 YAML 所在目录解析；`formats` 可填 `[csv]`、`[xlsx]` 或两者。
可另设 `excel_path: results/glm-comparison.xlsx` 指定Excel文件路径。首次导出创建文件；追加导出时，同名子表按完整列名对齐后追加本次Run的数据。原有列顺序和历史行保留，新指标列追加到末尾，各次测试缺失的指标保持空白。列名中的单位参与匹配，单位不同的指标分别保存。首列标题须一致，所有标题须为唯一的非空文本；校验失败时原文件保留。缺少的子表自动创建，其余子表保留。数值类型、部分可用标记和单元格样式沿用原生导出。该设置只控制Excel，CSV仍写到 `output`。

静态 Prefill、Decode 和多轮对话可以共用一个工作簿：把静态配置的 `output.excel_path` 与多轮配置中 `pipeline.exporter.plugins` 列表内 tabular 的 `settings.excel_path` 指向同一个文件。静态结果写入 `Prefill性能测试`、`Decode性能测试` 等子表；多轮结果写入 `summary`、`minutes`、`requests`，采集服务容量时另附 `kv_capacity`。每次导出在文件锁内校验并原子追加，case 标识各次测试。相对路径分别以各自 YAML 所在目录解析。

```yaml
exporter:
  plugins:
  - plugin: tabular
    settings:
      excel_path: results/glm-comparison.xlsx
      formats: [xlsx]
```
默认只导出 Excel；显式填写 `[csv, xlsx]` 会同时生成 `summary.csv`、`minutes.csv`、`requests.csv`，采集服务容量时另附 `kv_capacity.csv`，可以同 HTML 导出器一起配置。

常规性能测试建议保持 `exporter: {}`，保存原生 SQLite/Parquet 与一份 Excel。
HTML 和 CSV 通过 `report`、`export` 按需从已保存的原生结果生成。
已结束测试可以先预览精简清单，再执行：

```bash
uv run perf-benchmark results compact --root /absolute/results
uv run perf-benchmark results compact --root /absolute/results --apply
```

该命令验证终态数据库和保留的 Excel，移除可重导出的 `report.html`、`summary.csv`、`minutes.csv`、`requests.csv` 及旧版 `metrics.csv`；
相同 `selected.jsonl` 按 SHA256 共享硬链接，原加载路径保持可用。SQLite/Parquet、配置、
Excel 和历史明文日志保留。精简对象限于已结束、WAL 为空且通过完整性验证的结果。
封存清单及同步回执随精简记录更新，明细仍遵守七天保留规则。
仅显式添加 `--compress-metrics` 才将已关闭的指标观测日志无损压缩为 gzip。

| 文件 / Excel 工作表 | 内容 |
|---|---|
| `summary.csv` / `summary` | 每个测试配置一行：前几列为模型、实际平均输入/输出 token、并发、活跃用户数和生成参数；后面是全局延迟、长度及会话指标的 mean/P50/P90/P99，prefill/decode 速度的 time_weighted_mean/P50/P90/P99，成功请求数、吞吐、token 总量及费用；末尾为 maxtpm、输入/输出 TPM、分钟数、命中率、收入及同窗延迟和prefill/decode速度、样本数与输出吞吐 |
| `minutes.csv` / `minutes` | 同一窗口内每个完整分钟的 token 用量、命中率和收入，保留零请求分钟 |
| `requests.csv` / `requests` | 每次 Attempt 及最终 Turn 的实际输入、输出、缓存命中/未命中 token、命中率、状态和响应完成时间 |
Excel/CSV 的导出表为 `summary`、`minutes`、`requests` 及可选的 `kv_capacity`。全部测试窗口的原始指标、统计量、单位及缺失原因保留在 SQLite / 同身份 Parquet。

Excel 文件为 `results.xlsx`，包含上面三张表；配置服务容量时另含 `kv_capacity`。`scope` 区分 `attempt` 与 `turn`，
汇总请求用量时选择一种范围。命中率为 0–1 的数值，未知值留空。maxTPM 命中率、缓存输入收入和总收入仅展示数值及单位；完整缺失原因、失败/取消计数、成功率、流式设置和 Trial 身份保存在 SQLite / 同身份 Parquet。其余保留的 reason 列说明对应指标缺失原因，原始指标继续保存可用性元数据。
`*` 表示该汇总存在数据缺失。Excel 使用数字格式显示星号，单元格仍是数值；CSV 在数值后追加星号。
`summary` 的全局值直接读取同一 Trial 的 `overall` 指标窗口，分位数按全局请求样本统计。
延迟、长度和会话指标的 `mean` 对应 `am`；prefill/decode 速度的主汇总列为 `prefill_throughput_time_weighted_mean`、`decode_throughput_time_weighted_mean`，对应 `time_wm`。速度的 P50/P90/P99 仍按单请求样本计算。TTFT/E2E 单位为秒，TPOT/ITL 为毫秒，速度为 token/s，单位也保留在列中。
生成参数来自已保存的配置、Trial 覆盖参数和请求画像，只输出已知的生成字段；缺失设置由服务端决定。
`summary` 自动省略在当前 Run 全部 Trial 中均为空的列，CSV 与 Excel 使用相同列集。
任一 Trial 有值就保留该列，包括数值 0；有内容的缺失原因仍会导出。
指标目录及 `minutes`、`requests` 保留相关明细，未知数值单元格留空并注明原因。
maxtpm 的三项收入同时展示在 summary 与 minutes，完整计费与缓存指标保存在 SQLite / 同身份 Parquet。
summary 同时展示该 Trial 冻结的 `input_price`、`cached_input_price`、`output_price`、`per_tokens`、`currency` 及已填写的 `pricing_source`。
单价保持数值，Excel 标题标明币种与计价 token 数；`per_tokens` 列保留实际计价基数。
多 Trial 使用各自已保存的价格，离线导出沿用同一依据；旧结果缺失的价格来源留空。

已有 SQLite 结果也可单独导出：

```bash
uv run perf-benchmark export /absolute/results/run-001.db --output /absolute/results/tables --formats csv xlsx
```

单轮、多轮、开环、闭环、并发扫描和自动 SLO 搜索的配置片段见第 4 节。
Docker、监控与 GPU 操作见下文。


开发契约、模块职责和验证命令见 [开发指南](development-guide.md)。

## 8. 快照与协议

OpenAI 快照按 `token_name` 分组，并按记录时间排序回放。
请求保留 system、tools、tool_calls 与 tool 结果；工具信息作为录制上下文发送。
recorded 回放使用数据中的下一份完整上下文；live continuation 追加当前轮实际回答。
记录的 token 长度是参考值，实际吞吐和收入始终使用响应 usage。

`open-swe-traces` 使用空的 normalization `settings: {}`，直接采用录制请求上下文。
将第 2 节的 normalization 改为 `plugin: open-swe-traces`、`settings: {}`，并指定轨迹数据文件。
准备阶段构造每轮完整请求，
完整保留录制上下文、reasoning 和工具声明；请求长度处理交给服务端，超长请求失败如实保存。
SWE 工具定义中的 `name`、`description`、`input_schema` 在准备阶段转换为 OpenAI 的
`type: function`、`function.name`、`function.description`、`function.parameters`。
已有 OpenAI 工具定义沿用原值，原始 JSONL、消息轨迹和参考 token 计数保持原样。
默认最多生成4,096 token并保留自然 EOS；显式声明的正整数 `requested_output_tokens` 按原值发送。
Chat 请求收到完整响应及正常终止标记后按成功统计。`finish_reason: length` 表示达到输出上限，
仅含思考内容的正常响应沿用该判定；停止原因保存在请求明细中，吞吐和计费采用响应 usage。
数据集预计算输入/输出计数用于描述参考分词器下的数据画像；实测吞吐和收入采用服务端响应 usage。

数据集全量逐轮均值与 maxTPM 有效窗口均值分别描述各自的请求队列。持续增长的录制历史
使后段窗口偏向较长输入。压缩轨迹长度时，可从各会话尾部随机裁掉若干完整轮次，
保留从首轮开始的连续消息前缀，同时截取对应的输入和参考输出计数数组。
会话轮数和最终深度保留样本之间的差异，读取沿用现有轨迹格式。离线调度模拟用于分布敏感性检查，实测验收采用同窗
服务端 `usage.prompt_tokens`。token 加权缓存命中率为同窗 cached token 总量除以
prompt token 总量，分子分母均来自响应 usage。

`tool` 结果通过 `tool_call_id` 关联前一条 assistant 的工具调用。
默认 `missing_tool_call_id: preserve` 沿用数据中的关联字段；要求字符串占位字段的协议解析器可配置：

```yaml
normalization:
  plugin: open-swe-traces
  settings:
    missing_tool_call_id: empty
```

该模式给缺失或 null 的 `tool_call_id` 补空字符串，以满足要求字符串字段的协议解析器；
已有 ID、工具调用、参数、reasoning、结果正文及顺序保持原样，并行调用与结果沿用轨迹原有关系。
空字符串仅作为协议字段占位；要求非空 ID 或按 ID 渲染内容的服务，需要具有真实关联 ID 的数据。
启用前应在实际模型模板上验证修复前后的编码完全一致，并验证目标 Router 接受该请求。
每轮 metadata 记录 `tool_call_id_policy` 和该轮历史中的 `tool_call_id_placeholders` 数量。
处理仅作用于内存请求，原数据文件和统计公式保持原样，token 用量沿用数据集参考值与服务端实测 usage。

要求调用与结果 ID 对应的服务可选 `missing_tool_call_id: match-single`。该模式维护当前待响应的调用集合，
在结果 ID 缺失、null 或空字符串，且仅有一个待响应调用时，填入该调用已有的真实 ID。
已有非空 ID 按值匹配，显式 ID 的并行结果支持按记录顺序返回。
多个待响应调用遇到缺失结果 ID、调用 ID 缺失、ID 冲突或关联中断时，准备阶段明确报错。
每轮 metadata 的 `tool_call_id_repairs` 记录历史中修复的结果数；正文、调用参数与会话顺序保持原值。
原始 JSONL 与数据集参考计数继续作为冻结来源，实际输入、输出、缓存和计费采用响应 usage。


| 协议 | Executor 插件 | 测量边界 |
|---|---|---|
| Chat Completions | `openai-chat`，见第 14 节示例 | 聊天消息、recorded/live；可用流式时间点 |
| Completions | `openai-completions` 执行器 | 文本快照；默认自然结束，显式目标时校验固定输出长度 |
| Responses | `openai-responses` | 原生 input/tools 快照；每轮发送完整录制上下文 |
| Embeddings | `openai-embeddings` | float 向量、请求延迟、输入用量；无生成时间指标 |
| Rerank | `rerank` | `/v1/rerank`、请求延迟、成功率；无生成时间指标 |

非流式 TTFT/TPOT/ITL 标注缺失原因；SSE chunk 按传输事件记录，输出 token 数取自服务端 usage。

Chat Completions流包含结构有效的终止choice（对象类型的`delta`和已识别的`finish_reason`），并以`[DONE]`完整结束时，空内容也记录为API请求成功。过滤省略内容或达到生成上限等终止原因单独保留，内容质量由对应评估判定。零输出事件时TTFT、TPOT和prefill/decode速度保持未知；E2E与服务端usage继续按实际观测保存。格式错误、API错误对象、缺少有效终止choice的空流以及截断流保留各自协议失败状态。

### 8.1 客户端时间与引擎监控

客户端使用同一单调纳秒时钟记录请求发送、首个输出事件、末个输出事件和响应结束。
TTFT为请求发送到首输出的时间；E2E为请求发送到响应结束的时间。
E2E等于TTFT、首末输出间隔、响应收尾时间三项之和。
TPOT为首末输出间隔除以`completion_tokens - 1`，单请求decode速度使用相同两项的比值倒数。
整体输出吞吐为成功请求的输出token总量除以完整测量时长。decode 时间加权速度为有效请求的 `Σ(completion_tokens - 1) / Σ(末输出时间 - 首输出时间)`；prefill 时间加权速度为 `Σprompt_tokens / ΣTTFT`，各时间项统一使用秒。每项公式的分子和分母采用同一有效请求队列。

进度日志的 `decode_time_weighted_tok_s` 展示当前 Trial 累计成功请求的时间加权速度，`completed_output_tok_s` 展示本次日志间隔内完成请求的总输出吞吐。summary 的全局及 maxTPM 速度主列、Web 的时间加权均值和实时桶采用相同公式及各自窗口队列。有效 token 用量与正时间间隔共同决定样本资格，缺失观测保留 unknown 或空白，并保存对应原因和样本数。

单请求速度及其算术平均、P50/P90/P99 描述客户端接收过程。服务端批量发送、代理缓冲或网络集中交付可以使首末输出间隔很短，对应速度值很高；这些值适合结合 TTFT、TPOT 和原始时间点分析交付行为。GPU 内部生成速度由引擎执行时间与实际计算 token 描述。SQLite / 同身份 Parquet 保留 `am`、`twm` 和 `time_wm`：`twm` 为 token 加权算术平均，`time_wm` 为时间加权平均。历史结果的时间加权统计可由原始事实重算，独立导出直接读取已保存统计。

`prefill_throughput`表示完整输入token除以客户端TTFT，包含缓存复用、排队、传输与首输出的影响。
GPU纯prefill工作量使用引擎报告的实际计算token与对应执行时间描述。
`itl`保存相邻输出事件间隔；投机解码或SSE批量发送时，一个事件可以包含多个token。
TPOT使用服务返回的总输出token数，适合对照这种批量输出情况。

引擎Prometheus计数器按具体版本的更新位置和标签解释。SGLang v0.5.21的
`prompt_tokens_total`、`generation_tokens_total`在请求完成时更新，输入包含缓存；
`cached_tokens_total`按device、host、storage来源记录。vLLM v0.31按engine迭代更新
输入和输出计数，`prompt_tokens_cached_total`包含本地缓存与外部KV载入。
多DP汇总按独立engine计一次；全局计数器沿用全局身份，累计直方图使用sum/count或bucket分位数。
perf计费采用响应usage与客户端完成时间队列，Prometheus原始采样用于独立对照。

TTFT、请求延迟与整体输出吞吐参考[NVIDIA GenAI-Perf](https://github.com/triton-inference-server/perf_analyzer/blob/main/genai-perf/README.md)的客户端口径。
[vLLM基准实现](https://github.com/vllm-project/vllm/blob/main/vllm/benchmarks/lib/endpoint_request_func.py)将末输出事件作为latency终点，
对应本工具的`time_to_last_output`；本工具E2E另包含响应收尾，`response_drain`保存该段时间。maxTPM采用本工具明确约定的
首遍可配置进度起点（按样本与并发比例自动取 53% 或 40%，显式配置优先）、并发连续60秒低于目标值的动态终点和完整分钟均值；跨窗口请求的整条usage归入完成时刻，原始事件和监控采样保留各自时间身份。

## 9. 离线数据与实验

在 Run 之外准备和检查数据：

```bash
uv run perf-benchmark dataset inspect /absolute/configs/run.yaml
uv run perf-benchmark dataset generate --help
uv run perf-benchmark dataset stats --help
uv run perf-benchmark dataset compare --help
uv run perf-benchmark dataset prepare-trace --help
```

inspect 报告字段与选择前后规模；正式字段映射由 YAML 显式指定，数据正文保存在输入文件。
stats/generate/compare 使用各自 YAML；通过对应命令的 `--help` 查询入口。
generate 支持 random、seed、profile、string、tokenid。
生成包包含 Parquet、画像、manifest，临时发布目录通过原子 rename 转为正式输出。
系统缓存 `perf-benchmark/corpus/v1` 按配置、tokenizer 指纹与来源文件身份复用；
`.generation-cache.yaml` 记录缓存及校验值。离线生成缓存、服务端 KV cache 和 Run SQLite 分别管理。
string/tokenid 的采样长度用于生成语料；实际聊天模板长度在准备阶段重新计算，超出显式上下文预算时报告失败。
prepare-trace 先离线生成完整请求正文，再用 timeline/causal 模式回放；
具体参数用 `dataset prepare-trace --help` 查询。

```bash
uv run perf-benchmark experiment /absolute/configs/serial-experiment.yaml
uv run perf-benchmark compare /absolute/results/baseline.sqlite /absolute/results/candidate.sqlite --config /absolute/configs/comparison.yaml --output /absolute/results/comparison.html
```

experiment 严格串行执行已有 Run YAML，每次生成独立结果目录与 SQLite；失败后再次执行创建新 Run。
compare 检查数据、筛选、tokenizer、seed、协议、生成、部署和缓存条件，以及指标单位与可用性。
回归判定要求比较条件已知且变化符合配置；单次比较报告描述差异，统计显著性需要重复实验支撑。
`application.quality` 只导入已有评测报告，`application.preparation` 仅用于明确独占的服务。
服务准备和正式请求分别使用独立 cache salt，缓存隔离效果通过后端观测确认；预热使用独立记录。
完整 Run YAML 与负载参数见第 2–4 节；`experiment --help` 和 `compare --help` 列出应用入口参数。

## 10. 结果位置与空间

结果位置由 YAML 的 `pipeline.saver.settings.path` 指定。
相对路径按 YAML 目录解析；三份使用示例默认落在 `examples/debug/results/`。
正式任务建议指定独立绝对路径。
默认保存 SQLite 和一份 Excel；Excel 位于 SQLite 同级的 `<数据库名>-tables/results.xlsx`。
显式配置 exporter 的 `output` 可覆盖表格目录；report/export 也可在事后从 SQLite 派生。

仓库根目录执行以下示例时，默认结果位置为：

| 配置 | 数据库 | Excel |
| --- | --- | --- |
| `examples/source-only-openai.yaml` | `examples/debug/results/source-only-001.db` | `examples/debug/results/source-only-001-tables/results.xlsx` |
| `examples/concurrency-sweep.yaml` | `examples/debug/results/concurrency-sweep-001.db` | `examples/debug/results/concurrency-sweep-001-tables/results.xlsx` |
| `examples/multi-turn.yaml` | `examples/debug/results/multi-turn-001.db` | `examples/debug/results/multi-turn-001-tables/results.xlsx` |

SQLite 保存请求事实、指标及必要元数据；请求正文使用生命周期内的临时存储，鉴权来自环境变量。
requests 表格保存 Attempt 和最终 Turn 明细，行数随实际请求数量增长；完整原始指标保存在 SQLite / 同身份 Parquet。汇总用量时选择
requests 中的 attempt 或 turn 一种 scope。运行中的事实写入 SQLite，收尾后封存 Parquet；
统一 ResultReader 自动路由结果，派生 CSV/XLSX 可按需重导出。

同级 `<数据库文件名>.archive/summary/` 长期保存汇总、趋势与比较；`details/` 和原 SQLite
从 `Run.finished_at` 起保留七天。明细过期后，汇总仍可浏览；已导出的 Excel/CSV 是独立文件。
备份或同步结果时同时保留 SQLite、封存目录和已导出的表格。活动读取与 writer 使用协调租约，
跨容器必须共享缓存根下的 `perf-benchmark/result-sealing` 协调目录，详见下节。

运行期间保持 SQLite/WAL 的位置与内容稳定。writer 关闭后再备份数据库与结果文件；schema 校验采用严格版本匹配。
当前版本号以 `perf_benchmark/plugins/saver/sqlite/schema.py` 为准。

## 11. Observation 与交互

```bash
uv run --extra observation perf-benchmark run /absolute/configs/run.yaml --tui
uv run --extra observation perf-benchmark observe /absolute/results/run-001.db
uv run perf-benchmark results serve --root /absolute/results --config-root /absolute/configs --host 127.0.0.1 --port 8088
uv run perf-benchmark tui --url http://127.0.0.1:8088
```

Run TUI 的 q 只关闭界面，Ctrl-C 取消 Run。results serve 读取已提交结果；配置目录可提供受控的
新 Run 启动入口。服务依据自身启动记录校验进程身份，并取消匹配的进程。
Observation 运行在独立进程，`observation.enabled: true` 开启基础观察。
`trace_mode: benchmark` 为默认，使用基础请求测量；`observe` 按 `sample_rate` 采样，默认 0.01；
`debug` 全采样。`trace_export: false` 可以保留构建而关闭网络导出，OTLP endpoint 单独配置。
离线重建反映已保存的客户端事实，服务端埋点由服务端观测提供；输出事件和队列均有上限，丢弃数如实记录。
引擎/GPU targets 只是采集关联，
HTTP 发压地址仍是 executor.base_url。共享实例的服务器计数用于关联分析，请求事实以客户端测量为准。
Observation 开销须在同负载下对照验证；缺失样本保留 unknown。

源码启动浏览器工作台前先在 `frontend/` 执行 `npm ci --omit=peer` 和 `npm run build`。
打开 `http://127.0.0.1:8088/#/runs` 浏览测试记录、轮次指标、请求明细与结果比较。
网页通过统一读取器展示已提交的正式统计；前端资源构建完成后提供页面，资源缺失时返回 503 提示。
使用 `--config-root` 可选已有 YAML 启动测试，取消会发送 SIGINT 并等待收尾；测试生命周期独立于网页连接。

Docker 标准部署通过工作台同源 `/grafana/` 访问诊断页。工作台与 Grafana 的公开地址、
Host/Origin 和 WebSocket 转发必须一致；修改端口时同步修改 `WORKBENCH_PUBLIC_URL`。
完整汇总长期保留，请求明细七天后显示过期原因。跨容器运行时 writer、reader 和 cleanup
共享缓存根下的 `perf-benchmark/result-sealing`；协调文件管理运行状态，结果备份在 writer 关闭后执行。

## 12. Docker 部署


Docker 方案包含三类进程：

| 组件 | 职责 | 运行方式 |
| --- | --- | --- |
| `perf-observation` | Alloy、Prometheus、Tempo、Loki、Pyroscope、Grafana | 常驻服务 |
| `perf-benchmark` | 进入后手动执行 `perf-benchmark run` 的工具箱 | 工具箱常驻，测试按命令启动 |
| `results` | 为 Grafana 提供 SQLite 只读查询 | 常驻服务 |

### 12.1 宿主机前置

- Docker Engine；
- Docker Compose v2；
- 首次构建时可访问镜像仓库、PyPI 和 Rust 模块代理。

Python、uv 和 Rust 由构建镜像提供。

### 12.2 初始化

```bash
cp deploy/.env.example deploy/.env
chmod 600 deploy/.env
```

编辑 `deploy/.env`，至少把下面的占位密码换掉：

```text
GRAFANA_ADMIN_PASSWORD='replace-with-a-long-random-password'
```

自定义数据、tokenizer 或配置目录时，在 `.env` 中填宿主机绝对路径：

```text
BENCHMARK_CONFIG=/absolute/configs
BENCHMARK_DATA=/absolute/data
BENCHMARK_TOKENIZER=/absolute/tokenizer
```

### 12.3 启动平台和工具箱

```bash
bash deploy/observation/deploy.sh up
bash deploy/benchmark/deploy.sh up
```

检查：

```bash
bash deploy/observation/deploy.sh status
bash deploy/benchmark/deploy.sh status
docker exec perf-observation supervisorctl status
```

打开：

```text
http://<Docker宿主机IP>:3000/d/perf-runs
```

### 12.4 准备容器内的运行文件

默认挂载关系：

| 宿主机 | 容器 | 用途 |
| --- | --- | --- |
| `deploy/benchmark/config/` | `/work/config/` | Run YAML |
| `deploy/data/` | `/work/data/` | 数据，只读 |
| `deploy/tokenizer/` | `/work/tokenizer/` | tokenizer，只读 |
| Docker named volume | `/work/results/` | SQLite 和 cache |
| `deploy/observation/logs/` | `/work/logs/` | Benchmark 文件日志 |

开源数据集也应先在宿主机下载和校验，再放入 `BENCHMARK_DATA` 指定的目录。容器内的正式 Run 从 `/work/data/` 读取预备数据。

第一次启动会在宿主机配置目录创建 `run.yaml`。编辑它，至少确认：

```yaml
pipeline:
  loader:
    settings:
      data_files: [/work/data/requests.jsonl]
  normalization:
    settings:
      tokenizer_path: /work/tokenizer
  executor:
    settings:
      base_url: http://gpu-server.internal:8000
      model: model-a
  saver:
    settings:
      path: /work/results/run-001.db
```

`model-a` 只是说明中的占位符，必须换成真实模型名。容器连远程模型时填写容器可达的 DNS 名或 IP；`localhost` 指向容器自身。

每次 Run 创建独立结果，使用新的 SQLite 路径，例如 `run-002.db`。

### 12.5 执行正式测试

```bash
docker exec -it perf-benchmark bash
perf-benchmark run /work/config/run.yaml 2>/work/logs/run-001.log
```

部署脚本启动工具箱，测试通过 `perf-benchmark run` 手动发起。按 Ctrl-C 取消当前 Run，并等待正常收尾后再停止容器。

### 12.6 停止

所有 Run 完全结束后：

```bash
bash deploy/benchmark/deploy.sh stop
bash deploy/observation/deploy.sh stop
```

这两个命令停止部署进程并保留 Docker volume。结果备份和清理由使用者单独管理。

### 12.7 备份结果

只在 Run writer 关闭后复制结果：

```bash
mkdir -p deploy/results/run-001
docker cp perf-benchmark:/work/results/. deploy/results/run-001/
```

SQLite WAL 在运行时保存待合并数据，完整备份在 writer 关闭后执行。正式 SQLite 使用 Docker 本地 volume，以满足本地文件系统的锁和事务要求。


## 13. GPU 诊断

GPU 服务器需要 NVIDIA 驱动、Container Toolkit 与有权限的 Docker/nerdctl；脚本使用主机现有的全局运行时。
DCGM 在 GPU 节点运行，客户端通过网络读取观测数据。可配置远程部署：

```bash
cp deploy/gpu/remote.example.yaml deploy/gpu/remote.local.yaml
chmod 600 deploy/gpu/remote.local.yaml
uv run --group gpu-deploy python deploy/gpu/remote.py deploy/gpu/remote.local.yaml
```

先核对 SSH 指纹，填写兼容的固定镜像及 Nsight CLI 包。部署保留独立远程目录供 bind mount 使用，
DCGM 运行期间保持该目录可用。连接中断后核对并复用已有部署状态。

```bash
bash deploy/gpu/dcgm.sh docker up
bash deploy/gpu/dcgm.sh docker status
bash deploy/gpu/dcgm.sh docker logs
bash deploy/gpu/dcgm.sh docker stop
bash deploy/gpu/nsight.sh docker build
```

DCGM_EXPORTER_IMAGE、INFERENCE_IMAGE、NSIGHT_IMAGE 等变量见脚本及 `remote.example.yaml`；已有 Exporter 可直接配置
`observation.targets.gpu`。Nsight 使用独立空闲 GPU 与诊断服务，原始报告在 REPORT_DIR；
kernel 累计耗时用于算子诊断，墙钟 GPU 利用率取自相应采样指标；正式延迟验收使用独立压测结果。
导入器 `python -m perf_benchmark.app.nsight --help` 校验版本、单位与全部报告后向 Loki 提交摘要。
完整原始报告保存在外部 artifact，Benchmark SQLite 只记录关联。

XLSX 导出使用单次流式读取，写入时同步计算精确列宽，当前工作表 XML 使用临时文件存储。指标值、缺失原因、收入星号和 maxTPM 单位均读取已保存的原生结果。

## 14. 示例选择与排错

仓库保留四份代表性 YAML。静态 Prefill/Decode 矩阵使用本地 tokenizer 构造输入，三份通用压测配置共用 `examples/data/sharegpt.jsonl` 小样本：

| 示例 | 负载与主要参数 |
| --- | --- |
| [`static-benchmark.yaml`](../examples/static-benchmark.yaml) | 静态 Prefill/Decode 矩阵；修改输入长度、并发、请求配额、本地 tokenizer 与 Excel 路径 |
| [`source-only-openai.yaml`](../examples/source-only-openai.yaml) | 静态单轮、并发上限 2、循环执行 8 个会话；修改 `max_concurrency` 和 `stop.session_count` |
| [`concurrency-sweep.yaml`](../examples/concurrency-sweep.yaml) | 动态扫描多个并发档位，逐档判断 SLO；修改 `sweep.values`、`slo.rules` 和 `stop` |
| [`multi-turn.yaml`](../examples/multi-turn.yaml) | 完整多轮会话，分别配置活跃用户数与请求并发；`live_continuation` 决定回放记录或使用实际生成回复 |

运行前修改服务、模型、tokenizer、数据集及结果路径；每次使用新的数据库文件名。动态示例的小样本演示请求流程，
正式压测按目标并发和统计窗口准备数据量及测试时长。将动态示例的 `sweep.values` 替换为第 4.6 节的
`sweep.search` 可搜索满足 SLO 的最大并发；多轮示例也可配置同样的扫描或搜索。

并发、用户池、扫描与 SLO 搜索见第 4 节；数据集见第 5 节，计费与 maxtpm 见第 6 节，
表格与默认位置见第 7、10 节，Open-SWE 见第 8 节。核心回归测试使用临时构造的小样本及 `tests/metrics/fixtures/` 中的指标基准。

unknown setting 先检查 Stage/settings 位置；超时区分单请求、单 Trial 和搜索总预算。
缓存缺失先检查原始 usage 与服务报告开关，数值以原始响应为准；输入慢先区分客户端准备和服务等待。
检查运行状态、失败/取消、SLO、unavailable reason 后再比较性能。


### 大会话池的准备缓存和关键指标模式

固定本地 SWE JSON/Parquet、`open-swe-traces` 和 `all` 组合可以设置
`PERF_BENCHMARK_PREPARED_CACHE=/absolute/path/to/prepared-cache`，复用已经封存的正文与索引。
源文件、seed、准备设置、实现和依赖版本共同决定身份；修改 C/A 或结果路径不重新准备。
未完成或校验失败的缓存不复用。这个缓存只保存客户端请求正文，与服务端 KV、分词或路由缓存无关；正式冷启动测试仍需在打流前清空服务端缓存。调用方负责缓存目录的磁盘预算及旧版本归档。

容量测试只需关键指标时，可显式配置：

```yaml
pipeline:
  saver:
    plugin: sqlite
    settings:
      path: results/capacity.db
      summary_only: true
```

该模式不保存 Session/Turn/Attempt 明细，仍由标准 Metrics 计算精确分位数、吞吐、用量和失败统计。
小型 SQLite 保存元数据和聚合指标；同路径的 `.summary.json` 每10秒原子更新计数，结束时保存完整汇总。
错误分类有界；该模式没有逐请求回溯。默认模式继续保存完整事实。

若单个 Python 调度进程成为瓶颈，可使用 [分片客户端示例](../benchmarks/sharded-clients.md)，
在多个 CPU 进程中运行现有 pipeline，并合并数值样本计算全局分位数。
