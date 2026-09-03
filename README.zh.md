# Meme Cohort Observatory

[English](README.md)

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![License](https://img.shields.io/badge/License-MIT-green)
![Status](https://img.shields.io/badge/Status-Alpha-orange)

**当公共 DEX 数据开始缺失时，仍然保住 cohort 的统计分母。**

Meme Cohort Observatory（MCO）是一套面向新 token pool 的可重放研究 CLI。它记录一批对象是怎样被发现和录取的、后续哪些观测没有拿到，以及现有证据到底允许得出什么结论。

它不是 MEME 喊单器、AI 风险分、买入信号、钱包或自动交易 Agent。

```text
chain      status    coverage          complete  seen  admit  drop  mcap_C1evt  fdv_C1evt
---------  --------  ----------------  --------  ----  -----  ----  ----------  ---------
base       success   fixture_snapshot  no        1     0      0     0           0
bsc        success   fixture_snapshot  no        1     0      0     0           0
robinhood  degraded  promoted_subset   no        1     0      0     0           0
solana     success   fixture_snapshot  no        1     0      0     0           0
```

上面真正有价值的不是某个 token 分数，而是系统明确拒绝下结论：这份 fixture 并不是完整、可比较的发现窗口，因此不能拿 4 条观测去算“成功率”。

## 你能得到什么

- Solana、BSC、Base、Robinhood Chain 的公开新池采集。
- watermark、分页、请求预算、reorg 和 enrichment 的完整来源记录。
- 带 inclusion probability 与 sample fraction 的有界 cohort 录取。
- 从录取时刻锚定的 10 分钟到 7 天跟踪。
- market cap 与 FDV 两条估值轨道严格分开。
- “曾观测到高于阈值”和“同一轨道明确发生 below→above 穿越”严格分开。
- 记录 missed refresh slot，不把拿不到的数据偷偷算成失败或存活。
- SQLite、JSON/JSONL、Markdown/表格报告，以及单 token 路径检查。
- 可离线运行的 fixture 与可重放测试。

## 怎么工作

```text
公开 pool 数据源
      │
      ▼
覆盖感知的发现 ───────► watermark / pagination / reorg 证据
      │
      ▼
可比较窗口硬门 ───────► 不完整窗口不得录取 cohort
      │
      ▼
有界 cohort 录取 ─────► inclusion probability + sample fraction
      │
      ▼
锚定式后续跟踪 ───────► 10m / 1h / 1d / 7d 观测与延迟
      │
      ▼
lifecycle 报告 ───────► 分母、缺口、阈值和结论边界
```

普通 current snapshot 回答“现在还能读到什么”；MCO 进一步回答“最初研究的是哪一群对象、发现覆盖是否完整、后续证据在哪里断了”。

## 安装

```bash
git clone https://github.com/runesleo/meme-cohort-observatory.git
cd meme-cohort-observatory
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[test]'
```

v0.1.0 暂不上 PyPI，请直接从仓库安装。

## 环境要求与隐私

- Python 3.10 或更高版本。
- 内置路径不需要账号、API key、浏览器 Cookie、钱包、签名或私有 RPC。
- 真实采集只向配置好的公共数据源发送 GET-only 或只读 RPC 请求。
- 本地 state 会保存公开 token 地址、pool 地址、估值和时间戳。若你加入自己的标签或研究备注，应自行保护 state 目录。
- 代码没有钱包连接或交易执行面。

## 快速开始

### 60 秒离线演示

```bash
mco collect \
  --fixtures tests/fixtures \
  --state-dir /tmp/mco-demo \
  --observed-at 2026-07-14T02:00:00Z

mco report --state-dir /tmp/mco-demo --format markdown
mco export --state-dir /tmp/mco-demo --format jsonl | head
mco doctor --state-dir /tmp/mco-demo --json
```

不安装也可以运行：

```bash
PYTHONPATH=src python -m meme_cohort_observatory collect \
  --fixtures tests/fixtures \
  --state-dir /tmp/mco-demo \
  --observed-at 2026-07-14T02:00:00Z
```

### 采集公开实时数据

```bash
mco collect --network solana --network bsc --state-dir ./state
mco report --state-dir ./state --format table
```

首次 RPC bootstrap 可能需要几十秒。每次请求和整轮运行都有明确预算；单个数据源失败只会让对应链降级，并留下 coverage 证据。

### 检查一个已记录 token

```bash
mco inspect-token bsc 0x... --state-dir ./state --format markdown
```

它只读取本地 state，不查钱包，也不会下单。

## 输出

- `cohorts.sqlite3`：token、观测、cohort、估值轨道、阈值事件、覆盖记录、pool event 与 cursor。
- `latest_summary.json`：原子写入的低噪声状态。
- `mco report`：覆盖情况与可比较 cohort 的 lifecycle 分析。
- `mco export`：确定性的 JSON 或 JSONL 导出。
- `mco inspect-token`：单 token 的观测、checkpoint、估值轨道、穿越事件和漏采上下文。

## 已验证

v0.1.0 候选版本已经完成：

- 108 个自动化测试。
- public-surface audit：私有绝对路径、内嵌 secret、钱包/交易 import 和调用均为 0。
- wheel 构建，并在全新虚拟环境中安装通过。
- `collect`、`report`、`export`、`inspect-token`、`doctor` 五个命令可发现并完成 fixture 路径。
- BSC 和 Base 的真实无 Key 只读 smoke；Solana 遇到 HTTP 429 时正确降级，且没有把不完整窗口录取成可比较 cohort。

一个按时间固定、事先未挑赢家的 BSC 案例可以说明它的工作。首批 5 个 token 后续再查 current snapshot 时，5 个地址都还返回 pair row，但只有 1 个存在正流动性 pair，0 个能给出可用当前估值。MCO 仍保存最初 5 个 admission 和合计 10 个 missed refresh slot。这里证明的是 provenance 与 missingness，不是策略盈利。

详细验收见 [Stage A](docs/STAGE_A.md)、[Stage B](docs/STAGE_B.md) 和 [竞争力评估](docs/COMPETITIVENESS.md)。

## 已知限制（v0.1.0）

- MCO 不会把发现的每个 token 都判定为 MEME。
- 各链的覆盖受公共数据源限制；自定义 AMM、私有 bonding curve 和未被索引的 pool 可能不在覆盖范围内。
- checkpoint 取目标年龄之后的第一条实际观测并显示 lag，因此它描述的是可观测性，不是精确时刻的 survival rate。
- 阈值只使用实际观测到的 provider 值，不重建区间内 ATH。
- market cap 与 FDV 依赖 provider 口径，不独立重建流通供应量。
- 当前输出会给出 inclusion probability 和 missingness，但不宣称正式统计置信区间。
- 没有 dashboard、托管 API、MCP、PyPI、钱包或交易功能。

## 路线图

近期：

- 提高 1 天和 7 天跟踪覆盖，同时继续显式暴露漏采。
- 为抽样 cohort 增加更严格的不确定性摘要。
- 发布更多按规则固定、没有事后挑选的 cohort 案例。

只有出现真实使用需求才做：

- 新增一个公共数据 adapter。
- 抽出更窄的可复用 Python library interface。
- 增加 Agent 调用层，但不赋予交易执行权。

## 研究边界

观测到 pool event，只能证明某个匹配的公开事件被记录，不能证明流动性安全、token 来源可靠、需求真实、可执行或未来会上涨。发现不是买入建议。

来源和公开/私有边界见 [docs/EXTRACTION.md](docs/EXTRACTION.md)。

## 关于作者

*Leo（[@runes_leo](https://x.com/runes_leo)）— AI × Crypto 独立构建者。在 [Polymarket](https://polymarket.com/?via=runes-leo&r=runesleo&utm_source=github&utm_content=meme-cohort-observatory) 做量化，用 Claude Code 和 Codex 搭数据与内容管线。*

*[leolabs.me](https://leolabs.me) — 写作 · 社群 · 开源工具 · 独立项目*

*[X 订阅](https://x.com/runes_leo/creator-subscriptions/subscribe) — 付费内容周刊*

*Learn in public, Build in public.*

## License

MIT，见 [LICENSE](LICENSE)。
