"""Bounded, dependency-injected public market-data adapters.

asset-version: v1.9
updated: 2026-07-15
owner_surface: Meme Cohort Observatory
behavior_change: Bounded heavy-window bisection in BlockscoutLogClient.get_logs — windows failing with server-shape errors (HTTP 5xx / timeout / URLError) bisect down to min_scan_span (250 blocks) before failing; budget-exhausted errors still fail fast and never bisect; exact log-cap responses still bisect as in v1.7. Fixes the poison-window cursor stall at block 10393222 (2000-block getLogs returns HTTP 500, 500-block succeeds).
rollback: Remove _is_heavy_window_error + the try/except around _get_log_window in get_logs and drop min_scan_span (restores v1.7 fail-fast); remove catchup_max_blocks/scan_to and scan block_from->latest again (restores v1.8 unbounded catch-up).
v1.9 change: RobinhoodPoolEventAdapter.fetch closes at most catchup_max_blocks (default 20000) per run and advances the cursor to that boundary, so deep backlogs are walked forward across runs instead of re-scanning to head and exhausting the request budget with zero cursor progress.
"""

import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .cohort import Candidate, canonical_address, ensure_utc


USER_AGENT = "MemeCohortObservatory/0.1 (read-only research tool)"
GECKO_URL = "https://api.geckoterminal.com/api/v2/networks/{network}/new_pools?page={page}"
GECKO_MAX_PAGE = 10
DEX_PROFILES_URL = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_BOOSTS_URL = "https://api.dexscreener.com/token-boosts/latest/v1"
DEX_PAIRS_URL = "https://api.dexscreener.com/token-pairs/v1/{chain}/{token}"
DEX_TOKENS_URL = "https://api.dexscreener.com/tokens/v1/{chain}/{tokens}"
DEX_REFRESH_BATCH_SIZE = 30

RH_BLOCKSCOUT_API_URL = "https://robinhoodchain.blockscout.com/api"
RH_BLOCKSCOUT_V2_URL = "https://robinhoodchain.blockscout.com/api/v2"
BLOCKSCOUT_LOG_CAP = 1000

RH_V2_PAIR_CREATED_TOPIC = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)
RH_V3_POOL_CREATED_TOPIC = (
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
)
RH_V4_INITIALIZE_TOPIC = (
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
)
RH_TOKEN_DEPLOYED_TOPIC = (
    "0x1461370115e1c2be79cb529f8cfcbd11316e789d9c6099fc83417b0b4c48c62a"
)
RH_TOKEN_LAUNCHED_TOPIC = (
    "0xdb51ea9ad51ab453a65a4cb7e60c3cb378c9501bb002609f8f97778fb6c4235a"
)
RH_DISCOVERY_TOPICS = (
    RH_V2_PAIR_CREATED_TOPIC,
    RH_V3_POOL_CREATED_TOPIC,
    RH_V4_INITIALIZE_TOPIC,
    RH_TOKEN_DEPLOYED_TOPIC,
    RH_TOKEN_LAUNCHED_TOPIC,
)

RH_V2_FACTORY = "0x8bcEaa40b9acdFaEDF85AdF4fF01F5Ad6517937F".lower()
RH_V3_FACTORY = "0x1f7d7550B1b028f7571E69A784071F0205FD2EfA".lower()
RH_V4_POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951".lower()
RH_PONS_FACTORY = "0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB".lower()
RH_NOXA_FACTORY = "0xD9eC2db5f3D1b236843925949fe5bd8a3836FCcB".lower()

RH_KNOWN_EMITTERS = {
    RH_V2_FACTORY: ("uniswap_v2", "verified_emitter"),
    RH_V3_FACTORY: ("uniswap_v3", "verified_emitter"),
    RH_V4_POOL_MANAGER: ("uniswap_v4", "verified_emitter"),
    RH_PONS_FACTORY: ("pons", "verified_emitter"),
    RH_NOXA_FACTORY: ("noxa", "known_unverified_emitter"),
}

RH_CORE_TOKENS = frozenset(
    address.lower()
    for address in (
        "0x0000000000000000000000000000000000000000",
        "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
        "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
        "0xc6911796042b15d7Fa4F6CDe69e245DdCd3d9c31",
    )
)


class SourceError(RuntimeError):
    pass


@dataclass(frozen=True)
class SourceBatch:
    candidates: list
    status: str
    errors: list
    discovery_type: str = "unknown"
    candidate_scope: str = "unknown"
    page_count: int = 0
    oldest_pool_created_at: object = None
    newest_pool_created_at: object = None
    coverage_scope: str = "unknown"
    gap_reason: object = None
    pool_rows_seen: int = 0
    missing_fields: list = field(default_factory=list)
    row_loss_count: int = 0
    pool_events: list = field(default_factory=list)
    next_cursor: object = None
    enrichment_complete: bool = True
    pending_token_addresses: list = field(default_factory=list)
    enrichment_attempted_token_addresses: list = field(default_factory=list)
    block_from: object = None
    block_to: object = None
    latest_indexed_block: object = None
    rewind_from_block: object = None
    raw_scan_status: object = None
    raw_scan_errors: list = field(default_factory=list)


@dataclass(frozen=True)
class BlockCursor:
    chain: str
    source: str
    next_block: int
    last_block: object = None
    last_block_hash: object = None


@dataclass(frozen=True)
class PoolEvent:
    chain: str
    source: str
    event_type: str
    protocol: str
    emitter_address: str
    token0_address: object = None
    token1_address: object = None
    pool_address: object = None
    pool_id: object = None
    block_number: int = 0
    block_hash: object = None
    tx_hash: str = ""
    log_index: int = 0
    event_timestamp: object = None
    venue_status: str = "unverified_emitter"
    candidate_token_addresses: tuple = ()


def _float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value):
    parsed = _float(value)
    return int(parsed) if parsed is not None else None


def _datetime(value):
    if value is None or value == "":
        return None
    if isinstance(value, (float, int)):
        # DexScreener pairCreatedAt is milliseconds.
        seconds = float(value) / 1000.0 if value > 10_000_000_000 else float(value)
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return ensure_utc(parsed)


def _mapping(value):
    return value if isinstance(value, dict) else {}


def _ordered_unique(values):
    result = []
    seen = set()
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _time_bounds(candidates):
    values = [item.pool_created_at for item in candidates if item.pool_created_at]
    if not values:
        return None, None
    return min(values), max(values)


def _valuation(market_cap, fdv):
    observed = _float(market_cap)
    if observed is not None:
        return observed, "market_cap"
    proxy = _float(fdv)
    if proxy is not None:
        return proxy, "fdv"
    return None, None


class RequestBudget:
    """Shared wall-clock budget for every public request in one run."""

    def __init__(self, max_seconds=480.0, monotonic=None):
        max_seconds = float(max_seconds)
        if max_seconds <= 0 or max_seconds > 540:
            raise ValueError("run budget must be > 0 and <= 540 seconds")
        self.monotonic = monotonic or time.monotonic
        self.deadline = self.monotonic() + max_seconds

    def bounded_timeout(self, requested):
        remaining = self.deadline - self.monotonic()
        if remaining <= 0:
            raise SourceError("public request budget exhausted")
        return max(0.05, min(float(requested), remaining))

    def sleep(self, delay, sleeper):
        delay = max(0.0, float(delay))
        remaining = self.deadline - self.monotonic()
        if delay > remaining:
            raise SourceError("public request budget exhausted before sleep")
        sleeper(delay)


class SharedRateLimiter:
    """Sequential shared limiter used by all Gecko network adapters."""

    def __init__(
        self, min_interval=2.05, monotonic=None, sleeper=None, budget=None
    ):
        self.min_interval = max(0.0, float(min_interval))
        self.monotonic = monotonic or time.monotonic
        self.sleeper = sleeper or time.sleep
        self.budget = budget
        self.last_request_at = None

    def wait(self):
        now = self.monotonic()
        if self.last_request_at is not None:
            delay = self.min_interval - (now - self.last_request_at)
            if delay > 0:
                if self.budget is not None:
                    self.budget.sleep(delay, self.sleeper)
                else:
                    self.sleeper(delay)
                now = self.monotonic()
        self.last_request_at = now


class JsonHttpClient:
    def __init__(
        self,
        timeout=6.0,
        opener=None,
        budget=None,
        max_retries=1,
        sleeper=None,
        before_attempt=None,
    ):
        timeout = float(timeout)
        if timeout <= 0 or timeout > 30:
            raise ValueError("timeout must be > 0 and <= 30 seconds")
        self.timeout = timeout
        self.opener = opener or urllib.request.urlopen
        self.budget = budget
        self.max_retries = max(0, min(int(max_retries), 1))
        self.sleeper = sleeper or time.sleep
        self.before_attempt = before_attempt

    def get_json(self, url):
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            method="GET",
        )
        for attempt in range(self.max_retries + 1):
            if self.before_attempt is not None:
                self.before_attempt()
            timeout = (
                self.budget.bounded_timeout(self.timeout)
                if self.budget is not None
                else self.timeout
            )
            try:
                with self.opener(request, timeout=timeout) as response:
                    payload = response.read()
                return json.loads(payload.decode("utf-8"))
            except urllib.error.HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code <= 599
                if retryable and attempt < self.max_retries:
                    header = exc.headers.get("Retry-After") if exc.headers else None
                    try:
                        delay = float(header) if header is not None else 2.0
                    except (TypeError, ValueError):
                        delay = 2.0
                    delay = max(0.0, min(delay, 10.0))
                    if self.budget is not None:
                        self.budget.sleep(delay, self.sleeper)
                    else:
                        self.sleeper(delay)
                    continue
                raise SourceError(
                    "GET failed for %s (HTTP %s)" % (url, exc.code)
                )
            except (
                urllib.error.URLError,
                TimeoutError,
                OSError,
                ValueError,
                json.JSONDecodeError,
            ) as exc:
                raise SourceError(
                    "GET failed for %s (%s)" % (url, exc.__class__.__name__)
                )


def _hex_int(value):
    if isinstance(value, int):
        return value
    text = str(value or "").strip().lower()
    try:
        return int(text, 16 if text.startswith("0x") else 10)
    except (TypeError, ValueError):
        raise SourceError("invalid integer field")


def _hex_payload(value):
    text = str(value or "").strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if any(character not in "0123456789abcdef" for character in text):
        raise SourceError("invalid hex payload")
    return text


def _topic_address(value):
    payload = _hex_payload(value)
    if len(payload) != 64:
        raise SourceError("address topic is not 32 bytes")
    return "0x" + payload[-40:]


def _word_address(value):
    payload = _hex_payload(value)
    if len(payload) != 64:
        raise SourceError("address word is not 32 bytes")
    return "0x" + payload[-40:]


def _data_words(value):
    payload = _hex_payload(value)
    if len(payload) % 64 != 0:
        raise SourceError("event data is not word aligned")
    return [payload[offset : offset + 64] for offset in range(0, len(payload), 64)]


def _event_timestamp(log):
    value = log.get("timeStamp")
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(_hex_int(value), tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise SourceError("invalid event timestamp")


def _candidate_tokens(event_type, token0, token1):
    values = (token0,) if event_type in ("token_deployed", "token_launched") else (token0, token1)
    return tuple(
        _ordered_unique(
            canonical_address("robinhood", value)
            for value in values
            if canonical_address("robinhood", value) not in RH_CORE_TOKENS
        )
    )


def decode_pool_log(topic, log, *, chain="robinhood", known_emitters=None):
    """Decode one standard pool/launch event without trusting its emitter.

    2026-07-30 参数化: 事件本身 (Uniswap V2 PairCreated / V3 PoolCreated) 是跨链
    标准, 链特有的只有 emitter 白名单与地址规范化。故按 chain 参数化, 让 base
    等 EVM 链复用同一套解码, 而不是复制一份。emitter 白名单是第 2 层安全门 ——
    未在白名单的一律标 unverified_emitter, 不因换链而放宽。
    """
    if not isinstance(log, dict):
        raise SourceError("blockscout log is not an object")
    known_emitters = RH_KNOWN_EMITTERS if known_emitters is None else known_emitters
    topic = str(topic or "").lower()
    topics = [str(value).lower() for value in log.get("topics", [])]
    if not topics or topics[0] != topic:
        raise SourceError("topic0 mismatch")
    emitter = canonical_address(chain, str(log.get("address") or ""))
    if not emitter:
        raise SourceError("missing event emitter")
    known_protocol, known_status = known_emitters.get(
        emitter, (None, "unverified_emitter")
    )
    words = _data_words(log.get("data", "0x"))
    event_type = None
    protocol = None
    token0 = None
    token1 = None
    pool_address = None
    pool_id = None

    if topic == RH_V2_PAIR_CREATED_TOPIC:
        if len(topics) < 3 or len(words) < 2:
            raise SourceError("short v2 PairCreated log")
        event_type = "pair_created"
        protocol = "uniswap_v2"
        token0 = _topic_address(topics[1])
        token1 = _topic_address(topics[2])
        pool_address = _word_address(words[0])
    elif topic == RH_V3_POOL_CREATED_TOPIC:
        if len(topics) < 4 or len(words) < 2:
            raise SourceError("short v3 PoolCreated log")
        event_type = "pool_created"
        protocol = "uniswap_v3"
        token0 = _topic_address(topics[1])
        token1 = _topic_address(topics[2])
        pool_address = _word_address(words[1])
    elif topic == RH_V4_INITIALIZE_TOPIC:
        if len(topics) < 4:
            raise SourceError("short v4 Initialize log")
        event_type = "pool_initialized"
        protocol = "uniswap_v4"
        pool_id = "0x" + _hex_payload(topics[1])
        if len(pool_id) != 66:
            raise SourceError("v4 PoolId is not 32 bytes")
        token0 = _topic_address(topics[2])
        token1 = _topic_address(topics[3])
    elif topic == RH_TOKEN_DEPLOYED_TOPIC:
        if len(topics) < 4 or len(words) < 3:
            raise SourceError("short TokenDeployed log")
        event_type = "token_deployed"
        protocol = known_protocol if known_protocol in ("pons", "noxa") else "launchpad"
        token0 = _topic_address(topics[1])
        token1 = _word_address(words[0])
    elif topic == RH_TOKEN_LAUNCHED_TOPIC:
        if len(topics) < 4 or len(words) < 2:
            raise SourceError("short TokenLaunched log")
        event_type = "token_launched"
        protocol = known_protocol if known_protocol in ("pons", "noxa") else "launchpad"
        token0 = _topic_address(topics[1])
        token1 = _word_address(words[0])
        pool_address = _word_address(words[1])
    else:
        raise SourceError("unsupported robinhood discovery topic")

    if known_protocol != protocol:
        known_status = "unverified_emitter"
    token0 = canonical_address(chain, token0)
    token1 = canonical_address(chain, token1)
    return PoolEvent(
        chain=chain,
        source="blockscout_pool_events",
        event_type=event_type,
        protocol=protocol,
        emitter_address=emitter,
        token0_address=token0,
        token1_address=token1,
        pool_address=(canonical_address(chain, pool_address) if pool_address else None),
        pool_id=pool_id,
        block_number=_hex_int(log.get("blockNumber")),
        block_hash=str(log.get("blockHash") or "").lower() or None,
        tx_hash=str(log.get("transactionHash") or "").lower(),
        log_index=_hex_int(log.get("logIndex")),
        event_timestamp=_event_timestamp(log),
        venue_status=known_status,
        candidate_token_addresses=_candidate_tokens(event_type, token0, token1),
    )


class BlockscoutLogClient:
    """GET-only Blockscout indexed-head and chain-wide topic-log reader."""

    def __init__(
        self,
        timeout=6.0,
        opener=None,
        budget=None,
        http_client=None,
        api_url=RH_BLOCKSCOUT_API_URL,
        api_v2_url=RH_BLOCKSCOUT_V2_URL,
        log_cap=BLOCKSCOUT_LOG_CAP,
        max_block_span=2000,
        min_scan_span=250,
    ):
        self.client = http_client or JsonHttpClient(
            timeout=timeout, opener=opener, budget=budget
        )
        self.api_url = api_url.rstrip("/")
        self.api_v2_url = api_v2_url.rstrip("/")
        self.log_cap = int(log_cap)
        self.max_block_span = max(1, int(max_block_span))
        self.min_scan_span = max(1, min(int(min_scan_span), self.max_block_span))

    def _etherscan_get(self, **params):
        url = self.api_url + "?" + urllib.parse.urlencode(params)
        return self.client.get_json(url)

    def latest_indexed_block(self):
        payload = self._etherscan_get(module="block", action="eth_block_number")
        if not isinstance(payload, dict) or payload.get("result") in (None, ""):
            raise SourceError("blockscout latest indexed block missing")
        return _hex_int(payload["result"])

    def block_by_time(self, timestamp):
        payload = self._etherscan_get(
            module="block",
            action="getblocknobytime",
            timestamp=int(timestamp),
            closest="before",
        )
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            result = result.get("blockNumber")
        if result in (None, ""):
            raise SourceError("blockscout time lookup missing")
        return _hex_int(result)

    def block_hash(self, height):
        payload = self.client.get_json("%s/blocks/%d" % (self.api_v2_url, int(height)))
        value = payload.get("hash") if isinstance(payload, dict) else None
        if not value:
            raise SourceError("blockscout block hash missing")
        return str(value).lower()

    def _get_log_window(self, topic, from_block, to_block):
        payload = self._etherscan_get(
            module="logs",
            action="getLogs",
            fromBlock=int(from_block),
            toBlock=int(to_block),
            topic0=str(topic),
        )
        if not isinstance(payload, dict):
            raise SourceError("blockscout logs payload is not an object")
        result = payload.get("result")
        status = str(payload.get("status") or "")
        message = str(payload.get("message") or "").strip().lower()
        if isinstance(result, list) and status == "1":
            return result
        explicit_empty = (
            status == "0"
            and isinstance(result, list)
            and not result
            and ("no record" in message or "no log" in message)
        )
        if explicit_empty:
            return []
        raise SourceError("blockscout logs query failed")

    @staticmethod
    def _is_heavy_window_error(exc):
        # Server-shape failures on a wide window (Blockscout cannot compute the
        # range) may succeed on a narrower one. Budget exhaustion must never
        # bisect: splitting only multiplies requests against a spent budget.
        message = str(exc)
        if "budget exhausted" in message:
            return False
        if "(HTTP 5" in message:
            return True
        return message.endswith(("(URLError)", "(TimeoutError)", "(timeout)"))

    def get_logs(self, topic, from_block, to_block):
        from_block = int(from_block)
        to_block = int(to_block)
        if from_block > to_block:
            return []
        span = to_block - from_block + 1
        if span > self.max_block_span:
            midpoint = min(to_block, from_block + self.max_block_span - 1)
            return self.get_logs(topic, from_block, midpoint) + self.get_logs(
                topic, midpoint + 1, to_block
            )
        try:
            rows = self._get_log_window(topic, from_block, to_block)
        except SourceError as exc:
            if span > self.min_scan_span and self._is_heavy_window_error(exc):
                midpoint = (from_block + to_block) // 2
                return self.get_logs(topic, from_block, midpoint) + self.get_logs(
                    topic, midpoint + 1, to_block
                )
            raise
        if len(rows) < self.log_cap:
            return rows
        if from_block == to_block:
            raise SourceError("single_block_log_cap:%d" % from_block)
        midpoint = (from_block + to_block) // 2
        return self.get_logs(topic, from_block, midpoint) + self.get_logs(
            topic, midpoint + 1, to_block
        )


# ---------------------------------------------------------------------------
# 多链链上发现配置 (2026-07-30)
#
# 由来: 实测 default_adapters() 只有 robinhood 走链上事件, 其余链走
# CompositeAdapter([GeckoTerminalAdapter, DexScreenerAdapter]) —— base/solana 因此
# 撞 HTTP 429 (gap_reason=page_fetch_error_before_watermark), 而 robinhood 走 RPC
# 的那条 coverage_scope=fresh_head_window_closed / status=success。链上 RPC 直采
# 不受第三方聚合器限流, 是可复制的模式。
#
# 每条链的参数都来自**实测**, 不得凭记忆填:
#   robinhood 出块 9.96 block/s · getLogs 50,000 span 实测可用 · 21,331 block/s
#   base      出块 0.50 block/s · getLogs 硬上限 10,000 (-32614) · 2,303 block/s
#                                                        (余量 4,606 倍)
#   bsc       三个公共节点全不可用: bsc-dataseed=limit exceeded(-32005) /
#             publicnode=History has been pruned(-32701) / llamarpc=空响应
#             ⇒ 需先解决数据源(自建/付费=capex 需单独评估), 不是"补个 job"就行
#
# factory 白名单是第 2 层安全门的一部分, 写错等于把伪造事件当真, 故每个地址都要
# **链上实证 + 官方来源交叉验证**双通过才收录。
CHAIN_LOG_SOURCES = {
    "robinhood": {
        "rpc_url": "https://rpc.mainnet.chain.robinhood.com",
        "max_block_span": 25000,
        "batch_supported": True,   # 实测 50 个 getBlockByNumber 一次 4.10s
        "resolve_timestamps": True,
        "note": "urllib 对该 RPC 会 403(指纹过滤), RpcLogClient 走 curl",
    },
    "base": {
        "rpc_url": "https://mainnet.base.org",
        # 硬上限 10,000: 超过返回 -32614 "eth_getLogs is limited to a 10,000 range"。
        # RpcLogClient 内建超限自动切分, 设对上限即可。
        "max_block_span": 10000,
        # 实测 base **不支持 JSON-RPC batch**(返回非 list) → 逐个查时间戳会让
        # 20,000 区块的一次扫描超过 300s。区块时间是链上不可变量, 可按 blockNumber
        # 事后精确回填, 故这里关闭即时补齐以保吞吐 —— 不是丢数据, 是延后解析。
        "batch_supported": False,
        "resolve_timestamps": False,
        "topics": {
            # Uniswap 标准事件 topic, 跨链通用(同一 topic 在 RH 与 base 均验证过)
            "v2_pair_created": (
                "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
            ),
            "v3_pool_created": (
                "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118"
            ),
        },
        "known_emitters": {
            # ✅ 双通过: 链上实测 30,000 区块内 175 条(压倒性第一) +
            #    官方 developers.uniswap.org v2 deployments 表载明 Base 为该地址
            "0x8909dc15e40173ff4699343b6eb8132c65e18ec6": ("uniswap_v2", "verified"),
            # ✅ 链上实测 17 条; 官方 v3 deployments 待同样交叉验证后再升 verified
            "0x33128a8fc17869897dce68ed026d694621f6fdfd": ("uniswap_v3", "onchain_only"),
        },
        # 实测出现但**尚未验证归属**的 emitter —— 不进白名单, 仅记录以免下次重复调研。
        # 收录前必须同样双通过, 否则伪造 factory 的事件会被当成真实新池。
        "unverified_emitters_seen": {
            "0x488db0978b34c6fd901760b9024b565c1117c7c8": "v2_pair_created x18",
            "0x02a84c1b3bbd7401a5f7fa98a384ebc70bb5749e": "v2_pair_created x12",
            "0xa30aa42726c0050b46bae4b567087b3f6724965a": "v2_pair_created x1",
            "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865": "v3_pool_created x1",
            "0x7ca1dccfb4f49564b8f13e18a67747fd428f1c40": "v3_pool_created x1",
        },
        # 从链上反推: 175 个 UniswapV2 新池中, 0x4200…0006 作为配对币出现 156 次
        # (89%), 与 OP Stack 标准预部署 WETH 地址精确一致 → 双通过。核心配对币用于
        # 从 enrichment 请求里排除, 放错会漏掉真实候选, 故只收双通过的。
        "core_tokens": (
            "0x0000000000000000000000000000000000000000",
            "0x4200000000000000000000000000000000000006",  # WETH (OP Stack 预部署)
        ),
        "status": "wired",  # 2026-07-30 已接入 default_adapters
    },
}



def decode_robinhood_log(topic, log):
    """向后兼容包装 —— 保持 RH 调用点与既有测试逐字不变。"""
    return decode_pool_log(topic, log, chain="robinhood",
                           known_emitters=RH_KNOWN_EMITTERS)
def chain_log_client(chain, **overrides):
    """按 CHAIN_LOG_SOURCES 造 RpcLogClient —— 参数来自实测而非默认值。"""
    cfg = CHAIN_LOG_SOURCES.get(chain)
    if not cfg:
        raise SourceError("no on-chain log source configured for %s" % chain)
    kwargs = {
        "rpc_url": cfg["rpc_url"],
        "max_block_span": cfg["max_block_span"],
        "resolve_timestamps": cfg.get("resolve_timestamps", True),
    }
    kwargs.update(overrides)
    return RpcLogClient(**kwargs)


class RpcLogClient:
    """官方 RPC 版 topic-log reader，与 BlockscoutLogClient 接口等价（duck-typed）。

    2026-07-30 新增。动机是实测出的数据源不对称：
      官方 RPC   50,000 区块 getLogs = 2.34s  → 21,331 block/s
      Blockscout  2,000 区块         = 1.40s + 2.05s 强制间隔 → 实效 580 block/s
    差 37 倍。真实后果记录在 artifacts/2026-07-29-solana-coverage-budget-deadlock.md：
    RH 单链一轮 stage_durations_seconds=26,444s(7.3 小时) 吃满预算，
    solana/base/bsc 全部 skipped(run_budget_exhausted_before_chain)。
    按本 client 的速率，同样 80 万区块 backlog 约 7.2 分钟走完。

    时间戳: eth_getLogs 不返回 timeStamp。只对**确有事件的区块**用 JSON-RPC batch
    补齐(实测 82ms/区块，已验证该 RPC 支持 batch)，因此成本随事件数而非区块数增长。
    resolve_timestamps=False 时留空 —— 区块时间是链上不可变量，可按 blockNumber
    事后精确回填，这与"错过即永久丢失"的量不同，不用近似值冒充。

    注意 urllib 对该 RPC 会 403(指纹过滤)，故与 noxa_factory_logs.py 一致走 curl。
    """

    def __init__(
        self,
        rpc_url="https://rpc.mainnet.chain.robinhood.com",
        timeout=30.0,
        budget=None,
        max_block_span=25000,
        resolve_timestamps=True,
        batch_size=50,
        runner=None,
    ):
        self.rpc_url = rpc_url
        self.timeout = float(timeout)
        self.budget = budget
        # 实测 50,000 span 可用; 默认取一半留余量
        self.max_block_span = max(1, int(max_block_span))
        self.resolve_timestamps = bool(resolve_timestamps)
        self.batch_size = max(1, int(batch_size))
        self._runner = runner or self._curl
        self._ts_cache = {}
        # 首次 batch 失败后置 False, 之后一律逐个查 (base 不支持 batch, RH 支持)
        self._batch_supported = True

    # curl 退出码里可恢复的瞬时故障: 7=连不上 28=超时 35=SSL 握手失败
    # 52=空回复 56=接收失败。2026-07-30 首次真实 fetch 即遇 rc=35, 单次失败就抛错
    # 会让整轮 discovery 带 error 收尾, 而这类错误重试一次通常就过。
    _RETRYABLE_CURL_RC = frozenset((7, 28, 35, 52, 56))

    def _curl(self, payload, attempts=3):
        last = None
        for attempt in range(attempts):
            proc = subprocess.run(
                ["curl", "-sS", "-X", "POST", self.rpc_url,
                 "-H", "content-type: application/json", "-d", json.dumps(payload)],
                capture_output=True, text=True, timeout=self.timeout,
            )
            if proc.returncode == 0:
                try:
                    return json.loads(proc.stdout)
                except ValueError:
                    raise SourceError("rpc response is not json")
            last = proc.returncode
            if last not in self._RETRYABLE_CURL_RC or attempt == attempts - 1:
                break
            if self.budget is not None:
                self.budget.check()
            time.sleep(0.5 * (2 ** attempt))  # 0.5s / 1s 退避
        raise SourceError("rpc curl failed rc=%s" % last)

    def _call(self, method, params):
        if self.budget is not None:
            self.budget.check()
        body = self._runner({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
        if not isinstance(body, dict):
            raise SourceError("rpc response is not an object")
        if body.get("error"):
            raise SourceError("rpc error: %s" % str(body["error"])[:160])
        return body.get("result")

    def latest_indexed_block(self):
        return _hex_int(self._call("eth_blockNumber", []))

    def block_hash(self, block_number):
        block = self._call("eth_getBlockByNumber", [hex(int(block_number)), False])
        if not isinstance(block, dict) or not block.get("hash"):
            raise SourceError("rpc block hash missing")
        return str(block["hash"]).lower()

    def _block_timestamp(self, block_number):
        block = self._call("eth_getBlockByNumber", [hex(int(block_number)), False])
        if not isinstance(block, dict) or block.get("timestamp") is None:
            raise SourceError("rpc block timestamp missing")
        return _hex_int(block["timestamp"])

    def block_by_time(self, unix_ts):
        """RPC 无 by-time 端点 → 对区块高度二分。只在 bootstrap 用，约 25 次调用。"""
        target = int(unix_ts)
        low, high = 1, self.latest_indexed_block()
        if self._block_timestamp(high) <= target:
            return high
        while low < high:
            mid = (low + high) // 2
            if self._block_timestamp(mid) < target:
                low = mid + 1
            else:
                high = mid
        return low

    def _fill_timestamps(self, rows):
        """只为确有事件的区块补时间戳，用 batch 摊薄往返。"""
        wanted = []
        for row in rows:
            try:
                bn = _hex_int(row.get("blockNumber"))
            except SourceError:
                continue
            if bn not in self._ts_cache and bn not in wanted:
                wanted.append(bn)
        for start in range(0, len(wanted), self.batch_size):
            chunk = wanted[start : start + self.batch_size]
            if self.budget is not None:
                self.budget.check()
            payload = [
                {"jsonrpc": "2.0", "id": idx, "method": "eth_getBlockByNumber",
                 "params": [hex(bn), False]}
                for idx, bn in enumerate(chunk)
            ]
            body = self._runner(payload) if self._batch_supported else None
            if isinstance(body, list):
                by_id = {item.get("id"): item for item in body if isinstance(item, dict)}
                for idx, bn in enumerate(chunk):
                    item = by_id.get(idx) or {}
                    result = item.get("result")
                    if isinstance(result, dict) and result.get("timestamp") is not None:
                        self._ts_cache[bn] = _hex_int(result["timestamp"])
                continue
            # 2026-07-30 实测: batch 不是所有 RPC 都支持 —— robinhood 支持(50 个
            # getBlockByNumber 一次 4.10s), base 不支持(返回非 list)。此前直接抛
            # SourceError, 等于把一条可用的链判死。降级为逐个查询并记 flag, 避免
            # 每个 chunk 都重试一次 batch。
            self._batch_supported = False
            for bn in chunk:
                if self.budget is not None:
                    self.budget.check()
                try:
                    self._ts_cache[bn] = self._block_timestamp(bn)
                except SourceError:
                    # 单个区块拿不到时间戳不该毁掉整批 —— 该行留空,
                    # 区块时间可按 blockNumber 事后精确回填
                    continue
        for row in rows:
            try:
                bn = _hex_int(row.get("blockNumber"))
            except SourceError:
                continue
            if bn in self._ts_cache:
                row["timeStamp"] = self._ts_cache[bn]

    def get_logs(self, topic, from_block, to_block):
        from_block = int(from_block)
        to_block = int(to_block)
        if from_block > to_block:
            return []
        span = to_block - from_block + 1
        if span > self.max_block_span:
            midpoint = min(to_block, from_block + self.max_block_span - 1)
            return self.get_logs(topic, from_block, midpoint) + self.get_logs(
                topic, midpoint + 1, to_block
            )
        result = self._call(
            "eth_getLogs",
            [{"fromBlock": hex(from_block), "toBlock": hex(to_block), "topics": [topic]}],
        )
        rows = result if isinstance(result, list) else []
        if self.resolve_timestamps and rows:
            self._fill_timestamps(rows)
        return rows


class EvmPoolEventAdapter:
    """Discover standard EVM pools/launches from full topic streams.

    2026-07-30 由 RobinhoodPoolEventAdapter 参数化而来。Uniswap V2 PairCreated /
    V3 PoolCreated 是跨链标准事件, 链特有的只有: chain 名、emitter 白名单、
    核心配对币、日志源。故一次实现服务多链, 而不是每链复制 200 行。
    RobinhoodPoolEventAdapter 保留为预置实例, RH 行为逐字不变。
    """

    source_name = "blockscout_pool_events"
    cursor_source = source_name

    def __init__(
        self,
        timeout=6.0,
        opener=None,
        budget=None,
        log_client=None,
        enricher=None,
        topics=RH_DISCOVERY_TOPICS,
        bootstrap_lookback=timedelta(minutes=15),
        reorg_rewind_blocks=64,
        max_enrichment_tokens=300,
        confirmation_blocks=64,
        catchup_max_blocks=20000,
        chain="robinhood",
        known_emitters=None,
        core_tokens=None,
        source_name=None,
    ):
        self.chain = chain
        self.known_emitters = (
            RH_KNOWN_EMITTERS if known_emitters is None else known_emitters
        )
        self.core_tokens = RH_CORE_TOKENS if core_tokens is None else core_tokens
        if source_name is not None:
            self.source_name = source_name
            self.cursor_source = source_name
        # 2026-07-30: 默认路径由 Blockscout 切到官方 RPC。实测 25,000 区块含时间戳
        # 补齐 10.73s (2,330 block/s), 对比 Blockscout 实效 580 block/s 快 4 倍;
        # 追 80 万区块 backlog 由 7.3 小时降到 5.7 分钟。根因见
        # artifacts/2026-07-29-solana-coverage-budget-deadlock.md —— RH 单链吃满
        # 预算导致 solana/base/bsc 每轮 run_budget_exhausted_before_chain。
        # BlockscoutLogClient 保留为可注入回退 (log_client=BlockscoutLogClient(...))。
        self.log_client = log_client or RpcLogClient(
            budget=budget,
            timeout=max(30.0, float(timeout)),
        )
        self._fallback_log_client_factory = lambda: BlockscoutLogClient(
            timeout=timeout, opener=opener, budget=budget
        )
        self.enricher = enricher
        self.topics = tuple(topics)
        self.bootstrap_lookback = bootstrap_lookback
        self.reorg_rewind_blocks = max(1, int(reorg_rewind_blocks))
        self.max_enrichment_tokens = max(1, int(max_enrichment_tokens))
        self.confirmation_blocks = max(0, int(confirmation_blocks))
        self.catchup_max_blocks = max(1, int(catchup_max_blocks))

    def fetch(
        self,
        observed_at,
        since=None,
        cursor=None,
        pending_token_addresses=None,
    ):
        observed_at = ensure_utc(observed_at)
        if cursor is not None and (
            cursor.chain != self.chain or cursor.source != self.source_name
        ):
            raise SourceError("%s block cursor identity mismatch" % self.chain)
        indexed_head = self.log_client.latest_indexed_block()
        latest = max(0, indexed_head - self.confirmation_blocks)
        rewind_from_block = None
        if cursor is not None:
            if cursor.last_block is not None and latest < int(cursor.last_block):
                raise SourceError(
                    "confirmed_indexed_head_regressed:%d<%d"
                    % (latest, int(cursor.last_block))
                )
            block_from = int(cursor.next_block)
            if cursor.last_block is not None and cursor.last_block_hash:
                current_hash = self.log_client.block_hash(cursor.last_block)
                if current_hash.lower() != str(cursor.last_block_hash).lower():
                    block_from = max(
                        0, int(cursor.last_block) - self.reorg_rewind_blocks + 1
                    )
                    rewind_from_block = block_from
        else:
            requested_since = ensure_utc(since or observed_at)
            bootstrap_since = min(
                requested_since, observed_at - self.bootstrap_lookback
            )
            block_from = self.log_client.block_by_time(int(bootstrap_since.timestamp()))

        # Bounded catch-up: close a fixed-size range per run so the cursor
        # advances through a deep backlog instead of re-scanning to head and
        # exhausting the budget without progress.
        scan_to = min(latest, block_from + self.catchup_max_blocks - 1)
        raw_rows_seen = 0
        events = []
        discovery_errors = []
        row_loss_count = 0
        if block_from <= scan_to:
            for topic in self.topics:
                try:
                    rows = self.log_client.get_logs(topic, block_from, scan_to)
                except SourceError as exc:
                    discovery_errors.append("topic_scan_error:%s:%s" % (topic, str(exc)))
                    continue
                raw_rows_seen += len(rows)
                for row in rows:
                    try:
                        events.append(decode_pool_log(topic, row, chain=self.chain, known_emitters=self.known_emitters))
                    except SourceError as exc:
                        row_loss_count += 1
                        discovery_errors.append("event_decode_error:%s:%s" % (topic, str(exc)))
        selected = {}
        for event in events:
            selected.setdefault((event.tx_hash, event.log_index), event)
        events = sorted(
            selected.values(), key=lambda item: (item.block_number, item.log_index)
        )

        coverage_complete = not discovery_errors and row_loss_count == 0
        next_cursor = None
        if coverage_complete:
            if block_from <= scan_to:
                try:
                    last_hash = self.log_client.block_hash(scan_to)
                except SourceError as exc:
                    discovery_errors.append("head_hash_error:%s" % str(exc))
                    coverage_complete = False
                else:
                    next_cursor = BlockCursor(
                        self.chain, self.source_name, scan_to + 1, scan_to, last_hash
                    )
            else:
                next_cursor = cursor or BlockCursor(
                    self.chain, self.source_name, block_from, latest, None
                )

        new_event_tokens = _ordered_unique(
            [
                token
                for event in events
                for token in event.candidate_token_addresses
            ]
        )
        persisted_pending_tokens = _ordered_unique(
            [
                canonical_address(self.chain, token)
                for token in (pending_token_addresses or [])
                if canonical_address(self.chain, token) not in self.core_tokens
            ]
        )
        requested_tokens = _ordered_unique(
            new_event_tokens + persisted_pending_tokens
        )
        enrichment_request = requested_tokens[: self.max_enrichment_tokens]
        candidates = []
        enrichment_errors = []
        if enrichment_request and self.enricher is not None:
            try:
                enriched = self.enricher.fetch_tokens(enrichment_request, observed_at)
                enrichment_errors.extend(enriched.errors)
                requested_set = set(enrichment_request)
                candidates = [
                    replace(
                        item,
                        source="blockscout_pool_events+dexscreener",
                        discovery_type="onchain_pool_events",
                        candidate_scope="full_chain_pool_events",
                        observation_phase="discovery",
                    )
                    for item in enriched.candidates
                    if item.chain.strip().lower() == self.chain
                    and canonical_address(self.chain, item.token_address) in requested_set
                ]
            except SourceError as exc:
                enrichment_errors.append("dex_enrichment_error:%s" % str(exc))
        usable = {
            canonical_address(self.chain, item.token_address)
            for item in candidates
            if item.valuation_kind in ("market_cap", "fdv")
            and item.valuation_usd is not None
        }
        pending = [token for token in requested_tokens if token not in usable]
        enrichment_complete = not pending and not enrichment_errors
        all_errors = discovery_errors + enrichment_errors
        return SourceBatch(
            candidates=candidates,
            status=("success" if coverage_complete and enrichment_complete else "degraded"),
            errors=all_errors,
            discovery_type="onchain_pool_events",
            candidate_scope="full_chain_pool_events",
            page_count=len(self.topics),
            oldest_pool_created_at=_time_bounds(candidates)[0],
            newest_pool_created_at=_time_bounds(candidates)[1],
            coverage_scope="block_range_closed" if coverage_complete else "block_gap",
            gap_reason=(";".join(discovery_errors) if discovery_errors else None),
            pool_rows_seen=raw_rows_seen,
            row_loss_count=row_loss_count,
            pool_events=events,
            next_cursor=next_cursor,
            enrichment_complete=enrichment_complete,
            pending_token_addresses=pending,
            enrichment_attempted_token_addresses=enrichment_request,
            block_from=block_from,
            block_to=scan_to,
            latest_indexed_block=indexed_head,
            rewind_from_block=rewind_from_block,
            raw_scan_status=("success" if coverage_complete else "error"),
            raw_scan_errors=discovery_errors,
        )

class RobinhoodPoolEventAdapter(EvmPoolEventAdapter):
    """RH 预置实例 —— 默认参数即原硬编码值, 行为逐字不变。"""



class GeckoTerminalAdapter:
    source_name = "geckoterminal"

    def __init__(
        self,
        chain,
        network,
        timeout=6.0,
        opener=None,
        page_delay=0.2,
        sleeper=None,
        budget=None,
        rate_limiter=None,
    ):
        self.chain = chain
        self.network = network
        self.page_delay = max(0.0, float(page_delay))
        self.sleeper = sleeper or time.sleep
        self.rate_limiter = rate_limiter
        self.client = JsonHttpClient(
            timeout=timeout,
            opener=opener,
            budget=budget,
            sleeper=sleeper,
            before_attempt=(rate_limiter.wait if rate_limiter else None),
        )

    def fetch(self, observed_at, since=None):
        observed_at = ensure_utc(observed_at)
        since = ensure_utc(since or (observed_at - timedelta(minutes=10)))
        candidates = []
        errors = []
        missing_fields = []
        pool_rows_seen = 0
        page_count = 0
        watermark_closed = False
        exhausted = False
        page_fetch_error = False
        row_loss_count = 0

        for page in range(1, GECKO_MAX_PAGE + 1):
            if self.rate_limiter is None and page > 1 and self.page_delay:
                self.sleeper(self.page_delay)
            url = GECKO_URL.format(
                network=urllib.parse.quote(self.network, safe=""), page=page
            )
            page_count = page
            try:
                payload = self.client.get_json(url)
            except SourceError as exc:
                if not candidates:
                    raise
                errors.append(str(exc))
                page_fetch_error = True
                break
            page_batch = self.parse_payload(payload, observed_at)
            candidates.extend(page_batch.candidates)
            errors.extend(page_batch.errors)
            missing_fields.extend(page_batch.missing_fields)
            row_loss_count += page_batch.row_loss_count
            pool_rows_seen += page_batch.pool_rows_seen
            if page_batch.pool_rows_seen == 0:
                exhausted = True
                watermark_closed = True
                break
            if (
                page_batch.oldest_pool_created_at is not None
                and page_batch.oldest_pool_created_at <= since
            ):
                watermark_closed = True
                break

        oldest, newest = _time_bounds(candidates)
        gap_reason = None
        coverage_scope = "watermark_closed"
        if page_fetch_error:
            coverage_scope = "pagination_gap"
            gap_reason = "page_fetch_error_before_watermark"
        elif not watermark_closed:
            coverage_scope = "pagination_gap"
            gap_reason = "max_pages_before_watermark"
        if not candidates and exhausted:
            coverage_scope = "watermark_closed"
        missing_fields = _ordered_unique(missing_fields)
        status = "degraded" if errors or missing_fields or gap_reason else "success"
        return SourceBatch(
            candidates=candidates,
            status=status,
            errors=errors,
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            page_count=page_count,
            oldest_pool_created_at=oldest,
            newest_pool_created_at=newest,
            coverage_scope=coverage_scope,
            gap_reason=gap_reason,
            pool_rows_seen=pool_rows_seen,
            missing_fields=missing_fields,
            row_loss_count=row_loss_count,
        )

    def parse_payload(self, payload, observed_at):
        included = {}
        for resource in payload.get("included", []) if isinstance(payload, dict) else []:
            if not isinstance(resource, dict):
                continue
            address = _mapping(resource.get("attributes")).get("address")
            if address:
                included[resource.get("id")] = address

        candidates = []
        errors = []
        missing_fields = []
        row_loss_count = 0
        data = payload.get("data", []) if isinstance(payload, dict) else []
        if not isinstance(data, list):
            raise SourceError("GeckoTerminal data is not a list")
        for index, resource in enumerate(data):
            try:
                attrs = _mapping(resource.get("attributes"))
                relation = (
                    _mapping(resource.get("relationships"))
                    .get("base_token", {})
                    .get("data", {})
                )
                token_id = relation.get("id")
                token_address = included.get(token_id)
                if not token_address and token_id:
                    prefix = self.network + "_"
                    token_address = (
                        token_id[len(prefix) :] if token_id.startswith(prefix) else token_id
                    )
                pool_address = attrs.get("address")
                if not token_address or not pool_address:
                    errors.append("row %d missing token or pool address" % index)
                    row_loss_count += 1
                    continue
                valuation_usd, valuation_kind = _valuation(
                    attrs.get("market_cap_usd"), attrs.get("fdv_usd")
                )
                volume_map = _mapping(attrs.get("volume_usd"))
                transaction_map = _mapping(attrs.get("transactions"))
                h24_transactions = _mapping(transaction_map.get("h24"))
                h24_volume = volume_map.get("h24")
                if attrs.get("reserve_in_usd") is None:
                    missing_fields.append("liquidity_usd")
                if h24_volume is None:
                    missing_fields.append("volume_24h_usd")
                if not h24_transactions:
                    missing_fields.append("transactions_24h")
                candidates.append(
                    Candidate(
                        chain=self.chain,
                        token_address=str(token_address),
                        pool_address=str(pool_address),
                        source=self.source_name,
                        observed_at=observed_at,
                        valuation_usd=valuation_usd,
                        valuation_kind=valuation_kind,
                        liquidity_usd=_float(attrs.get("reserve_in_usd")),
                        volume_24h_usd=_float(h24_volume),
                        buys_24h=_int(h24_transactions.get("buys")),
                        sells_24h=_int(h24_transactions.get("sells")),
                        pool_created_at=_datetime(attrs.get("pool_created_at")),
                        discovery_type="unfiltered_new_pools",
                        candidate_scope="unfiltered_new_pools",
                        token_origin="unverified",
                        meme_classification="unclassified",
                    )
                )
            except (AttributeError, TypeError, ValueError) as exc:
                errors.append("row %d parse error: %s" % (index, exc.__class__.__name__))
                row_loss_count += 1
        oldest, newest = _time_bounds(candidates)
        missing_fields = _ordered_unique(missing_fields)
        return SourceBatch(
            candidates=candidates,
            status="degraded" if errors or missing_fields else "success",
            errors=errors,
            discovery_type="unfiltered_new_pools",
            candidate_scope="unfiltered_new_pools",
            page_count=1,
            oldest_pool_created_at=oldest,
            newest_pool_created_at=newest,
            coverage_scope="single_page_unbounded",
            pool_rows_seen=len(data),
            missing_fields=missing_fields,
            row_loss_count=row_loss_count,
        )


class DexScreenerAdapter:
    source_name = "dexscreener"

    def __init__(
        self,
        chain,
        api_chain,
        timeout=6.0,
        opener=None,
        max_profiles=20,
        budget=None,
    ):
        self.chain = chain
        self.api_chain = api_chain
        self.client = JsonHttpClient(timeout=timeout, opener=opener, budget=budget)
        self.max_profiles = max(1, min(int(max_profiles), 20))

    def fetch(self, observed_at, since=None):
        discovery_payloads = {}
        discovery_errors = []
        for label, url in (
            ("profiles", DEX_PROFILES_URL),
            ("boosts", DEX_BOOSTS_URL),
        ):
            try:
                payload = self.client.get_json(url)
                if not isinstance(payload, list):
                    raise SourceError(
                        "DexScreener %s payload is not a list" % label
                    )
                discovery_payloads[label] = payload
            except SourceError as exc:
                discovery_errors.append(str(exc))
        if not discovery_payloads:
            raise SourceError("; ".join(discovery_errors))

        addresses_by_source = {}
        for label in ("profiles", "boosts"):
            payload = discovery_payloads.get(label, [])
            addresses = []
            for profile in payload:
                if not isinstance(profile, dict):
                    continue
                if str(profile.get("chainId", "")).lower() != self.api_chain.lower():
                    continue
                address = profile.get("tokenAddress")
                if address and address not in addresses:
                    addresses.append(address)
            addresses_by_source[label] = addresses

        token_addresses = []
        positions = {"profiles": 0, "boosts": 0}
        while len(token_addresses) < self.max_profiles:
            added = False
            for label in ("profiles", "boosts"):
                values = addresses_by_source.get(label, [])
                while positions[label] < len(values):
                    address = values[positions[label]]
                    positions[label] += 1
                    if address not in token_addresses:
                        token_addresses.append(address)
                        added = True
                        break
                if len(token_addresses) >= self.max_profiles:
                    break
            if not added:
                break

        candidates = []
        errors = list(discovery_errors)
        missing_fields = []
        pool_rows_seen = 0
        row_loss_count = 0
        for address in token_addresses:
            url = DEX_PAIRS_URL.format(
                chain=urllib.parse.quote(self.api_chain, safe=""),
                token=urllib.parse.quote(str(address), safe=""),
            )
            try:
                pairs = self.client.get_json(url)
                batch = self.parse_pairs(
                    pairs, observed_at, requested_token=str(address)
                )
                candidates.extend(batch.candidates)
                errors.extend(batch.errors)
                missing_fields.extend(batch.missing_fields)
                pool_rows_seen += batch.pool_rows_seen
                row_loss_count += batch.row_loss_count
            except SourceError as exc:
                errors.append(str(exc))
        oldest, newest = _time_bounds(candidates)
        return SourceBatch(
            candidates=candidates,
            status="degraded",
            errors=errors,
            discovery_type="promoted_subset",
            candidate_scope="promoted_subset",
            page_count=1,
            oldest_pool_created_at=oldest,
            newest_pool_created_at=newest,
            coverage_scope="promoted_subset",
            gap_reason="discovery_endpoint_is_promoted_only",
            pool_rows_seen=pool_rows_seen,
            missing_fields=_ordered_unique(missing_fields),
            row_loss_count=row_loss_count,
        )

    def parse_pairs(self, payload, observed_at, requested_token=None):
        if isinstance(payload, dict):
            pairs = payload.get("pairs", [])
        else:
            pairs = payload
        if not isinstance(pairs, list):
            raise SourceError("DexScreener pair payload is not a list")
        candidates = []
        errors = []
        missing_fields = []
        row_loss_count = 0
        for index, pair in enumerate(pairs):
            if not isinstance(pair, dict):
                errors.append("pair %d is not an object" % index)
                row_loss_count += 1
                continue
            pair_chain = str(pair.get("chainId", ""))
            if pair_chain and pair_chain.lower() != self.api_chain.lower():
                continue
            base_token = _mapping(pair.get("baseToken"))
            token_address = base_token.get("address")
            pool_address = pair.get("pairAddress")
            if requested_token is not None and not self._same_address(
                token_address, requested_token
            ):
                # Dex pair valuation fields describe the base token. If discovery
                # found the quote token, recording the other base would corrupt
                # the canonical candidate cohort, so this pair is skipped.
                continue
            if not token_address or not pool_address:
                errors.append("pair %d missing token or pool address" % index)
                row_loss_count += 1
                continue
            valuation_usd, valuation_kind = _valuation(
                pair.get("marketCap"), pair.get("fdv")
            )
            liquidity = _mapping(pair.get("liquidity")).get("usd")
            volume = _mapping(pair.get("volume")).get("h24")
            txns = _mapping(_mapping(pair.get("txns")).get("h24"))
            if liquidity is None:
                missing_fields.append("liquidity_usd")
            if volume is None:
                missing_fields.append("volume_24h_usd")
            if not txns:
                missing_fields.append("transactions_24h")
            candidates.append(
                Candidate(
                    chain=self.chain,
                    token_address=str(token_address),
                    pool_address=str(pool_address),
                    source=self.source_name,
                    observed_at=observed_at,
                    valuation_usd=valuation_usd,
                    valuation_kind=valuation_kind,
                    liquidity_usd=_float(liquidity),
                    volume_24h_usd=_float(volume),
                    buys_24h=_int(txns.get("buys")),
                    sells_24h=_int(txns.get("sells")),
                    pool_created_at=_datetime(pair.get("pairCreatedAt")),
                    discovery_type="promoted_subset",
                    candidate_scope="promoted_subset",
                    token_origin="unverified",
                    meme_classification="unclassified",
                )
            )
        oldest, newest = _time_bounds(candidates)
        missing_fields = _ordered_unique(missing_fields)
        return SourceBatch(
            candidates=candidates,
            status="degraded",
            errors=errors,
            discovery_type="promoted_subset",
            candidate_scope="promoted_subset",
            page_count=1,
            oldest_pool_created_at=oldest,
            newest_pool_created_at=newest,
            coverage_scope="promoted_subset",
            gap_reason="discovery_endpoint_is_promoted_only",
            pool_rows_seen=len(pairs),
            missing_fields=missing_fields,
            row_loss_count=row_loss_count,
        )

    def _same_address(self, left, right):
        if left is None or right is None:
            return False
        if self.chain == "solana":
            return str(left) == str(right)
        return str(left).lower() == str(right).lower()


class DexScreenerRefreshAdapter:
    """Refresh an explicit known-token cohort through the official batch endpoint."""

    source_name = "dexscreener_refresh"

    def __init__(self, chain, api_chain, timeout=6.0, opener=None, budget=None):
        self.chain = chain
        self.api_chain = api_chain
        self.client = JsonHttpClient(timeout=timeout, opener=opener, budget=budget)
        self.pair_parser = DexScreenerAdapter(
            chain, api_chain, timeout=timeout, opener=opener, budget=budget
        )

    def fetch_tokens(self, token_addresses, observed_at):
        addresses = _ordered_unique(
            canonical_address(self.chain, address) for address in token_addresses
        )
        candidates = []
        errors = []
        missing_fields = []
        pool_rows_seen = 0
        row_loss_count = 0
        batch_count = 0
        for offset in range(0, len(addresses), DEX_REFRESH_BATCH_SIZE):
            chunk = addresses[offset : offset + DEX_REFRESH_BATCH_SIZE]
            if not chunk:
                continue
            batch_count += 1
            url = DEX_TOKENS_URL.format(
                chain=urllib.parse.quote(self.api_chain, safe=""),
                tokens=urllib.parse.quote(",".join(chunk), safe=","),
            )
            try:
                payload = self.client.get_json(url)
            except SourceError as exc:
                errors.append(
                    "refresh_batch_error:%d:%s" % (len(chunk), str(exc))
                )
                if "budget exhausted" in str(exc).lower():
                    break
                continue
            if isinstance(payload, dict):
                pairs = payload.get("pairs", [])
            else:
                pairs = payload
            if not isinstance(pairs, list):
                errors.append("refresh batch payload is not a list")
                continue
            pool_rows_seen += len(pairs)
            for address in chunk:
                matching = []
                for pair in pairs:
                    if not isinstance(pair, dict):
                        continue
                    pair_chain = str(pair.get("chainId", ""))
                    if pair_chain and pair_chain.lower() != self.api_chain.lower():
                        continue
                    base_address = _mapping(pair.get("baseToken")).get("address")
                    if not self.pair_parser._same_address(base_address, address):
                        continue
                    liquidity = _float(_mapping(pair.get("liquidity")).get("usd"))
                    if liquidity is None or liquidity <= 0:
                        continue
                    matching.append(pair)
                if not matching:
                    errors.append("refresh_no_positive_base_pool:%s" % address)
                    continue
                parsed = self.pair_parser.parse_pairs(
                    matching, observed_at, requested_token=address
                )
                missing_fields.extend(parsed.missing_fields)
                errors.extend(parsed.errors)
                row_loss_count += parsed.row_loss_count
                if not parsed.candidates:
                    errors.append("refresh_parse_empty:%s" % address)
                    continue
                best = max(
                    parsed.candidates,
                    key=lambda item: (
                        item.liquidity_usd
                        if item.liquidity_usd is not None
                        else -1.0,
                        item.volume_24h_usd
                        if item.volume_24h_usd is not None
                        else -1.0,
                        item.pool_address,
                    ),
                )
                candidates.append(
                    replace(
                        best,
                        source=self.source_name,
                        discovery_type="refresh",
                        candidate_scope="known_token_refresh",
                        observation_phase="tracking",
                    )
                )
        missing_fields = _ordered_unique(missing_fields)
        return SourceBatch(
            candidates=candidates,
            status="degraded" if errors or missing_fields else "success",
            errors=errors,
            discovery_type="refresh",
            candidate_scope="known_token_refresh",
            page_count=batch_count,
            coverage_scope="known_token_refresh",
            pool_rows_seen=pool_rows_seen,
            missing_fields=missing_fields,
            row_loss_count=row_loss_count,
        )


class CompositeAdapter:
    """Use the first non-empty source while preserving fallback degradation."""

    def __init__(self, adapters):
        if not adapters:
            raise ValueError("at least one adapter is required")
        self.adapters = list(adapters)

    def fetch(self, observed_at, since=None):
        errors = []
        successful_empty = False
        for index, adapter in enumerate(self.adapters):
            try:
                batch = adapter.fetch(observed_at, since=since)
            except SourceError as exc:
                errors.append(str(exc))
                continue
            successful_empty = successful_empty or not batch.candidates
            if batch.candidates:
                combined_errors = errors + list(batch.errors)
                fallback_used = index > 0
                status = (
                    "degraded"
                    if fallback_used or combined_errors or batch.status == "degraded"
                    else "success"
                )
                return replace(batch, status=status, errors=combined_errors)
            errors.extend(batch.errors)
        if successful_empty:
            return SourceBatch(
                [],
                "degraded" if errors else "success",
                errors,
                coverage_scope="empty_source",
            )
        raise SourceError("; ".join(errors) if errors else "all sources failed")


class FixtureGeckoAdapter:
    def __init__(self, path, chain, network):
        self.path = Path(path)
        self.parser = GeckoTerminalAdapter(chain, network)

    def fetch(self, observed_at, since=None):
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return replace(
            self.parser.parse_payload(payload, observed_at),
            coverage_scope="fixture_snapshot",
        )


class FixtureDexAdapter:
    def __init__(self, path, chain, api_chain):
        self.path = Path(path)
        self.parser = DexScreenerAdapter(chain, api_chain)

    def fetch(self, observed_at, since=None):
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return replace(
            self.parser.parse_pairs(payload, observed_at),
            coverage_scope="promoted_subset",
        )


NETWORK_CONFIG = {
    "solana": {"gecko": "solana", "dex": "solana"},
    "base": {"gecko": "base", "dex": "base"},
    "bsc": {"gecko": "bsc", "dex": "bsc"},
    "robinhood": {"dex": "robinhood"},
}


def default_adapters(
    networks, timeout=6.0, opener=None, budget=None, gecko_rate_limiter=None
):
    adapters = {}
    limiter = gecko_rate_limiter or SharedRateLimiter(
        min_interval=2.05, budget=budget
    )
    for chain in networks:
        config = NETWORK_CONFIG[chain]
        if chain == "robinhood":
            adapters[chain] = RobinhoodPoolEventAdapter(
                timeout=timeout,
                opener=opener,
                budget=budget,
                enricher=DexScreenerRefreshAdapter(
                    chain,
                    config["dex"],
                    timeout=timeout,
                    opener=opener,
                    budget=budget,
                ),
            )
            continue
        sources = []
        # 2026-07-30: 有链上日志源的 EVM 链优先走 RPC 直采, 聚合器降为兜底。
        # 动机是实测: base/solana 走 DexScreener 分页撞 HTTP 429
        # (gap_reason=page_fetch_error_before_watermark), 而走 RPC 的 robinhood 是
        # coverage_scope=fresh_head_window_closed / status=success。CompositeAdapter
        # 本就是"first non-empty source with fallback degradation", 把链上源放首位
        # 即可, 无需改其语义。
        chain_source = CHAIN_LOG_SOURCES.get(chain)
        if chain_source and chain_source.get("known_emitters"):
            topics = tuple(chain_source["topics"].values())
            sources.append(
                EvmPoolEventAdapter(
                    timeout=timeout,
                    opener=opener,
                    budget=budget,
                    chain=chain,
                    topics=topics,
                    known_emitters=dict(chain_source["known_emitters"]),
                    core_tokens=frozenset(
                        a.lower() for a in chain_source.get("core_tokens", ())
                    ),
                    source_name="rpc_pool_events",
                    log_client=chain_log_client(chain, budget=budget),
                    enricher=DexScreenerRefreshAdapter(
                        chain, config["dex"], timeout=timeout,
                        opener=opener, budget=budget,
                    ),
                )
            )
        if "gecko" in config:
            sources.append(
                GeckoTerminalAdapter(
                    chain,
                    config["gecko"],
                    timeout=timeout,
                    opener=opener,
                    page_delay=0.0,
                    budget=budget,
                    rate_limiter=limiter,
                )
            )
        sources.append(
            DexScreenerAdapter(
                chain,
                config["dex"],
                timeout=timeout,
                opener=opener,
                budget=budget,
            )
        )
        adapters[chain] = CompositeAdapter(sources)
    return adapters


def default_refresh_adapters(networks, timeout=6.0, opener=None, budget=None):
    return {
        chain: DexScreenerRefreshAdapter(
            chain,
            NETWORK_CONFIG[chain]["dex"],
            timeout=timeout,
            opener=opener,
            budget=budget,
        )
        for chain in networks
    }


def fixture_adapters(fixtures_dir, networks):
    fixtures_dir = Path(fixtures_dir)
    adapters = {}
    for chain in networks:
        config = NETWORK_CONFIG[chain]
        if "gecko" in config:
            path = fixtures_dir / ("geckoterminal_%s.json" % chain)
            adapters[chain] = FixtureGeckoAdapter(path, chain, config["gecko"])
        else:
            path = fixtures_dir / ("dexscreener_%s.json" % chain)
            adapters[chain] = FixtureDexAdapter(path, chain, config["dex"])
    return adapters
