"""RpcLogClient 契约测试 —— 官方 RPC 替代 Blockscout 的等价性与时间戳补齐。

动机 (2026-07-30 实测): 官方 RPC 50,000 区块 getLogs = 2.34s (21,331 block/s),
Blockscout 2,000 区块 = 1.40s + 2.05s 强制间隔 (实效 580 block/s), 差 37 倍。
真实后果见 artifacts/2026-07-29-solana-coverage-budget-deadlock.md: RH 单链一轮
26,444s (7.3 小时) 吃满预算, solana/base/bsc 全部 run_budget_exhausted_before_chain。

本测试用注入的假 runner 验证协议正确性, 不打真实网络。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from meme_cohort_observatory.adapters import RpcLogClient, SourceError  # noqa: E402


class FakeRpc:
    """记录每次请求, 按 method 返回预置结果。"""

    def __init__(self, *, head=1000, logs=None, timestamps=None):
        self.head = head
        self.logs = logs if logs is not None else []
        self.timestamps = timestamps or {}
        self.calls = []
        self.batch_calls = 0

    def __call__(self, payload):
        if isinstance(payload, list):
            self.batch_calls += 1
            self.calls.append(("batch", len(payload)))
            out = []
            for item in payload:
                bn = int(item["params"][0], 16)
                ts = self.timestamps.get(bn)
                out.append({
                    "jsonrpc": "2.0", "id": item["id"],
                    "result": None if ts is None else {"timestamp": hex(ts)},
                })
            return out
        method = payload["method"]
        self.calls.append((method, payload["params"]))
        if method == "eth_blockNumber":
            return {"result": hex(self.head)}
        if method == "eth_getBlockByNumber":
            bn = int(payload["params"][0], 16)
            ts = self.timestamps.get(bn, bn * 10)
            return {"result": {"hash": "0xabc%d" % bn, "timestamp": hex(ts),
                               "number": hex(bn)}}
        if method == "eth_getLogs":
            p = payload["params"][0]
            lo, hi = int(p["fromBlock"], 16), int(p["toBlock"], 16)
            return {"result": [r for r in self.logs
                               if lo <= int(r["blockNumber"], 16) <= hi]}
        raise AssertionError("unexpected method %s" % method)


def _client(fake, **kw):
    return RpcLogClient(runner=fake, **kw)


def test_latest_indexed_block_reads_head() -> None:
    c = _client(FakeRpc(head=23196432))
    assert c.latest_indexed_block() == 23196432


def test_rpc_error_becomes_source_error() -> None:
    def boom(_payload):
        return {"error": {"code": -32000, "message": "limit exceeded"}}
    with pytest.raises(SourceError):
        _client(boom).latest_indexed_block()


def test_span_larger_than_cap_is_split_not_dropped() -> None:
    """核心等价性: 超过 max_block_span 必须切分并合并, 不能静默丢一半。"""
    logs = [{"blockNumber": hex(b), "topics": ["0xt"], "address": "0xa", "data": "0x"}
            for b in (10, 5000, 15000, 24999)]
    fake = FakeRpc(head=30000, logs=logs)
    c = _client(fake, max_block_span=10000, resolve_timestamps=False)
    rows = c.get_logs("0xt", 1, 25000)
    assert len(rows) == 4, "切分后必须无遗漏"
    spans = [p for m, p in fake.calls if m == "eth_getLogs"]
    assert len(spans) == 3, "25000 区块按 10000 上限应切 3 段"


def test_timestamps_filled_only_for_blocks_with_events() -> None:
    """成本随事件数而非区块数增长 —— 这是能用 RPC 的前提。"""
    logs = [{"blockNumber": hex(b), "topics": ["0xt"], "address": "0xa", "data": "0x"}
            for b in (100, 100, 205)]
    fake = FakeRpc(head=1000, logs=logs, timestamps={100: 1700000000, 205: 1700002050})
    c = _client(fake, resolve_timestamps=True, batch_size=50)
    rows = c.get_logs("0xt", 1, 1000)
    assert [r["timeStamp"] for r in rows] == [1700000000, 1700000000, 1700002050]
    assert fake.batch_calls == 1
    # 3 条 log 只涉及 2 个唯一区块 → batch 只问 2 个
    assert ("batch", 2) in fake.calls


def test_timestamp_cache_avoids_refetch() -> None:
    logs = [{"blockNumber": hex(100), "topics": ["0xt"], "address": "0xa", "data": "0x"}]
    fake = FakeRpc(head=1000, logs=logs, timestamps={100: 1700000000})
    c = _client(fake, resolve_timestamps=True)
    c.get_logs("0xt", 1, 500)
    c.get_logs("0xt", 1, 500)
    assert fake.batch_calls == 1, "同一区块的时间戳不应重复拉取"


def test_resolve_timestamps_off_leaves_field_absent() -> None:
    """关闭时留空而非填近似值 —— 区块时间可按 blockNumber 事后精确回填。"""
    logs = [{"blockNumber": hex(100), "topics": ["0xt"], "address": "0xa", "data": "0x"}]
    fake = FakeRpc(head=1000, logs=logs)
    c = _client(fake, resolve_timestamps=False)
    rows = c.get_logs("0xt", 1, 500)
    assert "timeStamp" not in rows[0]
    assert fake.batch_calls == 0


def test_block_by_time_binary_search_hits_first_block_at_or_after() -> None:
    # timestamps: block b → b*10
    fake = FakeRpc(head=1000)
    c = _client(fake)
    assert c.block_by_time(5000) == 500
    assert c.block_by_time(5001) == 501


def test_block_by_time_clamps_to_head_when_target_in_future() -> None:
    fake = FakeRpc(head=1000)
    c = _client(fake)
    assert c.block_by_time(99999999) == 1000


def test_inverted_range_returns_empty_without_calling_rpc() -> None:
    fake = FakeRpc(head=1000)
    c = _client(fake)
    assert c.get_logs("0xt", 500, 400) == []
    assert fake.calls == []


class TestChainLogSources:
    """多链配置的守卫 —— factory 白名单是安全门, 配置错了等于把伪造事件当真。"""

    def test_base_span_respects_provider_hard_limit(self) -> None:
        from meme_cohort_observatory.adapters import CHAIN_LOG_SOURCES
        # 超过 10,000 会被 base 节点拒: -32614 "limited to a 10,000 range"
        assert CHAIN_LOG_SOURCES["base"]["max_block_span"] == 10000

    def test_verified_emitters_are_lowercase_and_well_formed(self) -> None:
        from meme_cohort_observatory.adapters import CHAIN_LOG_SOURCES
        for addr in CHAIN_LOG_SOURCES["base"]["known_emitters"]:
            assert addr == addr.lower(), "emitter 必须小写, 否则匹配会漏"
            assert addr.startswith("0x") and len(addr) == 42

    def test_unverified_emitters_never_leak_into_whitelist(self) -> None:
        """实测见过 ≠ 可信。未双通过的 emitter 不得进白名单。"""
        from meme_cohort_observatory.adapters import CHAIN_LOG_SOURCES
        base = CHAIN_LOG_SOURCES["base"]
        overlap = set(base["known_emitters"]) & set(base["unverified_emitters_seen"])
        assert overlap == set(), "未验证 emitter 泄漏进白名单: %s" % overlap

    def test_uniswap_v2_factory_matches_official_deployment(self) -> None:
        """链上实测(175 条居首) + 官方 developers.uniswap.org v2 deployments 双通过。"""
        from meme_cohort_observatory.adapters import CHAIN_LOG_SOURCES
        official = "0x8909Dc15e40173Ff4699343b6eB8132c65e18eC6".lower()
        assert CHAIN_LOG_SOURCES["base"]["known_emitters"][official][0] == "uniswap_v2"

    def test_chain_log_client_uses_measured_params_not_defaults(self) -> None:
        from meme_cohort_observatory.adapters import chain_log_client
        c = chain_log_client("base", runner=lambda payload: {"result": "0x1"})
        assert c.rpc_url == "https://mainnet.base.org"
        assert c.max_block_span == 10000

    def test_unknown_chain_raises_instead_of_silently_defaulting(self) -> None:
        import pytest as _pytest
        from meme_cohort_observatory.adapters import SourceError, chain_log_client
        with _pytest.raises(SourceError):
            chain_log_client("bsc")  # 三个公共节点均不可用, 故意未配置


class TestCurlRetry:
    """瞬时故障重试 —— 2026-07-30 首次真实 fetch 即遇 curl rc=35 (SSL 握手失败)。"""

    def _client_with_rc(self, rcs):
        """按序返回给定退出码, 0 表示成功。"""
        from meme_cohort_observatory.adapters import RpcLogClient
        calls = {"n": 0}
        seq = list(rcs)

        class FakeProc:
            def __init__(self, rc):
                self.returncode = rc
                self.stdout = '{"result":"0x1"}' if rc == 0 else ""
                self.stderr = ""

        def fake_run(_cmd, **_kw):
            rc = seq[min(calls["n"], len(seq) - 1)]
            calls["n"] += 1
            return FakeProc(rc)

        c = RpcLogClient()
        from meme_cohort_observatory import adapters as _a
        c._orig_run = _a.subprocess.run
        _a.subprocess.run = fake_run
        return c, calls, _a

    def test_retryable_rc_is_retried_then_succeeds(self) -> None:
        c, calls, mod = self._client_with_rc([35, 0])
        try:
            assert c.latest_indexed_block() == 1
            assert calls["n"] == 2, "rc=35 应重试一次后成功"
        finally:
            mod.subprocess.run = c._orig_run

    def test_non_retryable_rc_fails_fast(self) -> None:
        from meme_cohort_observatory.adapters import SourceError
        c, calls, mod = self._client_with_rc([2, 0])
        try:
            with pytest.raises(SourceError):
                c.latest_indexed_block()
            assert calls["n"] == 1, "不可恢复的 rc 不应重试"
        finally:
            mod.subprocess.run = c._orig_run

    def test_retry_gives_up_after_attempts(self) -> None:
        from meme_cohort_observatory.adapters import SourceError
        c, calls, mod = self._client_with_rc([35])
        try:
            with pytest.raises(SourceError):
                c.latest_indexed_block()
            assert calls["n"] == 3, "默认 3 次尝试后放弃"
        finally:
            mod.subprocess.run = c._orig_run
