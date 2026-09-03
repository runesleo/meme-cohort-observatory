"""Deterministic, coverage-aware reports from collector state."""

from __future__ import annotations

import json
from pathlib import Path


def load_summary(state_dir: str | Path) -> dict:
    path = Path(state_dir).expanduser().resolve() / "latest_summary.json"
    if not path.exists():
        raise FileNotFoundError(f"summary not found: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not isinstance(value.get("chains"), dict):
        raise ValueError("latest_summary.json has an unsupported schema")
    return value


def _crossings(info: dict, valuation_track: str) -> dict:
    return (
        info.get("valuation_headlines", {})
        .get(valuation_track, {})
        .get("raw_threshold_crossings", {})
    )


def report_rows(summary: dict) -> list[dict]:
    rows = []
    for chain, info in sorted(summary.get("chains", {}).items()):
        coverage = info.get("coverage") or {}
        market_cap = _crossings(info, "observed_market_cap")
        fdv = _crossings(info, "fdv_proxy")
        rows.append(
            {
                "chain": chain,
                "status": coverage.get("status") or "unknown",
                "coverage_scope": coverage.get("coverage_scope") or "unknown",
                "frame_complete": bool(coverage.get("admission_frame_complete")),
                "discovered": coverage.get("discovered_tokens"),
                "admitted": coverage.get("admission_admitted"),
                "dropped": coverage.get("admission_dropped"),
                "sample_fraction": coverage.get("admission_sample_fraction"),
                "missing_refresh_slots": coverage.get("refresh_deferred"),
                "mcap_c1": market_cap.get("RAW_C1_CROSSING"),
                "mcap_c3": market_cap.get("RAW_C3_CROSSING"),
                "mcap_c10": market_cap.get("RAW_C10_CROSSING"),
                "fdv_c1": fdv.get("RAW_C1_CROSSING"),
                "gap_reason": coverage.get("gap_reason"),
            }
        )
    return rows


def _display(value) -> str:
    if value is None:
        return "UNKNOWN"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    return str(value)


def render_markdown(summary: dict) -> str:
    lines = [
        "# Launch Cohort Report",
        "",
        f"Generated: {_display(summary.get('generated_at'))}",
        f"Qualification: `{_display(summary.get('qualification_status'))}`",
        f"Security: `{_display(summary.get('security_status'))}`",
        f"Executability: `{_display(summary.get('executability_status'))}`",
        f"Main-chain score: `{_display(summary.get('main_chain_score_status'))}`",
        "",
        "| Chain | Status | Coverage | Comparable frame | Discovered | Admitted | Dropped | Sample fraction | Missing refresh | MCAP ≥$1M | MCAP ≥$3M | MCAP ≥$10M |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    caveats = []
    for row in report_rows(summary):
        display_row = {key: _display(value) for key, value in row.items()}
        lines.append(
            "| {chain} | {status} | {coverage_scope} | {frame_complete} | {discovered} | {admitted} | {dropped} | {sample_fraction} | {missing_refresh_slots} | {mcap_c1} | {mcap_c3} | {mcap_c10} |".format(
                **display_row
            )
        )
        if row["status"] != "success" or not row["frame_complete"] or row["gap_reason"]:
            caveats.append(
                f"- `{row['chain']}`: status={row['status']}; comparable_frame={row['frame_complete']}; gap={_display(row['gap_reason'])}."
            )
    lines.extend(
        [
            "",
            "## Interpretation limits",
            "",
            "- Crossings are observed provider values, not reconstructed intraperiod ATHs.",
            "- Market cap and FDV remain separate tracks.",
            "- Discovery is not a meme classification, security verdict, or buy signal.",
            "- Incomplete coverage is not a valid denominator for comparable cohort rates.",
        ]
    )
    if caveats:
        lines.extend(["", "## Coverage caveats", "", *caveats])
    return "\n".join(lines) + "\n"


def render_table(summary: dict) -> str:
    headers = ["chain", "status", "coverage", "complete", "seen", "admit", "drop", "mcap>=1m"]
    data = []
    for row in report_rows(summary):
        data.append(
            [
                row["chain"],
                row["status"],
                row["coverage_scope"],
                _display(row["frame_complete"]),
                _display(row["discovered"]),
                _display(row["admitted"]),
                _display(row["dropped"]),
                _display(row["mcap_c1"]),
            ]
        )
    widths = [len(header) for header in headers]
    for values in data:
        widths = [max(widths[index], len(str(values[index]))) for index in range(len(headers))]

    def line(values):
        return "  ".join(str(values[index]).ljust(widths[index]) for index in range(len(values))).rstrip()

    output = [line(headers), line(["-" * width for width in widths])]
    output.extend(line(values) for values in data)
    output.append("")
    output.append(
        f"qualification={_display(summary.get('qualification_status'))}; "
        f"security={_display(summary.get('security_status'))}; "
        f"executability={_display(summary.get('executability_status'))}"
    )
    return "\n".join(output) + "\n"


def render_report(summary: dict, format_name: str) -> str:
    if format_name == "markdown":
        return render_markdown(summary)
    if format_name == "table":
        return render_table(summary)
    if format_name == "json":
        return json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    raise ValueError(f"unsupported report format: {format_name}")
