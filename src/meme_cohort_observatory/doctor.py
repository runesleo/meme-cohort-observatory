"""Local, non-network health checks."""

from __future__ import annotations

import json
import platform
import sys
from pathlib import Path

from .adapters import NETWORK_CONFIG


def doctor_payload(state_dir: str | Path | None = None) -> dict:
    state = Path(state_dir).expanduser().resolve() if state_dir else None
    checks = {
        "python_supported": sys.version_info >= (3, 10),
        "runtime_dependency_count": 0,
        "configured_networks": sorted(NETWORK_CONFIG),
        "read_only_public_sources": True,
        "wallet_surface": False,
        "trade_surface": False,
    }
    if state is not None:
        checks.update(
            {
                "state_dir": str(state),
                "state_dir_exists": state.exists(),
                "database_exists": (state / "cohorts.sqlite3").exists(),
                "summary_exists": (state / "latest_summary.json").exists(),
            }
        )
    return {
        "status": "pass" if checks["python_supported"] else "fail",
        "python": platform.python_version(),
        "checks": checks,
    }


def render_doctor(state_dir=None, as_json=False) -> str:
    payload = doctor_payload(state_dir)
    if as_json:
        return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    lines = [f"status: {payload['status']}", f"python: {payload['python']}"]
    for key, value in payload["checks"].items():
        lines.append(f"{key}: {value}")
    return "\n".join(lines) + "\n"
