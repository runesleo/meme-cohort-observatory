#!/usr/bin/env python3
"""Fail closed on private paths, obvious embedded secrets, or wallet/trade imports."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEXT_SUFFIXES = {".py", ".md", ".toml", ".json", ".txt", ".yml", ".yaml"}
SECRET_PATTERNS = {
    "private_key_pem": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "assigned_secret": re.compile(r"(?i)(?:api[_-]?key|private[_-]?key|bot[_-]?token|mnemonic|seed[_-]?phrase)\s*[:=]\s*['\"][^'\"]{8,}['\"]"),
    "openai_key": re.compile(r"sk-[A-Za-z0-9_-]{20,}"),
}
FORBIDDEN_IMPORTS = {"web3", "eth_account", "solders", "solana", "ccxt"}
FORBIDDEN_CALLS = {
    "send_transaction",
    "sign_transaction",
    "broadcast_transaction",
    "swap",
    "approve",
    "place_order",
    "create_order",
}

failures = []
for path in ROOT.rglob("*"):
    if not path.is_file() or ".git" in path.parts or path.suffix.lower() not in TEXT_SUFFIXES:
        continue
    text = path.read_text(encoding="utf-8", errors="replace")
    private_prefix = "/Users/" + "zhangxu"
    if private_prefix in text:
        failures.append(f"absolute_private_path:{path.relative_to(ROOT)}")
    for name, pattern in SECRET_PATTERNS.items():
        if pattern.search(text):
            failures.append(f"{name}:{path.relative_to(ROOT)}")
    if path.suffix == ".py" and "tests" not in path.parts and path.name != Path(__file__).name:
        tree = ast.parse(text, filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                roots = {alias.name.split(".")[0] for alias in node.names}
                if roots & FORBIDDEN_IMPORTS:
                    failures.append(
                        f"wallet_trade_import:{path.relative_to(ROOT)}:{sorted(roots & FORBIDDEN_IMPORTS)}"
                    )
            elif isinstance(node, ast.ImportFrom) and node.module:
                root = node.module.split(".")[0]
                if root in FORBIDDEN_IMPORTS:
                    failures.append(f"wallet_trade_import:{path.relative_to(ROOT)}:{node.module}")
            elif isinstance(node, ast.Call):
                name = (
                    node.func.attr
                    if isinstance(node.func, ast.Attribute)
                    else node.func.id
                    if isinstance(node.func, ast.Name)
                    else None
                )
                if name in FORBIDDEN_CALLS:
                    failures.append(f"wallet_trade_call:{path.relative_to(ROOT)}:{name}")

if failures:
    print("FAIL")
    print("\n".join(sorted(set(failures))))
    raise SystemExit(1)
print("PASS: no private absolute path, embedded secret, or wallet/trade execution surface")
