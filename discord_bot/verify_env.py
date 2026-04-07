#!/usr/bin/env python3
"""
Print safe diagnostics for discord_bot/.env (never prints the bot token).
Run: python3 verify_env.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def _parse_value(raw_file: str, key: str) -> str | None:
    for line in raw_file.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        m = re.match(rf"{re.escape(key)}\s*=\s*(.*)$", s)
        if m:
            return m.group(1).strip()
    return None


def _strip_env(val: str) -> str:
    t = val.replace("\ufeff", "").replace("\r", "").strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in ("'", '"'):
        t = t[1:-1].strip()
    return t


def main() -> int:
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        print(f"Missing {env_path}", file=sys.stderr)
        return 1
    raw = env_path.read_text(encoding="utf-8")
    print(f"path: {env_path}\n")

    tok = _parse_value(raw, "DISCORD_BOT_TOKEN")
    if tok is None:
        print("DISCORD_BOT_TOKEN: (no line found)")
    else:
        tok = _strip_env(tok)
        print(f"DISCORD_BOT_TOKEN: length={len(tok)} dot_count={tok.count('.')} segments={len(tok.split('.'))}")

    gid_raw = _parse_value(raw, "DISCORD_GUILD_ID")
    if gid_raw is None:
        print("DISCORD_GUILD_ID: (no line found) — slash commands use slow global sync")
    else:
        gid = _strip_env(gid_raw)
        print(f"DISCORD_GUILD_ID raw repr: {gid_raw[:40]!r}{'…' if len(gid_raw) > 40 else ''}")
        print(f"DISCORD_GUILD_ID normalized: {gid!r}  (must be digits only)")
        print(f"  valid_snowflake: {gid.isdigit() and len(gid) >= 17}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
