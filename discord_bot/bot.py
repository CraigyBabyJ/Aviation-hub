#!/usr/bin/env python3
"""
Aviation Hub Discord bot: slash commands that read from the local widget HTTP API
(same JSON as `data_fetch/src/widget_server.py` serves alongside the ingestor).

Environment:
  DISCORD_BOT_TOKEN       — required; **Bot** token (Developer Portal → Bot → Reset Token).
  AVIATION_HUB_BASE_URL   — optional; default http://127.0.0.1:4010
  DISCORD_GUILD_ID        — optional; sync slash commands to this guild only (faster while testing)

  /info invite & support (optional):
  AVIATION_HUB_ADD_BOT_URL       — full OAuth2 “add bot” URL; overrides auto-built link
  DISCORD_APPLICATION_ID  — Application ID (same as OAuth client_id); used to build add-bot link if
                            AVIATION_HUB_ADD_BOT_URL is unset
  DISCORD_CLIENT_ID       — alias for DISCORD_APPLICATION_ID (either may be set)
  AVIATION_HUB_SUPPORT_SERVER_URL — support Discord invite (e.g. https://discord.gg/…)

  Full GND + TWR airport alerts (optional, configured per Discord server):
  DISCORD_FULL_GND_TWR_ALERT_POLL_SECONDS — poll interval; default 60
  DISCORD_FULL_GND_TWR_ALERT_STATE_FILE  — optional config/state file path; default beside bot.py

Utility slash commands (no hub call): /help lists every command’s description from this tree; /info
shows Aviation Hub text and the invite links above; /ping shows Discord gateway latency.

Discord Developer Portal:
  • **Public Key** — not used here (gateway bot, not an interactions HTTP endpoint).
"""
from __future__ import annotations

import io
import json
import logging
import math
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

LOG = logging.getLogger("aviation_hub.discord")
FULL_GND_TWR_ALERT_MIN_CONTROLLERS = 3


def _normalize_snowflake_env(raw: str | None) -> str:
    """Strip .env junk from numeric Discord IDs (guild, etc.)."""
    if not raw:
        return ""
    t = raw.replace("\ufeff", "").replace("\r", "").strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in ("'", '"'):
        t = t[1:-1].strip()
    return t


def _normalize_discord_bot_token(raw: str | None) -> str:
    """Strip whitespace, BOM, CR, optional quotes (common in .env / Windows copy-paste)."""
    if not raw:
        return ""
    t = raw.replace("\ufeff", "").replace("\r", "").strip()
    if len(t) >= 2 and t[0] == t[-1] and t[0] in ("'", '"'):
        t = t[1:-1].strip()
    if t.lower().startswith("bot "):
        t = t[4:].strip()
    return t


def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw.strip())
    except ValueError:
        LOG.warning("Invalid integer env %s=%r; using %s", name, raw, default)
        return default
    return max(min_value, min(max_value, value))


def _hub_base() -> str:
    return os.environ.get("AVIATION_HUB_BASE_URL", "http://127.0.0.1:4010").rstrip("/")



def _hub_add_invite_url() -> str | None:
    """Full OAuth invite, or built from Application ID + default permissions."""
    url = os.environ.get("AVIATION_HUB_ADD_BOT_URL", "").strip()
    if url:
        return url
    app_id = (
        os.environ.get("DISCORD_APPLICATION_ID", "").strip()
        or os.environ.get("DISCORD_CLIENT_ID", "").strip()
    )
    if not app_id:
        return None
    # View channel, send messages, embed links, read history — enough for slash replies in guild text.
    perms = 84992
    return (
        "https://discord.com/oauth2/authorize"
        f"?client_id={app_id}&permissions={perms}&scope=bot%20applications.commands"
    )


def _hub_support_server_url() -> str | None:
    u = os.environ.get("AVIATION_HUB_SUPPORT_SERVER_URL", "").strip()
    return u or None


def _full_gnd_twr_alert_poll_seconds() -> int:
    return _env_int("DISCORD_FULL_GND_TWR_ALERT_POLL_SECONDS", 60, 30, 3600)


def _full_gnd_twr_alert_state_file() -> Path:
    raw = os.environ.get("DISCORD_FULL_GND_TWR_ALERT_STATE_FILE", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parent / ".full_ground_tower_alerts.json"


def _airport_controller_count(airport: dict[str, Any]) -> int:
    try:
        return int(airport.get("controller_count") or 0)
    except (TypeError, ValueError):
        controllers = airport.get("controllers") or []
        return len(controllers) if isinstance(controllers, list) else 0


def _hub_url(path: str, params: dict[str, Any]) -> str:
    q = {k: str(v) for k, v in params.items() if v is not None}
    encoded = urlencode(q)
    return f"{_hub_base()}{path}" + (f"?{encoded}" if encoded else "")


async def _hub_get(session: aiohttp.ClientSession, path: str, **params: Any) -> tuple[int, Any]:
    url = _hub_url(path, params)
    try:
        async with session.get(url) as resp:
            text = await resp.text()
            try:
                data = json.loads(text) if text else {}
            except json.JSONDecodeError:
                data = {"_parse_error": True, "snippet": text[:400]}
            return resp.status, data
    except aiohttp.ClientError as exc:
        LOG.warning("HTTP client error: %s", exc)
        return 0, {"error": "hub_unreachable", "detail": str(exc)}


def _truncate(text: str | None, max_len: int = 350) -> str:
    if not text:
        return ""
    t = text.strip().replace("\r\n", "\n")
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


def _iso_to_unix(iso_utc: str | None) -> int | None:
    if not iso_utc:
        return None
    try:
        dt = datetime.fromisoformat(str(iso_utc).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except ValueError:
        return None


def _format_event_time_range(start_utc: str | None, end_utc: str | None) -> str:
    s = _iso_to_unix(start_utc)
    e = _iso_to_unix(end_utc)
    if s and e:
        return f"<t:{s}:f> → <t:{e}:t>  (<t:{s}:R>)"
    if s:
        return f"<t:{s}:f>  (<t:{s}:R>)"
    if start_utc and end_utc:
        return f"{start_utc} → {end_utc}"
    return start_utc or "Unknown time"


def _parse_airports_list(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = value
    elif isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
            raw = decoded if isinstance(decoded, list) else [value]
        except json.JSONDecodeError:
            raw = [value]
    else:
        raw = []
    out: list[str] = []
    for item in raw:
        code = str(item).strip().upper()
        if len(code) == 4 and code.isalnum():
            out.append(code)
    return out


def _iso_utc_date(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).date().isoformat()
    except ValueError:
        return None


def _format_online_since(logon_time_utc: str | None) -> str:
    ts = _iso_to_unix(logon_time_utc)
    if ts:
        return f"<t:{ts}:R>"
    return "unknown"


_SPICY_REGION_PREFIXES: dict[str, tuple[str, ...]] = {
    "europe": ("E", "L", "U"),
    "asia": ("R", "V", "W", "Z", "O", "H"),
    "us": ("K", "P"),
    "south_america": ("S",),
}


def _airport_in_region(icao: str, region_key: str | None) -> bool:
    if not region_key:
        return True
    prefixes = _SPICY_REGION_PREFIXES.get(region_key, ())
    if not prefixes:
        return True
    code = (icao or "").strip().upper()
    return any(code.startswith(p) for p in prefixes)


# ── Airport reference data (pre-loaded for distance / nearby / xwind) ────────

_DB_PATH = Path(__file__).resolve().parent.parent / "data_fetch" / "data" / "aviation_hub.db"


def _load_airport_ref() -> dict[str, dict]:
    """Load all airports from the hub SQLite DB into memory, keyed by ICAO."""
    result: dict[str, dict] = {}
    try:
        conn = sqlite3.connect(str(_DB_PATH))
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            "SELECT icao, name, latitude_deg, longitude_deg, type, country, municipality "
            "FROM airport_reference_latest"
        )
        for row in c.fetchall():
            result[row["icao"]] = dict(row)
        conn.close()
        LOG.info("Loaded %s airports from reference DB.", len(result))
    except Exception as _exc:
        LOG.warning("Could not load airport reference DB: %s", _exc)
    return result


_AIRPORT_REF: dict[str, dict] = _load_airport_ref()


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles."""
    R_NM = 3440.065
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R_NM * 2 * math.asin(math.sqrt(a))


def _initial_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial true bearing in degrees (0–360)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dlambda) * math.cos(phi2)
    y = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(dlambda)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def _decode_winds_field(raw: str, high_level: bool = False) -> tuple[int | None, int | None, int | None]:
    """
    Decode one FAA winds-aloft value field → (dir_deg, speed_kt, temp_c).
    Low-level: "2134+21" (DDSS[+/-TT]) or just "1210"
    High-level: "181331" (DDSSTT, temp always expressed as positive but means negative)
    Returns (None, None, None) for missing/trace entries.
    """
    raw = raw.strip()
    if not raw or set(raw) <= {" "}:
        return None, None, None
    # Light and variable
    if raw.startswith("9900"):
        temp_str = raw[4:]
        temp: int | None = None
        m_t = re.match(r'^([+\-]\d+)$', temp_str)
        if m_t:
            temp = int(m_t.group(1))
        return 0, 0, temp
    # Low-level: 4 base digits + optional sign+temp
    m_lo = re.match(r'^(\d{4})([+\-]\d+)?$', raw)
    if m_lo:
        dd = int(raw[0:2])
        ss = int(raw[2:4])
        if dd >= 51:        # speed > 100 kt encoded
            dd -= 50
            ss += 100
        dir_deg = (dd * 10) % 360
        temp_c: int | None = int(m_lo.group(2)) if m_lo.group(2) else None
        return dir_deg, ss, temp_c
    # High-level: 6 digits DDSSTT (temp always negative above FL240)
    m_hi = re.match(r'^(\d{6})$', raw)
    if m_hi:
        dd = int(raw[0:2])
        ss = int(raw[2:4])
        tt = int(raw[4:6])
        if dd >= 51:
            dd -= 50
            ss += 100
        dir_deg = (dd * 10) % 360
        return dir_deg, ss, -tt
    return None, None, None


def _parse_winds_table(text: str, station_id: str) -> dict[int, tuple[int | None, int | None, int | None]]:
    """
    Parse the FAA FD winds-aloft text table and return data for station_id.
    Returns dict keyed by altitude_ft → (dir_deg, speed_kt, temp_c).
    """
    sid = station_id.upper().strip()
    lines = text.splitlines()
    alt_levels: list[int] = []
    header_idx = -1
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("FT ") or stripped.startswith("FT\t"):
            header_idx = i
            for p in stripped.split()[1:]:
                try:
                    alt_levels.append(int(p))
                except ValueError:
                    pass
            break
    if header_idx < 0 or not alt_levels:
        return {}
    HIGH_ALTS = {30000, 34000, 39000}
    for line in lines[header_idx + 1:]:
        if len(line) < 3:
            continue
        row_id = line[:3].strip().upper()
        if row_id != sid:
            continue
        parts = line[3:].split()
        result: dict[int, tuple] = {}
        for i, alt in enumerate(alt_levels):
            if i < len(parts):
                d, s, t = _decode_winds_field(parts[i], high_level=(alt in HIGH_ALTS))
                result[alt] = (d, s, t)
            else:
                result[alt] = (None, None, None)
        return result
    return {}


def _fmt_wind_row(alt_ft: int, entry: tuple) -> str:
    """Format one altitude level for display."""
    d, s, t = entry
    fl = alt_ft // 100
    if d is None and s is None:
        return f"FL{fl:03d}: —"
    if d == 0 and s == 0:
        wind = "Light & variable"
    else:
        wind = f"{d:03d}° @ {s} kt"
    temp_str = f"   {'+' if (t or 0) >= 0 else ''}{t}°C" if t is not None else ""
    return f"`FL{fl:03d}`: {wind}{temp_str}"


_HELP_DESC_SUFFIXES = (
    " (Aviation Hub)",
    " (Aviation Hub DB)",
    " (Aviation Hub snapshot)",
)


def _help_tidy_description(desc: str | None) -> str:
    d = (desc or "").strip() or "—"
    for suf in _HELP_DESC_SUFFIXES:
        if d.endswith(suf):
            d = d[: -len(suf)].rstrip()
    return d


def _help_embed_field_lines(cmds: list[app_commands.AppCommand]) -> str:
    lines = [
        f"**`/{c.name}`** · {_help_tidy_description(c.description)}"
        for c in sorted(cmds, key=lambda x: x.name)
    ]
    return "\n".join(lines)


class AviationHubBot(commands.Bot):
    def __init__(self) -> None:
        # Slash-only: no prefix commands (avoids confusing default `!help` vs `/help`).
        super().__init__(
            command_prefix=lambda _bot, _message: [],
            intents=discord.Intents.default(),
            help_command=None,
        )
        self.http_session: aiohttp.ClientSession | None = None
        self._full_gnd_twr_config: dict[str, dict[str, Any]] = {}
        self._full_gnd_twr_state_loaded = False

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession(
            headers={"User-Agent": "AviationHubDiscord/1.0"},
            timeout=aiohttp.ClientTimeout(total=45),
        )
        self._load_full_gnd_twr_alert_state()
        self.full_gnd_twr_alert_loop.change_interval(
            seconds=_full_gnd_twr_alert_poll_seconds()
        )
        self.full_gnd_twr_alert_loop.start()
        self.ivao_gnd_twr_alert_loop.change_interval(
            seconds=_full_gnd_twr_alert_poll_seconds()
        )
        self.ivao_gnd_twr_alert_loop.start()
        LOG.info(
            "Full GND + TWR alert worker ready; polling every %ss when servers opt in.",
            _full_gnd_twr_alert_poll_seconds(),
        )
        # Sync once per process start (avoid repeating on every reconnect in on_ready → rate limits).
        # Always sync globals so slash commands are available in DMs.
        try:
            global_synced = await self.tree.sync()
            LOG.info("Slash commands synced globally (%s commands).", len(global_synced))
        except discord.HTTPException as exc:
            detail = getattr(exc, "text", None) or str(exc)
            LOG.error(
                "Global slash sync failed: HTTP %s. Detail: %s",
                exc.status,
                (detail[:500] + "…") if len(detail) > 500 else detail,
            )
        except Exception:
            LOG.exception("Global slash command sync failed.")

        guild_raw = _normalize_snowflake_env(os.environ.get("DISCORD_GUILD_ID"))
        try:
            if guild_raw:
                guild = discord.Object(id=int(guild_raw))
                # Copy global commands to the guild so they appear instantly (no 1-hour propagation delay).
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                LOG.info(
                    "Slash commands synced directly to guild %s (%s commands). Instant availability.",
                    guild.id,
                    len(synced),
                )
            else:
                LOG.info(
                    "No DISCORD_GUILD_ID set; using global commands for servers/DMs "
                    "(may take up to ~1 hour to propagate)."
                )
        except discord.HTTPException as exc:
            detail = getattr(exc, "text", None) or str(exc)
            LOG.error(
                "Slash sync failed: HTTP %s — fix DISCORD_GUILD_ID, invite the bot to that server, "
                "and re‑invite with scopes bot + applications.commands. Detail: %s",
                exc.status,
                (detail[:500] + "…") if len(detail) > 500 else detail,
            )
        except Exception:
            LOG.exception(
                "Slash command sync failed — commands may not appear. "
                "Check DISCORD_GUILD_ID matches your server (right‑click server → Copy Server ID, Developer Mode on). "
                "Ensure the bot was invited with **applications.commands** scope."
            )

    def _load_full_gnd_twr_alert_state(self) -> None:
        self._full_gnd_twr_state_loaded = True
        state_path = _full_gnd_twr_alert_state_file()
        if not state_path.is_file():
            return
        try:
            data = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            LOG.warning("Could not read full GND + TWR alert state file %s: %s", state_path, exc)
            return
        guilds = data.get("guilds") if isinstance(data, dict) else None
        if not isinstance(guilds, dict):
            return
        loaded: dict[str, dict[str, Any]] = {}
        for guild_id, cfg in guilds.items():
            if not isinstance(cfg, dict):
                continue
            guild_key = str(guild_id).strip()
            if not guild_key.isdigit():
                continue
            channel_id = str(cfg.get("channel_id") or "").strip()
            alerted_airports = cfg.get("alerted_airports") or []
            loaded[guild_key] = {
                "enabled": bool(cfg.get("enabled", False)),
                "channel_id": channel_id if channel_id.isdigit() else "",
                "initialized": bool(cfg.get("initialized", False)),
                "alerted_airports": sorted(
                    {
                        str(icao).strip().upper()
                        for icao in alerted_airports
                        if len(str(icao).strip()) == 4
                    }
                ),
            }
        self._full_gnd_twr_config = loaded

    def _save_full_gnd_twr_alert_state(self) -> None:
        state_path = _full_gnd_twr_alert_state_file()
        payload = {
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "guilds": self._full_gnd_twr_config,
        }
        try:
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        except OSError as exc:
            LOG.warning("Could not write full GND + TWR alert state file %s: %s", state_path, exc)

    def _full_gnd_twr_enabled_configs(self) -> dict[str, dict[str, Any]]:
        return {
            guild_id: cfg
            for guild_id, cfg in self._full_gnd_twr_config.items()
            if cfg.get("enabled") and str(cfg.get("channel_id") or "").isdigit()
        }

    def _ivao_gnd_twr_enabled_configs(self) -> dict[str, dict[str, Any]]:
        return {
            guild_id: cfg
            for guild_id, cfg in self._full_gnd_twr_config.items()
            if cfg.get("ivao_enabled") and str(cfg.get("ivao_channel_id") or "").isdigit()
        }

    def set_full_gnd_twr_alerts(
        self,
        *,
        guild_id: int,
        channel_id: int,
        enabled: bool,
    ) -> dict[str, Any]:
        if not self._full_gnd_twr_state_loaded:
            self._load_full_gnd_twr_alert_state()
        guild_key = str(guild_id)
        cfg = self._full_gnd_twr_config.setdefault(
            guild_key,
            {"enabled": False, "channel_id": "", "initialized": False, "alerted_airports": []},
        )
        cfg["enabled"] = enabled
        cfg["channel_id"] = str(channel_id)
        if enabled:
            cfg["initialized"] = False
            cfg["alerted_airports"] = []
        cfg.setdefault("alerted_airports", [])
        self._save_full_gnd_twr_alert_state()
        return cfg

    def set_ivao_gnd_twr_alerts(
        self,
        *,
        guild_id: int,
        channel_id: int,
        enabled: bool,
    ) -> dict[str, Any]:
        if not self._full_gnd_twr_state_loaded:
            self._load_full_gnd_twr_alert_state()
        guild_key = str(guild_id)
        cfg = self._full_gnd_twr_config.setdefault(
            guild_key,
            {"enabled": False, "channel_id": "", "initialized": False, "alerted_airports": []},
        )
        cfg["ivao_enabled"] = enabled
        cfg["ivao_channel_id"] = str(channel_id)
        if enabled:
            cfg["ivao_initialized"] = False
            cfg["ivao_alerted_airports"] = []
        cfg.setdefault("ivao_alerted_airports", [])
        self._save_full_gnd_twr_alert_state()
        return cfg

    def get_full_gnd_twr_alerts(self, guild_id: int) -> dict[str, Any] | None:
        if not self._full_gnd_twr_state_loaded:
            self._load_full_gnd_twr_alert_state()
        return self._full_gnd_twr_config.get(str(guild_id))

    async def _full_gnd_twr_alert_channel(self, channel_id: int) -> Any | None:
        if channel_id <= 0:
            return None
        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except discord.HTTPException as exc:
                LOG.warning("Could not fetch full GND + TWR alert channel %s: %s", channel_id, exc)
                return None
        if not hasattr(channel, "send"):
            LOG.warning("Full GND + TWR alert channel %s is not messageable.", channel_id)
            return None
        return channel

    async def _send_full_gnd_twr_alert(self, channel_id: int, airport: dict[str, Any]) -> None:
        channel = await self._full_gnd_twr_alert_channel(channel_id)
        if channel is None:
            return
        icao = str(airport.get("icao") or "?").upper()
        name = airport.get("name") or "Airport"
        country = airport.get("country") or "—"
        controllers = airport.get("controllers") or []
        controller_lines = []
        for controller in controllers[:8]:
            callsign = controller.get("callsign") or "?"
            facility = controller.get("facility_label") or controller.get("facility") or "ATC"
            name_or_cid = controller.get("name") or controller.get("cid") or "Unknown"
            controller_lines.append(f"• `{callsign}` {facility} · {name_or_cid}")
        if len(controllers) > 8:
            controller_lines.append(f"… +{len(controllers) - 8} more")
        message = "\n".join(
            [
                f"**{icao} now has GND + TWR online**",
                f"**{name}** · {country}",
                "",
                f"**VATSIM controllers ({airport.get('controller_count', len(controllers))})**",
                "\n".join(controller_lines) or "No controller details returned.",
                "",
                "Aviation Hub live VATSIM coverage alert",
            ]
        )
        await channel.send(content=_truncate(message, 2000))

    @tasks.loop(seconds=60)
    async def full_gnd_twr_alert_loop(self) -> None:
        await self.wait_until_ready()
        session = self.http_session
        if session is None:
            return
        enabled_configs = self._full_gnd_twr_enabled_configs()
        if not enabled_configs:
            return
        status, data = await _hub_get(
            session,
            "/api/vatsim/airports",
            limit=200,
            has_gnd=1,
            has_twr=1,
            sort="icao",
        )
        if status != 200:
            LOG.warning(
                "Full GND + TWR alert poll failed: status=%s error=%r",
                status,
                data.get("error") if isinstance(data, dict) else data,
            )
            return
        airports = data.get("airports") if isinstance(data, dict) else []
        if not isinstance(airports, list):
            LOG.warning("Full GND + TWR alert poll returned unexpected payload: %r", data)
            return

        online = {
            str(airport.get("icao") or "").strip().upper()
            for airport in airports
            if (
                isinstance(airport, dict)
                and len(str(airport.get("icao") or "").strip()) == 4
                and _airport_controller_count(airport) >= FULL_GND_TWR_ALERT_MIN_CONTROLLERS
            )
        }
        if not self._full_gnd_twr_state_loaded:
            self._load_full_gnd_twr_alert_state()

        airport_by_icao = {
            str(airport.get("icao") or "").strip().upper(): airport
            for airport in airports
            if (
                isinstance(airport, dict)
                and _airport_controller_count(airport) >= FULL_GND_TWR_ALERT_MIN_CONTROLLERS
            )
        }
        for guild_id, cfg in enabled_configs.items():
            alerted = {
                str(icao).strip().upper()
                for icao in (cfg.get("alerted_airports") or [])
                if len(str(icao).strip()) == 4
            }
            channel_id = int(cfg["channel_id"])
            if not cfg.get("initialized"):
                cfg["initialized"] = True
                cfg["alerted_airports"] = sorted(online)
                continue
            for icao in sorted(online - alerted):
                airport = airport_by_icao.get(icao)
                if not airport:
                    continue
                try:
                    await self._send_full_gnd_twr_alert(channel_id, airport)
                except discord.HTTPException as exc:
                    LOG.warning(
                        "Could not send full GND + TWR alert for guild %s: %s",
                        guild_id,
                        exc,
                    )
            cfg["alerted_airports"] = sorted(online)
        self._save_full_gnd_twr_alert_state()

    @full_gnd_twr_alert_loop.error
    async def full_gnd_twr_alert_loop_error(self, error: Exception) -> None:
        LOG.exception("Full GND + TWR alert loop failed: %s", error)

    async def _send_ivao_gnd_twr_alert(self, channel_id: int, airport: dict[str, Any]) -> None:
        channel = await self._full_gnd_twr_alert_channel(channel_id)
        if channel is None:
            return
        icao = str(airport.get("icao") or "?").upper()
        name = airport.get("name") or "Airport"
        country = airport.get("country") or "—"
        controllers = airport.get("controllers") or []
        controller_lines = []
        for c in controllers[:8]:
            callsign = c.get("callsign") or "?"
            facility = c.get("facility_label") or c.get("facility") or "ATC"
            name_or_vid = c.get("name") or f"VID {c.get('user_id') or '?'}"
            controller_lines.append(f"• `{callsign}` {facility} · {name_or_vid}")
        if len(controllers) > 8:
            controller_lines.append(f"… +{len(controllers) - 8} more")
        message = "\n".join([
            f"**{icao} now has GND + TWR online**",
            f"**{name}** · {country}",
            "",
            f"**IVAO controllers ({airport.get('controller_count', len(controllers))})**",
            "\n".join(controller_lines) or "No controller details returned.",
            "",
            "Aviation Hub live IVAO coverage alert",
        ])
        await channel.send(content=_truncate(message, 2000))

    @tasks.loop(seconds=60)
    async def ivao_gnd_twr_alert_loop(self) -> None:
        await self.wait_until_ready()
        session = self.http_session
        if session is None:
            return
        enabled_configs = self._ivao_gnd_twr_enabled_configs()
        if not enabled_configs:
            return
        status, data = await _hub_get(
            session,
            "/api/ivao/airports",
            limit=200,
            has_gnd=1,
            has_twr=1,
            sort="icao",
        )
        if status != 200:
            LOG.warning("IVAO GND + TWR alert poll failed: status=%s", status)
            return
        airports = data.get("airports") if isinstance(data, dict) else []
        if not isinstance(airports, list):
            return

        online = {
            str(a.get("icao") or "").strip().upper()
            for a in airports
            if isinstance(a, dict) and len(str(a.get("icao") or "").strip()) == 4
            and _airport_controller_count(a) >= FULL_GND_TWR_ALERT_MIN_CONTROLLERS
        }
        airport_by_icao = {
            str(a.get("icao") or "").strip().upper(): a
            for a in airports
            if isinstance(a, dict) and _airport_controller_count(a) >= FULL_GND_TWR_ALERT_MIN_CONTROLLERS
        }
        if not self._full_gnd_twr_state_loaded:
            self._load_full_gnd_twr_alert_state()

        for guild_id, cfg in enabled_configs.items():
            alerted = {
                str(icao).strip().upper()
                for icao in (cfg.get("ivao_alerted_airports") or [])
                if len(str(icao).strip()) == 4
            }
            channel_id = int(cfg["ivao_channel_id"])
            if not cfg.get("ivao_initialized"):
                cfg["ivao_initialized"] = True
                cfg["ivao_alerted_airports"] = sorted(online)
                continue
            for icao in sorted(online - alerted):
                airport = airport_by_icao.get(icao)
                if not airport:
                    continue
                try:
                    await self._send_ivao_gnd_twr_alert(channel_id, airport)
                except discord.HTTPException as exc:
                    LOG.warning("Could not send IVAO GND + TWR alert for guild %s: %s", guild_id, exc)
            cfg["ivao_alerted_airports"] = sorted(online)
        self._save_full_gnd_twr_alert_state()

    @ivao_gnd_twr_alert_loop.error
    async def ivao_gnd_twr_alert_loop_error(self, error: Exception) -> None:
        LOG.exception("IVAO GND + TWR alert loop failed: %s", error)

    async def close(self) -> None:
        if self.full_gnd_twr_alert_loop.is_running():
            self.full_gnd_twr_alert_loop.cancel()
        if self.ivao_gnd_twr_alert_loop.is_running():
            self.ivao_gnd_twr_alert_loop.cancel()
        if self.http_session:
            await self.http_session.close()
        await super().close()


bot = AviationHubBot()


@bot.event
async def on_ready() -> None:
    LOG.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else "")
    guild_raw = _normalize_snowflake_env(os.environ.get("DISCORD_GUILD_ID"))
    if guild_raw:
        try:
            gid = int(guild_raw)
        except ValueError:
            LOG.error("DISCORD_GUILD_ID must be digits only after cleanup; got: %r", guild_raw)
            return
        g = bot.get_guild(gid)
        if g is None:
            LOG.error(
                "Bot is **not a member** of server id=%s — slash commands will not show there. "
                "Invite this bot to that Discord server, or set DISCORD_GUILD_ID to a server the bot has joined.",
                gid,
            )
        else:
            LOG.info("Bot is in target guild: %s (id=%s)", g.name, g.id)


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction,
    error: app_commands.AppCommandError,
) -> None:
    LOG.exception("Slash command failed: %s", interaction.command)
    msg = "Command failed (see server logs). If API commands break, check the hub is running and `AVIATION_HUB_BASE_URL`."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except Exception:
        LOG.exception("Could not send slash error message to Discord")


@bot.tree.command(
    name="events",
    description="VATSIM events in the next N days (from Aviation Hub DB)",
)
@app_commands.describe(
    days="Only events that start within this many days (1–90)",
    limit="Max events to fetch from hub (1–80)",
)
async def cmd_events(
    interaction: discord.Interaction,
    days: app_commands.Range[int, 1, 90] = 30,
    limit: app_commands.Range[int, 1, 80] = 60,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(
        session,
        "/api/vatsim/events",
        days=str(days),
        limit=str(limit),
    )
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    events = data.get("events") or []
    if not events:
        await interaction.followup.send(
            f"No events in the database for the next **{days}** days "
            "(enable `VATSIM_EVENTS_ENABLED` and run the ingestor).",
        )
        return

    def _event_lines(chunk: list[dict[str, Any]]) -> str:
        lines_out: list[str] = []
        for ev in chunk:
            name = ev.get("name") or "?"
            start = ev.get("start_time_utc")
            end = ev.get("end_time_utc")
            icaos = _parse_airports_list(ev.get("airports_json"))
            link = ev.get("link_url") or ""
            when = _format_event_time_range(start, end)
            where = ", ".join(f"`{x}`" for x in icaos) if icaos else "—"
            open_link = f"[Open event]({link})" if link else ""
            lines_out.append(
                f"**{name}**\n"
                f"{when}\n"
                f"Airports: {where}"
                + (f" · {open_link}" if open_link else "")
            )
        return "\n".join(lines_out)

    snap = data.get("snapshot_fetched_at")
    w_end = data.get("window_end_utc")
    total = len(events)
    per_embed = 6
    chunks = [events[i : i + per_embed] for i in range(0, len(events), per_embed)]
    for idx, chunk in enumerate(chunks):
        title = f"VATSIM events — next {days} days"
        if len(chunks) > 1:
            title += f" (part {idx + 1}/{len(chunks)})"
        embed = discord.Embed(
            title=title,
            description=_truncate(_event_lines(chunk), 3900),
            color=discord.Color.blue(),
        )
        foot = f"{total} event(s) · start ≤ {w_end or '—'} · snapshot: {snap or '—'}"
        embed.set_footer(text=_truncate(foot, 200))
        if idx == 0:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(embed=embed)


@bot.tree.command(name="bookings", description="VATSIM ATC bookings (advisory; from Aviation Hub)")
@app_commands.describe(airport="Optional ICAO to filter (e.g. EDDN)", limit="Max rows (1–25)")
async def cmd_bookings(
    interaction: discord.Interaction,
    airport: str | None = None,
    limit: app_commands.Range[int, 1, 25] = 12,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    icao = airport.strip().upper() if airport else None
    if icao is not None and (len(icao) != 4 or not icao.isalnum()):
        await interaction.followup.send("Airport must be a 4-character ICAO.", ephemeral=True)
        return
    status, data = await _hub_get(session, "/api/vatsim/bookings", icao=icao, limit=limit)
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    rows = data.get("bookings") or []
    if not rows:
        msg = f"No upcoming bookings in the database"
        if icao:
            msg += f" for **{icao}**"
        msg += "."
        await interaction.followup.send(msg)
        return
    lines = []
    for b in rows:
        cs = b.get("callsign")
        t0_unix = _iso_to_unix(b.get("starts_at_utc"))
        t1_unix = _iso_to_unix(b.get("ends_at_utc"))
        pos = b.get("position_type") or ""
        start_str = f"<t:{t0_unix}:f>" if t0_unix else (b.get("starts_at_utc") or "?")
        end_str = f"<t:{t1_unix}:t>" if t1_unix else (b.get("ends_at_utc") or "?")
        relative = f"  (<t:{t0_unix}:R>)" if t0_unix else ""
        pos_label = f" · {pos}" if pos else ""
        lines.append(f"**`{cs}`**{pos_label}\n{start_str} → {end_str}{relative}")
    desc = "\n\n".join(lines)
    embed = discord.Embed(
        title=f"ATC bookings{f' @ {icao}' if icao else ''}",
        description=_truncate(desc, 3900),
        color=discord.Color.dark_green(),
    )
    embed.set_footer(text="Advisory only — not guaranteed online coverage.")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="inbounds",
    description="Pilots currently online filed to land at this ICAO (VATSIM snapshot in Hub DB)",
)
@app_commands.describe(icao="4-letter ICAO", limit="Max pilots to list (1–60)")
async def cmd_inbounds(
    interaction: discord.Interaction,
    icao: str,
    limit: app_commands.Range[int, 1, 60] = 40,
) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/vatsim/inbounds", icao=code, limit=str(limit))
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    pilots = data.get("pilots") or []
    cnt = data.get("count", len(pilots))
    if not pilots:
        await interaction.followup.send(
            f"**0** online pilots filed **`{code}`** as arrival (per `flight_plan_arrival`).",
        )
        return

    def _lines(chunk: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for p in chunk:
            cs = p.get("callsign") or "?"
            dep = p.get("flight_plan_departure") or "?"
            ac = (p.get("flight_plan_aircraft") or "").strip()
            alt = (p.get("flight_plan_altitude") or "").strip()
            gs = p.get("groundspeed")
            tail = f" · {ac}" if ac else ""
            if alt:
                tail += f" FL/{alt}"
            if gs is not None:
                tail += f" · {gs} gs"
            out.append(f"`{cs}` {dep}→**{code}**{tail}")
        return "\n".join(out)

    per = 14
    chunks = [pilots[i : i + per] for i in range(0, len(pilots), per)]
    note = data.get("note") or "Online snapshot only."
    for idx, chunk in enumerate(chunks):
        title = f"Inbounds → {code} ({cnt} total)"
        if len(chunks) > 1:
            title += f" — part {idx + 1}/{len(chunks)}"
        embed = discord.Embed(
            title=title,
            description=_truncate(_lines(chunk), 3900),
            color=discord.Color.orange(),
        )
        embed.set_footer(text=_truncate(note, 200))
        if idx == 0:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="summary",
    description="Light airport row: ATC, weather flags, spicy, upcoming bookings/events (Aviation Hub)",
)
@app_commands.describe(icao="4-letter ICAO", hours="Hours ahead for booking/event counts (1–168)")
async def cmd_summary(
    interaction: discord.Interaction,
    icao: str,
    hours: app_commands.Range[int, 1, 168] = 24,
) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/airport/summary", icao=code, hours=str(hours))
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    atc = data.get("atc") or {}
    wf = data.get("weather_flags")
    sp = data.get("spicy")
    up = data.get("upcoming_signals") or {}
    lines = [
        f"**ATC:** {atc.get('controller_count', 0)} online"
        + ("" if atc.get("has_live_status_row") else " _(no live-status row)_"),
    ]
    if isinstance(wf, dict):
        active = [k.replace("has_", "").replace("is_", "") for k, v in wf.items() if v]
        lines.append("**Wx flags:** " + (", ".join(active) if active else "none highlighted"))
    else:
        lines.append("**Wx flags:** _n/a_")
    if isinstance(sp, dict):
        lines.append(
            f"**Spicy:** score **{sp.get('overall_score')}** · {sp.get('challenge_level')} · "
            f"{sp.get('flight_category') or '—'}"
        )
    else:
        lines.append("**Spicy:** _n/a_")
    lines.append(
        f"**Next {hours}h:** bookings **{up.get('bookings_count', '—')}** · "
        f"events **{up.get('events_count', '—')}**"
    )
    embed = discord.Embed(
        title=f"Summary — {code}",
        description="\n".join(lines),
        color=discord.Color.light_grey(),
    )
    embed.set_footer(text="Bookings/events counts are overlap-window; bookings advisory.")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="upcoming",
    description="Schedule signal only: upcoming bookings/events by airport (next hours, Aviation Hub)",
)
@app_commands.describe(hours="Look-ahead hours (1–72)", limit="Max airports to list (1–40)")
async def cmd_upcoming(
    interaction: discord.Interaction,
    hours: app_commands.Range[int, 1, 72] = 6,
    limit: app_commands.Range[int, 1, 40] = 20,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/airports/upcoming", hours=str(hours), limit=str(limit))
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    rows = data.get("airports") or []
    if not rows:
        await interaction.followup.send(f"No airports with bookings/events in the next **{hours}**h (per DB).")
        return
    groups = data.get("groups") or {}
    likely_group = ((groups.get("likely_staffed") or {}).get("airports")) or []
    event_group = ((groups.get("event_only") or {}).get("airports")) or []

    # Backward compatibility if API groups are absent.
    if not likely_group and not event_group:
        for r in rows:
            if int(r.get("bookings") or 0) > 0:
                likely_group.append(r)
            else:
                event_group.append(r)

    def _fmt_upcoming_row(r: dict) -> str:
        ap = str(r.get("airport") or "").upper()
        b = int(r.get("bookings") or 0)
        e = int(r.get("events") or 0)
        return f"`{ap}` · ATC bookings: **{b}** · events: **{e}**"

    likely_staffed = [_fmt_upcoming_row(r) for r in likely_group]
    event_only = [_fmt_upcoming_row(r) for r in event_group]

    desc_parts = [
        f"Scheduled activity in the next **{hours}h**.",
    ]
    if likely_staffed:
        desc_parts.append("**Likely staffed (bookings > 0)**\n" + "\n".join(likely_staffed))
    if event_only:
        desc_parts.append("**Event only (no bookings yet)**\n" + "\n".join(event_only))
    desc_parts.append("Use `/airport ICAO` for a full breakdown at one airport.")

    embed = discord.Embed(
        title=f"Busy soon — next {hours}h",
        description=_truncate("\n\n".join(desc_parts), 3900),
        color=discord.Color.purple(),
    )
    embed.set_footer(text="Score uses bookings + events in window; bookings are advisory (not guaranteed online ATC).")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="ranked",
    description="Best airports now: live ATC + filed traffic + upcoming + weather (Aviation Hub)",
)
@app_commands.describe(
    hours="Look-ahead hours for bookings/events (1–72)",
    limit="Max airports to list (1–40)",
    include_unmanned="Include airports without live ATC (default on)",
)
async def cmd_ranked(
    interaction: discord.Interaction,
    hours: app_commands.Range[int, 1, 72] = 6,
    limit: app_commands.Range[int, 1, 40] = 20,
    include_unmanned: bool = True,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(
        session,
        "/api/airports/ranked",
        hours=str(hours),
        limit=str(limit),
        include_unmanned="true" if include_unmanned else "false",
    )
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    rows = data.get("airports") or []
    if not rows:
        await interaction.followup.send("No airports matched (try widening filters or check DB ingest).")
        return
    lines = []
    for r in rows:
        ap = r.get("airport")
        manned = "ATC" if r.get("manned") else "—"
        cc = r.get("controller_count")
        inb = r.get("inbounds")
        dep = r.get("departures", 0)
        up = r.get("upcoming_score")
        up_b = r.get("upcoming_bookings", 0)
        up_e = r.get("upcoming_events", 0)
        rank_score = r.get("rank_score")
        lines.append(
            f"`{ap}` · **rank {rank_score}** · {manned}\n"
            f"Controllers online now: **{cc}** positions filled\n"
            f"Filed traffic now: **{inb} arrivals** · **{dep} departures**\n"
            f"Upcoming ({hours}h): ATC bookings **{up_b}** · events **{up_e}**"
        )
    embed = discord.Embed(
        title=f"Ranked — next {hours}h window",
        description=_truncate(
            "**How to read**: higher rank = more active/interesting now. "
            "Filed traffic means online pilots whose flight plans include this airport.\n\n"
            + "\n\n".join(lines),
            3900,
        ),
        color=discord.Color.teal(),
    )
    embed.set_footer(text="For exact controller callsigns/positions at one airport, use /airport ICAO.")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="airport",
    description="One airport: weather, spicy, VATSIM, bookings, inbounds sample (Aviation Hub)",
)
@app_commands.describe(
    icao="4-letter ICAO",
    bookings_limit="Max upcoming bookings to include (1–25)",
)
async def cmd_airport(
    interaction: discord.Interaction,
    icao: str,
    bookings_limit: app_commands.Range[int, 1, 25] = 12,
) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(
        session,
        "/api/airport/brief",
        icao=code,
        bookings_limit=str(bookings_limit),
    )
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}** for `{code}`: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return

    embed = discord.Embed(title=f"Airport brief — {code}", color=discord.Color.dark_blue())

    wx = data.get("weather")
    if isinstance(wx, dict) and wx.get("metar"):
        metar = wx.get("metar") or ""
        cat = wx.get("flight_category") or "unavailable"
        summary = wx.get("wx_summary") or ""
        embed.add_field(
            name="Weather (METAR)",
            value=_truncate(f"{metar}\n_{summary}_" if summary else metar, 1024),
            inline=False,
        )
        embed.add_field(name="Flight category", value=str(cat), inline=True)
    else:
        embed.add_field(name="Weather", value="No METAR in database for this ICAO.", inline=False)

    sp = data.get("spicy")
    if isinstance(sp, dict):
        score = sp.get("overall_score")
        lvl = sp.get("challenge_level") or "—"
        embed.add_field(
            name="Spicy / live snapshot",
            value=_truncate(
                f"Score: **{score}** · {lvl}\n"
                f"ATIS: {'yes' if sp.get('has_atis') else 'no'}",
                500,
            ),
            inline=False,
        )
    else:
        embed.add_field(
            name="Spicy / live snapshot",
            value="No `airport_live_status` row (reference or refresh may be missing).",
            inline=False,
        )

    v = data.get("vatsim") or {}
    ctrls = v.get("controllers") or []
    v_lines = []
    for c in ctrls[:12]:
        fac = c.get("facility_label") or c.get("facility")
        name = c.get("name") or "Unknown"
        since = _format_online_since(c.get("logon_time"))
        v_lines.append(f"• `{c.get('callsign')}` {fac} · {name} · online {since}")
    if not v_lines:
        v_lines.append("No ATC positions online right now.")
    if len(ctrls) > 12:
        v_lines.append(f"… +{len(ctrls) - 12} more")
    embed.add_field(
        name=f"VATSIM coverage (online now: {v.get('controller_count', len(ctrls))})",
        value=_truncate("\n".join(v_lines), 1024),
        inline=False,
    )

    ib = data.get("inbounds") or {}
    if ib.get("error"):
        embed.add_field(
            name="Filed traffic (arrivals/departures)",
            value=f"Unavailable (`{ib['error']}`)",
            inline=False,
        )
    else:
        icnt = int(ib.get("count", 0) or 0)
        dcnt = int(ib.get("departures_count", 0) or 0)
        arr_sample = ib.get("pilots_sample") or []
        dep_sample = ib.get("departures_sample") or []
        if icnt == 0 and dcnt == 0:
            embed.add_field(
                name="Filed traffic (arrivals/departures)",
                value=f"**0 arrivals** · **0 departures** filed for **`{code}`**.",
                inline=False,
            )
        else:
            flines = [f"**{icnt} arrivals** · **{dcnt} departures** filed for **`{code}`**"]
            if arr_sample:
                flines.append("Arrivals sample:")
                for p in arr_sample[:5]:
                    dep = p.get("flight_plan_departure") or "?"
                    cs = p.get("callsign")
                    ac = (p.get("flight_plan_aircraft") or "").strip()
                    flines.append(f"• `{cs}` {dep}→{code}" + (f" · {ac}" if ac else ""))
                if ib.get("truncated"):
                    flines.append("… `/inbounds` for full arrivals list")
            if dep_sample:
                flines.append("Departures sample:")
                for p in dep_sample[:5]:
                    arr = p.get("flight_plan_arrival") or "?"
                    cs = p.get("callsign")
                    ac = (p.get("flight_plan_aircraft") or "").strip()
                    flines.append(f"• `{cs}` {code}→{arr}" + (f" · {ac}" if ac else ""))
                if ib.get("departures_truncated"):
                    flines.append("… more departures in snapshot")
            embed.add_field(
                name="Filed traffic (arrivals/departures)",
                value=_truncate("\n".join(flines), 900),
                inline=False,
            )

    bk = data.get("bookings") or {}
    items = bk.get("items") or []
    today_utc = datetime.now(timezone.utc).date().isoformat()
    todays = [b for b in items if _iso_utc_date(b.get("starts_at_utc")) == today_utc]
    if todays:
        blines = []
        for b in todays[:bookings_limit]:
            blines.append(
                f"`{b.get('starts_at_utc')}` **`{b.get('callsign')}`** "
                f"({b.get('position_type') or '?'})"
            )
        if len(todays) > bookings_limit:
            blines.append(f"… +{len(todays) - bookings_limit} more today")
        embed.add_field(
            name=f"Bookings today (advisory, n={len(todays)})",
            value=_truncate("\n".join(blines), 900),
            inline=False,
        )
    else:
        msg = "No bookings today in DB."
        if bk.get("error"):
            msg += f" ({bk['error']})"
        embed.add_field(name="Bookings today (advisory)", value=msg, inline=False)

    embed.set_footer(
        text="Inbounds = online pilots only. Bookings advisory. METAR from ingest.",
    )
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="metar", description="Raw METAR text from Aviation Hub DB")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_metar(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/metar", icao=code)
    if status != 200:
        await interaction.followup.send(
            f"No METAR for `{code}` (HTTP {status}).",
            ephemeral=True,
        )
        return
    raw = data.get("raw_text") or ""
    obs = data.get("observation_time") or ""
    embed = discord.Embed(
        title=f"METAR — {code}",
        description=_truncate(raw, 3900) or "—",
        color=discord.Color.teal(),
    )
    if obs:
        embed.set_footer(text=f"Observation: {obs}")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivao", description="IVAO online ATC for an airport (Aviation Hub snapshot)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_ivao(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/airport", icao=code)
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}** for `{code}`: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return

    controllers = data.get("controllers") or []
    lines: list[str] = []
    for c in controllers:
        freq = c.get("frequency")
        pos = c.get("position") or ""
        since = _format_online_since(c.get("logon_time"))
        freq_part = f" · {freq} MHz" if freq else ""
        pos_part = f" {pos}" if pos else ""
        lines.append(f"• `{c.get('callsign')}`{pos_part}{freq_part} · online {since}")
    if not lines:
        lines.append("No IVAO ATC positions online right now.")

    embed = discord.Embed(
        title=f"IVAO ATC — {code} (online now: {len(controllers)})",
        description=_truncate("\n".join(lines), 3900),
        color=discord.Color.dark_gold(),
    )
    embed.set_footer(text="Live IVAO whazzup snapshot · served from Aviation Hub")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="spicy", description="Current spicy airports widget (Aviation Hub)")
@app_commands.describe(region="Optional region filter")
@app_commands.choices(
    region=[
        app_commands.Choice(name="Global", value="global"),
        app_commands.Choice(name="Europe", value="europe"),
        app_commands.Choice(name="Asia", value="asia"),
        app_commands.Choice(name="US", value="us"),
        app_commands.Choice(name="South America", value="south_america"),
    ]
)
async def cmd_spicy(
    interaction: discord.Interaction,
    region: app_commands.Choice[str] | None = None,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)

    region_key = (region.value if region and region.value != "global" else None)
    if region_key is None:
        status, data = await _hub_get(session, "/widgets/current-spicy-airports")
        if status != 200:
            await interaction.followup.send(f"Hub returned **{status}**.", ephemeral=True)
            return
        lines: list[str] = []
        for label, key in (("Airliner", "airliner"), ("GA", "ga")):
            row = data.get(key)
            if not isinstance(row, dict):
                lines.append(f"**{label}:** —")
                continue
            ap = row.get("airport") or "?"
            score = row.get("overall_score")
            lvl = row.get("challenge_level") or ""
            cond = row.get("primary_condition") or ""
            lines.append(f"**{label}:** `{ap}` · score **{score}** · {lvl} · {cond}")
        gen = data.get("generated_at") or ""
        embed = discord.Embed(
            title="Spicy airports",
            description="\n".join(lines),
            color=discord.Color.red(),
        )
        if gen:
            embed.set_footer(text=f"Generated: {gen}")
        await interaction.followup.send(embed=embed)
        return

    # Region mode: use ranked list + weather score to produce a local "spicy" shortlist.
    status, data = await _hub_get(
        session,
        "/api/airports/ranked",
        hours="6",
        limit="120",
        include_unmanned="true",
    )
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}**.", ephemeral=True)
        return

    rows = data.get("airports") or []
    filtered: list[dict[str, Any]] = []
    for r in rows:
        icao = str(r.get("airport") or "").upper()
        if not _airport_in_region(icao, region_key):
            continue
        overall = r.get("overall_score")
        if overall is None:
            continue
        filtered.append(r)

    filtered.sort(key=lambda x: float(x.get("overall_score") or 0), reverse=True)
    top = filtered[:10]
    if not top:
        await interaction.followup.send("No spicy airports found for that region right now.")
        return

    lines = []
    for r in top:
        icao = str(r.get("airport") or "").upper()
        score = r.get("overall_score")
        lvl = r.get("challenge_level") or "—"
        manned = "ATC online" if r.get("manned") else "no ATC"
        lines.append(f"`{icao}` · score **{score}** · {lvl} · {manned}")

    pretty_region = (region.name if region else "Region")
    embed = discord.Embed(
        title=f"Spicy airports — {pretty_region}",
        description="\n".join(lines),
        color=discord.Color.red(),
    )
    embed.set_footer(text="Regional spicy list based on weather overall_score.")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="weather", description="METAR + summary for an airport (Aviation Hub)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_weather(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/weather/current", icao=code)
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}** for `{code}`.",
            ephemeral=True,
        )
        return
    metar = data.get("metar") or ""
    wx = data.get("wx_summary") or ""
    cat = data.get("flight_category") or ""
    wind = data.get("wind") or {}
    temp_c = data.get("temp_c")
    pressure = data.get("pressure") or {}
    visibility = data.get("visibility") or {}
    cloud_layers = data.get("cloud_layers") or []
    precip = data.get("precip")
    observed_at = data.get("observed_at")

    # Flight category colour accent
    _cat_colors = {
        "VFR": discord.Color.green(),
        "MVFR": discord.Color.blue(),
        "IFR": discord.Color.red(),
        "LIFR": discord.Color.from_rgb(180, 0, 180),
    }
    color = _cat_colors.get(str(cat).upper(), discord.Color.teal())

    embed = discord.Embed(title=f"Weather — {code}", color=color)

    # METAR raw text
    embed.add_field(name="METAR", value=_truncate(metar, 1000) or "—", inline=False)

    # Key conditions row
    if cat:
        embed.add_field(name="Flight category", value=str(cat), inline=True)

    wind_s = ""
    if isinstance(wind, dict) and (wind.get("speed_kt") is not None):
        dir_deg = wind.get("dir_degrees")
        spd = wind.get("speed_kt")
        gust = wind.get("gust_kt")
        wind_s = (f"{dir_deg}°" if dir_deg is not None else "VRB") + f" @ {spd} kt"
        if gust:
            wind_s += f" G{gust} kt"
    if wind_s:
        embed.add_field(name="Wind", value=wind_s, inline=True)

    if temp_c is not None:
        temp_f = round(temp_c * 9 / 5 + 32, 1)
        embed.add_field(name="Temp", value=f"{temp_c}°C / {temp_f}°F", inline=True)

    if isinstance(visibility, dict):
        vis_m = visibility.get("meters")
        vis_mi = visibility.get("statute_mi")
        if vis_m is not None or vis_mi is not None:
            vis_parts = []
            if vis_m is not None:
                vis_parts.append(f"{vis_m:,} m")
            if vis_mi is not None:
                vis_parts.append(f"{vis_mi} SM")
            embed.add_field(name="Visibility", value=" / ".join(vis_parts), inline=True)

    if isinstance(pressure, dict):
        hpa = pressure.get("hpa")
        inhg = pressure.get("in_hg")
        if hpa or inhg:
            p_parts = []
            if hpa:
                p_parts.append(f"{hpa} hPa")
            if inhg:
                p_parts.append(f"{inhg} inHg")
            embed.add_field(name="Pressure", value=" / ".join(str(x) for x in p_parts), inline=True)

    if cloud_layers:
        _cov_labels = {"FEW": "Few", "SCT": "Scattered", "BKN": "Broken", "OVC": "Overcast", "VV": "Vert. vis."}
        layer_strs = []
        for layer in cloud_layers:
            cov = layer.get("coverage") or "?"
            base = layer.get("base_ft_agl")
            ctype = layer.get("cloud_type") or ""
            label = _cov_labels.get(cov, cov)
            base_str = f"{base:,} ft" if base is not None else "—"
            type_str = f" ({ctype})" if ctype else ""
            layer_strs.append(f"{label} {base_str}{type_str}")
        embed.add_field(name="Clouds", value="\n".join(layer_strs), inline=True)

    if precip:
        embed.add_field(name="Precipitation", value=precip.replace("-", " ").title(), inline=True)

    if wx:
        embed.add_field(name="Summary", value=_truncate(wx, 500), inline=False)

    if observed_at:
        ts = _iso_to_unix(observed_at)
        embed.set_footer(text=f"Observed: {f'<t:{ts}:f>' if ts else observed_at}")

    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="vatsim",
    description="VATSIM: flight (pilot callsign), ATC callsign, or airport ICAO (Aviation Hub snapshot)",
)
@app_commands.describe(
    query="Pilot callsign, ATC callsign (e.g. EGLL_TWR), or 3–4 letter airport ICAO",
)
async def cmd_vatsim(interaction: discord.Interaction, query: str) -> None:
    session = bot.http_session
    assert session is not None
    raw = query.strip().upper()
    if len(raw) < 2 or len(raw) > 20:
        await interaction.response.send_message(
            "Query must be 2–20 characters (callsign or ICAO).",
            ephemeral=True,
        )
        return
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not set(raw) <= allowed:
        await interaction.response.send_message(
            "Only letters, digits, and underscore (e.g. BAW123 or EGLL_TWR).",
            ephemeral=True,
        )
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/vatsim/lookup", q=raw)
    if status == 404:
        await interaction.followup.send(
            f"No online pilot, controller, or airport match for **`{raw}`** in the hub snapshot.",
            ephemeral=True,
        )
        return
    if status == 400:
        await interaction.followup.send(
            f"Bad request: `{data.get('error', data)}`",
            ephemeral=True,
        )
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}**.", ephemeral=True)
        return

    kind = data.get("kind")
    if kind == "pilot":
        p = data.get("pilot") or {}
        dep = p.get("flight_plan_departure") or "—"
        arr = p.get("flight_plan_arrival") or "—"
        ac = p.get("flight_plan_aircraft") or "—"
        rules = p.get("flight_plan_rules") or "—"
        alt = p.get("flight_plan_altitude") or "—"
        lines = [
            f"**{p.get('name') or '—'}** · CID {p.get('cid', '—')}",
            f"**Route:** {dep} → {arr}",
            f"**Aircraft / rules / alt:** {ac} · {rules} · {alt}",
            f"**Pos:** {p.get('altitude', '—')} ft · {p.get('groundspeed', '—')} kt · hdg {p.get('heading', '—')} · sq {p.get('transponder', '—')}",
            f"**Server:** {p.get('server') or '—'}",
        ]
        embed = discord.Embed(
            title=f"VATSIM pilot — `{p.get('callsign')}`",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.gold(),
        )
        embed.set_footer(text="From hub VATSIM snapshot; disconnect removes the row.")
        await interaction.followup.send(embed=embed)
        return

    if kind == "controller":
        c = data.get("controller") or {}
        fac = c.get("facility_label") or c.get("facility")
        online_since = _format_online_since(c.get("logon_time"))
        lines = [
            f"**{c.get('name') or '—'}** · CID {c.get('cid', '—')}",
            f"**{fac}** · {c.get('frequency') or '—'} · rating {c.get('rating', '—')}",
            f"**Online since:** {online_since}",
            f"**Server:** {c.get('server') or '—'}",
        ]
        embed = discord.Embed(
            title=f"VATSIM ATC — `{c.get('callsign')}`",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.gold(),
        )
        embed.set_footer(text="From hub VATSIM snapshot.")
        await interaction.followup.send(embed=embed)
        return

    if kind == "airport":
        code = data.get("icao") or raw
        ctrls = data.get("controllers") or []
        atis = data.get("atis") or []
        lines = [f"**{data.get('controller_count', len(ctrls))}** controller(s) online."]
        for c in ctrls[:15]:
            fac = c.get("facility_label") or c.get("facility")
            lines.append(f"• `{c.get('callsign')}` {fac} {c.get('frequency') or ''}")
        if len(ctrls) > 15:
            lines.append(f"… and {len(ctrls) - 15} more")
        if atis:
            lines.append("")
            for a in atis[:3]:
                lines.append(f"ATIS `{a.get('callsign')}` code **{a.get('atis_code')}**")
        embed = discord.Embed(
            title=f"VATSIM airport — {code}",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.gold(),
        )
        await interaction.followup.send(embed=embed)
        return

    await interaction.followup.send(f"Unexpected response kind: `{kind}`", ephemeral=True)


@bot.tree.command(name="taf", description="Terminal Aerodrome Forecast for an airport (Aviation Hub DB)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_taf(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/taf", icao=code)
    if status == 404:
        await interaction.followup.send(f"No TAF in database for **`{code}`** (not all airports have TAF coverage).")
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}** for `{code}`.", ephemeral=True)
        return

    raw = data.get("raw_text") or ""
    issue = data.get("issue_time")
    valid_from = data.get("valid_from_time")
    valid_to = data.get("valid_to_time")

    # Build a readable header showing the validity window
    header_parts: list[str] = []
    if issue:
        ts = _iso_to_unix(issue)
        header_parts.append(f"**Issued:** {f'<t:{ts}:f>' if ts else issue}")
    if valid_from and valid_to:
        ts_f = _iso_to_unix(valid_from)
        ts_t = _iso_to_unix(valid_to)
        f_str = f"<t:{ts_f}:f>" if ts_f else valid_from
        t_str = f"<t:{ts_t}:t>" if ts_t else valid_to
        r_str = f" (<t:{ts_t}:R>)" if ts_t else ""
        header_parts.append(f"**Valid:** {f_str} → {t_str}{r_str}")

    embed = discord.Embed(
        title=f"TAF — {code}",
        description="\n".join(header_parts) if header_parts else None,
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="Forecast",
        value=f"```{_truncate(raw, 990)}```" if raw else "—",
        inline=False,
    )
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="atis", description="Live VATSIM ATIS for an airport (Aviation Hub snapshot)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_atis(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/atis", icao=code)
    if status == 404:
        await interaction.followup.send(
            f"No ATIS online for **`{code}`** right now — an ATIS controller must be active on VATSIM."
        )
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}** for `{code}`.", ephemeral=True)
        return

    callsign = data.get("callsign") or f"{code}_ATIS"
    atis_code = data.get("atis_code") or "?"
    freq = data.get("frequency") or "—"
    text = data.get("text") or ""
    updated = data.get("last_updated")

    # ATIS information letter in readable form
    letter_map = {c: f"Information **{c}**" for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"}
    info_label = letter_map.get(str(atis_code).upper(), f"Code **{atis_code}**")

    embed = discord.Embed(
        title=f"ATIS — {code}  ·  {info_label}",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(name="Station", value=f"`{callsign}`", inline=True)
    embed.add_field(name="Frequency", value=str(freq), inline=True)
    if text:
        embed.add_field(
            name="Text",
            value=f"```{_truncate(text, 990)}```",
            inline=False,
        )
    if updated:
        ts = _iso_to_unix(updated)
        embed.set_footer(text=f"Last updated: {f'<t:{ts}:R>' if ts else updated}")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivaoatis", description="Live IVAO ATIS for an airport (Aviation Hub snapshot)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_ivaoatis(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/atis", icao=code)
    if status == 404:
        await interaction.followup.send(
            f"No IVAO ATIS online for **`{code}`** right now — an ATC position with ATIS must be active."
        )
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}** for `{code}`.", ephemeral=True)
        return

    callsign  = data.get("callsign") or f"{code}_ATIS"
    revision  = data.get("revision") or "?"
    freq      = data.get("frequency") or "—"
    text      = data.get("text") or ""
    updated   = data.get("updated")
    info_label = f"Information **{revision.upper()}**" if revision and revision != "?" else f"Revision **{revision}**"

    embed = discord.Embed(
        title=f"IVAO ATIS — {code}  ·  {info_label}",
        color=discord.Color.blue(),
    )
    embed.add_field(name="Station", value=f"`{callsign}`", inline=True)
    embed.add_field(name="Frequency", value=str(freq), inline=True)
    if text:
        embed.add_field(
            name="Text",
            value=f"```{_truncate(text, 990)}```",
            inline=False,
        )
    if updated:
        ts = _iso_to_unix(updated)
        embed.set_footer(text=f"Last updated: {f'<t:{ts}:R>' if ts else updated}")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="sigmet", description="Active international SIGMETs (Aviation Hub DB)")
@app_commands.describe(
    hazard="Filter by hazard type (leave blank for all)",
    fir="Filter by FIR prefix, e.g. EG, LS, K (leave blank for global)",
)
@app_commands.choices(
    hazard=[
        app_commands.Choice(name="All", value="all"),
        app_commands.Choice(name="Turbulence (TURB)", value="TURB"),
        app_commands.Choice(name="Icing (ICE)", value="ICE"),
        app_commands.Choice(name="Thunderstorm (TS)", value="TS"),
        app_commands.Choice(name="Volcanic Ash (VA)", value="VA"),
        app_commands.Choice(name="Tropical Cyclone (TC)", value="TC"),
        app_commands.Choice(name="Mountain Wave (MTW)", value="MTW"),
    ]
)
async def cmd_sigmet(
    interaction: discord.Interaction,
    hazard: app_commands.Choice[str] | None = None,
    fir: str | None = None,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)

    hazard_val = None if (hazard is None or hazard.value == "all") else hazard.value
    fir_val = fir.strip().upper() if fir else None

    status, data = await _hub_get(
        session,
        "/api/sigmets",
        hazard=hazard_val,
        fir=fir_val,
        limit=15,
    )
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True)
        return

    sigmets = data.get("sigmets") or []
    count = data.get("count", len(sigmets))

    # Hazard emoji map
    _hazard_icon: dict[str, str] = {
        "TURB": "〰️",
        "ICE": "🧊",
        "TS": "⛈️",
        "VA": "🌋",
        "TC": "🌀",
        "MTW": "🏔️",
    }

    if not sigmets:
        filt = ""
        if hazard_val:
            filt += f" hazard={hazard_val}"
        if fir_val:
            filt += f" FIR={fir_val}"
        await interaction.followup.send(f"No active SIGMETs in database{filt}.")
        return

    lines: list[str] = []
    for s in sigmets:
        haz = (s.get("hazard") or "?").upper()
        icon = _hazard_icon.get(haz, "⚠️")
        qual = s.get("qualifier") or ""
        fir_id = s.get("fir") or "?"
        fir_name = s.get("fir_name") or ""
        base_ft = s.get("base_ft")
        top_ft = s.get("top_ft")
        valid_from = s.get("valid_from")
        valid_to = s.get("valid_to")
        raw = s.get("raw_text") or ""

        # Altitude band
        if base_ft is not None and top_ft is not None:
            alt_str = f"FL{base_ft // 100:03d}–FL{top_ft // 100:03d}"
        elif top_ft is not None:
            alt_str = f"up to FL{top_ft // 100:03d}"
        else:
            alt_str = ""

        # Validity window with Discord timestamps
        ts_f = _iso_to_unix(valid_from)
        ts_t = _iso_to_unix(valid_to)
        when = ""
        if ts_f and ts_t:
            when = f"<t:{ts_f}:t> → <t:{ts_t}:t>  (<t:{ts_t}:R>)"
        elif ts_f:
            when = f"<t:{ts_f}:f>"

        # Concise label line
        hazard_label = f"{icon} **{haz}**" + (f" ({qual})" if qual else "")
        fir_label = f"`{fir_id}`" + (f" {fir_name}" if fir_name else "")
        detail_parts = [x for x in [alt_str, when] if x]
        detail = "  ·  ".join(detail_parts)

        lines.append(f"{hazard_label}  ·  {fir_label}")
        if detail:
            lines.append(f"  {detail}")
        if raw:
            lines.append(f"  `{_truncate(raw, 160)}`")
        lines.append("")  # blank separator between SIGMETs

    title = "Active SIGMETs"
    if hazard_val:
        title += f" — {hazard_val}"
    if fir_val:
        title += f" · FIR {fir_val}"

    embed = discord.Embed(
        title=title,
        description=_truncate("\n".join(lines).rstrip(), 3900),
        color=discord.Color.orange(),
    )
    embed.set_footer(text=f"{count} active SIGMET(s) · sourced from AviationWeather.gov · advisory only")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="runway", description="Runway details for an airport (length, surface, headings)")
@app_commands.describe(icao="4-letter ICAO")
async def cmd_runway(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/airport/runways", icao=code)
    if status == 404:
        await interaction.followup.send(
            f"No runway data in database for **`{code}`** — airport may not be in OurAirports reference."
        )
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}** for `{code}`.", ephemeral=True)
        return

    runways = data.get("runways") or []
    name = data.get("name") or code

    if not runways:
        await interaction.followup.send(f"**{code}** is in the reference database but has no runway records.")
        return

    # Surface class display labels
    _surface_labels: dict[str, str] = {
        "hard": "Hard",
        "soft": "Soft",
        "water": "Water",
        "unknown": "?",
    }

    lines: list[str] = []
    for rwy in runways:
        le = rwy.get("le_ident") or "??"
        he = rwy.get("he_ident") or "??"
        length = rwy.get("length_ft")
        width = rwy.get("width_ft")
        surface = rwy.get("surface") or ""
        surface_class = rwy.get("surface_class") or ""
        lighted = rwy.get("lighted", False)
        closed = rwy.get("closed", False)
        le_hdg = rwy.get("le_heading_degT")
        he_hdg = rwy.get("he_heading_degT")

        # Runway designator pair
        rwy_id = f"**{le}/{he}**"
        if closed:
            rwy_id += "  🚫 CLOSED"

        # Heading string
        if le_hdg is not None and he_hdg is not None:
            hdg_str = f"{le_hdg:.0f}°/{he_hdg:.0f}°T"
        elif le_hdg is not None:
            hdg_str = f"{le_hdg:.0f}°T"
        else:
            hdg_str = ""

        # Dimensions
        dim_parts: list[str] = []
        if length:
            dim_parts.append(f"{length:,} ft")
        if width:
            dim_parts.append(f"{width} ft wide")
        dim_str = " · ".join(dim_parts)

        # Surface
        surf_display = _surface_labels.get(surface_class.lower(), surface_class) if surface_class else ""
        if surface and surface.upper() != surface_class.upper():
            surf_display = surface if not surf_display else f"{surf_display} ({surface})"

        # Icons
        icons: list[str] = []
        if lighted:
            icons.append("💡 Lighted")
        icon_str = "  ·  ".join(icons)

        # Assemble the line
        detail_parts = [x for x in [hdg_str, dim_str, surf_display, icon_str] if x]
        lines.append(f"{rwy_id}")
        lines.append("  " + "  ·  ".join(detail_parts) if detail_parts else "  —")

    embed = discord.Embed(
        title=f"Runways — {code}",
        description=f"**{name}** · {len(runways)} runway pair(s)\n\n" + "\n".join(lines),
        color=discord.Color.dark_gray(),
    )
    embed.set_footer(text="Source: OurAirports reference data")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="sat", description="Satellite aerial image of an airport (ESRI World Imagery, cached locally)")
@app_commands.describe(icao="4-letter ICAO code")
async def cmd_sat(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)

    # Fetch airport name for the embed title (best-effort)
    airport_name = ""
    st_status, st_data = await _hub_get(session, "/api/station", icao=code)
    if st_status == 200:
        airport_name = st_data.get("name") or ""

    # Request the image from the hub — it handles cache lookup and ESRI fetch
    url = _hub_url("/api/satellite", {"icao": code})
    try:
        async with session.get(url) as resp:
            if resp.status == 404:
                await interaction.followup.send(
                    f"No satellite image available for **`{code}`** — airport may not be in the reference DB.",
                    ephemeral=True,
                )
                return
            if resp.status != 200:
                await interaction.followup.send(
                    f"Hub returned **{resp.status}** for satellite image of `{code}`.", ephemeral=True
                )
                return
            content_type = resp.headers.get("Content-Type", "image/jpeg")
            image_bytes = await resp.read()
    except aiohttp.ClientError as exc:
        LOG.warning("sat fetch failed for %s: %s", code, exc)
        await interaction.followup.send("Could not reach the Aviation Hub API.", ephemeral=True)
        return

    # Pick filename extension to match actual content type
    filename = "satellite.jpg" if "jpeg" in content_type or "jpg" in content_type else "satellite.png"

    title = f"Satellite — {code}"
    if airport_name:
        title += f"  ·  {airport_name}"

    embed = discord.Embed(title=title, color=discord.Color.dark_blue())
    embed.set_image(url=f"attachment://{filename}")
    embed.set_footer(text="Aerial imagery · served from Aviation Hub cache")

    await interaction.followup.send(
        embed=embed,
        file=discord.File(io.BytesIO(image_bytes), filename=filename),
    )


@bot.tree.command(
    name="ivaolookup",
    description="IVAO: look up a pilot callsign, ATC callsign, or airport ICAO in the live snapshot",
)
@app_commands.describe(query="Pilot callsign (e.g. BAW123), ATC callsign (e.g. EGLL_TWR), or 3–4 letter airport ICAO")
async def cmd_ivaolookup(interaction: discord.Interaction, query: str) -> None:
    session = bot.http_session
    assert session is not None
    raw = query.strip().upper()
    if len(raw) < 2 or len(raw) > 20:
        await interaction.response.send_message("Query must be 2–20 characters.", ephemeral=True)
        return
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    if not set(raw) <= allowed:
        await interaction.response.send_message(
            "Only letters, digits, and underscore (e.g. BAW123 or EGLL_TWR).", ephemeral=True
        )
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/lookup", q=raw)
    if status == 404:
        await interaction.followup.send(
            f"No IVAO pilot, controller, or airport match for **`{raw}`** in the hub snapshot.",
            ephemeral=True,
        )
        return
    if status != 200:
        await interaction.followup.send(f"Hub returned **{status}**.", ephemeral=True)
        return

    kind = data.get("kind")

    if kind == "pilot":
        p = data.get("pilot") or {}
        dep = p.get("departure") or "—"
        arr = p.get("arrival") or "—"
        ac  = p.get("aircraft") or "—"
        alt = p.get("altitude") or 0
        gs  = p.get("groundspeed") or 0
        hdg = p.get("heading") or 0
        vid = p.get("user_id") or "—"
        lines = [
            f"**VID:** {vid}",
            f"**Route:** {dep} → {arr}",
            f"**Aircraft:** {ac}",
            f"**Position:** {alt:,} ft · {gs} kt · hdg {hdg}°",
        ]
        embed = discord.Embed(
            title=f"IVAO pilot — `{p.get('callsign')}`",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Live IVAO snapshot · Aviation Hub DB (updated every 60 s)")
        await interaction.followup.send(embed=embed)
        return

    if kind == "atc":
        c = data.get("atc") or {}
        online_since = _format_online_since(c.get("logon_time"))
        lines = [
            f"**{c.get('name') or '—'}** · VID {c.get('user_id') or '—'}",
            f"**{c.get('position') or '—'}** · {c.get('frequency') or '—'} MHz",
            f"**Airport:** {c.get('airport') or '—'}",
            f"**Online since:** {online_since}",
        ]
        embed = discord.Embed(
            title=f"IVAO ATC — `{c.get('callsign')}`",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Live IVAO snapshot · Aviation Hub DB (updated every 60 s)")
        await interaction.followup.send(embed=embed)
        return

    if kind == "airport":
        icao  = data.get("icao") or raw
        ctrls = data.get("controllers") or []
        lines = [f"**{len(ctrls)}** controller(s) online at **{icao}**:"]
        for c in ctrls[:15]:
            name = c.get("name") or ""
            name_part = f" — {name}" if name else ""
            lines.append(f"• `{c.get('callsign')}` {c.get('position') or ''} {c.get('frequency') or ''}{name_part}")
        if len(ctrls) > 15:
            lines.append(f"… and {len(ctrls) - 15} more")
        embed = discord.Embed(
            title=f"IVAO airport — {icao}",
            description=_truncate("\n".join(lines), 3900),
            color=discord.Color.blue(),
        )
        await interaction.followup.send(embed=embed)
        return

    await interaction.followup.send(f"Unexpected response: `{kind}`", ephemeral=True)


@bot.tree.command(
    name="ivaostats",
    description="IVAO member profile: rating, pilot & ATC hours",
)
@app_commands.describe(vid="IVAO VID (numeric member ID, e.g. 684077)")
async def cmd_ivaostats(interaction: discord.Interaction, vid: str) -> None:
    session = bot.http_session
    assert session is not None
    vid_clean = vid.strip()
    if not vid_clean.isdigit():
        await interaction.response.send_message("VID must be a numeric IVAO member ID.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/member", vid=vid_clean)
    if status == 404:
        await interaction.followup.send(f"VID **{vid_clean}** not found on IVAO.", ephemeral=True)
        return
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True
        )
        return

    name     = f"{data.get('first_name', '')} {data.get('last_name', '')}".strip() or "—"
    country  = data.get("country") or "—"
    division = data.get("division") or "—"
    center   = data.get("center") or "—"
    atc_rat  = data.get("atc_rating") or "—"
    pil_rat  = data.get("pilot_rating") or "—"
    pilot_h  = (data.get("pilot_minutes") or 0) / 60
    atc_h    = (data.get("atc_minutes") or 0) / 60
    created  = data.get("created_at") or ""
    reg_str  = ""
    if created:
        try:
            reg_str = datetime.fromisoformat(created.replace("Z", "+00:00")).strftime("%d %b %Y")
        except ValueError:
            reg_str = created[:10]

    lines = [
        f"**VID:** {vid_clean}",
        f"**Name:** {name}",
        f"**ATC Rating:** {atc_rat}",
        f"**Pilot Rating:** {pil_rat}",
        f"**Pilot hours:** {pilot_h:.1f} h",
        f"**ATC hours:** {atc_h:.1f} h",
        f"**Country / Division / Center:** {country} / {division} / {center}",
    ]
    if reg_str:
        lines.append(f"**Member since:** {reg_str}")
    if data.get("is_staff"):
        lines.append("**Staff member** ✓")

    embed = discord.Embed(
        title=f"IVAO member — VID {vid_clean}",
        description="\n".join(lines),
        color=discord.Color.blue(),
    )
    embed.set_footer(text="Data: api.ivao.aero · hours in flight-minutes converted to hours")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivaobookings", description="Scheduled IVAO ATC bookings for today and next 2 days")
@app_commands.describe(
    icao="Filter by airport ICAO (leave blank for all)",
    limit="Max bookings to show (1–30)",
)
async def cmd_ivaobookings(
    interaction: discord.Interaction,
    icao: str = "",
    limit: app_commands.Range[int, 1, 30] = 15,
) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if code and (len(code) != 4 or not code.isalnum()):
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    kwargs: dict[str, str] = {"limit": str(limit)}
    if code:
        kwargs["icao"] = code
    status, data = await _hub_get(session, "/api/ivao/bookings", **kwargs)
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True
        )
        return
    bookings = data.get("bookings") or []
    cnt      = data.get("count", len(bookings))
    if not bookings:
        where = f" for **`{code}`**" if code else ""
        await interaction.followup.send(f"No IVAO ATC bookings found{where}.")
        return

    def _fmt(b: dict[str, Any]) -> str:
        cs   = b.get("callsign") or "?"
        name = b.get("controller") or "?"
        rat  = b.get("rating") or ""
        div  = b.get("division") or ""
        freq = b.get("frequency") or 0
        ts_s = _iso_to_unix(b.get("starts_at") or "")
        ts_e = _iso_to_unix(b.get("ends_at") or "")
        when = (f" · <t:{ts_s}:t>–<t:{ts_e}:t>" if ts_s and ts_e else
                f" · from <t:{ts_s}:t>" if ts_s else "")
        meta = [x for x in [rat, div, f"{freq:.3f}" if freq else ""] if x]
        meta_str = f" ({', '.join(meta)})" if meta else ""
        return f"`{cs}` · {name}{meta_str}{when}"

    title_str = "IVAO bookings" + (f" — {code}" if code else "") + f" ({cnt} total)"
    embed = discord.Embed(
        title=title_str,
        description=_truncate("\n".join(_fmt(b) for b in bookings), 3900),
        color=discord.Color.blue(),
    )
    embed.set_footer(text="Advisory only · Aviation Hub DB (updated every 30 min from IVAO API)")
    await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivaoevents", description="Upcoming IVAO events (from Aviation Hub DB)")
@app_commands.describe(limit="Max events to show (1–20)")
async def cmd_ivaoevents(
    interaction: discord.Interaction,
    limit: app_commands.Range[int, 1, 20] = 10,
) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/events", limit=str(limit))
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True
        )
        return
    events = data.get("events") or []
    if not events:
        await interaction.followup.send("No IVAO events in the database yet — check back shortly.")
        return

    lines: list[str] = []
    for ev in events:
        title   = ev.get("title") or "?"
        date_l  = ev.get("date_label") or ""
        time_l  = ev.get("time_label") or ""
        airports = ev.get("airports") or []
        url     = ev.get("url") or ""
        where   = ", ".join(f"`{a}`" for a in airports) if airports else "—"
        when    = f"{date_l}" + (f" · {time_l}" if time_l else "")
        link    = f" · [Details]({url})" if url else ""
        lines.append(f"**{title}**\n{when}\nAirports: {where}{link}")

    per = 5
    chunks = [lines[i : i + per] for i in range(0, len(lines), per)]
    for idx, chunk in enumerate(chunks):
        title_str = "IVAO upcoming events"
        if len(chunks) > 1:
            title_str += f" (part {idx + 1}/{len(chunks)})"
        embed = discord.Embed(
            title=title_str,
            description=_truncate("\n\n".join(chunk), 3900),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Source: ivao.events · Aviation Hub DB")
        if idx == 0:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivaoinbounds", description="IVAO pilots currently filed to land at an airport")
@app_commands.describe(icao="4-letter ICAO", limit="Max pilots to list (1–60)")
async def cmd_ivaoinbounds(
    interaction: discord.Interaction,
    icao: str,
    limit: app_commands.Range[int, 1, 60] = 40,
) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/inbounds", icao=code, limit=str(limit))
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True
        )
        return
    pilots = data.get("pilots") or []
    cnt    = data.get("count", len(pilots))
    if not pilots:
        await interaction.followup.send(
            f"**0** IVAO pilots currently filed for **`{code}`** as arrival."
        )
        return

    def _lines(chunk: list[dict[str, Any]]) -> str:
        out: list[str] = []
        for p in chunk:
            cs  = p.get("callsign") or "?"
            dep = p.get("departure") or "?"
            ac  = (p.get("aircraft") or "").strip()
            gs  = p.get("groundspeed")
            tail = f" · {ac}" if ac else ""
            if gs:
                tail += f" · {gs} gs"
            out.append(f"`{cs}` {dep}→**{code}**{tail}")
        return "\n".join(out)

    per = 14
    chunks = [pilots[i : i + per] for i in range(0, len(pilots), per)]
    for idx, chunk in enumerate(chunks):
        title_str = f"IVAO inbounds → {code} ({cnt} total)"
        if len(chunks) > 1:
            title_str += f" — part {idx + 1}/{len(chunks)}"
        embed = discord.Embed(
            title=title_str,
            description=_truncate(_lines(chunk), 3900),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Live IVAO snapshot · Aviation Hub DB (updated every 60 s)")
        if idx == 0:
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(embed=embed)


@bot.tree.command(name="ivaocount", description="Total pilots and controllers currently online on IVAO")
async def cmd_ivaocount(interaction: discord.Interaction) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    status, data = await _hub_get(session, "/api/ivao/count")
    if status != 200:
        await interaction.followup.send(
            f"Hub returned **{status}**: `{data.get('error', data)}`", ephemeral=True
        )
        return
    pilots  = data.get("pilots", 0)
    atc     = data.get("atc", 0)
    updated = data.get("updated_at") or ""
    ts      = _iso_to_unix(updated)
    update_str = f"<t:{ts}:R>" if ts else (updated[:19] if updated else "—")
    lines = [
        f"**Pilots online:** {pilots:,}",
        f"**ATC online:** {atc:,}",
        f"**Total connected:** {pilots + atc:,}",
        f"**Snapshot:** {update_str}",
    ]
    embed = discord.Embed(
        title="IVAO online now",
        description="\n".join(lines),
        color=discord.Color.blue(),
    )
    embed.set_footer(text="Source: Aviation Hub DB (IVAO whazzup, updated every 60 s)")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="help",
    description="Show every slash command and its description",
)
async def cmd_help(interaction: discord.Interaction) -> None:
    cmds = list(bot.tree.get_commands())
    by_name = {c.name: c for c in cmds}

    weather_names = ("atis", "ivaoatis", "metar", "pirep", "sigmet", "taf", "weather", "winds")
    airport_names = ("airport", "charts", "nearby", "runway", "sat", "spicy", "summary", "xwind")
    network_names = ("bookings", "events", "inbounds", "ivao", "ivaobookings", "ivaocount", "ivaoevents", "ivaoinbounds", "ivaolookup", "ivaostats", "ranked", "stats", "upcoming", "vatsim", "vatsimcount")
    simbrief_names = ("airlines", "myplan", "postflight")
    utility_names = ("convert", "distance")
    meta_names = ("gnd-twr-alerts", "help", "info", "ping")

    def pick(names: tuple[str, ...]) -> list[app_commands.AppCommand]:
        return [by_name[n] for n in names if n in by_name]

    weather_cmds = pick(weather_names)
    airport_cmds = pick(airport_names)
    network_cmds = pick(network_names)
    simbrief_cmds = pick(simbrief_names)
    meta_cmds = pick(meta_names)
    known = set(weather_names) | set(airport_names) | set(network_names) | set(simbrief_names) | set(utility_names) | set(meta_names)
    other_cmds = [c for c in cmds if c.name not in known]

    embed = discord.Embed(
        title="Aviation Hub · slash commands",
        description="Type **`/`** and start typing to filter. Most commands need the **Aviation Hub** service running on your machine.",
        color=discord.Color.from_rgb(52, 152, 219),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Weather & conditions",
        value=_truncate(_help_embed_field_lines(weather_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="Airport & reference",
        value=_truncate(_help_embed_field_lines(airport_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="Online networks & traffic",
        value=_truncate(_help_embed_field_lines(network_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="SimBrief & flight planning",
        value=_truncate(_help_embed_field_lines(simbrief_cmds), 1024) or "—",
        inline=False,
    )
    utility_cmds = pick(utility_names)
    embed.add_field(
        name="Utilities",
        value=_truncate(_help_embed_field_lines(utility_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="Bot",
        value=_truncate(_help_embed_field_lines(meta_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="Server alerts",
        value=(
            "Use **`/gnd-twr-alerts enabled:true`** in the channel where alerts should post. "
            "Add **`network:ivao`** for IVAO alerts (separate from VATSIM). "
            "Use **`/gnd-twr-alerts enabled:false`** to turn off. "
            "Requires **Manage Server**."
        ),
        inline=False,
    )
    if other_cmds:
        embed.add_field(
            name="Other",
            value=_truncate(_help_embed_field_lines(other_cmds), 1024),
            inline=False,
        )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="info",
    description="About Aviation Hub, plus links to add the bot and join the support server",
)
async def cmd_info(interaction: discord.Interaction) -> None:
    lines = [
        "**Aviation Hub** is the Discord client: slash commands call your local **widget HTTP API** "
        "(METAR, VATSIM pilots/controllers lookup, events, bookings, airport summaries, and more).",
        "",
        f"**Hub base URL** (this bot): `{_hub_base()}`",
        "",
    ]
    add = _hub_add_invite_url()
    if add:
        lines.append(f"**Add Aviation Hub:** [Invite link]({add})")
    else:
        lines.append(
            "**Add Aviation Hub:** set **`AVIATION_HUB_ADD_BOT_URL`** (full OAuth URL) or **`DISCORD_APPLICATION_ID`** "
            "on the bot host to show an invite."
        )
    sup = _hub_support_server_url()
    if sup:
        lines.append(f"**Support server:** [Join]({sup})")
    else:
        lines.append("**Support server:** set **`AVIATION_HUB_SUPPORT_SERVER_URL`** on the bot host (e.g. `https://discord.gg/…`).")

    embed = discord.Embed(
        title="Aviation Hub",
        description=_truncate("\n".join(lines), 3900),
        color=discord.Color.green(),
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="ping",
    description="Check Aviation Hub's ping to the Discord server (gateway latency)",
)
async def cmd_ping(interaction: discord.Interaction) -> None:
    lat = bot.latency
    if math.isnan(lat):
        ws = "— (heartbeat not ready yet; try again in a moment)"
    else:
        ws = f"**{round(lat * 1000)}** ms"
    await interaction.response.send_message(f"Pong — Discord gateway: {ws}")


@bot.tree.command(
    name="gnd-twr-alerts",
    description="Enable or disable GND + TWR online alerts for this server (VATSIM and/or IVAO)",
)
@app_commands.default_permissions(manage_guild=True)
@app_commands.describe(
    enabled="Turn alerts on or off for this Discord server",
    network="Which network to configure (default: vatsim)",
)
@app_commands.choices(network=[
    app_commands.Choice(name="VATSIM", value="vatsim"),
    app_commands.Choice(name="IVAO",   value="ivao"),
])
async def cmd_gnd_twr_alerts(
    interaction: discord.Interaction,
    enabled: bool,
    network: str = "vatsim",
) -> None:
    if interaction.guild is None:
        await interaction.response.send_message(
            "This setting can only be changed inside a Discord server.",
            ephemeral=True,
        )
        return
    target = interaction.channel
    if target is None or not hasattr(target, "id"):
        await interaction.response.send_message(
            "I couldn't work out which channel to use.",
            ephemeral=True,
        )
        return
    bot_member = interaction.guild.me
    if isinstance(target, discord.TextChannel) and bot_member is not None:
        perms = target.permissions_for(bot_member)
        if not perms.send_messages:
            await interaction.response.send_message(
                f"I need Send Messages in {target.mention} before I can post alerts here.",
                ephemeral=True,
            )
            return

    net = network.lower()
    if net == "ivao":
        cfg = bot.set_ivao_gnd_twr_alerts(
            guild_id=interaction.guild.id,
            channel_id=int(target.id),
            enabled=enabled,
        )
        if enabled:
            await interaction.response.send_message(
                f"IVAO GND + TWR alerts are now **on** for this server in <#{cfg['ivao_channel_id']}>.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "IVAO GND + TWR alerts are now **off** for this server.",
                ephemeral=True,
            )
    else:
        cfg = bot.set_full_gnd_twr_alerts(
            guild_id=interaction.guild.id,
            channel_id=int(target.id),
            enabled=enabled,
        )
        if enabled:
            await interaction.response.send_message(
                f"VATSIM GND + TWR alerts are now **on** for this server in <#{cfg['channel_id']}>.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "VATSIM GND + TWR alerts are now **off** for this server.",
                ephemeral=True,
            )


# ── SimBrief helpers ──────────────────────────────────────────────────────────

_AIRLINES_PATH = Path(__file__).parent / "airlines.json"
try:
    with _AIRLINES_PATH.open(encoding="utf-8") as _f:
        _AIRLINE_DATA: dict[str, Any] = json.load(_f)["airlines"]
except Exception as _exc:
    LOG.warning("Could not load airlines.json: %s", _exc)
    _AIRLINE_DATA = {}


def _get_airline_name(icao: str) -> str:
    if not icao or len(icao) != 3:
        return "Unknown"
    return _AIRLINE_DATA.get(icao.upper(), {}).get("name", "Unknown")


def _metar_pretty(metar: str, code: str) -> tuple[str, str]:
    try:
        parts = metar.split()
        icao = parts[0] if parts else code
        time_raw = parts[1] if len(parts) > 1 else "------Z"
        wind = next((p for p in parts if "KT" in p), "00000KT")
        visibility = next((p for p in parts if p.isdigit() or p == "9999"), "0000")
        clouds = next(
            (p for p in parts if any(c in p for c in ["FEW", "SCT", "BKN", "OVC", "NCD", "NSC"])),
            None,
        )
        temp_dew = next((p for p in parts if "/" in p and p.count("/") == 1), "??/??")
        qnh_val = next((p for p in parts if p.startswith("Q")), None)

        hours = time_raw[2:4] if len(time_raw) >= 6 else "??"
        minutes = time_raw[4:6] if len(time_raw) >= 6 else "??"
        time_fmt = f"{hours}:{minutes}Z"
        wind_dir = wind[:3] if len(wind) >= 5 else "000"
        wind_speed = "".join(filter(str.isdigit, wind[3:]))[:2] if len(wind) >= 5 else "00"
        temp, dew = temp_dew.split("/") if "/" in temp_dew else ("??", "??")
        visibility_km = "10+km" if visibility == "9999" else f"{float(visibility) / 1000:.1f}km"
        qnh_display = qnh_val[1:] if qnh_val else "----"

        cloud_desc = ""
        cloud_emoji = ""
        if clouds:
            cl = clouds.lower()
            if "few" in cl:
                cloud_emoji, cloud_desc = "🌤️", f"🌤️ Few clouds ({clouds})"
            elif "sct" in cl:
                cloud_emoji, cloud_desc = "⛅", f"⛅ Scattered clouds ({clouds})"
            elif "bkn" in cl:
                cloud_emoji, cloud_desc = "☁️", f"☁️ Broken clouds ({clouds})"
            elif "ovc" in cl:
                cloud_emoji, cloud_desc = "☁️", f"☁️ Overcast ({clouds})"
            elif "ncd" in cl or "nsc" in cl:
                cloud_emoji, cloud_desc = "☀️", "☀️ Clear sky"
            else:
                cloud_emoji, cloud_desc = "☁️", f"☁️ {clouds}"
        else:
            cloud_desc = "No clouds"

        wx_labels: list[str] = []
        ml = metar.lower()
        if "cavok" in ml:
            wx_labels.append("☀️ CAVOK")
        if "ts" in ml:
            wx_labels.append("⛈️ Thunderstorm")
        if "ra" in ml:
            wx_labels.append("🌧️ Rain")
        if "sn" in ml:
            wx_labels.append("🌨️ Snow")
        if "fg" in ml or "br" in ml or "fog" in ml:
            wx_labels.append("🌫️ Fog")

        blocks = [
            f"{icao} @ {time_fmt}",
            f"💨 Wind {wind_dir}°/{wind_speed}kt",
            f"👁️ Vis {visibility_km}",
        ]
        if wx_labels:
            blocks.extend(wx_labels)
        if cloud_desc:
            blocks.append(cloud_desc)
        blocks.append(f"🌡️ {temp}°C/{dew}°C")

        prefix = wx_labels[0].split()[0] if wx_labels else (cloud_emoji or "🛰️")
        return prefix + " " + " | ".join(blocks), qnh_display
    except Exception:
        return f"🛰️ {code} | {metar}", "----"


def _simbrief_fmt_creation_time(creation_time: str) -> str:
    try:
        if "T" in creation_time and creation_time.endswith("Z"):
            date_part, time_part = creation_time.replace("Z", "").split("T")
        elif " " in creation_time:
            date_part, time_part = creation_time.split(" ", 1)
            time_part = time_part.split()[0]
        else:
            return creation_time
        hm = ":".join(time_part.split(":")[:2])
        return f"📅 {date_part}  🕒 {hm} UTC"
    except Exception:
        return creation_time


def _simbrief_format_data(
    discord_username: str, simbrief_username: str, flight_data: dict[str, Any]
) -> tuple[str, str, str]:
    g = flight_data.get("general", {})
    p = flight_data.get("api_params", {})
    t = flight_data.get("times", {})
    f = flight_data.get("fuel", {})
    aircraft_data = flight_data.get("aircraft", {})
    origin = flight_data.get("origin", {})
    destination = flight_data.get("destination", {})

    creation_time_val = t.get("creation_time")
    if creation_time_val and str(creation_time_val).isdigit() and int(creation_time_val) > 0:
        creation_time = datetime.fromtimestamp(
            int(creation_time_val), tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S Zulu")
    else:
        creation_time = flight_data.get("params", {}).get("time_generated") or "Unknown"

    dep_code = origin.get("icao_code", p.get("orig", "???"))
    dep_name = origin.get("name", dep_code)
    dep_rwy = p.get("origrwy", "N/A")
    dep_metar_line, dep_qnh = _metar_pretty(origin.get("metar", "No METAR available"), dep_code)

    arr_code = destination.get("icao_code", p.get("dest", "???"))
    arr_name = destination.get("name", arr_code)
    arr_rwy = p.get("destrwy", "N/A")
    arr_metar_line, arr_qnh = _metar_pretty(destination.get("metar", "No METAR available"), arr_code)

    initial_alt = g.get("initial_altitude", "N/A")
    if initial_alt and str(initial_alt).isdigit():
        initial_alt = f"FL{initial_alt}"

    icao_code = g.get("icao_airline", "")
    flt_num = f"{icao_code}{g.get('flight_number', '')}" or "N/A"
    airline_name = _get_airline_name(icao_code)
    aircraft_type = (
        aircraft_data.get("name") or g.get("icao_aircraft") or p.get("type") or "Unknown Aircraft"
    )
    registration = aircraft_data.get("reg", g.get("registration", "N/A"))

    sid = g.get("sid_ident", "N/A")
    star = g.get("star_ident", "N/A")
    airac = flight_data.get("params", {}).get("airac", "N/A")
    airac_ver = flight_data.get("params", {}).get("airac_ver", "")
    release = g.get("release", "")
    airac_version = airac_ver or (f"v{release}" if release else "v1")
    airac_display = f"{airac}{airac_version}".strip()

    total_burn = f.get("enroute_burn", "N/A")
    air_dist = g.get("air_distance", "N/A")
    route_dist = g.get("route_distance", "N/A")
    cost_index = g.get("costindex", "N/A")
    cruise_profile = g.get("cruise_profile", "N/A")
    cruise_mach = g.get("cruise_mach", "N/A")
    avg_wind_dir = g.get("avg_wind_dir", "N/A")
    avg_wind_spd = g.get("avg_wind_spd", "N/A")
    avg_wind_comp = g.get("avg_wind_comp", "N/A")
    avg_temp_dev = g.get("avg_temp_dev", "N/A")

    route_parts = g.get("route", p.get("route", "N/A")).split()
    if sid != "N/A" and route_parts and route_parts[0].upper() == sid.upper():
        route_parts = route_parts[1:]
    if star != "N/A" and route_parts and route_parts[-1].upper() == star.upper():
        route_parts = route_parts[:-1]
    core_route = " ".join(route_parts)

    creation_time_fmt = _simbrief_fmt_creation_time(creation_time)
    message = (
        "==============================\n"
        f"📝 **Flight Plan Created:** {creation_time_fmt}\n"
        f"👤 **Discord User:** {discord_username}\n"
        f"🪪 **SimBrief Username:** {simbrief_username}\n"
        "-----------------------------------------\n"
        f"🆔 **Flight Number:** {flt_num}\n"
        f"🛩️ **Airline:** {airline_name}\n"
        f"🚗 **Aircraft Type:** {aircraft_type}\n"
        f"📝 **Aircraft Registration:** {registration}\n"
        "-----------------------------------------\n"
        f"🎈 **Initial Altitude:** {initial_alt}\n"
        f"🔼 **SID:** {sid} | **RWY:** {dep_rwy}\n"
        f"🔽 **STAR:** {star} | **RWY:** {arr_rwy}\n"
        f"🗺️ **Route:** {core_route}\n"
        f"📅 **AIRAC:** {airac_display}\n"
        "-----------------------------------------\n"
        f"🛢️ **Burn:** {total_burn}kg | **Dist (air):** {air_dist}nm, **Route:** {route_dist}nm\n"
        f"🚀 **Cost Index:** {cost_index} | **Cruise:** {cruise_profile} | **Mach:** {cruise_mach}\n"
        f"🧭 **Avg Wind:** {avg_wind_dir}°/{avg_wind_spd}kt (Component: {avg_wind_comp}kt) | **Temp dev:** ISA+{avg_temp_dev}°C\n"
        "-----------------------------------------\n"
        f"🛬 **Departure:** {dep_code} ({dep_name})   **RWY:** {dep_rwy}   🧊 **QNH:** {dep_qnh} hPa\n"
        f"{dep_metar_line}\n"
        f"🛫 **Arrival:** {arr_code} ({arr_name})   **RWY:** {arr_rwy}   🧊 **QNH:** {arr_qnh} hPa\n"
        f"{arr_metar_line}\n"
        "=============================="
    )
    return message, dep_code, arr_code


async def _validate_simbrief_username(session: aiohttp.ClientSession, username: str) -> bool:
    try:
        async with session.get(
            f"https://www.simbrief.com/api/xml.fetcher.php?username={username}"
        ) as resp:
            text = await resp.text()
            return "<status>Success</status>" in text
    except Exception as exc:
        LOG.warning("SimBrief validation failed for %r: %s", username, exc)
        return False


async def _fetch_and_format_simbrief_plan(
    session: aiohttp.ClientSession,
    simbrief_username: str,
    display_name: str,
) -> tuple[tuple[str, str, str] | None, str | None]:
    try:
        async with session.get(
            f"https://www.simbrief.com/api/xml.fetcher.php?username={simbrief_username}&json=v2"
        ) as resp:
            if resp.status == 400:
                return None, "❌ No active SimBrief flight plan found."
            if resp.status != 200:
                return None, f"❌ SimBrief returned status {resp.status}."
            flight_data = await resp.json(content_type=None)
    except Exception as exc:
        LOG.warning("SimBrief fetch failed for %r: %s", simbrief_username, exc)
        return None, "❌ Could not reach SimBrief API."

    if "general" not in flight_data:
        return None, "❌ SimBrief returned data I couldn't format safely."

    try:
        result = _simbrief_format_data(display_name, simbrief_username, flight_data)
    except Exception as exc:
        LOG.warning("SimBrief format failed: %s", exc)
        return None, "❌ Failed to format SimBrief plan."

    return result, None


# ── SimBrief slash commands ────────────────────────────────────────────────────

@bot.tree.command(
    name="airlines",
    description="Send the full airline ICAO code & callsign list to your DMs",
)
async def cmd_airlines(interaction: discord.Interaction) -> None:
    if not _AIRLINE_DATA:
        await interaction.response.send_message("❌ Airline data not loaded.", ephemeral=True)
        return

    fields = [
        (icao, f"**{info.get('name', 'Unknown')}** (`{info.get('call_sign', 'N/A')}`)")
        for icao, info in sorted(_AIRLINE_DATA.items(), key=lambda item: item[1].get("name", "").lower())
    ]
    chunks = [fields[i: i + 25] for i in range(0, len(fields), 25)]
    await interaction.response.defer(thinking=True, ephemeral=True)

    dm_sent = False
    for i, chunk in enumerate(chunks):
        embed = discord.Embed(
            title="✈️ Airline ICAO Codes & Callsigns",
            description="Use these ICAO codes when setting up your SimBrief flight plan:",
            color=discord.Color.teal(),
        )
        for name, value in chunk:
            embed.add_field(name=name, value=value, inline=True)
        embed.set_footer(text=f"Page {i + 1} of {len(chunks)}")
        try:
            await interaction.user.send(embed=embed)
            dm_sent = True
        except discord.Forbidden:
            break

    if dm_sent:
        await interaction.followup.send(
            "📡 Airline directory dispatched to your DMs. Check your inbox, captain! ✈️",
            ephemeral=True,
        )
    else:
        await interaction.followup.send(
            "⚠️ Couldn't send you a DM — you may have DMs disabled from server members.",
            ephemeral=True,
        )


@bot.tree.command(
    name="myplan",
    description="Fetch and DM the latest SimBrief flight plan for a username",
)
@app_commands.describe(simbrief_username="SimBrief username to fetch the plan for")
async def cmd_myplan(interaction: discord.Interaction, simbrief_username: str) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True, ephemeral=True)

    if not await _validate_simbrief_username(session, simbrief_username):
        await interaction.followup.send(
            "❌ That SimBrief username doesn't appear to exist. Please double-check!",
            ephemeral=True,
        )
        return

    result, error = await _fetch_and_format_simbrief_plan(
        session, simbrief_username, str(interaction.user)
    )
    if error:
        await interaction.followup.send(error, ephemeral=True)
        return

    message, dep, arr = result  # type: ignore[misc]
    try:
        await interaction.user.send(f"```\n{message}\n```")
        await interaction.followup.send(
            f"📬 Sent `{simbrief_username}`'s latest plan to your DMs: `{dep}` → `{arr}`",
            ephemeral=True,
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ I couldn't DM you. Please enable DMs from server members.",
            ephemeral=True,
        )


@bot.tree.command(
    name="postflight",
    description="Post the latest SimBrief flight plan for a username in this channel",
)
@app_commands.describe(simbrief_username="SimBrief username to fetch the plan for")
async def cmd_postflight(interaction: discord.Interaction, simbrief_username: str) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)

    if not await _validate_simbrief_username(session, simbrief_username):
        await interaction.followup.send(
            "❌ That SimBrief username doesn't appear to exist. Please double-check!"
        )
        return

    result, error = await _fetch_and_format_simbrief_plan(
        session, simbrief_username, str(interaction.user)
    )
    if error:
        await interaction.followup.send(error)
        return

    message, dep, arr = result  # type: ignore[misc]
    await interaction.followup.send(
        f"🛫 Flight plan for `{simbrief_username}` — `{dep}` → `{arr}`\n```\n{message}\n```"
    )


# ── New utility / external-API commands ──────────────────────────────────────


@bot.tree.command(
    name="pirep",
    description="Recent PIREPs near an airport (AviationWeather.gov, last 6 hours, 200 nm radius)",
)
@app_commands.describe(icao="4-letter ICAO code")
async def cmd_pirep(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)
    url = (
        f"https://aviationweather.gov/api/data/pirep"
        f"?id={code}&format=json&age=6&distance=200"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 204:
                await interaction.followup.send(
                    f"No PIREPs within 200 nm of **`{code}`** in the last 6 hours.",
                )
                return
            if resp.status != 200:
                await interaction.followup.send(
                    f"AviationWeather returned **{resp.status}** for PIREPs near `{code}`.",
                    ephemeral=True,
                )
                return
            pireps: list[dict] = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:
        LOG.warning("PIREP fetch failed for %s: %s", code, exc)
        await interaction.followup.send("Could not reach AviationWeather.gov.", ephemeral=True)
        return

    if not pireps:
        await interaction.followup.send(
            f"No PIREPs within 200 nm of **`{code}`** in the last 6 hours.",
        )
        return

    lines: list[str] = []
    for p in pireps[:5]:
        ac = p.get("acType") or "?"
        fl = p.get("fltLvl")
        fl_type = p.get("fltLvlType") or ""
        lat = p.get("lat")
        lon = p.get("lon")
        obs_ts = p.get("obsTime")
        raw = (p.get("rawOb") or "").strip()

        # Icing
        icing_parts: list[str] = []
        for idx in range(1, 3):
            ice_int = p.get(f"icgInt{idx}") or ""
            ice_type = p.get(f"icgType{idx}") or ""
            ice_bas = p.get(f"icgBas{idx}")
            ice_top = p.get(f"icgTop{idx}")
            if ice_int:
                s = f"ICE {ice_int}"
                if ice_type:
                    s += f" ({ice_type})"
                if ice_bas is not None and ice_top is not None:
                    s += f" {ice_bas}–{ice_top} ft"
                icing_parts.append(s)
        # Turbulence
        turb_parts: list[str] = []
        for idx in range(1, 3):
            tb_int = p.get(f"tbInt{idx}") or ""
            tb_type = p.get(f"tbType{idx}") or ""
            tb_bas = p.get(f"tbBas{idx}")
            tb_top = p.get(f"tbTop{idx}")
            if tb_int:
                s = f"TURB {tb_int}"
                if tb_type:
                    s += f" ({tb_type})"
                if tb_bas is not None and tb_top is not None:
                    s += f" {tb_bas}–{tb_top} ft"
                turb_parts.append(s)

        fl_str = f"FL{fl:03d}" if isinstance(fl, int) and fl > 0 else (fl_type or "—")
        time_str = f"<t:{obs_ts}:t>" if isinstance(obs_ts, int) else "?"
        pos_str = f"{lat:.1f},{lon:.1f}" if lat is not None and lon is not None else "?"

        header = f"**{ac}** · {fl_str} · {time_str} · {pos_str}"
        detail_parts = icing_parts + turb_parts
        detail = "  ·  ".join(detail_parts) if detail_parts else ""
        raw_short = _truncate(raw, 120)
        lines.append(header)
        if detail:
            lines.append(f"  {detail}")
        if raw_short:
            lines.append(f"  `{raw_short}`")
        lines.append("")

    embed = discord.Embed(
        title=f"PIREPs near {code} (last 6 h · 200 nm)",
        description=_truncate("\n".join(lines).rstrip(), 3900),
        color=discord.Color.orange(),
    )
    embed.set_footer(text=f"{min(len(pireps), 5)} of {len(pireps)} PIREP(s) · AviationWeather.gov")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="winds",
    description="Winds aloft forecast for a US airport (AviationWeather.gov FD data)",
)
@app_commands.describe(icao="4-letter ICAO code (US airports only, e.g. KLAX)")
async def cmd_winds(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    # FAA winds data only covers US; US ICAO codes start with K
    if not code.startswith("K"):
        await interaction.response.send_message(
            "Winds aloft data via this endpoint is only available for US airports "
            "(ICAO code starting with **K**, e.g. `KLAX`, `KJFK`).",
            ephemeral=True,
        )
        return
    station = code[1:]  # strip leading K → 3-letter FAA ID
    await interaction.response.defer(thinking=True)
    url = f"https://aviationweather.gov/api/data/windtemp?id={code}&fcst=06&format=json"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                await interaction.followup.send(
                    f"AviationWeather returned **{resp.status}** for winds aloft.",
                    ephemeral=True,
                )
                return
            text = await resp.text()
    except aiohttp.ClientError as exc:
        LOG.warning("Winds fetch failed for %s: %s", code, exc)
        await interaction.followup.send("Could not reach AviationWeather.gov.", ephemeral=True)
        return

    data = _parse_winds_table(text, station)
    if not data:
        await interaction.followup.send(
            f"No winds aloft data found for station **{station}** (`{code}`).\n"
            "Note: not all airports have winds aloft data — large FBOs and airline hubs are most likely covered.",
        )
        return

    # Extract header validity line from the raw text
    valid_line = ""
    for line in text.splitlines():
        if "VALID" in line.upper():
            valid_line = line.strip()
            break

    show_alts = [6000, 9000, 18000, 30000, 39000]
    display_labels = {6000: "FL060 (~5,000 ft)", 9000: "FL090 (~10,000 ft)", 18000: "FL180", 30000: "FL300", 39000: "FL390"}
    lines: list[str] = []
    for alt in show_alts:
        entry = data.get(alt)
        if entry:
            label = display_labels.get(alt, f"FL{alt // 100:03d}")
            lines.append(f"**{label}**: {_fmt_wind_row(alt, entry).split(': ', 1)[-1]}")

    if not lines:
        await interaction.followup.send(f"No wind levels decoded for **{station}**.")
        return

    embed = discord.Embed(
        title=f"Winds aloft — {code} (station {station})",
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )
    if valid_line:
        embed.set_footer(text=_truncate(valid_line, 100) + " · AviationWeather.gov FD data")
    else:
        embed.set_footer(text="AviationWeather.gov FD winds aloft")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="stats",
    description="VATSIM member stats: rating, pilot & ATC hours, total connections",
)
@app_commands.describe(cid="VATSIM CID (numeric member ID)")
async def cmd_stats(interaction: discord.Interaction, cid: str) -> None:
    session = bot.http_session
    assert session is not None
    cid_clean = cid.strip()
    if not cid_clean.isdigit():
        await interaction.response.send_message("CID must be a numeric VATSIM member ID.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)

    _VATSIM_RATING_LABELS: dict[int, str] = {
        -1: "Inactive", 0: "Suspended", 1: "OBS", 2: "S1", 3: "S2",
        4: "S3", 5: "C1", 7: "C3", 8: "I1", 10: "I3", 11: "SUP", 12: "ADM",
    }
    _VATSIM_PILOT_RATING_LABELS: dict[int, str] = {
        0: "P0", 1: "P1 (PPL)", 3: "P2 (IR)", 7: "P3 (CMEL)", 15: "P4 (ATPL)", 31: "P5",
    }

    base = "https://api.vatsim.net/v2/members"
    try:
        # Fetch profile and stats in parallel
        async with session.get(f"{base}/{cid_clean}", timeout=aiohttp.ClientTimeout(total=15)) as r_profile:
            if r_profile.status == 404:
                await interaction.followup.send(f"CID **{cid_clean}** not found on VATSIM.", ephemeral=True)
                return
            if r_profile.status != 200:
                await interaction.followup.send(
                    f"VATSIM API returned **{r_profile.status}** for CID {cid_clean}.", ephemeral=True
                )
                return
            profile: dict = await r_profile.json(content_type=None)

        async with session.get(f"{base}/{cid_clean}/stats", timeout=aiohttp.ClientTimeout(total=15)) as r_stats:
            stats: dict = await r_stats.json(content_type=None) if r_stats.status == 200 else {}
    except aiohttp.ClientError as exc:
        LOG.warning("VATSIM stats fetch failed for CID %s: %s", cid_clean, exc)
        await interaction.followup.send("Could not reach VATSIM API.", ephemeral=True)
        return

    rating_id = profile.get("rating", 0)
    pilot_rating_id = profile.get("pilotrating", 0)
    reg_date = profile.get("reg_date") or ""
    region = profile.get("region_id") or "—"
    division = profile.get("division_id") or "—"

    atc_hrs = stats.get("atc", 0.0) or 0.0
    pilot_hrs = stats.get("pilot", 0.0) or 0.0

    rating_label = _VATSIM_RATING_LABELS.get(rating_id, f"Rating {rating_id}")
    pilot_rating_label = _VATSIM_PILOT_RATING_LABELS.get(pilot_rating_id, f"P{pilot_rating_id}")

    reg_str = ""
    if reg_date:
        try:
            dt = datetime.fromisoformat(reg_date.replace("Z", "+00:00"))
            reg_str = dt.strftime("%d %b %Y")
        except ValueError:
            reg_str = reg_date[:10]

    lines = [
        f"**CID:** {cid_clean}",
        f"**ATC Rating:** {rating_label}",
        f"**Pilot Rating:** {pilot_rating_label}",
        f"**Pilot hours:** {pilot_hrs:.1f} h",
        f"**ATC hours:** {atc_hrs:.1f} h",
        f"**Region / Division:** {region} / {division}",
    ]
    if reg_str:
        lines.append(f"**Member since:** {reg_str}")

    embed = discord.Embed(
        title=f"VATSIM member — CID {cid_clean}",
        description="\n".join(lines),
        color=discord.Color.gold(),
    )
    embed.set_footer(text="Data: api.vatsim.net · hours from public stats")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="distance",
    description="Great-circle distance between two airports",
)
@app_commands.describe(icao1="Departure ICAO", icao2="Arrival ICAO")
async def cmd_distance(interaction: discord.Interaction, icao1: str, icao2: str) -> None:
    a1 = icao1.strip().upper()
    a2 = icao2.strip().upper()
    for code in (a1, a2):
        if len(code) != 4 or not code.isalnum():
            await interaction.response.send_message(
                f"`{code}` is not a valid 4-character ICAO.", ephemeral=True
            )
            return
    ap1 = _AIRPORT_REF.get(a1)
    ap2 = _AIRPORT_REF.get(a2)
    missing = [c for c, a in ((a1, ap1), (a2, ap2)) if a is None]
    if missing:
        await interaction.response.send_message(
            f"Airport(s) not found in reference database: {', '.join(f'`{c}`' for c in missing)}",
            ephemeral=True,
        )
        return

    lat1, lon1 = ap1["latitude_deg"], ap1["longitude_deg"]  # type: ignore[index]
    lat2, lon2 = ap2["latitude_deg"], ap2["longitude_deg"]  # type: ignore[index]
    dist_nm = _haversine_nm(lat1, lon1, lat2, lon2)
    dist_km = dist_nm * 1.852
    bearing = _initial_bearing(lat1, lon1, lat2, lon2)

    n1 = ap1.get("name") or a1  # type: ignore[union-attr]
    n2 = ap2.get("name") or a2  # type: ignore[union-attr]

    lines = [
        f"**{a1}** ({n1})",
        f"**{a2}** ({n2})",
        "",
        f"**Distance:** {dist_nm:.0f} nm · {dist_km:.0f} km",
        f"**Initial bearing:** {bearing:.0f}°T",
    ]
    embed = discord.Embed(
        title=f"Distance — {a1} → {a2}",
        description="\n".join(lines),
        color=discord.Color.teal(),
    )
    embed.set_footer(text="Great-circle (Haversine) · OurAirports reference data")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="xwind",
    description="Crosswind & headwind components for each runway at an airport (uses live METAR)",
)
@app_commands.describe(icao="4-letter ICAO code")
async def cmd_xwind(interaction: discord.Interaction, icao: str) -> None:
    session = bot.http_session
    assert session is not None
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    await interaction.response.defer(thinking=True)

    # Fetch wind and runway data from the hub
    wx_status, wx_data = await _hub_get(session, "/api/weather/current", icao=code)
    rwy_status, rwy_data = await _hub_get(session, "/api/airport/runways", icao=code)

    if wx_status != 200:
        await interaction.followup.send(f"No weather data for **`{code}`** (HTTP {wx_status}).", ephemeral=True)
        return
    if rwy_status == 404:
        await interaction.followup.send(f"No runway data for **`{code}`** in the reference database.")
        return
    if rwy_status != 200:
        await interaction.followup.send(f"Hub returned **{rwy_status}** for runways.", ephemeral=True)
        return

    wind = wx_data.get("wind") or {}
    wind_dir = wind.get("dir_degrees")
    wind_spd = wind.get("speed_kt")
    wind_gust = wind.get("gust_kt")
    runways = rwy_data.get("runways") or []
    ap_name = rwy_data.get("name") or code
    metar = wx_data.get("metar") or ""

    if wind_dir is None or wind_spd is None:
        await interaction.followup.send(
            f"No wind data available in METAR for **`{code}`** — cannot compute components.\n"
            + (f"`{_truncate(metar, 200)}`" if metar else ""),
        )
        return

    if not runways:
        await interaction.followup.send(f"No runways found for **`{code}`** in reference data.")
        return

    wind_rad = math.radians(wind_dir)
    lines: list[str] = []
    for rwy in runways:
        for end_id, hdg_key in (
            (rwy.get("le_ident", "?"), "le_heading_degT"),
            (rwy.get("he_ident", "?"), "he_heading_degT"),
        ):
            hdg = rwy.get(hdg_key)
            if hdg is None:
                continue
            rwy_rad = math.radians(hdg)
            angle = wind_rad - rwy_rad
            headwind = wind_spd * math.cos(angle)
            crosswind = wind_spd * math.sin(angle)
            hw_str = f"HW {headwind:+.0f} kt" if headwind >= 0 else f"TW {abs(headwind):.0f} kt"
            xw_str = f"XW {abs(crosswind):.0f} kt"
            lines.append(f"`{end_id}` ({hdg:.0f}°T) · {hw_str} · {xw_str}")
        if rwy.get("closed"):
            lines[-1] += "  — CLOSED"

    wind_str = f"{wind_dir}° @ {wind_spd} kt"
    if wind_gust:
        wind_str += f" G{wind_gust} kt"

    embed = discord.Embed(
        title=f"Crosswind components — {code}",
        color=discord.Color.dark_teal(),
    )
    embed.add_field(name="Surface wind", value=wind_str, inline=False)
    embed.add_field(
        name=f"Runway components ({len(runways)} runway pair(s))",
        value=_truncate("\n".join(lines), 1024),
        inline=False,
    )
    embed.set_footer(text=f"{ap_name} · HW = headwind, TW = tailwind, XW = crosswind (absolute)")
    await interaction.followup.send(embed=embed)


@bot.tree.command(
    name="nearby",
    description="Find airports within a radius of the given ICAO",
)
@app_commands.describe(
    icao="Centre airport ICAO",
    radius_nm="Search radius in nm (default 50, max 200)",
)
async def cmd_nearby(
    interaction: discord.Interaction,
    icao: str,
    radius_nm: app_commands.Range[int, 1, 200] = 50,
) -> None:
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    centre = _AIRPORT_REF.get(code)
    if centre is None:
        await interaction.response.send_message(
            f"**`{code}`** not found in airport reference database.", ephemeral=True
        )
        return

    clat, clon = centre["latitude_deg"], centre["longitude_deg"]

    # Rough bounding-box filter first (1° lat ≈ 60 nm)
    lat_margin = radius_nm / 60.0 + 0.5
    lon_margin = radius_nm / (60.0 * math.cos(math.radians(clat))) + 0.5

    nearby: list[tuple[float, dict]] = []
    for ap_icao, ap in _AIRPORT_REF.items():
        if ap_icao == code:
            continue
        lat = ap.get("latitude_deg")
        lon = ap.get("longitude_deg")
        if lat is None or lon is None:
            continue
        if abs(lat - clat) > lat_margin or abs(lon - clon) > lon_margin:
            continue
        dist = _haversine_nm(clat, clon, lat, lon)
        if dist <= radius_nm:
            nearby.append((dist, ap))

    nearby.sort(key=lambda x: x[0])
    top = nearby[:10]

    _type_labels = {
        "large_airport": "Large",
        "medium_airport": "Medium",
        "small_airport": "Small",
        "heliport": "Heliport",
        "seaplane_base": "Seaplane",
        "balloonport": "Balloon",
        "closed": "Closed",
    }

    if not top:
        await interaction.response.send_message(
            f"No airports found within **{radius_nm} nm** of **`{code}`**.",
        )
        return

    lines: list[str] = []
    for dist, ap in top:
        ap_icao = ap["icao"]
        name = ap.get("name") or ap_icao
        ap_type = _type_labels.get(ap.get("type") or "", ap.get("type") or "?")
        lines.append(f"`{ap_icao}` · {dist:.0f} nm · {ap_type} · {name}")

    embed = discord.Embed(
        title=f"Nearby airports — {code} (within {radius_nm} nm)",
        description=_truncate("\n".join(lines), 3900),
        color=discord.Color.dark_gray(),
    )
    embed.set_footer(text=f"Showing {len(top)} of {len(nearby)} airports · OurAirports reference")
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="charts",
    description="Chart and reference links for an airport",
)
@app_commands.describe(icao="4-letter ICAO code")
async def cmd_charts(interaction: discord.Interaction, icao: str) -> None:
    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return
    ap = _AIRPORT_REF.get(code)
    ap_name = (ap.get("name") if ap else None) or code

    links: list[str] = [
        f"[SkyVector](https://skyvector.com/airport/{code})",
        f"[FlightAware](https://www.flightaware.com/resources/airport/{code})",
        f"[Autorouter](https://www.autorouter.aero/airports/{code})",
        f"[OurAirports](https://ourairports.com/airports/{code}/)",
    ]

    # UK airports: AIP via NATS Aurora
    if code.startswith("EG"):
        links.append("[UK AIP (NATS)](https://www.aurora.nats.co.uk/htmlAIP/Publications/current-en-GB/html/index-en-GB.html)")

    # US airports: FAA digital products
    if code.startswith("K"):
        faa_id = code[1:]  # 3-letter FAA identifier
        links.append(
            f"[FAA Chart Supplement](https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dafd/)"
        )
        links.append(
            f"[FAA D-TPP Approach Charts](https://www.faa.gov/air_traffic/flight_info/aeronav/digital_products/dtpp/)"
        )

    embed = discord.Embed(
        title=f"Charts & references — {code}",
        description=f"**{ap_name}**\n\n" + "  ·  ".join(links),
        color=discord.Color.dark_blue(),
    )
    embed.set_footer(text="External links · always check NOTAMs before flight")
    await interaction.response.send_message(embed=embed)


_CONVERT_UNITS = [
    app_commands.Choice(name="hPa (pressure)", value="hpa"),
    app_commands.Choice(name="inHg (pressure)", value="inhg"),
    app_commands.Choice(name="ft (altitude)", value="ft"),
    app_commands.Choice(name="m (metres)", value="m"),
    app_commands.Choice(name="nm (nautical miles)", value="nm"),
    app_commands.Choice(name="km (kilometres)", value="km"),
    app_commands.Choice(name="mi (statute miles)", value="mi"),
    app_commands.Choice(name="kt (knots)", value="kt"),
    app_commands.Choice(name="kph (km/h)", value="kph"),
    app_commands.Choice(name="mph (miles/h)", value="mph"),
    app_commands.Choice(name="°C (Celsius)", value="c"),
    app_commands.Choice(name="°F (Fahrenheit)", value="f"),
]

_CONVERT_GROUPS = {
    "pressure": {"hpa", "inhg"},
    "altitude": {"ft", "m"},
    "distance": {"nm", "km", "mi"},
    "speed": {"kt", "kph", "mph"},
    "temperature": {"c", "f"},
}

_CONVERT_FACTORS: dict[str, dict[str, float]] = {
    # convert TO base unit (hpa, ft, nm, kt, c) then from base
    "hpa":  {"hpa": 1.0, "inhg": 0.02953},
    "inhg": {"hpa": 33.8639, "inhg": 1.0},
    "ft":   {"ft": 1.0, "m": 0.3048},
    "m":    {"ft": 3.28084, "m": 1.0},
    "nm":   {"nm": 1.0, "km": 1.852, "mi": 1.15078},
    "km":   {"nm": 0.539957, "km": 1.0, "mi": 0.621371},
    "mi":   {"nm": 0.868976, "km": 1.60934, "mi": 1.0},
    "kt":   {"kt": 1.0, "kph": 1.852, "mph": 1.15078},
    "kph":  {"kt": 0.539957, "kph": 1.0, "mph": 0.621371},
    "mph":  {"kt": 0.868976, "kph": 1.60934, "mph": 1.0},
}

_CONVERT_UNIT_LABELS: dict[str, str] = {
    "hpa": "hPa", "inhg": "inHg",
    "ft": "ft", "m": "m",
    "nm": "nm", "km": "km", "mi": "mi",
    "kt": "kt", "kph": "km/h", "mph": "mph",
    "c": "°C", "f": "°F",
}


def _convert_value(value: float, from_u: str, to_u: str) -> float | None:
    """Convert value from from_u to to_u. Returns None if conversion not possible."""
    if from_u == to_u:
        return value
    # Temperature special case
    if from_u == "c" and to_u == "f":
        return value * 9 / 5 + 32
    if from_u == "f" and to_u == "c":
        return (value - 32) * 5 / 9
    if from_u in {"c", "f"} or to_u in {"c", "f"}:
        return None  # mixed temperature with other units
    # Check same group
    from_group = next((g for g, units in _CONVERT_GROUPS.items() if from_u in units), None)
    to_group = next((g for g, units in _CONVERT_GROUPS.items() if to_u in units), None)
    if from_group is None or to_group is None or from_group != to_group:
        return None
    # Use the factors dict
    row = _CONVERT_FACTORS.get(from_u)
    if row is None:
        return None
    factor = row.get(to_u)
    if factor is None:
        return None
    return value * factor


@bot.tree.command(
    name="convert",
    description="Aviation unit converter: pressure, altitude, distance, speed, temperature",
)
@app_commands.describe(
    value="Numeric value to convert",
    from_unit="Unit to convert from",
    to_unit="Unit to convert to",
)
@app_commands.choices(from_unit=_CONVERT_UNITS, to_unit=_CONVERT_UNITS)
async def cmd_convert(
    interaction: discord.Interaction,
    value: float,
    from_unit: app_commands.Choice[str],
    to_unit: app_commands.Choice[str],
) -> None:
    fu = from_unit.value
    tu = to_unit.value
    result = _convert_value(value, fu, tu)
    if result is None:
        await interaction.response.send_message(
            f"Cannot convert **{_CONVERT_UNIT_LABELS.get(fu, fu)}** to "
            f"**{_CONVERT_UNIT_LABELS.get(tu, tu)}** — incompatible unit types.",
            ephemeral=True,
        )
        return
    from_label = _CONVERT_UNIT_LABELS.get(fu, fu)
    to_label = _CONVERT_UNIT_LABELS.get(tu, tu)
    # Format output: avoid excessive decimal places
    if abs(result) >= 100:
        result_str = f"{result:.2f}"
    elif abs(result) >= 1:
        result_str = f"{result:.4f}"
    else:
        result_str = f"{result:.6f}"
    result_str = result_str.rstrip("0").rstrip(".")

    embed = discord.Embed(
        title="Unit conversion",
        description=f"**{value:g} {from_label}** = **{result_str} {to_label}**",
        color=discord.Color.blurple(),
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="vatsimcount",
    description="Total pilots and controllers currently online on VATSIM",
)
async def cmd_vatsimcount(interaction: discord.Interaction) -> None:
    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)
    try:
        async with session.get(
            "https://data.vatsim.net/v3/vatsim-data.json",
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status != 200:
                await interaction.followup.send(
                    f"VATSIM data feed returned **{resp.status}**.", ephemeral=True
                )
                return
            data: dict = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:
        LOG.warning("VATSIM count fetch failed: %s", exc)
        await interaction.followup.send("Could not reach VATSIM data feed.", ephemeral=True)
        return

    general = data.get("general") or {}
    connected = general.get("connected_clients", 0)
    unique = general.get("unique_users", 0)
    update_ts = general.get("update_timestamp") or ""

    pilots = data.get("pilots") or []
    controllers = data.get("controllers") or []
    atis_list = data.get("atis") or []

    pilot_count = len(pilots)
    atc_count = len(controllers)
    atis_count = len(atis_list)

    ts = _iso_to_unix(update_ts)
    update_str = f"<t:{ts}:R>" if ts else update_ts[:19]

    lines = [
        f"**Pilots online:** {pilot_count:,}",
        f"**ATC online:** {atc_count:,}",
        f"**ATIS stations:** {atis_count:,}",
        f"**Total connected clients:** {connected:,}",
        f"**Unique users:** {unique:,}",
        f"**Data updated:** {update_str}",
    ]
    embed = discord.Embed(
        title="VATSIM online now",
        description="\n".join(lines),
        color=discord.Color.blue(),
    )
    embed.set_footer(text="Source: data.vatsim.net/v3/vatsim-data.json")
    await interaction.followup.send(embed=embed)


def _skylink_api_key() -> str:
    return os.environ.get("SKYLINK_API_KEY", "").strip()


@bot.tree.command(name="notam", description="Active NOTAMs for an airport (SkyLink API)")
@app_commands.describe(icao="4-letter ICAO code (e.g. EGLL)")
async def cmd_notam(interaction: discord.Interaction, icao: str) -> None:
    key = _skylink_api_key()
    if not key:
        await interaction.response.send_message(
            "⚠️ NOTAM lookups are not yet configured — the API key is pending approval.\n"
            "Check back soon!",
            ephemeral=True,
        )
        return

    code = icao.strip().upper()
    if len(code) != 4 or not code.isalnum():
        await interaction.response.send_message("ICAO must be 4 alphanumeric characters.", ephemeral=True)
        return

    session = bot.http_session
    assert session is not None
    await interaction.response.defer(thinking=True)

    try:
        async with session.get(
            f"https://skylink-api.p.rapidapi.com/v3/notams/{code}",
            headers={
                "x-rapidapi-key": key,
                "x-rapidapi-host": "skylink-api.p.rapidapi.com",
            },
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            if resp.status == 404:
                await interaction.followup.send(f"No NOTAMs found for **{code}**.", ephemeral=True)
                return
            if resp.status != 200:
                await interaction.followup.send(f"NOTAM API returned **{resp.status}**.", ephemeral=True)
                return
            data: dict = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:
        LOG.warning("NOTAM fetch failed for %s: %s", code, exc)
        await interaction.followup.send("Could not reach the NOTAM service. Try again shortly.", ephemeral=True)
        return

    notams: list[dict] = data.get("notams") or []
    total: int = data.get("total_count") or len(notams)

    if not notams:
        await interaction.followup.send(f"No active NOTAMs found for **{code}**.")
        return

    embed = discord.Embed(
        title=f"NOTAMs — {code}",
        description=f"{total} active NOTAM{'s' if total != 1 else ''}  ·  showing {min(5, len(notams))}",
        color=discord.Color.orange(),
    )

    for n in notams[:5]:
        notam_id = n.get("notam_id") or n.get("id") or "—"
        body = n.get("body") or n.get("text") or n.get("raw") or "No text"
        effective = n.get("effective_start") or n.get("effective") or ""
        expires = n.get("effective_end") or n.get("expiration") or "PERM"
        # Trim long bodies to fit Discord field limits
        if len(body) > 900:
            body = body[:897] + "…"
        value = f"**{effective[:10] if effective else ''}**"
        if expires:
            value += f" → {expires[:10]}"
        value += f"\n```{body}```"
        embed.add_field(name=notam_id, value=value, inline=False)

    embed.set_footer(text="Source: SkyLink API · FAA SWIM real-time feed")
    await interaction.followup.send(embed=embed)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    token = _normalize_discord_bot_token(os.environ.get("DISCORD_BOT_TOKEN"))
    if not token:
        LOG.error("Set DISCORD_BOT_TOKEN to your Discord bot token.")
        return 1
    try:
        bot.run(token)
    except discord.LoginFailure as exc:
        dots = token.count(".")
        LOG.error(
            "Discord rejected the token (%s). Safe checks: length=%s, dot_count=%s (a real **Bot** token "
            "is usually ~68–72 chars with **exactly 2** dots / three segments). Use Portal → **Bot** → "
            "**Token**, not OAuth2 **Client Secret**. In `.env`: `DISCORD_BOT_TOKEN=...` one line, no quotes, "
            "no spaces around `=`. Confirm systemd uses EnvironmentFile= that same file. "
            "Stop the loop: `sudo systemctl stop aviation-hub-bot` until the token is fixed.",
            exc,
            len(token),
            dots,
        )
        return 1
    except Exception:
        LOG.exception("Bot crashed")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
