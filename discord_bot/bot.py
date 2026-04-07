#!/usr/bin/env python3
"""
Aviation Hub Discord bot (AvBot): slash commands that read from the local widget HTTP API
(same JSON as `data_fetch/src/widget_server.py` serves alongside the ingestor).

Environment:
  DISCORD_BOT_TOKEN       — required; **Bot** token (Developer Portal → Bot → Reset Token).
  AVIATION_HUB_BASE_URL   — optional; default http://127.0.0.1:4010
  DISCORD_GUILD_ID        — optional; sync slash commands to this guild only (faster while testing)

  /info invite & support (optional):
  AVBOT_ADD_BOT_URL       — full OAuth2 “add bot” URL; overrides auto-built link
  DISCORD_APPLICATION_ID  — Application ID (same as OAuth client_id); used to build add-bot link if
                            AVBOT_ADD_BOT_URL is unset
  DISCORD_CLIENT_ID       — alias for DISCORD_APPLICATION_ID (either may be set)
  AVBOT_SUPPORT_SERVER_URL — support Discord invite (e.g. https://discord.gg/…)

Utility slash commands (no hub call): /help lists every command’s description from this tree; /info
shows AvBot text and the invite links above; /ping shows Discord gateway latency.

Discord Developer Portal:
  • **Public Key** — not used here (gateway bot, not an interactions HTTP endpoint).
"""
from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

LOG = logging.getLogger("aviation_hub.discord")


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


def _hub_base() -> str:
    return os.environ.get("AVIATION_HUB_BASE_URL", "http://127.0.0.1:4010").rstrip("/")


def _avbot_add_invite_url() -> str | None:
    """Full OAuth invite, or built from Application ID + default permissions."""
    url = os.environ.get("AVBOT_ADD_BOT_URL", "").strip()
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


def _avbot_support_server_url() -> str | None:
    u = os.environ.get("AVBOT_SUPPORT_SERVER_URL", "").strip()
    return u or None


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

    async def setup_hook(self) -> None:
        self.http_session = aiohttp.ClientSession(
            headers={"User-Agent": "AviationHubDiscord/1.0"},
            timeout=aiohttp.ClientTimeout(total=45),
        )
        # Sync once per process start (avoid repeating on every reconnect in on_ready → rate limits).
        guild_raw = _normalize_snowflake_env(os.environ.get("DISCORD_GUILD_ID"))
        try:
            if guild_raw:
                guild = discord.Object(id=int(guild_raw))
                # Guild sync only uploads *guild* command entries; @bot.tree.command registers globals.
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                LOG.info(
                    "Slash commands synced to guild %s (%s commands). "
                    "If commands are missing in other servers, unset DISCORD_GUILD_ID for global sync.",
                    guild.id,
                    len(synced),
                )
            else:
                synced = await self.tree.sync()
                LOG.info(
                    "Slash commands synced globally (%s commands). "
                    "They can take up to ~1 hour to appear; set DISCORD_GUILD_ID for instant sync in one server.",
                    len(synced),
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

    async def close(self) -> None:
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
        t0 = b.get("starts_at_utc")
        t1 = b.get("ends_at_utc")
        pos = b.get("position_type") or ""
        lines.append(f"`{t0}`–`{t1}` **{cs}** ({pos})".strip())
    desc = "\n".join(lines)
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
    description="Airports busy soon (bookings + events in the next hours, Aviation Hub)",
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
        return f"`{ap}` · score **{r.get('busyness_score')}** (booked ATC positions: {b}, events: {e})"

    likely_staffed = [_fmt_upcoming_row(r) for r in likely_group]
    event_only = [_fmt_upcoming_row(r) for r in event_group]

    desc_parts = [
        f"**How to read this**: `booked ATC positions` = scheduled controller slots in next **{hours}h**. `events` = published VATSIM events touching that airport.",
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
    description="Airports by manned ATC + busyness (inbounds, upcoming, weather score, Aviation Hub)",
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
        lines.append(
            f"`{ap}` · **{r.get('rank_score')}** · {manned} "
            f"ctl={r.get('controller_count')} inb={r.get('inbounds')} "
            f"up={r.get('upcoming_score')}"
        )
    embed = discord.Embed(
        title=f"Ranked — next {hours}h window",
        description=_truncate("\n".join(lines), 3900),
        color=discord.Color.teal(),
    )
    embed.set_footer(text=(data.get("note") or "")[:200])
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
        cat = wx.get("flight_category") or "—"
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
                f"ATC online: **{sp.get('controller_count', 0)}** "
                f"({'yes' if sp.get('has_atc') else 'no'}) · ATIS: "
                f"{'yes' if sp.get('has_atis') else 'no'}",
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
        v_lines.append(f"• `{c.get('callsign')}` {fac}")
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
            name="Inbounds (filed arrival)",
            value=f"Unavailable (`{ib['error']}`)",
            inline=False,
        )
    else:
        icnt = ib.get("count", 0)
        sample = ib.get("pilots_sample") or []
        if icnt == 0:
            embed.add_field(
                name="Inbounds (filed arrival)",
                value=f"**0** online pilots filed **`{code}`**.",
                inline=False,
            )
        else:
            ilines = [f"**{icnt}** online · filed `flight_plan_arrival` = `{code}`"]
            for p in sample[:10]:
                dep = p.get("flight_plan_departure") or "?"
                cs = p.get("callsign")
                ac = (p.get("flight_plan_aircraft") or "").strip()
                ilines.append(f"• `{cs}` {dep}→{code}" + (f" · {ac}" if ac else ""))
            if ib.get("truncated"):
                ilines.append("… `/inbounds` for full list")
            embed.add_field(
                name="Inbounds (sample)",
                value=_truncate("\n".join(ilines), 900),
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
    wind_s = ""
    if isinstance(wind, dict):
        wind_s = f"{wind.get('dir_degrees', '')}° @ {wind.get('speed_kt', '')} kt"
        if wind.get("gust_kt"):
            wind_s += f" G{wind['gust_kt']}"
    embed = discord.Embed(title=f"Weather — {code}", color=discord.Color.teal())
    embed.add_field(name="METAR", value=_truncate(metar, 1000) or "—", inline=False)
    if wx:
        embed.add_field(name="Summary", value=_truncate(wx, 500), inline=False)
    if cat:
        embed.add_field(name="Flight category", value=str(cat), inline=True)
    if wind_s:
        embed.add_field(name="Wind", value=wind_s, inline=True)
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
        lines = [
            f"**{c.get('name') or '—'}** · CID {c.get('cid', '—')}",
            f"**{fac}** · {c.get('frequency') or '—'} · rating {c.get('rating', '—')}",
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


@bot.tree.command(
    name="help",
    description="Show every slash command and its description",
)
async def cmd_help(interaction: discord.Interaction) -> None:
    cmds = list(bot.tree.get_commands())
    by_name = {c.name: c for c in cmds}

    weather_airport_names = ("airport", "metar", "spicy", "summary", "weather")
    vatsim_names = ("bookings", "events", "inbounds", "ranked", "upcoming", "vatsim")
    meta_names = ("help", "info", "ping")

    def pick(names: tuple[str, ...]) -> list[app_commands.AppCommand]:
        return [by_name[n] for n in names if n in by_name]

    weather_cmds = pick(weather_airport_names)
    vatsim_cmds = pick(vatsim_names)
    meta_cmds = pick(meta_names)
    known = set(weather_airport_names) | set(vatsim_names) | set(meta_names)
    other_cmds = [c for c in cmds if c.name not in known]

    embed = discord.Embed(
        title="AvBot · slash commands",
        description="Type **`/`** and start typing to filter. Most commands need the **Aviation Hub** service running on your machine.",
        color=discord.Color.from_rgb(52, 152, 219),
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(
        name="Weather & airports",
        value=_truncate(_help_embed_field_lines(weather_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="VATSIM & traffic",
        value=_truncate(_help_embed_field_lines(vatsim_cmds), 1024) or "—",
        inline=False,
    )
    embed.add_field(
        name="Bot",
        value=_truncate(_help_embed_field_lines(meta_cmds), 1024) or "—",
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
    description="About AvBot, plus links to add the bot and join the support server",
)
async def cmd_info(interaction: discord.Interaction) -> None:
    lines = [
        "**AvBot** is the Aviation Hub Discord client: slash commands call your local **widget HTTP API** "
        "(METAR, VATSIM pilots/controllers lookup, events, bookings, airport summaries, and more).",
        "",
        f"**Hub base URL** (this bot): `{_hub_base()}`",
        "",
    ]
    add = _avbot_add_invite_url()
    if add:
        lines.append(f"**Add AvBot:** [Invite link]({add})")
    else:
        lines.append(
            "**Add AvBot:** set **`AVBOT_ADD_BOT_URL`** (full OAuth URL) or **`DISCORD_APPLICATION_ID`** "
            "on the bot host to show an invite."
        )
    sup = _avbot_support_server_url()
    if sup:
        lines.append(f"**Support server:** [Join]({sup})")
    else:
        lines.append("**Support server:** set **`AVBOT_SUPPORT_SERVER_URL`** on the bot host (e.g. `https://discord.gg/…`).")

    embed = discord.Embed(
        title="AvBot — Aviation Hub",
        description=_truncate("\n".join(lines), 3900),
        color=discord.Color.green(),
    )
    await interaction.response.send_message(embed=embed)


@bot.tree.command(
    name="ping",
    description="Check AvBot's ping to the Discord server (gateway latency)",
)
async def cmd_ping(interaction: discord.Interaction) -> None:
    lat = bot.latency
    if math.isnan(lat):
        ws = "— (heartbeat not ready yet; try again in a moment)"
    else:
        ws = f"**{round(lat * 1000)}** ms"
    await interaction.response.send_message(f"Pong — Discord gateway: {ws}")


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
