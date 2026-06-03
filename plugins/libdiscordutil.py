import asyncio
import re
from datetime import datetime, timezone, timedelta
import plugins.libmesh as LibMesh
import plugins.liblogger as logger
import discord

def _normalize(text):
    """Strip whitespace and role-ping lines for fuzzy matching."""
    lines = text.splitlines()
    lines = [l for l in lines if not re.match(r'^\s*@(here|everyone|\S+)\s*$', l.strip())]
    return re.sub(r'\s+', ' ', '\n'.join(lines)).strip()

async def _find_duplicate(channel, client, config, embed_description=None):
    """Scan recent channel history for a matching message from another bot."""
    dedup = config.get("dedup", {})
    limit = dedup.get("history_limit", 10)
    window = dedup.get("time_window_seconds", 60)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=window)

    try:
        async for msg in channel.history(limit=limit):
            if msg.created_at < cutoff:
                break
            if not msg.author.bot or msg.author == client.user:
                continue
            if embed_description and msg.embeds:
                for emb in msg.embeds:
                    if emb.description and _normalize(emb.description) == _normalize(embed_description):
                        return msg
    except Exception as e:
        logger.warn(f"Dedup: failed to read channel history: {e}")
    return None

_HOP_DIGIT_EMOJI = [
    "0️⃣", "1️⃣", "2️⃣", "3️⃣",
    "4️⃣", "5️⃣", "6️⃣", "7️⃣",
    "8️⃣", "9️⃣",
]
_HOP_TEN_EMOJI = "\U0001f51a"   # 🔟
_HOP_MANY_EMOJI = "\U0001f522"  # 🔢

def hop_reaction_emoji(packet):
    """Return a keycap-digit emoji representing hops-away, or None if unknown.

    Callers pass the resulting string to send_embed as reaction_emoji so that
    the dedup reaction shows how many hops away this collector heard the
    message — small but useful coverage signal when several collectors share
    a Discord channel.
    """
    hop_start = packet.get('hopStart')
    hop_limit = packet.get('hopLimit')
    if hop_start is None or hop_limit is None:
        return None
    hops = hop_start - hop_limit
    if 0 <= hops <= 9:
        return _HOP_DIGIT_EMOJI[hops]
    if hops == 10:
        return _HOP_TEN_EMOJI
    return _HOP_MANY_EMOJI

async def _react_to_duplicate(message, config, reaction_emoji=None):
    """React to an existing duplicate message.

    Uses reaction_emoji when provided, otherwise the configured default.
    """
    if reaction_emoji is None:
        dedup = config.get("dedup", {})
        reaction_emoji = dedup.get("reaction_emoji", "\U0001F4E1")
    try:
        await message.add_reaction(reaction_emoji)
    except Exception as e:
        logger.warn(f"Dedup: failed to add reaction: {e}")

async def _dedup_send_embed(channel, embed, client, config, reaction_emoji=None):
    """Check for duplicate, react if found, otherwise send the embed."""
    dup = await _find_duplicate(channel, client, config, embed_description=embed.description)
    if dup:
        logger.info(f"Dedup: found duplicate in #{channel.name}, reacting instead of posting")
        await _react_to_duplicate(dup, config, reaction_emoji=reaction_emoji)
    else:
        await channel.send(embed=embed)

def genUserName(interface, packet, details=True):
    short = LibMesh.getUserShort(interface, packet)
    long  = LibMesh.getUserLong(interface, packet) or ""
    lat, lon, hasPos = LibMesh.getPosition(interface, packet)

    ret = f"**{long}** \n _(Short: {short})_ " if short is not None else " \n"

    #ret += f"Short: ({short}) " if short is not None else " "

    if details:
        if packet.get("fromId") is not None:
            ret += f"_ID: {packet['fromId']}_ \n"

    if details and hasPos:
        ret += f" [map](<https://www.google.com/maps/search/?api=1&query={lat}%2C{lon}>) "

    if "hopLimit" in packet:
        if "hopStart" in packet:
            ret += f"🐇 {packet['hopStart'] - packet['hopLimit']} of {packet['hopStart']} \n"
        else:
            ret += f"🐇 {packet['hopLimit']} \n"

    if "viaMqtt" in packet and str(packet["viaMqtt"]) == "True":
        ret += " `MQTT`"

    return ret

def send_msg(message,client,config,channel_id=0):
    if config["use_discord"]:
        if (client.is_ready()):
            if config.get("secondary_channel_message_ids") and channel_id and channel_id > 0:
                chan = config["secondary_channel_message_ids"][channel_id-1]
                asyncio.run_coroutine_threadsafe(client.get_channel(chan).send(message),client.loop)
            else:
                for i in config["message_channel_ids"]:
                    asyncio.run_coroutine_threadsafe(client.get_channel(i).send(message),client.loop)

def send_embed(title, description, client, config, channel_id=0, footer=None, color=0x3c90ba, reaction_emoji=None):
    if config["use_discord"]:
        if (client.is_ready()):
            embed = discord.Embed(title=title, description=description, color=color)
            if footer:
                embed.set_footer(text=footer)
            channels = []
            if config.get("secondary_channel_message_ids") and channel_id and channel_id > 0:
                channels.append(config["secondary_channel_message_ids"][channel_id-1])
            else:
                channels = config["message_channel_ids"]
            dedup_enabled = config.get("dedup", {}).get("enabled", False)
            for chan_id in channels:
                channel = client.get_channel(chan_id)
                if dedup_enabled:
                    asyncio.run_coroutine_threadsafe(_dedup_send_embed(channel, embed, client, config, reaction_emoji=reaction_emoji), client.loop)
                else:
                    asyncio.run_coroutine_threadsafe(channel.send(embed=embed), client.loop)

def send_info(message,client,config):
    if config["use_discord"]:
        if (client.is_ready()):
            for i in config["info_channel_ids"]:
                asyncio.run_coroutine_threadsafe(client.get_channel(i).send(message),client.loop)
