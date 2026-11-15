#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Force-Join Reward Bot  —  single file, aiogram 3.x

Install:
    pip install "aiogram>=3.30,<4"

Run:
    python bot.py
"""

import asyncio
import json
import logging
import re
import sqlite3
import threading
from contextlib import closing
from typing import Any, Callable, Iterable, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    MessageEntity,
)

# ─────────────────────────────────────────────────────────────
#  CONFIGURATION  (hardcoded on purpose — no env vars, no .env)
# ─────────────────────────────────────────────────────────────

BOT_TOKEN = "8838401240:AAGz4GgwEPgJfBaD0DWXWnkIk86FwKtF-G4"
ADMIN_ID = 8955367959

JOIN_EMOJI_ID = "5296369303661067030"
CLAIM_EMOJI_ID = "6170055790146098906"

MAX_CHANNELS = 4
CASHBACK_PERCENT = 15
SIGNUP_BONUS = 500.0
DB_PATH = "bot.db"

BROADCAST_DELAY = 0.05  # ~20 messages/sec, inside Telegram limits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
log = logging.getLogger("reward-bot")

# Flipped to False at runtime if Telegram rejects styled / custom-emoji buttons
# (icon_custom_emoji_id needs a Fragment username or a Premium bot owner).
FANCY_BUTTONS = True
BOT_USERNAME = ""


# ─────────────────────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────────────────────

_db_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id     INTEGER PRIMARY KEY,
    username    TEXT,
    first_name  TEXT,
    balance     REAL    NOT NULL DEFAULT 0,
    reward      REAL    NOT NULL DEFAULT 0,
    pending     REAL    NOT NULL DEFAULT 0,
    claimed     INTEGER NOT NULL DEFAULT 0,
    active      INTEGER NOT NULL DEFAULT 1,
    joined_at   TEXT    NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS channels (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id     INTEGER UNIQUE NOT NULL,
    title       TEXT,
    username    TEXT,
    invite_link TEXT,
    added_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pending_channel (
    admin_id    INTEGER PRIMARY KEY,
    raw_link    TEXT,
    username    TEXT,
    chat_id     INTEGER,
    title       TEXT,
    invite_link TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS admins (
    user_id     INTEGER PRIMARY KEY,
    added_at    TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS button_settings (
    slot        INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    entities    TEXT,
    color       TEXT NOT NULL DEFAULT 'green'
);

CREATE INDEX IF NOT EXISTS idx_users_active ON users(active);
CREATE INDEX IF NOT EXISTS idx_users_claimed ON users(claimed);
CREATE INDEX IF NOT EXISTS idx_admins_user_id ON admins(user_id);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-8000")
    return conn


def _run(sql: str, params: Iterable[Any] = (), fetch: Optional[str] = None) -> Any:
    with _db_lock, closing(_connect()) as conn:
        cur = conn.execute(sql, tuple(params))
        if fetch == "one":
            out = cur.fetchone()
        elif fetch == "all":
            out = cur.fetchall()
        else:
            out = cur.rowcount
        conn.commit()
        return out


async def db(sql: str, params: Iterable[Any] = (), fetch: Optional[str] = None) -> Any:
    return await asyncio.to_thread(_run, sql, params, fetch)


# Small TTL caches keep hot user paths off SQLite without changing persisted data.
_CACHE_TTL = 3.0
_channels_cache: tuple[float, list[sqlite3.Row]] = (0.0, [])
_settings_cache: tuple[float, dict[str, Optional[str]]] = (0.0, {})
_button_cache: tuple[float, dict[int, sqlite3.Row]] = (0.0, {})


def _invalidate_config_cache() -> None:
    global _channels_cache, _settings_cache, _button_cache
    _channels_cache = (0.0, [])
    _settings_cache = (0.0, {})
    _button_cache = (0.0, {})


def init_db() -> None:
    with _db_lock, closing(_connect()) as conn:
        conn.executescript(SCHEMA)
        conn.commit()
    log.info("Database ready at %s", DB_PATH)


async def get_setting(key: str) -> Optional[str]:
    global _settings_cache
    now = asyncio.get_running_loop().time()
    ts, cache = _settings_cache
    if now - ts > _CACHE_TTL:
        rows = await db("SELECT key, value FROM settings", fetch="all")
        cache = {row["key"]: row["value"] for row in rows}
        _settings_cache = (now, cache)
    return cache.get(key)


async def set_setting(key: str, value: Optional[str]) -> None:
    await db(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


async def register_user(user) -> sqlite3.Row:
    await db(
        "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET username = excluded.username, "
        "first_name = excluded.first_name, active = 1",
        (user.id, user.username, user.first_name),
    )
    return await db(
        "SELECT * FROM users WHERE user_id = ?", (user.id,), fetch="one"
    )


async def get_channels() -> list[sqlite3.Row]:
    global _channels_cache
    now = asyncio.get_running_loop().time()
    ts, cache = _channels_cache
    if now - ts <= _CACHE_TTL:
        return cache
    rows = await db("SELECT * FROM channels ORDER BY id", fetch="all")
    _channels_cache = (now, rows)
    return rows


async def is_admin_user(user_id: int) -> bool:
    if user_id == ADMIN_ID:
        return True
    row = await db("SELECT 1 FROM admins WHERE user_id = ?", (user_id,), fetch="one")
    return bool(row)


async def get_admins() -> list[sqlite3.Row]:
    return await db("SELECT user_id, added_at FROM admins ORDER BY added_at", fetch="all")


async def get_button_setting(slot: int) -> Optional[sqlite3.Row]:
    return await db(
        "SELECT * FROM button_settings WHERE slot = ?", (slot,), fetch="one"
    )


async def get_button_settings() -> dict[int, sqlite3.Row]:
    global _button_cache
    now = asyncio.get_running_loop().time()
    ts, cache = _button_cache
    if now - ts <= _CACHE_TTL:
        return cache
    rows = await db("SELECT * FROM button_settings ORDER BY slot", fetch="all")
    cache = {int(row["slot"]): row for row in rows}
    _button_cache = (now, cache)
    return cache


async def save_button_setting(
    slot: int, name: str, entities: Optional[list[MessageEntity]], color: str
) -> None:
    await db(
        "INSERT INTO button_settings (slot, name, entities, color) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(slot) DO UPDATE SET name = excluded.name, "
        "entities = excluded.entities, color = excluded.color",
        (slot, name, dump_entities(entities), color),
    )
    _invalidate_config_cache()


# ─────────────────────────────────────────────────────────────
#  ENTITY HELPERS  (premium custom emoji must survive round-trips)
# ─────────────────────────────────────────────────────────────


def dump_entities(entities: Optional[list[MessageEntity]]) -> Optional[str]:
    if not entities:
        return None
    return json.dumps(
        [e.model_dump(exclude_none=True, mode="json") for e in entities],
        ensure_ascii=False,
    )


def load_entities(raw: Optional[str]) -> Optional[list[MessageEntity]]:
    if not raw:
        return None
    try:
        return [MessageEntity.model_validate(d) for d in json.loads(raw)]
    except Exception:
        log.warning("Could not decode stored entities", exc_info=True)
        return None


def strip_custom_emoji(
    entities: Optional[list[MessageEntity]],
) -> Optional[list[MessageEntity]]:
    if not entities:
        return None
    kept = [e for e in entities if e.type != "custom_emoji"]
    return kept or None


def u16(text: str) -> int:
    """Telegram measures entity offsets in UTF-16 code units."""
    return len(text.encode("utf-16-le")) // 2


class TextBuilder:
    """Builds text + entities together so offsets are always correct."""

    def __init__(
        self, text: str = "", entities: Optional[list[MessageEntity]] = None
    ) -> None:
        self.text = text
        self.entities: list[MessageEntity] = list(entities or [])

    def add(self, chunk: str, entity_type: Optional[str] = None, **kwargs) -> "TextBuilder":
        if entity_type:
            self.entities.append(
                MessageEntity(
                    type=entity_type,
                    offset=u16(self.text),
                    length=u16(chunk),
                    **kwargs,
                )
            )
        self.text += chunk
        return self


def _is_style_error(err: Exception) -> bool:
    text = str(err).upper()
    return any(
        token in text
        for token in ("EMOJI", "PREMIUM", "STYLE", "BUTTON_TYPE", "ICON")
    )


# ─────────────────────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────────────────────


def btn(
    text: str,
    *,
    style: Optional[str] = None,
    icon: Optional[str] = None,
    **kwargs,
) -> InlineKeyboardButton:
    extra: dict[str, Any] = {}
    if FANCY_BUTTONS:
        if style:
            extra["style"] = style
        if icon:
            extra["icon_custom_emoji_id"] = icon
    return InlineKeyboardButton(text=text, **extra, **kwargs)


COLOR_STYLES = {"red": "danger", "blue": "primary", "green": "success"}


def custom_emoji_ids(entities: Optional[list[MessageEntity]]) -> list[str]:
    """Return Premium Custom Emoji IDs from a Telegram message."""
    if not entities:
        return []
    return [
        e.custom_emoji_id
        for e in entities
        if e.type == "custom_emoji" and e.custom_emoji_id
    ]


def remove_custom_emoji_characters(
    text: str, entities: Optional[list[MessageEntity]]
) -> str:
    """Remove characters covered by custom-emoji entities using UTF-16 offsets."""
    if not text or not entities:
        return text
    spans = []
    for e in entities:
        if e.type == "custom_emoji":
            spans.append((e.offset, e.offset + e.length))
    if not spans:
        return text

    out = []
    pos16 = 0
    for ch in text:
        ch_len = len(ch.encode("utf-16-le")) // 2
        end16 = pos16 + ch_len
        if not any(pos16 < end and end16 > start for start, end in spans):
            out.append(ch)
        pos16 = end16
    return "".join(out).strip()


def button_label_and_icon(
    name: str, entities: Optional[list[MessageEntity]], default_icon: Optional[str] = None
) -> tuple[str, Optional[str]]:
    """
    Telegram inline buttons cannot carry MessageEntity objects inside their text.
    Bot API exposes Premium Custom Emoji on buttons through icon_custom_emoji_id.
    Therefore a Premium emoji sent by the admin becomes the button's Premium icon,
    while the remaining text stays unchanged.
    """
    ids = custom_emoji_ids(entities)
    if ids:
        label = remove_custom_emoji_characters(name, entities)
        # A Premium Custom Emoji-only button still needs a non-empty Telegram
        # button text. Use a zero-width character so only the Premium icon is
        # visibly shown instead of the old "Button" fallback.
        if not label:
            label = "\u200b"
        return label, ids[0]
    return name or "Button", default_icon


async def configured_button(slot: int, default_name: str, default_color: str, **kwargs):
    row = await get_button_setting(slot)
    if row:
        name = row["name"] or default_name
        style = COLOR_STYLES.get((row["color"] or default_color).lower(), "success")
        entities = load_entities(row["entities"])
        label, icon = button_label_and_icon(name, entities)
        return btn(label, style=style, icon=icon, **kwargs)
    return btn(default_name, style=default_color, **kwargs)


async def start_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    join_row: list[InlineKeyboardButton] = []
    settings = await get_button_settings()

    for index, ch in enumerate(await get_channels(), 1):
        url = ch["invite_link"] or (
            f"https://t.me/{ch['username']}" if ch["username"] else None
        )
        if not url:
            continue

        row = settings.get(index)
        if row:
            name = row["name"] or "Join"
            style = COLOR_STYLES.get((row["color"] or "green").lower(), "success")
            entities = load_entities(row["entities"])
            label, icon = button_label_and_icon(name, entities)
            join_row.append(btn(label, style=style, icon=icon, url=url))
        else:
            join_row.append(btn("Join", style="success", icon=JOIN_EMOJI_ID, url=url))

        if len(join_row) == 2:
            rows.append(join_row)
            join_row = []

    if join_row:
        rows.append(join_row)

    # Full-width Claim button (slot 5), independently configurable.
    claim = settings.get(5)
    if claim:
        name = claim["name"] or "Claim"
        style = COLOR_STYLES.get((claim["color"] or "green").lower(), "success")
        entities = load_entities(claim["entities"])
        label, icon = button_label_and_icon(name, entities)
        rows.append([btn(label, style=style, icon=icon, callback_data="claim")])
    else:
        rows.append([btn("Claim", style="success", icon=CLAIM_EMOJI_ID, callback_data="claim")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("📢 Broadcast", style="primary", callback_data="adm:broadcast")],
            [btn("📊 Statistics", style="primary", callback_data="adm:stats")],
            [btn("➕ Add Channel", style="success", callback_data="adm:addch")],
            [btn("🎬 Upload Reward Video", style="success", callback_data="adm:video")],
            [btn("✍️ Set Welcome Message", style="primary", callback_data="adm:welcome")],
            [btn("👤 Add New Admin", style="success", callback_data="adm:addadmin")],
            [btn("🎨 Button Settings", style="primary", callback_data="adm:buttons")],
        ]
    )


def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("✖️ Cancel", style="danger", callback_data="adm:cancel")]]
    )


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[btn("⬅️ Back", style="primary", callback_data="adm:menu")]]
    )


def confirm_channel_keyboard() -> InlineKeyboardMarkup:
    add_url = (
        f"https://t.me/{BOT_USERNAME}?startchannel&admin="
        "invite_users+post_messages+change_info"
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [btn("➕ Add to Channel", style="primary", url=add_url)],
            [btn("✅ Confirm", style="success", callback_data="adm:confirm_ch")],
            [btn("✖️ Cancel", style="danger", callback_data="adm:cancel")],
        ]
    )


# ─────────────────────────────────────────────────────────────
#  SAFE SEND / EDIT  (auto-fallback for styled buttons + emoji)
# ─────────────────────────────────────────────────────────────


async def send_text(
    bot: Bot,
    chat_id: int,
    text: str,
    entities: Optional[list[MessageEntity]] = None,
    kb_factory: Optional[Callable[[], Any]] = None,
) -> Optional[Message]:
    global FANCY_BUTTONS
    markup = kb_factory() if kb_factory else None
    try:
        return await bot.send_message(
            chat_id, text, entities=entities, reply_markup=markup
        )
    except TelegramBadRequest as err:
        if not _is_style_error(err):
            raise
        log.warning("Falling back to plain buttons/entities: %s", err)
        FANCY_BUTTONS = False
        markup = kb_factory() if kb_factory else None
        return await bot.send_message(
            chat_id,
            text,
            entities=strip_custom_emoji(entities),
            reply_markup=markup,
        )


async def edit_text(
    message: Message,
    text: str,
    entities: Optional[list[MessageEntity]] = None,
    kb_factory: Optional[Callable[[], Any]] = None,
) -> None:
    global FANCY_BUTTONS
    markup = kb_factory() if kb_factory else None
    try:
        await message.edit_text(text, entities=entities, reply_markup=markup)
        return
    except TelegramBadRequest as err:
        if "message is not modified" in str(err).lower():
            return
        if not _is_style_error(err):
            # Message may be un-editable (e.g. it carried media) — send instead.
            await send_text(message.bot, message.chat.id, text, entities, kb_factory)
            return
        FANCY_BUTTONS = False
        markup = kb_factory() if kb_factory else None
        await message.edit_text(
            text, entities=strip_custom_emoji(entities), reply_markup=markup
        )


# ─────────────────────────────────────────────────────────────
#  START MESSAGE COMPOSITION
# ─────────────────────────────────────────────────────────────

DEFAULT_WELCOME = "👋 Welcome! Your daily earning account is ready."


async def save_welcome_media(message: Message) -> None:
    """Save a photo/GIF sent by admin as the welcome media, preserving caption entities."""
    media_type = None
    file_id = None

    if message.photo:
        media_type = "photo"
        file_id = message.photo[-1].file_id
    elif message.animation:
        media_type = "animation"
        file_id = message.animation.file_id

    if not file_id:
        return

    await set_setting("welcome_media_type", media_type)
    await set_setting("welcome_media_file_id", file_id)
    await set_setting("welcome_media_caption", message.caption or "")
    await set_setting(
        "welcome_media_caption_entities",
        dump_entities(message.caption_entities),
    )


async def get_welcome_media():
    """Return the admin-configured welcome media and its caption entities."""
    media_type = await get_setting("welcome_media_type")
    file_id = await get_setting("welcome_media_file_id")
    caption = await get_setting("welcome_media_caption")
    entities = load_entities(
        await get_setting("welcome_media_caption_entities")
    )
    return media_type, file_id, caption, entities


async def build_start_message(row: sqlite3.Row) -> tuple[str, list[MessageEntity]]:
    welcome = await get_setting("welcome_text") or DEFAULT_WELCOME
    welcome_entities = load_entities(await get_setting("welcome_entities"))

    # The custom welcome replaces the old hardcoded welcome/marketing text.
    # Keep only the live payment/balance card and the force-join instruction below it.
    b = TextBuilder(welcome, welcome_entities)
    b.add("\n\n")
    b.add("🔗", "custom_emoji", custom_emoji_id=JOIN_EMOJI_ID)
    b.add(" Join every channel below, then press ")
    b.add("✅", "custom_emoji", custom_emoji_id=CLAIM_EMOJI_ID)
    b.add(" Claim")
    return b.text, b.entities


async def show_start(bot: Bot, chat_id: int, row: sqlite3.Row) -> None:
    text, entities = await build_start_message(row)
    kb = await start_keyboard()

    media_type, file_id, caption, caption_entities = await get_welcome_media()

    # When an admin uploads an image/GIF with a caption, that exact welcome
    # caption is the message text and the 4 Join + Claim buttons are attached
    # directly underneath it. No second welcome message is sent.
    if media_type and file_id:
        try:
            final_caption = caption or ""
            final_entities = caption_entities

            if media_type == "photo":
                await bot.send_photo(
                    chat_id,
                    photo=file_id,
                    caption=final_caption or None,
                    caption_entities=final_entities,
                    reply_markup=kb,
                )
            elif media_type == "animation":
                await bot.send_animation(
                    chat_id,
                    animation=file_id,
                    caption=final_caption or None,
                    caption_entities=final_entities,
                    reply_markup=kb,
                )
            return
        except Exception as err:
            log.warning("Welcome media send failed, falling back to text: %s", err)

    # Text-only welcome also gets the same Join + Claim keyboard.
    await send_text(bot, chat_id, text, entities, lambda: kb)


# ─────────────────────────────────────────────────────────────
#  MEMBERSHIP CHECK
# ─────────────────────────────────────────────────────────────

JOINED_STATUSES = {
    ChatMemberStatus.CREATOR,
    ChatMemberStatus.ADMINISTRATOR,
    ChatMemberStatus.MEMBER,
}


async def is_member(bot: Bot, chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except TelegramBadRequest as err:
        log.warning("Membership check failed for %s: %s", chat_id, err)
        return False
    except Exception:
        log.exception("Unexpected membership check error for %s", chat_id)
        return False
    if member.status in JOINED_STATUSES:
        return True
    if member.status == ChatMemberStatus.RESTRICTED:
        return bool(getattr(member, "is_member", False))
    return False


async def missing_channels(bot: Bot, user_id: int) -> list[sqlite3.Row]:
    channels = await get_channels()
    if not channels:
        return []
    results = await asyncio.gather(
        *(is_member(bot, ch["chat_id"], user_id) for ch in channels)
    )
    return [ch for ch, ok in zip(channels, results) if not ok]


# ─────────────────────────────────────────────────────────────
#  FSM STATES
# ─────────────────────────────────────────────────────────────


class AdminSG(StatesGroup):
    channel_link = State()
    reward_video = State()
    welcome_msg = State()
    broadcast = State()
    add_admin = State()
    button_name = State()


dp = Dispatcher(storage=MemoryStorage())
IS_ADMIN = F.from_user.id == ADMIN_ID


# ─────────────────────────────────────────────────────────────
#  USER HANDLERS
# ─────────────────────────────────────────────────────────────


@dp.message(CommandStart())
async def cmd_start(message: Message, bot: Bot, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_ID:
        await state.clear()
    row = await register_user(message.from_user)
    await show_start(bot, message.chat.id, row)


async def send_reward(bot: Bot, chat_id: int) -> bool:
    file_id = await get_setting("reward_file_id")
    if not file_id:
        return False

    media_type = await get_setting("reward_media_type") or "video"
    caption = await get_setting("reward_caption")
    entities = load_entities(await get_setting("reward_caption_entities"))

    senders = {
        "video": bot.send_video,
        "animation": bot.send_animation,
        "document": bot.send_document,
        "photo": bot.send_photo,
    }
    sender = senders.get(media_type, bot.send_video)
    kwarg = {
        "video": "video",
        "animation": "animation",
        "document": "document",
        "photo": "photo",
    }.get(media_type, "video")

    payload = {kwarg: file_id, "caption": caption, "caption_entities": entities}
    try:
        await sender(chat_id, **payload)
    except TelegramBadRequest as err:
        if not _is_style_error(err):
            raise
        payload["caption_entities"] = strip_custom_emoji(entities)
        await sender(chat_id, **payload)
    return True


@dp.callback_query(F.data == "claim")
async def cb_claim(callback: CallbackQuery, bot: Bot) -> None:
    user_id = callback.from_user.id
    await register_user(callback.from_user)

    pending = await missing_channels(bot, user_id)
    if pending:
        await callback.answer("Please join all channels first.", show_alert=True)
        return

    await callback.answer("Claim successful ✅", show_alert=False)

    row = await db("SELECT * FROM users WHERE user_id = ?", (user_id,), fetch="one")
    first_claim = row is not None and not row["claimed"]
    if first_claim:
        await db(
            "UPDATE users SET claimed = 1, reward = reward + ? WHERE user_id = ?",
            (SIGNUP_BONUS, user_id),
        )

    lines = ["✅ Claim successful — all channels verified."]
    if first_claim:
        lines.append(f"🎉 ₹{int(SIGNUP_BONUS)} signup bonus credited to your reward.")
    lines.append("\n🎁 Here is your reward:")
    await bot.send_message(callback.message.chat.id, "\n".join(lines))

    if not await send_reward(bot, callback.message.chat.id):
        await bot.send_message(
            callback.message.chat.id,
            "⚠️ The reward is not configured yet. Please try again shortly.",
        )


# ─────────────────────────────────────────────────────────────
#  ADMIN PANEL
# ─────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    await state.clear()
    await send_text(
        message.bot,
        message.chat.id,
        "🛠 Admin Panel\n\nChoose an action below.",
        None,
        admin_keyboard,
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    await state.clear()
    await message.answer("Cancelled.", reply_markup=back_keyboard())


@dp.callback_query(F.data == "adm:menu")
async def cb_menu(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await state.clear()
    await callback.answer()
    await edit_text(
        callback.message, "🛠 Admin Panel\n\nChoose an action below.", None, admin_keyboard
    )


@dp.callback_query(F.data == "adm:cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await state.clear()
    await db("DELETE FROM pending_channel WHERE admin_id = ?", (ADMIN_ID,))
    await callback.answer("Cancelled.")
    await edit_text(
        callback.message, "🛠 Admin Panel\n\nChoose an action below.", None, admin_keyboard
    )


# ── Statistics ───────────────────────────────────────────────


@dp.callback_query(F.data == "adm:stats")
async def cb_stats(callback: CallbackQuery) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    total = (await db("SELECT COUNT(*) c FROM users", fetch="one"))["c"]
    active = (
        await db("SELECT COUNT(*) c FROM users WHERE active = 1", fetch="one")
    )["c"]
    claimed = (
        await db("SELECT COUNT(*) c FROM users WHERE claimed = 1", fetch="one")
    )["c"]
    channels = await get_channels()
    video_set = bool(await get_setting("reward_file_id"))
    welcome_set = bool(await get_setting("welcome_text"))

    lines = [
        "📊 Statistics",
        "━━━━━━━━━━━━━━━━━━━━",
        f"👥 Total users     : {total}",
        f"✅ Active users    : {active}",
        f"🎁 Claimed reward  : {claimed}",
        f"📢 Channels        : {len(channels)} / {MAX_CHANNELS}",
        f"🎬 Reward video    : {'configured' if video_set else 'not set'}",
        f"✍️ Welcome message : {'custom' if welcome_set else 'default'}",
        "━━━━━━━━━━━━━━━━━━━━",
    ]
    if channels:
        lines.append("\nForce-join channels:")
        for i, ch in enumerate(channels, 1):
            handle = f"@{ch['username']}" if ch["username"] else "private"
            lines.append(f"{i}. {ch['title'] or ch['chat_id']} ({handle})")
        lines.append("\nUse /channels to remove one.")
    await edit_text(callback.message, "\n".join(lines), None, back_keyboard)


@dp.message(Command("channels"))
async def cmd_channels(message: Message) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    channels = await get_channels()
    if not channels:
        await message.answer("No channels configured yet.")
        return
    rows = [
        [
            btn(
                f"🗑 {ch['title'] or ch['chat_id']}",
                style="danger",
                callback_data=f"adm:delch:{ch['id']}",
            )
        ]
        for ch in channels
    ]
    await message.answer(
        "Configured channels — tap one to remove it:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
    )


@dp.callback_query(F.data.startswith("adm:delch:"))
async def cb_delete_channel(callback: CallbackQuery) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    channel_id = int(callback.data.split(":")[-1])
    await db("DELETE FROM channels WHERE id = ?", (channel_id,))
    _invalidate_config_cache()
    await callback.answer("Channel removed.")
    remaining = await get_channels()
    if not remaining:
        await edit_text(callback.message, "No channels configured.", None, back_keyboard)
        return
    rows = [
        [
            btn(
                f"🗑 {ch['title'] or ch['chat_id']}",
                style="danger",
                callback_data=f"adm:delch:{ch['id']}",
            )
        ]
        for ch in remaining
    ]
    await edit_text(
        callback.message,
        "Configured channels — tap one to remove it:",
        None,
        lambda: InlineKeyboardMarkup(inline_keyboard=rows),
    )


# ── Add Channel ──────────────────────────────────────────────

USERNAME_RE = re.compile(
    r"(?:https?://)?(?:t(?:elegram)?\.me/|@)([A-Za-z0-9_]{4,32})/?$", re.I
)
INVITE_RE = re.compile(
    r"(?:https?://)?t(?:elegram)?\.me/(?:joinchat/|\+)([A-Za-z0-9_\-]+)/?$", re.I
)


def parse_channel_link(text: str) -> tuple[str, Optional[str]]:
    text = (text or "").strip()
    if INVITE_RE.match(text):
        return "invite", text
    m = USERNAME_RE.match(text)
    if m:
        return "username", m.group(1)
    return "unknown", None


@dp.callback_query(F.data == "adm:addch")
async def cb_add_channel(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    if len(await get_channels()) >= MAX_CHANNELS:
        await edit_text(
            callback.message,
            f"⚠️ Limit reached — {MAX_CHANNELS} force-join channels are already "
            "configured.\n\nUse /channels to remove one first.",
            None,
            back_keyboard,
        )
        return
    await state.set_state(AdminSG.channel_link)
    await edit_text(
        callback.message,
        "Please send the channel link.\n\n"
        "Accepted formats:\n"
        "• https://t.me/yourchannel\n"
        "• @yourchannel\n"
        "• https://t.me/+InviteHash (private)\n\n"
        "You can also simply forward any message from the channel.",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.channel_link)
async def on_channel_link(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    raw_link = None
    username = None
    chat_id = None
    title = None

    forwarded = getattr(message, "forward_from_chat", None)
    if forwarded is not None:
        chat_id = forwarded.id
        username = forwarded.username
        title = forwarded.title
        raw_link = f"https://t.me/{username}" if username else None
    else:
        kind, value = parse_channel_link(message.text or "")
        if kind == "unknown":
            await message.answer(
                "❌ That does not look like a channel link.\n\n"
                "Send something like https://t.me/yourchannel, @yourchannel, "
                "or a https://t.me/+ invite link.",
                reply_markup=cancel_keyboard(),
            )
            return
        if kind == "username":
            username = value
            raw_link = f"https://t.me/{value}"
        else:
            raw_link = value

    await db("DELETE FROM pending_channel WHERE admin_id = ?", (ADMIN_ID,))
    await db(
        "INSERT INTO pending_channel (admin_id, raw_link, username, chat_id, title) "
        "VALUES (?, ?, ?, ?, ?)",
        (ADMIN_ID, raw_link, username, chat_id, title),
    )
    await state.clear()

    target = f"@{username}" if username else (title or raw_link or "the channel")
    await send_text(
        message.bot,
        message.chat.id,
        f"Channel saved for setup: {target}\n\n"
        "Now do the following:\n"
        "1. Add this bot to the channel.\n"
        "2. Make the bot an administrator there.\n"
        "3. The bot needs administrator access so it can verify whether users "
        "have actually joined — membership checks fail without it.\n\n"
        "Use the button below to open Telegram's add-to-channel flow, then press "
        "Confirm.",
        None,
        confirm_channel_keyboard,
    )


@dp.callback_query(F.data == "adm:confirm_ch")
async def cb_confirm_channel(callback: CallbackQuery, bot: Bot) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    pending = await db(
        "SELECT * FROM pending_channel WHERE admin_id = ?", (ADMIN_ID,), fetch="one"
    )
    if not pending:
        await edit_text(
            callback.message,
            "⚠️ No channel setup in progress. Start again from the admin panel.",
            None,
            back_keyboard,
        )
        return

    if len(await get_channels()) >= MAX_CHANNELS:
        await edit_text(
            callback.message,
            f"⚠️ Limit reached — {MAX_CHANNELS} channels are already configured.",
            None,
            back_keyboard,
        )
        return

    # Resolve the chat: prefer an id detected via my_chat_member, else the username.
    chat = None
    if pending["chat_id"]:
        try:
            chat = await bot.get_chat(pending["chat_id"])
        except Exception as err:
            log.warning("get_chat by id failed: %s", err)
    if chat is None and pending["username"]:
        try:
            chat = await bot.get_chat(f"@{pending['username']}")
        except Exception as err:
            log.warning("get_chat by username failed: %s", err)

    if chat is None:
        await edit_text(
            callback.message,
            "❌ Could not detect the channel.\n\n"
            "For private channels the bot cannot resolve an invite link on its own. "
            "Add the bot to the channel as an administrator first — it will then "
            "detect the channel automatically — or forward a message from the "
            "channel and try again.",
            None,
            confirm_channel_keyboard,
        )
        return

    try:
        me = await bot.get_chat_member(chat.id, (await bot.get_me()).id)
    except Exception as err:
        log.warning("Bot membership check failed: %s", err)
        await edit_text(
            callback.message,
            "❌ The bot is not inside that channel yet.\n\n"
            "Add it to the channel, promote it to administrator, then press Confirm "
            "again.",
            None,
            confirm_channel_keyboard,
        )
        return

    if me.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
        await edit_text(
            callback.message,
            f"❌ The bot is in “{chat.title}” but is not an administrator.\n\n"
            "Open the channel → Administrators → add this bot, then press Confirm "
            "again.",
            None,
            confirm_channel_keyboard,
        )
        return

    invite_link = None
    if not chat.username:
        try:
            invite = await bot.create_chat_invite_link(chat.id, name="Force Join")
            invite_link = invite.invite_link
        except Exception as err:
            log.warning("Invite link creation failed: %s", err)
            invite_link = pending["raw_link"]

    await db(
        "INSERT INTO channels (chat_id, title, username, invite_link) "
        "VALUES (?, ?, ?, ?) "
        "ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title, "
        "username = excluded.username, invite_link = excluded.invite_link",
        (chat.id, chat.title, chat.username, invite_link),
    )
    await db("DELETE FROM pending_channel WHERE admin_id = ?", (ADMIN_ID,))

    total = len(await get_channels())
    await edit_text(
        callback.message,
        f"✅ Channel saved.\n\n"
        f"Title : {chat.title}\n"
        f"ID    : {chat.id}\n"
        f"Link  : {('@' + chat.username) if chat.username else (invite_link or 'private')}\n\n"
        f"Configured channels: {total}/{MAX_CHANNELS}\n"
        "It now appears as a Join button on /start.",
        None,
        back_keyboard,
    )


@dp.my_chat_member()
async def on_my_chat_member(event: ChatMemberUpdated, bot: Bot) -> None:
    """Auto-detect channels (including private ones) when the bot is promoted."""
    chat = event.chat
    if chat.type not in ("channel", "supergroup"):
        return
    status = event.new_chat_member.status

    if status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
        removed = await db(
            "SELECT * FROM channels WHERE chat_id = ?", (chat.id,), fetch="one"
        )
        if removed:
            await db("DELETE FROM channels WHERE chat_id = ?", (chat.id,))
            _invalidate_config_cache()
            try:
                await bot.send_message(
                    ADMIN_ID,
                    f"⚠️ The bot was removed from “{chat.title}”. "
                    "That channel is no longer used for force-join.",
                )
            except Exception:
                pass
        return

    if status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
        return

    await db(
        "UPDATE channels SET title = ?, username = ? WHERE chat_id = ?",
        (chat.title, chat.username, chat.id),
    )

    pending = await db(
        "SELECT * FROM pending_channel WHERE admin_id = ?", (ADMIN_ID,), fetch="one"
    )
    if not pending:
        return
    if pending["username"] and chat.username:
        if pending["username"].lower() != chat.username.lower():
            return

    await db(
        "UPDATE pending_channel SET chat_id = ?, title = ?, username = ? "
        "WHERE admin_id = ?",
        (chat.id, chat.title, chat.username, ADMIN_ID),
    )
    try:
        await send_text(
            bot,
            ADMIN_ID,
            f"✅ Detected “{chat.title}” with administrator rights.\n\n"
            "Press Confirm to save it as a force-join channel.",
            None,
            confirm_channel_keyboard,
        )
    except Exception:
        log.exception("Could not notify admin about detected channel")


# ── Upload Reward Video ──────────────────────────────────────


@dp.callback_query(F.data == "adm:video")
async def cb_upload_video(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(AdminSG.reward_video)
    await edit_text(
        callback.message,
        "Please send the reward video with its caption.\n\n"
        "The caption is stored exactly as you send it — links, formatting and "
        "premium custom emojis are all preserved — and it is delivered together "
        "with the video after a user claims.",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.reward_video)
async def on_reward_video(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    media_type = None
    file_id = None
    if message.video:
        media_type, file_id = "video", message.video.file_id
    elif message.animation:
        media_type, file_id = "animation", message.animation.file_id
    elif message.document:
        media_type, file_id = "document", message.document.file_id
    elif message.photo:
        media_type, file_id = "photo", message.photo[-1].file_id

    if not file_id:
        await message.answer(
            "❌ Please send an actual video file (or GIF / document / photo) "
            "with the caption attached.",
            reply_markup=cancel_keyboard(),
        )
        return

    await set_setting("reward_media_type", media_type)
    await set_setting("reward_file_id", file_id)
    await set_setting("reward_caption", message.caption)
    await set_setting("reward_caption_entities", dump_entities(message.caption_entities))
    await state.clear()

    caption_len = len(message.caption or "")
    await send_text(
        message.bot,
        message.chat.id,
        "✅ Reward saved.\n\n"
        f"Type    : {media_type}\n"
        f"Caption : {caption_len} characters stored unchanged\n"
        f"Entities: {len(message.caption_entities or [])}\n\n"
        "It will be sent exactly like this after a successful claim.",
        None,
        back_keyboard,
    )


# ── Set Welcome Message ──────────────────────────────────────


@dp.callback_query(F.data == "adm:welcome")
async def cb_set_welcome(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(AdminSG.welcome_msg)
    await edit_text(
        callback.message,
        "Please send the new welcome message.\n\n"
        "You can send text, or an image/GIF with a caption. "
        "Formatting, links and Premium Custom Emojis are preserved.",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.welcome_msg)
async def on_welcome_message(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    # Photo/GIF + caption is supported as the welcome media.
    if message.photo or message.animation:
        await save_welcome_media(message)

        caption = message.caption or ""
        entities = message.caption_entities or []
        await set_setting("welcome_text", caption)
        await set_setting("welcome_entities", dump_entities(entities))

        await state.clear()
        custom = sum(1 for e in entities if e.type == "custom_emoji")

        await send_text(
            message.bot,
            message.chat.id,
            "✅ Welcome image/message saved.\n\n"
            f"Premium emojis: {custom}\n"
            "The image will appear above the welcome/card on /start.\n\n"
            "Send /start to preview it.",
            None,
            back_keyboard,
        )
        return

    text = message.text
    entities = message.entities
    if not text:
        await message.answer(
            "❌ Send text, or send an image/GIF with its caption.",
            reply_markup=cancel_keyboard()
        )
        return

    await set_setting("welcome_text", text)
    await set_setting("welcome_entities", dump_entities(entities))
    await set_setting("welcome_media_type", "")
    await set_setting("welcome_media_file_id", "")
    await set_setting("welcome_media_caption", "")
    await set_setting("welcome_media_caption_entities", "")

    await state.clear()
    custom = sum(1 for e in (entities or []) if e.type == "custom_emoji")

    await send_text(
        message.bot,
        message.chat.id,
        "✅ Welcome message saved.\n\n"
        f"Premium emojis: {custom}\n\n"
        "Send /start to preview it.",
        None,
        back_keyboard,
    )


# ── Broadcast ────────────────────────────────────────────────


@dp.callback_query(F.data == "adm:broadcast")
async def cb_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(AdminSG.broadcast)
    total = (await db("SELECT COUNT(*) c FROM users WHERE active = 1", fetch="one"))["c"]
    await edit_text(
        callback.message,
        f"Send the message you want to broadcast to {total} users.\n\n"
        "Text, photos, videos, documents and albums-as-single-items are all "
        "supported — the message is copied exactly as you send it.",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.broadcast)
async def on_broadcast(message: Message, bot: Bot, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    await state.clear()

    users = await db("SELECT user_id FROM users WHERE active = 1", fetch="all")
    user_ids = [int(row["user_id"]) for row in users]
    total = len(user_ids)
    if not total:
        await message.answer("No users to broadcast to.", reply_markup=back_keyboard())
        return

    status = await message.answer(f"📢 Broadcasting to {total} users…")

    # Bounded concurrency keeps the event loop responsive while respecting Telegram
    # flood limits. RetryAfter is handled per worker instead of stalling everyone.
    concurrency = 12
    queue: asyncio.Queue[int] = asyncio.Queue()
    for uid in user_ids:
        queue.put_nowait(uid)

    sent = failed = blocked = 0
    counter_lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal sent, failed, blocked
        while True:
            try:
                uid = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                while True:
                    try:
                        await bot.copy_message(
                            chat_id=uid,
                            from_chat_id=message.chat.id,
                            message_id=message.message_id,
                        )
                        async with counter_lock:
                            sent += 1
                        break
                    except TelegramRetryAfter as err:
                        await asyncio.sleep(float(err.retry_after) + 0.2)
                    except TelegramForbiddenError:
                        async with counter_lock:
                            blocked += 1
                            failed += 1
                        await db("UPDATE users SET active = 0 WHERE user_id = ?", (uid,))
                        break
                    except TelegramBadRequest as err:
                        async with counter_lock:
                            failed += 1
                        if "chat not found" in str(err).lower():
                            await db("UPDATE users SET active = 0 WHERE user_id = ?", (uid,))
                        break
                    except Exception:
                        async with counter_lock:
                            failed += 1
                        log.exception("Broadcast failed for %s", uid)
                        break
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(min(concurrency, total))]

    # Keep UI progress updates lightweight and non-blocking.
    while not queue.empty() or any(not task.done() for task in workers):
        await asyncio.sleep(0.4)
        done = sent + failed
        try:
            await status.edit_text(
                f"📢 Broadcasting…\n\n"
                f"Progress : {min(done, total)}/{total}\n"
                f"Sent     : {sent}\n"
                f"Failed   : {failed}"
            )
        except TelegramBadRequest:
            pass

    await asyncio.gather(*workers, return_exceptions=True)

    await send_text(
        bot,
        message.chat.id,
        "✅ Broadcast finished.\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👥 Total   : {total}\n"
        f"✅ Sent    : {sent}\n"
        f"❌ Failed  : {failed}\n"
        f"🚫 Blocked : {blocked}\n"
        "━━━━━━━━━━━━━━━━━━━━",
        None,
        back_keyboard,
    )



# ── Add New Admin ──────────────────────────────────────────────

@dp.callback_query(F.data == "adm:addadmin")
async def cb_add_admin(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    await state.set_state(AdminSG.add_admin)
    await edit_text(
        callback.message,
        "👤 <b>Add New Admin</b>\n\n"
        "Send the Telegram User ID of the user you want to make an admin.\n\n"
        "Example: <code>123456789</code>",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.add_admin)
async def on_add_admin(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    raw = (message.text or "").strip()
    if not raw.isdigit():
        await message.answer(
            "❌ Invalid User ID.\n\nPlease send a numeric Telegram User ID.",
            reply_markup=cancel_keyboard(),
        )
        return
    user_id = int(raw)
    await db(
        "INSERT OR IGNORE INTO admins (user_id) VALUES (?)",
        (user_id,),
    )
    await state.clear()
    await message.answer(
        f"✅ Admin added successfully.\n\nUser ID: <code>{user_id}</code>",
        reply_markup=back_keyboard(),
    )


# ── Button Settings ───────────────────────────────────────────

@dp.callback_query(F.data == "adm:buttons")
async def cb_button_settings(callback: CallbackQuery) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await callback.answer()
    await edit_text(
        callback.message,
        "🎨 <b>Button Settings</b>\n\n"
        "Choose a button slot to edit.\n\n"
        "Slots 1–4 are the four Join buttons (2 × 2).\n"
        "Slot 5 is the full-width Claim button.",
        None,
        lambda: InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("Slot 1", style="primary", callback_data="adm:btnslot:1"),
                 btn("Slot 2", style="primary", callback_data="adm:btnslot:2")],
                [btn("Slot 3", style="primary", callback_data="adm:btnslot:3"),
                 btn("Slot 4", style="primary", callback_data="adm:btnslot:4")],
                [btn("Slot 5 (Claim)", style="success", callback_data="adm:btnslot:5")],
                [btn("🧹 Clear Button Names", style="danger", callback_data="adm:btnclear")],
                [btn("⬅️ Back", style="primary", callback_data="adm:menu")],
            ]
        ),
    )


@dp.callback_query(F.data == "adm:btnclear")
async def cb_button_clear(callback: CallbackQuery) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    await db("DELETE FROM button_settings")
    await callback.answer("Button names cleared.")
    await edit_text(
        callback.message,
        "🧹 <b>Button names cleared.</b>\n\n"
        "All custom button names have been removed. "
        "The default button names will be used again.",
        None,
        lambda: InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("🎨 Button Settings", style="primary", callback_data="adm:buttons")],
                [btn("⬅️ Back", style="primary", callback_data="adm:menu")],
            ]
        ),
    )


@dp.callback_query(F.data.startswith("adm:btnslot:"))
async def cb_button_slot(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    slot = int(callback.data.split(":")[-1])
    await state.update_data(button_slot=slot)
    await state.set_state(AdminSG.button_name)
    await callback.answer()
    await edit_text(
        callback.message,
        f"✏️ <b>Send the button name for Slot No. {slot}.</b>\n\n"
        "You can send <b>normal text, Premium Custom Emoji, or both together.</b>\n\n"
        "Example:\n"
        "🔥 Join Channel\n"
        "🚀 Continue\n"
        "Claim",
        None,
        cancel_keyboard,
    )


@dp.message(AdminSG.button_name)
async def on_button_name(message: Message, state: FSMContext) -> None:
    if not await is_admin_user(message.from_user.id):
        return
    name = message.text or ""
    entities = message.entities or []
    if not name:
        await message.answer(
            "❌ Please send the button name as text, including any Premium Custom Emoji.",
            reply_markup=cancel_keyboard(),
        )
        return

    data = await state.get_data()
    slot = int(data.get("button_slot", 1))
    await state.update_data(button_name=name, button_entities=dump_entities(entities))
    await state.set_state(AdminSG.button_name)

    await message.answer(
        f"🎨 <b>Select the button colour for Slot No. {slot}:</b>",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [btn("🔴 Red", style="danger", callback_data=f"adm:btncolor:{slot}:red")],
                [btn("🔵 Blue", style="primary", callback_data=f"adm:btncolor:{slot}:blue")],
                [btn("🟢 Green", style="success", callback_data=f"adm:btncolor:{slot}:green")],
                [btn("✖️ Cancel", style="danger", callback_data="adm:cancel")],
            ]
        ),
    )


@dp.callback_query(F.data.startswith("adm:btncolor:"))
async def cb_button_color(callback: CallbackQuery, state: FSMContext) -> None:
    if not await is_admin_user(callback.from_user.id):
        await callback.answer("This action is not available.", show_alert=True)
        return
    _, _, slot_s, color = callback.data.split(":")
    slot = int(slot_s)
    data = await state.get_data()
    name = data.get("button_name")
    raw_entities = data.get("button_entities")
    if not name:
        await callback.answer("Button name is missing. Please try again.", show_alert=True)
        await state.clear()
        return

    entities = load_entities(raw_entities)
    await save_button_setting(slot, name, entities, color)
    await state.clear()
    await callback.answer("Button saved successfully.")
    await edit_text(
        callback.message,
        f"✅ <b>Slot No. {slot} saved.</b>\n\n"
        f"Name: {name}\n"
        f"Colour: {color.title()}\n"
        f"Premium Custom Emoji: {'Yes' if custom_emoji_ids(entities) else 'No'}",
        None,
        back_keyboard,
    )


# ── Deny every admin callback to everyone else ───────────────


@dp.callback_query(F.data.startswith("adm:"))
async def cb_admin_denied(callback: CallbackQuery) -> None:
    if await is_admin_user(callback.from_user.id):
        await callback.answer()
        return
    await callback.answer("This action is not available.", show_alert=True)


@dp.message(StateFilter(None), F.chat.type == "private")
async def fallback(message: Message, bot: Bot) -> None:
    if (message.text or "").startswith("/"):
        return
    row = await register_user(message.from_user)
    await show_start(bot, message.chat.id, row)


# ─────────────────────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────────────────────


async def main() -> None:
    global BOT_USERNAME

    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(link_preview_is_disabled=True),
    )

    me = await bot.get_me()
    BOT_USERNAME = me.username or ""
    log.info("Starting @%s (id=%s)", BOT_USERNAME, me.id)

    await bot.delete_webhook(drop_pending_updates=True)

    try:
        await dp.start_polling(
            bot, allowed_updates=dp.resolve_used_update_types()
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot stopped.")
