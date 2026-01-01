import asyncio
import logging
import sqlite3
import os
import secrets
import uuid
import time
import math
import re

from collections import defaultdict
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    LabeledPrice,
    PreCheckoutQuery,
    ChatPermissions,
)
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramUnauthorizedError,
)

# === НАСТРОЙКИ ===
BOT_TOKEN = "7450306310:AAEbW6K1qikGfY_lMmWDkPZaZszZ_X2a8l0"

# СПИСОК АДМИНОВ (Владельцы бота)
ADMIN_IDS = {1945167560}

# ID ГИФКИ ДЛЯ РУЛЕТКИ
ROULETTE_GIF_ID = "CgACAgQAAxkBAAIBiWlRlvawg03en0bv3rWEEChk0i6sAALaAwACDqNEUUeQWUAmRJ3dNgQ"

DB_FILE = "casino.db"
CURRENCY = "Luxe 💎"

# КАНАЛ ДЛЯ БОНУСА И РЕФЕРАЛОВ
CHANNEL_ID = "@luxe_newsi"
CHANNEL_URL = "https://t.me/luxe_newsi"
CHAT_URL = "https://t.me/luxe_games"
POLICY_URL = "https://teletype.in/@luxetg/LUXE"

STAR_RATE = 2000  # 1 ⭐ = 2000 вашей валюты
MIN_STARS = 1
MAX_STARS = 10000  # безопасный верхний предел для Stars-инвойсов :contentReference[oaicite:1]{index=1}

GAMES = {
    "slots": "🎰 Слоты",
    "dice": "🎲 Кости",
    "mines": "💣 Mines",
    "roulette": "🎡 Рулетка",
    "coin": "🪙 Монетка",
    "blackjack": "🃏 Блэкджек",   # ← ВОТ ОН
}

CLICKER_DAILY_LIMIT = 100
CLICKER_BONUS_CHANCE = 0.03   # 3%
CLICKER_BONUS_MIN = 3
CLICKER_BONUS_MAX = 10

COIN_INACTIVITY_TIMEOUT = 300  # 5 минут (в секундах)


AUDIT_PER_PAGE = 10


# Настройка логов
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Locks to prevent race conditions in games ---
# Use defaultdict(asyncio.Lock) so each key has its own lock lazily.
roulette_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
blackjack_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
mines_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
# coin locks keyed by (challenger_id, target_id) or any stable lock key
coin_locks: defaultdict[object, asyncio.Lock] = defaultdict(asyncio.Lock)


# Инициализация
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# === ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ===
chat_bets: dict[int, list] = {}
game_states: dict[int, str] = {}
last_action_time: dict[int, float] = {}
chat_first_bet_time: dict[int, float] = {}
coin_challenges: dict[str, dict] = {}
active_mines_games: dict[int, dict] = {}
active_blackjack_games: dict[int, dict] = {}

# --- ANTI-SPAM LOCKS (per-user) ---
DICE_IN_PROGRESS: set[int] = set()
SLOTS_IN_PROGRESS: set[int] = set()
DONATE_WAITING_STARS: set[int] = set()


# --- ДИНАМИЧЕСКИЕ КОЭФФИЦИЕНТЫ ВЫПЛАТ (анти-овербет) ---
BJ_PAYOUT_START = 1_000_000  # с какой ставки начинаем снижать коэффициент
BJ_DECAY_PER_DOUBLING = 0.06  # -0.06 за каждое удвоение ставки после 1_000_000
BJ_MIN_FACTOR = 0.60  # ниже этого не опускаем

DICE_PAYOUT_START = 10 ** 18
DICE_DECAY_PER_DOUBLING = 0.0
DICE_MIN_FACTOR = 1.0
DICE_WIN_MULTIPLIER_BASE = 1.6


def payout_factor(bet: int, start: int, decay_per_doubling: float, min_factor: float) -> float:
    if bet < start:
        return 1.0
    scale = math.log(bet / start, 2)
    return max(min_factor, 1.0 - decay_per_doubling * scale)


def bj_payout_factor(bet: int) -> float:
    return payout_factor(bet, BJ_PAYOUT_START, BJ_DECAY_PER_DOUBLING, BJ_MIN_FACTOR)


def dice_payout_factor(bet: int) -> float:
    return payout_factor(bet, DICE_PAYOUT_START, DICE_DECAY_PER_DOUBLING, DICE_MIN_FACTOR)


# === ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ЧИСЕЛ ===
def fmt(num):
    """Форматирует число с пробелами (1 000)"""
    return f"{num:,}".replace(",", " ")

# ===== TOPBOT HELPERS (ставь НИЖЕ CURRENCY и fmt()) =====
TOPBOT_PER_PAGE = 10

def build_topbot_kb(page: int, total_pages: int) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"topbot:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"topbot:{page+1}"))
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def render_topbot_text(rows, page: int, total_pages: int, total_users: int) -> str:
    text = (
        f"🏆 <b>ТОП игроков по балансу</b>\n"
        f"👥 Всего игроков: <b>{total_users}</b>\n"
        f"📄 Страница: <b>{page}/{total_pages}</b>\n\n"
    )
    if not rows:
        return text + "Пока нет игроков."

    start_rank = (page - 1) * TOPBOT_PER_PAGE + 1
    for i, (user_id, name, username, balance) in enumerate(rows, start_rank):
        uname = f"@{username}" if username else "нет"
        safe_name = name or "Unknown"
        text += (
            f"<b>{i}.</b> {safe_name}\n"
            f"🆔 <code>{user_id}</code> | 👤 {uname}\n"
            f"💰 <b>{fmt(balance)} {CURRENCY}</b>\n\n"
        )
    return text

def games_kb(chat_id: int) -> InlineKeyboardMarkup:
    rows = []
    for game_key, title in GAMES.items():
        enabled = is_game_enabled(chat_id, game_key)
        status = "✅" if enabled else "⛔"
        rows.append([InlineKeyboardButton(
            text=f"{status} {title}",
            callback_data=f"gset:{chat_id}:{game_key}"
        )])

    rows.append([
        InlineKeyboardButton(text="⛔ Выключить все", callback_data=f"gall:{chat_id}:0"),
        InlineKeyboardButton(text="✅ Включить все", callback_data=f"gall:{chat_id}:1"),
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# === БАЗА ДАННЫХ ===
def db_start():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            last_bonus REAL DEFAULT 0,
            name TEXT,
            username TEXT,
            prefix TEXT,
            referrer_id INTEGER DEFAULT 0,
            start_bonus_received INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            number INTEGER,
            color TEXT,
            emoji TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_members (
            chat_id INTEGER,
            user_id INTEGER,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bans (
            user_id INTEGER PRIMARY KEY,
            ban_until TEXT,
            reason TEXT,
            admin_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id INTEGER,
            sender_name TEXT,
            target_id INTEGER,
            target_name TEXT,
            amount INTEGER,
            date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS clicker_daily (
        user_id INTEGER PRIMARY KEY,
        day TEXT NOT NULL,
        clicks INTEGER NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_game_settings (
            chat_id INTEGER NOT NULL,
            game_key TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (chat_id, game_key)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action_type TEXT,
            amount INTEGER,
            details TEXT,
            date TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promocodes (
            code TEXT PRIMARY KEY,
            amount INTEGER,
            activations_left INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS promo_activations (
            user_id INTEGER,
            code TEXT,
            activated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, code)
        )
    """)

    # --- ТАБЛИЦЫ ДЛЯ МОДЕРАЦИИ ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_warns (
            chat_id INTEGER,
            user_id INTEGER,
            count INTEGER DEFAULT 0,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_restrictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            user_id INTEGER,
            user_name TEXT,
            type TEXT,
            until_time REAL,
            reason TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # --- ТАБЛИЦА МОДЕРАТОРОВ ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_moderators (
            chat_id INTEGER,
            user_id INTEGER,
            added_by INTEGER,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    # --- ТАБЛИЦА АДМИН ДЕЙСТВИЙ ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            action_type TEXT,
            target_id INTEGER,
            amount INTEGER,
            reason TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()

def migrate_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # --- promo_activations.activated_at ---
    cur.execute("PRAGMA table_info(promo_activations)")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = column name

    if "activated_at" not in cols:
        # 1) Добавляем колонку
        cur.execute("ALTER TABLE promo_activations ADD COLUMN activated_at TEXT")
        # 2) Заполняем тем, что есть сейчас (чтобы MAX(ts) работал)
        cur.execute("UPDATE promo_activations SET activated_at = COALESCE(activated_at, CURRENT_TIMESTAMP)")
        conn.commit()

    conn.close()

def update_user_name(user_id, name, username=None):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    if name is None: name = "Unknown"
    clean_name = name.replace("<", "&lt;").replace(">", "&gt;")
    get_balance(user_id)

    if username:
        username = username.replace("@", "")
        cur.execute("UPDATE users SET name = ?, username = ? WHERE user_id = ?", (clean_name, username, user_id))
    else:
        cur.execute("UPDATE users SET name = ? WHERE user_id = ?", (clean_name, user_id))

    conn.commit()
    conn.close()

def check_user_exists(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
    val = cur.fetchone()
    conn.close()
    return val is not None


def get_balance(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    result = cur.fetchone()
    if result is None:
        cur.execute("INSERT INTO users (user_id, balance) VALUES (?, 0)", (user_id,))
        conn.commit()
        conn.close()
        return 0
    conn.close()
    return result[0]


def get_user_data(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT name, balance, prefix, username FROM users WHERE user_id = ?", (user_id,))
    result = cur.fetchone()
    conn.close()
    if result:
        return {
            'name': result[0],
            'balance': result[1],
            'prefix': result[2],
            'username': result[3]
        }
    return None


def update_balance(user_id, amount):
    # Ensures user row exists, then applies delta.
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO users (user_id, balance) VALUES (?, 0)", (user_id,))
    cur.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()

def is_game_enabled(chat_id: int, game_key: str) -> bool:
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        SELECT enabled FROM chat_game_settings
        WHERE chat_id = ? AND game_key = ?
    """, (chat_id, game_key))
    row = cur.fetchone()
    conn.close()
    return True if row is None else bool(row[0])

def set_game_enabled(chat_id: int, game_key: str, enabled: bool):
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO chat_game_settings(chat_id, game_key, enabled)
        VALUES(?,?,?)
        ON CONFLICT(chat_id, game_key) DO UPDATE SET enabled=excluded.enabled
    """, (chat_id, game_key, 1 if enabled else 0))
    conn.commit()
    conn.close()

def set_all_games(chat_id: int, enabled: bool):
    for game_key in GAMES.keys():
        set_game_enabled(chat_id, game_key, enabled)


def set_prefix(user_id, prefix):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("UPDATE users SET prefix = ? WHERE user_id = ?", (prefix, user_id))
    conn.commit()
    conn.close()


def ban_user_db(user_id, ban_until, reason, admin_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("REPLACE INTO bans (user_id, ban_until, reason, admin_id) VALUES (?, ?, ?, ?)",
                (user_id, ban_until, reason, admin_id))
    conn.commit()
    conn.close()


def unban_user_db(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("DELETE FROM bans WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def get_ban_status(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT ban_until, reason FROM bans WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()

    if not res:
        return None

    ban_until, reason = res

    if ban_until == "forever":
        return "навсегда", reason

    try:
        until_date = datetime.strptime(ban_until, "%d.%m.%Y")
        if datetime.now() > until_date:
            unban_user_db(user_id)
            return None
    except:
        return None

    return ban_until, reason


def track_chat_member(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO chat_members (chat_id, user_id) VALUES (?, ?)", (chat_id, user_id))
        conn.commit()
    except:
        pass
    conn.close()

def get_audit_logs(user_id: int, page: int, per_page: int = 10):
    conn = _db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM audit WHERE user_id = ?", (user_id,))
    total = cur.fetchone()[0] or 0

    offset = (page - 1) * per_page
    cur.execute("""
        SELECT action_type, amount, details, date
        FROM audit
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (user_id, per_page, offset))

    rows = cur.fetchall()
    conn.close()
    return rows, total



def set_referrer(user_id, referrer_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    if res and res[0] == 0 and user_id != referrer_id:
        cur.execute("UPDATE users SET referrer_id = ? WHERE user_id = ?", (referrer_id, user_id))
        conn.commit()
    conn.close()


def get_referrer(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT referrer_id FROM users WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else 0


def get_referrals_count(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else 0


def has_received_start_bonus(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT start_bonus_received FROM users WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] == 1 if res else False


def set_start_bonus_received(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("UPDATE users SET start_bonus_received = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


async def pay_referrer_commission(user_id, loss_amount):
    if loss_amount <= 0: return
    referrer_id = get_referrer(user_id)
    if referrer_id and referrer_id != 0:
        commission = int(loss_amount * 0.03)
        if commission > 0:
            update_balance(referrer_id, commission)


def get_top_players_in_chat(chat_id, limit=10):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT u.name, u.balance, u.prefix 
        FROM users u
        JOIN chat_members cm ON u.user_id = cm.user_id
        WHERE cm.chat_id = ?
        ORDER BY u.balance DESC LIMIT ?
    """, (chat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def create_promo(code, amount, activations):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO promocodes (code, amount, activations_left) VALUES (?, ?, ?)",
                    (code.upper(), amount, activations))
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        conn.close()
        return False


def activate_promo(user_id, code):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    code = code.upper()
    cur.execute("SELECT amount, activations_left FROM promocodes WHERE code = ?", (code,))
    promo = cur.fetchone()
    if not promo:
        conn.close()
        return "not_found", 0
    amount, left = promo
    if left <= 0:
        conn.close()
        return "ended", 0
    cur.execute("SELECT 1 FROM promo_activations WHERE user_id = ? AND code = ?", (user_id, code))
    if cur.fetchone():
        conn.close()
        return "already_used", 0
    cur.execute("UPDATE promocodes SET activations_left = activations_left - 1 WHERE code = ?", (code,))
    cur.execute("INSERT INTO promo_activations (user_id, code) VALUES (?, ?)", (user_id, code))
    cur.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()
    return "success", amount


def get_last_bonus_time(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT last_bonus FROM users WHERE user_id = ?", (user_id,))
    res = cur.fetchone()
    conn.close()
    return res[0] if res and res[0] else 0


def update_bonus_time(user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = time.time()
    cur.execute("UPDATE users SET last_bonus = ? WHERE user_id = ?", (now, user_id))
    conn.commit()
    conn.close()


def add_history(chat_id, number, color, emoji):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT INTO history (chat_id, number, color, emoji) VALUES (?, ?, ?, ?)",
                (chat_id, number, color, emoji))
    cur.execute(
        "DELETE FROM history WHERE id NOT IN (SELECT id FROM history WHERE chat_id = ? ORDER BY id DESC LIMIT 20) AND chat_id = ?",
        (chat_id, chat_id))
    conn.commit()
    conn.close()


def get_history(chat_id, limit=10):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT emoji, number FROM history WHERE chat_id = ? ORDER BY id DESC LIMIT ?", (chat_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def log_audit(user_id, action_type, amount, details=""):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    date = datetime.now().strftime("%d.%m %H:%M:%S")
    cur.execute(
        "INSERT INTO audit (user_id, action_type, amount, details, date) VALUES (?, ?, ?, ?, ?)",
        (user_id, action_type, amount, details, date)
    )
    conn.commit()
    conn.close()


def get_audit_logs(user_id, page=1, per_page=10):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    offset = (page - 1) * per_page
    cur.execute("SELECT COUNT(*) FROM audit WHERE user_id = ?", (user_id,))
    total_count = cur.fetchone()[0]
    cur.execute("""
        SELECT action_type, amount, details, date 
        FROM audit 
        WHERE user_id = ? 
        ORDER BY id DESC LIMIT ? OFFSET ?
    """, (user_id, per_page, offset))
    rows = cur.fetchall()
    conn.close()
    return rows, total_count


def log_transfer(sender_id, sender_name, target_id, target_name, amount):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    date = datetime.now().strftime("%d.%m %H:%M")
    cur.execute(
        "INSERT INTO transfers (sender_id, sender_name, target_id, target_name, amount, date) VALUES (?, ?, ?, ?, ?, ?)",
        (sender_id, sender_name, target_id, target_name, amount, date)
    )
    conn.commit()
    conn.close()


def get_user_transfers(user_id, limit=10):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT sender_id, sender_name, target_id, target_name, amount, date 
        FROM transfers 
        WHERE sender_id = ? OR target_id = ? 
        ORDER BY id DESC LIMIT ?
    """, (user_id, user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def _db():
    return sqlite3.connect(DB_FILE)
from datetime import datetime
import random

# --- CLICKER SETTINGS ---
CLICKER_DAILY_LIMIT = 100
CLICKER_BONUS_CHANCE = 0.03   # 3%
CLICKER_BONUS_MIN = 3
CLICKER_BONUS_MAX = 10


def clicker_today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


def clicker_ensure_table():
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS clicker_daily (
            user_id INTEGER PRIMARY KEY,
            day TEXT NOT NULL,
            clicks INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()


def clicker_reset_if_new_day(user_id: int) -> int:
    """
    Возвращает текущее число кликов за сегодня.
    Если день сменился — сбрасывает на 0.
    """
    clicker_ensure_table()
    today = clicker_today_key()

    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT day, clicks FROM clicker_daily WHERE user_id = ?", (user_id,))
    row = cur.fetchone()

    if not row:
        cur.execute(
            "INSERT INTO clicker_daily(user_id, day, clicks) VALUES(?, ?, 0)",
            (user_id, today)
        )
        conn.commit()
        conn.close()
        return 0

    saved_day, clicks = row
    if saved_day != today:
        cur.execute(
            "UPDATE clicker_daily SET day = ?, clicks = 0 WHERE user_id = ?",
            (today, user_id)
        )
        conn.commit()
        conn.close()
        return 0

    conn.close()
    return int(clicks or 0)


def clicker_add_click(user_id: int) -> tuple[bool, int, int]:
    """
    Делает +1 клик, если лимит не превышен.
    Возвращает: (ok, new_clicks, bonus_amount)
    """
    used = clicker_reset_if_new_day(user_id)
    if used >= CLICKER_DAILY_LIMIT:
        return (False, used, 0)

    new_clicks = used + 1

    conn = _db()
    cur = conn.cursor()
    cur.execute("UPDATE clicker_daily SET clicks = ? WHERE user_id = ?", (new_clicks, user_id))
    conn.commit()
    conn.close()

    bonus = 0
    if random.random() < CLICKER_BONUS_CHANCE:
        bonus = random.randint(CLICKER_BONUS_MIN, CLICKER_BONUS_MAX)

    return (True, new_clicks, bonus)

def get_user_core(user_id: int):
    conn = _db()
    cur = conn.cursor()

    # Получаем список колонок таблицы users
    cur.execute("PRAGMA table_info(users)")
    cols = [r[1] for r in cur.fetchall()]  # r[1] = имя колонки

    # Пытаемся найти колонку даты регистрации/создания
    date_candidates = ["created_at", "date", "created", "reg_date", "joined_at", "join_date", "timestamp"]
    date_col = next((c for c in date_candidates if c in cols), None)

    # Формируем SELECT только по тем колонкам, которые реально существуют
    base_cols = ["user_id", "name", "username", "balance", "prefix", "referrer_id"]
    select_cols = [c for c in base_cols if c in cols]

    if date_col:
        select_cols.append(date_col)

    cur.execute(f"""
        SELECT {", ".join(select_cols)}
        FROM users
        WHERE user_id = ?
    """, (user_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    # Преобразуем в dict по именам колонок
    data = dict(zip(select_cols, row))

    return {
        "user_id": data.get("user_id"),
        "name": data.get("name") or "Unknown",
        "username": data.get("username"),
        "balance": data.get("balance") or 0,
        "prefix": data.get("prefix"),
        "referrer_id": data.get("referrer_id") or 0,
        "created_at": data.get(date_col) if date_col else "—",
    }

def get_user_money_flows(user_id: int):
    """
    Сводка по движениям из audit + transfers + promo_activations + admin_actions
    """
    conn = _db()
    cur = conn.cursor()

    # --- AUDIT ---
    cur.execute("SELECT COUNT(*) FROM audit WHERE user_id = ?", (user_id,))
    audit_count = cur.fetchone()[0] or 0

    cur.execute("SELECT COALESCE(SUM(amount),0) FROM audit WHERE user_id = ? AND amount > 0", (user_id,))
    audit_in = cur.fetchone()[0] or 0

    cur.execute("SELECT COALESCE(SUM(amount),0) FROM audit WHERE user_id = ? AND amount < 0", (user_id,))
    audit_out = cur.fetchone()[0] or 0  # отрицательное число

    # --- PROMO (сколько раз активировал) ---
    cur.execute("SELECT COUNT(*) FROM promo_activations WHERE user_id = ?", (user_id,))
    promo_used_count = cur.fetchone()[0] or 0

    # --- ADMIN ACTIONS: сколько раз админ выдавал этому юзеру и на какую сумму ---
    cur.execute("""
        SELECT COUNT(*), COALESCE(SUM(amount),0)
        FROM admin_actions
        WHERE target_id = ?
    """, (user_id,))
    admin_ops_count, admin_sum = cur.fetchone()
    admin_ops_count = admin_ops_count or 0
    admin_sum = admin_sum or 0

    # --- TRANSFERS: вход/выход ---
    cur.execute("SELECT COALESCE(SUM(amount),0), COUNT(*) FROM transfers WHERE target_id = ?", (user_id,))
    tr_in_sum, tr_in_cnt = cur.fetchone()
    tr_in_sum = tr_in_sum or 0
    tr_in_cnt = tr_in_cnt or 0

    cur.execute("SELECT COALESCE(SUM(amount),0), COUNT(*) FROM transfers WHERE sender_id = ?", (user_id,))
    tr_out_sum, tr_out_cnt = cur.fetchone()
    tr_out_sum = tr_out_sum or 0
    tr_out_cnt = tr_out_cnt or 0

    # --- Последняя активность (по audit / transfers / promo_activations / admin_actions) ---
    cur.execute("""
        SELECT MAX(ts) FROM (
            SELECT created_at as ts FROM audit WHERE user_id = ?
            UNION ALL SELECT created_at as ts FROM transfers WHERE sender_id = ? OR target_id = ?
            UNION ALL SELECT activated_at as ts FROM promo_activations WHERE user_id = ?
            UNION ALL SELECT created_at as ts FROM admin_actions WHERE target_id = ?
        )
    """, (user_id, user_id, user_id, user_id, user_id))
    last_ts = cur.fetchone()[0]

    conn.close()

    return {
        "audit_count": audit_count,
        "audit_in": audit_in,
        "audit_out": audit_out,  # отрицательное
        "promo_used_count": promo_used_count,
        "admin_ops_count": admin_ops_count,
        "admin_sum": admin_sum,
        "tr_in_sum": tr_in_sum,
        "tr_in_cnt": tr_in_cnt,
        "tr_out_sum": tr_out_sum,
        "tr_out_cnt": tr_out_cnt,
        "last_ts": last_ts,
    }

def get_user_top_transfer_partners(user_id: int, limit: int = 5):
    """
    Топ контрагентов по переводам: кто чаще/больше гонял деньги с этим юзером.
    """
    conn = _db()
    cur = conn.cursor()

    # Входящие: кто отправлял этому юзеру
    cur.execute("""
        SELECT sender_id, sender_name, COALESCE(SUM(amount),0) as s, COUNT(*) as c
        FROM transfers
        WHERE target_id = ?
        GROUP BY sender_id, sender_name
        ORDER BY s DESC
        LIMIT ?
    """, (user_id, limit))
    incoming = cur.fetchall()

    # Исходящие: кому отправлял этот юзер
    cur.execute("""
        SELECT target_id, target_name, COALESCE(SUM(amount),0) as s, COUNT(*) as c
        FROM transfers
        WHERE sender_id = ?
        GROUP BY target_id, target_name
        ORDER BY s DESC
        LIMIT ?
    """, (user_id, limit))
    outgoing = cur.fetchall()

    conn.close()
    return incoming, outgoing

def get_user_recent_audit(user_id: int, limit: int = 15):
    conn = _db()
    cur = conn.cursor()
    cur.execute("""
        SELECT action_type, amount, details, date
        FROM audit
        WHERE user_id = ?
        ORDER BY id DESC
        LIMIT ?
    """, (user_id, limit))
    rows = cur.fetchall()
    conn.close()
    return rows

def build_antifraud_flags(core: dict, flows: dict):
    """
    Простые флаги, которые реально помогают ловить дюперов/скамеров.
    (Правила можно усиливать под ваши реалии.)
    """
    flags = []

    # 1) Много админ-начислений
    if flows["admin_sum"] >= 50_000:
        flags.append(f"⚠️ много админ-выдач: {fmt(flows['admin_sum'])}")

    # 2) Подозрительная активность промо
    if flows["promo_used_count"] >= 5:
        flags.append(f"⚠️ много промо-активаций: {flows['promo_used_count']}")

    # 3) Сильный перекос переводов (массово раскидывает)
    if flows["tr_out_cnt"] >= 20 and flows["tr_out_sum"] >= 100_000:
        flags.append(f"⚠️ много исходящих переводов: {flows['tr_out_cnt']} шт / {fmt(flows['tr_out_sum'])}")

    # 4) Слишком большой приток по audit (часто признак дюпа через баги/игры)
    if flows["audit_in"] >= 1_000_000:
        flags.append(f"⚠️ большой приход по играм/аудиту: {fmt(flows['audit_in'])}")

    # 5) Баланс огромный, а переводов/аудита мало — странно (ручные накрутки/дыру ищем)
    if core["balance"] >= 1_000_000 and flows["audit_count"] < 10 and flows["tr_in_cnt"] < 3 and flows["admin_ops_count"] == 0:
        flags.append("⚠️ большой баланс при низкой истории (проверь источник)")

    return flags


def parse_duration(time_str):
    if time_str.lower() in ["навсегда", "forever", "perm"]:
        return "forever"

    unit = time_str[-1].lower()
    value = time_str[:-1]

    if not value.isdigit():
        return None

    value = int(value)

    if unit == 'm' or unit == 'м':
        return timedelta(days=value * 30)
    elif unit == 'h' or unit == 'ч':
        return timedelta(hours=value)
    elif unit == 'd' or unit == 'д':
        return timedelta(days=value)
    elif unit == 'min' or unit == 'мин':
        return timedelta(minutes=value)
    else:
        return None


def add_chat_restriction_db(chat_id, user_id, user_name, r_type, until_ts, reason):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO chat_restrictions (chat_id, user_id, user_name, type, until_time, reason) VALUES (?, ?, ?, ?, ?, ?)",
        (chat_id, user_id, user_name, r_type, until_ts, reason))
    conn.commit()
    conn.close()


def get_warns(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT count FROM chat_warns WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
    res = cur.fetchone()
    conn.close()
    return res[0] if res else 0


def add_warn(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO chat_warns (chat_id, user_id, count) VALUES (?, ?, 0)", (chat_id, user_id))
    cur.execute("UPDATE chat_warns SET count = count + 1 WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))

    cur.execute("SELECT count FROM chat_warns WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
    new_count = cur.fetchone()[0]

    if new_count >= 3:
        cur.execute("UPDATE chat_warns SET count = 0 WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))

    conn.commit()
    conn.close()
    return new_count


def remove_warn(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT count FROM chat_warns WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
    res = cur.fetchone()
    current = res[0] if res else 0

    if current > 0:
        cur.execute("UPDATE chat_warns SET count = count - 1 WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        new_count = current - 1
    else:
        new_count = 0

    conn.commit()
    conn.close()
    return new_count


def add_moderator_db(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO chat_moderators (chat_id, user_id) VALUES (?, ?)", (chat_id, user_id))
        conn.commit()
    except:
        pass
    conn.close()


def is_user_moderator(chat_id, user_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM chat_moderators WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
    res = cur.fetchone()
    conn.close()
    return res is not None


def get_chat_moderators_list(chat_id):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT u.name, u.user_id 
        FROM chat_moderators cm
        JOIN users u ON cm.user_id = u.user_id
        WHERE cm.chat_id = ?
    """, (chat_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


async def check_admin(message: types.Message):
    if message.chat.type == 'private': return False
    return message.from_user.id in ADMIN_IDS


async def check_mod(message: types.Message):
    if message.chat.type == 'private': return False

    if message.from_user.id in ADMIN_IDS: return True

    if is_user_moderator(message.chat.id, message.from_user.id):
        return True

    member = await message.chat.get_member(message.from_user.id)
    return member.status in ['creator', 'administrator']


async def resolve_command_args(message: types.Message, args: list):
    if message.reply_to_message:
        return message.reply_to_message.from_user.id, message.reply_to_message.from_user.full_name, args[1:]

    if not args or len(args) < 2: return None, None, []

    identifier = args[1]
    rest_args = args[2:]

    if identifier.isdigit():
        return int(identifier), f"ID {identifier}", rest_args

    if identifier.startswith("@"):
        username = identifier[1:]
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT user_id, name FROM users WHERE username = ? COLLATE NOCASE", (username,))
        res = cur.fetchone()
        conn.close()

        if res:
            return res[0], res[1], rest_args
        else:
            return None, "Unknown (Not in DB)", rest_args

    return None, None, []


main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="💰 Баланс")],
        [KeyboardButton(text="🖱 Кликер"), KeyboardButton(text="🎁 Бонус")],
        [KeyboardButton(text="🤝 Рефералы"), KeyboardButton(text="💎 Донат")],
        [KeyboardButton(text="💬 Чаты")],
        [KeyboardButton(text="📌 Политика"), KeyboardButton(text="ℹ️ Помощь")],
    ],
    resize_keyboard=True
)

# === АНТИ-ФЛУД (ЗАДЕРЖКА 3 СЕКУНДЫ) ===
def check_flood(user_id):
    if user_id in ADMIN_IDS:
        return False
    now = time.time()
    last = last_action_time.get(user_id, 0)
    if now - last < 3:
        return True
    last_action_time[user_id] = now
    return False


def clicker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🖱 КЛИК (+1 LUXE)", callback_data="clicker:click")],
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="clicker:refresh")],
    ])



# === АНАЛИЗ СТАВОК И КОЭФФИЦИЕНТЫ ===
def get_bet_targets(raw_choice):
    raw = str(raw_choice).lower().strip()

    red_numbers = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
    black_numbers = {2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35}

    if raw in ['к', 'red', 'красное', '🔴']: return list(red_numbers)
    if raw in ['ч', 'black', 'черное', '⚫']: return list(black_numbers)
    if raw in ['з', 'green', 'зеленое', '0', '🟢']: return [0]

    if raw in ['odd', 'нечет', 'одд']:
        return [i for i in range(1, 37) if i % 2 != 0]
    if raw in ['even', 'чет', 'евен']:
        return [i for i in range(1, 37) if i % 2 == 0]

    if "-" in raw:
        try:
            start, end = map(int, raw.split("-"))
            if start < 0 or end > 36 or start > end: return []
            return list(range(start, end + 1))
        except ValueError:
            return []

    if raw.isdigit():
        num = int(raw)
        if 0 <= num <= 36: return [num]

    return []


async def check_subscription(user_id):
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        if member.status in ['creator', 'administrator', 'member']:
            return True
        return False
    except TelegramBadRequest:
        print(f"ОШИБКА: Бот не админ в канале {CHANNEL_ID}!")
        return False
    except Exception as e:
        print(f"Ошибка проверки подписки: {e}")
        return False


# --- ФУНКЦИИ ДЛЯ MINES ---
def calculate_mines_coeff(mines, moves):
    multiplier = 1.0
    total_cells = 25
    safe_cells_left = 25 - mines

    for _ in range(moves):
        multiplier *= (total_cells / safe_cells_left)
        total_cells -= 1
        safe_cells_left -= 1

    return multiplier * 0.85


def generate_mines_keyboard(user_id, game_id, revealed_map, game_over=False, mines_map=None):
    buttons = []
    current_field = mines_map
    if not current_field and user_id in active_mines_games:
        current_field = active_mines_games[user_id]['field']

    for i in range(25):
        btn_text = "🟦"
        callback = f"mine_click:{game_id}:{i}"

        cell_value = current_field[i] if current_field else 0

        if game_over:
            if cell_value == 1:
                btn_text = "💣"
            elif cell_value == 2:
                btn_text = "💎"
            else:
                btn_text = "🔸"
            callback = "ignore"
        else:
            if revealed_map[i]:
                # revealed safe
                btn_text = "💎" if cell_value == 2 else "🔸"

        buttons.append(InlineKeyboardButton(text=btn_text, callback_data=callback))

    rows = [buttons[i:i + 5] for i in range(0, 25, 5)]
    if not game_over:
        rows.append([InlineKeyboardButton(text="💰 ЗАБРАТЬ ДЕНЬГИ", callback_data=f"mine_cashout:{game_id}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# --- ЛОГИКА БЛЭКДЖЕКА (21) ---
def create_deck():
    suits = ['♠️', '♥️', '♦️', '♣️']
    ranks = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
    values = {
        '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, '10': 10,
        'J': 2, 'Q': 3, 'K': 4, 'A': 11
    }
    deck = []
    for suit in suits:
        for rank in ranks:
            deck.append({'rank': rank, 'suit': suit, 'value': values[rank]})
    secrets.SystemRandom().shuffle(deck)
    return deck


def calculate_score(hand):
    score = sum(card['value'] for card in hand)
    aces = sum(1 for card in hand if card['rank'] == 'A')
    while score > 21 and aces:
        score -= 10
        aces -= 1
    return score


def get_hand_text(hand, hide_second=False):
    text = ""
    for i, card in enumerate(hand):
        if hide_second and i == 1:
            text += "[❓] "
        else:
            text += f"[{card['rank']}{card['suit']}] "
    return text


# === ЛОГИКА РУЛЕТКИ (EXECUTE SPIN) ===
ROULETTE_SPIN_TIMEOUT = 90  # сек, защита от вечного "идёт"
roulette_spin_started_at: dict[int, float] = {}

async def execute_spin(chat_id: int):
    async with roulette_locks[chat_id]:

        # если ставок уже нет — выходим и чистим состояние на всякий
        if chat_id not in chat_bets or not chat_bets.get(chat_id):
            game_states.pop(chat_id, None)
            chat_first_bet_time.pop(chat_id, None)
            chat_bets.pop(chat_id, None)
            roulette_spin_started_at.pop(chat_id, None)
            return

        # если почему-то зависло состояние spinning давно — сбрасываем
        if game_states.get(chat_id) == "spinning":
            started = roulette_spin_started_at.get(chat_id, 0)
            if started and (time.time() - started) > ROULETTE_SPIN_TIMEOUT:
                game_states.pop(chat_id, None)
                roulette_spin_started_at.pop(chat_id, None)

        game_states[chat_id] = "spinning"
        roulette_spin_started_at[chat_id] = time.time()

        msg_text = None
        msg_dice = None

        try:
            msg_text = await bot.send_message(
                chat_id,
                "🎰 <b>Ставки сделаны, ставок больше нет!</b>",
                parse_mode="HTML"
            )

            # GIF или дайс
            if ROULETTE_GIF_ID:
                try:
                    msg_dice = await bot.send_animation(chat_id, ROULETTE_GIF_ID)
                except Exception as e:
                    print(f"Ошибка GIF: {e}")
                    msg_dice = await bot.send_dice(chat_id, emoji="🎰")
            else:
                msg_dice = await bot.send_dice(chat_id, emoji="🎰")

            await asyncio.sleep(4)

            # удалить анимации (не критично)
            try:
                if msg_text:
                    await msg_text.delete()
            except Exception:
                pass

            try:
                if msg_dice:
                    await msg_dice.delete()
            except Exception:
                pass

            winning_number = secrets.randbelow(37)
            red_numbers = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

            if winning_number == 0:
                color = "green"
                color_emoji = "🟢"
            elif winning_number in red_numbers:
                color = "red"
                color_emoji = "🔴"
            else:
                color = "black"
                color_emoji = "⚫️"

            bets = chat_bets.get(chat_id, [])
            all_bets_lines = []
            winners_lines = []

            for bet in bets:
                user_id = bet["user_id"]
                amount = bet["amount"]
                targets = bet["targets"]

                is_win = False
                payout = 0

                if targets and (winning_number in targets):
                    is_win = True
                    count = len(targets)
                    multiplier = 36 / count
                    payout = int(amount * multiplier)

                all_bets_lines.append(f"{bet['name']} {fmt(amount)} {CURRENCY} на {bet['raw'].upper()}")

                if is_win:
                    update_balance(user_id, payout)
                    log_audit(user_id, "Roulette WIN", payout, f"Bet: {amount} on {bet['raw']}")
                    winners_lines.append(
                        f"{bet['name']} ставка {fmt(amount)} {CURRENCY} выиграл {fmt(payout)} на {bet['raw'].upper()}"
                    )
                else:
                    # проигрыш — комиссия рефереру
                    await pay_referrer_commission(user_id, amount)

            add_history(chat_id, winning_number, color, color_emoji)

            res_text = f"Рулетка: {winning_number}{color_emoji}\n\n"
            if all_bets_lines:
                res_text += "\n".join(all_bets_lines) + "\n\n"

            if winners_lines:
                res_text += "\n".join(winners_lines)
            else:
                res_text += "😔 Победителей нет."

            await bot.send_message(chat_id, res_text, parse_mode="HTML")

        except Exception as e:
            # главное — НЕ оставлять чат в "spinning" навсегда
            print(f"[ROULETTE] execute_spin error in chat {chat_id}: {e}")
            try:
                await bot.send_message(
                    chat_id,
                    "⚠️ Рулетка прервалась из-за ошибки. Ставки сброшены, можно ставить заново.",
                    parse_mode="HTML"
                )
            except Exception:
                pass

        finally:
            # критично: всегда чистим состояние
            chat_bets.pop(chat_id, None)
            chat_first_bet_time.pop(chat_id, None)
            game_states.pop(chat_id, None)
            roulette_spin_started_at.pop(chat_id, None)

    # === ХЕНДЛЕРЫ ===
# --- ПЕРЕХВАТЧИК ЗАБАНЕННЫХ ---
@dp.message(lambda message: get_ban_status(message.from_user.id) is not None)
async def banned_interceptor(message: types.Message):
    if message.chat.type == 'private':
        ban_until, reason = get_ban_status(message.from_user.id)
        if not reason: reason = "не указана"
        await message.answer(
            f"🚫 <b>Вы заблокированы в боте</b>\n"
            f"📅 До: <b>{ban_until}</b>\n"
            f"❓ Причина: {reason}",
            parse_mode="HTML"
        )
    return


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id

    is_new_user = not check_user_exists(user_id)

    get_balance(user_id)
    update_user_name(user_id, message.from_user.first_name, message.from_user.username)

    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        referrer_id = int(args[1])
        if referrer_id != user_id:
            set_referrer(user_id, referrer_id)

    if message.chat.type == 'private':
        if is_new_user:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📢 Подписаться на Спонсора", url=CHANNEL_URL)],
                [InlineKeyboardButton(text="✅ Проверить подписку",
                                      callback_data="check_sub_start")]
            ])

            await message.answer(
                f"👋 <b>Добро пожаловать в Luxe!</b> 🌟\n\n"
                f"Чтобы получить стартовые 1000 Luxe, подпишись на канал!",
                reply_markup=kb,
                parse_mode="HTML"
            )
        else:
            await message.answer(
                f"👋 <b>С возвращением, {message.from_user.first_name}!</b>",
                reply_markup=main_kb,
                parse_mode="HTML"
            )
    else:
        track_chat_member(message.chat.id, user_id)
        await message.answer("👋 Я работаю! Меню доступно в ЛС.")

@dp.message(Command("clicker"))
@dp.message(F.text == "🖱 Кликер")
async def cmd_clicker(message: types.Message):
    if message.chat.type != "private":
        await message.answer("Кликер работает только в личных сообщениях с ботом.")
        return

    used = clicker_reset_if_new_day(message.from_user.id)
    left = CLICKER_DAILY_LIMIT - used

    await message.answer(
        f"🖱 <b>Кликер</b>\n"
        f"Лимит в сутки: <b>{CLICKER_DAILY_LIMIT}</b>\n"
        f"Осталось сегодня: <b>{left}</b>\n\n"
        f"1 клик = +1 {CURRENCY}\n"
        f"Иногда выпадает 💰 секретный мешок (шанс 3%).",
        reply_markup=clicker_kb(),
        parse_mode="HTML"
    )

@dp.message(Command("nogame"))
async def cmd_nogame(message: types.Message):
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("Эта команда работает только в группах.")
        return

    member = await bot.get_chat_member(message.chat.id, message.from_user.id)
    if member.status not in ("administrator", "creator"):
        await message.answer("Только администратор может менять настройки игр в этом чате.")
        return

    await message.answer(
        "Настройки игр для этого чата:\nНажимайте кнопки, чтобы включать/выключать.",
        reply_markup=games_kb(message.chat.id)
    )


@dp.callback_query(F.data == "check_sub_start")
async def check_sub_start_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if has_received_start_bonus(user_id):
        await callback.answer("Вы уже получили стартовый бонус!", show_alert=True)
        return

    is_sub = await check_subscription(user_id)
    if is_sub:
        set_start_bonus_received(user_id)
        update_balance(user_id, 1000)

        ref_id = get_referrer(user_id)
        if ref_id != 0:
            update_balance(ref_id, 2000)
            try:
                await bot.send_message(ref_id,
                                       f"🤝 Ваш реферал <b>{callback.from_user.first_name}</b> активировал бонус!\nВам начислено: <b>2000 {CURRENCY}</b>",
                                       parse_mode="HTML")
            except:
                pass

        await callback.message.edit_text(f"✅ <b>Бонус 1000 {CURRENCY} получен!</b>\nУдачной игры!", parse_mode="HTML")
    else:
        await callback.answer("❌ Вы не подписаны на канал!", show_alert=True)

@dp.callback_query(F.data.startswith("gset:"))
async def cb_toggle_game(cb: types.CallbackQuery):
    _, chat_id_s, game_key = cb.data.split(":", 2)
    chat_id = int(chat_id_s)

    member = await bot.get_chat_member(chat_id, cb.from_user.id)
    if member.status not in ("administrator", "creator"):
        await cb.answer("Недостаточно прав.", show_alert=True)
        return

    enabled_now = is_game_enabled(chat_id, game_key)
    set_game_enabled(chat_id, game_key, not enabled_now)

    await cb.message.edit_reply_markup(reply_markup=games_kb(chat_id))
    await cb.answer("Готово.")

@dp.callback_query(F.data.startswith("gall:"))
async def cb_toggle_all(cb: types.CallbackQuery):
    _, chat_id_s, enabled_s = cb.data.split(":", 2)
    chat_id = int(chat_id_s)
    enabled = bool(int(enabled_s))

    member = await bot.get_chat_member(chat_id, cb.from_user.id)
    if member.status not in ("administrator", "creator"):
        await cb.answer("Недостаточно прав.", show_alert=True)
        return

    set_all_games(chat_id, enabled)
    await cb.message.edit_reply_markup(reply_markup=games_kb(chat_id))
    await cb.answer("Готово.")

@dp.callback_query(F.data == "clicker:refresh")
async def clicker_refresh(cb: types.CallbackQuery):
    used = clicker_reset_if_new_day(cb.from_user.id)
    left = CLICKER_DAILY_LIMIT - used

    await cb.message.edit_text(
        f"🖱 <b>Кликер</b>\n"
        f"Осталось сегодня: <b>{left}</b>\n\n"
        f"1 клик = +1 {CURRENCY}\n"
        f"Шанс 💰 мешка: 3%",
        reply_markup=clicker_kb(),
        parse_mode="HTML"
    )
    await cb.answer()


@dp.callback_query(F.data == "clicker:click")
async def clicker_click(cb: types.CallbackQuery):
    user_id = cb.from_user.id

    ok, new_clicks, bonus = clicker_add_click(user_id)
    if not ok:
        await cb.answer("Лимит на сегодня исчерпан (100 кликов).", show_alert=True)
        return

    update_balance(user_id, 1)

    text_extra = ""
    if bonus > 0:
        update_balance(user_id, bonus)
        text_extra = (
            f"\n\n💰 <b>Удача на вашей стороне!</b>\n"
            f"Вы нашли секретный мешок и получили <b>+{bonus} {CURRENCY}</b> 🎉"
        )

    left = CLICKER_DAILY_LIMIT - new_clicks
    await cb.message.edit_text(
        f"🖱 <b>Кликер</b>\n"
        f"Клик засчитан: <b>+1 {CURRENCY}</b>\n"
        f"Осталось сегодня: <b>{left}</b>"
        f"{text_extra}",
        reply_markup=clicker_kb(),
        parse_mode="HTML"
    )
    await cb.answer("✅ +1")


# --- ПРОФИЛЬ ---
@dp.message(F.text == "👤 Профиль")
async def cmd_profile(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id
    update_user_name(user_id, message.from_user.first_name, message.from_user.username)

    data = get_user_data(user_id)
    if not data: return
    name = data['name']
    balance = data['balance']
    prefix = data['prefix']

    display_name = name
    if prefix:
        display_name = f"{prefix} {name}"

    referrals = get_referrals_count(user_id)

    text = (
        f"👤 <b>Твой Профиль</b>\n\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"👤 Имя: <b>{display_name}</b>\n"
        f"💎 Баланс: <b>{fmt(balance)} {CURRENCY}</b>\n"
        f"🤝 Рефералов: <b>{referrals}</b>\n"
    )
    await message.answer(text, parse_mode="HTML")


# --- РЕФЕРАЛЫ (СЧЕТЧИК) ---
@dp.message(F.text == "🤝 Рефералы")
async def cmd_referrals(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id
    bot_info = await bot.get_me()

    ref_count = get_referrals_count(user_id)
    link = f"https://t.me/{bot_info.username}?start={user_id}"

    text = (
        f"🤝 <b>Партнерская программа</b>\n\n"
        f"Приглашай друзей и зарабатывай на их игре!\n"
        f"1. Друг получает <b>1000 {CURRENCY}</b> за старт.\n"
        f"2. Ты получаешь <b>2000 {CURRENCY}</b> за каждого.\n"
        f"3. Ты получаешь <b>3%</b> от каждого проигрыша друга вечно!\n\n"
        f"👥 Вы пригласили: <b>{ref_count} чел.</b>\n\n"
        f"🔗 <b>Твоя ссылка:</b>\n<code>{link}</code>"
    )
    await message.answer(text, parse_mode="HTML")


# --- ДОНАТ (STARS) ---
@dp.message(F.text == "💎 Донат")
async def cmd_donate(message: types.Message):
    if check_flood(message.from_user.id):
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Ввести своё количество ⭐", callback_data="donate_custom")],
        [InlineKeyboardButton(text="100,000 💎 — 50 ⭐️", callback_data="donate:100000:50")],
        [InlineKeyboardButton(text="200,000 💎 — 100 ⭐️", callback_data="donate:200000:100")],
        [InlineKeyboardButton(text="500,000 💎 — 250 ⭐️", callback_data="donate:500000:250")],
    ])

    await message.answer(
        f"💎 <b>Пополнение баланса</b>\n\n"
        f"Курс: <b>1 ⭐ = {STAR_RATE} {CURRENCY}</b>\n"
        f"Минимум: <b>{MIN_STARS} ⭐</b>, максимум: <b>{MAX_STARS} ⭐</b>\n\n"
        f"Выберите пакет или введите своё количество ⭐:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@dp.callback_query(F.data == "donate_custom")
async def donate_custom_start(callback: types.CallbackQuery):
    DONATE_WAITING_STARS.add(callback.from_user.id)
    await callback.message.answer(
        "✏️ Введите количество ⭐ (числом)\n\n"
        f"Пример: <code>25</code>\n"
        f"Курс: <b>1 ⭐ = {STAR_RATE} {CURRENCY}</b>\n"
        f"Мин: {MIN_STARS} ⭐, Макс: {MAX_STARS} ⭐",
        parse_mode="HTML"
    )
    await callback.answer()


@dp.message(F.text.regexp(r"^\d+$"))
async def donate_custom_amount(message: types.Message):
    user_id = message.from_user.id

    # ВАЖНО: чтобы не ломать другие места, где люди пишут цифры
    if user_id not in DONATE_WAITING_STARS:
        return

    # Сбрасываем режим ожидания сразу, чтобы не было двойных инвойсов
    DONATE_WAITING_STARS.discard(user_id)

    stars = int(message.text)
    if stars < MIN_STARS or stars > MAX_STARS:
        await message.answer(f"❌ Введите число от {MIN_STARS} до {MAX_STARS} ⭐")
        return

    amount_luxe = stars * STAR_RATE

    prices = [LabeledPrice(label=f"{stars} ⭐ → {fmt(amount_luxe)} {CURRENCY}", amount=stars)]

    await bot.send_invoice(
        chat_id=user_id,
        title="Пополнение баланса",
        description=f"{stars} ⭐ → {fmt(amount_luxe)} {CURRENCY}",
        payload=f"stars_custom:{stars}:{amount_luxe}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="donate_custom"
    )


@dp.callback_query(F.data.startswith("donate:"))
async def donate_invoice(callback: types.CallbackQuery):
    _, amount_luxe, stars_price = callback.data.split(":")
    amount_luxe = int(amount_luxe)
    stars_price = int(stars_price)

    prices = [LabeledPrice(label=f"{amount_luxe} {CURRENCY}", amount=stars_price)]

    await bot.send_invoice(
        chat_id=callback.from_user.id,
        title=f"{amount_luxe} {CURRENCY}",
        description=f"Покупка игровой валюты {amount_luxe} {CURRENCY}",
        payload=f"stars_pack:{stars_price}:{amount_luxe}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="donate_pack"
    )
    await callback.answer()


@dp.pre_checkout_query()
async def process_pre_checkout_query(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)


@dp.message(F.successful_payment)
async def process_successful_payment(message: types.Message):
    payment_info = message.successful_payment
    payload = payment_info.invoice_payload

    # 1) Пакеты
    if payload.startswith("stars_pack:"):
        _, stars_str, amount_str = payload.split(":")
        amount = int(amount_str)

        update_balance(message.from_user.id, amount)
        log_audit(message.from_user.id, "DONATE", amount, f"Stars pack {stars_str}")

        await message.answer(
            f"✅ <b>Оплата прошла успешно!</b>\n"
            f"Вам начислено: <b>{fmt(amount)} {CURRENCY}</b>\n"
            f"Спасибо за поддержку!",
            parse_mode="HTML"
        )
        return

    # 2) Кастомный ввод
    if payload.startswith("stars_custom:"):
        _, stars_str, amount_str = payload.split(":")
        amount = int(amount_str)

        update_balance(message.from_user.id, amount)
        log_audit(message.from_user.id, "DONATE", amount, f"Stars custom {stars_str}")

        await message.answer(
            f"✅ <b>Оплата прошла успешно!</b>\n"
            f"Вы оплатили: <b>{stars_str} ⭐</b>\n"
            f"Вам начислено: <b>{fmt(amount)} {CURRENCY}</b>\n"
            f"Спасибо за поддержку!",
            parse_mode="HTML"
        )
        return

    # (на всякий случай) старый формат, если где-то остался
    if payload.startswith("luxe_pay_"):
        amount = int(payload.split("_")[2])
        update_balance(message.from_user.id, amount)
        log_audit(message.from_user.id, "DONATE", amount, "Stars Payment (legacy)")

        await message.answer(
            f"✅ <b>Оплата прошла успешно!</b>\n"
            f"Вам начислено: <b>{fmt(amount)} {CURRENCY}</b>\n"
            f"Спасибо за поддержку!",
            parse_mode="HTML"
        )


# --- ЧАТЫ ---
@dp.message(F.text == "💬 Чаты")
async def cmd_chats(message: types.Message):
    if check_flood(message.from_user.id): return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐️ Перейти в чат", url=CHAT_URL)]
    ])

    text = (
        f"💬 <b>Официальный чат Luxe</b>\n\n"
        f"Общайся, ищи соперников для игр, делись победами и лови промокоды первым!\n"
        f"👇 Жми кнопку ниже:"
    )
    await message.answer(text, reply_markup=kb, parse_mode="HTML")

# --- ПОЛИТИКА ---
@dp.message(F.text == "📌 Политика")
async def cmd_policy(message: types.Message):
    if check_flood(message.from_user.id):
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Открыть политику", url=POLICY_URL)]
    ])

    await message.answer(
        "📌 <b>Политика</b>\n\nНажмите кнопку ниже, чтобы открыть ссылку:",
        reply_markup=kb,
        parse_mode="HTML"
    )



# --- ПОМОЩЬ (ОБНОВЛЕННЫЙ ГАЙД) ---
@dp.message(F.text.in_({"ℹ️ Помощь", "/help"}))
async def cmd_help(message: types.Message):
    if check_flood(message.from_user.id): return

    text = (
        f"📚 <b>ПОДРОБНЫЙ ГАЙД ПО ИГРАМ</b>\n\n"
        f"🎰 <b>Слоты (Slots)</b>\n"
        f"• Команда: <code>/slots</code> или отправь 🎰\n"
        f"• Ставка: 50 {CURRENCY}\n"
        f"• Выигрыш: Джекпот (x20) или x3 при двух одинаковых.\n\n"

        f"🎲 <b>Кости (Dice)</b>\n"
        f"• Команда: <code>/dice 1000</code> или отправь 🎲 (ставка 50)\n"
        f"• Множитель: x1.8\n"
        f"• Выигрыш: Если твой кубик больше бота.\n\n"

        f"🎱 <b>Рулетка (Roulette)</b>\n"
        f"• Ставки:\n"
        f"  - Цвет: <code>100 к</code> (красное), <code>100 ч</code> (черное)\n"
        f"  - Число: <code>100 5</code>, <code>100 0</code> (зеро)\n"
        f"  - Диапазон: <code>100 1-12</code>\n"
        f"  - Чет/Нечет: <code>100 чет</code>, <code>100 нечет</code>\n"
        f"• Запуск: Напиши <b>ГО</b> или <b>Крути</b> (нужно быть в игре).\n"
        f"• Выплаты: Число (x36), Цвет (x2), Дюжина (x3).\n\n"

        f"🃏 <b>Блэкджек (21)</b>\n"
        f"• Команда: <code>21 100</code> (где 100 - ставка)\n"
        f"• Цель: Набрать 21 или больше дилера, но не перебрать.\n\n"

        f"💣 <b>Мины (Mines)</b>\n"
        f"• Команда: <code>мины 100 3</code> (ставка 100, 3 мины)\n"
        f"• Цель: Открывать клетки и не попасть на мину. Забрать деньги можно в любой момент.\n\n"

        f"🪙 <b>Монетка (PVP)</b>\n"
        f"• Команда: Ответь на сообщение друга <code>.монетка 100</code>\n"
        f"• Описание: Игра 1 на 1. Победитель забирает банк (комиссия 5%).\n\n"

        f"💸 <b>Перевод</b>\n"
        f"• Команда: <code>п 100</code> (ответом на сообщение) или <code>п ID 100</code>."
    )
    await message.answer(text, parse_mode="HTML")


# --- ИГРА БЛЭКДЖЕК ---
@dp.message(F.text.lower().in_({"🃏 21 (блэкджек)", "21", "блекджек"}))
@dp.message(F.text.lower().startswith("21 "))
async def cmd_blackjack(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id
    update_user_name(user_id, message.from_user.first_name, message.from_user.username)

    if message.chat.type in ("group", "supergroup"):
        if not is_game_enabled(message.chat.id, "blackjack"):
            return

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    if user_id in active_blackjack_games:
        await message.answer("⚠️ Вы уже играете! Закончите прошлую игру.")
        return

    text = message.text.lower()
    bet = 50

    args = text.split()
    if len(args) > 1 and args[0] == "21" and args[1].isdigit():
        bet = int(args[1])

    wallet = get_balance(user_id)
    if wallet < bet:
        await message.answer(f"❌ Не хватает денег ({fmt(wallet)}).")
        return

    update_balance(user_id, -bet)

    deck = create_deck()
    player_hand = [deck.pop(), deck.pop()]
    dealer_hand = [deck.pop(), deck.pop()]

    game_id = uuid.uuid4().hex[:8]
    active_blackjack_games[user_id] = {
        'game_id': game_id,
        'deck': deck,
        'player_hand': player_hand,
        'dealer_hand': dealer_hand,
        'bet': bet,
        'msg_id': None,
        'closed': False,
    }

    p_score = calculate_score(player_hand)
    d_score = dealer_hand[0]['value']

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Еще (Hit)", callback_data=f"bj_hit:{game_id}"),
         InlineKeyboardButton(text="🔴 Стоп (Stand)", callback_data=f"bj_stand:{game_id}")]
    ])

    sent = await message.answer(
        f"🃏 <b>Блэкджек (21)</b> | Ставка: {fmt(bet)}\n\n"
        f"👤 <b>Вы ({p_score}):</b>\n{get_hand_text(player_hand)}\n\n"
        f"🤵 <b>Дилер (??):</b>\n{get_hand_text(dealer_hand, hide_second=True)}",
        reply_markup=kb, parse_mode="HTML"
    )
    active_blackjack_games[user_id]['msg_id'] = sent.message_id

    if p_score == 21:
        await blackjack_end(user_id, sent, natural=True)


@dp.callback_query(F.data.startswith("bj_hit:") | F.data.startswith("bj_stand:"))
async def blackjack_action(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    data = callback.data.split(":")
    if len(data) != 2:
        await callback.answer("Некорректная кнопка.", show_alert=True)
        return
    action, game_id = data[0], data[1]

    if user_id not in active_blackjack_games:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    async with blackjack_locks[user_id]:
        if user_id not in active_blackjack_games:
            await callback.answer("Игра не найдена.", show_alert=True)
            return

        game = active_blackjack_games[user_id]
        if game.get('closed'):
            await callback.answer("Игра уже завершена.", show_alert=True)
            return

        if game.get('game_id') != game_id:
            await callback.answer("Эта игра устарела.", show_alert=True)
            return

        if callback.message and game.get('msg_id') and callback.message.message_id != game['msg_id']:
            await callback.answer("Используйте актуальное сообщение игры.", show_alert=True)
            return

        deck = game['deck']

        if action == "bj_hit":
            game['player_hand'].append(deck.pop())
            score = calculate_score(game['player_hand'])

            if score > 21:
                await blackjack_end(user_id, callback.message, bust=True)
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🟢 Еще (Hit)", callback_data=f"bj_hit:{game_id}"),
                     InlineKeyboardButton(text="🔴 Стоп (Stand)", callback_data=f"bj_stand:{game_id}")]
                ])
                try:
                    await callback.message.edit_text(
                    f"""🃏 <b>Блэкджек (21)</b> | Ставка: {fmt(game['bet'])}

👤 <b>Вы ({score}):</b>
{get_hand_text(game['player_hand'])}

🤵 <b>Дилер (??):</b>
{get_hand_text(game['dealer_hand'], hide_second=True)}""",
                    reply_markup=kb, parse_mode="HTML"
                )
                except:
                    pass

        elif action == "bj_stand":
            while calculate_score(game['dealer_hand']) < 17:
                game['dealer_hand'].append(deck.pop())
            await blackjack_end(user_id, callback.message)


async def blackjack_end(user_id, message, bust=False, natural=False):
    if user_id not in active_blackjack_games:
        return

    game = active_blackjack_games[user_id]
    if game.get('closed'):
        return

    # close game BEFORE payouts to prevent double-end
    game['closed'] = True
    del active_blackjack_games[user_id]

    bet = game['bet']
    k = bj_payout_factor(bet)

    p_score = calculate_score(game['player_hand'])
    d_score = calculate_score(game['dealer_hand'])

    result = ""
    win_amount = 0

    if bust:
        result = "❌ <b>Перебор! Вы проиграли.</b>"
        log_audit(user_id, "BJ Loss", -bet)
        await pay_referrer_commission(user_id, bet)
    elif natural:
        win_amount = int(bet * 2.5 * k)
        result = f"🔥 <b>Блэкджек! Победа! (+{fmt(win_amount)})</b>"
        update_balance(user_id, win_amount)
        log_audit(user_id, "BJ Win Natural", win_amount)
    elif d_score > 21:
        win_amount = int(bet * 2 * k)
        result = f"✅ <b>Дилер перебрал ({d_score})! Победа! (+{fmt(win_amount)})</b>"
        update_balance(user_id, win_amount)
        log_audit(user_id, "BJ Win DealerBust", win_amount)
    elif p_score > d_score:
        win_amount = int(bet * 2 * k)
        result = f"✅ <b>Победа! (+{fmt(win_amount)})</b>"
        update_balance(user_id, win_amount)
        log_audit(user_id, "BJ Win", win_amount)
    elif p_score < d_score:
        result = "❌ <b>Дилер победил.</b>"
        log_audit(user_id, "BJ Loss", -bet)
        await pay_referrer_commission(user_id, bet)
    else:
        update_balance(user_id, bet)
        result = "🤝 <b>Ничья (Возврат).</b>"
        log_audit(user_id, "BJ Push", bet)

    try:
        await message.edit_text(
            f"""🃏 <b>Блэкджек (21)</b>

👤 <b>Вы ({p_score}):</b>
{get_hand_text(game['player_hand'])}

🤵 <b>Дилер ({d_score}):</b>
{get_hand_text(game['dealer_hand'])}

{result}""",
            parse_mode="HTML"
        )
    except:
        pass


# --- ИГРА САПЕР (MINES) ---

MINES_INACTIVITY_TIMEOUT = 300  # 5 минут
MINES_INACTIVITY_CHECK_EVERY = 10  # проверка каждые 10 сек


@dp.message(F.text.lower().startswith("мины "))
async def cmd_start_mines(message: types.Message):
    if check_flood(message.from_user.id):
        return
    if message.chat.type in ("group", "supergroup"):
        if not is_game_enabled(message.chat.id, "mines"):
            return

    user_id = message.from_user.id

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    if user_id in active_mines_games:
        await message.answer("⚠️ У вас уже есть активная игра в Mines!")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.answer(
            "⚠️ Формат: <code>мины СТАВКА МИНЫ</code>\nПример: <code>мины 1000 2</code>",
            parse_mode="HTML"
        )
        return

    if not args[1].isdigit() or not args[2].isdigit():
        await message.answer("⚠️ Ставка и количество мин должны быть числами.")
        return

    bet = int(args[1])
    mines_count = int(args[2])

    if bet <= 0:
        await message.answer("⚠️ Ставка должна быть больше 0.")
        return

    if mines_count < 1 or mines_count > 24:
        await message.answer("⚠️ Мин может быть от 1 до 24.")
        return

    wallet = get_balance(user_id)
    if wallet < bet:
        await message.answer(f"❌ Не хватает денег ({fmt(wallet)}).")
        return

    # списываем ставку сразу
    update_balance(user_id, -bet)

    # генерируем поле
    field = [0] * 25
    indices = list(range(25))
    secrets.SystemRandom().shuffle(indices)

    for i in range(mines_count):
        field[indices[i]] = 1  # мина

    safe_indices = indices[mines_count:]
    diamonds_count = max(1, len(safe_indices) // 2)
    for i in range(diamonds_count):
        field[safe_indices[i]] = 2  # алмаз/бонусная клетка (визуально)

    game_id = uuid.uuid4().hex[:8]
    active_mines_games[user_id] = {
        'game_id': game_id,
        'bet': bet,
        'mines': mines_count,
        'field': field,
        'revealed': [False] * 25,
        'steps': 0,
        'msg_id': None,
        'closed': False,
        'last_action': time.time(),
        'chat_id': message.chat.id,
    }

    kb = generate_mines_keyboard(user_id, game_id, [False] * 25)

    try:
        sent = await message.answer(
            f"""💣 <b>Mines</b> | Ставка: {fmt(bet)} | Мины: {mines_count}
Множитель: <b>1.00x</b>
Ищите алмазы 💎 и золото 🔸!""",
            reply_markup=kb,
            parse_mode="HTML"
        )
        active_mines_games[user_id]['msg_id'] = sent.message_id
    except TelegramForbiddenError:
        print(f"Пользователь {user_id} заблокировал бота")


@dp.callback_query(F.data.startswith("mine_click:"))
async def mines_click_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer("Некорректная кнопка.", show_alert=True)
        return

    _, game_id, idx_s = parts

    if user_id not in active_mines_games:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    async with mines_locks[user_id]:
        if user_id not in active_mines_games:
            await callback.answer("Игра не найдена.", show_alert=True)
            return

        game = active_mines_games[user_id]

        if game.get('closed'):
            await callback.answer("Игра уже завершена.", show_alert=True)
            return

        if game.get('game_id') != game_id:
            await callback.answer("Эта игра устарела.", show_alert=True)
            return

        if callback.message and game.get('msg_id') and callback.message.message_id != game['msg_id']:
            await callback.answer("Используйте актуальное сообщение игры.", show_alert=True)
            return

        try:
            index = int(idx_s)
        except ValueError:
            await callback.answer("Некорректная клетка.", show_alert=True)
            return

        if index < 0 or index >= 25:
            await callback.answer("Некорректная клетка.", show_alert=True)
            return

        if game['revealed'][index]:
            await callback.answer("Уже открыто!")
            return

        # фиксируем активность
        game['last_action'] = time.time()

        cell_val = game['field'][index]

        # 💥 мина
        if cell_val == 1:
            game['revealed'][index] = True
            game['closed'] = True

            kb = generate_mines_keyboard(
                user_id,
                game_id,
                game['revealed'],
                game_over=True,
                mines_map=game['field']
            )

            await callback.message.edit_text(
                f"""💥 <b>БАБАХ!</b> Вы попали на мину.
Потеряно: <b>{fmt(game['bet'])} {CURRENCY}</b>""",
                reply_markup=kb,
                parse_mode="HTML"
            )

            log_audit(user_id, "Mines LOSS", -game['bet'], f"Mines: {game['mines']}")
            await pay_referrer_commission(user_id, game['bet'])

            del active_mines_games[user_id]
            return

        # ✅ безопасная клетка
        game['revealed'][index] = True
        game['steps'] += 1

        coeff = calculate_mines_coeff(game['mines'], game['steps'])
        current_win = int(game['bet'] * coeff)

        kb = generate_mines_keyboard(user_id, game_id, game['revealed'])

        await callback.message.edit_text(
            f"""💣 <b>Mines</b> | Ставка: {fmt(game['bet'])}
📈 Множитель: <b>{coeff:.2f}x</b>
💰 Выигрыш сейчас: <b>{fmt(current_win)} {CURRENCY}</b>""",
            reply_markup=kb,
            parse_mode="HTML"
        )


@dp.callback_query(F.data.startswith("mine_cashout:"))
async def mines_cashout_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    parts = callback.data.split(":")
    if len(parts) != 2:
        await callback.answer("Некорректная кнопка.", show_alert=True)
        return

    _, game_id = parts

    if user_id not in active_mines_games:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    async with mines_locks[user_id]:
        if user_id not in active_mines_games:
            await callback.answer("Игра не найдена.", show_alert=True)
            return

        game = active_mines_games[user_id]

        if game.get('closed'):
            await callback.answer("Игра уже завершена.", show_alert=True)
            return

        if game.get('game_id') != game_id:
            await callback.answer("Эта игра устарела.", show_alert=True)
            return

        if callback.message and game.get('msg_id') and callback.message.message_id != game['msg_id']:
            await callback.answer("Используйте актуальное сообщение игры.", show_alert=True)
            return

        # фиксируем активность
        game['last_action'] = time.time()

        if game['steps'] == 0:
            await callback.answer("Сначала открой хотя бы одну клетку!", show_alert=True)
            return

        # закрываем игру ДО выплаты (защита от double cashout)
        game['closed'] = True
        del active_mines_games[user_id]

        coeff = calculate_mines_coeff(game['mines'], game['steps'])
        win_amount = int(game['bet'] * coeff)

        update_balance(user_id, win_amount)
        log_audit(user_id, "Mines WIN", win_amount, f"Mines: {game['mines']} Steps: {game['steps']}")

        kb = generate_mines_keyboard(
            user_id,
            game_id,
            game['revealed'],
            game_over=True,
            mines_map=game['field']
        )

        await callback.message.edit_text(
            f"""💰 <b>Вы забрали выигрыш!</b>
📈 Множитель: <b>{coeff:.2f}x</b>
🏆 Получено: <b>{fmt(win_amount)} {CURRENCY}</b>""",
            reply_markup=kb,
            parse_mode="HTML"
        )

        await callback.answer()
        return


async def mines_inactivity_worker(bot: Bot):
    """
    Закрывает игру Mines по неактивности.
    Если 5 минут не было действий и steps == 0 — возвращает ставку.
    """
    while True:
        await asyncio.sleep(MINES_INACTIVITY_CHECK_EVERY)
        now = time.time()

        for user_id, game in list(active_mines_games.items()):
            try:
                last_action = game.get("last_action", now)
                if now - last_action < MINES_INACTIVITY_TIMEOUT:
                    continue

                async with mines_locks[user_id]:
                    if user_id not in active_mines_games:
                        continue

                    game = active_mines_games[user_id]
                    if game.get("closed"):
                        del active_mines_games[user_id]
                        continue

                    # ✅ Возврат ставки только если не было ходов
                    if game.get("steps", 0) == 0:
                        bet = int(game.get("bet", 0))
                        chat_id = game.get("chat_id")
                        msg_id = game.get("msg_id")
                        gid = game.get("game_id")

                        game["closed"] = True
                        del active_mines_games[user_id]

                        if bet > 0:
                            update_balance(user_id, bet)
                            log_audit(user_id, "Mines REFUND", bet, "Timeout 5m inactivity (no moves)")

                        # попытка обновить сообщение игры
                        if chat_id and msg_id:
                            try:
                                await bot.edit_message_text(
                                    chat_id=chat_id,
                                    message_id=msg_id,
                                    text=(
                                        f"⏳ <b>Mines</b>\n\n"
                                        f"Игра закрыта по неактивности (5 минут).\n"
                                        f"💸 Ставка возвращена: <b>{fmt(bet)} {CURRENCY}</b>"
                                    ),
                                    parse_mode="HTML"
                                )
                            except Exception:
                                pass
                    else:
                        # Если шаги были — по умолчанию просто закрываем без возврата,
                        # чтобы не было абуза. Если хочешь авто-кэшаут — скажи, добавлю.
                        game["closed"] = True
                        del active_mines_games[user_id]

            except Exception:
                continue

async def coin_timeout_watcher(challenge_id: str):
    await asyncio.sleep(COIN_INACTIVITY_TIMEOUT)

    lock_key = f"coin:{challenge_id}"
    async with coin_locks[lock_key]:
        game = coin_challenges.get(challenge_id)
        if not game:
            return

        # если уже закончена — ничего
        if game.get("state") == "finished":
            coin_challenges.pop(challenge_id, None)
            return

        last_action = game.get("last_action", 0)
        if last_action and (time.time() - last_action) < COIN_INACTIVITY_TIMEOUT:
            return  # была активность — не закрываем

        # закрываем по таймауту
        creator_id = game["creator_id"]
        target_id = game["target_id"]
        amount = int(game["amount"])
        staked = bool(game.get("staked", False))

        # ✅ Возврат ставок ТОЛЬКО если они уже были списаны (после accept)
        if staked and amount > 0:
            update_balance(creator_id, amount)
            update_balance(target_id, amount)
            log_audit(creator_id, "Coin REFUND", amount, "Timeout 5m inactivity")
            log_audit(target_id, "Coin REFUND", amount, "Timeout 5m inactivity")

        # попытка обновить сообщение игры (если сохранили ids)
        chat_id = game.get("chat_id")
        msg_id = game.get("msg_id")
        if chat_id and msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=("⏳ <b>Монетка</b>\n\n"
                          "Игра закрыта по неактивности (5 минут).\n"
                          + ("💸 Ставки возвращены." if staked else "❌ Предложение устарело.")),
                    parse_mode="HTML"
                )
            except Exception:
                pass
        else:
            # если не можем редактировать — просто сообщим в чат, если он есть
            if chat_id:
                try:
                    await bot.send_message(
                        chat_id,
                        "⏳ Монетка закрыта по неактивности (5 минут)."
                    )
                except Exception:
                    pass

        coin_challenges.pop(challenge_id, None)


async def coin_timeout_watcher(challenge_id: str):
    """
    Закрывает монетку по неактивности.
    Если ставки уже списаны (staked=True) — возвращает обоим.
    Таймер "самопереназначается" при активности через last_action.
    """
    lock_key = f"coin:{challenge_id}"

    while True:
        await asyncio.sleep(COIN_INACTIVITY_TIMEOUT)

        async with coin_locks[lock_key]:
            game = coin_challenges.get(challenge_id)
            if not game:
                return

            if game.get("state") == "finished":
                coin_challenges.pop(challenge_id, None)
                return

            last_action = float(game.get("last_action", time.time()))
            now = time.time()

            # Если была активность недавно — продолжим ждать (таймер "сдвигается")
            if now - last_action < COIN_INACTIVITY_TIMEOUT:
                continue

            # --- Таймаут: закрываем и делаем возврат, если ставки уже списаны ---
            creator_id = game["creator_id"]
            target_id = game["target_id"]
            amount = int(game["amount"])

            staked = bool(game.get("staked", False))
            chat_id = game.get("chat_id")
            msg_id = game.get("msg_id")

            game["state"] = "finished"
            coin_challenges.pop(challenge_id, None)

            if staked and amount > 0:
                update_balance(creator_id, amount)
                update_balance(target_id, amount)
                log_audit(creator_id, "Coin REFUND", amount, "Timeout 5m inactivity")
                log_audit(target_id, "Coin REFUND", amount, "Timeout 5m inactivity")

            # Сообщение в чат (редактируем, если есть msg_id, иначе — отправляем новое)
            text = (
                "⏳ <b>Монетка</b>\n\n"
                "Игра закрыта по неактивности (5 минут).\n"
                + ("💸 Ставки возвращены." if staked else "❌ Предложение устарело.")
            )

            try:
                if chat_id and msg_id:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=msg_id,
                        text=text,
                        parse_mode="HTML"
                    )
                elif chat_id:
                    await bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception:
                pass

            return


# --- ИГРА МОНЕТКА (ПЕРЕПИСАННАЯ) ---
@dp.message(F.text.lower().startswith(".монетка "))
async def cmd_coin_challenge(message: types.Message):
    if check_flood(message.from_user.id):
        return

    if message.chat.type in ("group", "supergroup"):
        if not is_game_enabled(message.chat.id, "coin"):
            return

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, message.from_user.id)

    if not message.reply_to_message:
        await message.answer("⚠️ Эту команду нужно писать в ответ на сообщение игрока!")
        return

    user_id = message.from_user.id
    target_id = message.reply_to_message.from_user.id

    if user_id == target_id:
        await message.answer("⚠️ С самим собой играть нельзя.")
        return

    if message.reply_to_message.from_user.is_bot:
        await message.answer("⚠️ С ботом играть нельзя.")
        return

    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        return

    amount = int(args[1])
    if amount <= 0:
        return

    user_bal = get_balance(user_id)
    if user_bal < amount:
        await message.answer(f"❌ Не хватает денег у вас ({fmt(user_bal)}).")
        return

    target_bal = get_balance(target_id)
    if target_bal < amount:
        await message.answer(f"❌ Не хватает денег у противника ({fmt(target_bal)}).")
        return

    challenge_id = f"{user_id}_{target_id}_{int(time.time())}"

    coin_challenges[challenge_id] = {
        'creator_id': user_id,
        'creator_name': message.from_user.first_name,
        'target_id': target_id,
        'target_name': message.reply_to_message.from_user.first_name,
        'amount': amount,
        'state': 'pending',
        'chooser_id': None,

        # ✅ Новое для тайм-аута/возврата
        'last_action': time.time(),
        'staked': False,               # ставки ещё НЕ списаны
        'chat_id': message.chat.id,
        'msg_id': None
    }

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Принять", callback_data=f"coin_action:accept:{challenge_id}"),
            InlineKeyboardButton(text="❌ Отклонить", callback_data=f"coin_action:decline:{challenge_id}")
        ]
    ])

    sent = await message.answer(
        f"🪙 <b>Монетка</b>\n\n"
        f"Игрок <b>{message.from_user.first_name}</b> предложил <b>{message.reply_to_message.from_user.first_name}</b> сыграть в монетку!\n"
        f"💰 Ставка: <b>{fmt(amount)} {CURRENCY}</b>",
        reply_markup=kb,
        parse_mode="HTML"
    )

    # ✅ сохраняем message_id для дальнейшего edit
    coin_challenges[challenge_id]["msg_id"] = getattr(sent, "message_id", None)

    # ✅ стартуем таймер авто-закрытия (один раз на игру)
    asyncio.create_task(coin_timeout_watcher(challenge_id))


@dp.callback_query(F.data.startswith("coin_action:"))
async def coin_action_handler(callback: types.CallbackQuery):
    _, action, challenge_id = callback.data.split(":")

    lock_key = f"coin:{challenge_id}"
    async with coin_locks[lock_key]:
        if challenge_id not in coin_challenges:
            await callback.answer("Игра не найдена или устарела.", show_alert=True)
            return

        game = coin_challenges[challenge_id]
        user_id = callback.from_user.id

        # ✅ фиксируем активность
        game["last_action"] = time.time()

        if user_id != game['target_id']:
            await callback.answer("Это предложение не для вас!", show_alert=True)
            return

        if game.get('state') in ('finished',):
            await callback.answer("Игра уже завершена.", show_alert=True)
            return

        if action == "decline":
            game['state'] = 'finished'
            # ставки ещё не списаны => возврат не нужен
            await callback.message.edit_text("❌ Игрок отказался от игры.", parse_mode="HTML")
            del coin_challenges[challenge_id]
            return

        if action == "accept":
            if game.get('state') != 'pending':
                await callback.answer("Уже принято.", show_alert=True)
                return

            # Mark accepted BEFORE any balance ops to prevent double-accept
            game['state'] = 'accepted'

            # Balance checks + списание
            if get_balance(game['creator_id']) < game['amount']:
                await callback.message.edit_text("❌ У создателя недостаточно средств.", parse_mode="HTML")
                del coin_challenges[challenge_id]
                return
            if get_balance(game['target_id']) < game['amount']:
                await callback.message.edit_text("❌ У второго игрока недостаточно средств.", parse_mode="HTML")
                del coin_challenges[challenge_id]
                return

            update_balance(game['creator_id'], -game['amount'])
            update_balance(game['target_id'], -game['amount'])
            log_audit(game['creator_id'], "Coin PVP Stake", -game['amount'], f"vs {game['target_id']}")
            log_audit(game['target_id'], "Coin PVP Stake", -game['amount'], f"vs {game['creator_id']}")

            # ✅ ставки списаны — теперь при тайм-ауте делаем возврат
            game["staked"] = True
            game["last_action"] = time.time()

            # Decide chooser
            chooser_id = secrets.choice([game['creator_id'], game['target_id']])
            game['chooser_id'] = chooser_id
            game['state'] = 'choosing'

            chooser_name = game['creator_name'] if chooser_id == game['creator_id'] else game['target_name']

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="🦅 Орёл", callback_data=f"coin_pick:eagle:{challenge_id}"),
                    InlineKeyboardButton(text="🪙 Решка", callback_data=f"coin_pick:tails:{challenge_id}")
                ],
                [InlineKeyboardButton(text="🎲 Автовыбор", callback_data=f"coin_pick:random:{challenge_id}")]
            ])

            await callback.message.edit_text(
                f"""🪙 <b>Игра началась!</b>
Банк: <b>{fmt(game['amount'] * 2)} {CURRENCY}</b>

👉 <b>{chooser_name}</b>, выберите сторону:""",
                reply_markup=kb,
                parse_mode="HTML"
            )


@dp.callback_query(F.data.startswith("coin_pick:"))
async def coin_pick_handler(callback: types.CallbackQuery):
    _, pick, challenge_id = callback.data.split(":")

    lock_key = f"coin:{challenge_id}"
    async with coin_locks[lock_key]:
        if challenge_id not in coin_challenges:
            await callback.answer("Игра завершена.", show_alert=True)
            return

        game = coin_challenges[challenge_id]

        # ✅ фиксируем активность
        game["last_action"] = time.time()

        if game.get('state') != 'choosing':
            await callback.answer("Сейчас нельзя выбирать.", show_alert=True)
            return

        if callback.from_user.id != game['chooser_id']:
            await callback.answer("Сейчас выбирает другой игрок!", show_alert=True)
            return

        result_side = secrets.choice(['eagle', 'tails'])
        if pick == "random":
            pick = secrets.choice(['eagle', 'tails'])

        winner_id = game['creator_id'] if pick == result_side else game['target_id']
        loser_id = game['target_id'] if winner_id == game['creator_id'] else game['creator_id']

        winner_name = game['creator_name'] if winner_id == game['creator_id'] else game['target_name']
        win_amount = game['amount'] * 2

        # Finish BEFORE payout to prevent double
        game['state'] = 'finished'
        del coin_challenges[challenge_id]

        update_balance(winner_id, win_amount)
        log_audit(winner_id, "Coin PVP Win", win_amount, f"vs {loser_id}")
        await pay_referrer_commission(loser_id, game['amount'])

        result_text = "🦅 ОРЁЛ" if result_side == 'eagle' else "🪙 РЕШКА"

        await callback.message.edit_text(
            f"""🪙 <b>Монетка брошена...</b>
Выпало: <b>{result_text}</b>

🏆 Победитель: <b>{winner_name}</b>
💰 Выигрыш: <b>{fmt(win_amount)} {CURRENCY}</b>""",
            parse_mode="HTML"
        )



# --- ТОП ИГРОКОВ (ТОЛЬКО ДЛЯ ЧАТА) ---
@dp.message(F.text.lower().in_({"топ", "top", "/top", "🏆 топ игроков"}))
async def cmd_top(message: types.Message):
    if check_flood(message.from_user.id): return

    if message.chat.type == 'private':
        await message.answer("🚫 Топ доступен только в групповых чатах!")
        return

    track_chat_member(message.chat.id, message.from_user.id)

    rows = get_top_players_in_chat(message.chat.id, 10)

    if not rows:
        await message.answer("📊 В этом чате пока пусто. Играйте, чтобы попасть в топ!")
        return

    text = f"🏆 <b>ТОП-10 ИГРОКОВ ЧАТА:</b>\n\n"

    for idx, (name, balance, prefix) in enumerate(rows, 1):
        if name is None: name = "Неизвестный"
        display_name = name
        if prefix:
            display_name = f"{prefix} {name}"

        medal = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."
        text += f"{medal} <b>{display_name}</b> — {fmt(balance)} {CURRENCY}\n"

    await message.answer(text, parse_mode="HTML")


# --- КОМАНДЫ ДЛЯ НАЗНАЧЕНИЯ МОДЕРАТОРОВ ---
@dp.message(F.text.lower().startswith(".назначить модером"))
async def cmd_appoint_mod(message: types.Message):
    if not await check_admin(message):
        return

    if not message.reply_to_message:
        await message.answer("⚠️ Эту команду нужно писать в ответ на сообщение будущего модератора.")
        return

    target_user = message.reply_to_message.from_user
    if target_user.is_bot:
        await message.answer("⚠️ Бота нельзя назначить.")
        return

    add_moderator_db(message.chat.id, target_user.id)
    update_user_name(target_user.id, target_user.first_name, target_user.username)

    await message.answer(
        f"✅ Пользователь <b>{target_user.first_name}</b> назначен модератором в этом чате!\n"
        f"Теперь ему доступны команды: /mute, /warn, /banchat и т.д.",
        parse_mode="HTML"
    )


@dp.message(F.text.lower() == "кто модер")
async def cmd_who_is_mod(message: types.Message):
    if check_flood(message.from_user.id): return
    if message.chat.type == 'private': return

    rows = get_chat_moderators_list(message.chat.id)

    if not rows:
        await message.answer("🤷‍♂️ В этом чате нет назначенных ботом модераторов.")
        return

    text = "👮‍♂️ <b>Модераторы чата:</b>\n\n"
    for name, uid in rows:
        text += f"• <b>{name}</b> (<code>{uid}</code>)\n"

    await message.answer(text, parse_mode="HTML")


# --- МОДЕРАЦИЯ (MUTE, WARN, BAN) ---
@dp.message(Command("mute"))
async def cmd_mute(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, rest_args = await resolve_command_args(message, args)

    if not target_id:
        await message.answer("⚠️ Формат: `/mute [ID/Reply/@user] [Время] [Причина]`\nПример: `/mute @durov 1h спам`",
                             parse_mode="Markdown")
        return

    if not rest_args:
        await message.answer("⚠️ Укажите время (1h, 1d, навсегда).")
        return

    time_str = rest_args[0]
    reason = " ".join(rest_args[1:]) if len(rest_args) > 1 else "не указана"

    duration = parse_duration(time_str)
    if not duration:
        await message.answer("⚠️ Неверный формат времени. Используйте: 1h (час), 1d (день), 30m (минут), навсегда.")
        return

    if duration == "forever":
        until_date = datetime.now() + timedelta(days=36500)
        readable_time = "навсегда"
    else:
        until_date = datetime.now() + duration
        readable_time = f"до {until_date.strftime('%d.%m.%Y %H:%M')}"

    permissions = ChatPermissions(can_send_messages=False)

    try:
        await bot.restrict_chat_member(message.chat.id, target_id, permissions, until_date=until_date)
        add_chat_restriction_db(message.chat.id, target_id, target_name, "mute", until_date.timestamp(), reason)
        await message.answer(
            f"🔇 <b>Мут</b> для пользователя {target_name}\n⏳ Срок: {readable_time}\n❓ Причина: {reason}",
            parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}\n(Проверьте права бота)")


@dp.message(Command("unmute"))
async def cmd_unmute(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, _ = await resolve_command_args(message, args)

    if not target_id:
        await message.answer("⚠️ Формат: `/unmute [ID/Reply/@user]`", parse_mode="Markdown")
        return

    permissions = ChatPermissions(
        can_send_messages=True,
        can_send_media_messages=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
        can_invite_users=True,
        can_change_info=False,
        can_pin_messages=False
    )

    try:
        await bot.restrict_chat_member(message.chat.id, target_id, permissions)
        await message.answer(f"🔊 <b>Мут снят</b> с пользователя {target_name}", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("warn"))
async def cmd_warn(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, rest_args = await resolve_command_args(message, args)

    if not target_id:
        await message.answer(
            "⚠️ Формат: `/warn [ID/Reply/@user] [Время бана] [Причина]`\nПример: `/warn @user 1d нарушение`",
            parse_mode="Markdown")
        return

    if not rest_args:
        await message.answer("⚠️ Укажите время бана за 3-й варн (например, 1d).")
        return

    ban_time_str = rest_args[0]
    reason = " ".join(rest_args[1:]) if len(rest_args) > 1 else "не указана"

    current_warns = add_warn(message.chat.id, target_id)
    msg_text = f"⚠️ <b>Варн ({current_warns}/3)</b> пользователю {target_name}\n❓ Причина: {reason}"

    if current_warns >= 3:
        duration = parse_duration(ban_time_str)
        if not duration: duration = timedelta(days=1)

        if duration == "forever":
            until_date = datetime.now() + timedelta(days=36500)
            readable_time = "навсегда"
        else:
            until_date = datetime.now() + duration
            readable_time = f"до {until_date.strftime('%d.%м %H:%M')}"

        try:
            await bot.ban_chat_member(message.chat.id, target_id, until_date=until_date)
            add_chat_restriction_db(message.chat.id, target_id, target_name, "ban", until_date.timestamp(),
                                    "3/3 Warns: " + reason)
            msg_text += f"\n\n⛔ <b>Достигнут лимит варнов! БАН</b>\n⏳ Срок: {readable_time}"
        except Exception as e:
            msg_text += f"\n❌ Не удалось забанить: {e}"

    await message.answer(msg_text, parse_mode="HTML")


@dp.message(Command("unwarn"))
async def cmd_unwarn(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, _ = await resolve_command_args(message, args)

    if not target_id:
        await message.answer("⚠️ Формат: `/unwarn [ID/Reply/@user]`", parse_mode="Markdown")
        return

    new_count = remove_warn(message.chat.id, target_id)
    await message.answer(f"✅ <b>Варн снят</b>. У пользователя {target_name} осталось: <b>{new_count}/3</b>",
                         parse_mode="HTML")


@dp.message(F.text.lower().in_({".варны", ".warns"}))
async def cmd_my_warns(message: types.Message):
    if message.chat.type == 'private': return

    args = message.text.split()
    if len(args) > 1 and await check_mod(message):
        target_id, target_name, _ = await resolve_command_args(message, args)
        if target_id:
            count = get_warns(message.chat.id, target_id)
            await message.answer(f"⚠️ У пользователя <b>{target_name}</b>: <b>{count}/3</b> варнов.", parse_mode="HTML")
            return

    target_id = message.from_user.id
    name = message.from_user.first_name

    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        name = message.reply_to_message.from_user.first_name

    count = get_warns(message.chat.id, target_id)
    await message.answer(f"⚠️ У пользователя <b>{name}</b>: <b>{count}/3</b> варнов.", parse_mode="HTML")


@dp.message(Command("banchat"))
async def cmd_banchat(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, rest_args = await resolve_command_args(message, args)

    if not target_id:
        await message.answer("⚠️ Формат: `/banchat [ID/Reply/@user] [Причина]`", parse_mode="Markdown")
        return

    reason = " ".join(rest_args) if rest_args else "не указана"

    try:
        await bot.ban_chat_member(message.chat.id, target_id)
        add_chat_restriction_db(message.chat.id, target_id, target_name, "ban", 0, reason)
        await message.answer(f"⛔ <b>БАН в чате</b> для {target_name}\n❓ Причина: {reason}", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("unbanchat"))
async def cmd_unbanchat(message: types.Message):
    if not await check_mod(message): return

    args = message.text.split()
    target_id, target_name, _ = await resolve_command_args(message, args)

    if not target_id:
        await message.answer("⚠️ Формат: `/unbanchat [ID/Reply/@user]`", parse_mode="Markdown")
        return

    try:
        await bot.unban_chat_member(message.chat.id, target_id)
        await message.answer(f"✅ <b>Разбан в чате</b> для {target_name}", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


@dp.message(Command("banlist"))
async def cmd_banlist(message: types.Message):
    if not await check_mod(message): return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = time.time()
    cur.execute(
        "SELECT user_name, reason FROM chat_restrictions WHERE chat_id = ? AND type = 'ban' AND (until_time = 0 OR until_time > ?) ORDER BY id DESC LIMIT 20",
        (message.chat.id, now))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("📜 Список банов пуст.")
        return

    text = "⛔ <b>Бан-лист чата:</b>\n"
    for name, reason in rows:
        text += f"• <b>{name}</b>: {reason}\n"
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("mutelist"))
async def cmd_mutelist(message: types.Message):
    if not await check_mod(message): return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    now = time.time()
    cur.execute(
        "SELECT user_name, until_time, reason FROM chat_restrictions WHERE chat_id = ? AND type = 'mute' AND until_time > ? ORDER BY id DESC LIMIT 20",
        (message.chat.id, now))
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await message.answer("📜 Список мутов пуст.")
        return

    text = "🔇 <b>Мут-лист чата:</b>\n"
    for name, until, reason in rows:
        date_str = datetime.fromtimestamp(until).strftime('%d.%m %H:%M')
        text += f"• <b>{name}</b> (до {date_str}): {reason}\n"
    await message.answer(text, parse_mode="HTML")

# --- УДАЛЕНИЕ СООБЩЕНИЯ МОДЕРОМ (reply: -sms) ---
@dp.message(F.text.lower().in_({"-sms", "–sms", "—sms"}))
async def cmd_delete_sms(message: types.Message):
        # только в чатах
        if message.chat.type == "private":
            return

        # права: модеры/админы (твоя функция)
        if not await check_mod(message):
            return

        # команда должна быть ответом
        if not message.reply_to_message:
            await message.answer("⚠️ Использование: ответь на сообщение командой <code>-sms</code>", parse_mode="HTML")
            return

        try:
            # удалить сообщение, на которое ответили
            await bot.delete_message(chat_id=message.chat.id, message_id=message.reply_to_message.message_id)

            # по желанию: удалить и саму команду модератора
            try:
                await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
            except:
                pass

        except TelegramBadRequest as e:
            # обычно: нет прав / сообщение уже удалено / нельзя удалить сервисные
            await message.answer(f"❌ Не удалось удалить. Проверь права бота (Delete messages).\n<code>{e}</code>",
                                 parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Ошибка удаления: <code>{e}</code>", parse_mode="HTML")


async def show_audit_page(message_or_call, user_id: int, page: int):
    is_callback = isinstance(message_or_call, types.CallbackQuery)
    message = message_or_call.message if is_callback else message_or_call

    if page < 1:
        page = 1

    rows, total_count = get_audit_logs(user_id, page, AUDIT_PER_PAGE)
    total_pages = max(1, math.ceil(total_count / AUDIT_PER_PAGE))

    if page > total_pages:
        page = total_pages
        rows, total_count = get_audit_logs(user_id, page, AUDIT_PER_PAGE)

    if total_count == 0:
        text = f"🕵️‍♂️ <b>Аудит ID: {user_id}</b>\n\nТранзакций не найдено."
        if is_callback:
            await message.edit_text(text, parse_mode="HTML")
        else:
            await message.answer(text, parse_mode="HTML")
        return

    text = f"🕵️‍♂️ <b>Аудит ID: {user_id}</b> (Всего: {total_count})\n"
    text += f"📄 Страница {page} из {total_pages}\n\n"

    for action, amount, details, date in rows:
        icon = "🔹"
        if "Roulette" in action: icon = "🎰"
        elif "Coin" in action: icon = "🪙"
        elif "Mines" in action: icon = "💣"
        elif "BJ" in action: icon = "🃏"
        elif "Transfer" in action: icon = "💸"
        elif "Admin" in action: icon = "👮‍♂️"
        elif "Slots" in action: icon = "🍒"
        elif "Bonus" in action: icon = "🎁"
        elif "Promo" in action: icon = "🎟"
        elif "DONATE" in action: icon = "💎"

        sign = "+" if amount > 0 else ""
        text += f"{icon} <b>{action}</b>: {sign}{fmt(amount)}\n"
        text += f"└ <i>{details}</i> | {date}\n\n"

    buttons = []
    row = []
    if page > 1:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"admin_history:{user_id}:{page-1}"))
    if page < total_pages:
        row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"admin_history:{user_id}:{page+1}"))
    if row:
        buttons.append(row)

    kb = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    if is_callback:
        await message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=kb, parse_mode="HTML")


# --- АДМИН КОМАНДА: BAN (БОТ) ---
@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return

    args = message.text.split()
    target_id = None
    date_idx = 1

    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
        date_idx = 1
    else:
        if len(args) < 2:
            await message.answer("⚠️ Укажите ID или сделайте реплай.")
            return
        if not args[1].isdigit():
            await message.answer("⚠️ ID должен быть числом.")
            return
        target_id = int(args[1])
        date_idx = 2

    if len(args) <= date_idx:
        await message.answer("⚠️ Укажите дату разбана (ДД.ММ.ГГГГ) или 'навсегда'.")
        return

    ban_until = args[date_idx]

    if ban_until.lower() == "навсегда":
        ban_until = "forever"
    else:
        try:
            datetime.strptime(ban_until, "%d.%m.%Y")
        except ValueError:
            await message.answer("⚠️ Неверный формат даты. Используйте ДД.ММ.ГГГГ")
            return

    reason = " ".join(args[date_idx + 1:])
    if not reason:
        reason = "не указана"

    ban_user_db(target_id, ban_until, reason, message.from_user.id)

    readable_date = ban_until if ban_until != "forever" else "навсегда"
    await message.answer(
        f"⛔ Пользователь {target_id} забанен в БОТЕ.\n"
        f"📅 Срок: {readable_date}\n"
        f"❓ Причина: {reason}"
    )


@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return

    args = message.text.split()
    target_id = None

    if message.reply_to_message:
        target_id = message.reply_to_message.from_user.id
    else:
        if len(args) < 2 or not args[1].isdigit():
            await message.answer("⚠️ Укажите ID или реплай.")
            return
        target_id = int(args[1])

    unban_user_db(target_id)
    await message.answer(f"✅ Пользователь {target_id} разбанен в боте.")


# --- СОЗДАНИЕ ПРОМОКОДА (АДМИН) ---
@dp.message(Command("addpromo"))
async def cmd_add_promo(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return

    args = message.text.split()
    if len(args) < 4:
        await message.answer("⚠️ Формат: `/addpromo КОД СУММА КОЛ-ВО`", parse_mode="Markdown")
        return

    code = args[1]
    amount = int(args[2])
    activations = int(args[3])

    if create_promo(code, amount, activations):
        await message.answer(f"✅ Промокод <code>{code}</code> создан!\nСумма: {fmt(amount)}\nАктиваций: {activations}",
                             parse_mode="HTML")
    else:
        await message.answer("❌ Такой код уже существует.")


# --- АКТИВАЦИЯ ПРОМОКОДА ЧЕРЕЗ #КОД ---
@dp.message(F.text.startswith("#"))
async def activate_promo_by_hashtag(message: types.Message):
    # разрешаем в личке и в группах
    if message.chat.type not in ("private", "group", "supergroup"):
        return

    if check_flood(message.from_user.id):
        return

    user_id = message.from_user.id
    update_user_name(
        user_id,
        message.from_user.first_name,
        message.from_user.username
    )

    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=CHANNEL_URL)]
        ])
        await message.answer(
            "🔒 <b>Промокоды только для своих!</b>\n\n"
            "Подпишись на канал, чтобы активировать код.",
            reply_markup=kb,
            parse_mode="HTML"
        )
        return

    # 👉 получаем код из #КОД
    code = message.text[1:].strip()

    if not code:
        return  # сообщение вида "#"

    status, amount = activate_promo(user_id, code)

    if status == "success":
        await message.answer(
            f"✅ Промокод активирован!\n"
            f"Получено: <b>{fmt(amount)} {CURRENCY}</b>",
            parse_mode="HTML"
        )
        log_audit(user_id, "Promo", amount, f"Code: {code}")

    elif status == "not_found":
        await message.answer("❌ Неверный код.")

    elif status == "ended":
        await message.answer("❌ Промокод закончился.")

    elif status == "already_used":
        await message.answer("❌ Вы уже активировали этот код.")



# --- ЕЖЕДНЕВНЫЙ БОНУС ---
@dp.message(F.text.lower().in_({"бонус", "/bonus", "bonus", "🎁 бонус"}))
async def cmd_daily_bonus(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id
    update_user_name(user_id, message.from_user.first_name, message.from_user.username)

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    is_subscribed = await check_subscription(user_id)
    if not is_subscribed:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Подписаться", url=CHANNEL_URL)]
        ])
        await message.answer(
            "🔒 <b>Бонус доступен только подписчикам!</b>\n\n"
            "Подпишись на наш новостной канал, чтобы получать халявную валюту.",
            reply_markup=kb,
            parse_mode="HTML"
        )
        return

    last_time = get_last_bonus_time(user_id)
    now = time.time()
    diff = now - last_time

    if diff < 43200:
        wait_sec = 43200 - diff
        hours = int(wait_sec // 3600)
        minutes = int((wait_sec % 3600) // 60)
        await message.answer(f"⏳ Бонус пока недоступен.\nПриходи через <b>{hours} ч {minutes} мин</b>.",
                             parse_mode="HTML")
        return

    bonus_amount = 1000
    update_balance(user_id, bonus_amount)
    update_bonus_time(user_id)

    log_audit(user_id, "Daily Bonus", bonus_amount, "Channel Sub")

    await message.answer(
        f"🎁 <b>Ежедневный бонус!</b>\n"
        f"Вы получили: <b>{fmt(bonus_amount)} {CURRENCY}</b>\n"
        f"Спасибо за подписку!",
        parse_mode="HTML"
    )


# --- ПРИВЕТСТВИЕ ПРИ ДОБАВЛЕНИИ В ЧАТ ---
@dp.message(F.new_chat_members)
async def on_user_join(message: types.Message):
    bot_id = (await bot.get_me()).id
    for user in message.new_chat_members:
        if user.id == bot_id:
            try:
                await message.answer(
                    "Напишите /start, чтобы зарегистрироваться.",
                    parse_mode="HTML"
                )
            except TelegramForbiddenError:
                pass
            return
        else:
            track_chat_member(message.chat.id, user.id)


# --- ПОИСК ПОЛЬЗОВАТЕЛЯ ПО ИМЕНИ ИЛИ ID ---
def search_users(query):
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    users = []

    # Поиск по ID
    if query.isdigit():
        cur.execute("SELECT user_id, name, username, balance FROM users WHERE user_id = ?", (int(query),))
        user = cur.fetchone()
        if user:
            users.append(user)

    # Поиск по имени
    cur.execute("SELECT user_id, name, username, balance FROM users WHERE name LIKE ? OR username LIKE ? LIMIT 10",
                (f"%{query}%", f"%{query}%"))
    users.extend(cur.fetchall())

    conn.close()
    return users

def get_users_count():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    conn.close()
    return total


def get_top_users_page(page: int, per_page: int = 10):
    offset = (page - 1) * per_page
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        SELECT user_id, name, username, balance
        FROM users
        ORDER BY balance DESC, user_id ASC
        LIMIT ? OFFSET ?
    """, (per_page, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


# --- АДМИН ПАНЕЛЬ: УПРАВЛЕНИЕ БАЛАНСОМ ---
@dp.message(Command("balance", "бал"))
async def admin_balance(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()
    if len(args) < 2:
        await message.answer("⚠️ Формат: `/balance ID` или `/balance @username`", parse_mode="Markdown")
        return

    target = args[1]
    users = search_users(target)

    if not users:
        await message.answer("❌ Пользователь не найден.")
        return

    user = users[0]
    user_id, name, username, balance = user

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Добавить", callback_data=f"admin_add:{user_id}"),
            InlineKeyboardButton(text="➖ Отнять", callback_data=f"admin_sub:{user_id}")
        ],
        [
            InlineKeyboardButton(text="📊 История операций", callback_data=f"admin_history:{user_id}:1"),
            InlineKeyboardButton(text="✏️ Изменить вручную", callback_data=f"admin_set:{user_id}")
        ]
    ])

    username_text = f"@{username}" if username else "нет"
    await message.answer(
        f"👤 <b>Пользователь:</b> {name}\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📝 Username: {username_text}\n"
        f"💰 Текущий баланс: <b>{fmt(balance)} {CURRENCY}</b>\n\n"
        f"Выберите действие:",
        reply_markup=kb,
        parse_mode="HTML"
    )


@dp.callback_query(F.data.startswith("admin_add:"))
async def admin_add_balance(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступно только админам!", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    user_data = get_user_data(user_id)

    if not user_data:
        await callback.answer("Пользователь не найден!", show_alert=True)
        return

    await callback.message.edit_text(
        f"➕ <b>Добавление баланса для {user_data['name']}</b>\n\n"
        f"Введите сумму для добавления (положительное число):",
        parse_mode="HTML"
    )

    # Сохраняем состояние
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.context import FSMContext

    class AdminState(StatesGroup):
        waiting_for_add_amount = State()

    # Для простоты создадим временное хранилище
    admin_actions[callback.from_user.id] = {
        'action': 'add',
        'target_id': user_id,
        'target_name': user_data['name']
    }

    await callback.answer()


@dp.callback_query(F.data.startswith("admin_sub:"))
async def admin_sub_balance(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступно только админам!", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    user_data = get_user_data(user_id)

    if not user_data:
        await callback.answer("Пользователь не найден!", show_alert=True)
        return

    await callback.message.edit_text(
        f"➖ <b>Списание баланса у {user_data['name']}</b>\n\n"
        f"Введите сумму для списания (положительное число):",
        parse_mode="HTML"
    )

    admin_actions[callback.from_user.id] = {
        'action': 'sub',
        'target_id': user_id,
        'target_name': user_data['name']
    }

    await callback.answer()


@dp.callback_query(F.data.startswith("admin_set:"))
async def admin_set_balance(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступно только админам!", show_alert=True)
        return

    user_id = int(callback.data.split(":")[1])
    user_data = get_user_data(user_id)

    if not user_data:
        await callback.answer("Пользователь не найден!", show_alert=True)
        return

    await callback.message.edit_text(
        f"✏️ <b>Установка баланса для {user_data['name']}</b>\n\n"
        f"Текущий баланс: {fmt(user_data['balance'])} {CURRENCY}\n"
        f"Введите новое значение баланса:",
        parse_mode="HTML"
    )

    admin_actions[callback.from_user.id] = {
        'action': 'set',
        'target_id': user_id,
        'target_name': user_data['name']
    }

    await callback.answer()


@dp.callback_query(F.data.startswith("admin_history:"))
async def admin_user_history(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Доступно только админам!", show_alert=True)
        return

    _, user_id, page = callback.data.split(":")
    user_id = int(user_id)
    page = int(page)

    await show_audit_page(callback, user_id, page)


# Временное хранилище для действий админа
admin_actions = {}


@dp.message(F.text.regexp(r'^\d+$'))
async def handle_admin_amount(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    if message.from_user.id not in admin_actions:
        return

    action_data = admin_actions.get(message.from_user.id)
    if not action_data:
        return

    try:
        amount = int(message.text)
        if amount <= 0:
            await message.answer("⚠️ Сумма должна быть положительным числом!")
            return

        user_id = action_data['target_id']
        user_name = action_data['target_name']
        action = action_data['action']

        current_balance = get_balance(user_id)

        if action == 'add':
            update_balance(user_id, amount)
            new_balance = current_balance + amount
            log_audit(user_id, "Admin Add", amount, f"Admin: {message.from_user.id}")
            action_text = f"добавлено {fmt(amount)}"

        elif action == 'sub':
            if amount > current_balance:
                await message.answer(f"⚠️ Нельзя списать больше текущего баланса ({fmt(current_balance)})!")
                return
            update_balance(user_id, -amount)
            new_balance = current_balance - amount
            log_audit(user_id, "Admin Subtract", -amount, f"Admin: {message.from_user.id}")
            action_text = f"списано {fmt(amount)}"

        elif action == 'set':
            update_balance(user_id, amount - current_balance)
            new_balance = amount
            log_audit(user_id, "Admin Set", amount - current_balance, f"Admin: {message.from_user.id}")
            action_text = f"установлен баланс {fmt(amount)}"

        else:
            return

        # Удаляем состояние
        if message.from_user.id in admin_actions:
            del admin_actions[message.from_user.id]

        await message.answer(
            f"✅ <b>Баланс обновлен!</b>\n\n"
            f"👤 Пользователь: {user_name}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"📊 Действие: {action_text}\n"
            f"💰 Новый баланс: <b>{fmt(new_balance)} {CURRENCY}</b>",
            parse_mode="HTML"
        )

    except ValueError:
        await message.answer("⚠️ Введите корректное число!")


# --- АДМИН ПАНЕЛЬ: ПОИСК (/search) ---
@dp.message(Command("search"))
async def cmd_admin_search(message: types.Message):
    # /search работает ТОЛЬКО для админов и ТОЛЬКО в личке
    if message.chat.type != "private":
        return
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip():
        await message.answer("Формат: <code>/search ID</code> или <code>/search @username</code>", parse_mode="HTML")
        return

    q = args[1].strip()
    target_id = None

    # 1) ID
    if q.isdigit():
        target_id = int(q)

    # 2) @username
    elif q.startswith("@"):
        username = q[1:]
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (username,))
        row = cur.fetchone()
        conn.close()
        if row:
            target_id = int(row[0])

    # 3) Поиск по имени (частичное) — вернём список (до 15)
    else:
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        like = f"%{q}%"
        cur.execute("""
            SELECT user_id, name, username, balance
            FROM users
            WHERE name LIKE ? OR username LIKE ?
            ORDER BY balance DESC
            LIMIT 15
        """, (like, like))
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await message.answer("Ничего не найдено.")
            return

        text = ["🔎 <b>Найдено (топ по балансу):</b>\n"]
        for uid, name, uname, bal in rows:
            u = f"@{uname}" if uname else "—"
            text.append(f"• <code>{uid}</code> | {name} | {u} | <b>{fmt(bal)} {CURRENCY}</b>")
        text.append("\nЧтобы открыть карточку: <code>/search ID</code>")
        await message.answer("\n".join(text), parse_mode="HTML")
        return

    if not target_id:
        await message.answer("Пользователь не найден.")
        return

    core = get_user_core(target_id)
    if not core:
        await message.answer("Пользователь не найден в базе.")
        return

    flows = get_user_money_flows(target_id)
    flags = build_antifraud_flags(core, flows)

    incoming, outgoing = get_user_top_transfer_partners(target_id, limit=5)
    recent = get_user_recent_audit(target_id, limit=12)

    uname = f"@{core['username']}" if core["username"] else "—"
    ref = core["referrer_id"]

    lines = []
    lines.append("🧾 <b>AntiFraud-карточка игрока</b>\n")
    lines.append(f"🆔 <code>{core['user_id']}</code>")
    lines.append(f"👤 {core['name']} | {uname}")
    lines.append(f"💰 Баланс: <b>{fmt(core['balance'])} {CURRENCY}</b>")
    lines.append(f"🤝 Реферер: <code>{ref}</code>" if ref else "🤝 Реферер: —")
    lines.append(f"🗓️ Создан: <code>{core['created_at']}</code>")
    lines.append(f"🕒 Последняя активность: <code>{flows['last_ts']}</code>\n")

    lines.append("📌 <b>Сводка потоков</b>")
    lines.append(f"• Audit: записей <b>{flows['audit_count']}</b> | приход <b>{fmt(flows['audit_in'])}</b> | расход <b>{fmt(abs(flows['audit_out']))}</b>")
    lines.append(f"• Promo: активаций <b>{flows['promo_used_count']}</b>")
    lines.append(f"• Admin: операций <b>{flows['admin_ops_count']}</b> | сумма <b>{fmt(flows['admin_sum'])}</b>")
    lines.append(f"• Transfers IN: <b>{flows['tr_in_cnt']}</b> / <b>{fmt(flows['tr_in_sum'])}</b>")
    lines.append(f"• Transfers OUT: <b>{flows['tr_out_cnt']}</b> / <b>{fmt(flows['tr_out_sum'])}</b>\n")

    if flags:
        lines.append("🚨 <b>Флаги</b>")
        for f in flags:
            lines.append(f"• {f}")
        lines.append("")

    # Топ контрагентов
    lines.append("🔁 <b>Топ контрагентов (переводы)</b>")
    if incoming:
        lines.append("⬇️ <b>Входящие</b>")
        for sid, sname, ssum, scnt in incoming:
            lines.append(f"• <code>{sid}</code> {sname} — {fmt(ssum)} ({scnt}×)")
    else:
        lines.append("⬇️ Входящие: —")

    if outgoing:
        lines.append("⬆️ <b>Исходящие</b>")
        for tid, tname, tsum, tcnt in outgoing:
            lines.append(f"• <code>{tid}</code> {tname} — {fmt(tsum)} ({tcnt}×)")
    else:
        lines.append("⬆️ Исходящие: —")

    # Последние события audit
    lines.append("\n🧾 <b>Последние события (audit)</b>")
    if recent:
        for action_type, amount, details, dt in recent:
            sign = "+" if amount > 0 else ""
            det = (details or "").strip()
            if len(det) > 80:
                det = det[:80] + "…"
            lines.append(f"• <code>{dt}</code> | {action_type} | <b>{sign}{fmt(amount)}</b> | {det}")
    else:
        lines.append("—")

    await message.answer("\n".join(lines), parse_mode="HTML")




@dp.message(Command("topbot"))
async def cmd_topbot(message: types.Message):
    # только админы
    if message.from_user.id not in ADMIN_IDS:
        return

    # только ЛС
    if message.chat.type != "private":
        await message.answer("⚠️ Команда доступна только в личке бота.")
        return

    total_users = get_users_count()
    total_pages = max(1, (total_users + TOPBOT_PER_PAGE - 1) // TOPBOT_PER_PAGE)

    page = 1
    rows = get_top_users_page(page, TOPBOT_PER_PAGE)

    text = render_topbot_text(rows, page, total_pages, total_users)
    kb = build_topbot_kb(page, total_pages)

    await message.answer(text, reply_markup=kb, parse_mode="HTML")



@dp.callback_query(F.data.startswith("admin_history:"))
async def audit_pagination(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Только для админа!", show_alert=True)
        return

    _, target_id, page = callback.data.split(":")
    await show_audit_page(callback, int(target_id), int(page))



@dp.message(Command("give"))
async def admin_give(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    args = message.text.split()

    if message.reply_to_message:
        if len(args) < 2:
            await message.answer("⚠️ Формат: ответом на сообщение пользователя: /give СУММА (можно отрицательную)")
            return
        try:
            amount = int(args[1])
        except ValueError:
            await message.answer("⚠️ Сумма должна быть числом. Пример: /give -1000")
            return

        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.full_name

    else:
        if len(args) < 3:
            await message.answer("⚠️ Формат: /give USER_ID СУММА  или  /give @username СУММА")
            return

        target_id, target_name, rest = await resolve_command_args(message, args)
        if not target_id:
            await message.answer("⚠️ Не нашёл пользователя. Используйте ID, @username (если он есть в БД) или реплай.")
            return

        if not rest or len(rest) < 1:
            await message.answer("⚠️ Укажите сумму. Пример: /give 12345 -1000")
            return

        try:
            amount = int(rest[0])
        except ValueError:
            await message.answer("⚠️ Сумма должна быть числом. Пример: /give 12345 -1000")
            return

    get_balance(target_id)
    before = get_balance(target_id)
    update_balance(target_id, amount)
    after = get_balance(target_id)

    log_transfer(message.from_user.id, "Admin Gift", target_id, f"{target_name}", amount)
    log_audit(target_id, "Admin Gift", amount, "From Admin")

    sign = "+" if amount > 0 else ""
    await message.answer(
        f"✅ {target_name}\n"
        f"Изменение: {sign}{fmt(amount)} {CURRENCY}\n"
        f"Баланс: {fmt(before)} → {fmt(after)} {CURRENCY}",
        parse_mode="HTML"
    )


# --- АДМИН СТАТИСТИКА (/stata) ---
@dp.message(Command("stata"))
async def admin_stats(message: types.Message):
    if message.from_user.id not in ADMIN_IDS: return

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*), SUM(balance) FROM users")
    data = cur.fetchone()
    users_count = data[0]
    total_balance = data[1] if data[1] else 0

    conn.close()

    text = (
        f"📊 <b>Статистика Бота</b>\n\n"
        f"👥 Пользователей: <b>{users_count}</b>\n"
        f"💰 Всего денег в мире: <b>{fmt(total_balance)} {CURRENCY}</b>\n"
    )
    await message.answer(text, parse_mode="HTML")


# --- ИСТОРИЯ ПЕРЕВОДОВ ---
@dp.message(Command("history", "история"))
async def cmd_transfers_history(message: types.Message):
    if check_flood(message.from_user.id): return
    user_id = message.from_user.id

    rows = get_user_transfers(user_id, 10)
    if not rows:
        await message.answer("📭 Переводов пока не было.")
        return

    lines = []
    for row in rows:
        s_id, s_name, t_id, t_name, amt, date = row
        if s_id == user_id:
            lines.append(f"📤 <b>{date}</b>: Вы отправили {fmt(amt)} ➜ {t_name}")
        else:
            lines.append(f"📥 <b>{date}</b>: Получено {fmt(amt)} ⬅️ от {s_name}")

    await message.answer("📜 <b>Ваши переводы:</b>\n\n" + "\n".join(lines), parse_mode="HTML")


# --- ПЕРЕВОДЫ ВАЛЮТЫ ---
@dp.message(F.text.lower().startswith(("п ", "p ")))
async def cmd_transfer(message: types.Message):
    if check_flood(message.from_user.id): return
    args = message.text.split()
    sender_id = message.from_user.id
    sender_name = message.from_user.first_name
    update_user_name(sender_id, sender_name, message.from_user.username)

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, sender_id)

    target_id = None
    amount = None
    target_name = "Unknown"

    if message.reply_to_message:
        if len(args) < 2 or not args[1].isdigit(): return
        amount = int(args[1])
        target_id = message.reply_to_message.from_user.id
        target_name = message.reply_to_message.from_user.first_name
    else:
        if len(args) < 3 or not args[1].isdigit() or not args[2].isdigit(): return
        target_id = int(args[1])
        amount = int(args[2])
        target_name = f"ID {target_id}"

    if sender_id == target_id: return
    if amount <= 0: return

    sender_bal = get_balance(sender_id)
    if sender_bal < amount:
        await message.answer(f"❌ Не хватает средств.")
        return

    get_balance(target_id)
    update_balance(sender_id, -amount)
    update_balance(target_id, amount)

    log_transfer(sender_id, sender_name, target_id, target_name, amount)
    log_audit(sender_id, "Transfer OUT", -amount, f"To {target_id}")
    log_audit(target_id, "Transfer IN", amount, f"From {sender_id}")

    await message.answer(
        f"💸 <b>Перевод:</b>\n"
        f"{sender_name} ➜ {target_name}: <b>{fmt(amount)} {CURRENCY}</b>",
        parse_mode="HTML"
    )


# --- БАЛАНС ---
@dp.message(F.text.lower().in_({"баланс", "б", "b", "balance", "money", "💰 баланс"}))
async def check_balance(message: types.Message):
    if check_flood(message.from_user.id): return
    bal = get_balance(message.from_user.id)
    update_user_name(message.from_user.id, message.from_user.first_name, message.from_user.username)
    await message.answer(
        text=(
            f"<b>{message.from_user.first_name}</b>\n"
            f"Баланс: <b>{fmt(bal)} Luxe</b>"
        ),
        parse_mode="HTML"
    )


# --- ОТМЕНА СТАВОК ---
@dp.message(F.text.lower().in_({"отмена", "cancel", "сброс", "❌ отмена ставок"}))
async def cancel_bets(message: types.Message):
    if check_flood(message.from_user.id): return
    chat_id = message.chat.id
    user_id = message.from_user.id

    if game_states.get(chat_id) == 'spinning':
        await message.answer("❌ Уже крутится, поздно!")
        return

    bets = chat_bets.get(chat_id, [])
    if not bets:
        await message.answer("🤷‍♂️ Ставок нет.")
        return

    new_bets = []
    refund = 0

    for bet in bets:
        if bet['user_id'] == user_id:
            refund += bet['amount']
        else:
            new_bets.append(bet)

    if refund > 0:
        chat_bets[chat_id] = new_bets
        update_balance(user_id, refund)
        log_audit(user_id, "Roulette Refund", refund, "Cancel bets")
        if not new_bets and chat_id in chat_first_bet_time:
            del chat_first_bet_time[chat_id]

        await message.answer(f"↩️ {message.from_user.first_name} вернул {fmt(refund)} {CURRENCY}.")
    else:
        await message.answer(f"{message.from_user.first_name}, у тебя нет ставок.")


# --- ЛОГ (Рулетка) ---
@dp.message(F.text.lower().in_({"лог", "log"}))
async def show_roulette_log(message: types.Message):
    if check_flood(message.from_user.id): return
    history = get_history(message.chat.id, 10)
    if not history:
        await message.answer("📭 История пуста.")
        return
    history_str = "\n".join([f"{num}{emoji}" for emoji, num in history])
    await message.answer(f"📋 <b>Рулетка (последние 10):</b>\n\n{history_str}", parse_mode="HTML")


@dp.message(F.text.contains("🎰"))
@dp.message(Command("slots"))
async def play_slots(message: types.Message):
    if check_flood(message.from_user.id):
        return
    if message.chat.type in ("group", "supergroup"):
        if not is_game_enabled(message.chat.id, game_key="slots"):
            return
    user_id = message.from_user.id

    if user_id in SLOTS_IN_PROGRESS:
        await message.answer("⏳ Дождитесь результата прошлой игры в слоты.")
        return

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    SLOTS_IN_PROGRESS.add(user_id)
    try:
        bet = 50
        parts = message.text.split()
        if len(parts) > 1 and parts[1].isdigit():
            bet = int(parts[1])

        if bet <= 0:
            return

        if get_balance(user_id) < bet:
            await message.answer(
                f"❌ Мало денег! Баланс: {fmt(get_balance(user_id))}, ставка: {fmt(bet)}"
            )
            return

        update_balance(user_id, -bet)
        log_audit(user_id, "Slots Bet", -bet)

        msg = await message.answer_dice(emoji="🎰")
        await asyncio.sleep(2)

        win = 0
        if msg.dice.value == 64:
            win = bet * 20
            text = "JACKPOT!"
        elif msg.dice.value in [1, 22, 43]:
            win = bet * 3
            text = "Победа!"
        else:
            text = "Мимо."
            try:
                await pay_referrer_commission(user_id, bet)
            except Exception:
                logging.exception("pay_referrer_commission failed in slots lose")

        if win > 0:
            update_balance(user_id, win)
            log_audit(user_id, "Slots WIN", win)

        await message.answer(
            f"{text} (+{fmt(win)} {CURRENCY})\nБаланс: {fmt(get_balance(user_id))}"
        )

    finally:
        SLOTS_IN_PROGRESS.discard(user_id)


@dp.message(F.text.lower().startswith(("/dice", "dice", "кости")))
@dp.message(F.text == "🎲")
async def play_dice(message: types.Message):
    if check_flood(message.from_user.id):
        return
    if message.chat.type in ("group", "supergroup"):
        if not is_game_enabled(message.chat.id, game_key="dice"):
            return

    user_id = message.from_user.id

    if user_id in DICE_IN_PROGRESS:
        await message.answer("⏳ Дождитесь результата прошлой игры в кости.")
        return

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    DICE_IN_PROGRESS.add(user_id)
    try:
        bet = 50
        parts = message.text.split()
        if len(parts) > 1 and parts[1].isdigit():
            bet = int(parts[1])

        if bet <= 0:
            return

        if get_balance(user_id) < bet:
            await message.answer(f"❌ Не хватает денег ({fmt(get_balance(user_id))}) для ставки {fmt(bet)}!")
            return

        update_balance(user_id, -bet)
        log_audit(user_id, "Dice Bet", -bet)

        m1 = await message.answer_dice(emoji="🎲")
        await asyncio.sleep(3)
        m2 = await message.answer_dice(emoji="🎲")
        await asyncio.sleep(3)

        val1 = m1.dice.value
        val2 = m2.dice.value

        try:
            k = dice_payout_factor(bet)
            k = max(1.0, float(k))
        except Exception:
            k = 1.0

        if val1 > val2:
            payout = int(bet * DICE_WIN_MULTIPLIER_BASE * k)
            profit = payout - bet

            update_balance(user_id, payout)
            log_audit(user_id, "Dice WIN", payout)

            res = (
                f"✅ <b>Победа!</b>\n"
                f"Выпало: {val1} &gt; {val2}\n"
                f"Выплата: <b>+{fmt(payout)} {CURRENCY}</b>\n"
                f"Чистая прибыль: <b>+{fmt(profit)} {CURRENCY}</b>"
            )

            await message.answer(res, parse_mode="HTML")
            return

        elif val1 < val2:
            res = (
                f"❌ <b>Проигрыш.</b>\n"
                f"Выпало: {val1} &lt; {val2}\n"
                f"Потеряно: <b>{fmt(bet)} {CURRENCY}</b>"
            )

            await message.answer(res, parse_mode="HTML")

            try:
                await pay_referrer_commission(user_id, bet)
            except Exception:
                logging.exception("pay_referrer_commission failed in dice lose")

            return

        else:
            update_balance(user_id, bet)
            log_audit(user_id, "Dice Refund", bet)

            await message.answer("🤝 <b>Ничья.</b>\nВозврат ставки.", parse_mode="HTML")
            return

    finally:
        DICE_IN_PROGRESS.discard(user_id)


# --- ДОБАВЛЕНИЕ СТАВОК ---
async def add_bet_to_pool(message: types.Message, amount: int, raw_choices: list):
    chat_id = message.chat.id
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    update_user_name(user_id, user_name, message.from_user.username)

    if message.chat.type != 'private':
        track_chat_member(message.chat.id, user_id)

    if game_states.get(chat_id) == 'spinning':
        await message.answer("⏳ Не ставь под руку, крутится!")
        return

    if chat_id not in chat_bets:
        chat_bets[chat_id] = []

    if not chat_bets[chat_id]:
        chat_first_bet_time[chat_id] = time.time()

    bets_to_add = []
    valid_raws = []

    current_user_bets_count = len([b for b in chat_bets[chat_id] if b['user_id'] == user_id])
    if current_user_bets_count + len(raw_choices) > 100:
        await message.answer(f"🛑 {user_name}, лимит 100 ставок за раунд!")
        return

    for raw in raw_choices:
        targets = get_bet_targets(raw)

        if targets:
            bets_to_add.append({
                'user_id': user_id,
                'name': user_name,
                'amount': amount,
                'targets': targets,
                'raw': str(raw)
            })
            valid_raws.append(str(raw))

    if not bets_to_add: return

    total_cost = amount * len(bets_to_add)
    current_balance = get_balance(user_id)
    if current_balance < total_cost:
        await message.answer(f"❌ {user_name}, не хватает денег (надо {fmt(total_cost)}).")
        return

    update_balance(user_id, -total_cost)
    log_audit(user_id, "Roulette Bet", -total_cost, f"On {', '.join(valid_raws)}")

    chat_bets[chat_id].extend(bets_to_add)

    confirm_lines = []
    for raw_choice in valid_raws:
        confirm_lines.append(f"Ставка принята: <b>{user_name}</b> {fmt(amount)} {CURRENCY} на <b>{raw_choice}</b>")

    confirm_text = "\n".join(confirm_lines)

    start_time = chat_first_bet_time.get(chat_id, 0)
    time_passed = time.time() - start_time
    if time_passed >= 10:
        confirm_text += "\n✅ <b>Можно крутить! Пиши ГО.</b>"

    await message.answer(confirm_text, parse_mode="HTML")


# --- ЗАПУСК (ГО) ---

ROULETTE_GO_RE = re.compile(r"^\s*(го|go|крути|погнали)\s*[!?.]*\s*$", re.IGNORECASE)

@dp.message(lambda m: m.text is not None and ROULETTE_GO_RE.match(m.text))
async def manual_spin(message: types.Message):
    if check_flood(message.from_user.id):
        return

    # только группы
    if message.chat.type not in ("group", "supergroup"):
        return

    chat_id = message.chat.id

    # отключено в чате
    if not is_game_enabled(chat_id, game_key="roulette"):
        return

    # анти-залип: если spinning висит слишком долго — сбрасываем
    if game_states.get(chat_id) == "spinning":
        started = chat_first_bet_time.get(chat_id, 0)
        # если прошло больше 120 сек — считаем зависло и сбрасываем
        if started and (time.time() - started) > 120:
            game_states.pop(chat_id, None)
        else:
            await message.answer("⚠️ Уже крутится!")
            return

    bets = chat_bets.get(chat_id, [])
    if not bets:
        await message.answer("⚠️ Ставок нет.")
        return

    bettors_ids = {bet["user_id"] for bet in bets}
    if message.from_user.id not in bettors_ids:
        await message.answer(
            f"⚠️ <b>{message.from_user.first_name}</b>, вы не сделали ставку! Крутить могут только игроки.",
            parse_mode="HTML",
        )
        return

    start_time = chat_first_bet_time.get(chat_id, 0)
    if not start_time:
        await message.answer("⚠️ Не найдено время первой ставки. Пул сброшен, ставьте заново.")
        chat_bets.pop(chat_id, None)
        game_states.pop(chat_id, None)
        chat_first_bet_time.pop(chat_id, None)
        return

    time_passed = time.time() - start_time
    if time_passed < 10:
        await message.answer(f"⏳ <b>Рано!</b> Еще {int(10 - time_passed)} сек.", parse_mode="HTML")
        return

    # ВАЖНО: запускать фоном, чтобы хендлер не блокировался
    asyncio.create_task(execute_spin(chat_id))

# --- ПАРСЕР ТЕКСТА ---
@dp.message()
async def roulette_text_parser(message: types.Message):
    # только группы
    if message.chat.type not in ("group", "supergroup"):
        return

    # отключено в чате
    if not is_game_enabled(message.chat.id, game_key="roulette"):
        return

    if not message.text:
        return

    text = message.text.strip()
    parts = text.split()

    # чтобы "го/крути" не обрабатывались тут
    if ROULETTE_GO_RE.match(text):
        return

    if not parts:
        return

    # ставка должна начинаться с числа
    if not parts[0].isdigit():
        return

    if check_flood(message.from_user.id):
        return

    amount = int(parts[0])
    raw_choices = [p.lower() for p in parts[1:]]

    if raw_choices:
        await add_bet_to_pool(message, amount, raw_choices)


# === МИГРАЦИЯ БД (добавляет отсутствующие колонки в уже существующих таблицах) ===
def get_all_tables(cur) -> set[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {r[0] for r in cur.fetchall()}


def migrate_db():
    print("[MIGRATE] DB_FILE =", DB_FILE)
    print("[MIGRATE] cwd =", os.getcwd())

    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    def table_columns(table: str) -> set[str]:
        cur.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}

    def ensure_column(table: str, column: str, ddl: str, backfill_sql: str | None = None):
        # ✅ защита: таблицы может не быть
        if table not in existing_tables:
            return

        cols = table_columns(table)
        if column not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
            if backfill_sql:
                cur.execute(backfill_sql)
            print(f"[MIGRATE] Added column {table}.{column}")

    existing_tables = get_all_tables(cur)

    # --- 1) promo_activations.activated_at ---
    ensure_column(
        table="promo_activations",
        column="activated_at",
        ddl="activated_at TEXT",
        backfill_sql="UPDATE promo_activations SET activated_at = COALESCE(activated_at, CURRENT_TIMESTAMP)"
    )

    # --- 2) created_at: сначала пробуем типовые таблицы ---
    money_tables = [
        "users",
        "deposits",
        "withdrawals",
        "bets",
        "transactions",
        "money_flows",
        "promo_activations",
        "referrals",
        "referral_rewards",
        "miner_purchases",
        "mines_games",
        "mines_bets",
    ]

    for t in money_tables:
        ensure_column(
            table=t,
            column="created_at",
            ddl="created_at TEXT",
            backfill_sql=f"UPDATE {t} SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
        )

    # --- 3) ✅ ЖЁСТКО: добавляем created_at во ВСЕ таблицы (кроме системных) ---
    # Это гарантирует, что UNION в get_user_money_flows больше не будет падать.
    for t in sorted(existing_tables):
        if t.startswith("sqlite_"):
            continue
        ensure_column(
            table=t,
            column="created_at",
            ddl="created_at TEXT",
            backfill_sql=f"UPDATE {t} SET created_at = COALESCE(created_at, CURRENT_TIMESTAMP)"
        )

    # --- 4) Диагностика: какие таблицы всё ещё без created_at ---
    no_created = []
    for t in sorted(existing_tables):
        if t.startswith("sqlite_"):
            continue
        cols = table_columns(t)
        if "created_at" not in cols:
            no_created.append(t)

    print("[MIGRATE] Tables WITHOUT created_at:", no_created)

    conn.commit()
    conn.close()


# === ЗАПУСК ===
async def main():
    db_start()
    migrate_db()  # ✅ ДОБАВЬ ЭТО СРАЗУ ПОСЛЕ db_start()
    print("Казино запущено!")

    try:
        # Быстрая проверка токена (если токен неверный/отозван — упадём здесь с понятной ошибкой)
        me = await bot.get_me()
        logger.info("Авторизация успешна: @%s (id=%s)", getattr(me, "username", None), getattr(me, "id", None))

        await bot.delete_webhook(drop_pending_updates=True)

        # ✅ Воркер возврата ставки по неактивности в Mines
        asyncio.create_task(mines_inactivity_worker(bot))

        await dp.start_polling(bot)

    except TelegramUnauthorizedError as e:
        logger.error("Telegram Unauthorized. Проверь BOT_TOKEN (BotFather -> /token). Ошибка: %s", e)
        raise

    finally:
        try:
            await bot.session.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())