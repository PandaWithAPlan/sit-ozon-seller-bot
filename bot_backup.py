# bot.py
from menu import back_home_menu  # кнопки «Назад/Домой»
import asyncio
import json
import logging
import os
import time
import re
from typing import Dict, List, Optional

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import (
    BotCommand,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
)
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from dotenv import load_dotenv

# --- Логирование (сразу, до чтения .env) ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("seller-bot")
logging.getLogger("dotenv").setLevel(logging.ERROR)
logging.getLogger("dotenv.main").setLevel(logging.ERROR)

# --- .env ---
BASE_DIR = os.path.dirname(__file__)
ENV_PATH = os.path.join(BASE_DIR, ".env")


def _load_env_with_fallback(path: str) -> str:
    """
    Загружает .env файл с поддержкой разных кодировок.

    Args:
        path: Путь к .env файлу

    Returns:
        Кодировка, с которой успешно загружен файл
    """
    encodings = ("utf-8", "utf-8-sig", "cp1251", "koi8-r", "iso-8859-5")
    for enc in encodings:
        try:
            ok = load_dotenv(path, override=True, encoding=enc)
            if ok:
                return enc
        except UnicodeDecodeError:
            log.debug(f"Failed to load {path} with encoding {enc}: UnicodeDecodeError")
            continue
        except (OSError, IOError) as e:
            log.warning(f"Failed to read {path} with encoding {enc}: {e}")
            continue
        except Exception as e:
            log.error(f"Unexpected error loading {path} with encoding {enc}: {e}", exc_info=True)
            continue
    # Финальная попытка с авто-определением
    try:
        load_dotenv(path, override=True)
        log.info(f".env loaded with auto-detection for {path}")
    except Exception as e:
        log.error(f"Failed to load .env with auto-detection: {e}", exc_info=True)
    return "auto"


used_env_encoding = _load_env_with_fallback(ENV_PATH)
log.info(f".env loaded using encoding: {used_env_encoding}")

# Таймзона (если задана)
TZ = os.getenv("TZ")
if TZ:
    os.environ["TZ"] = TZ
    try:
        time.tzset()
        log.info(f"Timezone set to {TZ}")
    except (AttributeError, OSError) as e:
        log.warning(f"Failed to set timezone {TZ}: {e}")
    except Exception as e:
        log.error(f"Unexpected error setting timezone {TZ}: {e}", exc_info=True)

# Токен
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("BOT_TOKEN")

# Пути/имена для загрузки Excel
try:
    from modules_common.paths import DATA_DIR  # type: ignore
except ImportError as e:
    log.debug(f"Failed to import DATA_DIR from modules_common.paths: {e}")
    DATA_DIR = os.path.join(BASE_DIR, "data")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        log.info(f"Created fallback DATA_DIR: {DATA_DIR}")
    except OSError as e:
        log.warning(f"Failed to create DATA_DIR {DATA_DIR}: {e}")
except Exception as e:
    log.error(f"Unexpected error importing DATA_DIR: {e}", exc_info=True)
    DATA_DIR = os.path.join(BASE_DIR, "data")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError:
        pass

PURCHASES_XLSX_NAME = os.getenv("PURCHASES_XLSX_NAME", "Товары.xlsx")
TARGET_XLSX_PATH = os.path.join(DATA_DIR, PURCHASES_XLSX_NAME)

# Корневые роутеры разделов
from handlers import router as handlers_router  # noqa: E402
from scheduler import scheduler_start  # noqa: E402

# === Уведомления: импортируем реестр и раннеры ===
from scheduler import (
    NOTICE_REGISTRY,
    run_notice,
    SPREAD_SEC,
    send_seller_reminder,
    register_notice_chat,
    send_digest_full,  # «Весь утренний дайджест»
    send_digest_short,  # «Сокращенный дайджест»
    # ↓ для показа главного меню при старте
    CHAT_IDS,
    NOTICES_PREFER_LOCAL,
    _read_notice_chat,
)  # noqa: E402

# Подключаем роутер «Юниты» (/units)
from modules_common.units import router as units_router  # noqa: E402

# Состояние сценария загрузки живёт в handlers_purchases
from handlers.handlers_purchases import BuyoutsUpload  # noqa: E402

# Для /method — список и текущий метод из модуля прогноза
from modules_sales.sales_forecast import (  # noqa: E402
    list_forecast_methods,
    get_forecast_method_title,
    get_forecast_method,
    ES_ALPHA,
)

# ---------- helpers ----------


async def _ack(cb: CallbackQuery) -> None:
    """Безопасный ответ на callback query."""
    try:
        await cb.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e).lower() or "query id expired" in str(e).lower():
            log.debug(f"Callback query expired: {e}")
        else:
            log.warning(f"Failed to answer callback query: {e}")
    except Exception as e:
        log.error(f"Unexpected error answering callback query: {e}", exc_info=True)


async def _safe_edit_msg(cb: CallbackQuery, text: str, **kwargs) -> None:
    """Безопасное редактирование сообщения."""
    try:
        await cb.message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            log.debug("Message not modified, skipping")
        elif "message to edit not found" in str(e):
            log.warning(f"Message not found for editing: {e}")
        else:
            log.error(f"Bad request editing message: {e}", exc_info=True)
    except Exception as e:
        log.error(f"Unexpected error editing message: {e}", exc_info=True)


def _fmt_alpha(a: float) -> str:
    s = f"{float(a):.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


# ---------- Главное меню (кнопки) ----------


def _build_main_menu_kb() -> InlineKeyboardMarkup:
    """Создает главное меню бота."""
    try:
        import menu

        for fn_name in ("main_menu", "home_menu", "root_menu", "start_menu", "main_menu_kb"):
            fn = getattr(menu, fn_name, None)
            if callable(fn):
                kb = fn()
                if isinstance(kb, InlineKeyboardMarkup):
                    log.debug(f"Using custom menu function: {fn_name}")
                    return kb
    except ImportError as e:
        log.debug(f"Could not import menu module: {e}")
    except Exception as e:
        log.warning(f"Error building custom menu, using fallback: {e}")

    rows = [
        [InlineKeyboardButton(text="📈 Продажи", callback_data="sales")],
        [InlineKeyboardButton(text="🏷️ Выкупы", callback_data="buyouts")],
        [InlineKeyboardButton(text="🚚 Отгрузки", callback_data="shipments")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _home_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]]
    )


def _welcome_text() -> str:
    return (
        "👋 Привет! Я помогу Вам спланировать продажи, выкупы и отгрузки.\n\n"
        "📈 В разделе <b>«Продажи»</b> — помогают понять динамику: что продаётся лучше, где растёт конверсия и насколько выполнен план.\n"
        "🏷️ В разделе <b>«Выкупы»</b> — показывают, какие товары нужно докупить и где сейчас ваши заказы в пути или обработке.\n"
        "🚚 В разделе <b>«Отгрузки»</b> — подсказывают, какие позиции пора отгрузить на склад, чтобы не потерять продажи и держать запас под контролем.\n\n"
        "Выберите раздел:"
    )


# ---------- Меню методов прогноза (/method) ----------


def _build_method_kb() -> InlineKeyboardMarkup:
    current_code = get_forecast_method()
    methods = dict(list_forecast_methods())

    def _btn(code: str) -> InlineKeyboardButton:
        title = methods.get(code, code)
        if code == "es":
            title = f"Эксп. сглаживание ({_fmt_alpha(ES_ALPHA)})"
        mark = "✓ " if code == current_code else ""
        return InlineKeyboardButton(text=f"{mark}{title}", callback_data=f"plan:method:set:{code}")

    rows = [
        [_btn("ma7"), _btn("ma14")],
        [_btn("ma30"), _btn("ma60")],
        [_btn("ma90"), _btn("ma180")],
        [_btn("ma360"), _btn("es")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------- Кнопки после загрузки (используются в handlers_purchases) ----------


def _after_upload_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🏷️ Перейти в «Выкупы»", callback_data="buyouts")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =====================================================================
#   /warehouse — глобальные настройки методики потребности (метод + период)
# =====================================================================


MODULES_SHIP_DIR = os.path.join(BASE_DIR, "modules_shipments")
WAREHOUSE_PREFS_PATH = os.path.join(
    MODULES_SHIP_DIR, "data", "cache", "common", "warehouse_prefs.json"
)
os.makedirs(os.path.dirname(WAREHOUSE_PREFS_PATH), exist_ok=True)

LEGACY_PREFS_PATH = os.path.join(BASE_DIR, "data", "cache", "common", "warehouse_prefs.json")

WH_METHODS = ("average", "dynamics", "hybrid")
WH_METHOD_TITLES = {
    "average": "Среднесуточный спрос",
    "dynamics": "Динамика заказов",
    "hybrid": "Адаптивный гибрид",
}
WH_PERIODS = (30, 60, 90, 180, 360)

CLEAR_ON_PERIOD_CHANGE = os.getenv("DEMAND_CLEAR_ON_PERIOD_CHANGE", "0").strip().lower() in (
    "1",
    "true",
    "yes",
)


def _read_json(path: str) -> dict:
    """
    Безопасное чтение JSON файла.

    Args:
        path: Путь к JSON файлу

    Returns:
        Словарь с данными или пустой словарь при ошибке
    """
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except json.JSONDecodeError as e:
        log.warning(f"Failed to parse JSON from {path}: {e}")
    except UnicodeDecodeError as e:
        log.warning(f"Failed to decode {path} (encoding issue): {e}")
    except (IOError, OSError) as e:
        log.warning(f"Failed to read JSON file {path}: {e}")
    except Exception as e:
        log.error(f"Unexpected error reading JSON from {path}: {e}", exc_info=True)
    return {}


def _write_json(path: str, payload: dict) -> bool:
    """
    Безопасная запись JSON файла.

    Args:
        path: Путь к JSON файлу
        payload: Данные для записи

    Returns:
        True если запись успешна, False в противном случае
    """
    try:
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log.debug(f"Successfully wrote JSON to {path}")
        return True
    except (TypeError, ValueError) as e:
        log.error(f"Failed to serialize payload for {path}: {e}")
    except (IOError, OSError) as e:
        log.error(f"Failed to write JSON file {path}: {e}")
    except Exception as e:
        log.error(f"Unexpected error writing JSON to {path}: {e}", exc_info=True)
    return False


def _load_wh_global() -> dict:
    data = _read_json(WAREHOUSE_PREFS_PATH) or _read_json(LEGACY_PREFS_PATH)
    m = (data.get("method") or "average").strip().lower()
    p = int(data.get("period") or 90)
    if m not in WH_METHODS:
        m = "average"
    if p not in WH_PERIODS:
        p = 90
    return {"method": m, "period": p}


def _save_wh_global(method: str, period: int) -> bool:
    """
    Сохраняет глобальные настройки потребности складов.

    Args:
        method: Метод расчета (average/dynamics/hybrid)
        period: Период в днях

    Returns:
        True если сохранение успешно
    """
    payload = {"method": method, "period": int(period)}
    old = _read_json(WAREHOUSE_PREFS_PATH) or _read_json(LEGACY_PREFS_PATH) or {}
    old_period = int(old.get("period") or 90)

    success1 = _write_json(WAREHOUSE_PREFS_PATH, payload)
    success2 = _write_json(LEGACY_PREFS_PATH, payload)

    try:
        if CLEAR_ON_PERIOD_CHANGE and int(period) != old_period:
            from modules_shipments.shipments_demand_data import clear_demand_cache  # type: ignore

            clear_demand_cache()
            log.info(f"Period changed from {old_period} to {period}, cache cleared")
    except ImportError as e:
        log.warning(f"Could not import clear_demand_cache: {e}")
    except Exception as e:
        log.error(f"Error clearing demand cache: {e}", exc_info=True)

    return success1 and success2


# ---------- Стартовый роутер ----------
start_router = Router(name="start")


@start_router.message(CommandStart())
async def on_start(message: Message):
    # запоминаем текущий чат как «локальную цель» для автодайджестов/событий
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /start")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)
    await message.answer(_welcome_text(), reply_markup=_build_main_menu_kb())


@start_router.message(Command("help"))
async def on_help(message: Message):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /help")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    text = (
        "ℹ️ <b>Методики и возможности бота</b>\n\n"
        "📈 <b>План продаж</b>\n"
        "Бот строит прогноз спроса по каждому SKU несколькими способами:\n"
        "• <b>Скользящее среднее</b> (MA7/14/30/60/90/180/360) — усредняем продажи за выбранные дни.\n"
        "• <b>Экспоненциальное сглаживание</b> (ES, параметр α из .env) — свежие продажи влияют сильнее.\n"
        "<i>Пример:</i> если за последние 30 дней продали 300 шт, прогноз MA30 ≈ 10 шт/день.\n"
        "Выбрать метод можно командой <code>/method</code> — бот покажет список и текущую настройку.\n\n"
        "🏷️ <b>Рекомендации по выкупам</b>\n"
        "Необходимый объём зависит от плана на горизонт и коэффициента выкупа.\n"
        "Файл заявок Seller — <b>«Товары.xlsx»</b> — загрузите командой <code>/data</code>.\n\n"
        "🚚 <b>Рекомендации по отгрузкам</b>\n"
        "Цель — поддерживать комфортный запас по сети с учётом лагов L/S, планов и остатков.\n\n"
        "🏬 <b>Потребность складов</b> управляется в <code>/warehouse</code>.\n\n"
        "🔔 <b>Куда приходят уведомления</b>\n"
        "Бот присылает уведомления в этот чат. Чат автоматически запоминается после команд <code>/start</code> и <code>/notice</code>."
    )
    await message.answer(text, reply_markup=_home_kb(), parse_mode=ParseMode.HTML)


@start_router.message(Command("method"))
async def on_method(message: Message):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /method")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    current = get_forecast_method_title()
    text = (
        "⚙️ <b>Метод расчёта прогноза продаж</b>\n"
        f"Текущий: <b>{current}</b>\n\n"
        "Выберите новый метод:"
    )
    await message.answer(text, reply_markup=_build_method_kb(), parse_mode=ParseMode.HTML)


# ---------- /warehouse ----------


@start_router.message(Command("warehouse"))
async def on_warehouse(message: Message, state: FSMContext):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /warehouse")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    prefs = _load_wh_global()
    await state.update_data(wh_method=prefs["method"], wh_period=prefs["period"])
    txt = (
        "🏬 <b>Методика расчёта потребности по складам/кластерам</b>\n"
        f"Текущие глобальные настройки:\n"
        f"• Метод: <b>{WH_METHOD_TITLES[prefs['method']]}</b>\n"
        f"• Период: <b>{prefs['period']} дн.</b>\n\n"
        "Измените метод и/или период — изменения сохраняются сразу для всех пользователей."
    )
    await message.answer(
        txt,
        reply_markup=_build_warehouse_kb(prefs["method"], prefs["period"]),
        parse_mode=ParseMode.HTML,
    )


def _build_warehouse_kb(method: str, period: int) -> InlineKeyboardMarkup:
    m_rows = [
        [
            InlineKeyboardButton(
                text=("✓ " if method == "average" else "") + WH_METHOD_TITLES["average"],
                callback_data="wh:method:set:average",
            )
        ],
        [
            InlineKeyboardButton(
                text=("✓ " if method == "dynamics" else "") + WH_METHOD_TITLES["dynamics"],
                callback_data="wh:method:set:dynamics",
            )
        ],
        [
            InlineKeyboardButton(
                text=("✓ " if method == "hybrid" else "") + WH_METHOD_TITLES["hybrid"],
                callback_data="wh:method:set:hybrid",
            )
        ],
    ]
    p_rows = [
        [
            InlineKeyboardButton(
                text=("✓ " if period == 30 else "") + "30 дн.", callback_data="wh:period:set:30"
            ),
            InlineKeyboardButton(
                text=("✓ " if period == 60 else "") + "60 дн.", callback_data="wh:period:set:60"
            ),
            InlineKeyboardButton(
                text=("✓ " if period == 90 else "") + "90 дн.", callback_data="wh:period:set:90"
            ),
        ],
        [
            InlineKeyboardButton(
                text=("✓ " if period == 180 else "") + "180 дн.", callback_data="wh:period:set:180"
            ),
            InlineKeyboardButton(
                text=("✓ " if period == 360 else "") + "360 дн.", callback_data="wh:period:set:360"
            ),
        ],
    ]
    tail = [[InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")]]
    return InlineKeyboardMarkup(inline_keyboard=m_rows + p_rows + tail)


@start_router.callback_query(F.data.startswith("wh:method:set:"))
async def wh_set_method(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    method = cb.data.split(":")[-1]
    if method not in WH_METHODS:
        method = "average"
    data = await state.get_data()
    period = int(data.get("wh_period", 90))
    _save_wh_global(method, period)
    await state.update_data(wh_method=method)
    try:
        await cb.message.edit_reply_markup(reply_markup=_build_warehouse_kb(method, period))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


@start_router.callback_query(F.data.startswith("wh:period:set:"))
async def wh_set_period(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        period = int(cb.data.split(":")[-1])
    except (ValueError, IndexError) as e:
        log.warning(f"Invalid period in callback data '{cb.data}': {e}")
        period = 90
    if period not in WH_PERIODS:
        log.warning(f"Invalid period {period}, defaulting to 90")
        period = 90
    data = await state.get_data()
    method = data.get("wh_method", "average")
    _save_wh_global(method, period)
    await state.update_data(wh_period=period)
    try:
        await cb.message.edit_reply_markup(reply_markup=_build_warehouse_kb(method, period))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


# ===================  /notice  ===================

# Человеческие названия уведомлений
NOTICE_TITLES: Dict[str, str] = {
    # Новые позиции
    "goal_revenue_30d": "Необходимо заработать",
    "goal_units_30d": "Необходимо продать",
    "fact_units_yday": "Факт вчера (юниты)",
    "fact_revenue_yday": "Факт вчера (выручка)",
    "fact_avgcheck_yday": "Факт вчера (ср. чек)",
    "conversion_yday": "Конверсия вчера",
    "ctr_yday": "Кликабельность вчера",
    "plan_units_30d": "План 30д (юниты)",
    "plan_revenue_30d": "План 30д (выручка)",
    "plan_avgcheck_30d": "План 30д (ср. чек)",
    "need_to_purchase": "Необходимо закупить",
    "need_to_ship": "Необходимо отгрузить",
    "demand_by_sku": "Потребность по SKU",
    "delivery_stats": "Сроки доставки по SKU",
    "seller_reminder": "Напоминание об Excel (выкупы)",
}

_VS_ANYWHERE_RE = re.compile(r"\(\s*vs\s*3\s*0.*?\)", re.IGNORECASE)
_CTR_WORD_RE = re.compile(r"\bCTR\b", re.IGNORECASE)
_CR_WORD_RE = re.compile(r"\bCR\b|\bCVR\b", re.IGNORECASE)


def _normalize_notice_title(raw: str) -> str:
    s = (raw or "").strip()
    s = _CTR_WORD_RE.sub("Кликабельность", s)
    s = _CR_WORD_RE.sub("Конверсия", s)
    s = _VS_ANYWHERE_RE.sub("", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


# Порядок кнопок после «дайджестов»
NOTICE_ORDER: List[List[str]] = [
    ["goal_revenue_30d", "goal_units_30d"],
    ["need_to_purchase", "need_to_ship"],
    ["plan_units_30d", "fact_units_yday"],
    ["plan_revenue_30d", "fact_revenue_yday"],
    ["plan_avgcheck_30d", "fact_avgcheck_yday"],
    ["ctr_yday", "conversion_yday"],
    ["demand_by_sku", "delivery_stats"],
]

NOTICE_LABEL_OVERRIDES: Dict[str, str] = {
    "ctr_yday": "Кликабельность вчера",
    "conversion_yday": "Конверсия вчера",
}


def _label_for(code: str) -> str:
    base = NOTICE_LABEL_OVERRIDES.get(code) or NOTICE_TITLES.get(code, code)
    return _normalize_notice_title(base)


def _make_btn(code: str) -> Optional[InlineKeyboardButton]:
    if code not in NOTICE_REGISTRY:
        return None
    return InlineKeyboardButton(text=_label_for(code), callback_data=f"notice:send:{code}")


def _build_notice_kb() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    # ← Переименовано и добавлен эмодзи
    short_title = os.getenv("NOTICE_DIGEST_SHORT_TITLE", "🗞️ Сокращенный дайджест")
    rows.append([InlineKeyboardButton(text=short_title, callback_data="notice:send:short")])

    digest_title = os.getenv("NOTICE_DIGEST_TITLE", "📬 Весь утренний дайджест")
    rows.append([InlineKeyboardButton(text=digest_title, callback_data="notice:send:all")])

    for pair in NOTICE_ORDER:
        btns: List[InlineKeyboardButton] = []
        for code in pair:
            b = _make_btn(code)
            if b:
                btns.append(b)
        if btns:
            rows.append(btns)

    rows.append(
        [
            InlineKeyboardButton(
                text=_normalize_notice_title(NOTICE_TITLES["seller_reminder"]),
                callback_data="notice:send:seller_reminder",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="ℹ️ Список кодов", callback_data="notice:list")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _codes_list_text() -> str:
    items: List[str] = []
    for pair in NOTICE_ORDER:
        for code in pair:
            if code in NOTICE_REGISTRY:
                items.append(f"<code>{code}</code> — {_label_for(code)}")
    items.append(
        f"<code>seller_reminder</code> — {_normalize_notice_title(NOTICE_TITLES['seller_reminder'])}"
    )
    return (
        "🔔 <b>Доступные уведомления</b>\n" + "\n".join(f"• {ln}" for ln in items) + "\n\n"
        "Можно вызвать: <code>/notice &lt;код&gt;</code> или кнопкой ниже."
    )


@start_router.message(Command("notice"))
async def on_notice(message: Message):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /notice")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    parts = (message.text or "").split(maxsplit=1)
    chat_id = message.chat.id
    if len(parts) > 1:
        arg = parts[1].strip().lower()
        if arg in ("all", "digest"):
            await message.answer("📬 Показываю полный утренний дайджест здесь…")
            await send_digest_full(message.bot, chat_id=chat_id)
            return
        if arg in ("short", "mini", "brief"):
            await message.answer("🗞️ Показываю сокращённый дайджест здесь…")
            await send_digest_short(message.bot, chat_id=chat_id)
            return
        if arg in NOTICE_REGISTRY:
            await run_notice(message.bot, arg, chat_id=chat_id)
            await message.answer("✅ Готово")
            return
        if arg == "seller_reminder":
            await send_seller_reminder(message.bot, chat_id=chat_id)
            await message.answer("✅ Готово")
            return
        await message.answer(
            "❌ Неизвестный код.\n\n" + _codes_list_text(), parse_mode=ParseMode.HTML
        )
        return

    text = "🔔 <b>Уведомления</b>\nВыберите действие:"
    await message.answer(text, reply_markup=_build_notice_kb(), parse_mode=ParseMode.HTML)


@start_router.callback_query(F.data == "notice:list")
async def on_notice_list(cb: CallbackQuery):
    await _ack(cb)
    await _safe_edit_msg(
        cb, _codes_list_text(), reply_markup=_build_notice_kb(), parse_mode=ParseMode.HTML
    )


@start_router.callback_query(F.data == "nav:home")
async def on_nav_home(cb: CallbackQuery):
    await _ack(cb)
    await _safe_edit_msg(
        cb, _welcome_text(), reply_markup=_build_main_menu_kb(), parse_mode=ParseMode.HTML
    )


@start_router.callback_query(F.data == "notice:send:all")
async def on_notice_send_all(cb: CallbackQuery):
    await _ack(cb)
    try:
        chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
        if chat_id:
            register_notice_chat(chat_id)
            log.debug(f"Registered chat {chat_id} for notices on digest send")
    except Exception as e:
        log.error(f"Failed to register chat for digest: {e}", exc_info=True)

    try:
        if cb.message:
            await cb.message.answer("📬 Показываю полный утренний дайджест здесь…")
    except TelegramForbiddenError as e:
        log.warning(f"User blocked bot while sending digest: {e}")
    except TelegramBadRequest as e:
        log.warning(f"Bad request sending digest message: {e}")
    except Exception as e:
        log.error(f"Unexpected error sending digest message: {e}", exc_info=True)

    chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
    await send_digest_full(cb.bot, chat_id=chat_id)


@start_router.callback_query(F.data == "notice:send:short")
async def on_notice_send_short(cb: CallbackQuery):
    await _ack(cb)
    try:
        chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
        if chat_id:
            register_notice_chat(chat_id)
            log.debug(f"Registered chat {chat_id} for notices on short digest send")
    except Exception as e:
        log.error(f"Failed to register chat for short digest: {e}", exc_info=True)

    try:
        if cb.message:
            await cb.message.answer("🗞️ Показываю сокращённый дайджест здесь…")
    except TelegramForbiddenError as e:
        log.warning(f"User blocked bot while sending short digest: {e}")
    except TelegramBadRequest as e:
        log.warning(f"Bad request sending short digest message: {e}")
    except Exception as e:
        log.error(f"Unexpected error sending short digest message: {e}", exc_info=True)

    chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
    await send_digest_short(cb.bot, chat_id=chat_id)


@start_router.callback_query(F.data.startswith("notice:send:"))
async def on_notice_send_one(cb: CallbackQuery):
    await _ack(cb)
    try:
        chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
        if chat_id:
            register_notice_chat(chat_id)
            log.debug(f"Registered chat {chat_id} for notices on notice send")
    except Exception as e:
        log.error(f"Failed to register chat for notice: {e}", exc_info=True)

    code = cb.data.split(":")[-1]
    # обработчики для :all и :short уже есть выше
    if code in ("all", "short"):
        return

    chat_id = cb.message.chat.id if cb.message else (cb.from_user.id if cb.from_user else None)
    if code == "seller_reminder":
        await send_seller_reminder(cb.bot, chat_id=chat_id)
        try:
            if cb.message:
                await cb.message.answer("✅ Отправлено: Напоминание об Excel (выкупы)")
        except TelegramForbiddenError:
            log.warning(f"User blocked bot, cannot send confirmation for seller_reminder")
        except Exception as e:
            log.error(f"Error sending seller_reminder confirmation: {e}", exc_info=True)
        return

    if code not in NOTICE_REGISTRY:
        try:
            if cb.message:
                await cb.message.answer("❌ Неизвестный код уведомления")
        except Exception as e:
            log.error(f"Error sending unknown code message: {e}", exc_info=True)
        return

    await run_notice(cb.bot, code, chat_id=chat_id)
    try:
        if cb.message:
            await cb.message.answer(f"✅ Отправлено: {_label_for(code)}")
    except TelegramForbiddenError:
        log.warning(f"User blocked bot, cannot send confirmation for {code}")
    except Exception as e:
        log.error(f"Error sending confirmation for notice {code}: {e}", exc_info=True)


# /data — тот же сценарий загрузки, что и в разделе «Выкупы»


@start_router.message(Command("data"))
async def on_data(message: Message, state: FSMContext):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /data")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    await state.set_state(BuyoutsUpload.waiting_file)
    hint = (
        "🗂 <b>Загрузка файла «Товары.xlsx»</b>\n\n"
        "Пришлите файл Excel <i>как документ</i> (формат .xlsx).\n"
        f"Будет сохранён как <code>{PURCHASES_XLSX_NAME}</code> в папку <code>data/</code>.\n\n"
        "Поддерживаемые столбцы: SKU/Артикул, Статус, Кол-во. Город (Москва/Хабаровск) можно не указывать — "
        "если указан, значения суммируются по SKU."
    )
    await message.answer(hint, reply_markup=back_home_menu(), parse_mode=ParseMode.HTML)


# ---------- Fallback ----------


@start_router.message(StateFilter(None), ~F.text.regexp(r"^/"))
async def on_any_message(message: Message, state: FSMContext):
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on fallback")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    try:
        await state.clear()
    except Exception as e:
        log.warning(f"Failed to clear FSM state: {e}")

    await message.answer(_welcome_text(), reply_markup=_build_main_menu_kb())


# ---------- Команды ----------


async def setup_bot_commands(bot: Bot) -> None:
    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Главное меню"),
            BotCommand(command="units", description="Посмотреть перечень отслеживаемых юнитов"),
            BotCommand(command="method", description="Выбрать метод расчета прогноза продаж"),
            BotCommand(command="warehouse", description="Выбрать метод расчета потребности"),
            BotCommand(command="data", description="Загрузить «Товары.xlsx»"),
            BotCommand(command="notice", description="Показать уведомления"),
            BotCommand(command="help", description="Справка по возможностям"),
        ]
    )


# ---------- Показ стартового меню при запуске ----------


async def _startup_show_start_menu(bot: Bot) -> None:
    """
    Отправляет приветствие + главное меню в сохранённый «локальный» чат,
    либо во все чаты из CHAT_IDS, если локальный чат ещё не задан.
    """
    try:
        recipients: List[int] = []
        if NOTICES_PREFER_LOCAL:
            cid = _read_notice_chat()
            if cid:
                recipients = [cid]
        if not recipients:
            recipients = [c for c in (CHAT_IDS or []) if isinstance(c, int)]
        if not recipients:
            log.info(
                "Startup menu: нет сохранённого или сконфигурированного chat_id — ждём /start."
            )
            return

        text = _welcome_text()
        kb = _build_main_menu_kb()
        for cid in recipients:
            try:
                await bot.send_message(cid, text, reply_markup=kb, parse_mode=ParseMode.HTML)
                log.info(f"Startup menu: отправлено в чат {cid}")
            except TelegramForbiddenError as e:
                log.warning(f"Startup menu: доступ запрещён для чата {cid}: {e}")
            except TelegramBadRequest as e:
                log.warning(f"Startup menu: ошибка отправки в чат {cid}: {e}")
            except Exception as e:
                log.warning(f"Startup menu: сбой отправки в чат {cid}: {e}")
    except Exception as e:
        log.warning(f"Startup menu: общий сбой — {e}")


# ---------- Main ----------


async def main():
    if not TELEGRAM_TOKEN:
        raise RuntimeError("Не найден токен бота: задайте TELEGRAM_TOKEN (или BOT_TOKEN) в .env")

    bot = Bot(token=TELEGRAM_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()

    dp.include_router(start_router)
    dp.include_router(units_router)
    dp.include_router(handlers_router)

    await setup_bot_commands(bot)
    await scheduler_start(bot)

    # ← Показываем главное меню при запуске
    await _startup_show_start_menu(bot)

    log.info("Bot is starting polling…")
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass
