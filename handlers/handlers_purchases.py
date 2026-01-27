# handlers/handlers_purchases.py
from dotenv import load_dotenv
import os as _os
from modules_purchases.purchases_need_data import (
    load_dist_prefs,
    save_dist_prefs,
    reset_dist_prefs,
    list_warehouses_for_dist,
    get_city_config,  # ← используем конфиг городов
)
from modules_purchases import (
    need_to_purchase_text,
    purchases_status_text,
    ensure_purchases_template,
)
from menu import back_home_menu, buyouts_menu, main_menu, buyouts_need_menu
from aiogram.fsm.state import StatesGroup, State
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter, TelegramForbiddenError
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram import Router, F, Bot
import asyncio
import logging
import os
import re
import time
from typing import List, Tuple, Optional

# Логирование
log = logging.getLogger("seller-bot.purchases")

# Константы безопасности для загрузки файлов
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB максимальный размер файла
ALLOWED_MIME_TYPES = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.ms-excel",
    "application/octet-stream",
}


# UI-меню

# Отчёты раздела «Выкупы» (ленивый прокси из пакета — безопасно)

# Настройки распределения (хранилище + список складов)

# Пути и имя целевого файла
try:
    from modules_common.paths import DATA_DIR  # type: ignore
except ImportError as e:
    log.debug(f"Failed to import DATA_DIR from modules_common.paths: {e}")
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "data")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        log.info(f"Created fallback DATA_DIR: {DATA_DIR}")
    except OSError as e:
        log.error(f"Failed to create DATA_DIR {DATA_DIR}: {e}", exc_info=True)
except Exception as e:
    log.error(f"Unexpected error importing DATA_DIR: {e}", exc_info=True)
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "data")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
    except OSError:
        pass

load_dotenv(_os.path.join(_os.path.dirname(_os.path.dirname(__file__)), "..", ".env"))
PURCHASES_XLSX_NAME = _os.getenv("PURCHASES_XLSX_NAME", "Товары.xlsx")
TARGET_XLSX_PATH = _os.path.join(DATA_DIR, PURCHASES_XLSX_NAME)

# Имя/путь шаблона (можно переопределить через .env PURCHASES_TEMPLATE_NAME)
PURCHASES_TEMPLATE_NAME = _os.getenv("PURCHASES_TEMPLATE_NAME", "Товары_шаблон.xlsx")
TEMPLATE_XLSX_PATH = _os.path.join(DATA_DIR, PURCHASES_TEMPLATE_NAME)


def _env_bool(val: str | None, default: bool = True) -> bool:
    """
    Универсальный парсер булевых флагов из .env.
    Поддерживаем значения: 1/0, true/false, yes/no, on/off, да/нет.
    """
    if val is None or val == "":
        return default
    s = str(val).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "да", "истина"}:
        return True
    if s in {"0", "false", "no", "n", "off", "нет"}:
        return False
    return default


# Флаг управления светофорной индикацией в блоке «Необходимо закупить»
# PURCHASES_NEED_LIGHTS_ENABLED=1 (по умолчанию) — показывать светофор и легенду
# PURCHASES_NEED_LIGHTS_ENABLED=0           — убрать 🟥🟨🟩🟢🟠🔴✅ и блок «Обозначения: …»
PURCHASES_NEED_LIGHTS_ENABLED: bool = _env_bool(
    _os.getenv("PURCHASES_NEED_LIGHTS_ENABLED"),
    default=True,
)

router = Router()

# ───────── helpers ─────────


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs):
    attempt = 0
    while True:
        try:
            await cb.message.edit_text(text, **kwargs)
            return
        except TelegramRetryAfter as e:
            attempt += 1
            if attempt > 3:
                raise
            await asyncio.sleep(getattr(e, "retry_after", 1) + 1)
        except TelegramBadRequest as e:
            if "message is not modified" in str(e):
                return
            raise


async def _safe_answer(cb: CallbackQuery, *args, **kwargs):
    try:
        await cb.answer(*args, **kwargs)
    except TelegramBadRequest as e:
        if "query is too old" in str(e):
            return
        raise


def _greeting_text() -> str:
    return (
        "👋 Привет! Я помогу Вам спланировать продажи, выкупы и отгрузки.\n\n"
        "📈 В разделе «Продажи» — помогают понять динамику: что продаётся лучше, "
        "где растёт конверсия и насколько выполнен план.\n"
        "🏷️ В разделе «Выкупы» — показывают, какие товары нужно докупить и где сейчас ваши заказы "
        "в пути или обработке.\n"
        "🚚 В разделе «Отгрузки» — подсказывают, какие позиции пора отгрузить на склад, "
        "чтобы не потерять продажи и держать запас под контролем.\n\n"
        "Выберите раздел:"
    )


def _after_upload_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🏷️ Перейти в «Выкупы»", callback_data="buyouts")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _upload_kb() -> InlineKeyboardMarkup:
    """Клавиатура для экрана загрузки файла: скачать шаблон / назад / домой."""
    rows = [
        [InlineKeyboardButton(text="📥 Скачать шаблон", callback_data="purchases:template")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _strip_dist_button_kb(markup: InlineKeyboardMarkup | None) -> InlineKeyboardMarkup | None:
    """
    Убираем кнопку «⚙️ Распределение» (callback_data='buyouts:dist') из произвольной разметки.
    Нужно, чтобы не править общий модуль меню.
    """
    if not isinstance(markup, InlineKeyboardMarkup):
        return markup
    new_rows: List[List[InlineKeyboardButton]] = []
    for row in markup.inline_keyboard or []:
        row_new = [btn for btn in row if getattr(btn, "callback_data", "") != "buyouts:dist"]
        if row_new:
            new_rows.append(row_new)
    return InlineKeyboardMarkup(inline_keyboard=new_rows)


def _normalize_need_layout(text: str) -> str:
    """
    Нормализуем вёрстку блока «Необходимо закупить».

    Цель:
      phone_metal_stand_black
      👤 У Seller 1504 шт • 📦 У Ozon 678 шт
      📊 Необходимо 3150 шт
      🛒 Закупить 968 шт
    """
    markers = (
        "👤 У Seller",
        "📊 Необходимо",
        "🛒 Закупить",
    )

    for marker in markers:
        pattern = rf"(?<!\n){re.escape(marker)}"
        replacement = f"\n{marker}"
        text = re.sub(pattern, replacement, text)

    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def _strip_need_lights(text: str) -> str:
    """
    Убираем светофорную индикацию и легенду из блока «Необходимо закупить».
    """
    marker = "Обозначения:"
    idx = text.find(marker)
    if idx != -1:
        text = text[:idx].rstrip()
    for sign in ("🟥", "🟨", "🟩", "🟢", "🟠", "🔴", "✅"):
        text = text.replace(sign, "")
    while "  " in text:
        text = text.replace("  ", " ")
    return text


# ───────── состояния ─────────
class BuyoutsUpload(StatesGroup):
    waiting_file = State()


# Внутреннее состояние экрана распределения (без отдельного класса FSM)
DIST_PAGE_SIZE = 20


def _dist_title() -> str:
    cfg = get_city_config()
    c1 = cfg.get("city1", "Город1")
    c2 = cfg.get("city2", "Город2")
    return (
        "⚙️ <b>Распределение</b>\n"
        f"Отметьте, какие склады относятся к <b>{c1}</b> и какие — к <b>{c2}</b>.\n"
        "Один склад не может находиться в обеих группах одновременно.\n"
        "Нажмите «💾 Сохранить», чтобы применить настройки.\n"
    )


def _dist_disabled_text() -> str:
    cfg = get_city_config()
    c1 = cfg.get("city1", "Город")
    return (
        "⚙️ <b>Распределение</b>\n\n"
        f"В конфигурации выбран <b>один город</b> — <b>{c1}</b>. "
        "Распределение между городами в этом режиме недоступно."
    )


def _dedup_names(names: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    """Защита в UI: удаляем дубликаты по warehouse_id."""
    seen: set[int] = set()
    uniq: List[Tuple[int, str]] = []
    for wid, wname in names or []:
        try:
            w = int(wid)
        except (ValueError, TypeError) as e:
            log.warning(f"Invalid warehouse ID {wid}: {e}")
            continue
        if w in seen:
            continue
        uniq.append((w, str(wname)))
        seen.add(w)
    return uniq


def _to_int_set(values) -> set[int]:
    """Безопасно приводим коллекцию ID к множеству int."""
    out: set[int] = set()
    for v in values or []:
        try:
            out.add(int(v))
        except (ValueError, TypeError) as e:
            log.warning(f"Failed to convert value to int: {v}, error: {e}")
            continue
    return out


def _dist_kb(
    page: int, city1_ids: set[int], city2_ids: set[int], names: List[Tuple[int, str]]
) -> InlineKeyboardMarkup:
    # Дополнительная дедупликация на всякий случай
    names = _dedup_names(names)

    cfg = get_city_config()
    c1 = cfg.get("city1", "Город1")
    c2 = cfg.get("city2", "Город2")

    start = max(0, page) * DIST_PAGE_SIZE
    end = start + DIST_PAGE_SIZE
    chunk = names[start:end]

    rows: List[List[InlineKeyboardButton]] = []
    for wid, wname in chunk:
        c1_mark = "✅" if wid in city1_ids else "⭕"
        c2_mark = "✅" if wid in city2_ids else "⭕"
        rows.append([InlineKeyboardButton(text=f"{wname}", callback_data="noop")])
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{c1_mark} {c1}", callback_data=f"buy:dist:toggle:m:{wid}:{page}"
                ),
                InlineKeyboardButton(
                    text=f"{c2_mark} {c2}", callback_data=f"buy:dist:toggle:h:{wid}:{page}"
                ),
            ]
        )

    nav: List[InlineKeyboardButton] = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад",                                            callback_data=f"buy:dist:page:{page - 1}"))
    if end < len(names):
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"buy:dist:page:{
                    page + 1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="💾 Сохранить", callback_data="buy:dist:save")])
    rows.append([InlineKeyboardButton(text="♻️ Сбросить", callback_data="buy:dist:reset")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────── Выкупы — корень ─────────
@router.callback_query(F.data == "buyouts")
async def on_buyouts_root(cb: CallbackQuery, state: FSMContext):
    """Корень раздела «🏷️ Выкупы»."""
    await state.clear()
    await state.update_data(step="buyouts_root")
    await _safe_edit(cb, "🏷️ Выкупы — выберите действие:", reply_markup=buyouts_menu())
    await _safe_answer(cb)


# «Необходимо закупить» — считаем по ЦЕЛИ продаж
@router.callback_query(F.data == "buyouts:need")
async def on_buyouts_need(cb: CallbackQuery, state: FSMContext):
    # Явно включаем goal-mode, чтобы меню всегда считало «как в меню» (по цели)
    text = need_to_purchase_text(use_goal=True)

    # 1) нормализуем вёрстку блоков по SKU
    text = _normalize_need_layout(text)

    # 2) заменяем иконку для Ozon: 🛒 → 📦 (на случай старых версий отчёта)
    text = text.replace("🛒 У Ozon", "📦 У Ozon")

    # 3) при необходимости убираем светофорную индикацию и легенду
    if not PURCHASES_NEED_LIGHTS_ENABLED:
        text = _strip_need_lights(text)

    await state.update_data(step="buyouts_need")

    # Если один город — удаляем кнопку распределения из подменю
    cfg = get_city_config()
    kb = buyouts_need_menu()
    if int(cfg.get("count", 2)) == 1:
        kb = _strip_dist_button_kb(kb)

    await _safe_edit(cb, text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await _safe_answer(cb)


# «Статус выкупов» — поддерживаем СТАРЫЙ и НОВЫЙ коллбек
@router.callback_query(F.data.in_({"buyouts:status", "buyouts:inprogress"}))
async def on_buyouts_status(cb: CallbackQuery, state: FSMContext):
    text = purchases_status_text()
    await state.update_data(step="buyouts_status")
    await _safe_edit(cb, text, reply_markup=back_home_menu(), parse_mode=ParseMode.HTML)
    await _safe_answer(cb)


# ───────── ⚙️ Распределение — экран ─────────
@router.callback_query(F.data == "buyouts:dist")
async def on_buyouts_dist(cb: CallbackQuery, state: FSMContext):
    # Если включён режим одного города — срываем возможность распределения
    cfg = get_city_config()
    if int(cfg.get("count", 2)) == 1:
        await _safe_edit(
            cb, _dist_disabled_text(), parse_mode=ParseMode.HTML, reply_markup=back_home_menu()
        )
        await _safe_answer(cb)
        return

    prefs = load_dist_prefs()
    city1 = _to_int_set(prefs.get("moscow_wids") or prefs.get("city1_wids"))
    city2 = _to_int_set(prefs.get("khabarovsk_wids") or prefs.get("city2_wids"))
    names = list_warehouses_for_dist()
    await state.update_data(
        step="buyouts_dist", dist_moscow=list(city1), dist_khab=list(city2), dist_page=0
    )
    kb = _dist_kb(0, city1, city2, names)
    await _safe_edit(cb, _dist_title(), parse_mode=ParseMode.HTML, reply_markup=kb)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("buy:dist:page:"))
async def on_buyouts_dist_page(cb: CallbackQuery, state: FSMContext):
    cfg = get_city_config()
    if int(cfg.get("count", 2)) == 1:
        await _safe_edit(
            cb, _dist_disabled_text(), parse_mode=ParseMode.HTML, reply_markup=back_home_menu()
        )
        await _safe_answer(cb)
        return

    try:
        page = int(cb.data.split(":")[-1])
    except (ValueError, IndexError) as e:
        log.warning(f"Invalid page in callback data '{cb.data}': {e}")
        page = 0
    data = await state.get_data()
    city1 = _to_int_set(data.get("dist_moscow"))
    city2 = _to_int_set(data.get("dist_khab"))
    names = list_warehouses_for_dist()
    await state.update_data(dist_page=page)
    await _safe_edit(
        cb,
        _dist_title(),
        parse_mode=ParseMode.HTML,
        reply_markup=_dist_kb(page, city1, city2, names),
    )
    await _safe_answer(cb)


@router.callback_query(F.data.regexp(r"^buy:dist:toggle:(m|h):\d+:\d+$"))
async def on_buyouts_dist_toggle(cb: CallbackQuery, state: FSMContext):
    cfg = get_city_config()
    if int(cfg.get("count", 2)) == 1:
        await _safe_edit(
            cb, _dist_disabled_text(), parse_mode=ParseMode.HTML, reply_markup=back_home_menu()
        )
        await _safe_answer(cb)
        return

    _, _, _, grp, wid_str, page_str = cb.data.split(":")
    wid = int(wid_str)
    page = int(page_str)
    data = await state.get_data()
    city1 = _to_int_set(data.get("dist_moscow"))
    city2 = _to_int_set(data.get("dist_khab"))
    # взаимоисключаем группы
    if grp == "m":
        if wid in city1:
            city1.remove(wid)
        else:
            city1.add(wid)
            city2.discard(wid)
    else:
        if wid in city2:
            city2.remove(wid)
        else:
            city2.add(wid)
            city1.discard(wid)
    await state.update_data(
        dist_moscow=sorted(list(city1)), dist_khab=sorted(list(city2)), dist_page=page
    )
    names = list_warehouses_for_dist()
    await _safe_edit(
        cb,
        _dist_title(),
        parse_mode=ParseMode.HTML,
        reply_markup=_dist_kb(page, city1, city2, names),
    )
    await _safe_answer(cb)


@router.callback_query(F.data == "buy:dist:save")
async def on_buyouts_dist_save(cb: CallbackQuery, state: FSMContext):
    """
    Сохраняем распределение и СРАЗУ выводим обновлённые рекомендации «Необходимо закупить»
    с учётом выбранных складов и их ΣD/день.
    """
    cfg = get_city_config()
    if int(cfg.get("count", 2)) == 1:
        await _safe_edit(
            cb, _dist_disabled_text(), parse_mode=ParseMode.HTML, reply_markup=back_home_menu()
        )
        await _safe_answer(cb)
        return

    data = await state.get_data()
    city1 = sorted(list(_to_int_set(data.get("dist_moscow"))))
    city2 = sorted(list(_to_int_set(data.get("dist_khab"))))
    save_dist_prefs(city1, city2)
    await _safe_answer(cb, "Сохранено")

    # Обновляем текущий экран «Распределение» (визуальное подтверждение)
    names = list_warehouses_for_dist()
    page = int(data.get("dist_page") or 0)
    await _safe_edit(
        cb,
        _dist_title() + "\n✅ Настройки сохранены.",
        parse_mode=ParseMode.HTML,
        reply_markup=_dist_kb(page, set(city1), set(city2), names),
    )

    # ⤵️ Дополнительно отправляем пересчитанные рекомендации «Необходимо закупить»
    #    — они учитывают сохранённые prefs и ΣD/день по складам
    need_text = need_to_purchase_text(use_goal=True)
    need_text = _normalize_need_layout(need_text).replace("🛒 У Ozon", "📦 У Ozon")
    if not PURCHASES_NEED_LIGHTS_ENABLED:
        need_text = _strip_need_lights(need_text)

    # Если один город — убираем кнопку «⚙️ Распределение» из меню рекомендаций (хотя тут 2 города)
    kb = buyouts_need_menu()
    if int(cfg.get("count", 2)) == 1:
        kb = _strip_dist_button_kb(kb)

    await cb.message.answer(need_text, parse_mode=ParseMode.HTML, reply_markup=kb)


@router.callback_query(F.data == "buy:dist:reset")
async def on_buyouts_dist_reset(cb: CallbackQuery, state: FSMContext):
    cfg = get_city_config()
    if int(cfg.get("count", 2)) == 1:
        await _safe_edit(
            cb, _dist_disabled_text(), parse_mode=ParseMode.HTML, reply_markup=back_home_menu()
        )
        await _safe_answer(cb)
        return

    reset_dist_prefs()
    await state.update_data(dist_moscow=[], dist_khab=[], dist_page=0)
    names = list_warehouses_for_dist()
    await _safe_edit(
        cb,
        _dist_title() + "\n♻️ Настройки сброшены.",
        parse_mode=ParseMode.HTML,
        reply_markup=_dist_kb(0, set(), set(), names),
    )
    await _safe_answer(cb)


# Лейбл-«пустышка» в списке складов, чтобы не подвисал спиннер
@router.callback_query(F.data == "noop")
async def on_noop(cb: CallbackQuery):
    await _safe_answer(cb)


# ───────── Загрузка «Товары.xlsx» ─────────
@router.callback_query(F.data.in_({"buyouts:upload", "purchases:upload"}))
async def on_buyouts_upload_start(cb: CallbackQuery, state: FSMContext):
    """Показываем инструкцию и переводим в состояние ожидания файла."""
    await state.set_state(BuyoutsUpload.waiting_file)
    hint = (
        "🗂 <b>Загрузка файла «Товары.xlsx»</b>\n\n"
        "Пришлите файл Excel <i>как документ</i> (формат .xlsx).\n"
        f"Будет сохранён как <code>{PURCHASES_XLSX_NAME}</code> в папку <code>data/</code>.\n\n"
        "Поддерживаемые столбцы: SKU/Артикул, Статус, Кол-во. Город (в отдельной колонке) можно не указывать — "
        "если указан, значения суммируются по SKU.\n\n"
        "📥 <b>Скачать шаблон</b> — кнопкой ниже. Шаблон автоматически содержит все SKU из .env "
        "(WATCH_SKU / ALIAS_*). При добавлении новых юнитов в .env он обновится."
    )
    await _safe_edit(cb, hint, parse_mode=ParseMode.HTML, reply_markup=_upload_kb())
    await _safe_answer(cb)


@router.callback_query(F.data == "purchases:template")
async def on_buyouts_template(cb: CallbackQuery, state: FSMContext):
    """
    Генерируем (или пересобираем) шаблон по .env и отправляем как документ.
    """
    try:
        path = ensure_purchases_template(TEMPLATE_XLSX_PATH)
        doc = FSInputFile(path, filename=os.path.basename(path))
        await cb.message.answer_document(
            document=doc,
            caption=(
                "📄 Шаблон «Товары.xlsx».\n"
                "Заполните количества по статусам и отправьте файл сюда <i>как документ</i>."
            ),
            parse_mode=ParseMode.HTML,
        )
        log.info(f"Template sent to chat {cb.message.chat.id}")
    except FileNotFoundError as e:
        log.error(f"Template file not found: {e}", exc_info=True)
        await cb.message.answer("😔 Шаблон не найден. Попробуйте еще раз.")
    except PermissionError as e:
        log.error(f"Permission denied accessing template: {e}", exc_info=True)
        await cb.message.answer("😔 Нет доступа к файлу шаблона.")
    except Exception as e:
        log.error(f"Unexpected error generating template: {e}", exc_info=True)
        await cb.message.answer("😔 Не удалось сформировать шаблон. Попробуйте ещё раз позже.")
    await _safe_answer(cb)


@router.message(BuyoutsUpload.waiting_file, F.document)
async def on_buyouts_upload_file(message: Message, state: FSMContext, bot: Bot):
    """
    Принимаем документ и сохраняем его как data/<PURCHASES_XLSX_NAME>,
    затем принудительно обновляем кэш purchases_report_data.load_excel(force=True).
    """
    # Проверяем rate limit для загрузки файлов
    if not _file_upload_limiter.can_upload(message.chat.id):
        reset_seconds = _file_upload_limiter.get_reset_time(message.chat.id)
        await message.answer(
            f"⚠️ Слишком много загрузок. Подождите {reset_seconds} секунд.\n"
            "Лимит: 3 файла в минуту."
        )
        return

    doc = message.document

    # 1. Проверяем расширение
    fname = (doc.file_name or "").lower()
    if not fname.endswith(".xlsx"):
        await message.answer("⚠️ Нужен файл Excel с расширением .xlsx. Попробуйте ещё раз.")
        return

    # 2. Проверяем размер файла
    if doc.file_size and doc.file_size > MAX_FILE_SIZE:
        await message.answer(
            f"⚠️ Файл слишком большой: {doc.file_size / 1024 / 1024:.1f} МБ\n"
            f"Максимальный размер: {MAX_FILE_SIZE / 1024 / 1024} МБ"
        )
        return

    # 3. Проверяем mime-type (если доступен)
    if doc.mime_type and doc.mime_type not in ALLOWED_MIME_TYPES:
        log.warning(f"Blocked file upload with mime-type: {doc.mime_type}")
        await message.answer("⚠️ Неверный формат файла. Пришлите Excel файл (.xlsx).")
        return

    log.info(f"Processing file upload: {doc.file_name} ({doc.file_size} bytes, {doc.mime_type})")

    os.makedirs(DATA_DIR, exist_ok=True)
    tmp_path = os.path.join(DATA_DIR, f"._upload_{int(time.time())}.xlsx")

    # скачиваем файл
    try:
        await bot.download(doc, destination=tmp_path)  # aiogram v3
        log.debug(f"File downloaded to {tmp_path}")
    except TelegramRetryAfter as e:
        log.warning(f"Rate limited while downloading file, retry in {e.retry_after} seconds")
        await message.answer("😔 Слишком много запросов. Подождите немного и попробуйте ещё раз.")
        return
    except (TelegramBadRequest, TelegramForbiddenError) as e:
        log.error(f"Telegram API error downloading file: {e}", exc_info=True)
        await message.answer("😔 Ошибка скачивания файла. Попробуйте ещё раз.")
        return
    except Exception as e:
        log.error(f"Unexpected error downloading file with bot.download(): {e}", exc_info=True)
        # Fallback: пробуем через get_file + download_file
        try:
            file = await bot.get_file(doc.file_id)
            await bot.download_file(file.file_path, tmp_path)
            log.debug(f"File downloaded via fallback to {tmp_path}")
        except Exception as fallback_e:
            log.error(f"Fallback download also failed: {fallback_e}", exc_info=True)
            await message.answer("😔 Не удалось скачать файл. Пришлите документ ещё раз.")
            return

    # перемещаем в целевое имя
    try:
        if os.path.exists(TARGET_XLSX_PATH):
            os.replace(tmp_path, TARGET_XLSX_PATH)
        else:
            os.rename(tmp_path, TARGET_XLSX_PATH)
        log.info(f"File saved to {TARGET_XLSX_PATH}")
    except PermissionError as e:
        log.error(f"Permission denied saving file: {e}", exc_info=True)
        await message.reply("😔 Нет прав доступа для сохранения файла.")
        return
    except (OSError, IOError) as e:
        log.error(f"OS error saving file: {e}", exc_info=True)
        await message.reply("😔 Ошибка при сохранении файла на диск.")
        return
    except Exception as e:
        log.error(f"Unexpected error saving file: {e}", exc_info=True)
        await message.reply("😔 Не удалось сохранить файл на диск.")
        return

    # форс-обновление кэша — тем же парсером, что используют отчёты
    try:
        from modules_purchases.purchases_report_data import load_excel as _refresh

        _ = _refresh(force=True)
        log.info("Excel cache refreshed successfully")
    except ImportError as e:
        log.error(f"Failed to import load_excel: {e}", exc_info=True)
    except Exception as e:
        log.error(f"Failed to refresh Excel cache: {e}", exc_info=True)
        # Не прерываем процесс, даже если кэш не обновился

    await state.clear()
    await message.answer(
        f"✅ Файл сохранён: <code>{PURCHASES_XLSX_NAME}</code>.\n"
        "Отчёты «Необходимо закупить» и «Статус выкупов» теперь используют свежие данные.",
        reply_markup=_after_upload_kb(),
        parse_mode=ParseMode.HTML,
    )


# ───────── Навигация: Назад / Домой ─────────
@router.callback_query(F.data == "nav:back")
async def on_back(cb: CallbackQuery, state: FSMContext):
    """
    Универсальный «Назад» для раздела Выкупы.
      • из экранов need/status/dist/calc/upload → назад к меню выкупов
      • из корня выкупов → на главный экран
    """
    data = await state.get_data()
    step = data.get("step")
    await state.clear()

    if step in {"buyouts_need", "buyouts_status", "buyouts_dist", "buyouts_calc", "buyouts_root"}:
        await _safe_edit(cb, "🏷️ Выкупы — выберите действие:", reply_markup=buyouts_menu())
    else:
        await _safe_edit(cb, _greeting_text(), reply_markup=main_menu())

    await _safe_answer(cb)


@router.callback_query(F.data == "nav:home")
async def on_home(cb: CallbackQuery, state: FSMContext):
    """Глобальная кнопка «Домой»."""
    await state.clear()
    await _safe_edit(cb, _greeting_text(), reply_markup=main_menu())
    await _safe_answer(cb)


# ───────── Rate Limiting ─────────
class FileUploadRateLimiter:
    """Простой rate limiter для загрузки файлов."""

    def __init__(self, max_uploads_per_minute: int = 3):
        self.max_uploads = max_uploads_per_minute
        self.uploads: dict[int, list[float]] = {}  # chat_id -> list of timestamps

    def can_upload(self, chat_id: int) -> bool:
        """Проверяет, может ли пользователь загрузить файл."""
        now = time.time()
        minute_ago = now - 60

        # Удаляем старые записи
        if chat_id in self.uploads:
            self.uploads[chat_id] = [ts for ts in self.uploads[chat_id] if ts > minute_ago]
        else:
            self.uploads[chat_id] = []

        # Проверяем лимит
        if len(self.uploads[chat_id]) >= self.max_uploads:
            return False

        self.uploads[chat_id].append(now)
        return True

    def get_reset_time(self, chat_id: int) -> int:
        """Возвращает секунды до сброса лимита."""
        if chat_id not in self.uploads or not self.uploads[chat_id]:
            return 0

        oldest = min(self.uploads[chat_id])
        reset_time = oldest + 60 - time.time()
        return max(0, int(reset_time))


# Глобальный инстанс limiter'а для загрузки файлов
_file_upload_limiter = FileUploadRateLimiter(max_uploads_per_minute=3)
