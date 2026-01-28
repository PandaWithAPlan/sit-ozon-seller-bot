# handlers/handlers_sales.py
from modules_common.calendar import (
    show_calendar,
    handle_date_switch as calendar_handle_date_switch,
    handle_date_nav as calendar_handle_date_nav,
    handle_date_clear as calendar_handle_date_clear,
    handle_date_pick as calendar_handle_date_pick,
)
import os
import math
import time
import asyncio
from datetime import date
from typing import List, Tuple, Dict

from aiogram import Router, F, Bot
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.enums import ParseMode
from aiogram.fsm.state import StatesGroup, State

from menu import (
    sales_menu,
    sales_goal_menu,
    sales_goal_report_menu,
    facts_metric_menu,
    facts_period_menu,
    plan_metric_menu,
    plan_period_menu,
    back_home_menu,
)

# ⬇️ Обновлённые пути по структуре ТЗ 5.0/5.1
from modules_sales.sales_facts_store import facts_text, _fmt_units
from modules_sales.sales_forecast import (
    forecast_text,
    list_forecast_methods,
    get_forecast_method_title,
    set_forecast_method,
    get_forecast_method,  # ← текущий код метода
    ES_ALPHA,  # ← для короткой подписи ES в клавиатуре
    _fetch_series_from_api as _fc_fetch_series,  # для Факт/План по SKU
    calculate_forecast as _fc_forecast_next,  # для Плана-30 по SKU
)
from modules_sales.sales_traffic import traffic_text
from modules_sales.sales_report import list_skus, sales_report_text

# 🎯 Цель продаж — расчёты/хранилище
from modules_sales.sales_goal import (
    sales_goal_report_text,
    get_goal_per_day_by_sku,
    set_goal_per_day,
    reset_goal_per_day,
)

# 🧾 Отчёт по выкупам — подраздел «Продажи»
from modules_sales.sales_buyout import (
    classify_excel as buyout_classify,
    process_files as buyout_process,
    get_help_text as buyout_help,
)

# Пути (DATA_DIR) — понадобятся для сохранения загрузок
try:
    from modules_common.paths import DATA_DIR  # type: ignore
except Exception:
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)


CALENDAR_PREFIX_SALES_REPORT = "srep"

router = Router()


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


async def _safe_edit_kb(cb: CallbackQuery, reply_markup):
    """Безопасная замена inline-клавиатуры (игнорируем 'message is not modified')."""
    try:
        await cb.message.edit_reply_markup(reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


def _fmt_alpha(a: float) -> str:
    s = f"{float(a):.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные клавиатуры и перерисовка блока Плана
# ──────────────────────────────────────────────────────────────────────────────
def _plan_result_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="⚙️ Метод прогноза", callback_data="plan:method:open")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="plan:back_to_metrics")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _plan_methods_kb() -> InlineKeyboardMarkup:
    """
    Клавиатура выбора метода прогноза с галочкой на текущем методе.
    Раскладка:
        1-я строка: 7 / 14
        2-я строка: 30 / 60
        3-я строка: 90 / 180
        4-я строка: 360 / Эксп. сглаживание (α -> число из .env)
    """
    current_code = get_forecast_method()
    methods = dict(list_forecast_methods())  # code -> title (для ES — полное название)

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
        [InlineKeyboardButton(text="⬅️ Назад к плану", callback_data="plan:method:close")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_plan_result(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    metric = data.get("plan_metric", "units")
    period_days = int(data.get("plan_period_days", 1))
    text = forecast_text(period_days=period_days, metric=metric)
    await _safe_edit(cb, text, reply_markup=_plan_result_kb())


# ──────────────────────────────────────────────────────────────────────────────
# 🎯 Цель продаж — вспомогательные функции и клавиатуры
# ──────────────────────────────────────────────────────────────────────────────
GOAL_EDIT_PAGE_SIZE = 20


def _watch_skus_order() -> List[int]:
    """
    Порядок SKU из .env (WATCH_SKU). Поддерживает форматы:
      WATCH_SKU="123,124, 125" или "123:alias,124:alias"
    Возвращает список SKU без дубликатов, в исходном порядке.
    """
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",").replace(" ", ",")
    out: List[int] = []
    seen: set[int] = set()
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        left = token.split(":")[0].strip()
        try:
            s = int(left)
        except Exception:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _sort_rows_by_env(rows: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    order = _watch_skus_order()
    if not order:
        return rows
    pos = {sku: i for i, sku in enumerate(order)}
    # неизвестные SKU — в конец, по имени
    return sorted(rows, key=lambda it: (pos.get(int(it[0]), 10**9), str(it[1]).lower()))


def _sum_last_n_units(seq: List[Tuple], n: int) -> float:
    if not seq:
        return 0.0
    tail = seq[-max(1, int(n)) :]
    total = 0.0
    for t in tail:
        try:
            total += float(t[1] or 0.0)
        except Exception:
            continue
    return total


def _fact_plan30_units_for_sku(sku: int) -> Tuple[float, float]:
    """
    Возвращает (Факт 30, План 30) по юнитам для SKU.
    """
    try:
        series_map = _fc_fetch_series(max(60, 180)) or {}
    except TypeError:
        series_map = _fc_fetch_series(days_back=max(60, 180)) or {}
    except Exception:
        series_map = {}
    seq = series_map.get(int(sku)) or []
    fact30 = _sum_last_n_units(seq, 30)
    try:
        plan30, _ = _fc_forecast_next(seq, 30)
    except Exception:
        plan30 = 0.0
    return float(fact30 or 0.0), float(plan30 or 0.0)


def _fact_plan1_units_for_sku(sku: int) -> Tuple[float, float]:
    """
    Возвращает (Факт день, План день) по юнитам для SKU.
    Используем средние за 30 дней: Факт30/30 и План30/30.
    """
    fact30, plan30 = _fact_plan30_units_for_sku(sku)
    return float(fact30) / 30.0, float(plan30) / 30.0


def _fmt_units_per_day(v: float) -> str:
    try:
        return f"{float(v):.1f} шт/д"
    except Exception:
        return "0.0 шт/д"


def _goal_edit_list_kb(
    sku_rows: List[Tuple[int, str]],
    page: int,
    goals: Dict[int, float],
) -> InlineKeyboardMarkup:
    n = len(sku_rows)
    pages = max(1, (n + GOAL_EDIT_PAGE_SIZE - 1) // GOAL_EDIT_PAGE_SIZE)
    p = max(0, min(page, pages - 1))
    start, end = p * GOAL_EDIT_PAGE_SIZE, min(n, (p + 1) * GOAL_EDIT_PAGE_SIZE)

    rows: List[List[InlineKeyboardButton]] = []
    for sku, name in sku_rows[start:end]:
        g = float(goals.get(int(sku), 0.0))
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{name} — {g:.2f} шт/д", callback_data=f"sales:goal:edit:sku:{sku}:{p}"
                )
            ]
        )

    nav: List[InlineKeyboardButton] = []
    if p > 0:
        nav.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"sales:goal:edit:list:{p - 1}")
        )
    if p < pages - 1:
        nav.append(
            InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"sales:goal:edit:list:{p + 1}")
        )
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sales:goal")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _goal_edit_item_kb(sku: int, page: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(
                text="✏️ Внести изменения", callback_data=f"sales:goal:edit:start:{sku}:{page}"
            )
        ],
        [
            InlineKeyboardButton(
                text="♻️ Сбросить в 0", callback_data=f"sales:goal:edit:reset:{sku}:{page}"
            )
        ],
        [
            InlineKeyboardButton(
                text="⬅️ Назад к списку", callback_data=f"sales:goal:edit:list:{page}"
            )
        ],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_goal_report(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    metric = (data.get("goal_metric") or "units").strip().lower()
    horizon = int(data.get("goal_horizon") or 30)
    horizon = 1 if horizon == 1 else 30
    text = sales_goal_report_text(horizon_days=horizon, metric=metric)
    await _safe_edit(cb, text, reply_markup=sales_goal_report_menu(horizon=horizon, metric=metric))


# FSM для ввода числового значения цели
class SalesGoalEdit(StatesGroup):
    waiting_value = State()


# ——— Продажи — уровень 1
@router.callback_query(F.data == "sales")
async def on_sales(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="sales_root")
    await _safe_edit(cb, "📈 Продажи — выберите раздел:", reply_markup=sales_menu())
    await _safe_answer(cb)


# ──────────────────────────────────────────────────────────────────────────────
# 🎯 Цель продаж — навигация и отчёты
# ──────────────────────────────────────────────────────────────────────────────
@router.callback_query(F.data == "sales:goal")
async def on_sales_goal_root(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="sales_goal_root", goal_metric="units", goal_horizon=30)
    await _safe_edit(cb, "🎯 Цель продаж — выберите метрику:", reply_markup=sales_goal_menu())
    await _safe_answer(cb)


@router.callback_query(F.data == "sales:goal:report:units")
async def on_sales_goal_units(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="sales_goal_report", goal_metric="units", goal_horizon=30)
    await _render_goal_report(cb, state)
    await _safe_answer(cb)


@router.callback_query(F.data == "sales:goal:report:revenue")
async def on_sales_goal_revenue(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="sales_goal_report", goal_metric="revenue", goal_horizon=30)
    await _render_goal_report(cb, state)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("sales:goal:horizon:"))
async def on_sales_goal_horizon(cb: CallbackQuery, state: FSMContext):
    h = int(cb.data.split(":")[-1])
    await state.update_data(goal_horizon=1 if h == 1 else 30)
    await _render_goal_report(cb, state)
    await _safe_answer(cb)


# — Изменение целей: список SKU
@router.callback_query(F.data.startswith("sales:goal:edit:list:"))
async def on_sales_goal_edit_list(cb: CallbackQuery, state: FSMContext):
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0

    rows = list_skus() or []  # List[Tuple[int, str]]
    if not rows:
        await _safe_edit(cb, "Список SKU пуст.", reply_markup=back_home_menu())
        await _safe_answer(cb)
        return

    # ⛔️ Фильтруем только те SKU, что есть в WATCH_SKU, и подставляем ALIAS
    order = _watch_skus_order()
    order_set = set(order)
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku  # ленивый импорт

        rows = [
            (int(s), (get_alias_for_sku(int(s)) or str(s)))
            for s, _name in rows
            if int(s) in order_set
        ]
    except Exception:
        rows = [(int(s), str(s)) for s, _name in rows if int(s) in order_set]

    # 🔢 Сортируем как в .env (WATCH_SKU)
    rows = _sort_rows_by_env(rows)

    goals = get_goal_per_day_by_sku()
    kb = _goal_edit_list_kb(rows, page, goals)
    await state.update_data(step="sales_goal_edit_list", goal_edit_page=page)
    await _safe_edit(cb, "✏️ Изменение целей — выберите SKU:", reply_markup=kb)
    await _safe_answer(cb)


# — Карточка SKU
@router.callback_query(F.data.startswith("sales:goal:edit:sku:"))
async def on_sales_goal_edit_item(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    # sales:goal:edit:sku:<sku>:<page>
    if len(parts) < 6:
        await _safe_answer(cb)
        return
    try:
        sku = int(parts[4])
    except Exception:
        await _safe_answer(cb)
        return
    try:
        page = int(parts[5])
    except Exception:
        page = 0

    goals = get_goal_per_day_by_sku()
    g = float(goals.get(int(sku), 0.0))

    # 📈 Подтянуть факт/план «за день» (средние за 30 дней)
    fact1_u, plan1_u = _fact_plan1_units_for_sku(sku)

    alias = None
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore

        alias = (get_alias_for_sku(sku) or str(sku)).strip()
    except Exception:
        alias = str(sku)

    text = (
        f"🎯 Изменение цели для <b>{alias}</b>\n"
        f"📦 Факт день: {_fmt_units_per_day(fact1_u)} • 🧮 План: {_fmt_units_per_day(plan1_u)}\n\n"
        f"Текущая цель: <b>{g:.2f} шт/д</b>\n\n"
        "Доступно:\n"
        "• «✏️ Внести изменения» — введите новое значение (число, шт/д)\n"
        "• «♻️ Сбросить в 0» — обнулить цель\n"
    )
    await state.update_data(step="sales_goal_edit_item", goal_edit_sku=sku, goal_edit_page=page)
    await _safe_edit(
        cb, text, reply_markup=_goal_edit_item_kb(sku, page), parse_mode=ParseMode.HTML
    )
    await _safe_answer(cb)


# — Начать ввод значения
@router.callback_query(F.data.startswith("sales:goal:edit:start:"))
async def on_sales_goal_edit_start(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    # sales:goal:edit:start:<sku>:<page>
    if len(parts) < 6:
        await _safe_answer(cb)
        return
    try:
        sku = int(parts[4])
    except Exception:
        await _safe_answer(cb)
        return
    try:
        page = int(parts[5])
    except Exception:
        page = 0

    await state.set_state(SalesGoalEdit.waiting_value)
    await state.update_data(goal_edit_sku=sku, goal_edit_page=page)
    await _safe_edit(
        cb,
        "Введите новое значение цели <b>шт/день</b> (например,                                                         <code>2.5</code> или <code>3</code>):",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="⬅️ Отмена", callback_data=f"sales:goal:edit:sku:{sku}:{page}"
                    )
                ],
                [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
            ]
        ),
    )
    await _safe_answer(cb)


# — Сбросить в 0
@router.callback_query(F.data.startswith("sales:goal:edit:reset:"))
async def on_sales_goal_edit_reset(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    # sales:goal:edit:reset:<sku>:<page>
    if len(parts) < 6:
        await _safe_answer(cb)
        return
    try:
        sku = int(parts[4])
    except Exception:
        await _safe_answer(cb)
        return
    try:
        page = int(parts[5])
    except Exception:
        page = 0

    reset_goal_per_day(sku)
    # После изменения — возвращаемся к отчёту с прежними параметрами
    data = await state.get_data()
    metric = (data.get("goal_metric") or "units").strip().lower()
    horizon = int(data.get("goal_horizon") or 30)
    text = sales_goal_report_text(horizon_days=(1 if horizon == 1 else 30), metric=metric)
    await _safe_edit(
        cb,
        text,
        reply_markup=sales_goal_report_menu(horizon=(1 if horizon == 1 else 30), metric=metric),
    )
    try:
        await cb.answer("Цель сброшена.")
    except Exception:
        pass


# — Обработка текстового ввода значения цели
@router.message(SalesGoalEdit.waiting_value)
async def on_sales_goal_value_input(message: Message, state: FSMContext):
    data = await state.get_data()
    sku = int(data.get("goal_edit_sku"))
    try:
        raw = (message.text or "").strip().replace(",", ".")
        val = float(raw)
        if math.isnan(val) or math.isinf(val) or val < 0:
            val = 0.0
    except Exception:
        await message.reply("Не удалось распознать число. Введите, например: 2.5")
        return

    set_goal_per_day(sku, val)
    await state.clear()
    await message.reply("✅ Цель сохранена.")

    # Показать свежий отчёт с сохранёнными параметрами
    metric = (data.get("goal_metric") or "units").strip().lower()
    horizon = int(data.get("goal_horizon") or 30)
    text = sales_goal_report_text(horizon_days=(1 if horizon == 1 else 30), metric=metric)
    kb = sales_goal_report_menu(horizon=(1 if horizon == 1 else 30), metric=metric)
    await message.answer(text, reply_markup=kb)


# ──────────────────────────────────────────────────────────────────────────────
# --- Факт
@router.callback_query(F.data == "sales:facts")
async def on_sales_facts(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="facts_metric")
    await _safe_edit(cb, "📊 Факт продаж — выберите метрику:", reply_markup=facts_metric_menu())
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("facts:metric:"))
async def on_facts_metric(cb: CallbackQuery, state: FSMContext):
    metric = cb.data.split(":")[-1]
    await state.update_data(fact_metric=metric, step="facts_period")
    await _safe_edit(
        cb, "📊 Факт продаж — выберите период:", reply_markup=facts_period_menu(metric)
    )
    await _safe_answer(cb)


@router.callback_query(F.data == "facts:back_to_metrics")
async def facts_back_to_metrics(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="facts_metric")
    await _safe_edit(cb, "📊 Факт продаж — выберите метрику:", reply_markup=facts_metric_menu())
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("facts:period:"))
async def on_facts_period(cb: CallbackQuery, state: FSMContext):
    _, _, period, metric = cb.data.split(":")
    period_days = int(period)
    text = (
        traffic_text(period_days=period_days, metric=metric)
        if metric in {"ctr", "cvr"}
        else facts_text(period_days=period_days, metric=metric)
    )
    await state.update_data(step="facts_result", fact_metric=metric)
    await _safe_edit(cb, text, reply_markup=back_home_menu())
    await _safe_answer(cb)


# --- План
@router.callback_query(F.data == "sales:plan")
async def on_sales_plan(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="plan_metric")
    await _safe_edit(cb, "📈 План продаж — выберите метрику:", reply_markup=plan_metric_menu())
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("plan:metric:"))
async def on_plan_metric(cb: CallbackQuery, state: FSMContext):
    metric = cb.data.split(":")[-1]
    await state.update_data(plan_metric=metric, step="plan_period")
    await _safe_edit(cb, "📈 План продаж — выберите период:", reply_markup=plan_period_menu(metric))
    await _safe_answer(cb)


@router.callback_query(F.data == "plan:back_to_metrics")
async def plan_back_to_metrics(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="plan_metric")
    await _safe_edit(cb, "📈 План продаж — выберите метрику:", reply_markup=plan_metric_menu())
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("plan:period:"))
async def on_plan_period(cb: CallbackQuery, state: FSMContext):
    _, _, period, metric = cb.data.split(":")
    period_days = int(period)
    # сохраняем выбранный период в state, чтобы уметь перерисовывать после смены метода
    await state.update_data(step="plan_result", plan_metric=metric, plan_period_days=period_days)
    # сразу рендерим с нашей кастомной клавиатурой, где есть «⚙️ Метод»
    await _render_plan_result(cb, state)
    await _safe_answer(cb)


# — Настройка метода прогноза прямо из раздела «План»
@router.callback_query(F.data == "plan:method:open")
async def plan_method_open(cb: CallbackQuery, state: FSMContext):
    title = get_forecast_method_title()
    text = (
        "⚙️ <b>Метод расчёта прогноза</b>\n" f"Текущий: <b>{title}</b>\n\n" "Выберите новый метод:"
    )
    await _safe_edit(cb, text, reply_markup=_plan_methods_kb())
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("plan:method:set:"))
async def plan_method_set(cb: CallbackQuery, state: FSMContext):
    code = cb.data.split(":")[-1]
    title = set_forecast_method(code)
    # после сохранения — перерисовываем результат плана с новым методом
    await _render_plan_result(cb, state)
    try:
        await cb.answer(f"Метод: {title}")
    except Exception:
        pass


@router.callback_query(F.data == "plan:method:close")
async def plan_method_close(cb: CallbackQuery, state: FSMContext):
    await _render_plan_result(cb, state)
    await _safe_answer(cb)


# --- Отчёт по продажам + календарь
@router.callback_query(F.data == "sales:report")
async def on_sales_report_root(cb: CallbackQuery, state: FSMContext):
    await state.update_data(step="sales_report_sku_list")
    skus = list_skus() or []
    if not skus:
        await _safe_edit(cb, "Нет доступных SKU.", reply_markup=back_home_menu())
        await _safe_answer(cb)
        return

    # ⛔️ Только наблюдаемые SKU + имена из ALIAS, порядок — по WATCH_SKU
    order = _watch_skus_order()
    order_set = set(order)
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku

        rows = [
            (int(s), (get_alias_for_sku(int(s)) or str(s)))
            for s, _name in skus
            if int(s) in order_set
        ]
    except Exception:
        rows = [(int(s), str(s)) for s, _name in skus if int(s) in order_set]
    rows = _sort_rows_by_env(rows)

    kb_rows = [
        [InlineKeyboardButton(text=f"{name[:32]}", callback_data=f"sales:report:sku:{sku}")]
        for sku, name in rows[:50]
    ]
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sales")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await _safe_edit(cb, "📊 Отчёт по продажам — выберите SKU:", reply_markup=kb)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("sales:report:sku:"))
async def on_sales_report_sku(cb: CallbackQuery, state: FSMContext):
    sku = int(cb.data.split(":")[-1])
    today = date.today()
    hint = (
        f"📊 Отчёт по продажам — SKU {sku}\n\n"
        "Выбор периода в календаре:\n"
        "• 1‑е нажатие — начало периода\n"
        "• 2‑е — конец периода\n"
        "• 3‑е — начать выбор заново\n"
        "• Два нажатия на одну дату — выбор одного дня\n\n"
        "После выбора нажмите <b>✅ Готово</b>."
    )
    await state.update_data(
        step="sales_report_period",
        report_sku=sku,
        report_start=None,
        report_end=None,
        date_mode="period",
        date_sel=[],
        date_from=None,
        date_to=None,
        cal_year=today.year,
        cal_month=today.month,
        cal_back_cb="sales:report:cancel",
        cb_prefix=CALENDAR_PREFIX_SALES_REPORT,
        cal_header=hint,
    )
    await show_calendar(cb.message, state, prefix=CALENDAR_PREFIX_SALES_REPORT)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("sales:report:period:"))
async def on_sales_report_period_direct(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split(":")
    if len(parts) < 6:
        await _safe_answer(cb)
        return
    _, _, _, sku_str, start_date, end_date = parts[:6]
    sku = int(sku_str)

    await state.update_data(
        step="sales_report_result", report_sku=sku, report_start=start_date, report_end=end_date
    )
    text = sales_report_text(sku, start_date, end_date)
    await _safe_edit(cb, text, reply_markup=back_home_menu(), parse_mode=ParseMode.HTML)
    await _safe_answer(cb)


@router.callback_query(F.data == f"{CALENDAR_PREFIX_SALES_REPORT}:date:done")
async def on_sales_report_calendar_done(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sku = data.get("report_sku")
    if not sku:
        await _safe_answer(cb)
        return

    mode = (data.get("date_mode") or "period").lower()
    start = data.get("date_from")
    end = data.get("date_to") or start

    # В режиме выбора отдельных дат — берём минимальную и максимальную из выбранных
    if mode == "dates":
        raw_sel = data.get("date_sel") or []
        if isinstance(raw_sel, str):
            raw_sel_list = [raw_sel]
        elif isinstance(raw_sel, (set, tuple)):
            raw_sel_list = list(raw_sel)
        elif isinstance(raw_sel, list):
            raw_sel_list = raw_sel
        else:
            raw_sel_list = []

        sel_dates = []
        for s in raw_sel_list:
            s_str = str(s).strip()
            if not s_str:
                continue
            try:
                d = date.fromisoformat(s_str[:10])
            except Exception:
                continue
            sel_dates.append(d)

        if sel_dates:
            sel_dates.sort()
            start = sel_dates[0].isoformat()
            end = sel_dates[-1].isoformat()

    if not start:
        try:
            await cb.answer("Сначала выберите даты", show_alert=True)
        except Exception:
            pass
        return

    try:
        sku_int = int(sku)
    except Exception:
        sku_int = int(str(sku).strip() or "0")

    await state.update_data(step="sales_report_result", report_start=start, report_end=end)
    text = sales_report_text(sku_int, start, end)
    await _safe_edit(cb, text, reply_markup=back_home_menu(), parse_mode=ParseMode.HTML)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith(f"{CALENDAR_PREFIX_SALES_REPORT}:date:switch:"))
async def on_sales_report_calendar_switch(cb: CallbackQuery, state: FSMContext):
    await calendar_handle_date_switch(cb, state)


@router.callback_query(F.data.startswith(f"{CALENDAR_PREFIX_SALES_REPORT}:date:nav:"))
async def on_sales_report_calendar_nav(cb: CallbackQuery, state: FSMContext):
    await calendar_handle_date_nav(cb, state)


@router.callback_query(F.data == f"{CALENDAR_PREFIX_SALES_REPORT}:date:clear")
async def on_sales_report_calendar_clear(cb: CallbackQuery, state: FSMContext):
    await calendar_handle_date_clear(cb, state)


@router.callback_query(F.data.startswith(f"{CALENDAR_PREFIX_SALES_REPORT}:date:pick:"))
async def on_sales_report_calendar_pick(cb: CallbackQuery, state: FSMContext):
    await calendar_handle_date_pick(cb, state)


@router.callback_query(F.data == f"{CALENDAR_PREFIX_SALES_REPORT}:noop")
async def on_sales_report_calendar_noop(cb: CallbackQuery, state: FSMContext):
    await _safe_answer(cb)


@router.callback_query(F.data == "sales:report:cancel")
async def on_sales_report_cancel(cb: CallbackQuery, state: FSMContext):
    skus = list_skus() or []
    if not skus:
        await _safe_edit(cb, "Нет доступных SKU.", reply_markup=back_home_menu())
        await _safe_answer(cb)
        return

    # ⛔️ Только наблюдаемые SKU + имена из ALIAS, порядок — по WATCH_SKU
    order = _watch_skus_order()
    order_set = set(order)
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku

        rows = [
            (int(s), (get_alias_for_sku(int(s)) or str(s)))
            for s, _name in skus
            if int(s) in order_set
        ]
    except Exception:
        rows = [(int(s), str(s)) for s, _name in skus if int(s) in order_set]
    rows = _sort_rows_by_env(rows)

    kb_rows = [
        [InlineKeyboardButton(text=f"{name[:32]}", callback_data=f"sales:report:sku:{s}")]
        for s, name in rows[:50]
    ]
    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sales")])
    kb_rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    await state.update_data(
        step="sales_report_sku_list",
        report_start=None,
        report_end=None,
        date_from=None,
        date_to=None,
        date_sel=[],
        cal_year=None,
        cal_month=None,
    )
    await _safe_edit(cb, "📊 Отчёт по продажам — выберите SKU:", reply_markup=kb)
    await _safe_answer(cb)


# ──────────────────────────────────────────────────────────────────────────────
# 🧾 Отчёт по выкупам — НОВЫЙ ПОДРАЗДЕЛ (внутри «Продажи»)
# ──────────────────────────────────────────────────────────────────────────────
class SalesBuyoutUpload(StatesGroup):
    waiting_files = State()


def _buyout_user_dir(user_id: int) -> str:
    path = os.path.join(DATA_DIR, "uploads", "buyout", str(user_id))
    os.makedirs(path, exist_ok=True)
    return path


def _buyout_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="♻️ Сбросить загрузки", callback_data="sales:buyout:reset")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav:back")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "sales:buyout")
async def on_sales_buyout_root(cb: CallbackQuery, state: FSMContext):
    """
    Стартовый экран: ждём два файла .xlsx (Заказы и Отчёт).
    """
    await state.set_state(SalesBuyoutUpload.waiting_files)
    await state.update_data(
        step="sales_report_buyout", buyout_orders_path=None, buyout_report_path=None
    )
    text = buyout_help(False, False)
    await _safe_edit(cb, text, parse_mode=ParseMode.HTML, reply_markup=_buyout_kb())
    await _safe_answer(cb)


@router.callback_query(F.data == "sales:buyout:reset")
async def on_sales_buyout_reset(cb: CallbackQuery, state: FSMContext):
    """
    Сброс загруженных файлов для текущего пользователя.
    """
    uid = int(cb.from_user.id) if cb.from_user else 0
    user_dir = _buyout_user_dir(uid)
    try:
        for name in os.listdir(user_dir):
            if name.lower().endswith(".xlsx"):
                try:
                    os.remove(os.path.join(user_dir, name))
                except Exception:
                    pass
    except Exception:
        pass
    await state.update_data(buyout_orders_path=None, buyout_report_path=None)
    text = buyout_help(False, False)
    await _safe_edit(cb, text, parse_mode=ParseMode.HTML, reply_markup=_buyout_kb())
    await _safe_answer(cb)


@router.message(SalesBuyoutUpload.waiting_files, F.document)
async def on_sales_buyout_file(message: Message, state: FSMContext, bot: Bot):
    """
    Принимаем документы .xlsx, определяем тип (orders/report), сохраняем.
    Когда загружены оба — запускаем обработку и отправляем результат.
    """
    uid = int(message.from_user.id) if message.from_user else 0
    user_dir = _buyout_user_dir(uid)

    doc = message.document
    fname = (doc.file_name or "").strip()
    if not fname.lower().endswith(".xlsx"):
        await message.reply("⚠️ Нужен файл Excel (.xlsx). Попробуйте ещё раз.")
        return

    tmp_path = os.path.join(user_dir, f"._upload_{int(time.time())}_{fname}")
    # скачиваем файл (aiogram v3)
    try:
        await bot.download(doc, destination=tmp_path)
    except Exception:
        try:
            file = await bot.get_file(doc.file_id)
            await bot.download_file(file.file_path, tmp_path)
        except Exception:
            await message.reply("😔 Не удалось скачать файл. Пришлите документ ещё раз.")
            return

    # классифицируем и переименовываем в каноническое имя
    role = buyout_classify(tmp_path)
    if not role:
        await message.reply(
            "⚠️ Не удалось распознать тип файла.\n"
            "Убедитесь, что это:\n"
            "• «Заказы.xlsx» (есть колонка «Номер отправления»), или\n"
            "• «Отчет.xlsx» (строка «№ п/п», «Отправление / Номер»)."
        )
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        return

    if role == "orders":
        final_path = os.path.join(user_dir, "Заказы.xlsx")
        role_human = "Заказы"
    else:
        final_path = os.path.join(user_dir, "Отчет.xlsx")
        role_human = "Отчёт"

    try:
        if os.path.exists(final_path):
            os.replace(tmp_path, final_path)
        else:
            os.rename(tmp_path, final_path)
    except Exception:
        await message.reply("Не удалось сохранить файл на диск.")
    else:
        # уведомление о загрузке файла
        try:
            await message.answer(
                f"📥 Файл <b>«{fname}»</b> принят как <b>{role_human}</b> и сохранён.",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

    # обновляем состояние
    data = await state.get_data()
    orders_path = final_path if role == "orders" else (data.get("buyout_orders_path") or None)
    report_path = final_path if role == "report" else (data.get("buyout_report_path") or None)
    await state.update_data(
        step="sales_report_buyout", buyout_orders_path=orders_path, buyout_report_path=report_path
    )

    has_orders = bool(orders_path)
    has_report = bool(report_path)

    # если есть оба файла — обрабатываем (с уведомлением «идут расчёты»)
    if has_orders and has_report:
        try:
            await message.answer(
                "⏳ Оба файла загружены. Выполняю расчёт… Пожалуйста, подождите завершения."
            )
        except Exception:
            pass
        try:
            result_path, count, stats = buyout_process(
                orders_path, report_path, output_dir=user_dir
            )
        except Exception as e:
            await message.reply(f"❌ Ошибка обработки: {e}")
            return

        # отправляем результат и краткую статистику
        try:
            doc = FSInputFile(result_path, filename=os.path.basename(result_path))
            await message.answer_document(
                document=doc,
                caption="✅ Готово. Файл с подсветкой совпадений.",
            )
        except Exception:
            await message.reply("Файл результата сформирован, но не удалось отправить документ.")

        filter_enabled = bool((os.getenv("WATCH_OFFERS") or "").strip())
        filters_line = (
            f"Фильтры: артикулы — "
            f"{('включены' if filter_enabled else 'выключены')}; "
            f"возвраты — исключены.\n"
            f"Строк данных: {stats.get('rows_total', 0)} • "
            f"исключено возвратов: {stats.get('excluded_returns', 0)} • "
            f"исключено по артикулам: {stats.get('excluded_art', 0)} • "
            f"треки к сопоставлению: {stats.get('tracks_after_filters', 0)}\n"
            f"🔍 Совпадений найдено: <b>{count}</b>"
        )
        await message.answer(filters_line, parse_mode=ParseMode.HTML, reply_markup=back_home_menu())

        # очищаем для следующего цикла (оставляем в том же подпункте)
        await state.update_data(buyout_orders_path=None, buyout_report_path=None)
        await state.set_state(SalesBuyoutUpload.waiting_files)

    # Если загружен только один файл — больше ничего не показываем (никаких меню/хелпов).
