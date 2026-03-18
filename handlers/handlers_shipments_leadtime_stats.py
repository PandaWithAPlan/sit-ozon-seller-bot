# handlers/handlers_shipments_leadtime_stats.py
from __future__ import annotations

import os
import json
import math
import asyncio
import datetime as _dt
from typing import Dict, Any, List, Tuple, Optional

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

# ─────────────────────────────────────────────────────────────────────────────
# Статистика Lead Time — фасад
# ─────────────────────────────────────────────────────────────────────────────
try:
    from modules_shipments.shipments_leadtime_stats import (  # type: ignore
        get_lead_stats_summary,
        get_lead_stats_by_sku,
        get_lead_stats_by_cluster,
        get_lead_stats_by_warehouse,
        get_lead_stats_sku_for_warehouse,
        get_lead_stats_sku_for_cluster,
        set_lead_allocation_flag,
        get_stat_period as _facade_get_stat_period,
        invalidate_stats_cache,
    )

    _FACade_OK = True
except Exception:
    _FACade_OK = False

    # мягкие заглушки
    async def get_lead_stats_summary(*_a, **_k):
        return {}

    async def get_lead_stats_by_sku(*_a, **_k):
        return []

    async def get_lead_stats_by_cluster(*_a, **_k):
        return []

    async def get_lead_stats_by_warehouse(*_a, **_k):
        return []

    async def get_lead_stats_sku_for_warehouse(*_a, **_k):
        return []

    async def get_lead_stats_sku_for_cluster(*_a, **_k):
        return []

    async def set_lead_allocation_flag(_flag: bool) -> None:
        pass

    async def _facade_get_stat_period() -> int:
        return 180

    async def invalidate_stats_cache() -> None:
        pass


# Статус инжеста
try:
    from modules_shipments.shipments_leadtime_stats_data import ingest_status  # type: ignore
except Exception:

    async def ingest_status() -> dict:
        return {}


# Алиасы SKU
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


# Имя склада для заголовков (фолбэк)
try:
    from modules_shipments.shipments_leadtime import get_warehouse_title as _wh_title  # type: ignore
except Exception:
    try:
        from modules_shipments.shipments_leadtime_data import get_warehouse_title as _wh_title  # type: ignore
    except Exception:

        def _wh_title(wid: int) -> str:  # type: ignore
            return f"wh:{wid}"


router = Router(name="leadtime_stats")

# ─────────────────────────────────────────────────────────────────────────────
# Настройки раздела
# ─────────────────────────────────────────────────────────────────────────────
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_PREFS_PATH = os.path.join(_BASE_DIR, "data", "cache", "common", "lead_stats_prefs.json")
os.makedirs(os.path.dirname(_PREFS_PATH), exist_ok=True)

PERIOD_CHOICES = (90, 180, 360)
SKU_REPORT_PAGE_SIZE = int(os.getenv("LTS_SKU_PAGE_SIZE", "30"))


def _now() -> str:
    return _dt.datetime.now().strftime("%d.%m.%Y %H:%M")


# We need sync version of _facade_get_stat_period or handle it async in _read_prefs?
# _read_prefs is sync.
# _facade_get_stat_period is async now.
# However, _read_prefs just needs a default.
# I'll use hardcoded default here to avoid making _read_prefs async which might complicate things
# or rely on json read.
DEFAULT_PERIOD = 180

def _read_prefs() -> Dict[str, Any]:
    try:
        if os.path.exists(_PREFS_PATH):
            with open(_PREFS_PATH, "r", encoding="utf-8") as f:
                d = json.load(f) or {}
        else:
            d = {}
    except Exception:
        d = {}

    period = int(d.get("period", DEFAULT_PERIOD))
    if period not in PERIOD_CHOICES:
        period = DEFAULT_PERIOD
    alloc = bool(d.get("allocate_by_qty", True))
    autotrack_enabled = bool(d.get("autotrack_enabled", True))
    autotrack_interval_min = int(
        d.get("autotrack_interval_min", int(os.getenv("LEAD_AUTOTRACK_INTERVAL_MIN", "30")))
    )
    return {
        "period": period,
        "allocate_by_qty": alloc,
        "autotrack_enabled": autotrack_enabled,
        "autotrack_interval_min": autotrack_interval_min,
    }


def _write_prefs(
    period: int | None = None,
    allocate_by_qty: bool | None = None,
    autotrack_enabled: bool | None = None,
    autotrack_interval_min: int | None = None,
) -> None:
    cur = _read_prefs()
    if period is not None and period in PERIOD_CHOICES:
        cur["period"] = int(period)
    if allocate_by_qty is not None:
        cur["allocate_by_qty"] = bool(allocate_by_qty)
    if autotrack_enabled is not None:
        cur["autotrack_enabled"] = bool(autotrack_enabled)
        os.environ["LEAD_AUTOTRACK_ENABLED"] = "1" if cur["autotrack_enabled"] else "0"
    if autotrack_interval_min is not None and autotrack_interval_min > 0:
        cur["autotrack_interval_min"] = int(autotrack_interval_min)
        os.environ["LEAD_AUTOTRACK_INTERVAL_MIN"] = str(int(autotrack_interval_min))
    try:
        os.makedirs(os.path.dirname(_PREFS_PATH), exist_ok=True)
        with open(_PREFS_PATH, "w", encoding="utf-8") as f:
            json.dump(cur, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Фоновый автосбор
# ─────────────────────────────────────────────────────────────────────────────
_AUTO_TASK: Optional[asyncio.Task] = None


def _autotrack_enabled() -> bool:
    if os.getenv("LEAD_AUTOTRACK_ENABLED", "1") == "0":
        return False
    try:
        return bool(_read_prefs().get("autotrack_enabled", True))
    except Exception:
        return True


async def _autotrack_loop():
    while True:
        try:
            interval_min = int(
                os.getenv(
                    "LEAD_AUTOTRACK_INTERVAL_MIN",
                    str(_read_prefs().get("autotrack_interval_min", 30)),
                )
            )
        except Exception:
            interval_min = 30

        if not _autotrack_enabled():
            await asyncio.sleep(max(60, interval_min * 60))
            continue

        try:
            pages = int(os.getenv("LEAD_INGEST_PAGES", "3"))
        except Exception:
            pages = 3

        try:
            from modules_shipments.shipments_leadtime_stats_data import ingest_tick  # type: ignore

            await ingest_tick(pages)
        except Exception:
            pass

        await asyncio.sleep(max(60, interval_min * 60))


def _ensure_autotrack_started():
    global _AUTO_TASK
    if _AUTO_TASK is None or _AUTO_TASK.done():
        loop = asyncio.get_running_loop()
        _AUTO_TASK = loop.create_task(_autotrack_loop())


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные форматтеры
# ─────────────────────────────────────────────────────────────────────────────


def _fmt_metrics(m: Dict[str, float]) -> str:
    if not m:
        return "—"
    return f"avg {m.get('avg', 0):.2f} дн"


def _fmt_line_sku(sku: int, alias: str, m: Dict[str, float]) -> str:
    n = int((m or {}).get("n", 0) or 0)
    return f"🔹 {alias or sku}: {m.get('avg', 0.0):.2f} дн" + (f" (n={n})" if n else "")


def _label_cluster(name: str, m: Dict[str, float]) -> str:
    return f"{name} — ∅={m.get('avg', 0.0):.2f} дн"


def _label_wh(name: str, m: Dict[str, float]) -> str:
    return f"{name} — ∅={m.get('avg', 0.0):.2f} дн"


def _weighted_total(rows: List[Tuple[int, str, Dict[str, float]]]) -> Tuple[float, int, int]:
    if not rows:
        return 0.0, 0, 0
    total_n, total_sum = 0, 0.0
    for _, _, m in rows:
        n = int(m.get("n", 0) or 0)
        total_n += n
        total_sum += float(m.get("avg", 0.0)) * n
    return ((total_sum / total_n) if total_n else 0.0), len(rows), total_n


# Имя кластера для заголовка по id (берём из статистики текущего периода)


async def _cluster_name_from_stats(period: int, cid: int) -> str:
    try:
        for _cid, cname, _m in await get_lead_stats_by_cluster(period) or []:
            if int(_cid) == int(cid):
                return str(cname)
    except Exception:
        pass
    return str(cid)


# ─────────────────────────────────────────────────────────────────────────────
# Главное меню
# ─────────────────────────────────────────────────────────────────────────────


def _menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔢 По SKU", callback_data="lts:view:sku")],
            [InlineKeyboardButton(text="🏢 По кластерам", callback_data="lts:view:cluster")],
            [InlineKeyboardButton(text="🏭 По складам", callback_data="lts:view:warehouse")],
            [InlineKeyboardButton(text="⚙️ Настройки", callback_data="lts:settings")],
            [InlineKeyboardButton(text="📦 Информация по заявкам", callback_data="lts:info")],
            [InlineKeyboardButton(text="◀️ К срокам доставки", callback_data="lead:start")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs):
    try:
        await cb.message.edit_text(text, **kwargs)
    except (TelegramBadRequest, TelegramNetworkError):
        try:
            await cb.message.answer(text, **kwargs)
        except Exception:
            pass


async def _ack(cb: CallbackQuery) -> None:
    try:
        await cb.answer()
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Главная
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "leadtime:stats")
async def lts_home(cb: CallbackQuery):
    await _ack(cb)
    _ensure_autotrack_started()
    prefs = _read_prefs()
    try:
        summary = await get_lead_stats_summary(prefs["period"])
    except Exception:
        summary = {}
    text = (
        "📄 <b>Статистика сроков доставки</b>\n"
        f"⏱ Обновлено: {_now()}\n\n"
        f"⚙️ Период: <b>{prefs['period']} дн.</b>\n"
        f"• Распределение по SKU: <b>{
            'учитывать вес партии' if prefs['allocate_by_qty'] else 'не учитывать вес партии'}</b>\n\n"
        f"📊 ИТОГО по сети — {_fmt_metrics(summary)}"
    )
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_menu_kb())


# ─────────────────────────────────────────────────────────────────────────────
# 📦 Информация по заявкам
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "lts:info")
async def lts_info(cb: CallbackQuery):
    await _ack(cb)
    _ensure_autotrack_started()
    st = await ingest_status() or {}
    prefs = _read_prefs()
    text = (
        "📦 <b>Информация по заявкам</b>\n"
        "Фаза: <b>после дроп-офф (ACCEPTED → ACCEPTANCE_AT_STORAGE_WAREHOUSE → REPORTS_CONFIRMATION_AWAITING → COMPLETED)</b>\n\n"
        f"🔹 Отслеживается (ACCEPTED…): <b>{int(st.get('tracked') or 0)}</b>\n"
        f"🔹 Завершено (финальная стадия): <b>{int(st.get('completed') or 0)}</b>\n"
        f"🔹 В обработке: <b>{max(0, int(st.get('tracked') or 0) -
                                 int(st.get('completed') or 0))}</b>\n\n"
        f"📅 Последний опрос: <code>{st.get('last_run_at') or '—'}</code>\n"
        f"🧾 Событий в кэше: <b>{int(st.get('total_cached') or 0)}</b>\n"
        f"   • агрегаты (заказ×склад): <b>{int(st.get('base_rows') or 0)}</b>  • по SKU: <b>{int(st.get('sku_rows') or 0)}</b>\n"
        f"🕓 Интервал опроса (env): <b>{int(prefs['autotrack_interval_min'])} мин</b>\n"
        f"🔘 Автосбор: <b>{'включён' if prefs['autotrack_enabled'] else 'выключен'}</b>"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К разделу", callback_data="leadtime:stats")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────────────────────────────────────────
# Списки «по складам / по кластерам» → дрилл-даун в отчёт по SKU
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "lts:view:warehouse")
async def lts_view_wh(cb: CallbackQuery):
    await _ack(cb)
    prefs = _read_prefs()
    rows = await get_lead_stats_by_warehouse(prefs["period"])
    header = (
        "📄 <b>Сроки доставки — по складам</b>\n"
        f"⏱ Обновлено: {_now()}\n"
        f"⚙️ Период: <b>{prefs['period']} дн.</b>\n"
        "📦 Нажмите на склад, чтобы открыть отчёт по его SKU:\n"
    )
    if not rows:
        await _safe_edit(
            cb,
            header + "ℹ️ Нет событий за выбранный период.",
            parse_mode="HTML",
            reply_markup=_menu_kb(),
        )
        return
    # ⚙️ ВАЖНО: всегда используем человеко‑читаемое имя склада
    kb = []
    for wid, _name_from_stats, m in rows:
        title = _wh_title(int(wid)) or _name_from_stats or f"wh:{int(wid)}"
        kb.append(
            [
                InlineKeyboardButton(
                    text=_label_wh(title, m), callback_data=f"lts:sku:wh:{int(wid)}:0"
                )
            ]
        )
    kb.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="leadtime:stats")])
    await _safe_edit(
        cb, header, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


@router.callback_query(F.data == "lts:view:cluster")
async def lts_view_cluster(cb: CallbackQuery):
    await _ack(cb)
    prefs = _read_prefs()
    rows = await get_lead_stats_by_cluster(prefs["period"])
    header = (
        "📄 <b>Сроки доставки — по кластерам</b>\n"
        f"⏱ Обновлено: {_now()}\n"
        f"⚙️ Период: <b>{prefs['period']} дн.</b>\n"
        "🏷 Нажмите на кластер, чтобы открыть отчёт по его SKU:\n"
    )
    if not rows:
        await _safe_edit(
            cb,
            header + "ℹ️ Нет событий за выбранный период.",
            parse_mode="HTML",
            reply_markup=_menu_kb(),
        )
        return
    kb = [
        [
            InlineKeyboardButton(
                text=_label_cluster(name, m), callback_data=f"lts:sku:cl:{int(cid)}:0"
            )
        ]
        for cid, name, m in rows
    ]
    kb.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="leadtime:stats")])
    await _safe_edit(
        cb, header, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Отчёты по SKU (общий / по складу / по кластеру)
# ─────────────────────────────────────────────────────────────────────────────


def _sku_report_header(title: str, prefs: Dict[str, Any]) -> str:
    return (
        f"{title}\n"
        f"⏱ Обновлено: {_now()}\n\n"
        f"⚙️ Период: {prefs['period']} дн. • Распределение по SKU: "
        f"{'учитывать вес партии' if prefs['allocate_by_qty'] else 'не учитывать вес партии'}\n"
        f"📦 По SKU:\n"
    )


def _sku_report_lines(
    rows: List[Tuple[int, str, Dict[str, float]]], page: int, page_size: int
) -> Tuple[str, int, Tuple[float, int, int]]:
    total = len(rows)
    if total == 0:
        return "ℹ️ Нет событий/поставок по SKU за выбранный период.", 0, (0.0, 0, 0)
    pages = max(1, math.ceil(total / page_size))
    page = max(0, min(page, pages - 1))
    start, end = page * page_size, min(total, (page + 1) * page_size)
    body = "\n".join(_fmt_line_sku(int(sku), alias, m) for sku, alias, m in rows[start:end])
    totals = _weighted_total(rows)
    return body, pages, totals


def _sku_report_kb(context: str, page: int, pages: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    nav: List[InlineKeyboardButton] = []
    if pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"lts:sku:{context}:{
                        page - 1}"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"lts:sku:{context}:{
                        page + 1}"))
    if nav:
        rows.append(nav)
    if context.startswith("wh:"):
        rows.append([InlineKeyboardButton(text="◀️ К складам",                                              callback_data="lts:view:warehouse")])
    elif context.startswith("cl:"):
        rows.append([InlineKeyboardButton(text="◀️ К кластерам",                                              callback_data="lts:view:cluster")])
    else:
        rows.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="leadtime:stats")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _fetch_sku_rows(context: str, period: int) -> List[Tuple[int, str, Dict[str, float]]]:
    if context == "all":
        return await get_lead_stats_by_sku(period)
    if context.startswith("wh:"):
        wid = int(context.split(":")[1])
        return await get_lead_stats_sku_for_warehouse(wid, period)
    if context.startswith("cl:"):
        cid = int(context.split(":")[1])
        return await get_lead_stats_sku_for_cluster(cid, period)
    return []


@router.callback_query(F.data == "lts:view:sku")
async def lts_view_sku(cb: CallbackQuery):
    await _ack(cb)
    prefs = _read_prefs()
    rows = await _fetch_sku_rows("all", prefs["period"])
    header = _sku_report_header("📄 Показатели сроков доставки — Σ∅/SKU", prefs)
    body, pages, totals = _sku_report_lines(rows, page=0, page_size=SKU_REPORT_PAGE_SIZE)
    total_avg, sku_count, _total_n = totals
    text = f"{header}{body}\n\n📊 <b>ИТОГО</b> — ∅={total_avg:.2f} дн • SKU: {sku_count}"
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_sku_report_kb("all", 0, pages))


@router.callback_query(F.data.regexp(r"^lts:sku:(?:all|wh:\d+|cl:\d+):\d+$"))
async def lts_sku_report_paginated(cb: CallbackQuery):
    await _ack(cb)
    parts = cb.data.split(":")
    page = int(parts[-1])
    ctx = ":".join(parts[2:-1])  # 'all' | 'wh:123' | 'cl:456'

    prefs = _read_prefs()
    rows = await _fetch_sku_rows(ctx, prefs["period"])

    # имена вместо чисел в заголовке
    if ctx.startswith("wh:"):
        wid = int(ctx.split(":")[1])
        title = f"📄 Показатели сроков доставки — склад {_wh_title(wid)}"
    elif ctx.startswith("cl:"):
        cid = int(ctx.split(":")[1])
        title = f"📄 Показатели сроков доставки — кластер {
            await _cluster_name_from_stats(
                prefs['period'], cid)}"
    else:
        title = "📄 Показатели сроков доставки — Σ∅/SKU"

    header = _sku_report_header(title, prefs)
    body, pages, totals = _sku_report_lines(rows, page=page, page_size=SKU_REPORT_PAGE_SIZE)
    total_avg, sku_count, _total_n = totals
    text = f"{header}{body}\n\n📊 <b>ИТОГО</b> — ∅={total_avg:.2f} дн • SKU: {sku_count}"
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_sku_report_kb(ctx, page, pages))


# ─────────────────────────────────────────────────────────────────────────────
# Настройки
# ─────────────────────────────────────────────────────────────────────────────


def _settings_text() -> str:
    p = _read_prefs()
    return (
        "⚙️ <b>Настройки сроков доставки</b>\n\n"
        "Параметры применяются ко <u>всем пользователям</u>.\n\n"
        f"Текущие:\n"
        f"• Период: <b>{p['period']} дн.</b>\n"
        f"• Распределение по SKU: <b>{
            'учитывать вес партии' if p['allocate_by_qty'] else 'не учитывать вес партии'}</b>"
    )


def _settings_kb() -> InlineKeyboardMarkup:
    p = _read_prefs()
    rows: List[List[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(
                text=("✓ 90 дн." if p["period"] == 90 else "90 дн."), callback_data="lts:per:90"
            ),
            InlineKeyboardButton(
                text=("✓ 180 дн." if p["period"] == 180 else "180 дн."), callback_data="lts:per:180"
            ),
            InlineKeyboardButton(
                text=("✓ 360 дн." if p["period"] == 360 else "360 дн."), callback_data="lts:per:360"
            ),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text=("✓ Учитывать вес партии" if p["allocate_by_qty"] else "Учитывать вес партии"),
                callback_data="lts:alloc:on",
            ),
            InlineKeyboardButton(
                text=(
                    "✓ Не учитывать вес партии"
                    if not p["allocate_by_qty"]
                    else "Не учитывать вес партии"
                ),
                callback_data="lts:alloc:off",
            ),
        ]
    )
    rows.append([InlineKeyboardButton(text="◀️ К статистике", callback_data="leadtime:stats")])
    rows.append([InlineKeyboardButton(text="◀️ К срокам доставки", callback_data="lead:start")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "lts:settings")
async def lts_settings(cb: CallbackQuery):
    await _ack(cb)
    await _safe_edit(cb, _settings_text(), parse_mode="HTML", reply_markup=_settings_kb())


@router.callback_query(F.data.startswith("lts:per:"))
async def lts_set_period(cb: CallbackQuery):
    await _ack(cb)
    try:
        period = int(cb.data.split(":")[-1])
    except Exception:
        # _facade_get_stat_period is async
        period = await _facade_get_stat_period()
    if period not in PERIOD_CHOICES:
        period = await _facade_get_stat_period()
    _write_prefs(period=period)
    try:
        await invalidate_stats_cache()
    except Exception:
        pass
    await _safe_edit(cb, _settings_text(), parse_mode="HTML", reply_markup=_settings_kb())


@router.callback_query(F.data.startswith("lts:alloc:"))
async def lts_set_alloc(cb: CallbackQuery):
    await _ack(cb)
    turn_on = cb.data.endswith(":on")

    ok_note = "\n\n♻️ Применили новое правило распределения и обновили события."
    err_note = "\n\n⚠️ Не удалось применить новое правило (проверьте логи)."
    try:
        await set_lead_allocation_flag(bool(turn_on))
        _write_prefs(allocate_by_qty=turn_on)
        note = ok_note
    except Exception:
        note = err_note

    await _safe_edit(cb, _settings_text() + note, parse_mode="HTML", reply_markup=_settings_kb())


__all__ = ["router"]
