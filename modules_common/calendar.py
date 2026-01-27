# modules_common/calendar.py
# Календарь выбора дат/периодов для кампаний и статистики (без ограничения 28 дней)

from __future__ import annotations

import os
import calendar as _py_calendar
from datetime import datetime, timedelta, date as date_cls
from typing import List, Optional, Iterable, Set, Union, Tuple

from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

from zoneinfo import ZoneInfo

# ----- TZ -----
_TIMEZONE = os.getenv("TIMEZONE", os.getenv("TZ", "Europe/Moscow"))
try:
    TZ = ZoneInfo(_TIMEZONE)
except Exception:
    TZ = ZoneInfo("Europe/Moscow")

_DEFAULT_PREFIX = os.getenv("CB_PREFIX_DEFAULT", "mon").strip() or "mon"


def _month_title(year: int, month: int) -> str:
    months = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    return f"{months[month - 1]} {year}"


def _to_date(val: Union[str, datetime, date_cls, None]) -> Optional[date_cls]:
    if val is None:
        return None
    if isinstance(val, date_cls) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if "T" in s:
            s = s.split("T", 1)[0]
        s = s[:10]
        try:
            return date_cls.fromisoformat(s)
        except Exception:
            return None
    return None


def _to_date_set(values: Optional[Iterable[Union[str, datetime, date_cls]]]) -> Set[date_cls]:
    out: Set[date_cls] = set()
    for v in values or []:
        d = _to_date(v)
        if d:
            out.add(d)
    return out


def build_calendar_kb(
    year: int,
    month: int,
    mode: str,
    sel_dates: List[Union[str, datetime, date_cls]],
    p_from: Optional[Union[str, datetime, date_cls]],
    p_to: Optional[Union[str, datetime, date_cls]],
    back_cb: Optional[str] = None,
    *,
    prefix: Optional[str] = None,
) -> types.InlineKeyboardMarkup:
    """Без ограничений по горизонту — можно листать любые месяцы и выбирать любые даты."""
    prefix = prefix or _DEFAULT_PREFIX
    kb = InlineKeyboardBuilder()

    # Шапка: навигация по месяцам
    prev_y, prev_m = (year - 1, 12) if month == 1 else (year, month - 1)
    next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)
    kb.row(
        InlineKeyboardButton(text="◀️", callback_data=f"{prefix}:date:nav:{prev_y}:{prev_m}"),
        InlineKeyboardButton(text=_month_title(year, month), callback_data=f"{prefix}:noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"{prefix}:date:nav:{next_y}:{next_m}"),
    )

    # Дни недели
    kb.row(
        *[
            InlineKeyboardButton(text=t, callback_data=f"{prefix}:noop")
            for t in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        ]
    )

    sel_dates_dt = _to_date_set(sel_dates)
    pf = _to_date(p_from)
    pt = _to_date(p_to)

    mode_norm = (mode or "period").lower()
    if mode_norm not in ("dates", "period", "multiple", "single"):
        mode_norm = "period"
    if mode_norm == "multiple":
        mode_norm = "dates"

    today = datetime.now(TZ).date()
    cal = _py_calendar.Calendar(firstweekday=_py_calendar.MONDAY)

    for week in cal.monthdatescalendar(year, month):
        row_btns = []
        for cur_date in week:
            day_num = f"{cur_date.day}"
            date_str = cur_date.isoformat()

            if mode_norm == "dates":
                is_selected = cur_date in sel_dates_dt
            else:
                if pf and pt:
                    lo, hi = (pf, pt) if pf <= pt else (pt, pf)
                    is_selected = lo <= cur_date <= hi
                else:
                    is_selected = (pf == cur_date) or (pt == cur_date)

            is_today = cur_date == today

            prefix_icon = ""
            if is_today:
                prefix_icon += "🔶"
            if is_selected:
                prefix_icon += "🔷"

            label = f"{prefix_icon}{day_num}"
            cb = f"{prefix}:date:pick:{date_str}"
            row_btns.append(InlineKeyboardButton(text=label, callback_data=cb))

        kb.row(*row_btns)

    if mode_norm == "dates":
        kb.row(
            InlineKeyboardButton(
                text="🔁 Переключить на 📆 Период", callback_data=f"{prefix}:date:switch:period"
            )
        )
    else:
        kb.row(
            InlineKeyboardButton(
                text="🔁 Переключить на 📅 Даты", callback_data=f"{prefix}:date:switch:dates"
            )
        )

    kb.row(
        InlineKeyboardButton(text="🧹 Очистить", callback_data=f"{prefix}:date:clear"),
        InlineKeyboardButton(text="✅ Готово", callback_data=f"{prefix}:date:done"),
    )
    kb.row(
        InlineKeyboardButton(text="◀️ Назад", callback_data=(back_cb or f"{prefix}:back")),
        InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home"),
    )
    return kb.as_markup()


async def show_calendar(msg: types.Message, state: FSMContext, *, prefix: Optional[str] = None):
    data = await state.get_data()
    mode = (data.get("date_mode") or "period").lower()
    if mode not in ("dates", "period"):
        mode = "period"

    now = datetime.now(TZ)

    raw_sel = data.get("date_sel")
    if isinstance(raw_sel, str):
        sel = [raw_sel]
    elif isinstance(raw_sel, (set, tuple)):
        sel = list(raw_sel)
    elif isinstance(raw_sel, list):
        sel = raw_sel
    else:
        sel = []

    sel_norm: List[str] = []
    for s in sel:
        d = _to_date(s)
        if d:
            sel_norm.append(d.isoformat())

    # Без ограничений — просто оставляем выбранные значения как есть
    await state.update_data(date_sel=sel_norm)

    year = int(data.get("cal_year") or 0) or now.year
    month = int(data.get("cal_month") or 0) or now.month

    p_from = data.get("date_from")
    p_to = data.get("date_to")
    back_cb_from_state = data.get("cal_back_cb") or data.get("hours_back_cb")
    prefix = prefix or data.get("cb_prefix") or _DEFAULT_PREFIX

    header = (
        data.get("cal_header")
        or "📅 Выбор периода\nВыделите диапазон дат (🔶 — сегодня, 🔷 — выделено)."
    )
    try:
        await msg.edit_text(
            header,
            reply_markup=build_calendar_kb(
                year,
                month,
                mode,
                sel_norm,
                p_from,
                p_to,
                back_cb=back_cb_from_state,
                prefix=prefix,
            ),
            parse_mode="HTML",
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise


# ---------- Обработчики callback’ов календаря ----------
async def handle_date_switch(cb: types.CallbackQuery, state: FSMContext):
    parts = (cb.data or "").split(":")
    prefix = parts[0] if parts else _DEFAULT_PREFIX
    now = datetime.now(TZ)
    mode = parts[-1]
    await state.update_data(
        date_mode=("dates" if mode == "dates" else "period"),
        date_sel=[],
        date_from=None,
        date_to=None,
        cal_year=now.year,
        cal_month=now.month,
        host_msg_id=cb.message.message_id,
        cb_prefix=prefix,
    )
    await show_calendar(cb.message, state, prefix=prefix)
    await cb.answer("Режим переключён")


async def handle_date_nav(cb: types.CallbackQuery, state: FSMContext):
    parts = (cb.data or "").split(":")
    prefix = parts[0] if parts else _DEFAULT_PREFIX
    try:
        y = int(parts[-2])
        m = int(parts[-1])
    except Exception:
        return await cb.answer()

    await state.update_data(
        cal_year=y, cal_month=m, host_msg_id=cb.message.message_id, cb_prefix=prefix
    )
    await show_calendar(cb.message, state, prefix=prefix)
    await cb.answer()


async def handle_date_clear(cb: types.CallbackQuery, state: FSMContext):
    parts = (cb.data or "").split(":")
    prefix = parts[0] if parts else _DEFAULT_PREFIX
    data = await state.get_data()
    await state.update_data(
        date_sel=[],
        date_from=None,
        date_to=None,
        cal_year=data.get("cal_year"),
        cal_month=data.get("cal_month"),
        host_msg_id=cb.message.message_id,
        cb_prefix=prefix,
    )
    await show_calendar(cb.message, state, prefix=prefix)
    await cb.answer("Сброшено")


async def handle_date_pick(cb: types.CallbackQuery, state: FSMContext):
    parts = (cb.data or "").split(":")
    prefix = parts[0] if parts else _DEFAULT_PREFIX
    date_str = parts[-1]
    data = await state.get_data()
    mode = (data.get("date_mode") or "period").lower()
    if mode not in ("dates", "period"):
        mode = "period"

    try:
        pick_dt = _to_date(date_str)
        if not pick_dt:
            raise ValueError("bad date")
        pick_iso = pick_dt.isoformat()
    except Exception:
        return await cb.answer("Неверный формат даты", show_alert=True)

    if mode == "dates":
        sel_raw = set(data.get("date_sel") or [])
        sel_norm = set()
        for s in sel_raw:
            d = _to_date(s)
            if d:
                sel_norm.add(d.isoformat())
        if pick_iso in sel_norm:
            sel_norm.remove(pick_iso)
        else:
            sel_norm.add(pick_iso)
        await state.update_data(
            date_sel=sorted(sel_norm), host_msg_id=cb.message.message_id, cb_prefix=prefix
        )
    else:
        p_from = data.get("date_from")
        p_to = data.get("date_to")
        from_dt = _to_date(p_from)
        to_dt = _to_date(p_to)

        if not from_dt:
            from_dt, to_dt = pick_dt, None
        elif not to_dt:
            if pick_dt < from_dt:
                from_dt, to_dt = pick_dt, from_dt
            else:
                to_dt = pick_dt
        else:
            from_dt, to_dt = pick_dt, None

        await state.update_data(
            date_from=(from_dt.isoformat() if from_dt else None),
            date_to=(to_dt.isoformat() if to_dt else None),
            host_msg_id=cb.message.message_id,
            cb_prefix=prefix,
        )

    await show_calendar(cb.message, state, prefix=prefix)
    await cb.answer()


# ─────────────────────────────────────────────────────────────────────────────
# Совместимый календарь для модулей отгрузок (ship:cal:* callbacks)
# ─────────────────────────────────────────────────────────────────────────────
def shipments_calendar_kb(
    *,
    prefix: str = "ship:cal",
    selected: Optional[str] = None,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> types.InlineKeyboardMarkup:
    """
    Упрощённый календарь с callback-ами:
      prev/next:  <prefix>:prev / <prefix>:next
      pick day:   <prefix>:pick:YYYY-MM-DD
      confirm:    <prefix>:confirm
      cancel:     <prefix>:cancel
    Работает только с выбором одной даты (selected).
    """
    from datetime import date as _date
    import calendar as _cal

    today = datetime.now(TZ).date()
    sel_dt = _to_date(selected)

    if year is None or month is None:
        base = sel_dt or today
        year, month = base.year, base.month

    kb = InlineKeyboardBuilder()

    # Шапка
    kb.row(
        InlineKeyboardButton(text="◀️", callback_data=f"{prefix}:prev"),
        InlineKeyboardButton(text=_month_title(year, month), callback_data=f"{prefix}:noop"),
        InlineKeyboardButton(text="▶️", callback_data=f"{prefix}:next"),
    )

    # Дни недели
    kb.row(
        *[
            InlineKeyboardButton(text=t, callback_data=f"{prefix}:noop")
            for t in ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        ]
    )

    cal = _cal.Calendar(firstweekday=_cal.MONDAY)
    for week in cal.monthdayscalendar(year, month):
        row_btns: List[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                row_btns.append(InlineKeyboardButton(text="·", callback_data=f"{prefix}:noop"))
                continue
            cur = _date(year, month, day)
            mark = ""
            if cur == today:
                mark += "🔶"
            if sel_dt and cur == sel_dt:
                mark += "🔷"
            label = f"{mark}{day}"
            row_btns.append(
                InlineKeyboardButton(
                    text=label,
                    callback_data=f"{prefix}:pick:{cur.isoformat()}",
                )
            )
        kb.row(*row_btns)

    # Действия (подтверждение / отмена)
    kb.row(
        InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"{prefix}:confirm"),
        InlineKeyboardButton(text="✖ Отмена", callback_data=f"{prefix}:cancel"),
    )
    return kb.as_markup()


__all__ = [
    "TZ",
    "build_calendar_kb",
    "show_calendar",
    "handle_date_switch",
    "handle_date_nav",
    "handle_date_clear",
    "handle_date_pick",
    # совместимый календарь для модулей отгрузок
    "shipments_calendar_kb",
]
