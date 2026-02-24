# handlers/handlers_shipments_leadtime.py
from __future__ import annotations

import os
import math
import datetime as _dt
from typing import List, Tuple, Dict

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError

# ─────────────────────────────────────────────────────────────────────────────
# Базовые CRUD/справочники сроков (float) + отчёты/списки + мосты в статистику
# ─────────────────────────────────────────────────────────────────────────────
try:
    from modules_shipments.shipments_leadtime import (  # type: ignore
        # CRUD + справочники
        LEAD_EDIT_PAGE_SIZE,
        get_progress,
        list_warehouses_page,
        get_warehouse_title,
        save_lead_days,
        reset_lead_days,
        delete_lead_record,
        get_lead_for_wid,
        # подписка
        enable_follow_stats,
        disable_follow_stats,
        get_following_wids,
        # отчёты на базе сроков
        manual_view_by_warehouse,
        manual_view_by_cluster,
        manual_view_by_sku,
        # новые списки для кнопок
        list_enabled_warehouses_for_report,
        list_enabled_clusters_for_report,
        # мосты в статистику для SKU-детализации
        stats_sku_for_warehouse,
        stats_sku_for_cluster,
        # 🔄 обновление имён по запросу пользователя
        refresh_warehouse_names,
    )
except Exception:
    LEAD_EDIT_PAGE_SIZE = 20

    def get_progress():
        return (0, 0)

    def list_warehouses_page(view_page: int = 0, page_size: int = 20):
        return ([], 0)

    def get_warehouse_title(_wid: int) -> str:
        return f"wh:{_wid}"

    def save_lead_days(_wid: int, _days: float, updated_by: str = "system") -> float:
        return float(_days)

    def reset_lead_days(_wid: int, updated_by: str = "system") -> float:
        return 0.0

    def delete_lead_record(_wid: int) -> None:
        return None

    def get_lead_for_wid(_wid: int):
        return None

    def enable_follow_stats(_wid: int, period: int = 90, metric: str = "avg") -> dict:
        return {}

    def disable_follow_stats(_wid: int) -> dict:
        return {}

    def get_following_wids() -> Dict[int, dict]:
        return {}

    def manual_view_by_warehouse() -> List[Tuple[int, str, float, int]]:
        return []

    def manual_view_by_cluster() -> List[Tuple[int, str, float, int]]:
        return []

    async def manual_view_by_sku() -> List[Tuple[int, str, float, int]]:
        return []

    def list_enabled_warehouses_for_report() -> List[Tuple[int, str, float]]:
        return []

    def list_enabled_clusters_for_report() -> List[Tuple[int, str, float, int]]:
        return []

    async def stats_sku_for_warehouse(_wid: int, _p: int | None = None):
        return []

    async def stats_sku_for_cluster(_cid: int, _p: int | None = None):
        return []

    async def refresh_warehouse_names() -> dict:
        return {"updated": 0, "total": 0}


# Период статистики для follow — берём из фасада (если есть)
try:
    from modules_shipments.shipments_leadtime_stats import get_stat_period, rebuild_events_from_states  # type: ignore
except Exception:

    async def get_stat_period() -> int:
        return 90

    async def rebuild_events_from_states() -> int:
        return 0


# Немедленная синхронизация «ведомых» складов (если функция доступна)
try:
    # ❗️исправлено: корректный модуль — shipments_leadtime_stats_data
    from modules_shipments.shipments_leadtime_stats_data import apply_stats_to_leads_for_followers  # type: ignore
except Exception:

    async def apply_stats_to_leads_for_followers() -> int:
        return 0


router = Router(name="leadtime")  # CRUD «сроки доставки»

# ─────────────────────────────────────────────────────────────────────────────
# FSM / helpers
# ─────────────────────────────────────────────────────────────────────────────


class LeadStates(StatesGroup):
    waiting_days = State()


def _now() -> str:
    # ⚙️ фикс: латинская M в минутах
    return _dt.datetime.now().strftime("%d.%m.%Y %H:%M")


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs):
    try:
        await cb.message.edit_text(text, **kwargs)
    except (TelegramBadRequest, TelegramNetworkError) as e:
        try:
            await cb.message.answer(text, **kwargs)
        except Exception:
            if "message is not modified" not in str(e):
                raise


async def _safe_send(msg: Message, text: str, **kwargs):
    try:
        await msg.answer(text, **kwargs)
    except (TelegramBadRequest, TelegramNetworkError):
        pass


async def _ack(cb: CallbackQuery) -> None:
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass
    except Exception:
        pass


# follow-status helper


def _get_follow_status(wid: int) -> Tuple[bool, int]:
    try:
        rec = get_following_wids().get(int(wid))
        if not rec:
            return (False, 0)
        return (bool(rec.get("follow_stats")), int(rec.get("follow_period") or 90))
    except Exception:
        return (False, 0)


# ───────── стартовый экран ─────────


def _lead_home_text() -> str:
    filled, total = get_progress()
    pct = (filled / total * 100.0) if total else 0.0
    return (
        "⏰ <b>Сроки доставки</b>\n"
        f"Обновлено {_now()}\n\n"
        f"Заполнено: <b>{filled}/{total}</b> складов ({pct:.0f}%)\n\n"
        "Выберите действие:"
    )


def _lead_home_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔢 По SKU", callback_data="lead:report:sku:0")],
        [InlineKeyboardButton(text="🏢 По кластерам", callback_data="lead:report:cluster:0")],
        [InlineKeyboardButton(text="📦 По складам", callback_data="lead:report:warehouse:0")],
        [InlineKeyboardButton(text="✍️ Изменить сроки доставки",                                  callback_data="lead:list:page:0")],
        [
            InlineKeyboardButton(
                text="📊 Статистика сроков доставки", callback_data="leadtime:stats"
            )
        ],
        [InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")],
        [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "lead:start")
async def lead_start(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    await state.clear()
    await state.update_data(lead_page=0, lead_selected_wid=None)
    await _safe_edit(cb, _lead_home_text(), parse_mode="HTML", reply_markup=_lead_home_kb())


# ───────── список складов (редактирование) ─────────


def _lead_list_text() -> str:
    filled, total = get_progress()
    pct = (filled / total * 100.0) if total else 0.0
    return (
        "✍️ <b>️Изменить сроки доставки — список складов</b>\n"
        f"Обновлено {_now()}\n\n"
        f"Заполнено: <b>{filled}/{total}</b> складов ({pct:.0f}%)\n"
        "• Для ускорения список использует кэш имён.\n"
        "• Нажмите «🔄 Обновить имена» (кнопка внизу), чтобы подтянуть новые/исправить плейсхолдеры.\n\n"
        "Выберите склад из списка:"
    )


def _lead_list_kb(page: int = 0) -> InlineKeyboardMarkup:
    items, total = list_warehouses_page(view_page=page, page_size=LEAD_EDIT_PAGE_SIZE)
    rows: List[List[InlineKeyboardButton]] = []

    if not items:
        rows.append(
            [
                InlineKeyboardButton(
                    text="🔄 Обновить список", callback_data=f"lead:list:page:{page}"
                )
            ]
        )
    else:
        for wid, name, indicator in items:
            rows.append(
                [
                    InlineKeyboardButton(
                        text=f"📦 {name} {indicator}", callback_data=f"lead:pick:{wid}:{page}"
                    )
                ]
            )

    pages = (total + LEAD_EDIT_PAGE_SIZE - 1) // LEAD_EDIT_PAGE_SIZE
    nav: List[InlineKeyboardButton] = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"lead:list:page:{
                    page - 1}"))
    if page + 1 < pages:
        nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"lead:list:page:{
                    page + 1}"))
    if nav:
        rows.append(nav)

    # ⬇️ Перенесено вниз: ручное обновление имён — после списка/навигации
    rows.append(
        [InlineKeyboardButton(text="🔄 Обновить склады", callback_data=f"lead:update_names:{page}")]
    )

    rows.append(
        [InlineKeyboardButton(text="◀️ В раздел «Сроки доставки»", callback_data="lead:start")]
    )
    rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "lead:list")
async def lead_list_root(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    await lead_list_page(cb, state)


@router.callback_query(F.data.startswith("lead:list:page:"))
async def lead_list_page(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    await state.update_data(lead_page=page, lead_selected_wid=None)
    await _safe_edit(cb, _lead_list_text(), parse_mode="HTML", reply_markup=_lead_list_kb(page))


# ───────── ручное обновление имён ─────────


@router.callback_query(F.data.startswith("lead:update_names:"))
async def lead_update_names(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    stats = {}
    try:
        stats = await refresh_warehouse_names() or {}
    except Exception:
        stats = {}

    upd = int(stats.get("updated") or 0)
    total = int(stats.get("total") or 0)
    info = f"🔄 Обновление имён завершено: улучшено/добавлено <b>{upd}</b> записей (всего в кэше: {total})."
    await _safe_edit(
        cb,
        f"{info}\n\n{_lead_list_text()}",
        parse_mode="HTML",
        reply_markup=_lead_list_kb(page),
    )


# ───────── карточка склада ─────────


def _lead_card_text(wid: int, draft: float | None) -> str:
    name = get_warehouse_title(wid)
    current = get_lead_for_wid(wid)
    cur_txt = "не задан" if current is None else f"{float(current):.2f} дн."
    is_follow, period = _get_follow_status(wid)
    lines = [f"🏭 {name} (ID: {wid})", f"Текущий срок доставки: {cur_txt}"]
    if is_follow:
        lines.append(f"Источник: статистика (P={period})")
    if draft is not None:
        lines.append(f"Черновик: {float(draft):.2f} дн. (не сохранён)")
    return "\n".join(lines)


def _lead_card_kb(wid: int, page: int, has_draft: bool) -> InlineKeyboardMarkup:
    is_follow, _period = _get_follow_status(wid)
    rows: List[List[InlineKeyboardButton]] = []
    rows.append(
        [
            InlineKeyboardButton(
                text="✍️ Ввести срок доставки", callback_data=f"lead:ask:{wid}:{page}"
            )
        ]
    )
    if is_follow:
        rows.append(
            [
                InlineKeyboardButton(
                    text="📉 Отключить авто-обновление",
                    callback_data=f"lead:follow:off:{wid}:{page}",
                )
            ]
        )
    else:
        # 🆕 переименовано под термин «подписка»
        rows.append(
            [
                InlineKeyboardButton(
                    text="📈 Данные из подписки", callback_data=f"lead:follow:on:{wid}:{page}"
                )
            ]
        )
    if has_draft:
        rows.append(
            [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"lead:save:{wid}:{page}")]
        )
    rows.extend(
        [
            [
                InlineKeyboardButton(
                    text="♻️ Сбросить в 0", callback_data=f"lead:reset:{wid}:{page}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Удалить запись", callback_data=f"lead:delete:{wid}:{page}"
                )
            ],
            [InlineKeyboardButton(text="◀️ К списку", callback_data=f"lead:list:page:{page}")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("lead:pick:"))
async def lead_pick(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = int(parts[2])
    page = int(parts[3]) if len(parts) >= 4 else 0
    await state.update_data(lead_selected_wid=wid, lead_page=page)
    await _safe_edit(cb, _lead_card_text(wid, None), reply_markup=_lead_card_kb(wid, page, False))


# ───────── включение/выключение подписки на статистику ─────────


@router.callback_query(F.data.startswith("lead:follow:on:"))
async def lead_follow_on(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = int(parts[3])
    page = int(parts[4]) if len(parts) >= 5 else 0
    period = await get_stat_period() or 90
    try:
        enable_follow_stats(wid, period=period, metric="avg")

        # 1) пробуем сразу подтянуть значения из статистики
        updated = int(await apply_stats_to_leads_for_followers() or 0)

        # 2) если нет событий/обновлений — регенерируем события из кэша состояний и пробуем ещё раз
        if updated == 0:
            try:
                _ = await rebuild_events_from_states()
            except Exception:
                _ = 0
            updated = int(await apply_stats_to_leads_for_followers() or 0)

        if updated > 0:
            note = f"📈 Подписка включена (P={period}). Значение обновлено из статистики."
        else:
            note = (
                f"📈 Подписка включена (P={period}). Пока нет свежих событий для этого периода — "
                "значение не изменилось."
            )
    except Exception:
        note = "⚠️ Не удалось включить подписку/синхронизацию."
    await _safe_edit(
        cb, f"{note}\n\n{_lead_card_text(wid, None)}", reply_markup=_lead_card_kb(wid, page, False)
    )


@router.callback_query(F.data.startswith("lead:follow:off:"))
async def lead_follow_off(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = int(parts[3])
    page = int(parts[4]) if len(parts) >= 5 else 0
    try:
        disable_follow_stats(wid)
        note = "📉 Подписка на статистику отключена. Теперь можно задать срок вручную."
    except Exception:
        note = "⚠️ Не удалось отключить подписку."
    await _safe_edit(
        cb, f"{note}\n\n{_lead_card_text(wid, None)}", reply_markup=_lead_card_kb(wid, page, False)
    )


# ───────── ввод значения ─────────


@router.callback_query(F.data.startswith("lead:ask:"))
async def lead_ask(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = int(parts[2])
    page = int(parts[3]) if len(parts) >= 4 else 0
    await state.update_data(lead_selected_wid=wid, lead_page=page)
    await state.set_state(LeadStates.waiting_days)
    name = get_warehouse_title(wid)
    await _safe_edit(
        cb,
        f"Введите срок доставки для:\n🏭 {name}\nЧисло дней (можно с дробной частью), ≥ 0. Пример: 1.75",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="🔙 Отмена", callback_data=f"lead:pick:{wid}:{page}")]
            ]
        ),
    )


@router.message(LeadStates.waiting_days, F.text)
async def lead_text_input(msg: Message, state: FSMContext):
    data = await state.get_data()
    wid = int(data.get("lead_selected_wid") or 0)
    page = int(data.get("lead_page") or 0)
    txt = (msg.text or "").replace(",", ".").strip()
    try:
        val = float(txt)
        if val < 0:
            raise ValueError
    except Exception:
        await _safe_send(msg, "Введите число дней ≥ 0 (например: 1.5)")
        return

    user = (msg.from_user.username or "").strip() or str(msg.from_user.id)
    res = save_lead_days(wid, val, updated_by=user)  # важно: updated_by, а не user
    if isinstance(res, tuple):
        days, info = res
        info_txt = info
    else:
        days = float(res)
        name = get_warehouse_title(wid)
        is_follow, period = _get_follow_status(wid)
        follow_note = (
            f"\n⚠️ Включена подписка на статистику (P={period}). Ручное значение может быть перезаписано."
            if is_follow
            else ""
        )
        info_txt = f"✅ Срок доставки для «{name}» сохранён: {float(days):.2f} дн.{follow_note}"

    await state.clear()
    # 🧭 Возвращаем пользователя НЕ в список, а в карточку склада
    await _safe_send(
        msg,
        f"{info_txt}\n\n{_lead_card_text(wid, None)}",
        reply_markup=_lead_card_kb(wid, page, False),
    )


@router.callback_query(F.data.startswith("lead:save:"))
async def lead_save(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    # Раньше зависели от FSM; теперь парсим из callback_data, с фолбэком на FSM
    parts = cb.data.split(":")
    wid = (
        int(parts[2])
        if len(parts) >= 3
        else int((await state.get_data()).get("lead_selected_wid") or 0)
    )
    page = int(parts[3]) if len(parts) >= 4 else int((await state.get_data()).get("lead_page") or 0)
    await _safe_edit(cb, _lead_card_text(wid, None), reply_markup=_lead_card_kb(wid, page, False))


@router.callback_query(F.data.startswith("lead:reset:"))
async def lead_reset_h(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = (
        int(parts[2])
        if len(parts) >= 3
        else int((await state.get_data()).get("lead_selected_wid") or 0)
    )
    page = int(parts[3]) if len(parts) >= 4 else int((await state.get_data()).get("lead_page") or 0)
    user = (cb.from_user.username or "").strip() or str(cb.from_user.id)
    res = reset_lead_days(wid, updated_by=user)
    if isinstance(res, tuple):
        _days, info = res
        info_txt = info
    else:
        name = get_warehouse_title(wid)
        info_txt = f"♻️ Срок доставки для «{name}» сброшен в 0.00 дн."
    # ➜ возвращаемся в карточку склада
    await _safe_edit(
        cb,
        f"{info_txt}\n\n{_lead_card_text(wid, None)}",
        reply_markup=_lead_card_kb(wid, page, False),
    )


@router.callback_query(F.data.startswith("lead:delete:"))
async def lead_delete_h(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    parts = cb.data.split(":")
    wid = (
        int(parts[2])
        if len(parts) >= 3
        else int((await state.get_data()).get("lead_selected_wid") or 0)
    )
    page = int(parts[3]) if len(parts) >= 4 else int((await state.get_data()).get("lead_page") or 0)
    res = delete_lead_record(wid)
    if isinstance(res, str) and res:
        info = res
    else:
        name = get_warehouse_title(wid)
        info = f"❌ Запись для «{name}» удалена."
    # ➜ возвращаемся в карточку склада (значение «не задано»)
    await _safe_edit(
        cb, f"{info}\n\n{_lead_card_text(wid, None)}", reply_markup=_lead_card_kb(wid, page, False)
    )


# ───────── Отчёты на основе «базы сроков» + дрилл-даун в SKU ─────────

REPORT_PAGE_SIZE = int(os.getenv("LEAD_REPORT_PAGE_SIZE", "30"))

# Вспомогательная пагинация


def _slice(rows: List, page: int, page_size: int) -> Tuple[List, int, int, int]:
    total = len(rows)
    pages = max(1, math.ceil(total / page_size))
    page = max(0, min(page, pages - 1))
    start, end = page * page_size, min(total, (page + 1) * page_size)
    return rows[start:end], total, pages, page


def _kb_with_nav(kind: str, page: int, pages: int) -> List[List[InlineKeyboardButton]]:
    nav: List[InlineKeyboardButton] = []
    if pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"lead:report:{kind}:{
                        page - 1}"))
        if page + 1 < pages:
            nav.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"lead:report:{kind}:{
                        page + 1}"))
    rows: List[List[InlineKeyboardButton]] = []
    if nav:
        rows.append(nav)
    rows.append(
        [InlineKeyboardButton(text="◀️ В раздел «Сроки доставки»", callback_data="lead:start")]
    )
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return rows


@router.callback_query(F.data.regexp(r"^lead:report:(warehouse|cluster|sku):\d+$"))
async def lead_report(cb: CallbackQuery):
    await _ack(cb)
    parts = cb.data.split(":")
    kind = parts[2]
    page = int(parts[3])

    if kind == "warehouse":
        title = "📦 <b>По складам</b>"
        rows = list_enabled_warehouses_for_report()  # [(wid, name, days)]
        if not rows:
            text = f"{title}\n⏱ Обновлено: {
                _now()}\n\nℹ️ Нет включённых складов. Введите срок вручную или включите подписку."
            await _safe_edit(
                cb,
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_with_nav("warehouse", 0, 1)),
            )
            return
        slice_rows, _total, pages, page = _slice(rows, page, REPORT_PAGE_SIZE)
        kb_items = [
            [
                InlineKeyboardButton(
                    text=f"🏭 {name} — ∅={days:.2f} дн", callback_data=f"lead:sku:wh:{int(wid)}:0"
                )
            ]
            for wid, name, days in slice_rows
        ]
        kb_items += _kb_with_nav("warehouse", page, pages)
        text = f"{title}\n⏱ Обновлено: {_now()}\n\nВыберите склад:"
        await _safe_edit(
            cb, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_items)
        )
        return

    if kind == "cluster":
        title = "🏢 <b>По кластерам</b>"
        rows = list_enabled_clusters_for_report()  # [(cid,name,avg,n)]
        if not rows:
            text = f"{title}\n⏱ Обновлено: {
                _now()}\n\nℹ️ Нет включённых складов (данные по кластерам недоступны)."
            await _safe_edit(
                cb,
                text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_with_nav("cluster", 0, 1)),
            )
            return
        slice_rows, _total, pages, page = _slice(rows, page, REPORT_PAGE_SIZE)
        kb_items = [
            [
                InlineKeyboardButton(
                    text=f"🏢 {name} — ∅={avg:.2f} дн (N={n})",
                    callback_data=f"lead:sku:cl:{int(cid)}:0",
                )
            ]
            for cid, name, avg, n in slice_rows
        ]
        kb_items += _kb_with_nav("cluster", page, pages)
        text = f"{title}\n⏱ Обновлено: {_now()}\n\nВыберите кластер:"
        await _safe_edit(
            cb, text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_items)
        )
        return

    title = "🔢 <b>По SKU</b>"
    rows = await manual_view_by_sku()  # [(sku, alias, avg, n)]
    slice_rows, _total, pages, page = _slice(rows, page, REPORT_PAGE_SIZE)
    body = (
        "\n".join(f"🔹 {alias} — ∅={avg:.2f} дн" for _sku, alias, avg, _n in slice_rows)
        or "ℹ️ Нет данных по SKU."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=_kb_with_nav("sku", page, pages))
    text = f"{title}\n⏱ Обновлено: {_now()}\n\n{body}"
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)


__all__ = ["router"]
