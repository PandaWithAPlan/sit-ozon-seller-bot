# handlers/handlers_shipments_status.py
from __future__ import annotations
from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# Пытаемся импортировать «родные» функции статуса.
# Если пакет недоступен/ломается — включаем безопасный фолбэк ниже.
try:
    from modules_shipments import shipments_status_text, shipments_status_text_group  # type: ignore
except Exception:
    shipments_status_text = None  # type: ignore
    shipments_status_text_group = None  # type: ignore

# ⬇️ Источник stocks перенесён в shipments_report_data (жёсткая фильтрация по WATCH_SKU)
try:
    from modules_shipments.shipments_report_data import fetch_stocks_view, metrics_display_pairs, total_on_ozon_from_row  # type: ignore
except Exception:
    # Легаси‑фолбэк (на случай старых установок)
    try:
        from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore

        def metrics_display_pairs(row):
            return []

        def total_on_ozon_from_row(row):
            return 0.0

    except Exception:
        # type: ignore
        def fetch_stocks_view(
            view: str = "cluster", force: bool = False, skus: list[int] | None = None
        ):
            return []

        def metrics_display_pairs(row):
            return []

        def total_on_ozon_from_row(row):
            return 0.0


# Алиас для SKU (фолбэк)
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


router = Router()
TG_MAX = 4096

# ───────── helpers ─────────


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs):
    try:
        await cb.message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


async def _safe_answer(cb: CallbackQuery, *args, **kwargs):
    try:
        await cb.answer(*args, **kwargs)
    except TelegramBadRequest:
        pass


def _title_for_view(view: str) -> str:
    return "Кластер" if view == "cluster" else "Склад"


def _is_zero_like(s: str) -> bool:
    s_norm = (s or "").strip().lower()
    if not s_norm:
        return True
    if s_norm in {"0", "none", "null"}:
        return True
    if s_norm.isdigit() and int(s_norm) == 0:
        return True
    return False


def _looks_module_missing(text: str | None) -> bool:
    """Признаки ответа-заглушки от агрегатора ('модуль отчёта недоступен')."""
    s = (text or "").lower()
    return ("модуль отчёта" in s) or ("отсутствует модуль" in s) or ("временно недоступен" in s)


def _row_group_key(view: str, row: dict) -> str:
    def _first_nonempty(lst):
        for v in lst:
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    dims = row.get("dimensions") or row.get("dimension") or []
    dims_vals = []
    if isinstance(dims, list):
        for d in dims:
            k = str(d.get("key") or "").lower()
            if k in {"cluster", "warehouse", "name", "title"}:
                dims_vals.append(d.get("value"))
            dims_vals.append(d.get("id"))

    if view == "cluster":
        name = _first_nonempty(
            [
                row.get("cluster_name"),
                row.get("cluster"),
                row.get("cluster_title"),
                row.get("cluster_id"),
                *dims_vals,
            ]
        )
    else:
        name = _first_nonempty(
            [
                row.get("warehouse_name"),
                row.get("warehouse"),
                row.get("warehouse_title"),
                row.get("warehouse_id"),
                *dims_vals,
            ]
        )

    if _is_zero_like(name):
        return "unknown"
    return name or "unknown"


def _extract_groups(view: str) -> list[str]:
    rows = fetch_stocks_view(view=view) or []
    groups = set()
    for r in rows:
        key = _row_group_key(view, r)
        if key and key.lower() != "unknown" and not _is_zero_like(key):
            groups.add(key)
    lst = sorted(groups, key=lambda s: s.lower())[:500]
    return lst


def _view_switch_rows() -> list[list[InlineKeyboardButton]]:
    return [
        [InlineKeyboardButton(text="🔢 По SKU", callback_data="shipments:view:sku")],
        [InlineKeyboardButton(text="🏢 По кластерам", callback_data="shipments:view:cluster")],
        [InlineKeyboardButton(text="🏭 По складам", callback_data="shipments:view:warehouse")],
    ]


def _groups_menu(
    view: str, groups: list[str], page: int = 0, page_size: int = 20
) -> InlineKeyboardMarkup:
    start = max(page, 0) * page_size
    chunk = groups[start : start + page_size]
    rows: list[list[InlineKeyboardButton]] = []

    for idx, name in enumerate(chunk, start=start):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"📊 {name}", callback_data=f"shipments:group:{view}:pick:{idx}"
                )
            ]
        )

    nav: list[InlineKeyboardButton] = []
    if start > 0:
        nav.append(
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"shipments:group:{view}:page:{
                    page - 1}")
        )
    if start + page_size < len(groups):
        nav.append(
            InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"shipments:group:{view}:page:{
                    page + 1}")
        )
    if nav:
        rows.append(nav)

    rows.extend(_view_switch_rows())
    rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────── Фолбэк-рендеры для «Статуса отгрузок», если основной модуль недоступен ─────────


def _extract_sku(row: dict) -> int:
    for k in ("sku", "SKU", "id"):
        v = row.get(k)
        if v is not None and str(v).strip().isdigit():
            return int(v)
    dims = row.get("dimensions") or row.get("dimension") or []
    if isinstance(dims, list):
        for d in dims:
            if str(d.get("key") or "").lower() == "sku":
                v = d.get("id") or d.get("value")
                if v is not None and str(v).strip().isdigit():
                    return int(v)
    return 0


def _fallback_status_text(view: str = "sku") -> str:
    # По SKU: короткая витрина топ‑позиции по общему наличию
    rows = fetch_stocks_view(view="sku") or []
    if not rows:
        return "🚚 <b>Статус отгрузок</b>\nℹ️ Нет данных для отображения."
    bucket = {}
    for r in rows:
        sku = _extract_sku(r)
        if not sku:
            continue
        bucket.setdefault(sku, 0.0)
        try:
            bucket[sku] += float(total_on_ozon_from_row(r))
        except Exception:
            pass
    items = sorted(bucket.items(), key=lambda kv: -kv[1])[:80]
    lines = []
    for sku, total in items:
        alias = (get_alias_for_sku(int(sku)) or str(sku)).strip() or str(sku)
        lines.append(f"🔹 {alias}: {int(total)} ед.")
    head = "🚚 <b>Статус отгрузок (укороченная версия)</b>\nПо SKU (топ по наличию):\n"
    return head + ("\n".join(lines) if lines else "—")


def _fallback_status_text_group(view: str, name: str) -> str:
    rows = fetch_stocks_view(view=view) or []
    if not rows:
        return f"🚚 <b>Статус отгрузок</b>\nℹ️ Нет данных по «{name}»."
    grp = []
    for r in rows:
        if _row_group_key(view, r) == name:
            grp.append(r)
    if not grp:
        return f"🚚 <b>Статус отгрузок</b>\nℹ️ Нет данных по «{name}»."
    # Выведем позиции SKU и ключевые метрики для группы
    lines = [f"🚚 <b>{_title_for_view(view)}:</b> {name}", ""]
    # SKU → сумма по группе
    agg = {}
    for r in grp:
        sku = _extract_sku(r)
        if not sku:
            continue
        agg.setdefault(sku, 0.0)
        agg[sku] += float(total_on_ozon_from_row(r))
    for sku, total in sorted(agg.items(), key=lambda kv: -kv[1])[:80]:
        alias = (get_alias_for_sku(int(sku)) or str(sku)).strip() or str(sku)
        lines.append(f"🔹 {alias}: {int(total)} ед.")
    return "\n".join(lines)


# ───────── витрина «Статус отгрузок» ─────────


@router.callback_query(F.data == "shipments:onsale")
async def on_shipments_onsale(cb: CallbackQuery, state: FSMContext):
    await state.update_data(
        step="ship_onsale", ship_view="sku", ship_groups=None, ship_groups_page=0
    )
    text = "🚚 <b>Статус отгрузок</b>\nВыберите представление:"
    rows = _view_switch_rows()
    rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)
    await _safe_answer(cb)


@router.callback_query(
    F.data.in_({"shipments:view:sku", "shipments:view:cluster", "shipments:view:warehouse"})
)
async def on_shipments_view_switch(cb: CallbackQuery, state: FSMContext):
    view = cb.data.split(":")[-1]
    await state.update_data(ship_view=view, step="ship_onsale")

    if view in {"cluster", "warehouse"}:
        groups = _extract_groups(view)
        rows = _view_switch_rows()
        if not groups:
            rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
            rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
            kb_empty = InlineKeyboardMarkup(inline_keyboard=rows)
            await _safe_edit(
                cb,
                f"🚚 Статус отгрузок — {_title_for_view(view)}: данных нет.",
                reply_markup=kb_empty,
            )
            await _safe_answer(cb)
            return
        await state.update_data(ship_groups=groups, ship_groups_page=0)
        kb = _groups_menu(view, groups, page=0)
        await _safe_edit(
            cb, f"🚚 Статус отгрузок — выберите {_title_for_view(view).lower()}:", reply_markup=kb
        )
        await _safe_answer(cb)
        return

    # По SKU — используем основной модуль, либо фолбэк
    if callable(shipments_status_text):
        text = shipments_status_text(view=view)  # type: ignore
        if _looks_module_missing(text):
            text = _fallback_status_text(view=view)
    else:
        text = _fallback_status_text(view=view)

    rows = _view_switch_rows()
    rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await _safe_edit(cb, (text or "")[:TG_MAX], parse_mode="HTML", reply_markup=kb)
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("shipments:group:") & F.data.contains(":page:"))
async def on_shipments_group_page(cb: CallbackQuery, state: FSMContext):
    _, _, view, _, page_str = cb.data.split(":")
    page = max(int(page_str), 0)
    data = await state.get_data()
    groups = data.get("ship_groups") or _extract_groups(view)
    await state.update_data(ship_groups=groups, ship_groups_page=page)
    kb = _groups_menu(view, groups, page=page)
    await _safe_edit(
        cb, f"🚚 Статус отгрузок — выберите {_title_for_view(view).lower()}:", reply_markup=kb
    )
    await _safe_answer(cb)


@router.callback_query(F.data.startswith("shipments:group:") & F.data.contains(":pick:"))
async def on_shipments_group_pick(cb: CallbackQuery, state: FSMContext):
    _, _, view, _, idx_str = cb.data.split(":")
    idx = int(idx_str)

    data = await state.get_data()
    groups = data.get("ship_groups") or _extract_groups(view)
    page = int(data.get("ship_groups_page") or 0)

    if idx < 0 or idx >= len(groups):
        await _safe_answer(cb, "Не найдено")
        return

    group_name = groups[idx]
    if callable(shipments_status_text_group):
        text = shipments_status_text_group(view, group_name)  # type: ignore
        if _looks_module_missing(text):
            text = _fallback_status_text_group(view, group_name)
    else:
        text = _fallback_status_text_group(view, group_name)

    kb = _groups_menu(view, groups, page=page)
    await _safe_edit(cb, (text or "")[:TG_MAX], parse_mode="HTML", reply_markup=kb)
    await _safe_answer(cb)
