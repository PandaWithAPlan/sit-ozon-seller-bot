# handlers/handlers_shipments_need.py
from __future__ import annotations

import os
import json
import datetime as _dt
from typing import List, Optional, Tuple, Dict

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.exceptions import TelegramBadRequest

from modules_shipments.shipments_need import compute_need, format_need_text, export_need_excel

# календарь выбора даты отгрузки
try:
    from modules_common.inline_calendar import shipments_calendar_kb  # type: ignore
except Exception:
    # 🔧 Фолбэк на универсальный календарь из modules_common.calendar
    try:
        from modules_common.calendar import shipments_calendar_kb  # type: ignore
    except Exception:
        shipments_calendar_kb = None  # type: ignore

# справочник складов
try:
    from modules_shipments.shipments_need_data import get_warehouses_map  # type: ignore
except Exception:

    def get_warehouses_map() -> Dict[int, Tuple[str, int, str]]:
        return {}


# ⚙️ Положительный спрос (ΣD/день > 0) — для фильтра складов/кластеров в разделе «Необходимо отгрузить»
try:
    from modules_shipments.shipments_demand import get_positive_demand_wids  # type: ignore
except Exception:

    def get_positive_demand_wids(_period_days: int | None = None) -> List[int]:
        return []


# единая точка выбора каталога отчётов
from modules_common.paths import resolve_reports_dir

router = Router(name="shipments_need")
TG_MAX = 4096

# ─────────────────────────────────────────────────────────────────────────────
# Пути к кэшу
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
MODULES_SHIP_DIR = os.path.join(BASE_DIR, "modules_shipments")

DISPATCH_PREFS_PATH = os.path.join(
    MODULES_SHIP_DIR, "data", "cache", "common", "need_dispatch.json"
)
CLOSED_WH_PATH = os.path.join(MODULES_SHIP_DIR,                                  "data",                                  "cache",                                  "common",                                  "closed_warehouses.json")
os.makedirs(os.path.dirname(DISPATCH_PREFS_PATH), exist_ok=True)
os.makedirs(os.path.dirname(CLOSED_WH_PATH), exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# JSON utils
# ─────────────────────────────────────────────────────────────────────────────


def _read_json(path: str) -> dict:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _write_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch date (S) — хранение/визуал
# ─────────────────────────────────────────────────────────────────────────────


def _load_dispatch() -> dict:
    """
    {"date":"YYYY-MM-DD","days":0,"view_year":YYYY,"view_month":M}
    """
    d = _read_json(DISPATCH_PREFS_PATH)
    return d if isinstance(d, dict) else {}


def _save_dispatch(
    date_iso: Optional[str],
    days: int,
    *,
    view_year: Optional[int] = None,
    view_month: Optional[int] = None,
) -> dict:
    payload = _load_dispatch()
    if date_iso is not None or "date" in payload:
        payload["date"] = date_iso  # допускаем None для сброса
    payload["days"] = max(0, int(days))
    if view_year is not None:
        payload["view_year"] = int(view_year)
    if view_month is not None:
        payload["view_month"] = int(view_month)
    _write_json(DISPATCH_PREFS_PATH, payload)
    return payload


def _calc_S_days(target_iso: Optional[str]) -> Tuple[int, Optional[str]]:
    if not target_iso:
        return 0, None
    try:
        target = _dt.datetime.strptime(target_iso, "%Y-%m-%d").date()
    except Exception:
        return 0, None
    today = _dt.date.today()
    delta = (target - today).days
    return (delta if delta > 0 else 0), target_iso


def _roll_view_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    m0 = month - 1 + delta
    y = year + m0 // 12
    m = m0 % 12 + 1
    return y, m


# ─────────────────────────────────────────────────────────────────────────────
# Closed warehouses — хранение
# ─────────────────────────────────────────────────────────────────────────────


def _load_closed_wids() -> List[int]:
    js = _read_json(CLOSED_WH_PATH)
    closed = js.get("closed") or []
    out: List[int] = []
    for x in closed:
        try:
            out.append(int(x))
        except Exception:
            continue
    return out


def _save_closed_wids(closed: List[int]) -> None:
    payload = {
        "closed": sorted(set(int(x) for x in closed if isinstance(x, (int,)))),
        "updated_at": _dt.datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    _write_json(CLOSED_WH_PATH, payload)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


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


# ─────────────────────────────────────────────────────────────────────────────
# Навигационная клавиатура отчёта (кнопки в столбик; перелистывание — СВЕРХУ)
# ─────────────────────────────────────────────────────────────────────────────


def _kb_need_root(scope: str, *, page: int = 0, pages: int = 1) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    if pages > 1:
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(InlineKeyboardButton(text="« Назад", callback_data=f"need:page:{scope}:{
                        page - 1}"))
        if page < pages - 1:
            nav_row.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"need:page:{scope}:{
                        page + 1}"))
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="🔢 По SKU", callback_data="need:view:sku")])
    rows.append([InlineKeyboardButton(text="🏢 По кластерам", callback_data="need:view:cluster")])
    rows.append([InlineKeyboardButton(text="🏭 По складам", callback_data="need:view:warehouse")])
    rows.append([InlineKeyboardButton(text="🗓 Дата отгрузки", callback_data="need:date")])
    rows.append(
        [InlineKeyboardButton(text="🚫 Закрытые склады", callback_data="need:closed:page:0")]
    )
    rows.append(
        [InlineKeyboardButton(text="📥 Экспорт в Excel", callback_data=f"need:export:{scope}")]
    )
    rows.append([InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_need_filtered(
    scope: str, *, page: int, pages: int, filter_kind: str, filter_id: int
) -> InlineKeyboardMarkup:
    """
    Клавиатура отчёта с фильтром (кластер/склад).
    Экспорт: need:export:<scope>:<filter_kind>:<filter_id>
    Пагинация: need:page:<scope>:<filter_kind>:<filter_id>:<page>
    """
    rows: List[List[InlineKeyboardButton]] = []
    if pages > 1:
        nav_row: List[InlineKeyboardButton] = []
        if page > 0:
            nav_row.append(
                InlineKeyboardButton(
                    text="« Назад",
                    callback_data=f"need:page:{scope}:{filter_kind}:{filter_id}:{page - 1}",
                )
            )
        if page < pages - 1:
            nav_row.append(
                InlineKeyboardButton(
                    text="Вперёд »",
                    callback_data=f"need:page:{scope}:{filter_kind}:{filter_id}:{page + 1}",
                )
            )
        rows.append(nav_row)

    if filter_kind == "cluster":
        rows.append(
            [InlineKeyboardButton(text="↩️ Кластеры", callback_data="need:clusters:page:0")]
        )
    else:
        rows.append([InlineKeyboardButton(text="↩️ Склады", callback_data="need:whs:page:0")])

    rows.append([InlineKeyboardButton(text="🔢 По SKU", callback_data="need:view:sku")])
    rows.append([InlineKeyboardButton(text="🗓 Дата отгрузки", callback_data="need:date")])
    rows.append(
        [InlineKeyboardButton(text="🚫 Закрытые склады", callback_data="need:closed:page:0")]
    )
    rows.append(
        [
            InlineKeyboardButton(
                text="📥 Экспорт в Excel",
                callback_data=f"need:export:{scope}:{filter_kind}:{filter_id}",
            )
        ]
    )
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────────────────────────────────────
# Меню выбора кластеров / складов (по 10 на страницу; пагинация — ПОД списком)
# ─────────────────────────────────────────────────────────────────────────────


def _clusters_pages(page_size: int = 10) -> List[List[int]]:
    """
    ⚙️ Фильтруем кластеры по наличию хотя бы одного склада с положительным спросом (ΣD/день > 0).
    """
    wm = get_warehouses_map()
    allowed = set(get_positive_demand_wids() or [])
    seen: Dict[int, str] = {}
    for wid, (_wname, cid, cname) in wm.items():
        if not cid:
            continue
        # если список разрешённых пуст — показываем все (фолбэк),
        # иначе — только кластеры, где есть разрешённый склад
        if allowed and wid not in allowed:
            continue
        if cid not in seen:
            seen[cid] = cname or f"Кластер {cid}"
    cids = sorted(seen.keys(), key=lambda x: seen[x])
    pages = [cids[i : i + page_size] for i in range(0, len(cids), page_size)]
    return pages or [[]]


def _kb_clusters(page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    pages = _clusters_pages(page_size)
    page = max(0, min(page, len(pages) - 1))
    wm = get_warehouses_map()
    cid2name: Dict[int, str] = {}
    for _wid, (_wn, cid, cname) in wm.items():
        if cid and cid not in cid2name:
            cid2name[int(cid)] = cname or f"Кластер {cid}"

    rows: List[List[InlineKeyboardButton]] = []
    for cid in pages[page]:
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"🌐 {cid2name.get(cid, f'Кластер {cid}')}",
                    callback_data=f"need:cluster:{cid}",
                )
            ]
        )
    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="« Назад", callback_data=f"need:clusters:page:{
                    page - 1}"))
    nav_row.append(
        InlineKeyboardButton(
            text=f"Стр. {page + 1}/{len(pages)}", callback_data="need:clusters:nop"
        )
    )
    if page < len(pages) - 1:
        nav_row.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"need:clusters:page:{
                    page + 1}"))
    rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="↩️ К отчёту", callback_data="shipments:need")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _wh_pages(page_size: int = 10) -> List[List[int]]:
    """
    ⚙️ В меню «По складам» выводим только склады с положительным спросом (ΣD/день > 0).
    При отсутствии такого списка (фолбэк) — выводим все.
    """
    wm = get_warehouses_map()
    allowed = set(get_positive_demand_wids() or [])
    all_sorted = sorted(wm.keys(), key=lambda w: (wm[w][2], wm[w][0], int(w)))
    if allowed:
        wids = [w for w in all_sorted if w in allowed]
    else:
        wids = all_sorted
    return [wids[i : i + page_size] for i in range(0, len(wids), page_size)] or [[]]


def _kb_whs(page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    pages = _wh_pages(page_size)
    page = max(0, min(page, len(pages) - 1))
    wm = get_warehouses_map()
    closed = set(_load_closed_wids())

    rows: List[List[InlineKeyboardButton]] = []
    for wid in pages[page]:
        wname, _cid, cname = wm.get(wid, (f"Склад {wid}", 0, "C?"))
        mark = "🚫 " if wid in closed else ""
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{mark}🏬 {cname} • {wname}", callback_data=f"need:wh:{wid}"
                )
            ]
        )
    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="« Назад", callback_data=f"need:whs:page:{
                    page - 1}"))
    nav_row.append(
        InlineKeyboardButton(text=f"Стр. {page + 1}/{len(pages)}", callback_data="need:whs:nop")
    )
    if page < len(pages) - 1:
        nav_row.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"need:whs:page:{
                    page + 1}"))
    rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="↩️ К отчёту", callback_data="shipments:need")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─────────────────────────────────────────────────────────────────────────────
# Пагинация текста: только если > TG_MAX
# ─────────────────────────────────────────────────────────────────────────────


def _split_head_body_legend(text: str) -> Tuple[List[str], List[str], List[str]]:
    lines = text.splitlines()
    leg_idx = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("Легенда:"):
            leg_idx = i
            break
    if leg_idx is None:
        head = lines[:5] if len(lines) >= 5 else lines[:]
        body = lines[5:]
        legend = []
        return head, body, legend
    head = lines[:5] if len(lines) >= 5 else lines[:leg_idx]
    legend = lines[leg_idx:]
    body = lines[len(head) : leg_idx]
    while body and body[0] == "":
        body = body[1:]
    while body and body[-1] == "":
        body = body[:-1]
    return head, body, legend


def _split_body_into_unit_blocks(body: List[str]) -> List[List[str]]:
    blocks: List[List[str]] = []
    cur: List[str] = []

    def _is_title_line(s: str) -> bool:
        s = s.strip()
        return (
            s.startswith("✅ ")
            or s.startswith("🔴 ")
            or s.startswith("🟠 ")
            or s.startswith("🟢 ")
            or s.startswith("🟥 ")
            or s.startswith("🟨 ")
            or s.startswith("🟩 ")
            or s.startswith("🚫 ")
        ) and s.endswith(":")

    for ln in body:
        if _is_title_line(ln):
            if cur:
                if cur[-1] != "":
                    cur.append("")
                blocks.append(cur)
                cur = []
        cur.append(ln)
        if ln == "" and cur:
            blocks.append(cur)
            cur = []
    if cur:
        if cur[-1] != "":
            cur.append("")
        blocks.append(cur)
    return [b for b in blocks if any(x.strip() for x in b)]


def _paginate_only_if_needed(full_text: str) -> List[str]:
    if len(full_text) <= TG_MAX:
        return [full_text]
    head, body, legend = _split_head_body_legend(full_text)
    blocks = _split_body_into_unit_blocks(body)
    pages: List[str] = []
    base_head = "\n".join(head)
    base_legend = "\n".join(legend) if legend else ""
    head_len = len(base_head) + 1
    legend_len = (1 + len(base_legend)) if base_legend else 0
    max_cards_len = TG_MAX - head_len - legend_len
    if max_cards_len < 200:
        max_cards_len = max(200, TG_MAX // 2)

    curr_lines: List[str] = []
    curr_len = 0

    def _flush():
        page_lines = head[:] + curr_lines[:]
        if not page_lines or page_lines[-1] != "":
            page_lines.append("")
        if legend:
            page_lines += legend
        pages.append("\n".join(page_lines))

    for b in blocks:
        block_text = "\n".join(b)
        blen = len(block_text) + 1
        if curr_len == 0:
            curr_lines += b
            curr_len += blen
            continue
        if curr_len + blen <= max_cards_len:
            curr_lines += b
            curr_len += blen
        else:
            _flush()
            curr_lines = b[:]
            curr_len = blen
    if curr_lines:
        _flush()
    return pages


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательная обёртка для календаря: добавляем кнопку «Снять дату»
# ─────────────────────────────────────────────────────────────────────────────


def _calendar_with_clear(kb: InlineKeyboardMarkup) -> InlineKeyboardMarkup:
    try:
        base_rows = [row[:] for row in (kb.inline_keyboard or [])]
    except Exception:
        base_rows = []
    base_rows.append(
        [InlineKeyboardButton(text="❌ Снять дату отгрузки", callback_data="need:date:clear")]
    )
    base_rows.append([InlineKeyboardButton(text="↩️ К отчёту", callback_data="shipments:need")])
    return InlineKeyboardMarkup(inline_keyboard=base_rows)


# ─────────────────────────────────────────────────────────────────────────────
# Entry
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "shipments:need")
async def need_root(cb: CallbackQuery):
    await _safe_answer(cb)
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    scope = "sku"
    data = compute_need(scope, dispatch_days=S)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    page_idx = 0
    await _safe_edit(
        cb,
        pages[page_idx],
        parse_mode="HTML",
        reply_markup=_kb_need_root(scope, page=page_idx, pages=len(pages)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Switch views
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "need:view:sku")
async def need_view_sku(cb: CallbackQuery):
    await _safe_answer(cb)
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    data = compute_need("sku", dispatch_days=S)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb, pages[0], parse_mode="HTML", reply_markup=_kb_need_root("sku", page=0, pages=len(pages))
    )


@router.callback_query(F.data == "need:view:cluster")
async def need_view_clusters_menu(cb: CallbackQuery):
    await _safe_answer(cb)
    text = "<b>Выберите кластер</b>\nПокажем детализацию по SKU."
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_kb_clusters(page=0))


@router.callback_query(F.data == "need:view:warehouse")
async def need_view_wh_menu(cb: CallbackQuery):
    await _safe_answer(cb)
    text = "<b>Выберите склад</b>\nПокажем детализацию по SKU."
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_kb_whs(page=0))


@router.callback_query(F.data.startswith("need:clusters:page:"))
async def need_clusters_page(cb: CallbackQuery):
    await _safe_answer(cb)
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    text = "<b>Выберите кластер</b>\nПокажем детализацию по SKU."
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_kb_clusters(page=page))


@router.callback_query(F.data.startswith("need:whs:page:"))
async def need_whs_page(cb: CallbackQuery):
    await _safe_answer(cb)
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    text = "<b>Выберите склад</b>\nПокажем детализацию по SKU."
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=_kb_whs(page=page))


# выбор кластера/склада → детализация по SKU


@router.callback_query(F.data.startswith("need:cluster:"))
async def need_cluster_detail(cb: CallbackQuery):
    await _safe_answer(cb)
    try:
        cid = int(cb.data.split(":")[-1])
    except Exception:
        cid = None
    if cid is None:
        return
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    data = compute_need("sku", dispatch_days=S, filter_cluster=cid)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb,
        pages[0],
        parse_mode="HTML",
        reply_markup=_kb_need_filtered(
            "sku", page=0, pages=len(pages), filter_kind="cluster", filter_id=cid
        ),
    )


@router.callback_query(F.data.startswith("need:wh:"))
async def need_wh_detail(cb: CallbackQuery):
    await _safe_answer(cb)
    try:
        wid = int(cb.data.split(":")[-1])
    except Exception:
        wid = None
    if wid is None:
        return
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    data = compute_need("sku", dispatch_days=S, filter_warehouse=wid)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb,
        pages[0],
        parse_mode="HTML",
        reply_markup=_kb_need_filtered(
            "sku", page=0, pages=len(pages), filter_kind="warehouse", filter_id=wid
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Переход по страницам отчёта (без/с фильтром)
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("need:page:"))
async def need_page(cb: CallbackQuery):
    await _safe_answer(cb)
    parts = cb.data.split(":")
    # варианты:
    # need:page:<scope>:<page>
    # need:page:<scope>:<filter_kind>:<filter_id>:<page>
    if len(parts) < 4:
        return
    scope = parts[2]
    filter_kind = None
    filter_id: Optional[int] = None
    try:
        if len(parts) == 4:
            page_idx = int(parts[3])
        elif len(parts) >= 6:
            filter_kind = parts[3]
            filter_id = int(parts[4])
            page_idx = int(parts[5])
        else:
            page_idx = 0
    except Exception:
        page_idx = 0

    disp = _load_dispatch()
    S = int(disp.get("days") or 0)

    if filter_kind == "cluster" and filter_id is not None:
        data = compute_need(scope, dispatch_days=S, filter_cluster=filter_id)
    elif filter_kind == "warehouse" and filter_id is not None:
        data = compute_need(scope, dispatch_days=S, filter_warehouse=filter_id)
    else:
        data = compute_need(scope, dispatch_days=S)

    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    page_idx = max(0, min(page_idx, len(pages) - 1))

    if filter_kind in {"cluster", "warehouse"} and filter_id is not None:
        kb = _kb_need_filtered(
            scope, page=page_idx, pages=len(pages), filter_kind=filter_kind, filter_id=filter_id
        )
    else:
        kb = _kb_need_root(scope, page=page_idx, pages=len(pages))
    await _safe_edit(cb, pages[page_idx], parse_mode="HTML", reply_markup=kb)


# ─────────────────────────────────────────────────────────────────────────────
# Dispatch date selection (S) — календарь
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "need:date")
async def need_date(cb: CallbackQuery):
    await _safe_answer(cb)
    if shipments_calendar_kb is None:
        await cb.message.answer("🗓 Календарь недоступен в этой сборке.")
        return
    disp = _load_dispatch()
    selected = disp.get("date") or None
    if disp.get("view_year") and disp.get("view_month"):
        vy, vm = int(disp["view_year"]), int(disp["view_month"])
    else:
        base = _dt.date.today()
        if selected:
            try:
                base = _dt.datetime.strptime(selected, "%Y-%m-%d").date()
            except Exception:
                pass
        vy, vm = base.year, base.month
        _save_dispatch(disp.get("date"), int(disp.get("days") or 0), view_year=vy, view_month=vm)
    kb = shipments_calendar_kb(
        prefix="ship:cal", selected=selected, year=vy, month=vm
    )  # type: ignore
    kb = _calendar_with_clear(kb)  # 🔧 добавили кнопку «Снять дату»
    text = (
        "<b>Выберите дату отгрузки</b>\n"
        "После подтверждения рассчитаем S — число дней до отгрузки\n"
        "и применим во всех расчётах (лаг до отгрузки)."
    )
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data.startswith("ship:cal:pick:"))
async def need_date_pick(cb: CallbackQuery):
    await _safe_answer(cb)
    iso = cb.data.split(":")[-1]
    try:
        d = _dt.datetime.strptime(iso, "%Y-%m-%d").date()
        vy, vm = d.year, d.month
    except Exception:
        t = _dt.date.today()
        vy, vm = t.year, t.month
    disp = _load_dispatch()
    _save_dispatch(iso, int(disp.get("days") or 0), view_year=vy, view_month=vm)
    if shipments_calendar_kb:
        kb = shipments_calendar_kb(
            prefix="ship:cal", selected=iso, year=vy, month=vm
        )  # type: ignore
        kb = _calendar_with_clear(kb)  # 🔧 держим кнопку «Снять дату»
        await _safe_edit(cb, cb.message.html_text or "🗓", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "ship:cal:prev")
async def need_date_prev(cb: CallbackQuery):
    await _safe_answer(cb)
    disp = _load_dispatch()
    selected = disp.get("date") or None
    vy = int(disp.get("view_year") or _dt.date.today().year)
    vm = int(disp.get("view_month") or _dt.date.today().month)
    vy, vm = _roll_view_month(vy, vm, -1)
    _save_dispatch(disp.get("date"), int(disp.get("days") or 0), view_year=vy, view_month=vm)
    if shipments_calendar_kb:
        kb = shipments_calendar_kb(
            prefix="ship:cal", selected=selected, year=vy, month=vm
        )  # type: ignore
        kb = _calendar_with_clear(kb)  # 🔧 держим кнопку «Снять дату»
        await _safe_edit(cb, cb.message.html_text or "🗓", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "ship:cal:next")
async def need_date_next(cb: CallbackQuery):
    await _safe_answer(cb)
    disp = _load_dispatch()
    selected = disp.get("date") or None
    vy = int(disp.get("view_year") or _dt.date.today().year)
    vm = int(disp.get("view_month") or _dt.date.today().month)
    vy, vm = _roll_view_month(vy, vm, +1)
    _save_dispatch(disp.get("date"), int(disp.get("days") or 0), view_year=vy, view_month=vm)
    if shipments_calendar_kb:
        kb = shipments_calendar_kb(
            prefix="ship:cal", selected=selected, year=vy, month=vm
        )  # type: ignore
        kb = _calendar_with_clear(kb)  # 🔧 держим кнопку «Снять дату»
        await _safe_edit(cb, cb.message.html_text or "🗓", parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "ship:cal:confirm")
async def need_date_confirm(cb: CallbackQuery):
    await _safe_answer(cb, "Дата принята")
    disp = _load_dispatch()
    iso = disp.get("date")
    S, iso_ok = _calc_S_days(iso)
    _save_dispatch(iso_ok, S)
    scope = "sku"
    data = compute_need(scope, dispatch_days=S)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb, pages[0], parse_mode="HTML", reply_markup=_kb_need_root(scope, page=0, pages=len(pages))
    )


@router.callback_query(F.data == "ship:cal:cancel")
async def need_date_cancel(cb: CallbackQuery):
    await _safe_answer(cb, "Отмена")
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    scope = "sku"
    data = compute_need(scope, dispatch_days=S)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb, pages[0], parse_mode="HTML", reply_markup=_kb_need_root(scope, page=0, pages=len(pages))
    )


# СБРОС ДАТЫ


@router.callback_query(F.data == "need:date:clear")
async def need_date_clear(cb: CallbackQuery):
    await _safe_answer(cb, "Дата отгрузки снята")
    _save_dispatch(None, 0)  # S=0 и date=None
    data = compute_need("sku", dispatch_days=0)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb, pages[0], parse_mode="HTML", reply_markup=_kb_need_root("sku", page=0, pages=len(pages))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Closed warehouses UI
# ─────────────────────────────────────────────────────────────────────────────


def _closed_wh_pages(page_size: int = 10) -> List[List[int]]:
    """
    ⚙️ В «🚫 Закрытые склады» тоже показываем ТОЛЬКО склады с положительным спросом (ΣD/день > 0).
       Если список положительных пуст (фолбэк) — показываем все склады.
    """
    wm = get_warehouses_map()  # wid -> (wname, cid, cname)
    allowed = set(get_positive_demand_wids() or [])
    all_wids = sorted(wm.keys(), key=lambda w: (wm[w][2], wm[w][0], int(w)))
    if allowed:
        wids = [w for w in all_wids if w in allowed]
    else:
        wids = all_wids
    pages: List[List[int]] = []
    for i in range(0, len(wids), page_size):
        pages.append(wids[i : i + page_size])
    if not pages:
        pages = [[]]
    return pages


def _closed_wh_text() -> str:
    return (
        "<b>🚫 Закрытые склады</b>\n"
        "Выберите склады, куда <u>нельзя</u> отгружать. Отметка — 🚫.\n"
        "После изменений нажмите «Сохранить», чтобы вернуться к отчёту."
    )


def _kb_closed_wh(page: int = 0, page_size: int = 10) -> InlineKeyboardMarkup:
    pages = _closed_wh_pages(page_size=page_size)
    page = max(0, min(page, len(pages) - 1))
    wm = get_warehouses_map()
    closed = set(_load_closed_wids())
    rows: List[List[InlineKeyboardButton]] = []

    for wid in pages[page]:
        wname, cid, cname = wm.get(wid, (f"wh:{wid}", 0, "C?"))
        mark = "🚫" if wid in closed else "✅"
        # 🚫 Требование: убрать ID из названия склада — не показываем (id:<wid>)
        label = f"{mark} {cname} • {wname}"
        rows.append(
            [InlineKeyboardButton(text=label, callback_data=f"need:closed:toggle:{wid}:{page}")]
        )

    nav_row: List[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text="« Назад", callback_data=f"need:closed:page:{
                    page - 1}"))
    nav_row.append(
        InlineKeyboardButton(text=f"Стр. {page + 1}/{len(pages)}", callback_data="need:closed:nop")
    )
    if page < len(pages) - 1:
        nav_row.append(InlineKeyboardButton(text="Вперёд »", callback_data=f"need:closed:page:{
                    page + 1}"))
    if nav_row:
        rows.append(nav_row)

    # Кнопки действий
    rows.append(
        [
            InlineKeyboardButton(text="🧹 Сбросить", callback_data="need:closed:reset"),
            InlineKeyboardButton(text="💾 Сохранить", callback_data="need:closed:save"),
        ]
    )
    rows.append([InlineKeyboardButton(text="↩️ К отчёту", callback_data="need:closed:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("need:closed:page:"))
async def closed_wh_page(cb: CallbackQuery):
    await _safe_answer(cb)
    try:
        page = int(cb.data.split(":")[-1])
    except Exception:
        page = 0
    await _safe_edit(
        cb, _closed_wh_text(), parse_mode="HTML", reply_markup=_kb_closed_wh(page=page)
    )


@router.callback_query(F.data.startswith("need:closed:toggle:"))
async def closed_wh_toggle(cb: CallbackQuery):
    await _safe_answer(cb)
    parts = cb.data.split(":")
    try:
        # need:closed:toggle:<wid>:<page>
        wid = int(parts[3])
        page = int(parts[4])
    except Exception:
        wid, page = None, 0
    if wid is None:
        return
    closed = set(_load_closed_wids())
    if wid in closed:
        closed.remove(wid)
    else:
        closed.add(wid)
    _save_closed_wids(sorted(closed))
    # ререндер текущей страницы (маркер должен обновиться сразу)
    await _safe_edit(
        cb, _closed_wh_text(), parse_mode="HTML", reply_markup=_kb_closed_wh(page=page)
    )


@router.callback_query(F.data == "need:closed:reset")
async def closed_wh_reset(cb: CallbackQuery):
    await _safe_answer(cb, "Список закрытых складов очищен")
    _save_closed_wids([])
    await _safe_edit(cb, _closed_wh_text(), parse_mode="HTML", reply_markup=_kb_closed_wh(page=0))


@router.callback_query(F.data == "need:closed:save")
async def closed_wh_save(cb: CallbackQuery):
    await _safe_answer(cb, "Сохранено")
    await closed_wh_back(cb)


@router.callback_query(F.data == "need:closed:back")
async def closed_wh_back(cb: CallbackQuery):
    await _safe_answer(cb)
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)
    scope = "sku"
    data = compute_need(scope, dispatch_days=S)
    full_text = format_need_text(data)
    pages = _paginate_only_if_needed(full_text)
    await _safe_edit(
        cb, pages[0], parse_mode="HTML", reply_markup=_kb_need_root(scope, page=0, pages=len(pages))
    )


# ─────────────────────────────────────────────────────────────────────────────
# Export (новая форма; уникальное имя файла; централизованный выбор каталога)
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data.startswith("need:export:"))
async def need_export(cb: CallbackQuery):
    await _safe_answer(cb, "Готовлю Excel (новая форма)…")
    disp = _load_dispatch()
    S = int(disp.get("days") or 0)

    # поддержка фильтра: need:export:<scope>[:cluster|warehouse:<id>]
    parts = cb.data.split(":")
    scope = parts[2] if len(parts) >= 3 else "sku"
    filter_kind = parts[3] if len(parts) >= 5 else None
    filter_id = None
    try:
        if filter_kind in {"cluster", "warehouse"}:
            filter_id = int(parts[4])
    except Exception:
        filter_kind = None
        filter_id = None

    # ДАННЫЕ: применяем фильтр ко всем листам (товары/кластеры/склады)
    if filter_kind == "cluster" and filter_id is not None:
        data_sku = compute_need("sku", dispatch_days=S, filter_cluster=filter_id)
        data_cluster = compute_need("cluster", dispatch_days=S, filter_cluster=filter_id)
        data_wh = compute_need("warehouse", dispatch_days=S, filter_cluster=filter_id)
        export_suffix = f"_cluster{filter_id}"
    elif filter_kind == "warehouse" and filter_id is not None:
        data_sku = compute_need("sku", dispatch_days=S, filter_warehouse=filter_id)
        data_cluster = compute_need("cluster", dispatch_days=S, filter_warehouse=filter_id)
        data_wh = compute_need("warehouse", dispatch_days=S, filter_warehouse=filter_id)
        export_suffix = f"_wh{filter_id}"
    else:
        data_sku = compute_need("sku", dispatch_days=S)
        data_cluster = compute_need("cluster", dispatch_days=S)
        data_wh = compute_need("warehouse", dispatch_days=S)
        export_suffix = ""

    reports_dir = resolve_reports_dir()

    # 💡 Уникальное имя файла (исключаем кэш/старые файлы) + пометка фильтра + v3.7
    ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    env_name = os.getenv("SHIPMENTS_REPORT_XLSX")
    if env_name:
        base, ext = os.path.splitext(env_name)
        ext = ext or ".xlsx"
        fname = f"{base}_need_v3.8{export_suffix}_{ts}{ext}"
    else:
        fname = f"shipments_need_v3.8{export_suffix}_{ts}.xlsx"

    path = os.path.join(reports_dir, fname)

    try:
        out_path = export_need_excel(path, data_sku, data_cluster, data_wh)
        await cb.message.answer_document(
            FSInputFile(out_path),
            caption="📥 Отчёт «ОТГРУЗКИ — РЕКОМЕНДАЦИИ» (Excel, новая форма v3.8)",
        )
    except Exception as e:
        await cb.message.answer(f"❗ Не удалось сформировать Excel (новая форма): {e}")
