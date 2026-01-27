# handlers/handlers_shipments_demand.py
from __future__ import annotations

import os
import json
import asyncio
import datetime as dt
from typing import Dict, Tuple, List, DefaultDict, Iterable, Optional
from collections import defaultdict

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# Профиль спроса / таблицы / экспорт
from modules_shipments.shipments_demand import (
    compute_D_average,
    compute_D_dynamics,
    compute_D_plan_distribution,
    compute_D_hybrid,
    aggregate_to_cluster,
    aggregate_to_sku,
    rows_by_warehouse,
    rows_by_cluster,
    rows_by_sku,
    export_excel,
)

# ⚙️ Используем «внутреннюю» утилиту для восстановления читаемых имён складов/кластеров
# (доступна даже если не экспортирована в __all__)
from modules_shipments.shipments_demand import _warehouse_name_maps  # type: ignore

# Автопрогрев (дельта только по новым данным)
try:
    from modules_shipments.shipments_demand_data import (
        clear_demand_cache,
        warm_incremental_recent,
        fetch_sales_view,
        WATCH_SKU as DATA_WATCH_SKU,  # список наблюдаемых SKU, собранный из WATCH_*
    )  # type: ignore
except Exception:

    def clear_demand_cache() -> None:  # type: ignore
        pass

    def warm_incremental_recent() -> Dict[str, str]:  # type: ignore
        return {"enabled": False}

    def fetch_sales_view(*args, **kwargs):  # type: ignore
        return []

    DATA_WATCH_SKU = []

# Для имён кластеров/складов
try:
    from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore
except Exception:

    def fetch_stocks_view(view: str = "warehouse", force: bool = False):  # type: ignore
        return []


# Алиасы
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


router = Router(name="shipments_demand")
TG_MAX = 4096

# ─────────────────────────────────────────────────────────────
# ENV / пути к кэшу
# ─────────────────────────────────────────────────────────────
EPS = float(os.getenv("DEMAND_EPS", "0.01"))
DEMAND_METHODS = ("average", "dynamics", "plandistr", "hybrid")
# Добавлены периоды 30 и 60 — синхронизация с /warehouse
DEMAND_PERIODS = (30, 60, 90, 180, 360)
DEMAND_VIEWS = ("sku", "cluster", "warehouse")

METHOD_TITLES = {
    "average": "Среднесуточный спрос",
    "dynamics": "Динамика заказов",
    "plandistr": "Распределение плана",
    "hybrid": "Адаптивный гибрид",
}
VIEW_TITLES = {"sku": "🔢 По SKU", "cluster": "🏢 По кластерам", "warehouse": "🏭 По складам"}

DEMAND_CACHE_TTL_HOURS = int(os.getenv("DEMAND_CACHE_MAX_AGE_HOURS", "6"))
DEMAND_WARM_ENABLED = os.getenv("DEMAND_WARM_ENABLED", "1").strip() in ("1", "true", "yes", "on")
DEMAND_WARM_INTERVAL_MIN = int(os.getenv("DEMAND_WARM_INTERVAL_MIN", "15"))
DEMAND_WARM_RECENT_DAYS = int(os.getenv("DEMAND_WARM_RECENT_DAYS", "3"))
DEMAND_WARM_MAX_SKU_CYCLE = int(os.getenv("DEMAND_WARM_MAX_SKU_PER_CYCLE", "8"))
DEMAND_LOOKBACK_DAYS = int(os.getenv("DEMAND_LOOKBACK_DAYS", "360"))

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_MODULES_SHIP_DIR = os.path.join(PROJECT_ROOT, "modules_shipments")
_PREFS_PATH = os.path.join(_MODULES_SHIP_DIR, "data", "cache", "common", "warehouse_prefs.json")
_LEGACY_PREFS_PATH = os.path.join(PROJECT_ROOT, "data", "cache", "common", "warehouse_prefs.json")

CACHE_DIR = os.path.join(PROJECT_ROOT, "data", "cache", "shipments")
SKU_CACHE_DIR = os.path.join(CACHE_DIR, "demand_sku")
os.makedirs(SKU_CACHE_DIR, exist_ok=True)

# ───────── helpers: алиасы / порядок из .env ─────────


def _safe_alias(sku: int) -> str:
    try:
        a = get_alias_for_sku(int(sku)) or ""
        a = a.strip()
        return a if a else str(int(sku))
    except Exception:
        return str(int(sku))


def _watch_sku_order() -> List[int]:
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",")
    out: List[int] = []
    seen = set()
    for t in raw.split(","):
        t = t.strip()
        if not t:
            continue
        try:
            val = t.split(":", 1)[-1]
            v = int(val)
            if v not in seen:
                out.append(v)
                seen.add(v)
        except Exception:
            continue
    # если WATCH_SKU пуст — попробуем список из data-модуля (он уже разобрал WATCH_*)
    if not out:
        try:
            for s in DATA_WATCH_SKU or []:
                if str(s).isdigit():
                    v = int(s)
                    if v not in seen:
                        out.append(v)
                        seen.add(v)
        except Exception:
            pass
    return out


WATCH_ORDER = _watch_sku_order()
WATCH_SET = set(WATCH_ORDER)
WATCH_RANK = {sku: i for i, sku in enumerate(WATCH_ORDER)}


def _order_by_watch_sku(items: List[Tuple[int, float, str]]) -> List[Tuple[int, float, str]]:
    """
    items: [(sku, value, alias)]
    Сортировка: по позиции в WATCH_SKU; если WATCH пуст — по alias (алфавит).
    """
    if WATCH_ORDER:
        return sorted(items, key=lambda t: (WATCH_RANK.get(int(t[0]), 10_000)))
    # fallback — алфавитно по alias
    return sorted(items, key=lambda t: t[2].lower())


# ───────── настройки чтения/записи ─────────
os.makedirs(os.path.dirname(_PREFS_PATH), exist_ok=True)
CLEAR_ON_PERIOD_CHANGE = os.getenv("DEMAND_CLEAR_ON_PERIOD_CHANGE", "0").strip() in (
    "1",
    "true",
    "yes",
)


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


def _load_global() -> dict:
    d = _read_json(_PREFS_PATH) or _read_json(_LEGACY_PREFS_PATH)
    method = (d.get("method") or "average").strip().lower()
    period = int(d.get("period") or 90)
    if method not in {"average", "dynamics", "hybrid", "plan_distribution"}:
        method = "average"
    if period not in DEMAND_PERIODS:
        period = 90
    if method == "plan_distribution":
        method = "average"
    return {"method": method, "period": period}


def _save_global(method: str, period: int) -> dict:
    if method not in {"average", "dynamics", "hybrid"}:
        method = "average"
    if period not in DEMAND_PERIODS:
        period = 90
    current = _read_json(_PREFS_PATH) or _read_json(_LEGACY_PREFS_PATH) or {}
    old_period = int(current.get("period") or 90)
    payload = {"method": method, "period": int(period)}
    _write_json(_PREFS_PATH, payload)
    _write_json(_LEGACY_PREFS_PATH, payload)
    # Очистку кэша по смене периода по умолчанию не применяем (единый per-SKU кэш)
    if CLEAR_ON_PERIOD_CHANGE and int(period) != old_period:
        try:
            clear_demand_cache()
        except Exception:
            pass
    return payload


# ───────── UI helpers ─────────


async def _ack(cb: CallbackQuery):
    try:
        await cb.answer()
    except Exception:
        pass


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs):
    try:
        await cb.message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise


def _fmt_title(method: str, period: int, view: str) -> str:
    """Шапка отчёта: единый стиль и время формирования."""
    return (
        "📄 Потребность — ΣD/день\n"
        f"⏱ Обновлено: {dt.datetime.now().strftime('%d.%m.%Y %H:%M')}\n\n"
        f"⚙️ Метод: {METHOD_TITLES.get(method, method)} • Период: {period} дн.\n"
        f"{VIEW_TITLES.get(view, view)}:\n\n"
    )


def _root_kb(view: str) -> InlineKeyboardMarkup:
    """Главное меню раздела «Потребность»."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔢 По SKU", callback_data="demand:view:sku")],
            [InlineKeyboardButton(text="🏢 По кластерам", callback_data="demand:view:cluster")],
            [InlineKeyboardButton(text="🏭 По складам", callback_data="demand:view:warehouse")],
            [InlineKeyboardButton(text="📥 Экспорт в Excel", callback_data="demand:export:excel")],
            [InlineKeyboardButton(text="⚙️ Настройки", callback_data="demand:settings")],
            [InlineKeyboardButton(text="📦 Информация по складам", callback_data="demand:info")],
            [InlineKeyboardButton(text="🔙 К отгрузкам", callback_data="shipments")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )


def _settings_kb(method: str, period: int) -> InlineKeyboardMarkup:
    """Клавиатура настроек метода и периода (с 30/60)."""
    m1 = [
        InlineKeyboardButton(
            text=("✓ " if method == "average" else "") + "Среднесуточный спрос",
            callback_data="demand:method:average",
        ),
        InlineKeyboardButton(
            text=("✓ " if method == "dynamics" else "") + "Динамика заказов",
            callback_data="demand:method:dynamics",
        ),
    ]
    m2 = [
        InlineKeyboardButton(
            text=("✓ " if method == "hybrid" else "") + "Адаптивный гибрид",
            callback_data="demand:method:hybrid",
        ),
    ]
    p1 = [
        InlineKeyboardButton(
            text=("✓ " if period == 30 else "") + "30 дн.", callback_data="demand:period:30"
        ),
        InlineKeyboardButton(
            text=("✓ " if period == 60 else "") + "60 дн.", callback_data="demand:period:60"
        ),
        InlineKeyboardButton(
            text=("✓ " if period == 90 else "") + "90 дн.", callback_data="demand:period:90"
        ),
    ]
    p2 = [
        InlineKeyboardButton(
            text=("✓ " if period == 180 else "") + "180 дн.", callback_data="demand:period:180"
        ),
        InlineKeyboardButton(
            text=("✓ " if period == 360 else "") + "360 дн.", callback_data="demand:period:360"
        ),
    ]
    return InlineKeyboardMarkup(
        inline_keyboard=[
            m1,
            m2,
            p1,
            p2,
            [InlineKeyboardButton(text="◀️ Вернуться", callback_data="demand:back")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )


# ───────── вычисления ─────────


def _compute_ws_sync(method: str, period: int) -> Dict[Tuple[int, int], float]:
    """
    Возвращает карту D[(warehouse_id, sku)] -> D/день для выбранного метода/периода.
    Важно: вызывать расчёты с view='warehouse' и извлекать 'D_by_w_sku'.
    Это устраняет прежние ошибки типов и ключей.
    """
    view = "warehouse"
    if method == "average":
        payload = compute_D_average(view=view, period=period)
    elif method == "dynamics":
        payload = compute_D_dynamics(view=view, period=period)
    elif method == "plandistr":
        payload = compute_D_plan_distribution(view=view, period=period)
    elif method == "hybrid":
        payload = compute_D_hybrid(view=view, period=period)
    else:
        payload = compute_D_average(view=view, period=period)
    d_map = payload.get("D_by_w_sku") or {}
    # гарантируем обычный dict с числовыми ключами
    out: Dict[Tuple[int, int], float] = {}
    for k, v in d_map.items():
        try:
            wid, sku = k  # ожидаем кортеж
            out[(int(wid), int(sku))] = float(v or 0.0)
        except Exception:
            # на всякий случай поддержим "строковый" ключ вида "wid,sku"
            try:
                if isinstance(k, str) and "," in k:
                    wid_s, sku_s = k.split(",", 1)
                    out[(int(wid_s), int(sku_s))] = float(v or 0.0)
            except Exception:
                continue
    return out


async def _compute_ws(method: str, period: int) -> Dict[Tuple[int, int], float]:
    return await asyncio.to_thread(_compute_ws_sync, method, period)


# ───────── утилиты — сумма и порядок ─────────


def _sum(values: List[float]) -> float:
    return float(sum(values)) if values else 0.0


def _name_maps() -> Tuple[Dict[int, str], Dict[int, Tuple[int, str]]]:
    """
    Получить:
      • name_by_wid: {wid -> warehouse_name}
      • cluster_by_wid: {wid -> (cid, cluster_name)}
    с попытками восстановления человеко‑читаемых имён.
    """
    try:
        name_by_w, cluster_by_w = _warehouse_name_maps()
        # типобезопасность
        n = {int(w): str(nm) for w, nm in (name_by_w or {}).items()}
        c = {
            int(w): (int(t[0]), str(t[1]))
            for w, t in (cluster_by_w or {}).items()
            if isinstance(t, (tuple, list)) and len(t) >= 2
        }
        return n, c
    except Exception:
        # запасной путь напрямую из stocks
        rows = fetch_stocks_view(view="warehouse") or []
        name_by_w: Dict[int, str] = {}
        cluster_by_w: Dict[int, Tuple[int, str]] = {}
        # карта id->имя кластера
        cid2name: Dict[int, str] = {}
        for r in fetch_stocks_view(view="cluster") or []:
            try:
                cid = int(r.get("cluster_id") or r.get("id") or r.get("clusterId") or 0)
                cname = str(r.get("cluster_name") or r.get("name") or r.get("title") or "").strip()
                if cid and cname:
                    cid2name[cid] = cname
            except Exception:
                continue
        for r in rows:
            try:
                wid = int(r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id") or 0)
                wname = str(
                    r.get("warehouse_name")
                    or r.get("warehouse")
                    or r.get("name")
                    or r.get("title")
                    or f"wh:{wid}"
                )
                cid = int(r.get("cluster_id") or 0)
                cname = str(r.get("cluster_name") or cid2name.get(cid) or f"кластер {cid}")
                if wid > 0:
                    name_by_w[wid] = wname
                    if cid > 0:
                        cluster_by_w[wid] = (cid, cname)
            except Exception:
                continue
        return name_by_w, cluster_by_w


# ───────── РЕНДЕР — SKU (Σ + Σ по сети) ─────────


async def _render_sku_async(cb: CallbackQuery, method: str, period: int):
    bot = cb.message.bot
    chat_id = cb.message.chat.id
    msg_id = cb.message.message_id
    try:
        D_ws = await _compute_ws(method, period)

        # агрегируем до SKU
        D_s_full: DefaultDict[int, float] = defaultdict(float)  # sku -> ΣD по сети
        for (wid, sku), d in D_ws.items():
            D_s_full[int(sku)] += float(d or 0.0)

        # фильтрация и порядок по WATCH_SKU
        items_all = [(int(s), float(d or 0.0), _safe_alias(int(s))) for s, d in D_s_full.items()]
        if WATCH_SET:
            items_all = [t for t in items_all if int(t[0]) in WATCH_SET]
        items = _order_by_watch_sku(items_all)

        lines = [f"🔹 {alias}: {d:.2f} /дн" for _, d, alias in items]
        total_network = _sum([d for _, d, _ in items])
        text = (
            _fmt_title(method, period, "sku")
            + ("\n".join(lines) if lines else "—")
            + f"\n\n📊 Σ ПО СЕТИ — {total_network:.2f} /дн"
        )
        await bot.edit_message_text(text, chat_id, msg_id, reply_markup=_root_kb("sku"))
    except Exception as e:
        err = _fmt_title(method, period, "sku") + f"⚠️ Ошибка: {e}"
        try:
            await bot.edit_message_text(err, chat_id, msg_id, reply_markup=_root_kb("sku"))
        except Exception:
            pass


# ───────── клавиатуры-меню (пагинация) ─────────


def _menu_kb(
    items: List[Tuple[str, float]], cb_prefix: str, page: int, per_page: int = 10  # (name, value)
) -> InlineKeyboardMarkup:
    total = len(items)
    start = page * per_page
    chunk = items[start : start + per_page]
    rows: List[List[InlineKeyboardButton]] = []
    for idx, (name, val) in enumerate(chunk, start=start):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{name} — {val:.2f}", callback_data=f"{cb_prefix}:pick_idx:{idx}:{page}"
                )
            ]
        )
    total_pages = max(1, (total + per_page - 1) // per_page)
    nav: List[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{cb_prefix}:menu:page:{
                        page - 1}"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{cb_prefix}:menu:page:{
                        page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="demand:start")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ───────── РЕНДЕР — КЛАСТЕРЫ (Σ + Σ по сети) ─────────


async def _render_cluster_async(cb: CallbackQuery, method: str, period: int, page: int = 0):
    bot = cb.message.bot
    chat_id = cb.message.chat.id
    msg_id = cb.message.message_id
    try:
        D_ws = await _compute_ws(method, period)
        name_by_w, cluster_by_w = _name_maps()

        # фильтрация по WATCH_SKU и агрегация по имени кластера
        sum_by_cluster: DefaultDict[str, float] = defaultdict(float)
        for (wid, sku), d in D_ws.items():
            if WATCH_SET and int(sku) not in WATCH_SET:
                continue
            cid_cname = cluster_by_w.get(int(wid))
            if not cid_cname:
                continue
            cname = str(cid_cname[1])
            sum_by_cluster[cname] += float(d or 0.0)

        names = sorted(sum_by_cluster.keys(), key=lambda x: x.lower())
        lines = [f"🔹 {name}: {sum_by_cluster[name]:.2f} /дн" for name in names]

        # Σ по сети = сумма по выбранным (после фильтра)
        total_network = _sum(list(sum_by_cluster.values()))
        text = (
            _fmt_title(method, period, "cluster")
            + ("\n".join(lines) if lines else "—")
            + f"\n\n📊 Σ ПО СЕТИ — {total_network:.2f} /дн"
        )
        items = [(name, sum_by_cluster[name]) for name in names]
        kb = _menu_kb(items, cb_prefix="demand:cluster", page=page, per_page=10)
        await bot.edit_message_text(text, chat_id, msg_id, reply_markup=kb)
    except Exception as e:
        err = _fmt_title(method, period, "cluster") + f"⚠️ Ошибка: {e}"
        try:
            await bot.edit_message_text(err, chat_id, msg_id, reply_markup=_root_kb("cluster"))
        except Exception:
            pass


@router.callback_query(F.data.startswith("demand:cluster:menu:page:"))
async def cluster_menu_page(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        page = int(cb.data.rsplit(":", 1)[-1])
    except Exception:
        page = 0
    gl = _load_global()
    await _render_cluster_async(cb, gl["method"], gl["period"], page=page)


# ───────── ДЕТАЛИ КЛАСТЕРА — header (Σ) + деталь (Σ) ─────────


def _cluster_header_kb(
    names: List[str], sum_by_cluster: Dict[str, float], page: int, idx_cur: int
) -> InlineKeyboardMarkup:
    per_page = 10
    start = page * per_page
    chunk = names[start : start + per_page]
    rows: List[List[InlineKeyboardButton]] = []
    for i, name in enumerate(chunk, start=start):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{name} • {sum_by_cluster[name]:.2f}",
                    callback_data=f"demand:cluster:pick_idx:{i}:{page}",
                )
            ]
        )
    total_pages = max(1, (len(names) + per_page - 1) // per_page)
    nav: List[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav.append(
                InlineKeyboardButton(text="⬅️", callback_data=f"demand:cluster:hdrpage:{idx_cur}:{
                        page - 1}")
            )
        if page + 1 < total_pages:
            nav.append(
                InlineKeyboardButton(text="➡️", callback_data=f"demand:cluster:hdrpage:{idx_cur}:{
                        page + 1}")
            )
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="demand:start")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("demand:cluster:hdrpage:"))
async def cluster_hdr_page(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        _, _, _, idx, page = cb.data.split(":")
        idx = int(idx)
        page = int(page)
    except Exception:
        return
    gl = _load_global()
    await _render_cluster_detail_async(cb, gl["method"], gl["period"], idx=idx, page=page)


async def _render_cluster_detail_async(
    cb: CallbackQuery, method: str, period: int, idx: int, page: int = 0
):
    bot = cb.message.bot
    chat_id = cb.message.chat.id
    msg_id = cb.message.message_id

    D_ws = await _compute_ws(method, period)
    name_by_w, cluster_by_w = _name_maps()

    # список имён кластеров в стабильном порядке
    sum_by_cluster: DefaultDict[str, float] = defaultdict(float)
    for (wid, sku), d in D_ws.items():
        if WATCH_SET and int(sku) not in WATCH_SET:
            continue
        t = cluster_by_w.get(int(wid))
        if not t:
            continue
        sum_by_cluster[str(t[1])] += float(d or 0.0)
    names = sorted(sum_by_cluster.keys(), key=lambda x: x.lower())
    if not names or idx < 0 or idx >= len(names):
        await _render_cluster_async(cb, method, period)
        return

    cname = names[idx]

    # детализация по SKU в выбранном кластере
    per_cluster_sku_vals: DefaultDict[int, float] = defaultdict(float)
    for (wid, sku), d in D_ws.items():
        t = cluster_by_w.get(int(wid))
        if not t:
            continue
        if str(t[1]) != cname:
            continue
        if WATCH_SET and int(sku) not in WATCH_SET:
            continue
        per_cluster_sku_vals[int(sku)] += float(d or 0.0)

    items_all = [(int(s), float(d), _safe_alias(int(s))) for s, d in per_cluster_sku_vals.items()]
    items = _order_by_watch_sku(items_all) if items_all else []
    cluster_total = _sum([d for _, d, _ in items])

    lines = [f"🏢 <b>{cname}</b> — детализация по SKU (Σ по кластеру: {cluster_total:.2f}/дн)", ""]
    lines += [f"  📦 {alias} — {d:.2f} /дн" for _, d, alias in items]
    lines.append(f"\n📊 Σ ПО КЛАСТЕРУ — {cluster_total:.2f} /дн")

    header_kb = _cluster_header_kb(names, sum_by_cluster, page=page, idx_cur=idx)
    text = _fmt_title(method, period, "cluster") + "\n".join(lines)
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=header_kb)


# ───────── РЕНДЕР — СКЛАДЫ (Σ + Σ по сети) ─────────


async def _render_warehouse_async(cb: CallbackQuery, method: str, period: int, page: int = 0):
    bot = cb.message.bot
    chat_id = cb.message.chat.id
    msg_id = cb.message.message_id
    try:
        D_ws = await _compute_ws(method, period)
        name_by_w, cluster_by_w = _name_maps()

        sum_by_w: DefaultDict[str, float] = defaultdict(float)
        for (wid, sku), d in D_ws.items():
            if WATCH_SET and int(sku) not in WATCH_SET:
                continue
            wname = name_by_w.get(int(wid)) or f"wh:{wid}"
            sum_by_w[wname] += float(d or 0.0)

        names = sorted(sum_by_w.keys(), key=lambda x: x.lower())
        lines = [f"🔹 {name}: {sum_by_w[name]:.2f} /дн" for name in names]
        total_network = _sum(list(sum_by_w.values()))
        text = (
            _fmt_title(method, period, "warehouse")
            + ("\n".join(lines) if lines else "—")
            + f"\n\n📊 Σ ПО СЕТИ — {total_network:.2f} /дн"
        )
        items = [(name, sum_by_w[name]) for name in names]
        kb = _menu_kb(items, cb_prefix="demand:wh", page=page, per_page=10)
        await bot.edit_message_text(text, chat_id, msg_id, reply_markup=kb)
    except Exception as e:
        err = _fmt_title(method, period, "warehouse") + f"⚠️ Ошибка: {e}"
        try:
            await bot.edit_message_text(err, chat_id, msg_id, reply_markup=_root_kb("warehouse"))
        except Exception:
            pass


@router.callback_query(F.data.startswith("demand:wh:menu:page:"))
async def wh_menu_page(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        page = int(cb.data.rsplit(":", 1)[-1])
    except Exception:
        page = 0
    gl = _load_global()
    await _render_warehouse_async(cb, gl["method"], gl["period"], page=page)


# ───────── ДЕТАЛИ СКЛАДА — header (Σ) + деталь (Σ) ─────────


def _wh_header_kb(
    names: List[str], sum_by_w: Dict[str, float], page: int, idx_cur: int
) -> InlineKeyboardMarkup:
    per_page = 10
    start = page * per_page
    chunk = names[start : start + per_page]
    rows: List[List[InlineKeyboardButton]] = []
    for i, name in enumerate(chunk, start=start):
        rows.append(
            [
                InlineKeyboardButton(
                    text=f"{name} • {sum_by_w[name]:.2f}",
                    callback_data=f"demand:wh:pick_idx:{i}:{page}",
                )
            ]
        )
    total_pages = max(1, (len(names) + per_page - 1) // per_page)
    nav: List[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"demand:wh:hdrpage:{idx_cur}:{
                        page - 1}"))
        if page + 1 < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"demand:wh:hdrpage:{idx_cur}:{
                        page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="◀️ К разделу", callback_data="demand:start")])
    rows.append([InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data.startswith("demand:wh:hdrpage:"))
async def wh_hdr_page(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        _, _, _, idx, page = cb.data.split(":")
        idx = int(idx)
        page = int(page)
    except Exception:
        return
    gl = _load_global()
    await _render_warehouse_detail_async(cb, gl["method"], gl["period"], idx=idx, page=page)


async def _render_warehouse_detail_async(
    cb: CallbackQuery, method: str, period: int, idx: int, page: int = 0
):
    bot = cb.message.bot
    chat_id = cb.message.chat.id
    msg_id = cb.message.message_id

    D_ws = await _compute_ws(method, period)
    name_by_w, _cluster_by_w = _name_maps()

    # имена складов в стабильном порядке
    wnames_set = {name_by_w.get(int(wid)) or f"wh:{wid}" for (wid, _sku) in D_ws.keys()}
    names = sorted(wnames_set, key=lambda x: x.lower())
    if not names or idx < 0 or idx >= len(names):
        await _render_warehouse_async(cb, method, period)
        return
    wname = names[idx]

    # детализация по SKU для выбранного склада
    per_wh_sku_vals: DefaultDict[int, float] = defaultdict(float)
    for (wid, sku), d in D_ws.items():
        if (name_by_w.get(int(wid)) or f"wh:{wid}") != wname:
            continue
        if WATCH_SET and int(sku) not in WATCH_SET:
            continue
        per_wh_sku_vals[int(sku)] += float(d or 0.0)

    items_all = [(int(s), float(d), _safe_alias(int(s))) for s, d in per_wh_sku_vals.items()]
    items = _order_by_watch_sku(items_all)
    wh_total = _sum([d for _, d, _ in items])

    lines = [f"🏭 <b>{wname}</b> — детализация по SKU (Σ по складу: {wh_total:.2f}/дн)", ""]
    lines += [f"  📦 {alias} — {d:.2f} /дн" for _, d, alias in items]
    lines.append(f"\n📊 Σ ПО СКЛАДУ — {wh_total:.2f} /дн")
    header_kb = _wh_header_kb(
        names,
        {
            nm: sum(
                float(v or 0.0)
                for (wid, sku), v in D_ws.items()
                if (name_by_w.get(int(wid)) or f"wh:{wid}") == nm
                and (not WATCH_SET or int(sku) in WATCH_SET)
            )
            for nm in names
        },
        page=page,
        idx_cur=idx,
    )
    text = _fmt_title(method, period, "warehouse") + "\n".join(lines)
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=header_kb)


# ───────── АКТИВНЫЕ SKU (для инфоэкрана) ─────────


def _active_sku_set() -> set[str]:
    """
    Что считаем «задействованными» SKU:
      • Если настроены WATCH_* (в т.ч. WATCH_SKU / WATCH_OFFERS / WATCH_OFFERS_DICT) — берём их.
      • Иначе пробуем прочитать справочник stocks (view="sku").
      • Иначе берём WATCH_SKU из .env (числа).
    """
    # 1) WATCH_* уже разобраны в DATA_WATCH_SKU (список строк)
    watch = [s for s in (DATA_WATCH_SKU or []) if str(s).strip().isdigit()]
    if watch:
        return set(watch)

    # 2) Попытка из справочника
    act: List[str] = []
    try:
        for r in fetch_stocks_view(view="sku") or []:
            sku = (
                r.get("sku")
                or (r.get("dimension") or {}).get("sku")
                or (r.get("dimensions") or [{}])[0].get("sku")
            )
            s = str(sku or "").strip()
            if s.isdigit():
                act.append(s)
    except Exception:
        pass
    if act:
        return set(act)

    # 3) Фаллбэк на простую переменную WATCH_SKU
    if WATCH_SET:
        return {str(x) for x in WATCH_SET}
    return set()


# ───────── ИНФО ─────────


def _scan_sku_cache_status() -> Dict[str, any]:
    try:
        files = [p for p in os.listdir(SKU_CACHE_DIR) if p.endswith(".json")]
    except Exception:
        files = []

    active = _active_sku_set()
    active_present: set[str] = set()

    total_files = len(files)
    fresh = stale = 0
    fresh_active = stale_active = 0
    last_saved: Optional[dt.datetime] = None

    for fname in files:
        path = os.path.join(SKU_CACHE_DIR, fname)
        sku_str = os.path.splitext(fname)[0]
        try:
            with open(path, "r", encoding="utf-8") as f:
                d = json.load(f) or {}
        except Exception:
            continue
        saved_at = d.get("saved_at", "")
        try:
            saved_dt = dt.datetime.fromisoformat(saved_at) if saved_at else None
        except Exception:
            saved_dt = None

        is_fresh = False
        if saved_dt:
            is_fresh = (
                (dt.datetime.now() - saved_dt).total_seconds() / 3600.0
            ) <= DEMAND_CACHE_TTL_HOURS
            if is_fresh:
                fresh += 1
            else:
                stale += 1
            if last_saved is None or saved_dt > last_saved:
                last_saved = saved_dt

        if sku_str in active:
            active_present.add(sku_str)
            if is_fresh:
                fresh_active += 1
            else:
                stale_active += 1

    last_iso = "—"
    if last_saved:
        last_iso = last_saved.replace(microsecond=0).isoformat()

    active_total = len(active)
    active_in_cache = len(active_present)
    active_missing = max(0, active_total - active_in_cache)

    return {
        "total_files": total_files,
        "fresh": fresh,
        "stale": stale,
        "last_saved_iso": last_iso,
        # активные по текущей конфигурации
        "active_total": active_total,
        "active_in_cache": active_in_cache,
        "active_fresh": fresh_active,
        "active_stale": stale_active,
        "active_missing": active_missing,
    }


def _format_info_text() -> str:
    st = _scan_sku_cache_status()
    now_iso = dt.datetime.now().isoformat(timespec="seconds")
    lines = [
        "📦 <b>Информация по складам</b>",
        "Фаза: автопрогрев (дельта новых данных, ротация SKU)",
        "",
        f"🗃 Всего файлов в per‑SKU кэше: {st['total_files']}",
        f"   • Свежих: {st['fresh']}  • Устаревших: {st['stale']}",
        "",
        f"🎯 Активных SKU (используются в расчётах сейчас): {st['active_total']}",
        f"   • Есть в кэше: {
            st['active_in_cache']}  — из них свежих: {
            st['active_fresh']}, устаревших: {
            st['active_stale']}",
        f"   • Отсутствуют в кэше: {st['active_missing']}",
        "",
        "ℹ️ Активные SKU берутся из настроек WATCH_* (если заданы),",
        "   иначе — из справочника «SKU» в stocks. Чтобы ограничить список, заполните WATCH_*.",
        "",
        f"📅 Последнее обновление кэша: {st['last_saved_iso']}",
        f"📡 Последний опрос (экрана): {now_iso}",
        f"🕓 Интервал автопрогрева (env): {DEMAND_WARM_INTERVAL_MIN} мин",
        f"🧊 TTL кэша (env): {DEMAND_CACHE_TTL_HOURS} ч",
        f"🔄 Дельта-догрузка (env): {DEMAND_WARM_RECENT_DAYS} дн",
        f"📦 Лимит SKU за тик (env): {DEMAND_WARM_MAX_SKU_CYCLE}",
        f"🔘 Автосбор: {'включён' if DEMAND_WARM_ENABLED else 'выключен'}",
    ]
    return "\n".join(lines)


@router.callback_query(F.data == "demand:info")
async def demand_info(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    text = _format_info_text()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧹 Очистить кэш", callback_data="demand:cache:clear")],
            [InlineKeyboardButton(text="◀️ К разделу", callback_data="demand:start")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)


@router.callback_query(F.data == "demand:cache:clear")
async def demand_clear_cache(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        clear_demand_cache()
        note = "🧹 Кэш очищен."
    except Exception as e:
        note = f"⚠️ Не удалось очистить кэш: {e}"
    text = note + "\n\n" + _format_info_text()
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="◀️ К разделу", callback_data="demand:start")],
            [InlineKeyboardButton(text="🏠 Домой", callback_data="nav:home")],
        ]
    )
    await _safe_edit(cb, text, parse_mode="HTML", reply_markup=kb)


# ───────── старт / настройки ─────────


@router.callback_query(F.data == "demand:start")
async def demand_start(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    await state.update_data(
        demand_method=gl["method"], demand_period=gl["period"], demand_view="sku"
    )
    await _safe_edit(
        cb, _fmt_title(gl["method"], gl["period"], "sku"), reply_markup=_root_kb("sku")
    )


@router.callback_query(F.data == "demand:settings")
async def demand_settings(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    await state.update_data(demand_method=gl["method"], demand_period=gl["period"])
    await _safe_edit(
        cb,
        "⚙️ <b>Настройки потребности</b>\nВыберите метод и период.",
        parse_mode="HTML",
        reply_markup=_settings_kb(gl["method"], gl["period"]),
    )


@router.callback_query(F.data == "demand:back")
async def demand_back(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    data = await state.get_data()
    await _safe_edit(
        cb,
        _fmt_title(
            data.get("demand_method", "average"),
            int(data.get("demand_period", 90)),
            data.get("demand_view", "sku"),
        ),
        reply_markup=_root_kb(data.get("demand_view", "sku")),
    )


@router.callback_query(F.data.startswith("demand:method:"))
async def demand_method(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    method = cb.data.split(":")[-1]
    if method not in DEMAND_METHODS and method != "plan":
        method = "average"
    method = "plandistr" if method == "plan" else method
    gl = _load_global()
    saved = _save_global(method if method != "plandistr" else "average", gl["period"])
    await state.update_data(demand_method=saved["method"], demand_period=saved["period"])
    await demand_settings(cb, state)


@router.callback_query(F.data.startswith("demand:period:"))
async def demand_period(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        period = int(cb.data.split(":")[-1])
    except Exception:
        period = 90
    if period not in DEMAND_PERIODS:
        period = 90
    gl = _load_global()
    saved = _save_global(gl["method"], period)
    await state.update_data(demand_method=saved["method"], demand_period=saved["period"])
    await demand_settings(cb, state)


# ───────── переключатели экранов ─────────


@router.callback_query(F.data.startswith("demand:view:sku"))
async def view_sku(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    await state.update_data(
        demand_view="sku", demand_method=gl["method"], demand_period=gl["period"]
    )
    placeholder = _fmt_title(gl["method"], gl["period"], "sku") + "⌛ Формирую отчёт…"
    await _safe_edit(cb, placeholder, reply_markup=_root_kb("sku"))
    asyncio.create_task(_render_sku_async(cb, gl["method"], gl["period"]))


@router.callback_query(F.data.startswith("demand:view:cluster"))
async def view_cluster(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    await state.update_data(
        demand_view="cluster", demand_method=gl["method"], demand_period=gl["period"]
    )
    placeholder = _fmt_title(gl["method"], gl["period"], "cluster") + "⌛ Формирую отчёт…"
    await _safe_edit(cb, placeholder, reply_markup=_root_kb("cluster"))
    asyncio.create_task(_render_cluster_async(cb, gl["method"], gl["period"], page=0))


@router.callback_query(F.data.startswith("demand:view:warehouse"))
async def view_warehouse(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    await state.update_data(
        demand_view="warehouse", demand_method=gl["method"], demand_period=gl["period"]
    )
    placeholder = _fmt_title(gl["method"], gl["period"], "warehouse") + "⌛ Формирую отчёт…"
    await _safe_edit(cb, placeholder, reply_markup=_root_kb("warehouse"))
    asyncio.create_task(_render_warehouse_async(cb, gl["method"], gl["period"], page=0))


# клики из подменю (страницы)


@router.callback_query(F.data.startswith("demand:cluster:menu:page:"))
async def view_cluster_menu_page(cb: CallbackQuery, state: FSMContext):
    await cluster_menu_page(cb, state)


@router.callback_query(F.data.startswith("demand:wh:menu:page:"))
async def view_wh_menu_page(cb: CallbackQuery, state: FSMContext):
    await wh_menu_page(cb, state)


# клики на элементы (детализация)


@router.callback_query(F.data.startswith("demand:cluster:pick_idx:"))
async def view_cluster_pick(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        _, _, _, idx, page = cb.data.split(":")
        idx = int(idx)
        page = int(page)
    except Exception:
        idx, page = 0, 0
    gl = _load_global()
    await state.update_data(demand_view="cluster")
    await _render_cluster_detail_async(cb, gl["method"], gl["period"], idx=idx, page=page)


@router.callback_query(F.data.startswith("demand:cluster:hdrpage:"))
async def view_cluster_hdrpage(cb: CallbackQuery, state: FSMContext):
    await cluster_hdr_page(cb, state)


@router.callback_query(F.data.startswith("demand:wh:pick_idx:"))
async def view_warehouse_pick(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    try:
        _, _, _, idx, page = cb.data.split(":")
        idx = int(idx)
        page = int(page)
    except Exception:
        idx, page = 0, 0
    gl = _load_global()
    await state.update_data(demand_view="warehouse")
    await _render_warehouse_detail_async(cb, gl["method"], gl["period"], idx=idx, page=page)


@router.callback_query(F.data.startswith("demand:wh:hdrpage:"))
async def view_wh_hdrpage(cb: CallbackQuery, state: FSMContext):
    await wh_hdr_page(cb, state)


# ───────── экспорт ─────────


@router.callback_query(F.data == "demand:export:excel")
async def demand_export_excel(cb: CallbackQuery, state: FSMContext):
    await _ack(cb)
    gl = _load_global()
    placeholder = _fmt_title(gl["method"], gl["period"], "sku") + "⌛ Готовлю Excel…"
    await _safe_edit(cb, placeholder, reply_markup=_root_kb("sku"))

    async def _do_export():
        try:
            D_ws = await _compute_ws(gl["method"], gl["period"])
            # фильтрация по WATCH_SKU
            if WATCH_SET:
                D_ws = {k: v for k, v in D_ws.items() if int(k[1]) in WATCH_SET}

            # карты имён
            name_by_w, cluster_by_w = _name_maps()

            # агрегаты для листов
            # 1) SKU
            D_s: DefaultDict[int, float] = defaultdict(float)
            for (wid, sku), d in D_ws.items():
                D_s[int(sku)] += float(d or 0.0)
            sku_rows: List[List[object]] = []
            for sku, d in D_s.items():
                sku_rows.append([int(sku), _safe_alias(int(sku)), float(d), int(gl["period"])])
            # переупорядочим лист SKU по WATCH_SKU
            if WATCH_ORDER:
                rank = {int(s): i for i, s in enumerate(WATCH_ORDER)}
                sku_rows = sorted(sku_rows, key=lambda r: rank.get(int(r[0]), 10_000))

            # 2) Склады
            rows_w: List[List[object]] = []
            for (wid, sku), d in D_ws.items():
                cid_cname = cluster_by_w.get(int(wid))
                cname = cid_cname[1] if cid_cname else ""
                wname = name_by_w.get(int(wid)) or f"wh:{wid}"
                rows_w.append(
                    [
                        int(sku),
                        _safe_alias(int(sku)),
                        str(cname),
                        str(wname),
                        float(d),
                        int(gl["period"]),
                    ]
                )

            # 3) Кластеры
            D_cs: DefaultDict[Tuple[str, int], float] = defaultdict(
                float
            )  # (cluster_name, sku) -> D
            sum_w_by_cluster: DefaultDict[str, float] = defaultdict(float)
            for (wid, sku), d in D_ws.items():
                cid_cname = cluster_by_w.get(int(wid))
                if not cid_cname:
                    continue
                cname = str(cid_cname[1])
                D_cs[(cname, int(sku))] += float(d or 0.0)
                sum_w_by_cluster[cname] += float(d or 0.0)
            rows_c: List[List[object]] = []
            for (cname, sku), d in D_cs.items():
                rows_c.append(
                    [
                        int(sku),
                        _safe_alias(int(sku)),
                        str(cname),
                        float(d),
                        float(sum_w_by_cluster[cname]),
                        int(gl["period"]),
                    ]
                )

            sheets = {"Склады": rows_w, "Кластеры": rows_c, "SKU": sku_rows}
            headers = {
                "Склады": ["SKU", "Alias", "Кластер", "Склад", "D[W,S]", "Период"],
                "Кластеры": ["SKU", "Alias", "Кластер", "D[C,S]", "ΣW", "Период"],
                "SKU": ["SKU", "Alias", "ΣD", "Период"],
            }
            outdir = os.path.abspath(os.path.join("data", "exports", "shipments"))
            os.makedirs(outdir, exist_ok=True)
            path = os.path.join(outdir, f"demand_{gl['method']}_{gl['period']}d.xlsx")
            export_excel(path, sheets, headers)
            try:
                await cb.message.answer_document(
                    FSInputFile(path), caption="📥 Потребность по складам (Excel)"
                )
            except Exception:
                pass
            try:
                await _safe_edit(
                    cb, _fmt_title(gl["method"], gl["period"], "sku"), reply_markup=_root_kb("sku")
                )
            except Exception:
                pass
        except Exception as e:
            try:
                await cb.message.answer(f"❗ Не удалось подготовить Excel: {e}")
            except Exception:
                pass

    asyncio.create_task(_do_export())


# ───────── регистрация фонового джоба ─────────


def register_demand_warmup_job(scheduler) -> None:
    try:
        if not DEMAND_WARM_ENABLED:
            print("[demand:warmup] disabled by env")
            return
        from apscheduler.triggers.interval import IntervalTrigger

        trigger = IntervalTrigger(minutes=max(1, DEMAND_WARM_INTERVAL_MIN))
        scheduler.add_job(
            func=warm_incremental_recent,
            trigger=trigger,
            id="demand_warmup",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            replace_existing=True,
        )
        try:
            warm_incremental_recent()
            fetch_sales_view(view="warehouse", force=True)
        except Exception:
            pass
        print(f"[demand:warmup] registered every {DEMAND_WARM_INTERVAL_MIN} min (init warmed)")
    except Exception as e:
        print("[demand:warmup] failed to register job:", e)


REGISTER_WARMUP_JOB = register_demand_warmup_job

__all__ = ["router", "register_demand_warmup_job", "REGISTER_WARMUP_JOB"]
