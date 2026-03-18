# modules_shipments/shipments_leadtime.py
from __future__ import annotations

import os
import json
import datetime as dt
import asyncio
from typing import Dict, Tuple, List, Optional
from collections import defaultdict

from dotenv import load_dotenv

# ── базовые пути/кэш ─────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
CACHE_SHIP_DIR = os.path.join(CACHE_DIR, "shipments")
os.makedirs(CACHE_SHIP_DIR, exist_ok=True)

load_dotenv(os.path.join(ROOT_DIR, ".env"))

# ── Lead Days UI ────────────────────────────────────────────────────────────
LEAD_EDIT_PAGE_SIZE = int(os.getenv("LEAD_EDIT_PAGE_SIZE", "20"))
LEAD_DEFAULT_DAYS = float(os.getenv("LEAD_DEFAULT_DAYS", "0"))
LEAD_MAX_DAYS = float(os.getenv("LEAD_MAX_DAYS", "60"))
LEAD_CACHE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_cache.json")
LEAD_STATES_PATH = os.path.join(
    CACHE_SHIP_DIR, "leadtime_states.json"
)  # для привязки SKU к складам

print("🚀 shipments_leadtime.py loaded:", __file__, "→ cache:", LEAD_CACHE_PATH)

# stocks (локальные справочники/кэш)
try:
    from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore
except Exception:

    def fetch_stocks_view(view: str = "warehouse", force: bool = False) -> List[dict]:
        return []


# ✅ Фолбэк на «устойчивую» логику имён из data‑модуля (для явного обновления)
try:
    from modules_shipments.shipments_leadtime_data import (
        get_current_warehouses as _data_current_warehouses,  # type: ignore
        refresh_warehouse_names as _data_refresh_names,  # type: ignore
    )
except Exception:
    _data_current_warehouses = None  # type: ignore
    _data_refresh_names = None  # type: ignore

# SKU алиасы (если модуль доступен)
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


# ── WATCH_SKU: ограничение набора и порядок (имена — из ALIAS) ─────────────
RAW_WATCH_SKGUARD = os.getenv("WATCH_SKU", "") or ""
RAW_WATCH_SKU = RAW_WATCH_SKGUARD  # сохранение совместимости


def _parse_watch_sku(raw: str) -> List[int]:
    """
    Разбор WATCH_SKU с поддержкой токенов '123' и '123:alias'.
    Сохраняем порядок и убираем дубли.
    """
    txt = (raw or "").replace("\n", ",").replace(" ", ",")
    out: List[int] = []
    seen: set[int] = set()
    for tok in [t.strip() for t in txt.split(",") if t.strip()]:
        left = tok.split(":", 1)[0].strip()
        try:
            v = int(left)
        except Exception:
            continue
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


WATCH_ORDER: List[int] = _parse_watch_sku(RAW_WATCH_SKU)
WATCH_POS = {sku: i for i, sku in enumerate(WATCH_ORDER)}
WATCH_SET = set(WATCH_ORDER)

# 🔧 FIX: ключ сортировки по SKU (раньше приводил к NameError в stats_sku_for_*)


def _order_key_for_sku(sku: int, alias: str = "") -> tuple[int, str]:
    """
    Стабильный ключ сортировки: позиция в WATCH_SKU (если задан), затем alias.
    """
    try:
        return (WATCH_POS.get(int(sku), 10**9), (alias or "").lower())
    except Exception:
        return (10**9, (alias or "").lower())


# ── helpers I/O ──────────────────────────────────────────────────────────────


def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _atomic_write_json(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


# ── лёгкий in-memory кэш справочников ───────────────────────────────────────
LEAD_STOCKS_TTL_MIN = int(os.getenv("LEAD_STOCKS_TTL_MIN", "10"))
STOCKS_MEMO: Dict[Tuple[str, bool], Tuple[dt.datetime, List[dict]]] = {}


def _get_stocks(view: str = "warehouse", force: bool = False) -> List[dict]:
    """Мемо-кэш поверх fetch_stocks_view с TTL по env LEAD_STOCKS_TTL_MIN."""
    now = dt.datetime.now()
    key = (str(view or "warehouse"), bool(force))
    ttl = dt.timedelta(minutes=max(1, LEAD_STOCKS_TTL_MIN))
    if key in STOCKS_MEMO:
        ts, rows = STOCKS_MEMO[key]
        if now - ts <= ttl:
            return rows or []
    rows = fetch_stocks_view(view=view, force=force) or []
    if not rows and not force:
        rows = fetch_stocks_view(view=view, force=True) or []
    STOCKS_MEMO[key] = (now, rows or [])
    return rows or []


# ── вспомогательное: определение «плейсхолдера» wh:<id>/цифровой id ─────────


def _is_placeholder_wh_name(name: str, wid: Optional[int] = None) -> bool:
    s = str(name or "").strip()
    if not s:
        return True
    if s.lower().startswith("wh:"):
        return True
    try:
        if wid is not None and s == str(int(wid)):
            return True
    except Exception:
        pass
    return False


def _extract_wh_name(r: dict, wid: Optional[int]) -> str:
    """
    Унифицированный разбор имени склада из разных провайдеров stocks.
    """
    candidates = [
        r.get("warehouse_name"),
        r.get("warehouse"),
        r.get("name"),
        r.get("title"),
        r.get("warehouseTitle"),
        r.get("warehouse_title"),
    ]
    dims = r.get("dimensions") or []
    if dims and isinstance(dims, list) and isinstance(dims[0], dict):
        candidates.extend(
            [
                dims[0].get("warehouse_name"),
                dims[0].get("warehouse"),
                dims[0].get("name"),
                dims[0].get("title"),
            ]
        )
    for c in candidates:
        s = str(c or "").strip()
        if s:
            return s
    return f"wh:{wid}" if wid is not None else "wh:unknown"


def _extract_cluster_name_and_id(r: dict) -> Tuple[Optional[int], str]:
    """
    Нормализованный разбор (cluster_id, cluster_name) из записи stocks.
    Поддерживает разные варианты полей и вложенность в dimensions[0].
    """
    cid = r.get("cluster_id")
    cname_candidates = [
        r.get("cluster_name"),
        r.get("cluster"),
        r.get("clusterTitle"),
        r.get("cluster_title"),
    ]
    dims = r.get("dimensions") or []
    if dims and isinstance(dims, list) and isinstance(dims[0], dict):
        cid = cid or dims[0].get("cluster_id") or dims[0].get("clusterId")
        cname_candidates.extend(
            [
                dims[0].get("cluster_name"),
                dims[0].get("cluster"),
                dims[0].get("clusterTitle"),
                dims[0].get("cluster_title"),
            ]
        )
    cname = ""
    for c in cname_candidates:
        s = str(c or "").strip()
        if s:
            cname = s
            break
    try:
        cid_i = int(cid) if cid is not None else None
    except Exception:
        cid_i = None
    return cid_i, cname


# ── текущие склады/кластеры (локальные справочники) ─────────────────────────


def get_current_warehouses() -> Dict[int, str]:
    """
    Возвращает {warehouse_id: warehouse_name}, где warehouse_name — человеко‑читаемое.
    Если из stocks пришёл плейсхолдер (wh:<id> или «числовой id»), подставляем
    сохранённое имя из leadtime_cache.json (если оно неплейсхолдерное).
    ⚠️ Быстрое открытие: никаких «тяжёлых» обогащений — только кэш + текущий stocks.
    Улучшение имён запускается вручную по кнопке «🔄 Обновить имена».
    """
    rows = _get_stocks(view="warehouse", force=False)
    # подглянем в кэш сроков ради «спасения» имён
    try:
        cache = _read_json(LEAD_CACHE_PATH)
    except Exception:
        cache = {}
    saved_map = {}
    try:
        for k, v in (cache.get("warehouses") or {}).items():
            if str(k).isdigit():
                saved_map[int(k)] = str((v or {}).get("name") or "")
    except Exception:
        saved_map = {}

    out: Dict[int, str] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        name = _extract_wh_name(r, wid)
        try:
            if wid is None:
                continue
            wid_i = int(wid)
            cur_name = str(name)
            if _is_placeholder_wh_name(cur_name, wid_i):
                cached = saved_map.get(wid_i) or ""
                if cached and not _is_placeholder_wh_name(cached, wid_i):
                    cur_name = cached
            out[wid_i] = cur_name
        except Exception:
            continue

    # ⚠️ Больше НЕ обращаемся к data‑модулю за «лучшим» именем автоматически,
    # чтобы ускорить открытие. Для улучшения используйте кнопку «🔄 Обновить имена».
    return out


def get_warehouse_cluster_map() -> Dict[int, int]:
    """
    Карта склад→кластер с надёжными фолбэками:
        1) пробуем готовую функцию из modules_shipments.shipments_data / shipments_report_data;
        2) берём поля из stocks(view='warehouse');
        3) в крайнем случае — парсим payload load_clusters().
    """
    # 1) готовый маппер, если реализован выше по стеку
    try:
        from modules_shipments.shipments_data import get_warehouse_cluster_map as _w2c  # type: ignore

        m = _w2c() or {}
        if m:
            return {int(k): int(v) for k, v in m.items()}
    except Exception:
        pass
    try:
        from modules_shipments.shipments_report_data import get_warehouse_cluster_map as _w2c_r  # type: ignore

        m = _w2c_r() or {}
        if m:
            return {int(k): int(v) for k, v in m.items()}
    except Exception:
        pass

    # 2) stocks(view='warehouse')
    rows = _get_stocks(view="warehouse", force=False) or []
    out: Dict[int, int] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        cid = r.get("cluster_id")
        # возможна вложенность id в dimensions
        dims = r.get("dimensions") or []
        if cid is None and dims and isinstance(dims[0], dict):
            cid = dims[0].get("cluster_id") or dims[0].get("clusterId")
        try:
            if wid is None or cid is None:
                continue
            out[int(wid)] = int(cid)
        except Exception:
            continue
    if out:
        return out

    # 3) payload load_clusters()
    try:
        from .shipments_report_data import load_clusters  # type: ignore

        js = load_clusters(force=False) or {}
    except Exception:
        js = {}
    if isinstance(js, dict):
        for cl in js.get("clusters") or []:
            cid = cl.get("cluster_id") or cl.get("id") or cl.get("clusterId")
            try:
                cid_i = int(cid)
            except Exception:
                continue
            for lc in cl.get("logistic_clusters") or []:
                for wh in lc.get("warehouses") or []:
                    wid = (
                        wh.get("warehouse_id")
                        or wh.get("id")
                        or wh.get("warehouseId")
                        or (wh.get("warehouse") or {}).get("id")
                    )
                    try:
                        if wid is None:
                            continue
                        out[int(wid)] = cid_i
                    except Exception:
                        continue
    return out


def _cluster_names_from_stocks() -> Dict[int, str]:
    """
    Карта id→name кластеров: сначала из stocks(view='warehouse'),
    затем — бест‑эффорт из payload load_clusters().
    """
    names: Dict[int, str] = {}
    for r in _get_stocks(view="warehouse", force=False) or []:
        try:
            cid, cname = _extract_cluster_name_and_id(r)
            if cid and cname and cid not in names:
                names[cid] = cname
        except Exception:
            continue

    if names:
        return names

    # fallback: load_clusters()
    try:
        from .shipments_report_data import load_clusters  # type: ignore

        js = load_clusters(force=False) or {}
    except Exception:
        js = {}
    if isinstance(js, dict):
        for cl in js.get("clusters") or []:
            cid = cl.get("cluster_id") or cl.get("id") or cl.get("clusterId")
            cname = (
                cl.get("name")
                or cl.get("title")
                or cl.get("cluster_name")
                or cl.get("clusterTitle")
                or ""
            ).strip()
            try:
                if cid and cname and int(cid) not in names:
                    names[int(cid)] = cname
            except Exception:
                continue
    return names


# ── Lead Days cache ─────────────────────────────────────────────────────────


def _empty_cache() -> dict:
    return {"saved_at": dt.datetime.now().isoformat(), "warehouses": {}, "version": 1}


def load_lead_cache() -> dict:
    d = _read_json(LEAD_CACHE_PATH)
    if not isinstance(d, dict) or "warehouses" not in d:
        d = _empty_cache()
        _atomic_write_json(LEAD_CACHE_PATH, d)
    return d


def save_lead_cache(cache: dict) -> None:
    cache = dict(cache or {})
    cache["saved_at"] = dt.datetime.now().isoformat()
    _atomic_write_json(LEAD_CACHE_PATH, cache)


# ── UI focus helpers (возврат к карточке склада) ────────────────────────────


def _set_ui_focus_wid(wid: int, action: str = "edit") -> None:
    try:
        wid = int(wid)
    except Exception:
        return
    cache = load_lead_cache()
    ui = dict(cache.get("ui") or {})
    ui.update(
        {
            "focus_wid": wid,
            "focus_action": str(action or "edit"),
            "focus_at": dt.datetime.now().isoformat(),
        }
    )
    cache["ui"] = ui
    save_lead_cache(cache)


def _consume_ui_focus_wid() -> Optional[int]:
    cache = load_lead_cache()
    ui = dict(cache.get("ui") or {})
    wid = ui.pop("focus_wid", None)
    ui.pop("focus_action", None)
    ui.pop("focus_at", None)
    cache["ui"] = ui
    save_lead_cache(cache)
    try:
        return int(wid) if wid is not None else None
    except Exception:
        return None


# ── Lead Days public (float) ────────────────────────────────────────────────
_MEM_LEADS: Dict[int, float] = {}


def get_lead_for_wid(wid: int) -> Optional[float]:
    try:
        wid = int(wid)
    except Exception:
        return None
    if wid in _MEM_LEADS:
        return float(_MEM_LEADS[wid])
    c = load_lead_cache()
    w = (c.get("warehouses") or {}).get(str(wid))
    if not w:
        return None
    try:
        if "days" not in w or w.get("days") is None:
            return None
        d = float(w.get("days", 0.0))
        return max(0.0, d)
    except Exception:
        return None


def set_lead_for_wid(wid: int, days: float, updated_by: str = "system") -> float:
    """
    Установить срок доставки (в днях, float) для склада wid.
    При ручном вводе (updated_by не начинается с 'stats_sync:') включаем показ склада (manual_enabled=True).
    Также устанавливаем UI‑фокус на карточку склада.
    ⚠️ ВАЖНО: не затираем сохранённое человекочитаемое имя плейсхолдером wh:<id>.
    """
    try:
        wid = int(wid)
        days = float(days)
    except Exception:
        raise ValueError("wid and days must be numeric")
    if days < 0:
        days = 0.0
    if days > LEAD_MAX_DAYS:
        days = float(LEAD_MAX_DAYS)

    cache = load_lead_cache()
    rec = cache.setdefault("warehouses", {}).get(str(wid)) or {}

    # Текущие/сохранённые имена
    curr_name = get_current_warehouses().get(wid)
    saved_name = rec.get("name")
    best_name = curr_name or saved_name or f"wh:{wid}"

    # Если «лучшее» имя оказалось плейсхолдером — попробуем подменить
    if _is_placeholder_wh_name(best_name, wid):
        if curr_name and not _is_placeholder_wh_name(curr_name, wid):
            best_name = curr_name
        elif saved_name and not _is_placeholder_wh_name(saved_name, wid):
            best_name = saved_name

    # сохранить существующие флаги, если они были
    follow_stats = bool(rec.get("follow_stats", False))
    follow_period = (
        int(rec.get("follow_period") or 90) if follow_stats else rec.get("follow_period")
    )
    follow_metric = (
        str(rec.get("follow_metric") or "avg") if follow_stats else rec.get("follow_metric")
    )

    rec.update(
        {
            "days": float(days),
            "updated_at": dt.datetime.now().isoformat(),
            "updated_by": str(updated_by or "system"),
            "follow_stats": follow_stats,
            "follow_period": follow_period,
            "follow_metric": follow_metric,
            "deleted": False,
        }
    )

    # Автовключение видимости в отчётах при ручном вводе (не автосинк)
    if not str(updated_by or "").startswith("stats_sync:"):
        rec["manual_enabled"] = True

    # ⚠️ НЕ перезаписываем 'name' плейсхолдером; если best_name нормальный — сохраняем,
    # иначе оставляем ранее сохранённое человекочитаемое (если было).
    if best_name and not _is_placeholder_wh_name(best_name, wid):
        rec["name"] = str(best_name)
    else:
        if saved_name and not _is_placeholder_wh_name(saved_name, wid):
            rec["name"] = str(saved_name)

    cache["warehouses"][str(wid)] = rec
    save_lead_cache(cache)
    _MEM_LEADS[wid] = float(days)
    print(f"✅ Lead updated for {wid}: {days} → {LEAD_CACHE_PATH}")

    # → вернуть в карточку склада
    _set_ui_focus_wid(wid, action="set")
    return float(days)


def reset_lead_for_wid(wid: int, updated_by: str = "system") -> float:
    val = set_lead_for_wid(wid, 0.0, updated_by=updated_by)
    _set_ui_focus_wid(int(wid), action="reset")
    return val


def delete_lead_for_wid(wid: int) -> None:
    """
    «Удаление» записи склада, не ломая имя:
      • сохраняем 'name' (максимально человеко‑читаемое),
      • удаляем ключ 'days' и ставим флаг 'deleted': True,
      • чистим in-memory кэш,
      • устанавливаем UI‑фокус на карточку склада.
    ⚠️ Если вычисленное имя — плейсхолдер, а в записи уже было сохранено нормальное имя,
      НЕ затираем его.
    """
    wid = int(wid)
    cache = load_lead_cache()
    ws = cache.get("warehouses", {})
    key = str(wid)

    rec = dict(ws.get(key) or {})

    # Максимально человеко‑читаемое имя
    computed = get_warehouse_title(wid)
    curr_name = get_current_warehouses().get(wid)
    saved_name = rec.get("name")

    if _is_placeholder_wh_name(computed, wid):
        if saved_name and not _is_placeholder_wh_name(saved_name, wid):
            computed = saved_name
        elif curr_name and not _is_placeholder_wh_name(curr_name, wid):
            computed = curr_name

    final_name = computed
    if (
        _is_placeholder_wh_name(computed, wid)
        and saved_name
        and not _is_placeholder_wh_name(saved_name, wid)
    ):
        final_name = saved_name

    rec["name"] = final_name
    rec["updated_at"] = dt.datetime.now().isoformat()
    rec["updated_by"] = "deleted:manual"
    rec["deleted"] = True
    if "days" in rec:
        del rec["days"]

    ws[key] = rec
    cache["warehouses"] = ws
    save_lead_cache(cache)

    _MEM_LEADS.pop(wid, None)

    # → вернуть в карточку склада
    _set_ui_focus_wid(wid, action="delete")


def get_all_leads() -> Dict[int, float]:
    cache = load_lead_cache()
    out: Dict[int, float] = {}
    for k, v in (cache.get("warehouses") or {}).items():
        try:
            if not isinstance(v, dict):
                continue
            if "days" not in v or v.get("days") is None:
                continue
            out[int(k)] = float((v or {}).get("days", 0.0) or 0.0)
        except Exception:
            continue
    out.update(_MEM_LEADS)
    return out


def get_progress() -> Tuple[int, int]:
    """
    Прогресс заполнения сроков доставки.
    БАЗА = список складов из «Потребности» (ΣD/день > 0).
    Если «Потребность» недоступна — мягкий фолбэк на справочник складов.
    """
    leads = get_all_leads()
    positive = _wids_with_positive_demand()
    if positive:
        filled = sum(1 for wid in positive if wid in leads and leads[wid] >= 0.0)
        return filled, len(positive)

    # fallback — прежнее поведение
    current = get_current_warehouses()
    filtered_wids = list(current.keys())
    filled = sum(1 for wid in filtered_wids if wid in leads and leads[wid] >= 0.0)
    return filled, len(filtered_wids)


def list_warehouses_page(
    view_page: int = 0, page_size: int | None = None
) -> Tuple[List[Tuple[int, str, str]], int]:
    """
    Постраничный список складов для UI «✍️ Изменить сроки доставки».
    • Если есть «фокус» после редактирования — отдаём карточку этого склада (1 строка).
    • Иначе:
      Основа — WID из «Потребности» (ΣD/день > 0); имена берём через get_warehouse_title().
      Если «Потребность» пустая — фолбэк на весь справочник складов.
      (rows, total), где rows = [(wid, name, indicator '✅'|'⭕'), ...]
    """
    # — карточка склада после редактирования —
    focus = _consume_ui_focus_wid()
    if focus is not None:
        leads = get_all_leads()
        name = get_warehouse_title(int(focus))
        return ([(int(focus), name, "✅" if int(focus) in leads else "⭕")], 1)

    if page_size is None:
        page_size = LEAD_EDIT_PAGE_SIZE

    leads = get_all_leads()
    positive = _wids_with_positive_demand()
    rows_all: List[Tuple[int, str]] = []

    if positive:
        for wid in sorted(positive):
            name = get_warehouse_title(int(wid))
            rows_all.append((int(wid), name))
    else:
        curr = get_current_warehouses()
        rows_all = sorted(
            ((int(wid), str(name)) for wid, name in curr.items()), key=lambda kv: kv[1].lower()
        )

    rows_all.sort(key=lambda kv: kv[1].lower())
    total = len(rows_all)
    start = max(0, int(view_page)) * max(1, int(page_size))
    end = start + page_size
    rows: List[Tuple[int, str, str]] = []
    for wid, name in rows_all[start:end]:
        rows.append((wid, name, "✅" if wid in leads else "⭕"))
    return rows, total


def get_warehouse_title(wid: int) -> str:
    curr = get_current_warehouses()
    curr_name = curr.get(wid)
    meta = (load_lead_cache().get("warehouses") or {}).get(str(int(wid))) or {}
    meta_name = meta.get("name")

    if curr_name and not _is_placeholder_wh_name(curr_name, wid):
        return str(curr_name)
    if meta_name and not _is_placeholder_wh_name(meta_name, wid):
        return str(meta_name)
    if meta_name:
        return str(meta_name)
    if curr_name:
        return str(curr_name)
    return str(f"wh:{wid}")


# ── FOLLOW-STATS: подписка на статистику для склада ─────────────────────────


def enable_follow_stats(wid: int, period: int = 90, metric: str = "avg") -> dict:
    """Включить подписку склада на статистику (period: 90/180/360, metric: 'avg')."""
    wid = int(wid)
    period = int(period or 90)
    metric = str(metric or "avg")
    cache = load_lead_cache()
    rec = cache.setdefault("warehouses", {}).get(str(wid)) or {}
    rec["follow_stats"] = True
    rec["follow_period"] = period
    rec["follow_metric"] = metric
    rec["updated_at"] = dt.datetime.now().isoformat()
    rec["updated_by"] = "follow_stats:on"
    cache["warehouses"][str(wid)] = rec
    save_lead_cache(cache)
    return rec


def disable_follow_stats(wid: int) -> dict:
    """Отключить подписку на статистику — склад возвращается к ручному управлению."""
    wid = int(wid)
    cache = load_lead_cache()
    rec = cache.setdefault("warehouses", {}).get(str(wid)) or {}
    rec["follow_stats"] = False
    rec["updated_at"] = dt.datetime.now().isoformat()
    rec["updated_by"] = "follow_stats:off"
    cache["warehouses"][str(wid)] = rec
    save_lead_cache(cache)
    return rec


def get_following_wids() -> Dict[int, dict]:
    """Вернуть {wid: rec} по всем складам с включённым follow_stats."""
    cache = load_lead_cache()
    out: Dict[int, dict] = {}
    for k, v in (cache.get("warehouses") or {}).items():
        try:
            if (v or {}).get("follow_stats"):
                out[int(k)] = dict(v)
        except Exception:
            continue
    return out


# ── утилиты включения ───────────────────────────────────────────────────────


def _enabled_manual_wids() -> set[int]:
    """
    Включены в ОБЩИЕ отчёты (склады/кластеры):
      • follow_stats == True, ИЛИ
      • manual_enabled == True, ИЛИ
      • обратная совместимость: есть days и updated_by пустой/не 'stats_sync:*'.
    """
    cache = load_lead_cache()
    out: set[int] = set()
    for k, v in (cache.get("warehouses") or {}).items():
        try:
            wid = int(k)
            rec = v or {}
            if rec.get("follow_stats") is True:
                out.add(wid)
                continue
            if rec.get("manual_enabled") is True:
                out.add(wid)
                continue
            if "days" in rec:
                ub = str(rec.get("updated_by") or "")
                if not ub or not ub.startswith("stats_sync:"):
                    out.add(wid)
        except Exception:
            continue
    return out


def _enabled_follow_wids() -> set[int]:
    """
    Включены для SKU-отчёта: ТОЛЬКО подписанные на статистику склады.
    Это убирает «мусор» SKU, если подписка не включалась.
    """
    cache = load_lead_cache()
    return {
        int(k)
        for k, v in (cache.get("warehouses") or {}).items()
        if isinstance(v, dict) and v.get("follow_stats") is True
    }


# ── Фильтр по «Потребности»: берём только склады с ΣD/день > 0 ─────────────


def _wids_with_positive_demand() -> set[int]:
    """
    Возвращает множество warehouse_id, по которым ΣD/день > 0
    согласно текущим настройкам раздела «Потребность».
    """
    try:
        from modules_shipments.shipments_demand import get_positive_demand_wids  # type: ignore

        return set(int(w) for w in (get_positive_demand_wids() or []))
    except Exception:
        return set()


# ── VIEW: отчёты на основе актуальной «базы сроков» ─────────────────────────


def list_enabled_warehouses_for_report() -> List[Tuple[int, str, float]]:
    """[(warehouse_id, warehouse_name, days)] для включённых складов (ручные + подписка),
    ОГРАНИЧЕНО списком складов, где 📄 Потребность — ΣD/день > 0 (если он есть).
    """
    enabled = _enabled_manual_wids()
    positive = _wids_with_positive_demand()
    leads = get_all_leads()
    if positive:
        effective = (enabled or set(leads.keys())) & positive
    else:
        effective = enabled or set(leads.keys())
    curr = get_current_warehouses()
    out: List[Tuple[int, str, float]] = []
    for wid, days in leads.items():
        if int(wid) not in effective:
            continue
        name = get_warehouse_title(int(wid))
        out.append((int(wid), name if name else curr.get(int(wid), f"wh:{wid}"), float(days)))
    out.sort(key=lambda t: str(t[1]).lower())
    return out


def list_enabled_clusters_for_report() -> List[Tuple[int, str, float, int]]:
    """[(cluster_id, cluster_name, avg_days, n_warehouses)] по включённым складам (ручные + подписка),
    ОГРАНИЧЕНО списком складов, где 📄 Потребность — ΣD/день > 0 (если он есть).
    Если по текущим правилам результат пуст — мягкий фолбэк на базовый провайдер.
    """
    enabled = _enabled_manual_wids()
    positive = _wids_with_positive_demand()
    leads = get_all_leads()
    if positive:
        effective = (enabled or set(leads.keys())) & positive
    else:
        effective = enabled or set(leads.keys())
    wid2cid = get_warehouse_cluster_map()
    cid_name = _cluster_names_from_stocks()
    buckets: Dict[int, List[float]] = defaultdict(list)
    for wid, days in leads.items():
        if int(wid) not in effective:
            continue
        cid = wid2cid.get(int(wid))
        if cid is None:
            continue
        buckets[int(cid)].append(float(days))
    out: List[Tuple[int, str, float, int]] = []
    for cid, arr in buckets.items():
        if not arr:
            continue
        avg = sum(arr) / len(arr)
        # 🆕 человеко‑читаемый фолбэк
        out.append((int(cid), cid_name.get(int(cid), f"Кластер {cid}"), float(avg), int(len(arr))))
    out.sort(key=lambda t: str(t[1]).lower())

    # ── ФОЛБЭК: если по включённым складам пусто, попробуем общий просмотр без дополнительных условий
    if not out:
        try:
            from modules_shipments.shipments_leadtime_data import manual_view_by_cluster as _legacy_view  # type: ignore
        except Exception:
            _legacy_view = None  # type: ignore
        if _legacy_view:
            legacy_rows = _legacy_view()  # [(cid, name, avg_days, n)]
            legacy_rows.sort(key=lambda t: str(t[1]).lower())
            return [(int(cid), str(name), float(avg), int(n)) for cid, name, avg, n in legacy_rows]
    return out


# ── Мост к статистике: SKU по складу/кластеру ───────────────────────────────


async def stats_sku_for_warehouse(
    warehouse_id: int, period_days: Optional[int] = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    """
    Оборачиваем источник статистики: фильтруем и упорядочиваем по WATCH_SKU,
    алиасы берём из ALIAS.
    """
    try:
        # ✅ используем фасад statistics (без опечаток)
        from modules_shipments.shipments_leadtime_stats import get_lead_stats_sku_for_warehouse, get_stat_period  # type: ignore

        period = int(period_days or await get_stat_period() or 90)
        tuples = await get_lead_stats_sku_for_warehouse(int(warehouse_id), period) or []
    except Exception:
        tuples = []

    # Принудительно применяем ALIAS + WATCH-фильтр и порядок
    items: List[Tuple[int, str, Dict[str, float]]] = []
    for sku, _alias, m in tuples or []:
        sku_i = int(sku)
        if WATCH_SET and sku_i not in WATCH_SET:
            continue
        alias = (await asyncio.to_thread(get_alias_for_sku, sku_i) or "").strip() or str(sku_i)
        items.append((sku_i, alias, dict(m or {})))

    if WATCH_ORDER:
        items.sort(key=lambda t: _order_key_for_sku(t[0], t[1]))
    else:
        items.sort(key=lambda t: (t[1].lower(), t[0]))

    return items


async def stats_sku_for_cluster(
    cluster_id: int, period_days: Optional[int] = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    """
    Оборачиваем источник статистики: фильтруем и упорядочиваем по WATCH_SKU,
    алиасы берём из ALIAS.
    """
    try:
        # ✅ используем фасад statistics (без опечаток)
        from modules_shipments.shipments_leadtime_stats import get_lead_stats_sku_for_cluster, get_stat_period  # type: ignore

        period = int(period_days or await get_stat_period() or 90)
        tuples = await get_lead_stats_sku_for_cluster(int(cluster_id), period) or []
    except Exception:
        tuples = []

    items: List[Tuple[int, str, Dict[str, float]]] = []
    for sku, _alias, m in tuples or []:
        sku_i = int(sku)
        if WATCH_SET and sku_i not in WATCH_SET:
            continue
        alias = (await asyncio.to_thread(get_alias_for_sku, sku_i) or "").strip() or str(sku_i)
        items.append((sku_i, alias, dict(m or {})))

    if WATCH_ORDER:
        items.sort(key=lambda t: _order_key_for_sku(t[0], t[1]))
    else:
        items.sort(key=lambda t: (t[1].lower(), t[0]))

    return items


# ── Агрегаты по «базе сроков» ───────────────────────────────────────────────


def manual_view_by_warehouse() -> List[Tuple[int, str, float, int]]:
    leads = get_all_leads()
    enabled = _enabled_manual_wids()
    positive = _wids_with_positive_demand()
    if positive:
        effective = (enabled or set(leads.keys())) & positive
    else:
        effective = enabled or set(leads.keys())
    out: List[Tuple[int, str, float, int]] = []
    for wid, days in leads.items():
        if int(wid) not in effective:
            continue
        name = get_warehouse_title(int(wid))
        out.append((int(wid), name, float(days), 1))
    out.sort(key=lambda t: str(t[1]).lower())
    return out


def manual_view_by_cluster() -> List[Tuple[int, str, float, int]]:
    wid2cid = get_warehouse_cluster_map()
    cid_name = _cluster_names_from_stocks()
    leads = get_all_leads()
    enabled = _enabled_manual_wids()
    positive = _wids_with_positive_demand()
    if positive:
        effective = (enabled or set(leads.keys())) & positive
    else:
        effective = enabled or set(leads.keys())
    buckets: Dict[int, List[float]] = defaultdict(list)
    for wid, days in leads.items():
        if int(wid) not in effective:
            continue
        cid = wid2cid.get(int(wid))
        if cid is None:
            continue
        buckets[int(cid)].append(float(days))
    out: List[Tuple[int, str, float, int]] = []
    for cid, arr in buckets.items():
        if not arr:
            continue
        avg = sum(arr) / len(arr)
        # 🆕 человеко‑читаемый фолбэк
        out.append((int(cid), cid_name.get(int(cid), f"Кластер {cid}"), float(avg), int(len(arr))))
    out.sort(key=lambda t: str(t[1]).lower())
    return out


async def manual_view_by_sku() -> List[Tuple[int, str, float, int]]:
    """
    Сводка по SKU строится ТОЛЬКО по подписанным складам; применяем
    фильтрацию и порядок по WATCH_SKU; имена — из ALIAS.
    """
    enabled_follow = _enabled_follow_wids()
    if not enabled_follow:
        return []

    try:
        from modules_shipments.shipments_leadtime_stats import get_stat_period  # type: ignore

        period = int(await get_stat_period() or 90)
    except Exception:
        period = 90

    sum_weighted: Dict[int, float] = defaultdict(float)
    sum_n: Dict[int, float] = defaultdict(float)

    for wid in enabled_follow:
        tuples = await stats_sku_for_warehouse(int(wid), period_days=period)
        for sku, _alias, m in tuples or []:
            n = float((m or {}).get("n", 0) or 0)
            avg = float((m or {}).get("avg", 0.0) or 0.0)
            if n <= 0:
                continue
            sum_weighted[int(sku)] += avg * n
            sum_n[int(sku)] += n

    out: List[Tuple[int, str, float, int]] = []
    for sku, total_n in sum_n.items():
        if total_n <= 0:
            continue
        avg = sum_weighted[sku] / total_n
        alias = (await asyncio.to_thread(get_alias_for_sku, int(sku)) or "").strip() or str(sku)
        if WATCH_SET and int(sku) not in WATCH_SET:
            continue
        out.append((int(sku), alias, float(avg), int(total_n)))

    if WATCH_ORDER:
        out.sort(key=lambda t: _order_key_for_sku(t[0], t[1]))
    else:
        out.sort(key=lambda t: (t[1].lower(), t[0]))

    return out


# ── совместимость имён (старые внешние вызовы) ─────────────────────────────


def save_lead_days(wid: int, days: float, updated_by: str = "system") -> float:
    return set_lead_for_wid(wid, days, updated_by=updated_by)


def reset_lead_days(wid: int, updated_by: str = "system") -> float:
    return reset_lead_for_wid(wid, updated_by=updated_by)


def delete_lead_record(wid: int) -> None:
    return delete_lead_for_wid(wid)


# ── Ручное обновление имён (кнопка «🔄 Обновить имена») ──────────────────────


async def refresh_warehouse_names() -> dict:
    """
    Пробрасывает ручное обновление имён в data‑модуль (если доступен).
    Фолбэк: обновляет кэш по именам из stocks без доп. источников.
    """
    if _data_refresh_names:
        try:
            # assuming _data_refresh_names is sync, wrap it
            return await asyncio.to_thread(_data_refresh_names) or {"updated": 0, "total": 0}
        except Exception:
            pass

    # Фолбэк — быстрое улучшение имен только по stocks (без кластеров/потребности)
    # wrap sync _get_stocks
    rows = await asyncio.to_thread(_get_stocks, view="warehouse", force=True) or []
    cache = load_lead_cache()
    ws = cache.setdefault("warehouses", {})
    updated = 0
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        try:
            if wid is None:
                continue
            wid = int(wid)
        except Exception:
            continue
        nm = _extract_wh_name(r, wid)
        if not nm or _is_placeholder_wh_name(nm, wid):
            continue
        rec = dict(ws.get(str(wid)) or {})
        old = str(rec.get("name") or "")
        if not old or _is_placeholder_wh_name(old, wid):
            rec["name"] = nm
            ws[str(wid)] = rec
            updated += 1
    save_lead_cache(cache)
    return {"updated": int(updated), "total": len(ws)}


# ─────────────────────────────────────────────────────────────
# ТЕКСТОВЫЙ АДАПТЕР ДЛЯ SCHEDULER
# ─────────────────────────────────────────────────────────────


def _format_cluster_lines(limit: int = 30) -> List[str]:
    rows = list_enabled_clusters_for_report()  # (cid, cname, avg_days, n_wh)
    lines: List[str] = []
    for _, cname, avg, n in rows[: max(1, int(limit))]:
        lines.append(f"• {cname}: {avg:.1f} дн (складов: {int(n)})")
    return lines


async def _format_sku_lines(limit: int = 50) -> List[str]:
    rows = await manual_view_by_sku()  # (sku, alias, avg_days, n)
    lines: List[str] = []
    for _, alias, avg, n in rows[: max(1, int(limit))]:
        lines.append(f"• {alias}: {avg:.1f} дн (n={int(n)})")
    return lines


async def leadtime_stats_text(*, view: str = "sku", limit: int = 50, **kwargs) -> str:
    """
    Готовый текстовый отчёт по срокам:
      view: "sku" (по умолчанию) | "cluster" | "warehouse"
      kwargs: допускаем days/period_days для совместимости — они учитываются
              в базовом модуле статистики (через get_stat_period()).
    """
    parts: List[str] = ["⏱ Сроки доставки по SKU"]
    # 🔧 FIX: строгая нормализация типа, чтобы исключить .lower()/.startswith() на int
    v = str(view or "sku").lower()

    # Сводка по кластерам — всегда первой, если есть
    cluster_lines = _format_cluster_lines(limit=30)
    if cluster_lines:
        parts.append("Кластеры:")
        parts.extend(cluster_lines)
        parts.append("")  # пустая строка-разделитель

    if v.startswith("clu"):
        body = parts
    elif v.startswith("ware"):
        rows = list_enabled_warehouses_for_report()
        if rows:
            parts.append("Склады:")
            for _, wname, days, _ in [
                (wid, name, days, 1) for wid, name, days in rows[: max(1, int(limit))]
            ]:
                parts.append(f"• {wname}: {float(days):.1f} дн")
        body = parts
    else:
        sku_lines = await _format_sku_lines(limit=limit)
        if sku_lines:
            parts.append("SKU (средние по подписанным складам):")
            parts.extend(sku_lines)
        body = parts

    text = "\n".join([ln for ln in body if ln is not None])
    return text if text.strip() else ""


# Совместимые алиасы (на случай разных интеграций)


async def delivery_stats_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


async def lead_stats_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


async def stats_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


async def report_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


async def leadtime_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


async def leadtime_report_text(**kwargs) -> str:
    return await leadtime_stats_text(**kwargs)


__all__ = [
    # Lead Days UI / CRUD
    "LEAD_EDIT_PAGE_SIZE",
    "get_progress",
    "list_warehouses_page",
    "get_warehouse_title",
    "set_lead_for_wid",
    "reset_lead_for_wid",
    "delete_lead_for_wid",
    "save_lead_days",
    "reset_lead_days",
    "delete_lead_record",
    "get_lead_for_wid",
    "get_all_leads",
    "get_warehouse_cluster_map",
    "get_current_warehouses",
    # follow-stats
    "enable_follow_stats",
    "disable_follow_stats",
    "get_following_wids",
    # для кнопок/отчётов и дрилл-дауна
    "list_enabled_warehouses_for_report",
    "list_enabled_clusters_for_report",
    "stats_sku_for_warehouse",
    "stats_sku_for_cluster",
    # агрегаты по «базе сроков» / SKU-метрики из статистики
    "manual_view_by_warehouse",
    "manual_view_by_cluster",
    "manual_view_by_sku",
    # текстовый адаптер
    "leadtime_stats_text",
    "delivery_stats_text",
    "lead_stats_text",
    "stats_text",
    "report_text",
    "leadtime_text",
    "leadtime_report_text",
    # ручное обновление имён
    "refresh_warehouse_names",
]
