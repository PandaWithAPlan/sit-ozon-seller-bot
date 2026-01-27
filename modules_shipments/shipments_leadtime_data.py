# modules_shipments/shipments_leadtime_data.py
from __future__ import annotations

import logging
import os
import json
import datetime as dt
from typing import Dict, Tuple, List, Optional, Iterable
from collections import defaultdict

from dotenv import load_dotenv
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.leadtime_data")

# ── base paths / env ─────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
CACHE_SHIP_DIR = os.path.join(CACHE_DIR, "shipments")
os.makedirs(CACHE_SHIP_DIR, exist_ok=True)

load_dotenv(os.path.join(ROOT_DIR, ".env"))

# ── Lead Days (ручной ввод) настройки ───────────────────────────────────────
LEAD_EDIT_PAGE_SIZE = int(os.getenv("LEAD_EDIT_PAGE_SIZE", "20"))
LEAD_DEFAULT_DAYS = float(os.getenv("LEAD_DEFAULT_DAYS", "0"))
LEAD_MAX_DAYS = float(os.getenv("LEAD_MAX_DAYS", "60"))
LEAD_CACHE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_cache.json")

# states для ручного отчёта по SKU
LEAD_STATES_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_states.json")

# ── справочники (без сети: используем локальные кэши модуля shipments_data) ─
try:
    from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore
except Exception:

    def fetch_stocks_view(view: str = "warehouse", force: bool = False) -> List[dict]:
        return []


# SKU алиасы (если модуль доступен)
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


# ── I/O helpers ─────────────────────────────────────────────────────────────


def _read_json(path: str) -> dict:
    """Читает JSON файл с логированием."""
    return safe_read_json(path)


def _atomic_write_json(path: str, payload: dict) -> None:
    """Записывает JSON файл с логированием."""
    safe_write_json(path, payload)


# ── мемо‑кэш stocks для ускорения UI ────────────────────────────────────────
LEAD_STOCKS_TTL_MIN = int(os.getenv("LEAD_STOCKS_TTL_MIN", "10"))
_STOCKS_MEMO: Dict[Tuple[str, bool], Tuple[dt.datetime, List[dict]]] = {}


def _get_stocks(view: str = "warehouse", force: bool = False) -> List[dict]:
    """
    Мемо‑обёртка над fetch_stocks_view с TTL (LEAD_STOCKS_TTL_MIN).
    Снижает задержки при открытии «Сроков доставки».
    """
    now = dt.datetime.now()
    key = (str(view or "warehouse"), bool(force))
    ttl = dt.timedelta(minutes=max(1, LEAD_STOCKS_TTL_MIN))
    if key in _STOCKS_MEMO:
        ts, rows = _STOCKS_MEMO[key]
        if now - ts <= ttl:
            return rows or []
    rows = fetch_stocks_view(view=view, force=force) or []
    if not rows and not force:
        rows = fetch_stocks_view(view=view, force=True) or []
    _STOCKS_MEMO[key] = (now, rows or [])
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
    Достаём человеко‑читаемое имя склада из разных вариантов полей провайдеров.
    """
    candidates = [
        r.get("warehouse_name"),
        r.get("warehouse"),
        r.get("name"),
        r.get("title"),
        r.get("warehouseTitle"),
        r.get("warehouse_title"),
    ]
    # Иногда имя лежит в dimensions[0]
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


# ── вспомогательные источники имён (из «старого» кода) ──────────────────────


def _states_wid_name_map() -> Dict[int, str]:
    """
    Пары wid→name из leadtime_states.json (если есть).
    Берём только «неплейсхолдерные» имена.
    """
    out: Dict[int, str] = {}
    st = _read_json(LEAD_STATES_PATH)
    if not isinstance(st, dict) or not st:
        return out

    def _good(s: Optional[str]) -> bool:
        return bool(s and not _is_placeholder_wh_name(str(s)))

    for _sid, rec in (st or {}).items():
        if not isinstance(rec, dict):
            continue

        # dropoff
        dw = rec.get("dropoff_wid")
        dn = None
        for key in ("dropoff_name", "dropoff_title", "dropoff_city", "dropoff"):
            v = rec.get(key)
            if isinstance(v, str) and _good(v):
                dn = v.strip()
                break
            if isinstance(v, dict):
                dn = (v.get("name") or v.get("title") or v.get("display_name") or "").strip()
                if _good(dn):
                    break
        try:
            if dw is not None and int(dw) > 0 and _good(dn):
                out[int(dw)] = str(dn)
        except Exception:
            pass

        # storage списки
        wids = rec.get("storage_wids") or []
        names = rec.get("storage_names") or []
        if isinstance(wids, list) and isinstance(names, list) and len(wids) == len(names):
            for w, n in zip(wids, names):
                try:
                    if int(w) > 0 and _good(n):
                        out[int(w)] = str(n).strip()
                except Exception:
                    continue

        # storage массив объектов
        stor = rec.get("storage") or rec.get("storages") or []
        if isinstance(stor, list):
            for obj in stor:
                if not isinstance(obj, dict):
                    continue
                wid = (
                    obj.get("id")
                    or obj.get("wid")
                    or obj.get("warehouse_id")
                    or obj.get("warehouseId")
                )
                nm = (
                    obj.get("name")
                    or obj.get("title")
                    or obj.get("display_name")
                    or obj.get("displayName")
                    or ""
                )
                try:
                    if wid is not None and int(wid) > 0 and _good(nm):
                        out[int(wid)] = str(nm).strip()
                except Exception:
                    continue
    return out


def _remember_names_in_cache(names: Dict[int, str]) -> None:
    """Сохраняем найденные «хорошие» имена в leadtime_cache.json, не трогая дни."""
    if not names:
        return
    cache = load_lead_cache()
    ws = cache.setdefault("warehouses", {})
    changed = False
    for wid, nm in names.items():
        key = str(int(wid))
        rec = ws.get(key) or {}
        old = str(rec.get("name") or "").strip()
        # не перезаписываем нормальное имя плейсхолдером
        if old and not _is_placeholder_wh_name(old):
            continue
        if nm and not _is_placeholder_wh_name(nm):
            rec["name"] = str(nm)
            ws[key] = rec
            changed = True
    if changed:
        save_lead_cache(cache)


def _augment_names_from_clusters(out: Dict[int, str]) -> None:
    """
    Бест‑эффорт: разбираем load_clusters() и вытаскиваем пары wid→name.
    """
    try:
        from .shipments_report_data import load_clusters  # type: ignore
    except Exception:
        return
    try:
        js = load_clusters(force=False) or {}
    except Exception:
        try:
            js = load_clusters(force=True) or {}
        except Exception:
            js = {}

    if not isinstance(js, dict):
        return

    found: Dict[int, str] = {}
    clusters = js.get("clusters") or []
    for cl in clusters:
        # имя склада встречается либо сразу в wh, либо в объекте warehouse
        for lc in cl.get("logistic_clusters") or []:
            for wh in lc.get("warehouses") or []:
                wid = (
                    wh.get("warehouse_id")
                    or wh.get("id")
                    or wh.get("warehouseId")
                    or (wh.get("warehouse") or {}).get("id")
                )
                nm = (
                    wh.get("name")
                    or wh.get("title")
                    or wh.get("warehouse_title")
                    or (wh.get("warehouse") or {}).get("name")
                    or (wh.get("warehouse") or {}).get("title")
                )
                try:
                    if wid is None:
                        continue
                    wid_i = int(wid)
                except Exception:
                    continue
                nm = str(nm or "").strip()
                # если в текущем out имя отсутствует/плейсхолдер — подставим найденное
                if wid_i not in out or _is_placeholder_wh_name(out.get(wid_i, ""), wid_i):
                    if nm and not _is_placeholder_wh_name(nm, wid_i):
                        out[wid_i] = nm
                        found[wid_i] = nm
    if found:
        _remember_names_in_cache(found)


def _augment_names_from_demand(out: Dict[int, str]) -> None:
    """
    Бест‑effort: пытаемся взять имена из блока «Потребность по складам».
    """
    try:
        from modules_shipments.shipments_demand_data import fetch_sales_view  # type: ignore
    except Exception:
        fetch_sales_view = None  # type: ignore

    if not fetch_sales_view:
        return

    try:
        rows = fetch_sales_view(view="warehouse", days=60) or []
    except Exception:
        rows = []

    found: Dict[int, str] = {}
    for r in rows:
        wid = r.get("warehouse_id")
        try:
            if wid is None:
                continue
            wid_i = int(wid)
        except Exception:
            continue
        nm = str(r.get("warehouse") or "").strip()
        if not nm or _is_placeholder_wh_name(nm, wid_i):
            continue
        if wid_i not in out or _is_placeholder_wh_name(out.get(wid_i, ""), wid_i):
            out[wid_i] = nm
            found[wid_i] = nm

    if found:
        _remember_names_in_cache(found)


# ── текущие склады / кластеры (локальные справочники) ──────────────────────


def get_current_warehouses() -> Dict[int, str]:
    """
    Возвращает {warehouse_id: warehouse_name}, где warehouse_name — всегда человеко‑читаемое:
      • если из stocks пришёл плейсхолдер (wh:<id> или «числовой id»),
        берём сохранённое имя из leadtime_cache.json (если оно неплейсхолдерное).
    ⚠️ Быстрое открытие: никаких «тяжёлых» обогащений — только кэш + текущий stocks.
       Улучшение имён запускается вручную по кнопке «🔄 Обновить имена».
    """
    rows = _get_stocks(view="warehouse", force=False) or []
    cache = load_lead_cache()
    saved = {
        int(k): str((v or {}).get("name") or "")
        for k, v in (cache.get("warehouses") or {}).items()
        if isinstance(v, dict) and str(k).isdigit()
    }

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
                cached = saved.get(wid_i) or ""
                if cached and not _is_placeholder_wh_name(cached, wid_i):
                    cur_name = cached

            out[wid_i] = cur_name
        except Exception:
            continue

    return out


def get_warehouse_cluster_map() -> Dict[int, int]:
    """
    Возвращает {warehouse_id: cluster_id}.
    Пытаемся сначала взять готовую функцию из modules_shipments.shipments_data,
    затем — из stocks(view="warehouse"), и напоследок — из payload load_clusters().
    """
    # 1) если есть готовый маппер в modules_shipments.shipments_data — используем его
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

    # 2) stocks(view="warehouse")
    rows = _get_stocks(view="warehouse", force=False) or []
    out: Dict[int, int] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        cid = r.get("cluster_id")
        if cid is None:
            dims = r.get("dimensions") or []
            if dims and isinstance(dims[0], dict):
                cid = dims[0].get("cluster_id") or dims[0].get("clusterId")
        try:
            if wid is None or cid is None:
                continue
            out[int(wid)] = int(cid)
        except Exception:
            continue
    if out:
        return out

    # 3) payload кластеров (load_clusters)
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
    затем бест‑эффорт из payload load_clusters().
    """
    names: Dict[int, str] = {}
    for r in fetch_stocks_view(view="warehouse") or []:
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


# ── Lead Days CRUD (публичные) ──────────────────────────────────────────────
_MEM_LEADS: Dict[int, float] = {}  # лёгкий in-memory кэш (float)


def get_lead_for_wid(wid: int) -> Optional[float]:
    """
    Текущий срок доставки (дней, float) для склада wid или None, если не задан.
    """
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
    Установить срок доставки (в днях, float) для склада wid. Возвращает сохранённое значение (float).
    Также устанавливает UI‑фокус на карточку склада.
    ⚠️ Не затираем нормальное имя плейсхолдером wh:<id>.
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

    # Лучшее известное имя (с защитой от плейсхолдеров)
    curr_name = get_current_warehouses().get(wid)
    saved_name = rec.get("name")
    best_name = curr_name or saved_name or f"wh:{wid}"
    if _is_placeholder_wh_name(best_name, wid):
        if curr_name and not _is_placeholder_wh_name(curr_name, wid):
            best_name = curr_name
        elif saved_name and not _is_placeholder_wh_name(saved_name, wid):
            best_name = saved_name

    rec.update(
        {
            "days": float(days),  # сохраняем float
            "updated_at": dt.datetime.now().isoformat(),
            "updated_by": str(updated_by or "system"),
            # сохранить текущие флаги подписки, если были
            "follow_stats": bool(rec.get("follow_stats", False)),
            "follow_period": (
                int(rec.get("follow_period") or 90)
                if rec.get("follow_stats")
                else rec.get("follow_period")
            ),
            "follow_metric": (
                str(rec.get("follow_metric") or "avg")
                if rec.get("follow_stats")
                else rec.get("follow_metric")
            ),
            "deleted": False,
        }
    )

    # НЕ перезаписываем хорошее имя плейсхолдером
    if best_name and not _is_placeholder_wh_name(best_name, wid):
        rec["name"] = str(best_name)
    else:
        if saved_name and not _is_placeholder_wh_name(saved_name, wid):
            rec["name"] = str(saved_name)

    cache["warehouses"][str(wid)] = rec
    save_lead_cache(cache)

    _MEM_LEADS[wid] = float(days)

    # → вернуть в карточку склада
    _set_ui_focus_wid(wid, action="set")
    return float(days)


def reset_lead_for_wid(wid: int, updated_by: str = "system") -> float:
    """
    Сбросить срок доставки в 0 дней (но запись оставить).
    Возвращает в карточку склада.
    """
    val = set_lead_for_wid(wid, float(LEAD_DEFAULT_DAYS), updated_by=updated_by)
    _set_ui_focus_wid(int(wid), action="reset")
    return val


def delete_lead_for_wid(wid: int) -> None:
    """
    «Удаление» записи склада:
      • сохраняем имя (rec['name']) — берём максимально человеко‑читаемое;
      • очищаем поле 'days' (и помечаем 'deleted': True), чтобы запись НЕ участвовала в расчётах;
      • убираем из in-memory кэша;
      • ставим UI‑фокус на карточку склада.
      ⚠️ Если вычисленное имя — плейсхолдер, а сохранённое имя нормальное — не затираем его.
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
    """
    Возвращает {wid: days(float)} по всем складам, где указан срок (включая in-memory обновления).
    Исключаем записи без ключа 'days' или с None (после «удаления»).
    """
    cache = load_lead_cache()
    out: Dict[int, float] = {}
    for k, v in (cache.get("warehouses") or {}).items():
        try:
            if not isinstance(v, dict):
                continue
            if "days" not in v or v.get("days") is None:
                continue
            out[int(k)] = float(v.get("days", 0.0))
        except Exception:
            continue
    # in-memory правки имеют приоритет
    out.update(_MEM_LEADS)
    return out


# ── фильтр по «Потребности»: берём только склады с ΣD/день > 0 ──────────────


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


def get_progress() -> Tuple[int, int]:
    """
    Прогресс заполнения для раздела «Сроки доставки».
    БАЗА = список складов из «Потребности» (ΣD/день > 0).
    Если «Потребность» недоступна — мягкий фолбэк на справочник складов.
    """
    leads = get_all_leads()
    pos = _wids_with_positive_demand()
    if pos:
        filled = sum(1 for wid in pos if wid in leads and leads[wid] >= 0.0)
        return filled, len(pos)

    # fallback: старое поведение
    current = get_current_warehouses()
    filtered = list(current.keys())
    filled = sum(1 for wid in filtered if wid in leads and leads[wid] >= 0.0)
    return filled, len(filtered)


def list_warehouses_page(
    view_page: int = 0, page_size: int | None = None
) -> Tuple[List[Tuple[int, str, str]], int]:
    """
    Постраничный список складов для UI.
    • Если есть «фокус» после редактирования — отдаём карточку этого склада (1 строка).
    • Иначе:
      Основа — WID из «Потребности» (ΣD/день > 0); имена берём через локальный справочник и leadtime_cache
      одним проходом (без повторных вызовов), что ускоряет открытие.
      Если «Потребность» пустая/недоступна — фолбэк на весь справочник складов.
      Возврат: (rows, total), где rows = [(wid, name, '✅'|'⭕'), ...]
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
    pos = _wids_with_positive_demand()
    # загружаем справочник один раз
    curr_map = get_current_warehouses()
    cache = load_lead_cache()
    meta_ws = cache.get("warehouses") or {}

    def _best_name_for_wid(wid: int) -> str:
        wid = int(wid)
        curr_name = curr_map.get(wid)
        meta_name = (meta_ws.get(str(wid)) or {}).get("name")
        # приоритет — неплейсхолдер
        if curr_name and not _is_placeholder_wh_name(curr_name, wid):
            return str(curr_name)
        if meta_name and not _is_placeholder_wh_name(meta_name, wid):
            return str(meta_name)
        if meta_name:
            return str(meta_name)
        if curr_name:
            return str(curr_name)
        return f"wh:{wid}"

    if pos:
        cand_wids = sorted(int(w) for w in pos)
        pairs = [(wid, _best_name_for_wid(wid)) for wid in cand_wids]
    else:
        cand_wids = sorted(int(w) for w in curr_map.keys())
        pairs = [(wid, _best_name_for_wid(wid)) for wid in cand_wids]

    # сортировка по имени
    pairs.sort(key=lambda kv: kv[1].lower())

    total = len(pairs)
    start = max(0, int(view_page)) * max(1, int(page_size))
    end = start + page_size
    slice_pairs = pairs[start:end]

    rows: List[Tuple[int, str, str]] = []
    for wid, name in slice_pairs:
        rows.append((wid, name, "✅" if wid in leads else "⭕"))
    return rows, total


def get_warehouse_title(wid: int) -> str:
    """
    Имя склада: отдаём человеко‑читаемое («лучшее из источников»).
    Предпочитаем неплейсхолдерные имена; если из справочника пришёл wh:<id>,
    используем сохранённое имя из кэша записи.
    """
    wid = int(wid)
    curr = get_current_warehouses()
    curr_name = curr.get(wid)
    meta = (load_lead_cache().get("warehouses") or {}).get(str(wid)) or {}
    meta_name = meta.get("name")

    # 1) из справочника, если это не плейсхолдер
    if curr_name and not _is_placeholder_wh_name(curr_name, wid):
        return str(curr_name)
    # 2) из записи, если это не плейсхолдер
    if meta_name and not _is_placeholder_wh_name(meta_name, wid):
        return str(meta_name)
    # 3) что есть
    if meta_name:
        return str(meta_name)
    if curr_name:
        return str(curr_name)
    return f"wh:{wid}"


# ── FOLLOW-STATS: подписка на статистику для склада ─────────────────────────


def enable_follow_stats(wid: int, period: int = 90, metric: str = "avg") -> dict:
    """
    Включить подписку склада на статистику (period: 90/180/360, metric: 'avg').
    Значение 'days' остаётся прежним — будет перезаписано при синхронизации.
    """
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
    """
    Отключить подписку на статистику — склад возвращается к ручному управлению.
    """
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
    """
    Вернуть {wid: rec} по всем складам с включённым follow_stats.
    """
    cache = load_lead_cache()
    out: Dict[int, dict] = {}
    for k, v in (cache.get("warehouses") or {}).items():
        try:
            if (v or {}).get("follow_stats"):
                out[int(k)] = dict(v)
        except Exception:
            continue
    return out


# ── MANUAL VIEWS: отчёты из «ручных» сроков ──────────────────────────────────


def manual_view_by_warehouse() -> List[Tuple[int, str, float, int]]:
    """
    [(warehouse_id, warehouse_name, avg_days, n)] на основе leadtime_cache.json (ручные/синхр. сроки)
    + ФИЛЬТР: только склады, у которых 📄 Потребность — ΣD/день > 0.
    """
    leads = get_all_leads()
    pos = _wids_with_positive_demand()
    out: List[Tuple[int, str, float, int]] = []
    # справочник один раз
    curr_map = get_current_warehouses()
    meta_ws = load_lead_cache().get("warehouses") or {}

    def _best_name(wid: int) -> str:
        curr_name = curr_map.get(wid)
        meta_name = (meta_ws.get(str(wid)) or {}).get("name")
        if curr_name and not _is_placeholder_wh_name(curr_name, wid):
            return str(curr_name)
        if meta_name and not _is_placeholder_wh_name(meta_name, wid):
            return str(meta_name)
        return str(meta_name or curr_name or f"wh:{wid}")

    for wid, days in leads.items():
        if pos and int(wid) not in pos:
            continue
        out.append((int(wid), _best_name(int(wid)), float(days), 1))
    # сортировка: по названию
    out.sort(key=lambda t: str(t[1]).lower())
    return out


def manual_view_by_cluster() -> List[Tuple[int, str, float, int]]:
    """
    [(cluster_id, cluster_name, avg_days, n_warehouses)]
    + ФИЛЬТР: только склады, у которых 📄 Потребность — ΣD/день > 0.
    """
    wid2cid = get_warehouse_cluster_map()
    cid_name = _cluster_names_from_stocks()
    leads = get_all_leads()
    pos = _wids_with_positive_demand()
    buckets: Dict[int, List[float]] = defaultdict(list)
    for wid, days in leads.items():
        if pos and int(wid) not in pos:
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
        # человеко‑читаемый фолбэк вместо "C{cid}"
        name = cid_name.get(int(cid), f"Кластер {cid}")
        out.append((int(cid), name, float(avg), int(len(arr))))
    # сортировка по имени
    out.sort(key=lambda t: str(t[1]).lower())
    return out


def manual_view_by_sku() -> List[Tuple[int, str, float, int]]:
    """
    [(sku, alias, avg_days, n_pairs)] по ручным срокам складов:
    • читаем leadtime_states.json → для каждой записи берём storage_wids и sku_items;
    • для каждого (sku, storage_wid) берём lead_days склада, копим в список;
    • усредняем по SKU. Если у склада нет срока — пара игнорируется.
    """
    states = _read_json(LEAD_STATES_PATH)
    if not isinstance(states, dict) or not states:
        return []
    leads = get_all_leads()
    # sku -> list of days
    sku_days: Dict[int, List[float]] = defaultdict(list)
    for _sid, rec in states.items():
        try:
            storage_wids = rec.get("storage_wids") or []
            sku_items = rec.get("sku_items") or []
            if not storage_wids or not sku_items:
                continue
            for wid in storage_wids:
                try:
                    wid_i = int(wid)
                except Exception:
                    continue
                d = leads.get(wid_i)
                if d is None:
                    continue  # нет ручного срока для этого склада
                day_f = float(d)
                for pair in sku_items:
                    try:
                        sku = int(pair[0])
                    except Exception:
                        continue
                    sku_days[sku].append(day_f)
        except Exception:
            continue

    out: List[Tuple[int, str, float, int]] = []
    for sku, arr in sku_days.items():
        if not arr:
            continue
        avg = sum(arr) / len(arr)
        alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
        out.append((int(sku), alias, float(avg), int(len(arr))))
    # сортировка: по alias
    out.sort(key=lambda t: str(t[1]).lower())
    return out


# ── РУЧНОЕ ОБНОВЛЕНИЕ ИМЁН (кнопка «🔄 Обновить имена») ─────────────────────


def refresh_warehouse_names() -> dict:
    """
    Обогащение и кэширование человеко‑читаемых имён складов.
    Источники: leadtime_states.json, payload кластеров, «Потребность по складам», stocks.
    Возвращает статистику: {'updated': X, 'total': Y}
    """
    # состояние «до»
    before = load_lead_cache()
    before_map = {
        int(k): str((v or {}).get("name") or "")
        for k, v in (before.get("warehouses") or {}).items()
        if str(k).isdigit()
    }

    def _good_set(m: Dict[int, str]) -> set[int]:
        return {wid for wid, nm in m.items() if nm and not _is_placeholder_wh_name(nm, wid)}

    before_good = _good_set(before_map)

    # 1) старт — имена из stocks (force=True) для новых складов
    rows = _get_stocks(view="warehouse", force=True) or []
    stocks_names: Dict[int, str] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        try:
            if wid is None:
                continue
            wid = int(wid)
        except Exception:
            continue
        nm = _extract_wh_name(r, wid)
        if nm and not _is_placeholder_wh_name(nm, wid):
            stocks_names[wid] = nm
    _remember_names_in_cache(stocks_names)

    # 2) имена из leadtime_states.json
    states_map = _states_wid_name_map()
    _remember_names_in_cache(states_map)

    # 3) имена из payload кластеров
    out: Dict[int, str] = dict(stocks_names)
    out.update(states_map)
    _augment_names_from_clusters(out)

    # 4) имена из «Потребности по складам»
    _augment_names_from_demand(out)

    # 5) итоговая фиксация всех найденных имён в кэше (на всякий случай)
    _remember_names_in_cache(out)

    # состояние «после»
    after = load_lead_cache()
    after_map = {
        int(k): str((v or {}).get("name") or "")
        for k, v in (after.get("warehouses") or {}).items()
        if str(k).isdigit()
    }
    after_good = _good_set(after_map)

    updated = len(after_good - before_good)
    total = len(after_map)
    return {"updated": int(updated), "total": int(total)}


# ── exports ──────────────────────────────────────────────────────────────────
__all__ = [
    # settings (ручной ввод)
    "LEAD_EDIT_PAGE_SIZE",
    "LEAD_DEFAULT_DAYS",
    "LEAD_MAX_DAYS",
    "LEAD_CACHE_PATH",
    # directories
    "get_current_warehouses",
    "get_warehouse_cluster_map",
    # CRUD
    "get_lead_for_wid",
    "set_lead_for_wid",
    "reset_lead_for_wid",
    "delete_lead_for_wid",
    "get_all_leads",
    "get_progress",
    "list_warehouses_page",
    "get_warehouse_title",
    # follow-stats
    "enable_follow_stats",
    "disable_follow_stats",
    "get_following_wids",
    # manual views for ⏰
    "manual_view_by_warehouse",
    "manual_view_by_cluster",
    "manual_view_by_sku",
    # io helpers (если нужны в верхних слоях)
    "_read_json",
    "_atomic_write_json",
    # ручное обновление имён
    "refresh_warehouse_names",
]
