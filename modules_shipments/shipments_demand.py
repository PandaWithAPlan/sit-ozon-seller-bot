# modules_shipments/shipments_demand.py
from __future__ import annotations

import os
import math
import json
import re
from typing import Dict, Tuple, List, Optional, DefaultDict
from collections import defaultdict
import datetime as dt

# ─────────────────────────────────────────────────────────────
# ЕДИНЫЙ ПРОФИЛЬ НАСТРОЕК (/warehouse)
# ─────────────────────────────────────────────────────────────


def get_demand_settings() -> dict:
    MOD_DIR = os.path.dirname(__file__)
    P1 = os.path.join(MOD_DIR, "data", "cache", "common", "warehouse_prefs.json")
    PROJECT_ROOT = os.path.abspath(os.path.join(MOD_DIR, ".."))
    P2 = os.path.join(PROJECT_ROOT, "data", "cache", "common", "warehouse_prefs.json")

    def _read(path: str) -> dict:
        try:
            if os.path.exists(path):
                import json

                with open(path, "r", encoding="utf-8") as f:
                    d = json.load(f) or {}
                return d if isinstance(d, dict) else {}
        except Exception:
            pass
        return {}

    d = _read(P1) or _read(P2)
    method = (d.get("method") or "").strip().lower()
    period = d.get("period")

    if method not in {"average", "dynamics", "hybrid", "plan_distribution", "plandistr"}:
        method = (os.getenv("DEMAND_METHOD") or os.getenv("WH_METHOD") or "hybrid").strip().lower()
        if method not in {"average", "dynamics", "hybrid", "plan_distribution", "plandistr"}:
            method = "hybrid"

    if method == "plandistr":
        method = "plan_distribution"

    try:
        period = int(period)
    except Exception:
        try:
            period = int(os.getenv("DEMAND_PERIOD") or os.getenv("WH_PERIOD") or "180")
        except Exception:
            period = 180

    return {"method": method, "period": max(1, int(period))}


# ─────────────────────────────────────────────────────────────
# Имена SKU — если понадобится
# ─────────────────────────────────────────────────────────────
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:

    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)


# ─────────────────────────────────────────────────────────────
# Источники справочников
# ─────────────────────────────────────────────────────────────
try:
    from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore
except Exception:
    try:
        from modules_shipments.shipments_report_data import fetch_stocks_view  # type: ignore
    except Exception:
        # type: ignore
        def fetch_stocks_view(view: str = "warehouse", force: bool = False) -> List[dict]:
            return []


try:
    from modules_shipments.shipments_data import get_current_warehouses  # type: ignore
except Exception:

    def get_current_warehouses() -> Dict[int, str]:  # type: ignore
        return {}


# ─────────────────────────────────────────────────────────────
# Данные продаж → D
# ─────────────────────────────────────────────────────────────
try:
    from modules_shipments.shipments_demand_data import (  # type: ignore
        fetch_sales_view,  # rows: {date, sku, cluster, warehouse, warehouse_id, units}
        sales_to_D_by_sku,
        sales_to_D_by_warehouse,
    )
except Exception:

    def fetch_sales_view(
        view: str = "warehouse",
        days: int = 60,
        force: bool = False,
        skus: Optional[List[int]] = None,
    ) -> List[dict]:  # type: ignore
        return []

    def sales_to_D_by_sku(rows: List[dict], period_days: int) -> Dict[int, float]:  # type: ignore
        return {}

    # type: ignore
    def sales_to_D_by_warehouse(rows: List[dict],                                    period_days: int) -> Dict[Tuple[int, int], float]:
        return {}


# ─────────────────────────────────────────────────────────────
# Временные ряды (если нужны)
# ─────────────────────────────────────────────────────────────
try:
    from modules_sales.sales_forecast import (  # type: ignore
        _fetch_series as _fc_fetch_series,
        _forecast_next as _fc_forecast_next,
    )
except Exception:
    _fc_fetch_series = None
    _fc_forecast_next = None

WID = int
CID = int
SKU = int

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LEAD_CACHE_PATH = os.path.join(_PROJECT_ROOT, "data", "cache", "shipments", "leadtime_cache.json")


def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


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


def _clean_warehouse_title(name: str) -> str:
    """
    Удаляем служебные префиксы/шаблоны: 'ID 123', '(ID:123)', 'wh:123', лишние тире/точки.
    """
    src = str(name or "")
    try:
        s = src
        s = re.sub(r"^\s*(?:ID|Id|id)\s*[:\-]?\s*\d+\s*[–—-]?\s*", "", s)
        s = re.sub(r"\(\s*(?:ID|Id|id)\s*[:\-]?\s*\d+\s*\)\s*", "", s)
        s = re.sub(r"^\s*wh:\s*\d+\s*[–—-]?\s*", "", s, flags=re.I)
        s = re.sub(r"\s{2,}", " ", s).strip(" -–—•\t\r\n")
        return s or src
    except Exception:
        return src


# ─────────────────────────────────────────────────────────────
# Имена кластеров (локальная функция; надёжнее, чем импортировать)
# ─────────────────────────────────────────────────────────────


def _cluster_name_map() -> Dict[int, str]:
    names: Dict[int, str] = {}
    for r in fetch_stocks_view(view="warehouse") or []:
        cid = r.get("cluster_id")
        cname = r.get("cluster_name")
        try:
            if cid is not None and str(cname or "").strip():
                names[int(cid)] = str(cname).strip()
        except Exception:
            continue
    for r in fetch_stocks_view(view="cluster") or []:
        cid = r.get("cluster_id") or r.get("id")
        cname = r.get("cluster_name") or r.get("name") or r.get("title")
        try:
            if cid is not None and str(cname or "").strip() and int(cid) not in names:
                names[int(cid)] = str(cname).strip()
        except Exception:
            continue
    if names:
        return names
    try:
        from modules_shipments.shipments_report_data import load_clusters  # type: ignore

        js = load_clusters(force=False) or {}
    except Exception:
        js = {}
    if isinstance(js, dict):
        for cl in js.get("clusters") or []:
            cid = cl.get("cluster_id") or cl.get("id") or cl.get("clusterId")
            cname = (cl.get("name") or cl.get("title") or cl.get("cluster_name") or "").strip()
            try:
                if cid and cname and int(cid) not in names:
                    names[int(cid)] = cname
            except Exception:
                continue
    return names


def map_warehouse_to_cluster() -> Dict[WID, CID]:
    rows = fetch_stocks_view(view="warehouse") or []
    out: Dict[WID, CID] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        cid = r.get("cluster_id")
        try:
            if wid is None or cid is None:
                continue
            out[int(wid)] = int(cid)
        except Exception:
            continue
    if out:
        return out
    try:
        from modules_shipments.shipments_report_data import load_clusters  # type: ignore

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


# ─────────────────────────────────────────────────────────────
# 🆕 Унифицированные карты имён складов и их кластеров
#     (используется хэндлером «📄 Потребность — ΣD/день»)
# ─────────────────────────────────────────────────────────────


def _warehouse_name_maps() -> Tuple[Dict[int, str], Dict[int, Tuple[int, str]]]:
    """
    Возвращает пару:
      • name_by_wid: {wid -> "<читаемое имя склада>"}
      • cluster_by_wid: {wid -> (cid, "<имя кластера>")}
    Источники:
      • get_current_warehouses() / leadtime — имена складов;
      • map_warehouse_to_cluster() — принадлежность к кластерам;
      • _cluster_name_map() — имена кластеров;
      • fallback на fetch_stocks_view(view="warehouse").
    """
    # — имена складов: сначала — leadtime
    name_by_w: Dict[int, str] = {}
    try:
        for wid, nm in (get_current_warehouses() or {}).items():
            try:
                w = int(wid)
                if w > 0 and str(nm).strip():
                    name_by_w[w] = str(nm).strip()
            except Exception:
                continue
    except Exception:
        name_by_w = {}

    # — fallback из stocks
    if not name_by_w:
        for r in fetch_stocks_view(view="warehouse") or []:
            try:
                wid = int(r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id") or 0)
                if wid <= 0:
                    continue
                nm = (
                    r.get("warehouse_name")
                    or r.get("warehouse")
                    and (r.get("warehouse") or {}).get("name")
                    or r.get("name")
                    or r.get("title")
                    or f"wh:{wid}"
                )
                name_by_w[wid] = _clean_warehouse_title(str(nm))
            except Exception:
                continue

    # — карта кластеров и их имён
    w2c = map_warehouse_to_cluster()
    cid2name = _cluster_name_map()

    cluster_by_w: Dict[int, Tuple[int, str]] = {}
    for wid, cid in (w2c or {}).items():
        try:
            w = int(wid)
            c = int(cid)
            cluster_by_w[w] = (c, cid2name.get(c, f"cluster:{c}"))
        except Exception:
            continue

    return name_by_w, cluster_by_w


# ─────────────────────────────────────────────────────────────
_POS_WIDS_CACHE: Dict[str, object] = {"period": None, "wids": [], "saved_at": None}
_POS_WIDS_TTL_MIN = int(os.getenv("DEMAND_POS_WIDS_TTL_MIN", "10"))


def _yesterday() -> dt.date:
    return dt.date.today() - dt.timedelta(days=1)


def _filter_rows_last_days(rows: List[dict], period_days: int) -> List[dict]:
    if not rows:
        return []
    days = max(1, int(period_days))
    end = _yesterday()
    start = end - dt.timedelta(days=days - 1)
    out: List[dict] = []
    for r in rows:
        try:
            d = dt.date.fromisoformat(str(r.get("date") or "")[:10])
        except Exception:
            continue
        if start <= d <= end:
            out.append(r)
    return out


def get_positive_demand_wids(period_days: Optional[int] = None) -> List[int]:
    period = int(period_days or get_demand_settings().get("period") or 180)

    try:
        saved_at: Optional[dt.datetime] = _POS_WIDS_CACHE.get("saved_at")  # type: ignore
        saved_period = _POS_WIDS_CACHE.get("period")
        if saved_at and int(saved_period or 0) == period:
            if (dt.datetime.now() - saved_at) <= dt.timedelta(minutes=max(1, _POS_WIDS_TTL_MIN)):
                return list(_POS_WIDS_CACHE.get("wids") or [])  # type: ignore
    except Exception:
        pass

    rows = _filter_rows_last_days(fetch_sales_view(view="warehouse", days=period), period)
    d_by_ws = sales_to_D_by_warehouse(rows, period)
    sum_by_w: DefaultDict[int, float] = defaultdict(float)
    for (wid, sku), d in d_by_ws.items():
        sum_by_w[int(wid)] += float(d)
    res = [wid for wid, total in sum_by_w.items() if total > 0]

    _POS_WIDS_CACHE["period"] = int(period)
    _POS_WIDS_CACHE["wids"] = list(res)
    _POS_WIDS_CACHE["saved_at"] = dt.datetime.now()

    return res


def _load_rows(period: int, skus: Optional[List[int]] = None) -> List[dict]:
    rows = fetch_sales_view(view="warehouse", days=period, skus=skus)
    return _filter_rows_last_days(rows, period)


def compute_D_average(
    view: str = "warehouse", period: Optional[int] = None, skus: Optional[List[int]] = None
) -> dict:
    v = str(view or "warehouse").lower()
    period_i = int(period or get_demand_settings().get("period") or 180)
    rows = _load_rows(period_i, skus)

    payload = {
        "method": "average",
        "period": period_i,
        "updated_at": dt.datetime.now().isoformat(),
        "view": v,
    }

    if v.startswith("ware"):
        d_map = sales_to_D_by_warehouse(rows, period_i)
        payload["D_by_w_sku"] = d_map
        return payload

    if v.startswith("clu"):
        d_map = sales_to_D_by_warehouse(rows, period_i)
        w2c = map_warehouse_to_cluster()
        agg: DefaultDict[Tuple[int, int], float] = defaultdict(float)
        for (wid, sku), d in d_map.items():
            cid = w2c.get(int(wid))
            if cid is None:
                continue
            agg[(int(cid), int(sku))] += float(d)
        payload["D_by_c_sku"] = dict(agg)
        return payload

    payload["D_by_sku"] = sales_to_D_by_sku(rows, period_i)
    return payload


def compute_D_dynamics(
    view: str = "warehouse", period: Optional[int] = None, skus: Optional[List[int]] = None
) -> dict:
    p = compute_D_average(view=view, period=period, skus=skus)
    p["method"] = "dynamics"
    return p


def compute_D_plan_distribution(
    view: str = "warehouse", period: Optional[int] = None, skus: Optional[List[int]] = None
) -> dict:
    p = compute_D_average(view=view, period=period, skus=skus)
    p["method"] = "plan_distribution"
    return p


def compute_D_hybrid(
    view: str = "warehouse", period: Optional[int] = None, skus: Optional[List[int]] = None
) -> dict:
    p = compute_D_average(view=view, period=period, skus=skus)
    p["method"] = "hybrid"
    return p


def aggregate_to_cluster(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {}
    d_w = payload.get("D_by_w_sku") or {}
    if not d_w:
        return payload if payload.get("D_by_c_sku") else {}
    w2c = map_warehouse_to_cluster()
    agg: DefaultDict[Tuple[int, int], float] = defaultdict(float)
    for k, v in d_w.items():
        try:
            wid, sku = k
            cid = w2c.get(int(wid))
            if cid is None:
                continue
            agg[(int(cid), int(sku))] += float(v)
        except Exception:
            continue
    out = dict(payload)
    out["D_by_c_sku"] = dict(agg)
    out["view"] = "cluster"
    return out


def aggregate_to_sku(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {}
    if "D_by_sku" in payload and payload["D_by_sku"]:
        return payload
    agg: DefaultDict[int, float] = defaultdict(float)
    if "D_by_w_sku" in payload:
        for (wid, sku), d in (payload["D_by_w_sku"] or {}).items():
            agg[int(sku)] += float(d)
    if "D_by_c_sku" in payload:
        for (cid, sku), d in (payload["D_by_c_sku"] or {}).items():
            agg[int(sku)] += float(d)
    out = dict(payload)
    out["D_by_sku"] = dict(agg)
    out["view"] = "sku"
    return out


def rows_by_warehouse(period: Optional[int] = None,                          skus: Optional[List[int]] = None) -> List[dict]:
    p = int(period or get_demand_settings().get("period") or 180)
    return _load_rows(p, skus)


def rows_by_cluster(period: Optional[int] = None, skus: Optional[List[int]] = None) -> List[dict]:
    rows = rows_by_warehouse(period=period, skus=skus)
    w2c = map_warehouse_to_cluster()
    by_key: DefaultDict[Tuple[str, int, int], float] = defaultdict(float)
    for r in rows:
        d = str(r.get("date") or "")
        sku = int(r.get("sku") or 0)
        wid = r.get("warehouse_id")
        try:
            wid = int(wid) if wid is not None else None
        except Exception:
            wid = None
        cid = w2c.get(int(wid)) if wid is not None else None
        if not d or not sku or cid is None:
            continue
        by_key[(d[:10], int(cid), int(sku))] += float(r.get("units") or 0.0)
    out: List[dict] = []
    for (d, cid, sku), units in by_key.items():
        out.append({"date": d, "cluster_id": int(cid), "sku": int(sku), "units": float(units)})
    return sorted(out, key=lambda x: (x["date"], x["cluster_id"], x["sku"]))


def rows_by_sku(period: Optional[int] = None, skus: Optional[List[int]] = None) -> List[dict]:
    rows = rows_by_warehouse(period=period, skus=skus)
    by_key: DefaultDict[Tuple[str, int], float] = defaultdict(float)
    for r in rows:
        d = str(r.get("date") or "")
        sku = int(r.get("sku") or 0)
        if not d or not sku:
            continue
        by_key[(d[:10], int(sku))] += float(r.get("units") or 0.0)
    out: List[dict] = []
    for (d, sku), units in by_key.items():
        out.append({"date": d, "sku": int(sku), "units": float(units)})
    return sorted(out, key=lambda x: (x["date"], x["sku"]))


def export_excel(*_a, **_k) -> str:
    return ""


__all__ = [
    "get_demand_settings",
    "get_positive_demand_wids",
    "compute_D_average",
    "compute_D_dynamics",
    "compute_D_plan_distribution",
    "compute_D_hybrid",
    "aggregate_to_cluster",
    "aggregate_to_sku",
    "rows_by_warehouse",
    "rows_by_cluster",
    "rows_by_sku",
    "export_excel",
]
