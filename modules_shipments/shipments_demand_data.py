# modules_shipments/shipments_demand_data.py
from __future__ import annotations

import logging
import os
import glob
import json
import hashlib
import datetime as dt
from typing import Dict, List, Any, Literal, Tuple, Optional, DefaultDict
from collections import defaultdict
import logging

from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# Импорты из config_package
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.demand_data")

# Логирование
log = logging.getLogger("seller-bot.demand_data")

# Импорты из config_package

# ─────────────────────────────────────────────────────────────
# TZ support (локальная таймзона из .env, если задана)
# ─────────────────────────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _local_zone() -> Optional[Any]:
    tz_name = os.getenv("TZ", "").strip() or None
    if tz_name and ZoneInfo is not None:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return None
    return None


def _to_local_date(iso_dt: str) -> Optional[dt.date]:
    s = str(iso_dt or "").strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            dtu = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dtu = dt.datetime.fromisoformat(s)
        if dtu.tzinfo is None:
            dtu = dtu.replace(tzinfo=dt.timezone.utc)
        tz = _local_zone()
        if tz is not None:
            return dtu.astimezone(tz).date()
        return dtu.astimezone().date()
    except Exception:
        try:
            return dt.date.fromisoformat(s.split("T")[0])
        except Exception:
            return None


# ─────────────────────────────────────────────────────────────
# Paths / env
# ─────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CACHE_DIR = os.path.join(ROOT_DIR, "data", "cache", "shipments")
os.makedirs(CACHE_DIR, exist_ok=True)
SKU_CACHE_DIR = os.path.join(CACHE_DIR, "demand_sku")
os.makedirs(SKU_CACHE_DIR, exist_ok=True)

WARM_STATE_PATH = os.path.join(CACHE_DIR, "demand_warm_state.json")

load_dotenv(os.path.join(ROOT_DIR, ".env"))

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")
OZON_COMPANY_ID = os.getenv("OZON_COMPANY_ID", "")

DEMAND_CACHE_TTL_HOURS = int(os.getenv("DEMAND_CACHE_MAX_AGE_HOURS", "6"))
LOOKBACK_DAYS = int(os.getenv("DEMAND_LOOKBACK_DAYS", "360"))

FBO_LIST_URL = "https://api-seller.ozon.ru/v2/posting/fbo/list"
FBO_LIMIT_MAX = 1000
FBO_OFFSET_MAX = 20000

SESSION_TIMEOUT = int(os.getenv("DEMAND_FETCH_TIMEOUT_SEC", "60"))
MAX_SKU_PER_FETCH = int(os.getenv("DEMAND_MAX_SKU_PER_FETCH", "6"))
SOFT_LIMIT_TOTAL_POSTINGS = int(os.getenv("DEMAND_MAX_TOTAL_POSTINGS", "18000"))

DEMAND_WARM_ENABLED = (os.getenv("DEMAND_WARM_ENABLED", "1").strip() in ("1", "true", "yes", "on"))
DEMAND_WARM_INTERVAL_MIN = int(os.getenv("DEMAND_WARM_INTERVAL_MIN", "15"))
DEMAND_WARM_RECENT_DAYS = int(os.getenv("DEMAND_WARM_RECENT_DAYS", "3"))
DEMAND_WARM_MAX_SKU_PER_CYCLE = int(os.getenv("DEMAND_WARM_MAX_SKU_PER_CYCLE", "8"))

CACHE_SCHEMA_VERSION = 2

# ─────────────────────────────────────────────────────────────
# SKU sources
# ─────────────────────────────────────────────────────────────


def _collect_watch_skus() -> List[str]:
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",")
    out: List[str] = []
    seen: set[str] = set()
    for t in raw.split(","):
        t = t.strip()
        if not t:
            continue
        val = t.split(":", 1)[0].strip()
        if val.isdigit() and val not in seen:
            out.append(val)
            seen.add(val)
    if out:
        return out

    out2: List[str] = []
    seen2: set[str] = set()

    def _push(raws: str):
        for part in (raws or "").replace("\n", ",").split(","):
            p = part.strip()
            if not p:
                continue
            if "=" in p:
                _, p = p.split("=", 1)
            elif ":" in p:
                _, p = p.split(":", 1)
            p = p.strip()
            if p.isdigit() and p not in seen2:
                out2.append(p)
                seen2.add(p)
    _push(os.getenv("WATCH_OFFERS_DICT", "") or "")
    _push(os.getenv("WATCH_OFFERS", "") or "")
    return out2


WATCH_SKU: List[str] = _collect_watch_skus()

# ─────────────────────────────────────────────────────────────
# Stocks helpers
# ─────────────────────────────────────────────────────────────
try:
    from modules_shipments.shipments_data import (
        fetch_stocks_view as _stocks_view,  # type: ignore
        get_warehouse_cluster_map as _w2c,  # type: ignore
        get_current_warehouses as _warehouses,  # type: ignore
    )
except Exception:
    _stocks_view = None  # type: ignore
    try:
        from modules_shipments.shipments_report_data import (  # type: ignore
            fetch_stocks_view as _stocks_view,
            get_warehouse_cluster_map as _w2c,
            get_current_warehouses as _warehouses,
        )
    except Exception:
        _stocks_view = None  # type: ignore
        def _w2c(): return {}    # type: ignore
        def _warehouses(): return {}  # type: ignore


def _cluster_name_map() -> Dict[int, str]:
    out: Dict[int, str] = {}
    if not _stocks_view:
        return out
    for r in _stocks_view(view="cluster") or []:
        try:
            cid = int(r.get("cluster_id") or r.get("id") or 0)
            cname = str(r.get("cluster_name") or r.get("name") or f"cluster:{cid}")
            if cid:
                out[cid] = cname
        except Exception:
            continue
    if out:
        return out
    # fallback: вытягиваем из вида "warehouse"
    for r in _stocks_view(view="warehouse") or []:
        try:
            cid = int(r.get("cluster_id") or 0)
            cname = str(r.get("cluster_name") or f"cluster:{cid}")
            if cid and cid not in out:
                out[cid] = cname
        except Exception:
            continue
    return out


def _warehouse_name_map() -> Dict[int, str]:
    try:
        return {int(k): str(v) for k, v in (_warehouses() or {}).items()}
    except Exception:
        return {}


def _name_to_wid_map() -> Dict[str, int]:
    """
    Маппер строковых названий складов → ID.
    ✅ Добавлены безопасные алиасы: 'wh:123', 'WH:123', 'Склад 123', '123' → 123
    чтобы строки, пришедшие из FBO/Analytics без нормального имени, корректно
    агрегировались по складам.
    """
    try:
        wid2name = _warehouse_name_map()
        out = {name: wid for wid, name in wid2name.items()}
        for wid in list(wid2name.keys()):
            try:
                w = int(wid)
            except Exception:
                continue
            out[f"wh:{w}"] = w
            out[f"WH:{w}"] = w
            out[f"Склад {w}"] = w
            out[str(w)] = w
        return out
    except Exception:
        return {}


def _discover_all_skus() -> List[str]:
    if WATCH_SKU:
        return [s for s in WATCH_SKU if str(s).strip().isdigit()]

    sku_set: List[str] = []
    if _stocks_view:
        try:
            for r in _stocks_view(view="sku") or []:
                sku = r.get("sku") or (r.get("dimension") or {}).get("sku") \
                    or (r.get("dimensions") or [{}])[0].get("sku")
                s = str(sku or "").strip()
                if s.isdigit():
                    sku_set.append(s)
        except Exception:
            pass
    if sku_set:
        seen, uniq = set(), []
        for s in sku_set:
            if s not in seen:
                seen.add(s)
                uniq.append(s)
        return uniq

    return [s for s in WATCH_SKU if str(s).strip().isdigit()]

# ─────────────────────────────────────────────────────────────
# Base helpers
# ─────────────────────────────────────────────────────────────


def _headers() -> Dict[str, str]:
    return {"Client-Id": OZON_CLIENT_ID, "Api-Key": OZON_API_KEY, "Content-Type": "application/json"}


def _read_cache(path: str) -> dict:
    """Читает данные из JSON файла с логированием."""
    return safe_read_json(path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
        except (IOError, OSError, json.JSONDecodeError) as e:
            log.warning(f"Failed to read cache from {path}: {e}")
            return {}


def _write_cache(path: str, data: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return None


def _merge_daily_rows(existing: List[dict], new_rows: List[dict]) -> List[dict]:
    name2wid = _name_to_wid_map()

    def _dkey(r: dict) -> Optional[Tuple[str, int, int, object]]:
        try:
            d = str(r.get("date") or "")
            sku = int(r.get("sku") or 0)
            if not d or sku <= 0:
                return None
            wid_raw = r.get("warehouse_id", None)
            if wid_raw is not None:
                return (d, 1, sku, int(wid_raw))
            wname = str(r.get("warehouse") or "").strip()
            wid = name2wid.get(wname)
            if wid is not None:
                return (d, 1, sku, int(wid))
            return (d, 0, sku, wname)
        except Exception:
            return None

    by_pk: Dict[Tuple[str, int], dict] = {}
    fb_existing: List[dict] = []
    for r in existing or []:
        k = _row_post_key(r)
        if k:
            by_pk[k] = r
        else:
            fb_existing.append(r)
    fb_new: List[dict] = []
    for r in new_rows or []:
        k = _row_post_key(r)
        if k:
            by_pk[k] = r
        else:
            fb_new.append(r)

    new_daily: Dict[Tuple[str, int, int, object], dict] = {}
    for r in fb_new:
        k = _dkey(r)
        if not k:
            continue
        cur = new_daily.get(k)
        if cur is None:
            new_daily[k] = {
                "date": str(r.get("date") or ""),
                "sku": int(r.get("sku") or 0),
                "cluster": "",
                "warehouse_id": (int(r.get("warehouse_id")) if r.get("warehouse_id") is not None else None),
                "warehouse": str(r.get("warehouse") or ""),
                "units": float(r.get("units") or 0.0),
            }
        else:
            cur["units"] = float(cur["units"]) + float(r.get("units") or 0.0)

    existing_daily: Dict[Tuple[str, int, int, object], dict] = {}
    for r in fb_existing:
        k = _dkey(r)
        if not k or k in new_daily:
            continue
        cur = existing_daily.get(k)
        if cur is None:
            existing_daily[k] = {
                "date": str(r.get("date") or ""),
                "sku": int(r.get("sku") or 0),
                "cluster": "",
                "warehouse_id": (int(r.get("warehouse_id")) if r.get("warehouse_id") is not None else None),
                "warehouse": str(r.get("warehouse") or ""),
                "units": float(r.get("units") or 0.0),
            }
        else:
            cur["units"] = float(cur["units"]) + float(r.get("units") or 0.0)

    out: List[dict] = list(by_pk.values()) + list(existing_daily.values()) + \
        list(new_daily.values())
    return out

# ─────────────────────────────────────────────────────────────
# FBO postings (DESC + ранний обрыв)
# ─────────────────────────────────────────────────────────────


def _fetch_fbo_postings_for_sku(sku: str, days: int) -> List[dict]:
    since, to, start_d, _ = _date_range(days)
    headers = _headers()
    all_rows: List[dict] = []
    offset = 0
    limit = FBO_LIMIT_MAX
    errors = 0
    stop = False

    while offset < FBO_OFFSET_MAX and not stop:
        payload = {
            "dir": "DESC",
            "filter": {"since": since, "to": to, "status": "delivered"},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": True, "financial_data": False, "products": True},
        }
        try:
            resp = requests.post(
                FBO_LIST_URL,
                headers=headers,
                json=payload,
                timeout=SESSION_TIMEOUT)
            resp.raise_for_status()
            chunk = resp.json().get("result") or []
        except Exception as e:
            errors += 1
            if errors > 3:
                print(f"[demand:fbo] too many errors for sku={sku}: {e}")
                break
            continue

        if not chunk:
            break

        for p in chunk:
            cd = _created_date(p)
            if cd and cd < start_d:
                stop = True
                continue
            for it in (p.get("products") or []):
                if str(it.get("sku") or "") == str(sku):
                    all_rows.append(p)
                    break

        offset += limit
        if len(chunk) < limit:
            break

    print(f"[demand:fbo] sku={sku} → {len(all_rows)} postings (DESC)")
    return all_rows


def _fetch_fbo_postings_fallback(days: int) -> List[dict]:
    since, to, start_d, _ = _date_range(days)
    headers = _headers()
    rows: List[dict] = []
    offset = 0
    limit = FBO_LIMIT_MAX
    stop = False
    total = 0

    while offset < FBO_OFFSET_MAX and not stop:
        payload = {
            "dir": "DESC",
            "filter": {"since": since, "to": to, "status": "delivered"},
            "limit": limit,
            "offset": offset,
            "with": {"analytics_data": True, "financial_data": False, "products": True},
        }
        try:
            resp = requests.post(
                FBO_LIST_URL,
                headers=headers,
                json=payload,
                timeout=SESSION_TIMEOUT)
            resp.raise_for_status()
            chunk = resp.json().get("result") or []
        except Exception as e:
            print(f"[demand:fbo] request failed at offset={offset}: {e}")
            break
        if not chunk:
            break

        rows.extend(chunk)
        total += len(chunk)
        if SOFT_LIMIT_TOTAL_POSTINGS and total >= SOFT_LIMIT_TOTAL_POSTINGS:
            break

        cd_last = _created_date(chunk[-1])
        if cd_last and cd_last < start_d:
            stop = True
        else:
            offset += limit
            if len(chunk) < limit:
                break

    print(f"[demand:fbo] fallback fetched {len(rows)} postings (DESC, clipped)")
    return rows


def _normalize_fbo_to_daily_rows(postings: List[dict]) -> List[dict]:
    """
    Нормализация постингов в дневные строки.
    ✅ Улучшен выбор имени склада: если в analytics_data нет названия,
      используем карту «текущих складов» по ID или читаемый «Склад <ID>»
      вместо суррогата вида 'wh:<ID>'.
    """
    daily: Dict[Tuple[str, int], float] = defaultdict(float)
    meta_by_post: Dict[str, Tuple[str, str, Optional[int]]] = {}
    name_map = _warehouse_name_map()

    for p in postings:
        a = p.get("analytics_data") or {}
        wid = a.get("warehouse_id")
        try:
            wid = int(wid) if wid is not None else None
        except Exception:
            wid = None
        wname_raw = str(a.get("warehouse_name") or "").strip()
        if wname_raw:
            wname = wname_raw
        elif wid is not None:
            nm = name_map.get(int(wid))
            wname = str(nm or "").strip() or f"Склад {int(wid)}"
        else:
            wname = ""
        created = str(p.get("created_at") or p.get("in_process_at") or "")
        d_local = _to_local_date(created)
        date_str = d_local.isoformat() if d_local else (created.split("T")[0] if created else "")
        pnum = str(p.get("posting_number") or "")
        if not pnum:
            raw = f"{date_str}|{wname}|{
                json.dumps(
                    p.get(
                        'products',
                        []),
                    ensure_ascii=False,
                    sort_keys=True)}"
            pnum = hashlib.md5(raw.encode("utf-8")).hexdigest()[:12]
        if pnum not in meta_by_post:
            meta_by_post[pnum] = (date_str, wname, wid)
        for it in (p.get("products") or []):
            try:
                sku = int(it.get("sku") or 0)
                qty = float(it.get("quantity") or 0)
            except Exception:
                continue
            if sku <= 0 or qty <= 0:
                continue
            daily[(pnum, sku)] += qty
    rows: List[dict] = []
    for (pnum, sku), units in daily.items():
        date_str, wname, wid = meta_by_post.get(pnum, ("", "", None))
        rows.append({
            "pkey": str(pnum),
            "date": date_str,
            "sku": int(sku),
            "cluster": "",
            "warehouse_id": (int(wid) if wid is not None else None),
            "warehouse": wname,
            "units": float(units),
        })
    print(f"[demand:fbo] normalized {len(rows)} daily rows (deduped by posting)")
    return rows

# ─────────────────────────────────────────────────────────────
# Incremental per-SKU fetch/load — ЕДИНОЕ окно LOOKBACK_DAYS
# ─────────────────────────────────────────────────────────────


def _fetch_or_load_sku_rows(sku: str) -> List[dict]:
    cached = _read_sku_cache_rows(sku)
    if cached is not None:
        return cached
    raw = _fetch_fbo_postings_for_sku(sku, LOOKBACK_DAYS)
    rows = _normalize_fbo_to_daily_rows(raw)
    s_int = int(sku) if str(sku).isdigit() else None
    if s_int is not None:
        rows = [r for r in rows if int(r.get("sku", 0)) == s_int]
    _write_sku_cache_rows(sku, rows)
    return rows


def _fetch_delta_for_sku(sku: str, recent_days: int) -> List[dict]:
    cache_path = _sku_cache_path(sku)
    existing = _read_cache(cache_path).get("rows", [])
    if existing:
        raw = _fetch_fbo_postings_for_sku(sku, max(1, int(recent_days)))
        delta_rows = _normalize_fbo_to_daily_rows(raw)
        s_int = int(sku) if str(sku).isdigit() else None
        if s_int is not None:
            delta_rows = [r for r in delta_rows if int(r.get("sku", 0)) == s_int]
        merged = _merge_daily_rows(existing, delta_rows)
        trimmed = _trim_rows_to_window(merged, LOOKBACK_DAYS)
        _write_sku_cache_rows(sku, trimmed)
        return trimmed
    return _fetch_or_load_sku_rows(sku)


def _merge_rows_for_skus(skus: List[str]) -> Tuple[List[dict], str]:
    if not skus:
        return [], "empty"

    fresh_rows: List[dict] = []
    stale_skus: List[str] = []

    for sku in skus:
        r = _read_sku_cache_rows(sku)
        if r is not None:
            fresh_rows.extend(r)
        else:
            stale_skus.append(sku)

    loaded_rows: List[dict] = []
    to_fetch = stale_skus[:max(0, int(MAX_SKU_PER_FETCH))]
    for sku in to_fetch:
        loaded_rows.extend(_fetch_or_load_sku_rows(sku))

    remaining = stale_skus[len(to_fetch):]
    fb_rows: List[dict] = []
    if remaining:
        postings = _fetch_fbo_postings_fallback(LOOKBACK_DAYS)
        fb_rows_all = _normalize_fbo_to_daily_rows(postings)
        remaining_set = {int(s) for s in remaining if str(s).isdigit()}
        fb_rows = [r for r in fb_rows_all if int(r.get("sku", 0)) in remaining_set]
        by_sku = _group_rows_by_sku(fb_rows)
        for sku in remaining:
            s = int(sku)
            rows_for_s = by_sku.get(s, [])
            if rows_for_s:
                _write_sku_cache_rows(sku, rows_for_s)

    all_rows = fresh_rows + loaded_rows + fb_rows
    source = "per_sku_cache+fetch" if not remaining else "per_sku_cache+fetch+fallback"
    return all_rows, source

# ─────────────────────────────────────────────────────────────
# Main fetcher — ВСЕГДА собираем единое окно LOOKBACK_DAYS
# ─────────────────────────────────────────────────────────────


def fetch_sales_view(
    view: Literal["sku", "cluster", "warehouse"] = "sku",
    days: int = 30,
    force: bool = False,
    skus: Optional[List[int]] = None,
) -> List[dict]:
    sku_list = [str(x) for x in (skus or []) if str(x).strip()]
    if not sku_list:
        sku_list = _discover_all_skus()

    rows, _ = _merge_rows_for_skus(sku_list)
    allowed = {int(s) for s in sku_list if str(s).isdigit()}
    rows = [r for r in (rows or []) if int(r.get("sku", 0)) in allowed]
    return rows

# ─────────────────────────────────────────────────────────────
# Aggregations
# ─────────────────────────────────────────────────────────────


def sales_to_D_by_sku(rows: List[dict], period_days: int) -> Dict[int, float]:
    days = max(1, int(period_days))
    agg: DefaultDict[int, float] = defaultdict(float)
    for r in rows:
        try:
            sku = int(r.get("sku") or 0)
            units = float(r.get("units") or 0)
        except Exception:
            continue
        if sku > 0 and units > 0:
            agg[sku] += units
    return {sku: u / days for sku, u in agg.items()}


def sales_to_D_by_warehouse(rows: List[dict], period_days: int) -> Dict[Tuple[int, int], float]:
    days = max(1, int(period_days))
    name2wid = _name_to_wid_map()
    agg_units: DefaultDict[Tuple[int, int], float] = defaultdict(float)

    for r in rows:
        try:
            sku = int(r.get("sku") or 0)
            if sku <= 0:
                continue
            u = float(r.get("units") or 0.0)
            if u <= 0:
                continue
            wid_raw = r.get("warehouse_id", None)
            if wid_raw is not None:
                wid = int(wid_raw)
            else:
                wname = str(r.get("warehouse") or "").strip()
                wid = name2wid.get(wname)
                if wid is None:
                    continue
            agg_units[(int(wid), int(sku))] += u
        except Exception:
            continue

    return {key: (u / days) for key, u in agg_units.items()}

# ─────────────────────────────────────────────────────────────
# View helpers (совместимость)
# ─────────────────────────────────────────────────────────────


def rows_by_warehouse(period: Optional[int] = None,                          skus: Optional[List[int]] = None) -> List[dict]:
    p = int(period or 30)
    rows, _ = _merge_rows_for_skus([str(s) for s in (skus or [])])
    return rows


def rows_by_cluster(period: Optional[int] = None, skus: Optional[List[int]] = None) -> List[dict]:
    rows = rows_by_warehouse(period=period, skus=skus)
    w2c = _w2c() if callable(_w2c) else {}
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

# ─────────────────────────────────────────────────────────────
# Cache maintenance / warm-up
# ─────────────────────────────────────────────────────────────


def clear_demand_cache() -> None:
    for p in glob.glob(os.path.join(CACHE_DIR, "demand_cache_*.json")):
        try:
            os.remove(p)
            print("[demand] cache removed:", p)
        except Exception:
            continue
    for p in glob.glob(os.path.join(SKU_CACHE_DIR, "*.json")):
        try:
            os.remove(p)
            print("[demand] per-SKU cache removed:", p)
        except Exception:
            continue
    try:
        if os.path.exists(WARM_STATE_PATH):
            os.remove(WARM_STATE_PATH)
    except Exception:
        pass


def warm_incremental_recent() -> Dict[str, Any]:
    if not DEMAND_WARM_ENABLED:
        return {"enabled": False}

    skus = _discover_all_skus()
    n = len(skus)
    now = dt.datetime.now()

    st = _read_json(WARM_STATE_PATH) or {}
    last_idx = int(st.get("last_idx") or 0)
    batch_size = max(1, int(DEMAND_WARM_MAX_SKU_PER_CYCLE))
    start = last_idx % max(1, n)
    end = min(n, start + batch_size)
    batch = skus[start:end] if n > 0 else []

    processed = 0
    updated_skus: List[str] = []

    for sku in batch:
        _fetch_delta_for_sku(sku, DEMAND_WARM_RECENT_DAYS)
        updated_skus.append(sku)
        processed += 1

    st["last_idx"] = (start + processed) % max(1, n or 1)
    st["last_run"] = now.isoformat()
    _write_json(WARM_STATE_PATH, st)

    try:
        fetch_sales_view(view="warehouse", force=True,
                         skus=[int(s) for s in skus if str(s).isdigit()])
    except Exception:
        pass

    return {
        "enabled": True,
        "now": now.isoformat(),
        "processed": processed,
        "updated_skus": updated_skus,
        "recent_days": DEMAND_WARM_RECENT_DAYS,
        "batch_size": batch_size,
        "total_skus": n,
    }


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
        print(
            f"[demand:warmup] registered every {DEMAND_WARM_INTERVAL_MIN} min, delta={DEMAND_WARM_RECENT_DAYS}d, batch={DEMAND_WARM_MAX_SKU_PER_CYCLE}")
    except Exception as e:
        print("[demand:warmup] failed to register job:", e)


__all__ = [
    "fetch_sales_view",
    "sales_to_D_by_sku",
    "sales_to_D_by_warehouse",
    "clear_demand_cache",
    "warm_incremental_recent",
    "register_demand_warmup_job",
]
