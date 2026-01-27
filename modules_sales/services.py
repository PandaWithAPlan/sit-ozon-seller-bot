from __future__ import annotations
import logging
import asyncio
import hashlib
import json
import time
import random
import datetime as dt
from typing import Dict, List, Tuple, Any, Optional
from zoneinfo import ZoneInfo

import aiohttp
from config_package import settings
from modules_common.cache_manager import SalesCache

log = logging.getLogger("seller-bot.sales_services")

# ── Константы из settings ────────────────────────────────────────────────────
TZ_NAME: str = settings.timezone
OZON_API_URL: str = settings.ozon_api_url
OZON_CLIENT_ID: str = settings.ozon_client_id
OZON_API_KEY: str = settings.ozon_api_key
OZON_COMPANY_ID: str = settings.ozon_company_id
PRODUCTS_MODE: str = settings.products_mode

SALES_API_MAX_RETRIES: int = settings.api_max_retries
SALES_API_BASE_PAUSE: float = settings.api_base_pause
SALES_API_MAX_PAUSE: float = settings.api_max_pause
SALES_API_JITTER: float = settings.api_jitter
ES_ALPHA: float = settings.es_alpha

# ── Кэш HTTP запросов (микро-LRU) ────────────────────────────────────────────
_HTTP_CACHE: Dict[str, dict] = {}
_HTTP_CACHE_MAX = 128

# ── Методы прогноза ──────────────────────────────────────────────────────────
_METHOD_LABELS_BASE = {
    "ma7": "Средняя за 7 дней",
    "ma14": "Средняя за 14 дней",
    "ma30": "Средняя за 30 дней",
    "ma60": "Средняя за 60 дней",
    "ma90": "Средняя за 90 дней",
    "ma180": "Средняя за 180 дней",
    "ma360": "Средняя за 360 дней",
}
_DEFAULT_METHOD = "ma30"


def _now_local() -> dt.datetime:
    return dt.datetime.now(ZoneInfo(TZ_NAME))

def _yesterday_local() -> dt.date:
    return _now_local().date() - dt.timedelta(days=1)


def _fmt_alpha(a: float) -> str:
    s = f"{float(a):.4f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _label_for(code: str) -> str:
    if code == "es":
        return f"Экспоненциальное сглаживание — {_fmt_alpha(ES_ALPHA)}"
    return _METHOD_LABELS_BASE.get(code, code)


def list_forecast_methods() -> List[Tuple[str, str]]:
    order = ("ma7", "ma14", "ma30", "ma60", "ma90", "ma180", "ma360", "es")
    return [(code, _label_for(code)) for code in order]


def get_forecast_method() -> str:
    mgr = SalesCache.get_forecast_prefs_manager()
    m = mgr.get_key("method") or _DEFAULT_METHOD
    avail = {code for code, _ in list_forecast_methods()}
    return m if m in avail else _DEFAULT_METHOD


def set_forecast_method(code: str) -> str:
    avail = {c for c, _ in list_forecast_methods()}
    if code not in avail:
        return _label_for(get_forecast_method())
    
    mgr = SalesCache.get_forecast_prefs_manager()
    mgr.update_key("method", code)
    return _label_for(code)


def get_forecast_method_title() -> str:
    return _label_for(get_forecast_method())


# ── API / HTTP ───────────────────────────────────────────────────────────────

def _headers() -> Dict[str, str]:
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
        "User-Agent": "seller-bot/forecast/3.0",
    }


def _cache_key(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def _sleep_with_backoff(attempt: int, retry_after_header: Optional[str]) -> None:
    if retry_after_header:
        try:
            pause = float(retry_after_header)
        except Exception:
            pause = None
        if pause is not None:
            time.sleep(min(pause, SALES_API_MAX_PAUSE))
            return
    base = min(SALES_API_BASE_PAUSE * (2 ** max(0, attempt - 1)), SALES_API_MAX_PAUSE)
    jitter = base * random.uniform(0.0, SALES_API_JITTER)
    time.sleep(base + jitter)


async def _post_analytics(payload: dict) -> dict:
    if OZON_COMPANY_ID and OZON_COMPANY_ID.isdigit() and "company_id" not in payload:
        payload = dict(payload)
        payload["company_id"] = int(OZON_COMPANY_ID)

    key = _cache_key(payload)
    if key in _HTTP_CACHE:
        return _HTTP_CACHE[key]

    tries = max(1, SALES_API_MAX_RETRIES)
    timeout = aiohttp.ClientTimeout(connect=5, total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for attempt in range(1, tries + 1):
            try:
                async with session.post(OZON_API_URL, headers=_headers(), json=payload) as r:
                    if r.status == 429:
                        _sleep_with_backoff(attempt, r.headers.get("Retry-After"))
                        continue
                    r.raise_for_status()
                    js = await r.json()
                    
                    # LRU cache update
                    if len(_HTTP_CACHE) >= _HTTP_CACHE_MAX:
                         # С удалением случайного или первого попавшегося для простоты (dict order preserved in Py3.7+)
                        _HTTP_CACHE.pop(next(iter(_HTTP_CACHE))) 
                    _HTTP_CACHE[key] = js
                    return js
            except Exception as e:
                log.warning(f"API attempt {attempt} failed: {e}")
                _sleep_with_backoff(attempt, None)
                continue

    return {"result": {"data": []}}


def _build_filters() -> List[dict]:
    if PRODUCTS_MODE == "SKU":
        # settings.parsed_watch_sku is List[int]
        ids = [str(s) for s in settings.parsed_watch_sku]
        if ids:
            return [{"key": "sku", "value": ",".join(ids)}]
    elif PRODUCTS_MODE == "OFFER":
        offers = settings.parsed_watch_offers
        if offers:
            return [{"key": "offer_id", "value": ",".join(offers)}]
    return []


# ── Fetching Data ────────────────────────────────────────────────────────────

async def fetch_series_from_api(days_back: int) -> Dict[int, List[Tuple[dt.date, float, float]]]:
    end = _yesterday_local()
    start = end - dt.timedelta(days=max(1, days_back) - 1)
    filters = _build_filters()
    series: Dict[int, List[Tuple[dt.date, float, float]]] = {}
    limit = 1000
    offset = 0

    while True:
        body = {
            "date_from": start.strftime("%Y-%m-%d"),
            "date_to":   end.strftime("%Y-%m-%d"),
            "metrics":   ["ordered_units", "revenue"],
            "dimension": ["day", "sku"],
            "filters":   filters,
            "limit":     limit,
            "offset":    offset,
        }
        js = await _post_analytics(body)
        rows = (js.get("result") or {}).get("data") or []
        if not rows:
            break

        for row in rows:
            # Parsing logic (simplified compared to original, assuming good API response)
            dims = row.get("dimensions") or []
            sku = None
            day = None
            for d in dims:
                val = d.get("id") or d.get("value")
                if str(val).isdigit(): sku = int(val)
                else: 
                    try: day = dt.datetime.strptime(str(val), "%Y-%m-%d").date()
                    except: pass
            
            if sku is None or day is None:
                continue

            metrics = row.get("metrics") or []
            units = float(metrics[0]) if len(metrics) > 0 else 0.0
            rev = float(metrics[1]) if len(metrics) > 1 else 0.0
            
            series.setdefault(sku, []).append((day, units, rev))

        if len(rows) < limit:
            break
        offset += limit

    for s in series.values():
        s.sort(key=lambda x: x[0])

    return series


async def fetch_avg_price(days_back: int = 30) -> Dict[int, float]:
    end = _yesterday_local()
    start = end - dt.timedelta(days=max(1, days_back) - 1)
    filters = _build_filters()
    limit = 1000
    offset = 0
    u_sum: Dict[int, float] = {}
    r_sum: Dict[int, float] = {}

    while True:
        body = {
            "date_from": start.strftime("%Y-%m-%d"),
            "date_to":   end.strftime("%Y-%m-%d"),
            "metrics":   ["ordered_units", "revenue"],
            "dimension": ["sku"],
            "filters":   filters,
            "limit":     limit,
            "offset":    offset,
        }
        js = await _post_analytics(body)
        rows = (js.get("result") or {}).get("data") or []
        if not rows:
            break

        for row in rows:
            dims = row.get("dimensions") or []
            sku = None
            if dims:
                val = dims[0].get("id") or dims[0].get("value")
                if str(val).isdigit(): sku = int(val)
            
            if sku is None: continue
            
            metrics = row.get("metrics") or []
            u = float(metrics[0]) if len(metrics) > 0 else 0.0
            r = float(metrics[1]) if len(metrics) > 1 else 0.0
            
            u_sum[sku] = u_sum.get(sku, 0.0) + u
            r_sum[sku] = r_sum.get(sku, 0.0) + r

        if len(rows) < limit:
            break
        offset += limit

    res = {}
    for sku in set(u_sum) | set(r_sum):
        u = u_sum.get(sku, 0.0)
        r = r_sum.get(sku, 0.0)
        res[sku] = (r / u) if u > 0 else 0.0
    return res


# ── Math ─────────────────────────────────────────────────────────────────────

def _ma(values: List[float], window: int) -> float:
    if not values: return 0.0
    w = max(1, min(int(window), len(values)))
    return sum(values[-w:]) / w

def _es(values: List[float], alpha: float) -> float:
    if not values: return 0.0
    s = float(values[0])
    for x in values[1:]:
        s = alpha * float(x) + (1 - alpha) * s
    return s

def calculate_forecast(series: List[Tuple[dt.date, float, float]], horizon: int) -> Tuple[float, float]:
    """
    Returns (units_forecast, revenue_forecast) for the horizon.
    """
    if not series: return 0.0, 0.0
    u = [x[1] for x in series]
    r = [x[2] for x in series]
    
    method = get_forecast_method()
    if method == "es":
        lvl_u = _es(u, ES_ALPHA)
        lvl_r = _es(r, ES_ALPHA)
    else:
        # ma7..ma360
        days = 30
        if method.startswith("ma"):
            try: days = int(method[2:])
            except: pass
        lvl_u = _ma(u, days)
        lvl_r = _ma(r, days)
        
    return lvl_u * horizon, lvl_r * horizon
