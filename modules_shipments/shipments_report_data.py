# modules_shipments/shipments_report_data.py
from __future__ import annotations

import logging
import os
import json
import time
import datetime as dt
import traceback
import asyncio
from typing import Dict, List, Tuple, Any, Optional

import aiohttp
from dotenv import load_dotenv
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.shipments_report_data")

# ─────────────────────────────────────────────────────────────────────────────
# Директории и окружение
# ─────────────────────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
CACHE_SHIP_DIR = os.path.join(CACHE_DIR, "shipments")
CACHE_COMMON_DIR = os.path.join(CACHE_DIR, "common")

for d in (DATA_DIR, CACHE_DIR, CACHE_SHIP_DIR, CACHE_COMMON_DIR):
    os.makedirs(d, exist_ok=True)

load_dotenv(os.path.join(ROOT_DIR, ".env"))

# ─────────────────────────────────────────────────────────────────────────────
# Конфигурация
# ─────────────────────────────────────────────────────────────────────────────
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")
OZON_COMPANY_ID = os.getenv("OZON_COMPANY_ID", "")
PRODUCTS_MODE = (os.getenv("PRODUCTS_MODE", "SKU") or "SKU").upper()

# Важно: в статусе отгрузок работаем по SKU. Переменная WATCH_OFFERS здесь не используется.
# Она имеет смысл для режимов по офферам (PRODUCTS_MODE=OFFER), чтобы
# фильтровать/упорядочивать офферы.
RAW_WATCH_SKU = os.getenv("WATCH_SKU", "") or ""


def _parse_watch_sku(raw: str) -> List[int]:
    """
    Разбираем WATCH_SKU с поддержкой токенов вида '123' и '123:alias'.
    Сохраняем исходный порядок и удаляем дубли (строгий режим).
    """
    txt = (raw or "").replace("\n", ",").replace(" ", ",")
    out: List[int] = []
    seen: set[int] = set()
    for tok in [t.strip() for t in txt.split(",") if t.strip()]:
        left = tok.split(":", 1)[0].strip()  # ← левая часть — это и есть SKU
        try:
            v = int(left)
        except Exception:
            continue
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


WATCH_SKU_ORDER: List[int] = _parse_watch_sku(RAW_WATCH_SKU)
WATCH_SET = set(WATCH_SKU_ORDER)

SHIP_USE_FORECAST_FALLBACK = int(os.getenv("SHIP_USE_FORECAST_FALLBACK", "1")) == 1
SHIP_WRITE_DEBUG = int(os.getenv("SHIP_WRITE_DEBUG", "1")) == 1
STOCKS_CACHE_TTL_HOURS = int(os.getenv("SHIPMENTS_CACHE_MAX_AGE_HOURS", "1"))

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────
CLUSTERS_URL = "https://api-seller.ozon.ru/v1/cluster/list"
STOCKS_URL = "https://api-seller.ozon.ru/v1/analytics/stocks"
STOCKS_BATCH_SIZE = 100

STOCK_METRICS: List[str] = [
    "checking", "in_transit", "valid_stock_count",
    "available_for_sale", "return_from_customer_stock_count", "reserved",
]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _headers() -> Dict[str, str]:
    return {"Client-Id": OZON_CLIENT_ID, "Api-Key": OZON_API_KEY, "Content-Type": "application/json"}


def _read_cache(path: str) -> dict:
    return safe_read_json(path)


def _write_cache(path: str, data: dict):
    safe_write_json(path, data)


def _is_fresh(saved_iso: str, ttl_hours: int) -> bool:
    try:
        saved = dt.datetime.fromisoformat(saved_iso)
        age = (dt.datetime.now() - saved).total_seconds() / 3600
        return age <= ttl_hours
    except Exception:
        return False


def _payload_stocks(dimensions: List[str], skus: List[str]) -> Dict[str, Any]:
    p = {"metrics": STOCK_METRICS, "dimension": dimensions, "limit": 1000, "skus": skus}
    if OZON_COMPANY_ID:
        p["company_id"] = OZON_COMPANY_ID
    return p


def _batch(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def _extract_items(payload: dict) -> List[dict]:
    if not isinstance(payload, dict):
        return []
    if "items" in payload and isinstance(payload["items"], list):
        return payload.get("items", [])
    if "result" in payload:
        return payload.get("result", {}).get("data", []) or []
    return payload.get("data", []) or []


def _fallback_skus_from_forecast() -> List[str]:
    if not SHIP_USE_FORECAST_FALLBACK:
        return []
    try:
        from modules_sales.sales_forecast import _fetch_series  # type: ignore
        return [str(int(k)) for k in (_fetch_series(90) or {}).keys()]
    except Exception:
        return []


def _prepare_skus(explicit: List[int] | None) -> List[str]:
    """
    Возвращает список SKU для запроса stocks.
    • Если переданы явно — используем их.
    • Если в .env задан WATCH_SKU — работаем ТОЛЬКО с ним (без фолбэков).
    • Иначе (WATCH_SKU пуст) — допускаем фолбэк на прогноз.
    """
    if explicit:
        return [str(int(x)) for x in explicit if str(x).strip()]
    if RAW_WATCH_SKU.strip() != "":
        # Жёсткий режим: только из списка наблюдения; никаких фолбэков.
        return [str(s) for s in WATCH_SKU_ORDER]
    # WATCH_SKU не задан — можно попробовать фолбэки
    fb = _fallback_skus_from_forecast()
    if fb:
        print(f"[shipments] fallback SKU из прогноза ({len(fb)} шт.)")
        return fb
    print("[shipments] нет списка SKU — данных не будет")
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Подгрузка и кэш
# ─────────────────────────────────────────────────────────────────────────────
CLUSTERS_CACHE = os.path.join(CACHE_COMMON_DIR, "clusters_cache.json")
STOCKS_CACHE = os.path.join(CACHE_SHIP_DIR, "stocks_cache_shipments.json")
STOCKS_DEBUG = os.path.join(CACHE_SHIP_DIR, "stocks_debug.json")


def load_clusters(force: bool = False) -> Dict[str, Any]:
    """
    Загружает кластеры для РФ и СНГ. Передаём обязательный 'cluster_type',
    чтобы не получать 400 Bad Request.
    """
    cache = _read_cache(CLUSTERS_CACHE)
    if cache and not force:
        return cache

    data: Dict[str, Any] = {"clusters": []}
    try:
        all_clusters: List[dict] = []
        for cluster_type in ("CLUSTER_TYPE_OZON", "CLUSTER_TYPE_CIS"):
            try:
                r = requests.post(CLUSTERS_URL, headers=_headers(), json={"cluster_type": cluster_type}, timeout=30)
                if r.status_code == 429:
                    log.warning(f"API rate limit hit for clusters (type {cluster_type})")
                    time.sleep(2)
                    continue
                r.raise_for_status()
                js = r.json() or {}
                if "clusters" in js and isinstance(js["clusters"], list):
                    all_clusters.extend(js["clusters"])
                    log.debug(f"Fetched {len(js['clusters'])} clusters for type {cluster_type}")
            except requests.Timeout as e:
                log.warning(f"Timeout fetching clusters (type {cluster_type}): {e}")
                continue
            except requests.ConnectionError as e:
                log.warning(f"Connection error fetching clusters (type {cluster_type}): {e}")
                continue
            except requests.HTTPError as e:
                log.error(f"HTTP error fetching clusters (type {cluster_type}): {e}", exc_info=True)
                if hasattr(e, "response") and e.response:
                    if e.response.status_code in (401, 403):
                        log.error(f"Authentication/access error (status {e.response.status_code})")
                        continue  # Пропускаем при 401/403
                continue
            except requests.RequestException as e:
                log.error(f"Request error fetching clusters (type {cluster_type}): {e}", exc_info=True)
                continue
            except Exception as e:
                log.error(f"Unexpected error fetching clusters (type {cluster_type}): {e}", exc_info=True)
                continue
        data["clusters"] = all_clusters
        _write_cache(CLUSTERS_CACHE, data)
        log.info(f"Successfully fetched {len(all_clusters)} clusters")
        return data
     except Exception as e:
         log.error(f"Error in fetch_clusters: {e}", exc_info=True)
         log.warning(f"Returning cached data ({len(cache.get('clusters', []))} clusters)")
         # вернём то, что было (если было)
         return cache

async def fetch_stocks_view(view: str = "sku",                                force: bool = False,                                skus: List[int] | None = None) -> List[dict]:
    """Загрузка остатков по складам / кластерам / SKU с кэшем (и фильтрацией по WATCH_SKU)."""
    cache = _read_cache(STOCKS_CACHE)
    vcache = (cache.get("views") or {}).get(view, {})

    # если кэш свежий
    if vcache and not force and _is_fresh(vcache.get("saved_at", ""), STOCKS_CACHE_TTL_HOURS):
        return vcache.get("rows", []) or []

    sku_list = _prepare_skus(skus)
    if not sku_list:
        return vcache.get("rows", []) if vcache else []

    dims = {"sku": ["sku"], "cluster": ["cluster", "sku"], "warehouse": ["warehouse", "sku"]}.get(view, ["sku"])
    rows: List[dict] = []

    timeout = aiohttp.ClientTimeout(connect=5, total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
         for chunk in _batch(sku_list, STOCKS_BATCH_SIZE):
              try:
                  async with session.post(STOCKS_URL, headers=_headers(), json=_payload_stocks(dims, chunk)) as r:
                      if r.status == 429:
                          log.warning(f"API rate limit hit for stocks request (view {view}, chunk {len(chunk)} SKUs)")
                          await asyncio.sleep(2)
                          continue
                      
                      r.raise_for_status()
                      items = _extract_items(await r.json())
                      rows.extend(items)
                      await asyncio.sleep(0.1)
              except asyncio.TimeoutError as e:
                  log.warning(f"Timeout fetching stocks (view {view}, chunk {len(chunk)} SKUs): {e}")
                  continue
              except aiohttp.ClientError as e:
                  log.warning(f"Connection error fetching stocks (view {view}, chunk {len(chunk)} SKUs): {e}")
                  continue
              except Exception as e:
                  log.error(f"Unexpected error fetching stocks (view {view}, chunk {len(chunk)} SKUs): {e}", exc_info=True)
                  if SHIP_WRITE_DEBUG:
                      traceback.print_exc()
                  continue

    if rows:
        # сортировка стабилизирует групповые списки
        rows.sort(key=lambda r: (str(r.get("cluster_name") or ""), str(r.get("warehouse_name") or "")))
        new_cache = cache or {}
        new_cache.setdefault("views", {})[view] = {
            "rows": rows, "dims": dims, "saved_at": dt.datetime.now().isoformat(), "source": "ozon_api"
        }
        _write_cache(STOCKS_CACHE, new_cache)

        # короткий дамп для диагностики
        if SHIP_WRITE_DEBUG:
            try:
                sample = []
                for rr in rows[:10]:
                    sample.append({
                        "sku": rr.get("sku") or (rr.get("dimension") or {}).get("sku"),
                        "cluster_id": rr.get("cluster_id"),
                        "cluster_name": rr.get("cluster_name"),
                        "warehouse_id": rr.get("warehouse_id"),
                        "warehouse_name": rr.get("warehouse_name"),
                        "metrics": rr.get("metrics"),
                        "available_stock_count": rr.get("available_stock_count"),
                        "other_stock_count": rr.get("other_stock_count"),
                        "transit_stock_count": rr.get("transit_stock_count"),
                        "valid_stock_count": rr.get("valid_stock_count"),
                        "return_from_customer_stock_count": rr.get("return_from_customer_stock_count"),
                        "reserved": rr.get("reserved") or rr.get("reserved_stock"),
                    })
                _write_cache(STOCKS_DEBUG, {
                    "view": view,
                    "saved_at": dt.datetime.now().isoformat(),
                    "sample": sample,
                })
            except Exception:
                pass

        return rows

    # если ничего не получили — отдаём кэш (если был)
    return vcache.get("rows", []) if vcache else []

# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные утилиты (списки)
# ─────────────────────────────────────────────────────────────────────────────
def get_current_warehouses() -> Dict[int, str]:
    """Список актуальных складов (id→имя) по данным Ozon."""
    rows = fetch_stocks_view(view="warehouse") or []
    out: Dict[int, str] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        wname = r.get("warehouse_name")
        if wid is not None:
            try:
                out[int(wid)] = str(wname or f"wh:{wid}")
            except Exception:
                pass
    return out

def get_warehouse_cluster_map() -> Dict[int, int]:
    """Сопоставление склад→кластер (используется в shipments_leadtime и calc_distribution)."""
    rows = fetch_stocks_view(view="warehouse") or []
    mapping: Dict[int, int] = {}
    for r in rows:
        wid = r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id")
        cid = r.get("cluster_id")
        if wid is not None and cid is not None:
            try:
                mapping[int(wid)] = int(cid)
            except Exception:
                pass
    return mapping

def list_warehouses() -> List[Tuple[int, str]]:
    """Упрощённый список складов для мастера расчёта."""
    return sorted(get_current_warehouses().items(), key=lambda x: x[1].lower())

def list_clusters() -> List[Tuple[int, str]]:
    """Список кластеров по кэшу."""
    rows = fetch_stocks_view(view="cluster") or []
    seen = set()
    result = []
    for r in rows:
        cid = r.get("cluster_id")
        cname = r.get("cluster_name")
        if cid is not None and cid not in seen:
            seen.add(cid)
            try:
                result.append((int(cid), str(cname or f"cluster:{cid}")))
            except Exception:
                pass
    return sorted(result, key=lambda x: x[1].lower())

# ─────────────────────────────────────────────────────────────────────────────
# Метрики (нормализация) и полезные утилиты
# ─────────────────────────────────────────────────────────────────────────────
def parse_metrics6(row: dict) -> Dict[str, float]:
    """
    Возвращает словарь 6 метрик с поддержкой альтернативных имён и
    fallback на верхнеуровневые поля ответа.
    """
    keys = [
        "checking",
        "in_transit",
        "valid_stock_count",
        "available_for_sale",
        "return_from_customer_stock_count",
        "reserved",
    ]
    out = {k: 0.0 for k in keys}

    # 1) Внутренний блок metrics / value (dict или list)
    m = row.get("metrics") or row.get("value") or {}
    if isinstance(m, list):
        order = keys  # ожидаем в запрошенном порядке
        for i, k in enumerate(order):
            if i < len(m):
                try:
                    out[k] = float(m[i] or 0)
                except Exception:
                    pass
    elif isinstance(m, dict):
        direct_map: Dict[str, Tuple[str, ...]] = {
            "checking": ("checking",),
            "in_transit": ("in_transit",),
            "valid_stock_count": ("valid_stock_count", "valid_stock", "valid"),
            "available_for_sale": ("available_for_sale",),
            "return_from_customer_stock_count": (
                "return_from_customer_stock_count",
                "return_from_customer_stock",
                "return_from_customer",
            ),
            "reserved": ("reserved", "reserved_stock"),
        }
        for out_key, candidates in direct_map.items():
            for src in candidates:
                if src in m:
                    try:
                        out[out_key] = float(m.get(src, 0) or 0)
                    except Exception:
                        pass
                    break  # к следующему полю

    # 2) Fallback к верхнеуровневым полям
    tops = {
        "available_for_sale": row.get("available_stock_count"),
        "checking": row.get("other_stock_count"),
        "in_transit": row.get("transit_stock_count"),
        "valid_stock_count": row.get("valid_stock_count") or row.get("valid_stock") or row.get("valid"),
        "return_from_customer_stock_count": (
            row.get("return_from_customer_stock_count")
            or row.get("return_from_customer_stock")
            or row.get("return_from_customer")
        ),
        "reserved": row.get("reserved") or row.get("reserved_stock"),
    }
    for k, v in tops.items():
        if v is not None and out[k] == 0.0:
            try:
                out[k] = float(v or 0.0)
            except Exception:
                pass

    # 3) нормализация -0.0 → 0.0
    for k in out:
        if abs(out[k]) < 1e-12:
            out[k] = 0.0

    return out

def total_on_ozon_from_row(row: dict) -> float:
    """Σ Итого на Ozon = сумма 6 метрик."""
    m = parse_metrics6(row)
    return (
        m["checking"]
        + m["in_transit"]
        + m["valid_stock_count"]
        + m["available_for_sale"]
        + m["return_from_customer_stock_count"]
        + m["reserved"]
    )

def metrics_display_pairs(row: dict) -> List[Tuple[str, float]]:
    """Удобный порядок вывода для текстовых витрин."""
    order = [
        "checking",
        "in_transit",
        "valid_stock_count",
        "available_for_sale",
        "return_from_customer_stock_count",
        "reserved",
    ]
    m = parse_metrics6(row)
    return [(k, m.get(k, 0.0)) for k in order]

# ─────────────────────────────────────────────────────────────────────────────
# ТЕКСТОВЫЕ ВИТРИНЫ ДЛЯ «СТАТУС ОТГРУЗОК» — ПОЛНЫЙ ПРЕЖНИЙ ФОРМАТ
# (6 метрик с иконками + Σ, упорядочение по WATCH_SKU)
# ─────────────────────────────────────────────────────────────────────────────

# Алиасы SKU
try:
    from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
except Exception:
    def get_alias_for_sku(sku: int) -> str:  # type: ignore
        return str(sku)

# — печатные настройки
DISPLAY_ORDER = [
    "checking",
    "in_transit",
    "valid_stock_count",
    "available_for_sale",
    "return_from_customer_stock_count",
    "reserved",
]

ICON_LABELS = {
    "checking": ("🧪", "Проверяются"),
    "in_transit": ("🚚", "В пути"),
    "valid_stock_count": ("🛠", "Готовим к продаже"),
    "available_for_sale": ("🛒", "Продаются"),
    "return_from_customer_stock_count": ("↩️", "Возврат"),
    "reserved": ("📦", "Резерв"),
}

def _now_stamp() -> str:
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")

def _head(title: str = "🚚 ОТГРУЗКИ — СТАТУС ТОВАРОВ") -> str:
    return f"{title}\nОбновлено {_now_stamp()}\n\n"

def _alias_or_sku(sku: int) -> str:
    try:
        alias = get_alias_for_sku(sku) or ""
        alias = alias.strip() if isinstance(alias, str) else ""
        return alias or str(sku)
    except Exception:
        return str(sku)

# — упорядочение по WATCH_SKU (как раньше)
WATCH_POS = {sku: i for i, sku in enumerate(WATCH_SKU_ORDER)}
def _sku_sort_key(sku: int) -> Tuple[int, str]:
    """Сначала позиция в WATCH_SKU, затем имя из ALIAS (для стабильности)."""
    return (WATCH_POS.get(int(sku), 10**9), (_alias_or_sku(int(sku)) or "").lower())

# — блок печати одной «карточки»
def _fmt_block(title_line: Optional[str], metric_map: Dict[str, float]) -> List[str]:
    """
    Красивый блок в стиле «Выкупов». ВСЕГДА выводим все 6 строк (даже 0), потом Σ.
    """
    out: List[str] = []
    if title_line:
        out.append(title_line)

    total = 0.0
    for key in DISPLAY_ORDER:
        val = float(metric_map.get(key, 0.0) or 0.0)
        total += val
        icon, label = ICON_LABELS[key]
        out.append(f"{icon} {label} {int(round(val))} шт")

    out.append(f"Σ {int(round(total))} шт")
    out.append("")  # разделитель
    return out

# — нормализация/сопоставление ключей (для legacy‑фолбэка)
def _only_alnum(s: str) -> str:
    import re as _re
    return _re.sub(r'[^0-9A-Za-zА-Яа-яЁё]+', '', s or '')

def _norm(s: str) -> str:
    s = (s or '').replace('Склад:', '').replace('Кластер:', '')
    s = s.strip().lower()
    return _only_alnum(s)

def _parse_numeric_id(key: str, prefixes: tuple[str, ...]) -> int | None:
    k = (key or '').strip().lower()
    for p in prefixes:
        if k.startswith(p):
            k = k[len(p):]
            break
    import re as _re
    k = _re.sub(r'\D+', '', k)
    try:
        return int(k) if k else None
    except Exception:
        return None

def legacy_key_for_cluster(cid: int, cname: Optional[str]) -> str:
    return str(cname).strip() if (cname and str(cname).strip()) else f"cluster:{cid}"

def legacy_key_for_warehouse(wid: int, wname: Optional[str]) -> str:
    return str(wname).strip() if (wname and str(wname).strip()) else f"wh:{wid}"

# — парсеры идентификаторов из строк stocks
def _parse_sku(row: dict) -> int | None:
    sku = (
        row.get("sku")
        or row.get("product_id")
        or (row.get("dimension") or {}).get("sku")
        or (row.get("dimensions", [{}])[-1].get("id") if row.get("dimensions") else None)
    )
    try:
        return int(sku)
    except Exception:
        return None

def _parse_cluster(row: dict) -> Tuple[int | None, str | None]:
    cid = row.get("cluster_id"); cname = row.get("cluster_name")
    if cid is not None:
        try:
            return int(cid), (str(cname) if cname is not None else None)
        except Exception:
            pass
    dims = row.get("dimensions") or []
    if dims:
        try:
            return int(dims[0].get("id")), None
        except Exception:
            pass
    return None, None

def _parse_warehouse(row: dict) -> Tuple[int | None, str | None]:
    wid = row.get("warehouse_id"); wname = row.get("warehouse_name")
    if wid is not None:
        try:
            return int(wid), (str(wname) if wname is not None else None)
        except Exception:
            pass
    dims = row.get("dimensions") or []
    if dims:
        try:
            return int(dims[0].get("id")), None
        except Exception:
            pass
    return None, None

# — агрегаторы «6 метрик» (основа статуса)
def _accumulate6(dst: Dict[str, float], add: Dict[str, float]) -> Dict[str, float]:
    for k, v in add.items():
        dst[k] = float(dst.get(k, 0.0)) + float(v or 0.0)
    return dst

def aggregate6_by_sku(rows: List[dict] | None = None) -> Dict[int, Dict[str, float]]:
    """
    SKU → { checking, in_transit, valid_stock_count, available_for_sale,
            return_from_customer_stock_count, reserved }
    Фильтруем по WATCH_SKU (если задан).
    """
    if rows is None:
        rows = fetch_stocks_view(view="sku") or []
    out: Dict[int, Dict[str, float]] = {}
    for r in rows:
        sku = _parse_sku(r)
        if sku is None:
            continue
        if WATCH_SET and sku not in WATCH_SET:
            continue
        m6 = parse_metrics6(r)
        bucket = out.setdefault(sku, {k: 0.0 for k in m6.keys()})
        out[sku] = _accumulate6(bucket, m6)
    return out

def aggregate6_by_cluster(rows: List[dict] | None = None) -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
    """
    (cluster_id, cluster_name|fallback) → { SKU → {6 метрик} }
    Фильтруем по WATCH_SKU (если задан).
    """
    if rows is None:
        rows = fetch_stocks_view(view="cluster") or []
    out: Dict[Tuple[int, str], Dict[int, Dict[str, float]]] = {}
    for r in rows:
        cid, cname = _parse_cluster(r)
        sku = _parse_sku(r)
        if cid is None or sku is None:
            continue
        if WATCH_SET and sku not in WATCH_SET:
            continue
        key = (cid, cname or f"cluster:{cid}")
        g = out.setdefault(key, {})
        m6 = parse_metrics6(r)
        bucket = g.setdefault(sku, {k: 0.0 for k in m6.keys()})
        g[sku] = _accumulate6(bucket, m6)
    return out

def aggregate6_by_warehouse(rows: List[dict] | None = None) -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
    """
    (warehouse_id, warehouse_name|fallback) → { SKU → {6 метрик} }
    Фильтруем по WATCH_SKU (если задан).
    """
    if rows is None:
        rows = fetch_stocks_view(view="warehouse") or []
    out: Dict[Tuple[int, str], Dict[int, Dict[str, float]]] = {}
    for r in rows:
        wid, wname = _parse_warehouse(r)
        sku = _parse_sku(r)
        if wid is None or sku is None:
            continue
        if WATCH_SET and sku not in WATCH_SET:
            continue
        key = (wid, wname or f"wh:{wid}")
        g = out.setdefault(key, {})
        m6 = parse_metrics6(r)
        bucket = g.setdefault(sku, {k: 0.0 for k in m6.keys()})
        g[sku] = _accumulate6(bucket, m6)
    return out

# — ЛЕГАСИ 3‑метричные срезы (мягкий фолбэк)
def _legacy_metric_tuple3(row: dict) -> Tuple[float, float, float]:
    """
    (available_for_sale, checking, in_transit)
    """
    a = row.get("available_stock_count")
    c = row.get("other_stock_count")
    t = row.get("transit_stock_count")
    if a is not None or c is not None or t is not None:
        return float(a or 0.0), float(c or 0.0), float(t or 0.0)
    m = row.get("metrics") or row.get("value") or {}
    if isinstance(m, list):
        aa = float(m[0] if len(m) > 0 else 0.0)
        cc = float(m[1] if len(m) > 1 else 0.0)
        tt = float(m[2] if len(m) > 2 else 0.0)
        return aa, cc, tt
    else:
        aa = float(m.get("available_for_sale", 0.0))
        cc = float(m.get("checking", 0.0))
        tt = float(m.get("in_transit", 0.0))
        return aa, cc, tt

def legacy_aggregate_by_cluster(rows: List[dict] | None = None) -> Dict[str, Dict[int, Tuple[float, float, float]]]:
    if rows is None:
        rows = fetch_stocks_view(view="cluster") or []
    out: Dict[str, Dict[int, Tuple[float, float, float]]] = {}
    for r in rows:
        cid, cname = _parse_cluster(r); sku = _parse_sku(r)
        if cid is None or sku is None:
            continue
        a, c, t = _legacy_metric_tuple3(r)
        key = legacy_key_for_cluster(cid, cname)
        bucket = out.setdefault(key, {})
        pa, pc, pt = bucket.get(sku, (0.0, 0.0, 0.0))
        bucket[sku] = (pa + a, pc + c, pt + t)
    return out

def legacy_aggregate_by_warehouse(rows: List[dict] | None = None) -> Dict[str, Dict[int, Tuple[float, float, float]]]:
    if rows is None:
        rows = fetch_stocks_view(view="warehouse") or []
    out: Dict[str, Dict[int, Tuple[float, float, float]]] = {}
    for r in rows:
        wid, wname = _parse_warehouse(r); sku = _parse_sku(r)
        if wid is None or sku is None:
            continue
        a, c, t = _legacy_metric_tuple3(r)
        key = legacy_key_for_warehouse(wid, wname)
        bucket = out.setdefault(key, {})
        pa, pc, pt = bucket.get(sku, (0.0, 0.0, 0.0))
        bucket[sku] = (pa + a, pc + c, pt + t)
    return out

# — повышение «находимости» кнопки группы (улучшенные матчеры)
def _pick_target_cluster(agg_keys: List[Tuple[int,                             str]],                             group_key: str) -> Optional[Tuple[int, str]]:
    g_raw = (group_key or "").strip()
    g_low = g_raw.lower()
    cid_wanted = _parse_numeric_id(group_key, ('cluster:',))
    for (cid, cname) in agg_keys:
        if (cname or "").strip().lower() == g_low:
            return (cid, cname)
    if cid_wanted is not None:
        for (cid, cname) in agg_keys:
            if cid == cid_wanted:
                return (cid, cname)
    g_norm = _norm(group_key)
    for (cid, cname) in agg_keys:
        if g_norm and g_norm == _norm(str(cname or "")):
            return (cid, cname)
        if g_norm and g_norm == _norm(str(cid)):
            return (cid, cname)
    for (cid, cname) in agg_keys:
        if g_norm and g_norm in _norm(str(cname or "")):
            return (cid, cname)
    return None

def _pick_target_warehouse(agg_keys: List[Tuple[int,                               str]],                               group_key: str) -> Optional[Tuple[int, str]]:
    g_raw = (group_key or "").strip()
    g_low = g_raw.lower()
    wid_wanted = _parse_numeric_id(group_key, ('wh:', 'warehouse:'))
    for (wid, wname) in agg_keys:
        if (wname or "").strip().lower() == g_low:
            return (wid, wname)
    if wid_wanted is not None:
        for (wid, wname) in agg_keys:
            if wid == wid_wanted:
                return (wid, wname)
    g_norm = _norm(group_key)
    for (wid, wname) in agg_keys:
        if g_norm and g_norm == _norm(str(wname or "")):
            return (wid, wname)
        if g_norm and g_norm == _norm(str(wid)):
            return (wid, wname)
    for (wid, wname) in agg_keys:
        if g_norm and g_norm in _norm(str(wname or "")):
            return (wid, wname)
    return None

# — публичные печатные функции статуса (ПРЕЖНИЙ ФОРМАТ)
def shipments_status_text(view: str = "sku") -> str:
    head = _head()
    lines: List[str] = []

    v = (view or "sku").strip().lower()
    if v == "cluster":
        agg = aggregate6_by_cluster()
        legacy = legacy_aggregate_by_cluster()
        for (cid, cname) in sorted(agg.keys(), key=lambda x: (str(x[1] or x[0]))):
            sku_map = agg[(cid, cname)]
            if not sku_map:
                continue
            # упорядочиваем SKU по WATCH_SKU
            sku_keys = [s for s in sku_map.keys() if (not WATCH_SET or s in WATCH_SET)]
            sku_keys.sort(key=_sku_sort_key)
            leg_bucket = legacy.get(legacy_key_for_cluster(cid, cname), {})
            for sku in sku_keys:
                m6 = dict(sku_map[sku])
                # если по 6 метрикам всё нули — подставим старые 3 метрики
                if sum(m6.get(k, 0.0) for k in DISPLAY_ORDER) == 0.0:
                    a, c, t = leg_bucket.get(sku, (0.0, 0.0, 0.0))
                    m6["available_for_sale"] = a
                    m6["checking"] = c
                    m6["in_transit"] = t
                lines.extend(_fmt_block(_alias_or_sku(sku), m6))

    elif v == "warehouse":
        agg = aggregate6_by_warehouse()
        legacy = legacy_aggregate_by_warehouse()
        for (wid, wname) in sorted(agg.keys(), key=lambda x: (str(x[1] or x[0]))):
            sku_map = agg[(wid, wname)]
            if not sku_map:
                continue
            sku_keys = [s for s in sku_map.keys() if (not WATCH_SET or s in WATCH_SET)]
            sku_keys.sort(key=_sku_sort_key)
            leg_bucket = legacy.get(legacy_key_for_warehouse(wid, wname), {})
            for sku in sku_keys:
                m6 = dict(sku_map[sku])
                if sum(m6.get(k, 0.0) for k in DISPLAY_ORDER) == 0.0:
                    a, c, t = leg_bucket.get(sku, (0.0, 0.0, 0.0))
                    m6["available_for_sale"] = a
                    m6["checking"] = c
                    m6["in_transit"] = t
                lines.extend(_fmt_block(_alias_or_sku(sku), m6))

    else:  # sku
        agg = aggregate6_by_sku()
        sku_keys = [s for s in agg.keys() if (not WATCH_SET or s in WATCH_SET)]
        sku_keys.sort(key=_sku_sort_key)
        for sku in sku_keys:
            lines.extend(_fmt_block(_alias_or_sku(sku), agg[sku]))

    if not lines:
        lines = ["Данных по остаткам нет."]
    return head + "\n".join(lines).rstrip()

def shipments_status_text_group(view: str, group_key: str) -> str:
    """
    Статус только по выбранному кластеру/складу.
    group_key — имя с кнопки, либо 'cluster:<id>'/'wh:<id>', либо число.
    """
    v = (view or "").strip().lower()
    assert v in ("cluster", "warehouse")
    head = _head()

    if v == "cluster":
        agg = aggregate6_by_cluster()
        legacy = legacy_aggregate_by_cluster()
        label = "Кластер"

        target = _pick_target_cluster(list(agg.keys()), group_key)
        if not target:
            # fallback — только по legacy
            key_norm = _norm(group_key)
            legacy_key = None
            for k in legacy.keys():
                if key_norm == _norm(k):
                    legacy_key = k; break
            if legacy_key is None:
                for k in legacy.keys():
                    if key_norm and key_norm in _norm(k):
                        legacy_key = k; break
            if legacy_key is None or not legacy.get(legacy_key):
                return head + f"— {label}: {group_key}\n   Данных по выбранной группе нет."

            lines: List[str] = [f"— {label}: {group_key}"]
            printed = False
            sku_keys = [s for s in legacy[legacy_key].keys() if (not WATCH_SET or s in WATCH_SET)]
            sku_keys.sort(key=_sku_sort_key)
            for sku in sku_keys:
                a, c, t = legacy[legacy_key][sku]
                m6 = {
                    "available_for_sale": float(a or 0.0),
                    "checking": float(c or 0.0),
                    "in_transit": float(t or 0.0),
                    "valid_stock_count": 0.0,
                    "return_from_customer_stock_count": 0.0,
                    "reserved": 0.0,
                }
                lines.extend(_fmt_block(_alias_or_sku(sku), m6))
                printed = True
            if not printed:
                lines.append("   Данных по выбранной группе нет.")
            return head + "\n".join(lines).rstrip()

        cid, cname = target
        title = f"— {label}: {cname or cid}"
        sku_map = agg.get(target, {})
        leg_bucket = legacy.get(legacy_key_for_cluster(cid, cname), {})
        lines: List[str] = [title]
        printed = False
        sku_keys = [s for s in sku_map.keys() if (not WATCH_SET or s in WATCH_SET)]
        sku_keys.sort(key=_sku_sort_key)
        for sku in sku_keys:
            m6 = dict(sku_map[sku])
            if sum(m6.get(k, 0.0) for k in DISPLAY_ORDER) == 0.0:
                a, c, t = leg_bucket.get(sku, (0.0, 0.0, 0.0))
                m6["available_for_sale"] = a
                m6["checking"] = c
                m6["in_transit"] = t
            lines.extend(_fmt_block(_alias_or_sku(sku), m6))
            printed = True
        if not printed:
            lines.append("   Данных по выбранной группе нет.")
        return head + "\n".join(lines).rstrip()

    else:  # warehouse
        agg = aggregate6_by_warehouse()
        legacy = legacy_aggregate_by_warehouse()
        label = "Склад"

        target = _pick_target_warehouse(list(agg.keys()), group_key)
        if not target:
            key_norm = _norm(group_key)
            legacy_key = None
            for k in legacy.keys():
                if key_norm == _norm(k):
                    legacy_key = k; break
            if legacy_key is None:
                for k in legacy.keys():
                    if key_norm and key_norm in _norm(k):
                        legacy_key = k; break
            if legacy_key is None or not legacy.get(legacy_key):
                return head + f"— {label}: {group_key}\n   Данных по выбранной группе нет."

            lines: List[str] = [f"— {label}: {group_key}"]
            printed = False
            sku_keys = [s for s in legacy[legacy_key].keys() if (not WATCH_SET or s in WATCH_SET)]
            sku_keys.sort(key=_sku_sort_key)
            for sku in sku_keys:
                a, c, t = legacy[legacy_key][sku]
                m6 = {
                    "available_for_sale": float(a or 0.0),
                    "checking": float(c or 0.0),
                    "in_transit": float(t or 0.0),
                    "valid_stock_count": 0.0,
                    "return_from_customer_stock_count": 0.0,
                    "reserved": 0.0,
                }
                lines.extend(_fmt_block(_alias_or_sku(sku), m6))
                printed = True
            if not printed:
                lines.append("   Данных по выбранной группе нет.")
            return head + "\n".join(lines).rstrip()

        wid, wname = target
        title = f"— {label}: {wname or wid}"
        sku_map = agg.get(target, {})
        leg_bucket = legacy.get(legacy_key_for_warehouse(wid, wname), {})
        lines: List[str] = [title]
        printed = False
        sku_keys = [s for s in sku_map.keys() if (not WATCH_SET or s in WATCH_SET)]
        sku_keys.sort(key=_sku_sort_key)
        for sku in sku_keys:
            m6 = dict(sku_map[sku])
            if sum(m6.get(k, 0.0) for k in DISPLAY_ORDER) == 0.0:
                a, c, t = leg_bucket.get(sku, (0.0, 0.0, 0.0))
                m6["available_for_sale"] = a
                m6["checking"] = c
                m6["in_transit"] = t
            lines.extend(_fmt_block(_alias_or_sku(sku), m6))
            printed = True
        if not printed:
            lines.append("   Данных по выбранной группе нет.")
        return head + "\n".join(lines).rstrip()

# ─────────────────────────────────────────────────────────────────────────────
# Регистрация алиаса модуля (без создания нового файла).
# Это позволяет импортировать modules_shipments.shipments_report, используя данный модуль.
import sys as _sys
_sys.modules.setdefault("modules_shipments.shipments_report", _sys.modules[__name__])

# ─────────────────────────────────────────────────────────────────────────────
# Экспорт
# ─────────────────────────────────────────────────────────────────────────────
__all__ = [
    # загрузка/списки
    "fetch_stocks_view", "load_clusters",
    "get_current_warehouses", "get_warehouse_cluster_map",
    "list_warehouses", "list_clusters",
    # метрики и утилиты
    "parse_metrics6", "total_on_ozon_from_row", "metrics_display_pairs",
    # агрегаторы 6‑метричные
    "aggregate6_by_sku", "aggregate6_by_cluster", "aggregate6_by_warehouse",
    # публичный вывод (как раньше)
    "shipments_status_text", "shipments_status_text_group",
]
