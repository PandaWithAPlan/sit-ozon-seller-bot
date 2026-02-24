from __future__ import annotations
from modules_common.paths import ensure_dirs, CACHE_SALES

import os
import json
import time
import asyncio
import datetime as dt
from typing import Dict, List, Tuple, Any, Optional

import aiohttp
from dotenv import load_dotenv

# ── базовые пути / env по ТЗ 5.1 ─────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(ROOT_DIR, ".env"))

ensure_dirs()

OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")
OZON_COMPANY_ID = os.getenv("OZON_COMPANY_ID", "")
PRODUCTS_MODE = (os.getenv("PRODUCTS_MODE", "SKU") or "SKU").upper()

# фильтры (как строки!)
WATCH_SKU: List[str] = [
    s.strip() for s in (os.getenv("WATCH_SKU", "") or "").split(",") if s.strip()
]
WATCH_OFFERS: List[str] = [
    s.strip() for s in (os.getenv("WATCH_OFFERS", "") or "").split(",") if s.strip()
]

# дроссель трафиковых запросов
TRAFFIC_MIN_INTERVAL = float(os.getenv("TRAFFIC_MIN_INTERVAL", "65"))
_LAST_TRAFFIC_CALL = 0.0

# внешний вид


def _now_stamp() -> str:
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")


def _fmt_pct(x: float) -> str:
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "0.00%"


# ── реюз хелперов из sales_facts_store: алиасы, порядок, допустимые SKU ─────
try:
    from modules_sales.sales_facts_store import (
        get_alias_for_sku,
        _watch_skus_order_list as _order_list,  # type: ignore
        _allowed_sku_set as _allowed_set,  # type: ignore
    )
except Exception:
    # Фолбэк, если прямой импорт недоступен
    def get_alias_for_sku(sku: int) -> str | None:
        return None

    def _order_list() -> List[int]:
        out: List[int] = []
        seen: set[int] = set()
        raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",")
        for tok in raw.split(","):
            tok = tok.strip()
            if not tok:
                continue
            left = tok.split(":", 1)[0].strip()
            if not left.isdigit():
                continue
            v = int(left)
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    def _allowed_set() -> set[int]:
        st: set[int] = set()
        for s in (os.getenv("WATCH_SKU", "") or "").split(","):
            s = s.strip()
            if ":" in s:
                s = s.split(":", 1)[0].strip()
            if s.isdigit():
                st.add(int(s))
        return st


# -------- HTTP
OZON_API_URL = "https://api-seller.ozon.ru/v1/analytics/data"


def _headers() -> Dict[str, str]:
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


# -------- кэш (для оффлайн-фолбэка) → в data/cache/sales/
TRAFFIC_CACHE_FILE = os.path.join(CACHE_SALES, "traffic_cache.json")


def _read_cache_sync() -> dict:
    if not os.path.exists(TRAFFIC_CACHE_FILE):
        return {}
    try:
        with open(TRAFFIC_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


async def _read_cache() -> dict:
    return await asyncio.to_thread(_read_cache_sync)


def _write_cache_sync(payload: dict) -> None:
    try:
        with open(TRAFFIC_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception:
        pass


async def _write_cache(payload: dict) -> None:
    await asyncio.to_thread(_write_cache_sync, payload)


# -------- подготовка payload
# нужные метрики трафика:
# - hits_view_pdp: показы на карточке
# - hits_tocart_pdp: клики "в корзину" с карточки
# - session_view_pdp: уникальные посетители карточки
# - ordered_units: заказанные юниты (для CVR к покупке)
TRAFFIC_METRICS = ["hits_view_pdp", "hits_tocart_pdp", "session_view_pdp", "ordered_units"]


def _payload_traffic(date_from: str, date_to: str) -> Dict[str, Any]:
    """Готовим payload для sku+day. ВАЖНО: filters.value строкой."""
    p: Dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "metrics": TRAFFIC_METRICS,
        "dimension": ["sku", "day"],  # ← только sku+day
        "limit": 1000,
        "offset": 0,
    }
    if OZON_COMPANY_ID:
        p["company_id"] = OZON_COMPANY_ID

    if PRODUCTS_MODE == "SKU" and WATCH_SKU:
        only_digits: List[str] = []
        for s in WATCH_SKU:
            s = s.strip()
            if ":" in s:
                s = s.split(":", 1)[0].strip()
            if s.isdigit():
                only_digits.append(s)
        if only_digits:
            p["filters"] = [
                {
                    "key": "sku",
                    "value": ",".join(only_digits),  # ← строка "123,456"
                    "operator": "IN",
                }
            ]
    elif PRODUCTS_MODE == "OFFER" and WATCH_OFFERS:
        p["filters"] = [
            {"key": "offer_id", "value": ",".join(WATCH_OFFERS), "operator": "IN"}  # ← строка
        ]
    return p


async def _try_fetch(
    payload: dict, tag: str, session: Optional[aiohttp.ClientSession] = None
) -> dict | None:
    """С учётом дросселя и мягкой обработки 429/403/400."""
    global _LAST_TRAFFIC_CALL

    # не чаще, чем раз в TRAFFIC_MIN_INTERVAL секунд
    if time.time() - _LAST_TRAFFIC_CALL < TRAFFIC_MIN_INTERVAL:
        return None

    try:
        timeout = aiohttp.ClientTimeout(total=30)
        if session:
            # Используем переданную сессию
            async with session.post(
                OZON_API_URL, headers=_headers(), json=payload, timeout=timeout
            ) as r:
                return await _handle_response(r, tag)
        else:
            # Создаем временную сессию (fallback)
            async with aiohttp.ClientSession() as tmp_session:
                async with tmp_session.post(
                    OZON_API_URL, headers=_headers(), json=payload, timeout=timeout
                ) as r:
                    return await _handle_response(r, tag)
    except Exception as e:
        print(f"[traffic] HTTP fetch failed ({tag}): {e}")
        return None


async def _handle_response(r: aiohttp.ClientResponse, tag: str) -> dict | None:
    global _LAST_TRAFFIC_CALL
    if r.status in (429, 403, 400):
        text = await r.text()
        body = text[:240].replace("\n", " ")
        print(f"[traffic] HTTP fetch failed ({tag}): {r.status} {body}")
        return None
    r.raise_for_status()
    _LAST_TRAFFIC_CALL = time.time()
    return await r.json()


async def _fetch_traffic(date_from: str, date_to: str) -> dict | None:
    """Пробуем: 1) с фильтрами  2) без фильтров (потом вручную отфильтруем)."""
    async with aiohttp.ClientSession() as session:
        # с фильтрами
        p = _payload_traffic(date_from, date_to)
        js = await _try_fetch(p, "sku+day", session=session)
        if js and (js.get("result") or js.get("data")):
            return js

        # без фильтров
        p.pop("filters", None)
        js = await _try_fetch(p, "sku+day/nofilter", session=session)
        if js and (js.get("result") or js.get("data")):
            return js

    return None


# -------- сбор матрицы: {sku: {date: (views, clicks, sessions, units)}}


async def _collect_traffic_matrix(
    days: int,
) -> Dict[int, Dict[dt.date, Tuple[float, float, float, float]]]:
    end = dt.date.today()
    start = end - dt.timedelta(days=days - 1)
    date_from = start.strftime("%Y-%m-%d")
    date_to = end.strftime("%Y-%m-%d")

    allowed = _allowed_set()

    js = await _fetch_traffic(date_from, date_to)
    matrix: Dict[int, Dict[dt.date, Tuple[float, float, float, float]]] = {}

    def _push(sku: int, d: dt.date, v: float, c: float, s: float, u: float) -> None:
        """Добавить строку в матрицу с фильтром по allowed."""
        if allowed and sku not in allowed:
            return
        pv, pc, ps, pu = matrix.get(sku, {}).get(d, (0.0, 0.0, 0.0, 0.0))
        matrix.setdefault(sku, {})[d] = (pv + v, pc + c, ps + s, pu + u)

    if not js:
        # оффлайн из кэша (строго по allowed)
        cached = await _read_cache()
        for sku_s, rows in (cached.get("rows") or {}).items():
            try:
                sku = int(sku_s)
            except Exception:
                continue
            if allowed and sku not in allowed:
                continue
            for r in rows:
                try:
                    d = dt.datetime.strptime(r["date"], "%Y-%m-%d").date()
                except Exception:
                    continue
                if not (start <= d <= end):
                    continue
                v = float(r.get("views", 0.0))
                c = float(r.get("clicks", 0.0))
                s = float(r.get("sessions", 0.0))
                u = float(r.get("units", 0.0))
                _push(sku, d, v, c, s, u)
        return matrix

    data = js.get("result", {}).get("data", []) or js.get("data", []) or []
    to_cache: Dict[str, List[dict]] = {}

    for row in data:
        # sku
        sku_raw = (
            row.get("sku")
            or row.get("product_id")
            or (row.get("dimension") or {}).get("sku")
            or (row.get("dimensions", [{}])[0].get("id") if row.get("dimensions") else None)
        )
        try:
            sku = int(sku_raw)
        except Exception:
            continue

        # дата
        d_str = (
            row.get("date")
            or (row.get("dimension") or {}).get("date")
            or row.get("day")
            or (row.get("dimension") or {}).get("day")
        )
        if not d_str and isinstance(row.get("dimensions"), list):
            for dim in row["dimensions"]:
                di = (dim or {}).get("id")
                if isinstance(di, str):
                    try:
                        dt.date.fromisoformat(di)
                        d_str = di
                        break
                    except Exception:
                        pass
        if not d_str:
            continue

        try:
            d = dt.date.fromisoformat(d_str)
        except Exception:
            continue

        # метрики
        metrics = row.get("metrics") or row.get("value") or {}
        if isinstance(metrics, list):
            # hits_view_pdp, hits_tocart_pdp, session_view_pdp, ordered_units
            v = float(metrics[0] if len(metrics) > 0 else 0.0)
            c = float(metrics[1] if len(metrics) > 1 else 0.0)
            s = float(metrics[2] if len(metrics) > 2 else 0.0)
            u = float(metrics[3] if len(metrics) > 3 else 0.0)
        else:
            v = float(metrics.get("hits_view_pdp", 0.0))
            c = float(metrics.get("hits_tocart_pdp", 0.0))
            s = float(metrics.get("session_view_pdp", 0.0))
            u = float(metrics.get("ordered_units", 0.0))

        _push(sku, d, v, c, s, u)

        # кэшируем только наблюдаемые (уменьшаем размер кэша)
        if not allowed or sku in allowed:
            to_cache.setdefault(str(sku), []).append(
                {"date": d.strftime("%Y-%m-%d"), "views": v, "clicks": c, "sessions": s, "units": u}
            )

    await _write_cache({"rows": to_cache})
    return matrix


# -------- агрегирование по периоду


def _aggregate_for_period(
    matrix: Dict[int, Dict[dt.date, Tuple[float, float, float, float]]], period_days: int
) -> Dict[int, Tuple[float, float, float, float]]:
    days = int(period_days)
    days = 1 if days == 0 else days
    today = dt.date.today()
    last_date = max((d for m in matrix.values() for d in m.keys()), default=today)
    if period_days == 0:
        start = last_date
        end = last_date
    elif period_days == 1:
        end = last_date - dt.timedelta(days=1)
        start = end
    else:
        end = last_date
        start = end - dt.timedelta(days=days - 1)

    result: Dict[int, Tuple[float, float, float, float]] = {}
    allowed = _allowed_set()
    for sku, dmap in matrix.items():
        if allowed and sku not in allowed:
            continue
        v = c = s = u = 0.0
        for d, (vv, cc, ss, uu) in dmap.items():
            if start <= d <= end:
                v += vv
                c += cc
                s += ss
                u += uu
        if v > 0 or c > 0 or s > 0 or u > 0:
            result[sku] = (v, c, s, u)
    return result


# -------- публичный текст


def _period_label(days: int) -> str:
    if days == 0:
        return "Сегодня"
    if days == 1:
        return "Вчера"
    return f"Последние {days} дней"


async def traffic_text(period_days: int, metric: str = "ctr") -> str:
    """metric: 'ctr' | 'cvr'"""
    metric = (metric or "ctr").lower()
    head_metric = {"ctr": "КЛИКАБЕЛЬНОСТЬ", "cvr": "КОНВЕРСИЯ"}.get(metric, "КЛИКАБЕЛЬНОСТЬ")

    # буфер 30 дней, чтобы любые периоды считались из одной матрицы
    matrix = await _collect_traffic_matrix(max(int(period_days) if period_days else 1, 30))
    agg = _aggregate_for_period(matrix, int(period_days))

    lines: List[str] = []
    order = _order_list()  # ← порядок из WATCH_SKU

    tot_ctr_num = tot_ctr_den = 0.0
    tot_cvr_num = tot_cvr_den = 0.0

    for sku in order:
        if sku not in agg:
            continue
        alias = get_alias_for_sku(sku)
        if not alias:
            continue
        views, clicks, sessions, units = agg[sku]

        # CTR = добавления в корзину / показы PDP
        ctr = (clicks / views * 100.0) if views > 0 else 0.0
        # CVR = заказанные юниты / уникальные посетители PDP
        cvr = (units / sessions * 100.0) if sessions > 0 else 0.0

        if metric == "ctr":
            lines.append(f"🔹 {alias} — {_fmt_pct(ctr)}")
            tot_ctr_num += clicks
            tot_ctr_den += views
        else:
            lines.append(f"🔹 {alias} — {_fmt_pct(cvr)}")
            tot_cvr_num += units
            tot_cvr_den += sessions

    if not lines:
        lines = ["— данных нет —"]

    if metric == "ctr":
        total = (tot_ctr_num / tot_ctr_den * 100.0) if tot_ctr_den > 0 else 0.0
        total_line = f"📊 Средний CTR: {_fmt_pct(total)}"
    else:
        total = (tot_cvr_num / tot_cvr_den * 100.0) if tot_cvr_den > 0 else 0.0
        total_line = f"📊 Средняя конверсия: {_fmt_pct(total)}"

    header = (
        f"📄 Трафик — Факт ({head_metric})\n"
        f"📅 {_now_stamp()} • {_period_label(int(period_days))}\n"
    )

    return "\n".join([header, *lines, "", total_line])
