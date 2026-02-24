# modules_sales/sales_facts_store.py
from __future__ import annotations
from modules_common.paths import ensure_dirs, DATA_DIR, CACHE_SALES
from modules_common.cache_manager import SalesCache
from config_package.constants import TrafficMetric
from config_package import settings
import logging
import os
import json
import time
import datetime as dt
import asyncio
from typing import Dict, List, Tuple, Any, Optional

import aiohttp
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.sales_facts_store")

# Единые пути
ensure_dirs()

# Используем типизированную конфигурацию
OZON_CLIENT_ID: str = settings.ozon_client_id
OZON_API_KEY: str = settings.ozon_api_key
OZON_COMPANY_ID: str = settings.ozon_company_id
PRODUCTS_MODE: str = settings.products_mode

# Дроссель API: минимальный интервал между удачными попытками (секунды)
MIN_INTERVAL: float = 65.0
_LAST_API_CALL: float = 0.0  # timestamp последнего удачного запроса

OZON_API_URL: str = settings.ozon_api_url

# ---------- алиасы из .env ----------
_ALIAS_CACHE: Dict[int, str] = {}


def _watch_skus_order_list() -> List[int]:
    """
    Читает WATCH_SKU, поддерживает 'sku' и 'sku:alias'.
    Сохраняет исходный порядок, убирает дубли.

    Returns:
        Список SKU в порядке WATCH_SKU
    """
    # Используем settings.parsed_watch_sku.
    # Эта проперти возвращает уже int список уникальных SKU в порядке указания.
    return settings.parsed_watch_sku


def _allowed_sku_set() -> set[int]:
    """
    Возвращает множество допустимых SKU.

    Returns:
        Множество SKU из WATCH_SKU
    """
    return set(settings.parsed_watch_sku)


def _parse_alias_pairs(raw: str) -> Dict[int, str]:
    """
    Парсит строку вида:
        "ALIAS_1831342831=stand_ABS_black,1831342958=stand_ABS_white,..."
    Поддерживает ключи как с префиксом ALIAS_, так и без него.

    Args:
        raw: Строка с парами алиасов

    Returns:
        Словарь {sku: alias}
    """
    res: Dict[int, str] = {}
    if not raw:
        return res
    text = raw.replace("\n", ",")
    for token in text.split(","):
        token = token.strip()
        if not token or "=" not in token:
            continue
        k, v = token.split("=", 1)
        key = (k or "").strip()
        val = (v or "").strip()
        if not key or not val:
            continue
        if key.upper().startswith("ALIAS_"):
            key = key[6:]
        key = key.strip()
        if not key.isdigit():
            continue
        try:
            sku = int(key)
            res[sku] = val
        except Exception:
            continue
    return res
    text = raw.replace("\n", ",")
    for token in text.split(","):
        token = token.strip()
        if not token or "=" not in token:
            continue
        k, v = token.split("=", 1)
        key = (k or "").strip()
        val = (v or "").strip()
        if not key or not val:
            continue
        if key.upper().startswith("ALIAS_"):
            key = key[6:]
        key = key.strip()
        if not key.isdigit():
            continue
        try:
            sku = int(key)
            res[sku] = val
        except Exception:
            continue
    return res


def _apply_aliases_from_watch_sku() -> Dict[int, str]:
    """
    Дополнительно подтягивает алиасы из формата WATCH_SKU="sku:alias,...",
    если такие встретятся; не перетирает явные ALIAS_*.

    Returns:
        Словарь {sku: alias}
    """
    res: Dict[int, str] = {}
    raw = (settings.watch_sku or "").replace("\n", ",").replace(" ", ",")
    for token in raw.split(","):
        token = token.strip()
        if ":" not in token:
            continue
        left, alias = token.split(":", 1)
        left = left.strip()
        alias = alias.strip()
        if not left.isdigit() or not alias:
            continue
        try:
            res[int(left)] = alias
        except Exception:
            continue
    return res


def _build_alias_cache() -> None:
    """
    Источники алиасов (по приоритету — позже перекрывает ранее):
        1) ALIAS=<pairs>  (единая переменная через запятую)
        2) ALIAS_<SKU>=<alias> (построчно)
        3) WATCH_SKU="sku:alias" (дополняем, если алиаса ещё нет)
    """
    _ALIAS_CACHE.clear()

    # 1) ALIAS="ALIAS_183=xxx,1831342958=yyy,..."
    _ALIAS_CACHE.update(_parse_alias_pairs(os.getenv("ALIAS", "") or ""))

    # 2) Классический формат построчно
    for k, v in os.environ.items():
        if not k.startswith("ALIAS_"):
            continue
        try:
            sku = int(k.split("_", 1)[1])
            _ALIAS_CACHE[sku] = (v or "").strip()
        except Exception:
            continue

    # 3) Алиасы прямо в WATCH_SKU (sku:alias) — добавляем, если нет
    for sku, alias in _apply_aliases_from_watch_sku().items():
        _ALIAS_CACHE.setdefault(sku, alias)

    print(f"[sales_facts_store] alias cache built for {len(_ALIAS_CACHE)} sku")


def get_alias_for_sku(sku: int) -> str | None:
    if not _ALIAS_CACHE:
        _build_alias_cache()
    return _ALIAS_CACHE.get(int(sku))

# ---------- утилиты ----------


def _headers() -> Dict[str, str]:
    """
    Формирует заголовки для API запросов.

    Returns:
        Словарь заголовков HTTP
    """
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


def _now_stamp() -> str:
    """
    Формирует текущую дату/время в локальном формате.

    Returns:
        Строка в формате "ДД.ММ.ГГГГ ЧЧ:ММ"
    """
    # Note: using Latin M for minutes
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")


def _fmt_money(value: float) -> str:
    """
    Формирует денежную сумму с разделителями.

    Args:
        value: Значение в рублях

    Returns:
        Строка в формате "1 234 ₽"
    """
    return f"{int(round(value)):,}".replace(",", " ") + " ₽"


def _fmt_units(value: float) -> str:
    """
    Формирует количество штук.

    Args:
        value: Значение в штуках

    Returns:
        Строка в формате "123 шт"
    """
    return f"{int(round(value))} шт"


# ---------- кэш (перенос в data/cache/sales) ----------

def _read_cache() -> dict:
    """
    Безопасно читает кэш с диска через CacheManager.
    """
    return SalesCache.get_facts_cache_manager().get_data()


def _write_cache(payload: dict) -> None:
    """
    Безопасно записывает кэш на диск через CacheManager.
    """
    SalesCache.get_facts_cache_manager().set_data(payload)

# ---------- запрос фактов ----------


def _payload_base(date_from: str, date_to: str) -> Dict[str, Any]:
    """
    Базовый payload для /v1/analytics/data.
    ВНИМАНИЕ: limit <= 1000 (ограничение API).
    Всегда просим разрез по дням, чтобы корректно фильтровать периоды.

    Args:
        date_from: Дата начала в формате "YYYY-MM-DD"
        date_to: Дата окончания в формате "YYYY-MM-DD"

    Returns:
        Словарь payload для API запроса
    """
    payload: Dict[str, Any] = {
        "date_from": date_from,
        "date_to": date_to,
        "dimension": ["sku", "day"],       # всегда sku+day
        "limit": 1000,
        "offset": 0,
    }
    if OZON_COMPANY_ID:
        payload["company_id"] = OZON_COMPANY_ID

    # 🔒 Жёстко ограничиваем выборку наблюдаемыми SKU/offer
    if PRODUCTS_MODE == "SKU":
        # settings.parsed_watch_sku - это List[int]
        only_digits = [str(s) for s in settings.parsed_watch_sku]
        if only_digits:
            payload["filters"] = [{
                "key": "sku",
                "value": ",".join(only_digits),
                "operator": "IN"
            }]
    elif PRODUCTS_MODE == "OFFER":
        offers = settings.parsed_watch_offers
        if offers:
            payload["filters"] = [{
                "key": "offer_id",
                "value": ",".join(offers),
                "operator": "IN"
            }]

    return payload


async def _try_fetch(payload: dict) -> dict | None:
    """
    Дросселируем обращения и мягко обходим 429/лимиты.
    Если слишком часто — вернём None (пусть сработает кэш).

    Args:
        payload: Payload для API запроса

    Returns:
        JSON-ответ от API или None при ошибке/лимите
    """
    global _LAST_API_CALL

    since = time.time() - _LAST_API_CALL
    if since < MIN_INTERVAL:
        return None

    try:
        timeout = aiohttp.ClientTimeout(connect=5, total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(OZON_API_URL, headers=_headers(), json=payload) as r:
                if r.status in (429, 403):
                    try:
                        text = await r.text()
                        log.warning(
                            f"API rate limited/forbidden (status {r.status}): {text[:180]}"
                        )
                    except Exception:
                        log.warning(f"API rate limited/forbidden (status {r.status})")
                    return None

                r.raise_for_status()
                _LAST_API_CALL = time.time()
                result = await r.json()
                log.info("Successfully fetched sales facts from API")
                return result

    except asyncio.TimeoutError as e:
        log.warning(f"API timeout when fetching sales facts: {e}")
        return None
    except aiohttp.ClientError as e:
        log.warning(f"API connection error when fetching sales facts: {e}")
        return None
    except Exception as e:
        log.critical(f"Unexpected error in _try_fetch: {e}", exc_info=True)
        return None


async def _fetch_matrix(date_from: str,                            date_to: str) -> Dict[int, Dict[dt.date, Tuple[float, float]]]:
    """
    Тянем ОДНИМ запросом metrics=["ordered_units","revenue"] и собираем матрицу sku+day.
    Так мы не упираемся в дроссель вторым вызовом и всегда получаем выручку.
    
    Args:
        date_from: Дата начала в формате "YYYY-MM-DD"
        date_to: Дата окончания в формате "YYYY-MM-DD"
    
    Returns:
        Словарь {sku: {date: (units, revenue)}}
    """
    payload = _payload_base(date_from, date_to)
    payload["metrics"] = ["ordered_units", "revenue"]

    js = await _try_fetch(payload)
    if not js:
        print("[sales_facts_store] no valid response after attempts")
        return {}

    data = js.get("result", {}).get("data", []) or js.get("data", []) or []
    matrix: Dict[int, Dict[dt.date, Tuple[float, float]]] = {}

    def _extract_one(row: dict) -> tuple[int | None, dt.date | None, float, float]:
        sku_raw = row.get("sku") or row.get("product_id") or (row.get("dimension") or {}).get("sku")
        day_raw = row.get("date") or (row.get("dimension") or {}).get("date") \
            or row.get("day") or (row.get("dimension") or {}).get("day")

        if sku_raw is None or day_raw is None:
            dims = row.get("dimensions")
            if isinstance(dims, list) and len(dims) >= 2:
                sku_raw = sku_raw or (dims[0].get("id") if isinstance(dims[0], dict) else None)
                day_raw = day_raw or (dims[1].get("id") if isinstance(dims[1], dict) else None)

        try:
            sku = int(str(sku_raw))
        except Exception:
            sku = None
        try:
            day = dt.datetime.strptime(str(day_raw), "%Y-%m-%d").date()
        except Exception:
            day = None

        u = r = 0.0
        m = row.get("metrics")
        if isinstance(m, list):
            u = float(m[0]) if len(m) > 0 and m[0] is not None else 0.0
            r = float(m[1]) if len(m) > 1 and m[1] is not None else 0.0
        elif isinstance(m, dict):
            u = float(m.get("ordered_units", 0) or 0)
            r = float(m.get("revenue", 0) or 0)
        else:
            v = row.get("value", {})
            if isinstance(v, dict):
                u = float(v.get("ordered_units", 0) or 0)
                r = float(v.get("revenue", 0) or 0)

        return sku, day, u, r

    for row in data:
        sku, day, u, r = _extract_one(row)
        if sku is None or day is None:
            continue
        pu, pr = matrix.get(sku, {}).get(day, (0.0, 0.0))
        matrix.setdefault(sku, {})[day] = (pu + u, pr + r)

    to_cache: Dict[str, List[dict]] = {}
    for sku, dmap in matrix.items():
        for day, (u, r) in dmap.items():
            to_cache.setdefault(str(sku), []).append({
                "date": day.strftime("%Y-%m-%d"),
                "units": u,
                "revenue": r,
            })
    _write_cache({"rows": to_cache})

    return matrix

# ---------- агрегаты факта ----------
def _period_label_fact(days: int) -> str:
    """
    Формирует подпись для периода в отчётах по фактам.
    
    Args:
        days: Количество дней (0 - сегодня, 1 - вчера)
    
    Returns:
        Строка с эмодзи календаря и периодом
    """
    if days == 0:
        return "📅 Сегодня:"
    if days == 1:
        return "📅 Вчера:"
    return f"📅 Последние {days} дней:"

def _matrix_from_cache(start: dt.date,                           end: dt.date) -> Dict[int, Dict[dt.date, Tuple[float, float]]]:
    """
    Чтение кэша С УЧЁТОМ фильтра по WATCH_SKU, чтобы «левые» SKU не попадали в отчёты.
    
    Args:
        start: Дата начала фильтра
        end: Дата окончания фильтра
    
    Returns:
        Словарь {sku: {date: (units, revenue)}}
    """
    allowed = _allowed_sku_set()
    cached = _read_cache()
    matrix: Dict[int, Dict[dt.date, Tuple[float, float]]] = {}
    for sku_s, rows in (cached.get("rows") or {}).items():
        try:
            sku = int(sku_s)
        except Exception:
            continue
        if allowed and sku not in allowed:
            continue
        for row in rows:
            try:
                d = dt.datetime.strptime(row["date"], "%Y-%m-%d").date()
            except Exception:
                continue
            if not (start <= d <= end):
                continue
            u = float(row.get("units", 0) or 0)
            r = float(row.get("revenue", 0) or 0)
            matrix.setdefault(sku, {})[d] = (u, r)
    return matrix

async def _collect_matrix(days: int) -> Dict[int, Dict[dt.date, Tuple[float, float]]]:
    """
    Собирает матрицу продаж за указанное количество дней.
    
    Args:
        days: Количество дней (минимум 1)
    
    Returns:
        Словарь {sku: {date: (units, revenue)}}
    """
    end = dt.date.today()
    start = end - dt.timedelta(days=days - 1)
    mx = await _fetch_matrix(start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")) or {}
    if mx:
        return mx
    return _matrix_from_cache(start, end)

async def get_facts_aggregated(period_days: int,                                   force_update: bool = False) -> Dict[int, Tuple[float, float]]:
    """
    Возвращает {sku: (units_sum, revenue_sum)} за выбранный период.
    Корректно обрабатывает «вчера»: берём именно вчерашнюю дату в локальной TZ.
    Гарантируем, что в выдаче только SKU из WATCH_SKU.
    
    Args:
        period_days: Количество дней (0 - сегодня, 1 - вчера, >1 - период)
        force_update: Принудительное обновление без кэша
    
    Returns:
        Словарь {sku: (units_sum, revenue_sum)}
    """
    days = max(1, int(period_days))
    matrix = await _collect_matrix(max(days, 30))

    if not matrix:
        return {}

    all_days = [d for m in matrix.values() for d in m.keys()]
    if not all_days:
        return {}
    last_available = max(all_days)

    if period_days == 0:   # сегодня (если в матрице уже есть сегодня)
        start = last_available
        end = last_available
    elif period_days == 1: # вчера
        yday = dt.date.today() - dt.timedelta(days=1)
        if yday in set(all_days):
            start = end = yday
        else:
            # фолбэк: последняя доступная дата ≤ вчера (если есть)
            candidates = [d for d in all_days if d <= yday]
            if candidates:
                end = start = max(candidates)
            else:
                end = start = last_available
    else:
        end = last_available
        start = end - dt.timedelta(days=days - 1)

    allowed = _allowed_sku_set()
    result: Dict[int, Tuple[float, float]] = {}
    for sku, dmap in matrix.items():
        if allowed and sku not in allowed:
            continue
        u_sum = r_sum = 0.0
        for d, (u, r) in dmap.items():
            if start <= d <= end:
                u_sum += u
                r_sum += r
        if u_sum > 0 or r_sum > 0:
            result[sku] = (u_sum, r_sum)
    return result

def _format_list(agg: Dict[int,                     Tuple[float,                     float]],                     metric: str) -> Tuple[List[str], float, float, float, int]:
    """
    Формирует строки списка в порядке WATCH_SKU. Не выводит SKU без алиаса.
    Итоги считаем по тем же наблюдаемым SKU, чтобы суммы совпадали с видимым списком.
    
    Args:
        agg: Агрегированные данные {sku: (units, revenue)}
        metric: Метрика для форматирования ("units" | "revenue" | "avgprice")
    
    Returns:
        Tuple (lines, total_units, total_revenue, total_avgprice, count_avgprice)
    """
    lines: List[str] = []
    order = _watch_skus_order_list()
    # Итоги только по наблюдаемым в нужном порядке
    tot_u = sum((agg.get(s, (0.0, 0.0))[0] for s in order))
    tot_r = sum((agg.get(s, (0.0, 0.0))[1] for s in order))

    sum_ap = 0.0
    cnt_ap = 0

    for sku in order:
        if sku not in agg:
            continue
        alias = get_alias_for_sku(sku)
        if not alias:
            continue
        u, r = agg[sku]
        if metric == "units":
            lines.append(f"🔹 {alias}: {_fmt_units(u)}")
        elif metric == "revenue":
            lines.append(f"🔹 {alias}: {_fmt_money(r)}")
        elif metric == "avgprice":
            ap = (r / u) if u > 0 else 0.0
            lines.append(f"🔹 {alias}: {_fmt_money(ap)}")
            sum_ap += ap
            cnt_ap += 1

    return lines, tot_u, tot_r, sum_ap, cnt_ap

def _normalize_metric(metric: Optional[str]) -> str:
    """
    Нормализует название метрики.
    
    Args:
        metric: Метрика ("units" | "revenue" | "avgprice" | ...)
    
    Returns:
        Нормализованный идентификатор ("units" | "revenue" | "avgprice")
    """
    m = (metric or "units").strip().lower()
    if m in {"avg_price", "avgprice", "avg_check", "avgcheck", "avg",
             "avg_receipt", "average_check", "avg_ticket"}:
        return "avgprice"
    if m in {"revenue", "rev", "money", "gmv"}:
        return "revenue"
    return "units"

async def facts_text(period_days: int, metric: str = "units", force_update: bool = False) -> str:
    """
    Генерирует текстовый отчёт по фактическим продажам.
    
    Args:
        period_days: Количество дней (0 - сегодня, 1 - вчера, >1 - период)
        metric: Метрика ("units" | "revenue" | "avgprice")
        force_update: Принудительное обновление без кэша
    
    Returns:
        Строка с форматированным отчётом
    """
    metric_norm = _normalize_metric(metric)
    head_metric = {"units": "ЮНИТЫ", "revenue": "ВЫРУЧКА", "avgprice": "СРЕДНИЙ ЧЕК"}[metric_norm]

    head = f"📄 Факт продаж — {head_metric}\n⏱ Обновлено: {_now_stamp()}\n"
    label = _period_label_fact(int(period_days))

    agg = await get_facts_aggregated(period_days=int(period_days), force_update=force_update)
    lines, tot_u, tot_r, sum_ap, cnt_ap = _format_list(agg, metric_norm)

    if not lines:
        lines = ["—"]

    if metric_norm == "units":
        total_line = f"📊 ИТОГО — {_fmt_units(tot_u)}"
    elif metric_norm == "revenue":
        total_line = f"📊 ИТОГО — {_fmt_money(tot_r)}"
    else:
        avg_all = (sum_ap / cnt_ap) if cnt_ap > 0 else 0.0
        total_line = f"📊 СРЕДНЕЕ — {_fmt_money(avg_all)}"

    return "\n".join([head, label, ""] + lines + ["", total_line])
