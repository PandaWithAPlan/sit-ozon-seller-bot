# modules_shipments/shipments_leadtime_stats_data.py
from __future__ import annotations

import logging
import os
import json
import time
import random
import datetime as dt
import asyncio
from typing import Dict, List, Tuple, Any, Optional
import hashlib

import aiohttp
from dotenv import load_dotenv
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.leadtime_stats_data")

# ── paths / env ──────────────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
CACHE_SHIP_DIR = os.path.join(CACHE_DIR, "shipments")
os.makedirs(CACHE_SHIP_DIR, exist_ok=True)

load_dotenv(os.path.join(ROOT_DIR, ".env"))

# ── settings ─────────────────────────────────────────────────────────────────
LEAD_STAT_DAYS_DEFAULT = int(os.getenv("LEAD_STAT_DAYS", "180"))
LEAD_STAT_TTL_HOURS = int(os.getenv("LEAD_STAT_TTL_HOURS", "12"))

LEAD_STATS_PREFS_PATH = os.path.join(CACHE_DIR, "common", "lead_stats_prefs.json")
os.makedirs(os.path.dirname(LEAD_STATS_PREFS_PATH), exist_ok=True)

EVENTS_CACHE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_events.json")
STATS_CACHE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_stats.json")

# кэш истории статусов (фаза B)
STATES_CACHE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_states.json")

LEAD_DISABLE_INGEST_ON_READ = bool(int(os.getenv("LEAD_DISABLE_INGEST_ON_READ", "1")))

# API creds/limits
OZON_CLIENT_ID = os.getenv("OZON_CLIENT_ID", "")
OZON_API_KEY = os.getenv("OZON_API_KEY", "")

LEAD_FETCH_BATCH = int(os.getenv("LEAD_FETCH_BATCH", "100"))
LEAD_GET_BATCH = int(os.getenv("LEAD_GET_BATCH", "50"))
LEAD_BUNDLE_MAX_PER_RUN = int(os.getenv("LEAD_BUNDLE_MAX_PER_RUN", "15"))
LEAD_BUNDLE_BASE_PAUSE_SEC = float(os.getenv("LEAD_BUNDLE_BASE_PAUSE_SEC", "0.4"))
LEAD_BUNDLE_MAX_TOTAL_TRIES = int(os.getenv("LEAD_BUNDLE_MAX_TOTAL_TRIES", "60"))
LEAD_RETENTION_DAYS = int(os.getenv("LEAD_RETENTION_DAYS", "360"))
LEAD_MAX_PAGES = int(os.getenv("LEAD_MAX_PAGES", "50"))

# HTTP throttling
LEAD_HTTP_TIMEOUT = int(os.getenv("LEAD_HTTP_TIMEOUT", "20"))
LEAD_RETRY_AFTER_CAP = float(os.getenv("LEAD_RETRY_AFTER_CAP", "2.5"))

# минимальная длительность события (шумовой порог)
LEAD_MIN_DAYS = float(os.getenv("LEAD_MIN_DAYS", "0.0"))

# ingest tick / state
LEAD_INGEST_INTERVAL_SEC = int(os.getenv("LEAD_INGEST_INTERVAL_SEC", "900"))
LEAD_INGEST_PAGES_DEFAULT = int(os.getenv("LEAD_INGEST_PAGES", "3"))
LEAD_INGEST_STATE_PATH = os.path.join(CACHE_SHIP_DIR, "leadtime_ingest_state.json")

# первичный прогон: рекомендуемая глубина
LEAD_PRIMARY_PAGES = int(os.getenv("LEAD_PRIMARY_PAGES", "5"))

# опциональный форс-запуск тика (игнор антидребезга)
LEAD_TICK_FORCE = bool(int(os.getenv("LEAD_TICK_FORCE", "0")))


LEAD_STAT_PERIODS = (90, 180, 360)

# ── WATCH_SKU: фильтр и порядок для «по SKU» ─────────────────────────────────
RAW_WATCH_SCU = os.getenv("WATCH_SKU", "") or ""


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


WATCH_ORDER: List[int] = _parse_watch_sku(RAW_WATCH_SCU)
WATCH_POS = {sku: i for i, sku in enumerate(WATCH_ORDER)}
WATCH_SET = set(WATCH_ORDER)


def get_current_watch_sku() -> List[int]:
    """Публичная утилита для отладки: вернуть эффективный список WATCH_SKU."""
    return list(WATCH_ORDER)


def _order_key_for_sku(sku: int, alias: str = "") -> Tuple[int, str]:
    """Ключ сортировки: позиция в WATCH_SKU, потом alias (для стабильности)."""
    return (WATCH_POS.get(int(sku), 10**9), (alias or "").lower())


# ── tiny json utils ──────────────────────────────────────────────────────────


def _read_json(path: str) -> dict:
    """Читает JSON файл с логированием."""
    return safe_read_json(path)


def _write_json(path: str, payload: dict) -> None:
    """Записывает JSON файл с логированием."""
    safe_write_json(path, payload)


# ── time helpers ────────────────────────────────────────────────────────────


def _parse_iso_dt(s: str) -> Optional[dt.datetime]:
    if not s:
        return None
    try:
        d = dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _iso_ge(a: str, b: str) -> bool:
    """
    True, если ISO‑строка a >= b (корректно обрабатывает 'Z'/таймзоны).
    Если b пустая — считаем условие выполненным.
    """
    if not b:
        return True
    da = _parse_iso_dt(a)
    db = _parse_iso_dt(b)
    if da and db:
        return da >= db
    return str(a) >= str(b)


def _events_saved_at() -> str:
    try:
        d = _read_json(EVENTS_CACHE_PATH)
        return str(d.get("saved_at") or "")
    except Exception:
        return ""


def _is_events_empty() -> bool:
    d = _read_json(EVENTS_CACHE_PATH)
    return not bool(d.get("rows"))


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


# ── prefs (period + allocation flag) ─────────────────────────────────────────


def _load_prefs() -> dict:
    d = _read_json(LEAD_STATS_PREFS_PATH)
    period = int(d.get("period", LEAD_STAT_DAYS_DEFAULT))
    if period not in LEAD_STAT_PERIODS:
        period = LEAD_STAT_DAYS_DEFAULT
    alloc = bool(d.get("allocate_by_qty", True))
    return {"period": period, "allocate_by_qty": alloc}


def get_stat_period() -> int:
    return int(_load_prefs().get("period", LEAD_STAT_DAYS_DEFAULT))


def save_stat_period(period: int) -> None:
    if period not in LEAD_STAT_PERIODS:
        return
    cur = _load_prefs()
    cur["period"] = int(period)
    _write_json(LEAD_STATS_PREFS_PATH, cur)


def get_lead_allocation_flag() -> bool:
    return bool(_load_prefs().get("allocate_by_qty", True))


# ── переключение аллокации ───────────────────────────────────────────────────


def set_lead_allocation_flag(flag: bool) -> None:
    """
    Обновляет флаг «учитывать вес партии», после чего:
        • полностью пересобирает события из states с новым правилом,
        • инвалидирует кэш статистики.
    """
    cur = _load_prefs()
    cur["allocate_by_qty"] = bool(flag)
    _write_json(LEAD_STATS_PREFS_PATH, cur)
    try:
        _write_json(EVENTS_CACHE_PATH, {"saved_at": _utc_now_iso(), "rows": [], "version": 2})
        _emit_phase_b_events_from_states(_utc_now_iso())
        _write_json(STATS_CACHE_PATH, {})
    except Exception:
        # хотя бы сбросим кэш статистики
        try:
            _write_json(STATS_CACHE_PATH, {})
        except Exception:
            pass


# ── freshness ────────────────────────────────────────────────────────────────


def _is_fresh(saved_iso: str, ttl_hours: int) -> bool:
    ts = _parse_iso_dt(saved_iso)
    if not ts:
        return False
    now = dt.datetime.now(dt.timezone.utc)
    return (now - ts) <= dt.timedelta(hours=max(1, int(ttl_hours)))


# ── materialize & filter ─────────────────────────────────────────────────────


def _materialize_events(period_days: int) -> List[dict]:
    d = _read_json(EVENTS_CACHE_PATH)
    rows = d.get("rows", []) if isinstance(d, dict) else []
    if not rows:
        return []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(1, int(period_days)))
    out: List[dict] = []
    for e in rows:
        try:
            if str(e.get("phase") or "") != "post_dropoff":
                continue
            tend = _parse_iso_dt(str(e.get("ts_end", "")))
            if tend and tend >= cutoff:
                out.append(e)
        except Exception:
            continue
    return out


def _only_completed_with_duration(events: List[dict]) -> List[dict]:
    out: List[dict] = []
    for e in events:
        try:
            dur_val = e.get("duration_days")
            if isinstance(dur_val, (int, float)) and float(dur_val) > 0:
                dur = float(dur_val)
            else:
                a = _parse_iso_dt(str(e.get("ts_start", "")))
                b = _parse_iso_dt(str(e.get("ts_end", "")))
                if not a or not b or b <= a:
                    continue
                dur = (b - a).total_seconds() / 86400.0

            if LEAD_MIN_DAYS <= dur <= 90:
                ee = dict(e)
                ee["duration_days"] = float(dur)
                out.append(ee)
        except Exception:
            continue
    return out


# ── stats helpers ────────────────────────────────────────────────────────────


def _percentile(vals: List[float], p: float) -> float:
    if not vals:
        return 0.0
    arr = sorted(vals)
    n = len(arr)
    if n == 1:
        return float(arr[0])
    if p <= 0:
        return float(arr[0])
    if p >= 1:
        return float(arr[-1])
    k = (n - 1) * float(p)
    f = int(k)
    c = f + 1
    if c >= n:
        return float(arr[-1])
    if f == c:
        return float(arr[f])
    d = k - f
    return float(arr[f] * (1.0 - d) + arr[c] * d)


def _aggregate_stats(events: List[dict], key_fn) -> List[Tuple[Any, Dict[str, float]]]:
    buckets: Dict[Any, List[float]] = {}
    for e in events:
        k = key_fn(e)
        if k is None:
            continue
        buckets.setdefault(k, []).append(float(e.get("duration_days", 0.0)))
    out: List[Tuple[Any, Dict[str, float]]] = []
    for k, arr in buckets.items():
        arr = [float(x) for x in arr if x is not None]
        if not arr:
            continue
        arr.sort()
        n = len(arr)
        stats = {
            "avg": sum(arr) / n,
            "p50": _percentile(arr, 0.5),
            "p90": _percentile(arr, 0.9),
            "min": arr[0],
            "max": arr[-1],
            "n": float(n),
        }
        out.append((k, stats))
    return out


# ── HTTP helpers ─────────────────────────────────────────────────────────────


def _ozon_headers() -> Dict[str, str]:
    return {
        "Client-Id": OZON_CLIENT_ID,
        "Api-Key": OZON_API_KEY,
        "Content-Type": "application/json",
    }


async def _post_json(url: str, payload: dict, timeout: Optional[int] = None):
    timeout_obj = aiohttp.ClientTimeout(connect=5, total=(timeout or LEAD_HTTP_TIMEOUT))
    async with aiohttp.ClientSession(timeout=timeout_obj) as session:
        try:
            async with session.post(url, headers=_ozon_headers(), json=payload) as r:
                try:
                    js = await r.json()
                except Exception:
                    js = {}
                if r.status == 429:
                    return js, r
                r.raise_for_status()
                return js, r
        except Exception:
            return {}, None


async def _respect_rate_limit_sleep(resp):
    try:
        if hasattr(resp, "status") and int(resp.status) == 429:
            ra = resp.headers.get("Retry-After")
            delay = float(ra) if ra is not None else 1.0
            delay = max(0.0, min(delay, LEAD_RETRY_AFTER_CAP))
            await asyncio.sleep(delay + random.uniform(0, 0.2))
    except Exception:
        await asyncio.sleep(0.5 + random.uniform(0, 0.2))


# ── v2→v3: нормализация статусов ─────────────────────────────────────────────
# Канонический набор (v3)
START_STATE = "ACCEPTED_AT_SUPPLY_WAREHOUSE"
END_STATE = "REPORTS_CONFIRMATION_AWAITING"
COMP_STATE = "COMPLETED"
# 🆕 Предпочтительная «конечная» стадия для расчёта lead time
STORAGE_ACCEPT_STATE = "ACCEPTANCE_AT_STORAGE_WAREHOUSE"

# Алиасы v2 → v3
_STATE_ALIASES = {
    "ORDER_STATE_ACCEPTED_AT_SUPPLY_WAREHOUSE": START_STATE,
    "ORDER_STATE_IN_TRANSIT": "IN_TRANSIT",
    "ORDER_STATE_ACCEPTANCE_AT_STORAGE_WAREHOUSE": STORAGE_ACCEPT_STATE,
    "ORDER_STATE_REPORTS_CONFIRMATION_AWAITING": END_STATE,
    "ORDER_STATE_COMPLETED": COMP_STATE,
    "ORDER_STATE_DATA_FILLING": "DATA_FILLING",
    "ORDER_STATE_READY_TO_SUPPLY": "READY_TO_SUPPLY",
    "ORDER_STATE_REJECTED_AT_SUPPLY_WAREHOUSE": "REJECTED_AT_SUPPLY_WAREHOUSE",
    "ORDER_STATE_CANCELLED": "CANCELLED",
    "ORDER_STATE_OVERDUE": "OVERDUE",
}


def _normalize_state(name: str) -> str:
    s = str(name or "").strip()
    up = s.upper()
    return _STATE_ALIASES.get(up, up)


def _js_pick(d: dict, *keys) -> Any:
    """
    Безопасное извлечение значения по одному из имён ключей.
    Пытается также заглянуть в известные обёртки ('result', 'data').
    """
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d:
            return d.get(k)
    for wrap in ("result", "data"):
        sub = d.get(wrap)
        if isinstance(sub, dict):
            for k in keys:
                if k in sub:
                    return sub.get(k)
    return None


# ── supply-order API ─────────────────────────────────────────────────────────


async def _supply_list(
    states: List[str], from_id: int = 0, limit: int = 100
) -> Tuple[List[str], int]:
    """
    v3: POST /v3/supply-order/list
      payload: {"filter":{"states":[...]}, "paging":{"from_supply_order_id":<int>|"from_order_id":<int>, "limit":<=100}}
      response (варианты): {"order_ids":[...], "last_supply_order_id":<int>} — или — {"supply_order_id":[...], ...}
    Встроен фолбэк: если сервер игнорирует ключ 'from_supply_order_id' и пагинация «стоит на месте»,
    автоматически пробуем 'from_order_id'.
    """
    # нормализуем названия статусов к v3
    states_v3 = []
    seen = set()
    for s in states or []:
        norm = _normalize_state(s)
        if norm and norm not in seen:
            seen.add(norm)
            states_v3.append(norm)

    async def _call_with(paging_key: str) -> dict:
        payload = {
            "filter": {"states": states_v3},
            "paging": {paging_key: int(from_id), "limit": min(int(limit), 100)},
        }
        js, resp = await _post_json("https://api-seller.ozon.ru/v3/supply-order/list", payload)
        if resp is not None and getattr(resp, "status", 200) == 429:
            await _respect_rate_limit_sleep(resp)
            js, _ = await _post_json("https://api-seller.ozon.ru/v3/supply-order/list", payload)
        return js or {}

    def _parse(js: dict) -> Tuple[List[str], int]:
        ids = _js_pick(js, "order_ids", "supply_order_id", "ids")
        if not isinstance(ids, list):
            ids = []
        nxt = _js_pick(js, "last_supply_order_id", "last_order_id", "last_id")
        try:
            nxt_i = int(nxt) if nxt is not None else 0
        except Exception:
            nxt_i = 0
        out_ids: List[str] = []
        for x in ids:
            try:
                out_ids.append(str(int(x)))
            except Exception:
                continue
        return out_ids, nxt_i

    # 1) основной вызов — старое поле from_supply_order_id
    js1 = await _call_with("from_supply_order_id")
    ids1, nxt1 = _parse(js1)

    # 2) фолбэк — новое/альтернативное from_order_id (если прогресса нет)
    use_fallback = from_id > 0 and (nxt1 <= from_id)
    if use_fallback:
        js2 = await _call_with("from_order_id")
        ids2, nxt2 = _parse(js2)
        # берём тот вариант, который даёт движение вперёд или непустой список
        if ids2 or (nxt2 > nxt1 and nxt2 != from_id):
            return ids2, nxt2

    return ids1, nxt1


async def _supply_get(order_ids: List[str]) -> List[dict]:
    if not order_ids:
        return []
    payload = {"order_ids": [str(x) for x in order_ids[:50]]}
    js, resp = await _post_json("https://api-seller.ozon.ru/v3/supply-order/get", payload)
    if resp is not None and getattr(resp, "status", 200) == 429:
        await _respect_rate_limit_sleep(resp)
        js, _ = await _post_json("https://api-seller.ozon.ru/v3/supply-order/get", payload)
    orders = _js_pick(js, "orders")
    return orders if isinstance(orders, list) else []


# ── status semantics ─────────────────────────────────────────────────────────
# Конечная фаза для расчёта lead time: приоритет
# 1) ACCEPTANCE_AT_STORAGE_WAREHOUSE → 2) REPORTS_CONFIRMATION_AWAITING → 3) COMPLETED


def _get_end_ts_from_states(states: Dict[str, str]) -> Optional[str]:
    """
    Возвращает ISO‑время «завершения» заявки.
    По новому ТЗ: приоритетно берём ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    если её нет — REPORTS_CONFIRMATION_AWAITING, далее — COMPLETED.
    """
    if not isinstance(states, dict):
        return None
    return states.get(STORAGE_ACCEPT_STATE) or states.get(END_STATE) or states.get(COMP_STATE)


def _has_end_state(states: Dict[str, str]) -> bool:
    s = states or {}
    return bool(s.get(STORAGE_ACCEPT_STATE) or s.get(END_STATE) or s.get(COMP_STATE))


# ── status cache (фаза B) ────────────────────────────────────────────────────


def _states_load() -> dict:
    d = _read_json(STATES_CACHE_PATH)
    return d if isinstance(d, dict) else {}


def _states_save(payload: dict) -> None:
    _write_json(STATES_CACHE_PATH, payload or {})


def _ensure_list_unique_int(lst) -> List[int]:
    out: List[int] = []
    for v in lst or []:
        try:
            i = int(v)
            if i not in out:
                out.append(i)
        except Exception:
            continue
    return out


def _bundle_items(bundle_ids: List[str]) -> Dict[str, List[Tuple[int, float]]]:
    """Чтение состава заявки {bundle_id: [(sku, qty), ...]} с постраничностью."""
    out: Dict[str, List[Tuple[int, float]]] = {}
    if not bundle_ids or not requests:
        return out
    todo = [str(bid).strip() for bid in bundle_ids if str(bid).strip()]
    if len(todo) > LEAD_BUNDLE_MAX_PER_RUN:
        todo = todo[:LEAD_BUNDLE_MAX_PER_RUN]
    total_tries = 0
    for bid in todo:
        if total_tries >= LEAD_BUNDLE_MAX_TOTAL_TRIES:
            break
        bucket: List[Tuple[int, float]] = []
        last_id: Optional[str] = None
        has_next = True
        while has_next:
            payload = {"bundle_ids": [bid], "limit": 100, "is_asc": True}
            if last_id:
                payload["last_id"] = last_id
            attempt = 0
            while attempt < 3:
                attempt += 1
                total_tries += 1
                js, resp = _post_json(
                    "https://api-seller.ozon.ru/v1/supply-order/bundle",
                    payload,
                    timeout=LEAD_HTTP_TIMEOUT,
                )
                if resp is not None and getattr(resp, "status_code", 200) == 429:
                    _respect_rate_limit_sleep(resp)
                    continue
                items = _js_pick(js, "items")
                items = items if isinstance(items, list) else []
                for it in items:
                    sku = it.get("sku")
                    qty = it.get("quantity", 1)
                    try:
                        sku_i = int(sku)
                        q_f = float(qty if qty is not None else 1.0)
                        if q_f <= 0:
                            q_f = 1.0
                        bucket.append((sku_i, q_f))
                    except Exception:
                        continue
                has_next = bool(js.get("has_next"))
                last_id = js.get("last_id") if has_next else None
                time.sleep(0.06)
                break
            time.sleep(0.03)
        if bucket:
            out[bid] = bucket
        time.sleep(0.03)
    return out


def _states_upsert_from_get(orders: List[dict], now_iso: Optional[str] = None) -> None:
    """
    Обновляем кэш состояний по списку orders из /v3/supply-order/get.

    ВАЖНО:
    - НЕ создаём запись и НЕ отмечаем «завершение», если у заявки ещё нет ACCEPTED (см. ниже purge).
    - Снимок sku_items делаем только при ПЕРВОМ ACCEPTED.
    - Конечная дата для расчёта lead time определяется как:
      ACCEPTANCE_AT_STORAGE_WAREHOUSE → REPORTS_CONFIRMATION_AWAITING → COMPLETED.
    """
    if not orders:
        return
    now_iso = now_iso or _utc_now_iso()
    cache = _states_load()

    order_to_bundles: Dict[str, List[str]] = {}

    for o in orders:
        # ID заявки: v3 — order_id; v2 — supply_order_id
        sid_i: Optional[int] = None
        try:
            sid_i = int(o.get("order_id"))
        except Exception:
            try:
                sid_i = int(o.get("supply_order_id"))
            except Exception:
                sid_i = None
        if sid_i is None:
            continue
        sid = str(sid_i)

        st_name = _normalize_state(o.get("state") or "")

        # Если заявка нам ещё не известна и это «голое завершение» — пропускаем
        if sid not in cache and st_name in {COMP_STATE, END_STATE, STORAGE_ACCEPT_STATE}:
            continue

        rec = cache.get(sid) or {}
        if not rec.get("first_seen"):
            rec["first_seen"] = now_iso

        # dropoff (v3: dropoff_warehouse.warehouse_id |
        # drop_off_warehouse.warehouse_id; v2: dropoff_warehouse_id)
        drop_wid = None
        try:
            drop_wid = int(o.get("dropoff_warehouse_id"))
        except Exception:
            try:
                drop_w = (
                    o.get("dropoff_warehouse") or o.get("drop_off_warehouse") or {}
                )  # ← доп. фолбэк
                drop_wid = int((drop_w or {}).get("warehouse_id"))
            except Exception:
                drop_wid = None
        rec["dropoff_wid"] = drop_wid if drop_wid else rec.get("dropoff_wid") or None

        # storage_wids + bundle_ids
        cur_wids = rec.get("storage_wids") or []
        found_wids: List[int] = []
        bundles: List[str] = []
        for s in o.get("supplies") or []:
            # v3: storage_warehouse.warehouse_id; v2: storage_warehouse_id
            wid = None
            try:
                wid = int(s.get("storage_warehouse_id"))
            except Exception:
                try:
                    wid = int((s.get("storage_warehouse") or {}).get("warehouse_id"))
                except Exception:
                    wid = None
            if wid:
                found_wids.append(wid)
            bid = s.get("bundle_id")
            if bid:
                bundles.append(str(bid))

        rec["storage_wids"] = _ensure_list_unique_int(cur_wids + found_wids)
        if bundles:
            rec["bundle_ids"] = sorted(
                list({*([str(b) for b in bundles]), *([str(b) for b in rec.get("bundle_ids",                                                                                   [])])})
            )

        # номер заявки (в v3: order_number; v2: supply_order_number)
        so_num = o.get("order_number") or o.get("supply_order_number")
        if so_num and not rec.get("supply_order_number"):
            rec["supply_order_number"] = str(so_num)

        # фиксируем «первое замеченное время» только корректных последовательностей
        states: Dict[str, str] = rec.get("states") or {}

        if st_name == START_STATE:
            if START_STATE not in states:
                states[START_STATE] = now_iso
                if rec.get("bundle_ids"):
                    order_to_bundles[sid] = rec["bundle_ids"]

        elif st_name == END_STATE:
            # «Согласование актов» — одна из финальных стадий
            if END_STATE not in states:
                states[END_STATE] = now_iso

        elif st_name == COMP_STATE:
            # Фолбэк (устойчивость к старым данным/провайдерам)
            if START_STATE in states and COMP_STATE not in states:
                states[COMP_STATE] = now_iso

        else:
            # сюда попадает и ACCEPTANCE_AT_STORAGE_WAREHOUSE (предпочтительная конечная)
            if st_name and st_name not in states:
                states[st_name] = now_iso

        rec["states"] = states
        cache[sid] = rec

    # Снимок sku_items на первый ACCEPTED
    if order_to_bundles:
        for sid, bundle_ids in order_to_bundles.items():
            try:
                bmap = _bundle_items(bundle_ids or [])
                sku_items: List[Tuple[int, float]] = []
                for bid in bundle_ids or []:
                    for pair in bmap.get(bid) or []:
                        sku_items.append(pair)
                if sku_items:
                    rec = cache.get(sid) or {}
                    if not rec.get("sku_items"):  # только один снимок
                        rec["sku_items"] = [[int(s), float(q)] for s, q in sku_items]
                        cache[sid] = rec
            except Exception:
                continue

    _states_save(cache)
    try:
        _purge_completed_without_start()
    except Exception:
        pass


# ── retention ────────────────────────────────────────────────────────────────


def _retain_events(now_utc: Optional[dt.datetime] = None) -> int:
    now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
    cutoff = now_utc - dt.timedelta(days=max(1, int(LEAD_RETENTION_DAYS)))
    cache = _read_json(EVENTS_CACHE_PATH)
    rows = cache.get("rows", []) if isinstance(cache, dict) else []
    kept: List[dict] = []
    removed = 0
    for e in rows:
        try:
            tend = _parse_iso_dt(str(e.get("ts_end", "")))
            if tend and tend < cutoff:
                removed += 1
                continue
            kept.append(e)
        except Exception:
            kept.append(e)
    if removed:
        _write_json(EVENTS_CACHE_PATH, {"saved_at": _utc_now_iso(), "rows": kept, "version": 2})
        try:
            _write_json(STATS_CACHE_PATH, {})
        except Exception:
            pass
    return removed


def _retain_states(now_utc: Optional[dt.datetime] = None) -> int:
    now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
    cutoff = now_utc - dt.timedelta(days=max(1, int(LEAD_RETENTION_DAYS)))
    cache = _states_load()
    removed = 0
    for sid, rec in list(cache.items()):
        try:
            states = rec.get("states") or {}
            cmp_iso = _get_end_ts_from_states(states)
            if not cmp_iso:
                continue
            tend = _parse_iso_dt(str(cmp_iso))
            if tend and tend < cutoff:
                cache.pop(sid, None)
                removed += 1
        except Exception:
            continue
    if removed:
        _states_save(cache)
    return removed


def _purge_completed_without_start() -> int:
    """
    Удаляет из кэша заявки, где есть «конечный» статус
    (ACCEPTANCE_AT_STORAGE_WAREHOUSE/REPORTS_CONFIRMATION_AWAITING/COMPLETED),
    но НЕТ ACCEPTED.
    Возвращает число удалённых записей.
    """
    cache = _states_load()
    removed = 0
    for sid, rec in list(cache.items()):
        st = rec.get("states") or {}
        if _has_end_state(st) and START_STATE not in st:
            cache.pop(sid, None)
            removed += 1
    if removed:
        _states_save(cache)
    return removed


# ── ingest state helpers (fix NameError) ─────────────────────────────────────


def _default_ingest_state() -> dict:
    return {
        "last_run_at": "",
        "last_added": 0,
        "last_pages": 0,
        "next_allowed_ts": 0.0,
        "is_running": False,
    }


def _read_state() -> dict:
    """
    Безопасное чтение состояния инжеста из LEAD_INGEST_STATE_PATH.
    Возвращает словарь со всеми обязательными ключами.
    """
    d = _read_json(LEAD_INGEST_STATE_PATH)
    if not isinstance(d, dict):
        d = {}
    state = _default_ingest_state()
    state.update({k: d.get(k, state[k]) for k in state.keys()})
    # типобезопасные приведения
    try:
        state["last_added"] = int(state.get("last_added") or 0)
    except Exception:
        state["last_added"] = 0
    try:
        state["last_pages"] = int(state.get("last_pages") or 0)
    except Exception:
        state["last_pages"] = 0
    try:
        state["next_allowed_ts"] = float(state.get("next_allowed_ts") or 0.0)
    except Exception:
        state["next_allowed_ts"] = 0.0
    state["is_running"] = bool(state.get("is_running", False))
    state["last_run_at"] = str(state.get("last_run_at") or "")
    return state


def _write_state(payload: dict) -> None:
    """
    Атомарная запись состояния инжеста в LEAD_INGEST_STATE_PATH.
    """
    st = _default_ingest_state()
    try:
        st.update(payload or {})
    except Exception:
        pass
    _write_json(LEAD_INGEST_STATE_PATH, st)


def _should_force_tick(now_ts: float, st: dict, primary_bootstrap: bool) -> bool:
    """
    Возвращает True, если нужно проигнорировать антидребезг и стартовать тик немедленно:
      • включён LEAD_TICK_FORCE,
      • первичный бутстрап (пустой кэш событий).
    """
    try:
        if bool(int(os.getenv("LEAD_TICK_FORCE", "0"))):
            return True
    except Exception:
        pass
    if primary_bootstrap:
        return True
    return False


# ── emit phase-B events from states ──────────────────────────────────────────


def _wid_to_cluster_map() -> Dict[int, int]:
    """
    Основной путь — из stocks (если доступен). Если нет — дополнительные фолбэки:
    • mapping из shipments_leadtime.get_warehouse_cluster_map()
    • /cluster/list → warehouse_id → cluster_name → синтетический int id (стабильно для агрегации).
    """
    wid2cid: Dict[int, int] = {}
    # 1) stocks (view=warehouse)
    try:
        from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore

        for r in fetch_stocks_view(view="warehouse") or []:
            try:
                wid = int(r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id"))
                cid = int(
                    r.get("cluster_id")
                    or (r.get("dimensions") or [{}])[0].get("cluster_id")
                    or (r.get("dimensions") or [{}])[0].get("clusterId")
                )
                wid2cid[wid] = cid
            except Exception:
                continue
    except Exception:
        pass
    if wid2cid:
        return wid2cid

    # 2) явная карта из leadtime (если есть)
    try:
        from .shipments_leadtime import get_warehouse_cluster_map  # type: ignore

        m = get_warehouse_cluster_map() or {}
        for w, c in (m or {}).items():
            try:
                wid2cid[int(w)] = int(c)
            except Exception:
                continue
        if wid2cid:
            return wid2cid
    except Exception:
        pass

    # 3) ФОЛБЭК: /v1/cluster/list → warehouse_id → cluster_name → синтетический id
    try:
        from .shipments_report_data import load_clusters  # type: ignore

        js = load_clusters(force=False) or {}
        name_by_wid: Dict[int, str] = {}
        for cl in js.get("clusters") or []:
            cname = (
                cl.get("name") or cl.get("title") or cl.get("cluster_name") or ""
            ).strip() or "Кластер"
            for lc in cl.get("logistic_clusters") or []:
                for wh in lc.get("warehouses") or []:
                    wid = wh.get("warehouse_id") or wh.get("id") or wh.get("warehouseId")
                    try:
                        name_by_wid[int(wid)] = cname
                    except Exception:
                        continue
        if name_by_wid:
            out: Dict[int, int] = {}
            for wid, cname in name_by_wid.items():
                h = int(hashlib.md5(cname.encode("utf-8")).hexdigest()[:8], 16)
                out[int(wid)] = int(h & 0x7FFFFFFF)
            return out
    except Exception:
        pass
    return {}


def _emit_phase_b_events_from_states(now_iso: Optional[str] = None) -> int:
    """
    Для каждой заявки с ACCEPTED и «завершением»
    (ACCEPTANCE_AT_STORAGE_WAREHOUSE/REPORTS_CONFIRMATION_AWAITING/COMPLETED)
    создаём:
      • агрегатную гранулу (sku=None) — ВСЕГДА, если известен склад хранения,
      • SKU‑гранулы (sku=int) — только если успешно снят снимок sku_items.
    """
    now_iso = now_iso or _utc_now_iso()
    states = _states_load()
    if not states:
        return 0

    wid2cid = _wid_to_cluster_map()

    prev = _read_json(EVENTS_CACHE_PATH)
    rows_prev = prev.get("rows", []) if isinstance(prev, dict) else []

    def _key(e: dict) -> Tuple[int, int, str, Optional[int]]:
        sid = int(e.get("supply_order_id") or 0)
        wid = int(e.get("storage_wid") or 0)
        ts = str(e.get("ts_end") or "")
        sku = e.get("sku")
        try:
            sku_i = int(sku) if sku is not None else None
        except Exception:
            sku_i = None
        return (sid, wid, ts, sku_i)

    seen = {_key(e) for e in rows_prev}

    cutoff_keep = dt.datetime.now(dt.timezone.utc) - dt.timedelta(
        days=max(1, int(LEAD_RETENTION_DAYS))
    )

    new_events: List[dict] = []
    added = 0

    for sid, rec in states.items():
        try:
            sid_i = int(sid)
        except Exception:
            continue
        st = rec.get("states") or {}
        ts_start = st.get(START_STATE)
        ts_end = _get_end_ts_from_states(st)
        if not (ts_start and ts_end):
            continue

        a = _parse_iso_dt(str(ts_start))
        b = _parse_iso_dt(str(ts_end))
        if not a or not b or b <= a:
            continue
        dur = (b - a).total_seconds() / 86400.0
        if not (LEAD_MIN_DAYS <= dur <= 90):
            continue
        if b < cutoff_keep:
            continue

        dropoff_wid = rec.get("dropoff_wid")
        storage_wids = rec.get("storage_wids") or []
        sku_items = rec.get("sku_items") or []

        if not storage_wids:
            continue

        has_sku_items = bool(sku_items)
        total_q = sum((float(q or 0.0) for _, q in sku_items)) if has_sku_items else 0.0
        alloc_by_qty = get_lead_allocation_flag()

        for storage_i in storage_wids:
            try:
                wid_i = int(storage_i)
            except Exception:
                continue

            base = {
                "phase": "post_dropoff",
                "supply_order_id": sid_i,
                "ts_start": ts_start,
                "ts_end": ts_end,
                "dropoff_wid": int(dropoff_wid) if dropoff_wid else None,
                "storage_wid": wid_i,
                "cluster_id": wid2cid.get(wid_i),
                "cluster_name": None,
                "supply_order_number": rec.get("supply_order_number"),
                "source": "supply_order",
                "quality": "phase_b",
                "duration_days": float(dur),
            }

            # 1) Агрегатная гранула — всегда
            k0 = (sid_i, wid_i, ts_end, None)
            if k0 not in seen:
                new_events.append(dict(base, **{"sku": None, "qty": None}))
                seen.add(k0)
                added += 1

            # 2) SKU‑гранулы — только при наличии снимка состава
            if has_sku_items:
                tq = total_q if total_q > 0 else 1.0
                for sku, qty in sku_items:
                    try:
                        sku_i = int(sku)
                        qty_f = float(qty or 0.0)
                    except Exception:
                        continue
                    part_days = float(dur) if not alloc_by_qty else float(dur) * (qty_f / tq)
                    e2 = dict(base)
                    e2["sku"] = sku_i
                    e2["qty"] = float(qty_f)
                    e2["duration_days"] = float(part_days)
                    k = (sid_i, wid_i, ts_end, sku_i)
                    if k in seen:
                        continue
                    new_events.append(e2)
                    seen.add(k)
                    added += 1

    if not added:
        return 0

    merged = rows_prev + new_events
    _write_json(EVENTS_CACHE_PATH, {"saved_at": _utc_now_iso(), "rows": merged, "version": 2})
    try:
        _write_json(STATS_CACHE_PATH, {})
    except Exception:
        pass
    return added


# ── ensure recent on read ────────────────────────────────────────────────────


def _ensure_recent_events(period_days: int, max_pages: int) -> None:
    if LEAD_DISABLE_INGEST_ON_READ:
        return
    ev = _materialize_events(period_days)
    if ev:
        return
    try:
        update_leadtime_events(
            days=int(period_days),
            pages=max(1, int(max_pages)),
            primary_bootstrap=_is_events_empty(),
        )
    except Exception as ex:
        print("[leadtime] ensure_recent_events soft-failed:", ex)


# ── stats cache helpers ──────────────────────────────────────────────────────


def _load_stats_cache() -> dict:
    return _read_json(STATS_CACHE_PATH)


def _save_stats_cache(key: str, payload: Any) -> None:
    allc = _load_stats_cache()
    if not isinstance(allc, dict):
        allc = {}
    allc[key] = {"saved_at": _utc_now_iso(), "payload": payload}
    _write_json(STATS_CACHE_PATH, allc)


def _stats_key(period: int, view: str) -> str:
    # Флаг аллокации влияет только на "sku"
    alloc = (
        "1" if (view == "sku" and get_lead_allocation_flag()) else ("0" if view == "sku" else "-")
    )
    return f"P{int(period)}:{str(view)}:alloc={alloc}"


# ── helpers: cluster names ───────────────────────────────────────────────────


def _extract_cluster_name_from_row(r: dict) -> Tuple[Optional[int], str]:
    cid = r.get("cluster_id")
    name_candidates = [
        r.get("cluster_name"),
        r.get("cluster"),
        r.get("clusterTitle"),
        r.get("cluster_title"),
        r.get("name"),
        r.get("title"),
    ]
    dims = r.get("dimensions") or []
    if dims and isinstance(dims, list) and isinstance(dims[0], dict):
        cid = cid or dims[0].get("cluster_id") or dims[0].get("clusterId")
        name_candidates.extend(
            [
                dims[0].get("cluster_name"),
                dims[0].get("cluster"),
                dims[0].get("clusterTitle"),
                dims[0].get("cluster_title"),
                dims[0].get("name"),
                dims[0].get("title"),
            ]
        )
    cname = ""
    for c in name_candidates:
        s = str(c or "").strip()
        if s:
            cname = s
            break
    try:
        cid_i = int(cid) if cid is not None else None
    except Exception:
        cid_i = None
    return cid_i, cname


def _cluster_names_by_id() -> Dict[int, str]:
    """
    Собираем карту {cluster_id -> name} из доступных источников:
      • stocks(view='warehouse')
      • внутренний _get_stocks(view='warehouse')
      • /cluster/list (если есть id у кластера)
    """
    out: Dict[int, str] = {}

    # 1) stocks(view='warehouse')
    try:
        from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore

        for r in fetch_stocks_view(view="warehouse") or []:
            cid, cname = _extract_cluster_name_from_row(r)
            if cid and cname and cid not in out:
                out[int(cid)] = str(cname)
    except Exception:
        pass

    # 2) leadtime._get_stocks(view='warehouse')
    if not out:
        try:
            from .shipments_leadtime import _get_stocks  # type: ignore

            for r in _get_stocks(view="warehouse") or []:
                cid, cname = _extract_cluster_name_from_row(r)
                if cid and cname and cid not in out:
                    out[int(cid)] = str(cname)
        except Exception:
            pass

    # 3) /cluster/list (id → name)
    if not out:
        try:
            from .shipments_report_data import load_clusters  # type: ignore

            js = load_clusters(force=False) or {}
            for cl in js.get("clusters") or []:
                cid = cl.get("id") or cl.get("cluster_id") or cl.get("clusterId")
                cname = (cl.get("name") or cl.get("title") or cl.get("cluster_name") or "").strip()
                try:
                    if cid is not None and cname:
                        out[int(cid)] = cname
                except Exception:
                    continue
        except Exception:
            pass

    return out


# ── public stats ─────────────────────────────────────────────────────────────


def get_lead_stats_summary(period_days: int | None = None) -> Dict[str, float]:
    period = int(period_days or get_stat_period())
    key = _stats_key(period, "summary")
    cache_all = _load_stats_cache()
    cache = cache_all.get(key) or {}
    stats_saved = str(cache.get("saved_at") or "")
    events_saved = _events_saved_at()
    if cache and _is_fresh(stats_saved, LEAD_STAT_TTL_HOURS) and _iso_ge(stats_saved,                                                                             events_saved):
        return cache.get("payload", {})
    _ensure_recent_events(period, max_pages=2)
    events = _only_completed_with_duration(_materialize_events(period))
    # Сводка только по базовым событиям (один заказ × один склад)
    events = [e for e in events if (e.get("sku") is None or int(e.get("sku") or 0) == 0)]
    vals = [float(e["duration_days"]) for e in events]
    if not vals:
        payload = {"avg": 0.0, "p50": 0.0, "p90": 0.0, "n": 0.0}
        _save_stats_cache(key, payload)
        return payload
    vals.sort()
    n = len(vals)
    payload = {
        "avg": sum(vals) / n,
        "p50": _percentile(vals, 0.5),
        "p90": _percentile(vals, 0.9),
        "n": float(n),
    }
    _save_stats_cache(key, payload)
    return payload


def get_lead_stats_by_warehouse(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    period = int(period_days or get_stat_period())
    key = _stats_key(period, "warehouse")
    cache_all = _load_stats_cache()
    cache = cache_all.get(key) or {}
    stats_saved = str(cache.get("saved_at") or "")
    events_saved = _events_saved_at()
    if cache and _is_fresh(stats_saved, LEAD_STAT_TTL_HOURS) and _iso_ge(stats_saved,                                                                             events_saved):
        return cache.get("payload", [])
    _ensure_recent_events(period, max_pages=2)

    events = _only_completed_with_duration(_materialize_events(period))
    # Аггрегируем по базовым событиям — одно на заказ×склад
    events = [e for e in events if (e.get("sku") is None or int(e.get("sku") or 0) == 0)]

    if not events:
        _save_stats_cache(key, [])
        return []
    try:
        from .shipments_leadtime import get_current_warehouses  # type: ignore

        wid_name = get_current_warehouses()
    except Exception:
        wid_name = {}
    try:
        from .shipments_leadtime_data import get_warehouse_title as _wh_title_fallback  # type: ignore
    except Exception:

        def _wh_title_fallback(wid: int) -> str:
            return f"wh:{wid}"

    aggr = _aggregate_stats(events, key_fn=lambda e: int(e.get("storage_wid") or 0) or None)
    out = []
    for wid, m in aggr:
        try:
            wid_i = int(wid)
        except Exception:
            continue
        title = wid_name.get(wid_i) or _wh_title_fallback(wid_i) or f"wh:{wid_i}"
        out.append((wid_i, title, m))
    out.sort(
        key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), str(t[1]).lower())
    )
    _save_stats_cache(key, out)
    return out


def get_lead_stats_by_cluster(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    period = int(period_days or get_stat_period())
    key = _stats_key(period, "cluster")
    cache_all = _load_stats_cache()
    cache = cache_all.get(key) or {}
    stats_saved = str(cache.get("saved_at") or "")
    events_saved = _events_saved_at()
    if cache and _is_fresh(stats_saved, LEAD_STAT_TTL_HOURS) and _iso_ge(stats_saved,                                                                             events_saved):
        return cache.get("payload", [])
    _ensure_recent_events(period, max_pages=2)

    events = _only_completed_with_duration(_materialize_events(period))
    events = [e for e in events if (e.get("sku") is None or int(e.get("sku") or 0) == 0)]

    if not events:
        _save_stats_cache(key, [])
        return []

    # Основной путь: агрегируем по id кластера (через map склад→кластер)
    try:
        from .shipments_leadtime import get_warehouse_cluster_map, _get_stocks  # type: ignore

        wid2cid = get_warehouse_cluster_map()
        cid_name: Dict[int, str] = {}
        # пробуем достать названия из stocks leadtime
        for r in _get_stocks(view="warehouse") or []:
            try:
                cid, cname = _extract_cluster_name_from_row(r)
                if cid and cname and cid not in cid_name:
                    cid_name[cid] = cname
            except Exception:
                continue
    except Exception:
        wid2cid, cid_name = {}, {}

    # Доп. источники имён кластеров, если пусто/неполно
    if wid2cid and not cid_name:
        try:
            cid_name = _cluster_names_by_id()
        except Exception:
            cid_name = {}

    if wid2cid:
        aggr = _aggregate_stats(
            events, key_fn=lambda e: wid2cid.get(int(e.get("storage_wid") or 0))
        )
        out: List[Tuple[int, str, Dict[str, float]]] = []
        for cid, m in aggr:
            if cid is None:
                continue
            cname = cid_name.get(int(cid))
            if not cname:
                cname = f"Кластер {int(cid)}"
            out.append((int(cid), cname, m))
        out.sort(
            key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), str(t[1]).lower())
        )
        _save_stats_cache(key, out)
        return out

    # ФОЛБЭК — без wid→cid: агрегируем по названию кластера из /cluster/list
    try:
        from .shipments_report_data import load_clusters  # type: ignore

        js = load_clusters(force=False) or {}
        name_by_wid: Dict[int, str] = {}
        for cl in js.get("clusters") or []:
            cname = (
                cl.get("name") or cl.get("title") or cl.get("cluster_name") or ""
            ).strip() or "Кластер"
            for lc in cl.get("logistic_clusters") or []:
                for wh in lc.get("warehouses") or []:
                    wid = wh.get("warehouse_id") or wh.get("id") or wh.get("warehouseId")
                    try:
                        name_by_wid[int(wid)] = cname
                    except Exception:
                        continue
    except Exception:
        name_by_wid = {}

    if not name_by_wid:
        _save_stats_cache(key, [])
        return []

    aggr2 = _aggregate_stats(
        events, key_fn=lambda e: name_by_wid.get(int(e.get("storage_wid") or 0))
    )
    out2: List[Tuple[int, str, Dict[str, float]]] = []
    for cname, m in aggr2:
        if not cname:
            continue
        cid_synth = int(hashlib.md5(str(cname).encode("utf-8")).hexdigest()[:8], 16) & 0x7FFFFFFF
        out2.append((cid_synth, str(cname), m))
    out2.sort(
        key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), str(t[1]).lower())
    )
    _save_stats_cache(key, out2)
    return out2


def get_lead_stats_by_sku(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    """
    Возвращает список по SKU с фильтром/порядком по WATCH_SKU (если задан).
    Если WATCH_SKU задан, но не дал попаданий — используем фолбэк к фактическим данным.
    """
    period = int(period_days or get_stat_period())
    key = _stats_key(period, "sku")
    cache_all = _load_stats_cache()
    cache = cache_all.get(key) or {}
    stats_saved = str(cache.get("saved_at") or "")
    events_saved = _events_saved_at()
    if cache and _is_fresh(stats_saved, LEAD_STAT_TTL_HOURS) and _iso_ge(stats_saved,                                                                             events_saved):
        return cache.get("payload", [])
    _ensure_recent_events(period, max_pages=2)
    events = _only_completed_with_duration(_materialize_events(period))
    events = [e for e in events if int(e.get("sku") or 0) > 0]
    if not events:
        _save_stats_cache(key, [])
        return []
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
    except Exception:

        def get_alias_for_sku(sku: int) -> str:  # type: ignore
            return str(sku)

    aggr = _aggregate_stats(events, key_fn=lambda e: int(e.get("sku") or 0) or None)
    aggr_map: Dict[int, Dict[str, float]] = {}
    for sku, m in aggr:
        try:
            aggr_map[int(sku)] = dict(m or {})
        except Exception:
            continue

    out: List[Tuple[int, str, Dict[str, float]]] = []
    if WATCH_ORDER:
        for sku in WATCH_ORDER:
            m = aggr_map.get(int(sku))
            if not m:
                continue
            alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
            out.append((int(sku), alias, m))
        if not out:
            tmp: List[Tuple[int, str, Dict[str, float]]] = []
            for sku, m in aggr:
                alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
                tmp.append((int(sku), alias, m))
            tmp.sort(
                key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower())
            )
            out = tmp
    else:
        tmp: List[Tuple[int, str, Dict[str, float]]] = []
        for sku, m in aggr:
            alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
            tmp.append((int(sku), alias, m))
        tmp.sort(key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower()))
        out = tmp

    _save_stats_cache(key, out)
    return out


# ── drill-down helpers ───────────────────────────────────────────────────────


def get_lead_stats_sku_for_warehouse(
    warehouse_id: int, period_days: int | None = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    period = int(period_days or get_stat_period())
    _ensure_recent_events(period, max_pages=2)
    events = _only_completed_with_duration(_materialize_events(period))
    ev = [
        e
        for e in events
        if int(e.get("sku") or 0) > 0 and int(e.get("storage_wid") or 0) == int(warehouse_id)
    ]
    if not ev:
        return []
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
    except Exception:

        def get_alias_for_sku(sku: int) -> str:  # type: ignore
            return str(sku)

    aggr = _aggregate_stats(ev, key_fn=lambda e: int(e.get("sku") or 0) or None)

    aggr_map: Dict[int, Dict[str, float]] = {}
    for sku, m in aggr:
        try:
            aggr_map[int(sku)] = dict(m or {})
        except Exception:
            continue

    out: List[Tuple[int, str, Dict[str, float]]] = []
    if WATCH_ORDER:
        for sku in WATCH_ORDER:
            m = aggr_map.get(int(sku))
            if m:
                alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
                out.append((int(sku), alias, m))
        if not out:
            tmp: List[Tuple[int, str, Dict[str, float]]] = []
            for sku, m in aggr:
                alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
                tmp.append((int(sku), alias, m))
            tmp.sort(
                key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower())
            )
            out = tmp
    else:
        tmp: List[Tuple[int, str, Dict[str, float]]] = []
        for sku, m in aggr:
            alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
            tmp.append((int(sku), alias, m))
        tmp.sort(key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower()))
        out = tmp

    return out


def get_lead_stats_sku_for_cluster(
    cluster_id: int, period_days: int | None = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    period = int(period_days or get_stat_period())
    _ensure_recent_events(period, max_pages=2)
    events = _only_completed_with_duration(_materialize_events(period))
    ev = [
        e
        for e in events
        if int(e.get("sku") or 0) > 0 and int(e.get("cluster_id") or 0) == int(cluster_id)
    ]
    if not ev:
        return []
    try:
        from modules_sales.sales_facts_store import get_alias_for_sku  # type: ignore
    except Exception:

        def get_alias_for_sku(sku: int) -> str:  # type: ignore
            return str(sku)

    aggr = _aggregate_stats(ev, key_fn=lambda e: int(e.get("sku") or 0) or None)

    aggr_map: Dict[int, Dict[str, float]] = {}
    for sku, m in aggr:
        try:
            aggr_map[int(sku)] = dict(m or {})
        except Exception:
            continue

    out: List[Tuple[int, str, Dict[str, float]]] = []
    if WATCH_ORDER:
        for sku in WATCH_ORDER:
            m = aggr_map.get(int(sku))
            if m:
                alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
                out.append((int(sku), alias, m))
        if not out:
            tmp: List[Tuple[int, str, Dict[str, float]]] = []
            for sku, m in aggr:
                alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
                tmp.append((int(sku), alias, m))
            tmp.sort(
                key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower())
            )
            out = tmp
    else:
        tmp: List[Tuple[int, str, Dict[str, float]]] = []
        for sku, m in aggr:
            alias = (get_alias_for_sku(int(sku)) or "").strip() or str(sku)
            tmp.append((int(sku), alias, m))
        tmp.sort(key=lambda t: (-int(t[2].get("n", 0)), -float(t[2].get("avg", 0.0)), t[1].lower()))
        out = tmp

    return out


# ── helpers for manual leads sync ────────────────────────────────────────────


def get_stats_avg_by_warehouse(period_days: int) -> Dict[int, float]:
    out: Dict[int, float] = {}
    for wid, _name, metrics in get_lead_stats_by_warehouse(period_days):
        try:
            out[int(wid)] = float(metrics.get("avg", 0.0) or 0.0)
        except Exception:
            continue
    return out


def apply_stats_to_leads_for_followers() -> int:
    try:
        from modules_shipments.shipments_leadtime_data import get_following_wids, set_lead_for_wid  # type: ignore
    except Exception:
        return 0

    followers = get_following_wids() or {}
    if not followers:
        return 0

    periods = sorted({int((rec or {}).get("follow_period") or 90) for rec in followers.values()})
    period_maps: Dict[int, Dict[int, float]] = {}
    for p in periods:
        try:
            period_maps[p] = get_stats_avg_by_warehouse(int(p))
        except Exception:
            period_maps[p] = {}

    updated = 0
    for wid, rec in followers.items():
        try:
            p = int((rec or {}).get("follow_period") or 90)
            metric = str((rec or {}).get("follow_metric") or "avg")
            if metric != "avg":
                continue
            avg = float(period_maps.get(p, {}).get(int(wid)) or 0.0)
            if avg <= 0:
                continue
            set_lead_for_wid(int(wid), round(avg, 2), updated_by=f"stats_sync:P{p}")
            updated += 1
        except Exception:
            continue
    return updated


# 🆕 ── авто‑включение подписки для складов, замеченных в статистике ──────────


def _auto_enable_follow_for_seen_wids() -> int:
    try:
        from modules_shipments.shipments_leadtime_data import get_following_wids, enable_follow_stats  # type: ignore
    except Exception:
        return 0
    try:
        existing = set(int(w) for w in (get_following_wids() or {}).keys())
    except Exception:
        existing = set()

    js = _read_json(EVENTS_CACHE_PATH)
    rows = js.get("rows", []) if isinstance(js, dict) else []
    seen: set[int] = set()
    for e in rows:
        try:
            if str(e.get("phase") or "") != "post_dropoff":
                continue
            w = int(e.get("storage_wid") or 0)
            if w > 0:
                seen.add(w)
        except Exception:
            continue

    todo = [w for w in seen if w not in existing]
    if not todo:
        return 0

    period = get_stat_period() or LEAD_STAT_DAYS_DEFAULT
    enabled = 0
    for w in todo:
        try:
            enable_follow_stats(int(w), period=int(period), metric="avg")
            enabled += 1
        except Exception:
            continue
    return enabled


def update_leadtime_events(
    days: int = LEAD_STAT_DAYS_DEFAULT,
    source: str = "all",
    pages: int = 1,
    *,
    primary_bootstrap: bool = False,
) -> int:
    if not requests or not OZON_CLIENT_ID or not OZON_API_KEY:
        print("[leadtime] requests or API keys missing; skip ingest")
        return 0

    _retain_events()
    _retain_states()

    from_id = 0
    pages_limit = max(1, min(int(pages), int(LEAD_MAX_PAGES)))
    page_cnt = 0
    # v3 статусы
    STATES = [
        START_STATE,
        "IN_TRANSIT",
        STORAGE_ACCEPT_STATE,  # ← ключевая «конечная» стадия
        END_STATE,  # ← фолбэк 1
        COMP_STATE,  # ← фолбэк 2
    ]

    while page_cnt < pages_limit:
        ids, nxt = _supply_list(
            states=STATES, from_id=from_id, limit=min(int(LEAD_FETCH_BATCH), 100)
        )
        if not ids:
            if page_cnt == 0:
                print("[leadtime] no supply-order ids (multi-status)")
            break
        page_cnt += 1

        for i in range(0, len(ids), max(1, int(LEAD_GET_BATCH))):
            batch_ids = ids[i : i + max(1, int(LEAD_GET_BATCH))]
            orders = _supply_get(batch_ids)
            if orders:
                try:
                    _states_upsert_from_get(orders, now_iso=_utc_now_iso())
                except Exception as ex:
                    print("[leadtime] states_upsert error:", ex)
            time.sleep(0.06)

        # защита от стагнации пагинации (если сервер не принимает from_* параметр)
        if not nxt or int(nxt) <= int(from_id):
            break
        from_id = nxt
        time.sleep(0.08)

    try:
        _purge_completed_without_start()
    except Exception:
        pass

    added = 0
    try:
        added = _emit_phase_b_events_from_states(_utc_now_iso())
    except Exception as ex:
        print("[leadtime] emit phase-B events failed:", ex)
        added = 0

    if added > 0:
        try:
            _write_json(STATS_CACHE_PATH, {})
        except Exception:
            pass

    # 🆕 авто‑включим подписку для всех появившихся складов и сразу подтянем значения
    try:
        newly_enabled = _auto_enable_follow_for_seen_wids()
        if newly_enabled:
            print(f"[leadtime] auto-follow enabled for {newly_enabled} warehouses")
    except Exception as ex:
        print("[leadtime] auto-follow failed:", ex)

    try:
        synced = apply_stats_to_leads_for_followers()
        if synced:
            print(f"[leadtime] stats_sync: updated manual leads for {synced} followers")
    except Exception as ex:
        print("[leadtime] stats_sync failed:", ex)

    print(f"[leadtime] phase-B events added: {int(added)}")
    return int(added or 0)


def ingest_tick(pages: Optional[int] = None, days: Optional[int] = None) -> int:
    st = _read_state()
    now = dt.datetime.now().timestamp()

    primary_bootstrap = _is_events_empty()

    if not _should_force_tick(now, st, primary_bootstrap):
        if now < float(st.get("next_allowed_ts") or 0.0) or st.get("is_running"):
            return 0

    st["is_running"] = True
    _write_state(st)
    try:
        period_days = int(days or get_stat_period() or LEAD_STAT_DAYS_DEFAULT)

        page_depth = int(pages if pages is not None else LEAD_INGEST_PAGES_DEFAULT)
        if primary_bootstrap:
            page_depth = max(page_depth, int(os.getenv("LEAD_PRIMARY_PAGES", LEAD_PRIMARY_PAGES)))
        page_depth = max(1, min(page_depth, int(os.getenv("LEAD_MAX_PAGES", "50"))))

        added = update_leadtime_events(
            days=period_days, pages=page_depth, primary_bootstrap=primary_bootstrap
        )

        try:
            _write_json(STATS_CACHE_PATH, {})
        except Exception:
            pass

        st.update(
            {
                "last_run_at": dt.datetime.now().isoformat(timespec="seconds"),
                "last_added": int(added or 0),
                "last_pages": page_depth,
                "next_allowed_ts": now + max(60, int(LEAD_INGEST_INTERVAL_SEC)),
                "is_running": False,
            }
        )
        _write_state(st)
        return int(added or 0)
    except Exception:
        st.update(
            {
                "last_run_at": dt.datetime.now().isoformat(timespec="seconds"),
                "last_added": 0,
                "last_pages": int(pages or LEAD_INGEST_PAGES_DEFAULT),
                "next_allowed_ts": now + max(60, int(LEAD_INGEST_INTERVAL_SEC)),
                "is_running": False,
            }
        )
        _write_state(st)
        return 0


# ── maintenance (public) ─────────────────────────────────────────────────────


def invalidate_stats_cache() -> None:
    try:
        _write_json(STATS_CACHE_PATH, {})
    except Exception:
        pass


def rebuild_events_from_states() -> int:
    try:
        _write_json(EVENTS_CACHE_PATH, {"saved_at": _utc_now_iso(), "rows": [], "version": 2})
        added = _emit_phase_b_events_from_states(_utc_now_iso())
        _write_json(STATS_CACHE_PATH, {})
        # 🆕 сразу включим follow для замеченных складов и подтянем значения
        try:
            newly_enabled = _auto_enable_follow_for_seen_wids()
            if newly_enabled:
                print(f"[leadtime] auto-follow enabled for {newly_enabled} warehouses (rebuild)")
        except Exception as ex:
            print("[leadtime] auto-follow failed (rebuild):", ex)

        try:
            synced = apply_stats_to_leads_for_followers()
            if synced:
                print(f"[leadtime] stats_sync: updated manual leads for {synced} followers")
        except Exception as ex:
            print("[leadtime] stats_sync failed:", ex)
        return int(added or 0)
    except Exception:
        return 0


def ingest_status() -> dict:
    st = _read_state()
    cache_ev = _read_json(EVENTS_CACHE_PATH)
    rows = cache_ev.get("rows") or []
    total = len(rows)
    base_rows = sum(1 for e in rows if (e.get("sku") is None or int(e.get("sku") or 0) == 0))
    sku_rows = max(0, total - base_rows)

    last_run_at = str(st.get("last_run_at") or "")
    events_saved_at = _events_saved_at() or ""

    def _to_ts(s: str) -> float:
        d = _parse_iso_dt(s)
        return d.timestamp() if d else 0.0

    last_activity_iso = last_run_at
    if _to_ts(events_saved_at) > _to_ts(last_run_at):
        last_activity_iso = events_saved_at

    st_cache = _states_load()
    tracked = 0
    completed = 0
    for _, rec in st_cache.items():
        states = rec.get("states") or {}
        has_a = START_STATE in states
        has_c = _has_end_state(states)
        if has_a:
            tracked += 1
        if has_a and has_c:
            completed += 1

    return {
        "last_run_at": last_activity_iso,
        "last_added": int(st.get("last_added") or 0),
        "last_pages": int(st.get("last_pages") or 0),
        "total_cached": total,
        "base_rows": base_rows,
        "sku_rows": sku_rows,
        "tracked": tracked,
        "completed": completed,
        "in_progress": max(0, tracked - completed),
        "state_path": LEAD_INGEST_STATE_PATH,
        "events_path": EVENTS_CACHE_PATH,
        "states_path": STATES_CACHE_PATH,
    }


__all__ = [
    "get_stat_period",
    "save_stat_period",
    "get_lead_allocation_flag",
    "set_lead_allocation_flag",
    "_is_fresh",
    "_materialize_events",
    "_only_completed_with_duration",
    "_aggregate_stats",
    "_ensure_recent_events",
    "STATS_CACHE_PATH",
    "EVENTS_CACHE_PATH",
    "STATES_CACHE_PATH",
    "LEAD_STAT_DAYS_DEFAULT",
    "LEAD_STAT_TTL_HOURS",
    "LEAD_DISABLE_INGEST_ON_READ",
    "get_lead_stats_summary",
    "get_lead_stats_by_warehouse",
    "get_lead_stats_by_cluster",
    "get_lead_stats_by_sku",
    "get_lead_stats_sku_for_warehouse",
    "get_lead_stats_sku_for_cluster",
    "get_stats_avg_by_warehouse",
    "apply_stats_to_leads_for_followers",
    "update_leadtime_events",
    "ingest_tick",
    "ingest_status",
    "invalidate_stats_cache",
    "rebuild_events_from_states",
    "get_current_watch_sku",
]
