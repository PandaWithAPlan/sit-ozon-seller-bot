# modules_purchases/purchases_need_data.py
from __future__ import annotations
from .purchases_report_data import load_excel, load_stocks  # type: ignore

import os
import json
import time
from typing import Dict, List, Tuple, Optional

from dotenv import load_dotenv
from config_package import safe_read_json, safe_write_json

# Логирование
log = logging.getLogger("seller-bot.purchases_need_data")

# ── .env / базовые пути ──────────────────────────────────────────────────────
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(ROOT_DIR, "data")
CACHE_DIR = os.path.join(DATA_DIR, "cache")
COMMON_DIR = os.path.join(CACHE_DIR, "common")
os.makedirs(COMMON_DIR, exist_ok=True)

load_dotenv(os.path.join(ROOT_DIR, ".env"))

# Настройки распределения
BUYOUTS_PREFS_PATH = os.path.join(COMMON_DIR, "buyouts_dist_prefs.json")

# ⬇️ Берём те же данные Excel, что и «Статус выкупов»

# План продаж (горизонт) и подпись методики
try:
    from modules_sales.sales_forecast import (  # type: ignore
        _fetch_series as _fc_fetch_series,
        _forecast_next as _fc_forecast_next,
        get_forecast_method_title,
    )
except Exception:

    def get_forecast_method_title() -> str:  # type: ignore
        return "методика не указана"

    def _fc_fetch_series(_n: int) -> Dict[int, list]:  # type: ignore
        return {}

    def _fc_forecast_next(_seq, _h: int):  # type: ignore
        return (0.0, 0.0)


# ⬇️ Ключевое: тот же каскад плана, что и в «Отгрузках» (goal-aware)
try:
    from modules_shipments.shipments_need_data import load_plan30_by_sku as _load_plan30_by_sku  # type: ignore
except Exception:

    def _load_plan30_by_sku() -> Dict[int, float]:  # type: ignore
        return {}


ALERT_PLAN_HORIZON_DAYS = int(os.getenv("ALERT_PLAN_HORIZON_DAYS", "30"))

# Список складов (для экрана распределения)
try:
    from modules_shipments.shipments_data import fetch_stocks_view  # type: ignore
except Exception:

    def fetch_stocks_view(view: str = "warehouse", force: bool = False) -> List[dict]:
        return []


# ✅ Предпочитаем стабильный источник перечня складов из блока leadtime
try:
    from modules_shipments.shipments_leadtime import (  # type: ignore
        get_current_warehouses as _lt_get_current_warehouses,
        get_dropoff_warehouses as _lt_get_dropoff_warehouses,
    )
except Exception:
    try:
        from modules_shipments.shipments_leadtime_data import (  # type: ignore
            get_current_warehouses as _lt_get_current_warehouses,
            get_dropoff_warehouses as _lt_get_dropoff_warehouses,
        )
    except Exception:
        _lt_get_current_warehouses = None  # type: ignore

        def _lt_get_dropoff_warehouses() -> Dict[int, str]:  # type: ignore
            return {}


# ─────────────────────────────────────────────────────────────
#   ПУБЛИЧНЫЕ API (данные «Выкупов»)
# ─────────────────────────────────────────────────────────────
def fetch_seller_rows(force: bool = False) -> Dict[int, Dict[str, int]]:
    rows = load_excel(force=force) or {}
    try:
        return {int(k): v for k, v in rows.items()}
    except Exception:
        return {}


def fetch_ozon_platform_totals(force: bool = False) -> Dict[int, float]:
    st = load_stocks(force=force) or {}
    out: Dict[int, float] = {}
    for k, m in st.items():
        try:
            sku = int(k)
        except Exception:
            continue
        total = (
            float(m.get("available_for_sale", 0.0) or 0.0)
            + float(m.get("checking", 0.0) or 0.0)
            + float(m.get("in_transit", 0.0) or 0.0)
            + float(m.get("valid_stock_count", 0.0) or 0.0)
            + float(m.get("return_from_customer_stock_count", 0.0) or 0.0)
            + float(m.get("reserved", 0.0) or 0.0)
        )
        out[sku] = total
    return out


def _forecast_series(days_back: int) -> Dict[int, list]:
    """
    Безопасный вызов источника временных рядов для прогноза.
    Поддерживаем как позиционный аргумент, так и именованный days_back.
    """
    try:
        return _fc_fetch_series(days_back) or {}
    except TypeError:
        try:
            return _fc_fetch_series(days_back=days_back) or {}
        except Exception:
            return {}
    except Exception:
        return {}


def fetch_plan_units(horizon_days: int = ALERT_PLAN_HORIZON_DAYS) -> Dict[int, float]:
    """
    План продаж по SKU на указанный горизонт H (по умолчанию 30 дней).
    Goal‑first → сначала каскад «как в Отгрузках», затем фолбэк на прогноз.
    """
    H = max(1, int(horizon_days))

    # (1) Каскадный план (уже goal-aware)
    try:
        plan30 = _load_plan30_by_sku() or {}
    except Exception:
        plan30 = {}
    if plan30:
        k = float(H) / 30.0
        out: Dict[int, float] = {}
        for sku, v in plan30.items():
            try:
                out[int(sku)] = float(v or 0.0) * k
            except Exception:
                continue
        return out

    # (2) Фолбэк: прогноз с корректным lookback
    days_back = max(60, H)
    try:
        lb = int(os.getenv("FORECAST_LOOKBACK_DAYS") or "0")
        if lb > 0:
            days_back = max(days_back, lb)
    except Exception:
        pass

    series = _forecast_series(days_back) or {}
    out: Dict[int, float] = {}
    for sku, seq in series.items():
        try:
            units_sum, _ = _fc_forecast_next(seq, H)
        except Exception:
            units_sum = 0.0
        try:
            out[int(sku)] = float(units_sum or 0.0)
        except Exception:
            continue

    # (2b) Наложить цели Goal×H, если есть
    try:
        try:
            from modules_sales.sales_goal import get_goal_per_day_by_sku as _goal_per_day  # type: ignore
        except Exception:
            from modules_sales.sales_goal import get_goal_units_by_sku as _goal_per_day  # type: ignore

        goals = _goal_per_day() or {}
        if isinstance(goals, dict) and goals:
            for k, per_day in goals.items():
                try:
                    s = int(k)
                    v = float(per_day or 0.0)
                    if v > 0:
                        out[s] = v * float(H)
                except Exception:
                    continue
    except Exception:
        pass

    return out


# ─────────────────────────────────────────────────────────────
#                        Городская конфигурация
# ─────────────────────────────────────────────────────────────
def get_city_config() -> dict:
    """Возвращает конфигурацию городов из .env: {"count": 1|2, "city1": "...", "city2": "..."}"""
    try:
        count = int(os.getenv("PURCHASES_CITY_COUNT", "2") or "2")
    except Exception:
        count = 2
    c1 = (os.getenv("PURCHASES_CITY1_NAME", "Москва") or "Москва").strip()
    c2 = (os.getenv("PURCHASES_CITY2_NAME", "Хабаровск") or "Хабаровск").strip()
    return {"count": (1 if count == 1 else 2), "city1": c1, "city2": c2}


# ─────────────────────────────────────────────────────────────
#                        Настройки «⚙️ Распределение»
# ─────────────────────────────────────────────────────────────
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


def load_dist_prefs() -> dict:
    """
    Загружает настройки распределения и дополняет их унифицированными ключами.
    Плюс: жёстко приводим ID к int и убираем дубликаты.
    """
    d = _read_json(BUYOUTS_PREFS_PATH) or {}
    if not isinstance(d, dict):
        d = {}

    raw_city1 = d.get("city1_wids") or d.get("moscow_wids") or []
    raw_city2 = d.get("city2_wids") or d.get("khabarovsk_wids") or []

    def _as_int_list(seq) -> List[int]:
        out: List[int] = []
        seen: set[int] = set()
        for x in seq or []:
            try:
                v = int(x)
            except Exception:
                continue
            if v not in seen:
                out.append(v)
                seen.add(v)
        return out

    city1 = _as_int_list(raw_city1)
    city2 = _as_int_list(raw_city2)

    return {
        "moscow_wids": list(city1),
        "khabarovsk_wids": list(city2),
        "city1_wids": list(city1),
        "city2_wids": list(city2),
        "version": int(d.get("version") or 2),
        "saved_at": d.get("saved_at"),
    }


def save_dist_prefs(moscow_wids: List[int], khab_wids: List[int]) -> None:
    """
    Сохраняем настройки распределения под старыми и новыми ключами одновременно.
    Исправлено: используем корректную переменную khab_wids (ранее была опечатка).
    """
    payload = {
        "saved_at": __import__("datetime").datetime.now().isoformat(),
        "moscow_wids": [int(w) for w in (moscow_wids or [])],
        "khabarovsk_wids": [int(w) for w in (khab_wids or [])],
        "city1_wids": [int(w) for w in (moscow_wids or [])],
        "city2_wids": [int(w) for w in (khab_wids or [])],
        "version": 2,
    }
    _write_json(BUYOUTS_PREFS_PATH, payload)


def reset_dist_prefs() -> None:
    _write_json(
        BUYOUTS_PREFS_PATH,
        {
            "moscow_wids": [],
            "khabarovsk_wids": [],
            "city1_wids": [],
            "city2_wids": [],
            "version": 2,
        },
    )


def _clean_warehouse_title(name: str) -> str:
    """
    Чистим служебные префиксы/артефакты в названиях складов:
      • 'ID 12345 — ...', '(ID:12345)', 'wh:23843917228000', 'WH-238...' и т.п.
      • если после чистки пусто, возвращаем человекочитаемый фолбэк «Склад N».
    """
    import re

    src = str(name or "")
    s = src
    try:
        # Удаляем конструкции вида "ID 12345 —", "(ID: 12345)", "wh: 12345" в любом месте
        s = re.sub(r"^\s*(?:ID|Id|id)\s*[:#\-]?\s*\d+\s*[–—-]?\s*", "", s)
        s = re.sub(r"\(\s*(?:ID|Id|id)\s*[:#\-]?\s*\d+\s*\)\s*", "", s)
        s = re.sub(r"\bwh\s*[:#-]?\s*\d+\b", "", s, flags=re.I)
        s = re.sub(r"^\s*wh\s*[:#-]?\s*\d+\s*[–—-]?\s*", "", s, flags=re.I)

        # Приводим дефисы и пробелы
        s = re.sub(r"\s{2,}", " ", s).strip(" -–—•\t\r\n")

        if s:
            return s

        # Фолбэк: если исходник был вида "wh:2384..." или "ID 123", подставим "Склад N"
        m = re.search(r"(?:wh\s*[:#-]?\s*|(?:ID|Id|id)\s*[:#-]?\s*)(\d+)", src, flags=re.I)
        if m:
            return f"Склад {m.group(1)}"

        return src
    except Exception:
        return src


def list_warehouses_for_dist() -> List[Tuple[int, str]]:
    """
    Список складов для «⚙️ Распределение»:
      • только по складам с ΣD/день > 0 (за текущий период из /warehouse),
      • исключаем drop‑off,
      • отображаем как «Кластер - Склад» (как в «Необходимо отгрузить — 🚫 Закрытые склады»),
      • источник имён — leadtime (fallback: stocks/report_data),
      • без дубликатов, сортировка по названию.
    """

    # вспомогательный геттер stocks-рядов с надёжным фолбэком
    def _stocks_rows(view: str) -> List[dict]:
        rows: List[dict] = []
        try:
            r = fetch_stocks_view(view=view, force=False) or []
            if r:
                rows = r
            else:
                r = fetch_stocks_view(view=view, force=True) or []
                if r:
                    rows = r
        except Exception:
            rows = []
        if rows:
            return rows
        # доп. фолбэк на report_data
        try:
            from modules_shipments.shipments_report_data import fetch_stocks_view as _sv2  # type: ignore

            r2 = _sv2(view=view, force=False) or []
            if not r2:
                r2 = _sv2(view=view, force=True) or []
            return r2 or []
        except Exception:
            return []

    # 0) Ограничивающий набор по спросу ΣD>0
    pos_wids_set: Optional[set[int]]
    try:
        from modules_shipments.shipments_demand import get_positive_demand_wids  # type: ignore

        _lst = get_positive_demand_wids() or []
        pos_wids_set = {int(w) for w in _lst if w is not None}
    except Exception:
        pos_wids_set = None  # при ошибке не режем по спросу

    # 1) Drop‑off
    try:
        drop = {int(w) for w in (_lt_get_dropoff_warehouses() or {}).keys()}  # type: ignore
    except Exception:
        drop = set()

    # 2) Карты кластеров и имён кластеров
    def _w2c_map() -> Dict[int, int]:
        try:
            from modules_shipments.shipments_demand import map_warehouse_to_cluster  # type: ignore

            return {int(w): int(c) for w, c in (map_warehouse_to_cluster() or {}).items()}
        except Exception:
            pass
        w2c: Dict[int, int] = {}
        for r in _stocks_rows("warehouse"):
            try:
                wid = int(r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id") or 0)
                cid = int(r.get("cluster_id") or 0)
                if wid and cid:
                    w2c[wid] = cid
            except Exception:
                continue
        return w2c

    def _cid2name() -> Dict[int, str]:
        # 1) Из «потребность» (надёжная карта имён)
        try:
            from modules_shipments.shipments_demand_data import _cluster_name_map as _cl  # type: ignore

            m = _cl() or {}
            return {int(k): str(v) for k, v in m.items()}
        except Exception:
            pass
        # 2) Из stocks cluster‑вида
        out: Dict[int, str] = {}
        for r in _stocks_rows("cluster"):
            try:
                cid = int(r.get("cluster_id") or r.get("id") or 0)
                cname = str(r.get("cluster_name") or r.get("name") or r.get("title") or "").strip()
                if cid and cname:
                    out[cid] = cname
            except Exception:
                continue
        return out

    w2c = _w2c_map()
    cid2name = _cid2name()

    # 3) Базовый перечень складов
    base_map: Dict[int, str] = {}
    if callable(_lt_get_current_warehouses):  # type: ignore
        try:
            base_map = {
                int(w): str(nm) for w, nm in (_lt_get_current_warehouses() or {}).items()
            }  # type: ignore
        except Exception:
            base_map = {}

    if not base_map:
        for r in _stocks_rows("warehouse"):
            try:
                wid = (
                    r.get("warehouse_id")
                    or r.get("warehouseId")
                    or (r.get("warehouse") or {}).get("id")
                    or r.get("id")
                )
                wid = int(wid)
                name = (
                    r.get("warehouse_name")
                    or r.get("warehouseName")
                    or (r.get("warehouse") or {}).get("name")
                    or (r.get("warehouse") or {}).get("title")
                    or r.get("name")
                    or r.get("title")
                    or r.get("display_name")
                    or r.get("displayName")
                    or f"Склад {wid}"
                )
                base_map[int(wid)] = _clean_warehouse_title(str(name))
            except Exception:
                continue

    # 4) Собираем финальный список с фильтрами и форматированием
    out: List[Tuple[int, str]] = []
    seen: set[int] = set()
    has_pos_filter = pos_wids_set is not None and len(pos_wids_set) > 0

    for wid, wname in base_map.items():
        try:
            wid_i = int(wid)
        except Exception:
            continue
        if wid_i in seen:
            continue
        if wid_i in drop:
            continue
        if has_pos_filter and wid_i not in pos_wids_set:
            continue

        cname = cid2name.get(int(w2c.get(wid_i, 0)), "")
        title = f"{cname} - {
            _clean_warehouse_title(wname)}" if cname else _clean_warehouse_title(wname)
        out.append((wid_i, title))
        seen.add(wid_i)

    # Fallback, если спрос ещё не прогрет
    if not out:
        for wid, wname in base_map.items():
            try:
                wid_i = int(wid)
            except Exception:
                continue
            if wid_i in seen or wid_i in drop:
                continue
            cname = cid2name.get(int(w2c.get(wid_i, 0)), "")
            title = f"{cname} - {
                _clean_warehouse_title(wname)}" if cname else _clean_warehouse_title(wname)
            out.append((wid_i, title))
            seen.add(wid_i)

    out.sort(key=lambda t: (str(t[1]).lower(), t[0]))
    return out


# ─────────────────────────────────────────────────────────────
#                   ΣD/день по складам — из блока «Отгрузки»
# ─────────────────────────────────────────────────────────────
try:
    from modules_shipments import (  # type: ignore
        compute_D_average,
        compute_D_dynamics,
        compute_D_plan_distribution,
        compute_D_hybrid,
    )
except Exception:
    compute_D_average = compute_D_dynamics = compute_D_plan_distribution = compute_D_hybrid = None  # type: ignore

try:
    from modules_shipments.shipments_demand import get_demand_settings  # type: ignore
except Exception:

    def get_demand_settings() -> dict:  # type: ignore
        m = (os.getenv("DEMAND_METHOD") or "hybrid").strip().lower()
        p = int(os.getenv("DEMAND_PERIOD") or "180")
        return {"method": m, "period": p}


_DEMAND_LOCAL_TTL_SEC = int(os.getenv("DEMAND_LOCAL_TTL_SEC", "600"))
_D_CACHE_KEY: Optional[Tuple[str, int]] = None
_D_CACHE_TS: float = 0.0
_D_CACHE_DATA: Dict[Tuple[int, int], float] = {}  # {(wid, sku): D}


def _select_and_compute_D_ws(method: str, period_days: int) -> Dict[Tuple[int, int], float]:
    """
    Универсальный вызов compute_D_* из блока «Потребность», нормализуем к {(wid, sku): D}.
    """
    m = (method or "").strip().lower()
    days = max(1, int(period_days))

    fn = None
    if m == "average":
        fn = compute_D_average
    elif m == "dynamics":
        fn = compute_D_dynamics
    elif m in ("plan_distribution", "plandistr"):
        fn = compute_D_plan_distribution
        days = max(30, days)
    else:
        fn = compute_D_hybrid

    if fn is None:
        return {}

    try:
        payload = fn(view="warehouse", period=days)  # type: ignore[arg-type]
    except TypeError:
        try:
            payload = fn(days)  # type: ignore[call-arg]
        except Exception:
            return {}
    except Exception:
        return {}

    if isinstance(payload, dict) and payload:
        try:
            if all(isinstance(k, tuple) and len(k) == 2 for k in payload.keys()):
                out: Dict[Tuple[int, int], float] = {}
                for (wid, sku), d in payload.items():
                    try:
                        out[(int(wid), int(sku))] = float(d or 0.0)
                    except Exception:
                        continue
                return out
        except Exception:
            pass

        mapping = None
        if "D_by_w_sku" in payload and isinstance(payload["D_by_w_sku"], dict):
            mapping = payload["D_by_w_sku"]
        elif "D_by_ws" in payload and isinstance(payload["D_by_ws"], dict):
            mapping = payload["D_by_ws"]

        if isinstance(mapping, dict):
            out2: Dict[Tuple[int, int], float] = {}
            for k, d in mapping.items():
                try:
                    if isinstance(k, (tuple, list)) and len(k) == 2:
                        wid, sku = k
                    elif isinstance(k, str) and ":" in k:
                        wid, sku = k.split(":", 1)
                    else:
                        continue
                    out2[(int(wid), int(sku))] = float(d or 0.0)
                except Exception:
                    continue
            return out2

    return {}


def _get_D_ws_cached() -> Dict[Tuple[int, int], float]:
    global _D_CACHE_KEY, _D_CACHE_TS, _D_CACHE_DATA
    settings = get_demand_settings() or {}
    key = (
        str(settings.get("method") or "hybrid").strip().lower(),
        int(settings.get("period") or 180),
    )
    now = time.time()
    if _D_CACHE_KEY == key and (now - _D_CACHE_TS) <= _DEMAND_LOCAL_TTL_SEC and _D_CACHE_DATA:
        return _D_CACHE_DATA
    try:
        data = _select_and_compute_D_ws(key[0], key[1]) or {}
    except Exception:
        try:
            data = _select_and_compute_D_ws("hybrid", 90) or {}
        except Exception:
            data = {}
    _D_CACHE_KEY = key
    _D_CACHE_TS = now
    _D_CACHE_DATA = data
    return _D_CACHE_DATA


def get_needs_by_warehouse_total() -> Dict[int, float]:
    D_ws = _get_D_ws_cached() or {}
    out: Dict[int, float] = {}
    for (wid, _sku), d in D_ws.items():
        try:
            w = int(wid)
            out[w] = out.get(w, 0.0) + float(max(0.0, d or 0.0))
        except Exception:
            continue
    return out


def get_needs_by_warehouse_for_sku(sku: int) -> Optional[Dict[int, float]]:
    try:
        s = int(sku)
    except Exception:
        return None
    D_ws = _get_D_ws_cached() or {}
    found = False
    out: Dict[int, float] = {}
    for (wid, s2), d in D_ws.items():
        try:
            if int(s2) != s:
                continue
            found = True
            w = int(wid)
            out[w] = out.get(w, 0.0) + float(max(0.0, d or 0.0))
        except Exception:
            continue
    return out if found else None


# ─────────────────────────────────────────────────────────────
#                    BUY_COEF — единая точка чтения
# ─────────────────────────────────────────────────────────────
def _buy_coef() -> float:
    val = os.getenv("BUY_COEF")
    if val is not None:
        try:
            x = float(val)
            return x if x > 0 else 1.0
        except Exception:
            return 1.0
    try:
        step = int(os.getenv("BUY_ROUND_STEP", "1"))
        return float(step) if step >= 1 else 1.0
    except Exception:
        return 1.0


BUY_COEF = _buy_coef()

__all__ = [
    "fetch_seller_rows",
    "fetch_ozon_platform_totals",
    "fetch_plan_units",
    "get_forecast_method_title",
    # городская конфигурация
    "get_city_config",
    # настройки распределения
    "load_dist_prefs",
    "save_dist_prefs",
    "reset_dist_prefs",
    "list_warehouses_for_dist",
    # ΣD/день
    "get_needs_by_warehouse_for_sku",
    "get_needs_by_warehouse_total",
    # коэффициент
    "BUY_COEF",
]
