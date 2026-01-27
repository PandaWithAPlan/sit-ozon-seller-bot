# modules_shipments/shipments_need_data.py
from __future__ import annotations

import os
import re
import json
import datetime as dt
from typing import Dict, Tuple, List, Optional, Any, DefaultDict
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────────────────
# Источники «Потребности» (нормализованные строки продаж и табличные представления)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .shipments_demand_data import (  # type: ignore
        fetch_sales_view,  # rows: {date, sku, cluster, warehouse, units}
        sales_to_D_by_sku,  # ускоренная агрегация
        rows_by_warehouse as _rows_view_warehouse,
        rows_by_cluster as _rows_view_cluster,
        rows_by_sku as _rows_view_sku,
    )
except Exception:

    def fetch_sales_view(
        view: str = "warehouse",
        days: int = 60,
        force: bool = False,
        skus: Optional[List[int]] = None,
    ) -> List[dict]:
        return []

    def sales_to_D_by_sku(_r, _p):
        return {}

    def _rows_view_warehouse(_D_ws, _p):
        return []

    def _rows_view_cluster(_D_cs, _p):
        return []

    def _rows_view_sku(_D_s, _p):
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Остатки (6 метрик) — прокси
# ВАЖНО: используем shipments_report_data, а не несуществующий shipments_analyzer.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .shipments_report_data import (
        aggregate6_by_sku as _aggregate6_by_sku,
        aggregate6_by_cluster as _aggregate6_by_cluster,
        aggregate6_by_warehouse as _aggregate6_by_warehouse,
    )
except Exception:

    def _aggregate6_by_sku(*_a, **_kw) -> Dict[int, Dict[str, float]]:
        return {}

    def _aggregate6_by_cluster(*_a, **_kw) -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
        return {}

    def _aggregate6_by_warehouse(*_a, **_kw) -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
        return {}


def get_stock6_by_sku() -> Dict[int, Dict[str, float]]:
    return _aggregate6_by_sku() or {}


def get_stock6_by_cluster() -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
    return _aggregate6_by_cluster() or {}


def get_stock6_by_warehouse() -> Dict[Tuple[int, str], Dict[int, Dict[str, float]]]:
    return _aggregate6_by_warehouse() or {}


# ─────────────────────────────────────────────────────────────────────────────
# Lead и справочники складов
# ⚠️ FIX: раньше импортировали из несуществующего `leadtime_settings`, из‑за чего L=0.
# Теперь берём из актуальных модулей: сначала shipments_leadtime_data, затем shipments_leadtime.
# ─────────────────────────────────────────────────────────────────────────────
try:
    from .shipments_leadtime_data import get_lead_for_wid as _get_lead_for_wid  # type: ignore
except Exception:
    try:
        from .shipments_leadtime import get_lead_for_wid as _get_lead_for_wid  # type: ignore
    except Exception:

        def _get_lead_for_wid(_wid: int) -> Optional[float]:
            return None  # type: ignore


def get_lead_for_wid(wid: int) -> int:
    try:
        v = _get_lead_for_wid(int(wid))  # type: ignore
        return int(float(v or 0))
    except Exception:
        return 0


# ⬇️ Тянем список складов из shipments_report_data (а не из устаревшего shipments_data)
try:
    from .shipments_report_data import fetch_stocks_view  # type: ignore
except Exception:

    def fetch_stocks_view(view: str = "warehouse", force: bool = False) -> List[dict]:
        return []


def _clean_warehouse_title(name: str) -> str:
    """
    Убираем служебные префиксы вида `ID 12345 — ...`, `(ID:12345)` и лишние разделители.
    Если после очистки строка пуста — возвращаем исходное имя.
    """
    src = str(name or "")
    s = src
    try:
        # Префикс "ID 12345 -"
        s = re.sub(r"^\s*(?:ID|Id|id)\s*[:\-]?\s*\d+\s*[–—-]?\s*", "", s)
        # Вставки "(ID:12345)"
        s = re.sub(r"\(\s*(?:ID|Id|id)\s*[:\-]?\s*\d+\s*\)\s*", "", s)
        # Префикс "wh:12345 -"
        s = re.sub(r"^\s*wh:\s*\d+\s*[–—-]?\s*", "", s, flags=re.I)
        # Сжать пробелы/разделители
        s = re.sub(r"\s{2,}", " ", s).strip(" -–—•\t\r\n")
        return s or src
    except Exception:
        return src


def get_warehouses_map() -> Dict[int, Tuple[str, int, str]]:
    """
    wid -> (warehouse_name, cluster_id, cluster_name)
    Подстраховываемся: если первый вызов ничего не вернул — пробуем с force=True.
    Названия очищаем от служебных ID, чтобы в «🚫 Закрытые склады» не было «ID 123…».
    """
    out: Dict[int, Tuple[str, int, str]] = {}
    rows = fetch_stocks_view(view="warehouse") or []
    if not rows:
        rows = fetch_stocks_view(view="warehouse", force=True) or []
    for r in rows:
        try:
            wid = int(r.get("warehouse_id") or (r.get("dimensions") or [{}])[0].get("id"))
            raw_name = r.get("warehouse_name") or f"wh:{wid}"
            wname = _clean_warehouse_title(str(raw_name))
            cid = int(r.get("cluster_id") or 0)
            cname = str(r.get("cluster_name") or (f"C{cid}" if cid else "C?"))
            out[wid] = (wname.strip(), cid, cname.strip())
        except Exception:
            continue
    return out


# ─────────────────────────────────────────────────────────────────────────────
# WATCH_SKU и утилиты
# ─────────────────────────────────────────────────────────────────────────────


def _watch_skus() -> List[int]:
    raw = (os.getenv("WATCH_SKU", "") or "").replace("\n", ",").replace(" ", ",")
    out: List[int] = []
    for t in raw.split(","):
        t = t.strip()
        if not t:
            continue
        try:
            out.append(int(t.split(":")[0]))
        except Exception:
            continue
    seen, uniq = set(), []
    for s in out:
        if s not in seen:
            seen.add(s)
            uniq.append(s)
    return uniq


def _as_int(x: Any) -> Optional[int]:
    try:
        return int(str(x).strip())
    except Exception:
        return None


def _units_from_row(r: dict) -> float:
    """
    Извлекаем количество единиц из разных ключей (унификация источников).
    """
    for k in ("units", "ordered_units", "qty", "quantity"):
        if k in r and r[k] is not None:
            try:
                return float(str(r[k]).replace(",", "."))
            except Exception:
                continue
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# /warehouse настройки — единый JSON
# ─────────────────────────────────────────────────────────────────────────────


def _resolve_warehouse_method_and_period(default_period: int) -> Tuple[str, int]:
    MOD_DIR = os.path.dirname(__file__)
    P1 = os.path.join(MOD_DIR, "data", "cache", "common", "warehouse_prefs.json")
    # fallback — если модуль используется «снаружи»
    P2 = os.path.join(
        os.path.abspath(os.path.join(MOD_DIR, "..")),
        "modules_shipments",
        "data",
        "cache",
        "common",
        "warehouse_prefs.json",
    )

    def _read(p: str) -> dict:
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    import json

                    return json.load(f) or {}
        except Exception:
            pass
        return {}

    d = _read(P1) or _read(P2)
    method = (d.get("method") or "").strip().lower()
    try:
        period = int(d.get("period"))
    except Exception:
        period = None

    if method not in {"average", "dynamics", "hybrid", "plan_distribution"}:
        method = (os.getenv("DEMAND_METHOD") or os.getenv("WH_METHOD") or "hybrid").strip().lower()
        if method not in {"average", "dynamics", "hybrid", "plan_distribution"}:
            method = "hybrid"

    if not period or period <= 0:
        try:
            period = int(
                os.getenv("DEMAND_PERIOD") or os.getenv("WH_PERIOD") or str(default_period)
            )
        except Exception:
            period = default_period

    return method, max(1, int(period))


# ✅ Публичный геттер


def get_demand_settings(default_period: int = 30) -> Tuple[str, int]:
    return _resolve_warehouse_method_and_period(default_period)


# ─────────────────────────────────────────────────────────────────────────────
# Методики расчёта D
# ─────────────────────────────────────────────────────────────────────────────
WID = int
CID = int
SKU = int


def compute_D_average(period_days: int) -> Dict[Tuple[WID, SKU], float]:
    rows = fetch_sales_view(view="warehouse", days=period_days, force=False)
    agg: DefaultDict[Tuple[int, int], float] = defaultdict(float)
    for r in rows:
        try:
            wid = int(r.get("wid") or r.get("warehouse_id") or 0)
            sku = int(r.get("sku") or 0)
            units = _units_from_row(r)
            if wid and sku and units > 0:
                agg[(wid, sku)] += units
        except Exception:
            continue
    days = max(1, int(period_days))
    return {k: u / days for k, u in agg.items()}


def compute_D_dynamics(period_days: int) -> Dict[Tuple[WID, SKU], float]:
    return compute_D_average(period_days)


def compute_D_hybrid(period_days: int) -> Dict[Tuple[WID, SKU], float]:
    D1, D2 = compute_D_average(period_days), compute_D_dynamics(period_days)
    keys = set(D1.keys()) | set(D2.keys())
    return {k: 0.4 * D1.get(k, 0.0) + 0.6 * D2.get(k, 0.0) for k in keys}


def compute_D_plan_distribution(period_days: int = 30) -> Dict[Tuple[WID, SKU], float]:
    D_avg = compute_D_average(max(30, period_days))
    if not D_avg:
        return {}
    try:
        from .shipments_need_data import load_plan30_by_sku  # type: ignore

        P30 = load_plan30_by_sku() or {}
    except Exception:
        P30 = {}

    w2c = {wid: cid for wid, (_wname, cid, _cname) in get_warehouses_map().items()}
    C_sum: DefaultDict[Tuple[int, int], float] = defaultdict(float)
    for (wid, sku), d in D_avg.items():
        cid = w2c.get(wid)
        if cid:
            C_sum[(cid, sku)] += d

    out: Dict[Tuple[WID, SKU], float] = {}
    for (wid, sku), d in D_avg.items():
        cid = w2c.get(wid)
        if cid is None:
            continue
        total = C_sum.get((cid, sku), 0.0)
        share = (d / total) if total > 0 else 0.0
        daily_plan = float(P30.get(sku, 0.0) or 0.0) / 30.0
        out[(wid, sku)] = daily_plan * share
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Табличные представления (По складам / кластерам / SKU)
# ─────────────────────────────────────────────────────────────────────────────


def _aggregate_to_cluster(D_ws: Dict[Tuple[WID, SKU], float]) -> Dict[Tuple[int, int], float]:
    wm = get_warehouses_map()
    agg: DefaultDict[Tuple[int, int], float] = defaultdict(float)
    for (wid, sku), d in D_ws.items():
        cid = wm.get(wid, ("", 0, ""))[1]
        if cid:
            agg[(cid, sku)] += float(d or 0.0)
    return dict(agg)


def _aggregate_to_sku(D_cs: Dict[Tuple[int, int], float]) -> Dict[int, float]:
    out: DefaultDict[int, float] = defaultdict(float)
    for (_cid, sku), d in D_cs.items():
        out[int(sku)] += float(d or 0.0)
    return dict(out)


def get_rows_by_warehouse(_params: dict | None, horizon_days: int) -> List[list]:
    """
    ✅ FIX: возвращаем корректные строки «по складам» за выбранный период.
    Раньше сюда по ошибке прокидывался D_ws, из‑за чего вьюшка ломалась.
    """
    method, period = _resolve_warehouse_method_and_period(horizon_days)
    # используем готовую вьюшку из shipments_demand_data
    return _rows_view_warehouse(period)


def get_rows_by_cluster(_params: dict | None, horizon_days: int) -> List[list]:
    """
    ✅ FIX: возвращаем корректные строки «по кластерам» за выбранный период.
    Раньше сюда по ошибке прокидывался D_cs.
    """
    method, period = _resolve_warehouse_method_and_period(horizon_days)
    return _rows_view_cluster(period)


def get_rows_by_sku(_params: dict | None, horizon_days: int) -> List[list]:
    """
    ✅ FIX: корректный вызов вьюшки «по SKU» (раньше подсовывалась мапа D_s).
    """
    method, period = _resolve_warehouse_method_and_period(horizon_days)
    return _rows_view_sku(period)


# ─────────────────────────────────────────────────────────────────────────────
# План продаж (30 дней) — КАСКАД ИСТОЧНИКОВ (с перекрытием ЦЕЛЯМИ)
# ─────────────────────────────────────────────────────────────────────────────


def load_plan30_by_sku() -> Dict[int, float]:
    """
    Возвращает план на 30 дней по SKU.

    Шаг 0 (Goal‑first): читаем цели продаж (Goal×30) и используем их для ПЕРЕКРЫТИЯ
    результата базового каскада. Таким образом:
      • если для SKU задана цель > 0 — берём её (Goal×30),
      • иначе — используем базовый план из каскада ниже.

    Базовый каскад (как раньше):
      1) Хранилище планов
      2) Прогноз продаж
      3) D×30 по методике /warehouse
      4) Аналитика за 30 дней
    """
    # ── Шаг 0: цели (Goal×30)
    goals30: Dict[int, float] = {}
    try:
        from modules_sales.sales_goal import get_goal30_by_sku  # type: ignore

        g = get_goal30_by_sku(30) or {}
        for k, v in (g.items() if isinstance(g, dict) else []):
            try:
                s = int(k)
                goals30[s] = float(v or 0.0)
            except Exception:
                continue
    except Exception:
        goals30 = {}

    # ── Базовый каскад
    base: Dict[int, float] = {}

    # 1) План-хранилище
    try:
        from modules_sales.sales_plan_store import get_plan_30_by_sku  # type: ignore

        d = get_plan_30_by_sku() or {}
        if isinstance(d, dict) and d:
            base = {int(k): float(v) for k, v in d.items()}
    except Exception:
        pass

    # 2) Прогноз
    if not base:
        try:
            from modules_sales import sales_forecast as SF  # type: ignore

            lb = int(os.getenv("FORECAST_LOOKBACK_DAYS", "180") or "180")
            series_map = {}
            for fn_name in ("_fetch_series", "fetch_series", "get_series_bulk"):
                fn = getattr(SF, fn_name, None)
                if callable(fn):
                    try:
                        series_map = fn(max(60, lb))
                    except TypeError:
                        series_map = fn(days_back=max(60, lb))
                    except Exception:
                        series_map = {}
                    break
            if not isinstance(series_map, dict):
                series_map = {}

            skus = _watch_skus() or list(series_map.keys())
            out: Dict[int, float] = {}
            for sku in skus:
                seq = series_map.get(int(sku)) or []
                if not seq:
                    continue
                try:
                    u_sum, _ = getattr(SF, "_forecast_next")(seq, 30)  # type: ignore[attr-defined]
                except Exception:
                    u_sum = 0.0
                if u_sum > 0:
                    out[int(sku)] = float(u_sum)
            if out:
                base = out
        except Exception:
            pass

    # 3) D×30 по текущей методике /warehouse
    if not base:
        try:
            method, period = _resolve_warehouse_method_and_period(30)
            if method == "plan_distribution":
                D_ws = compute_D_plan_distribution(max(30, period))
            elif method == "average":
                D_ws = compute_D_average(max(30, period))
            elif method == "dynamics":
                D_ws = compute_D_dynamics(max(30, period))
            else:
                D_ws = compute_D_hybrid(max(30, period))

            def _agg_to_cluster_local(
                D_ws_: Dict[Tuple[int, int], float],
            ) -> Dict[Tuple[int, int], float]:
                wm = get_warehouses_map()
                agg: DefaultDict[Tuple[int, int], float] = defaultdict(float)
                for (wid, sku), d in (D_ws_ or {}).items():
                    cid = wm.get(wid, ("", 0, ""))[1]
                    if cid:
                        agg[(cid, sku)] += float(d or 0.0)
                return dict(agg)

            def _agg_to_sku_local(D_cs_: Dict[Tuple[int, int], float]) -> Dict[int, float]:
                out: DefaultDict[int, float] = defaultdict(float)
                for (_cid, sku), d in (D_cs_ or {}).items():
                    out[int(sku)] += float(d or 0.0)
                return dict(out)

            D_cs = _agg_to_cluster_local(D_ws)
            D_s = _agg_to_sku_local(D_cs)
            out = {int(s): float(d * 30.0) for s, d in (D_s or {}).items() if d > 0}
            if out:
                base = out
        except Exception:
            pass

    # 4) Аналитика 30 дней
    if not base:
        try:
            from modules_sales import sales_report as SR  # type: ignore

            def _sum_units_30(sku: int) -> float:
                today = dt.date.today()
                start = today - dt.timedelta(days=29)
                rows = SR._analytics_query_rows(
                    sku, start.isoformat(), today.isoformat()
                )  # type: ignore[attr-defined]
                total_ord = total_cnc = total_ret = 0
                sku_str = str(sku)
                for r in rows or []:
                    dims = r.get("dimensions") or r.get("dimension") or []
                    if not any(
                        str(d.get("id") or d.get("value") or d.get("name")) == sku_str for d in dims
                    ):
                        continue
                    m = r.get("metrics") or []
                    if isinstance(m, list):
                        if len(m) >= 1:
                            total_ord += int(m[0] or 0)
                        if len(m) >= 2:
                            total_cnc += int(m[1] or 0)
                        if len(m) >= 3:
                            total_ret += int(m[2] or 0)
                    elif isinstance(m, dict):
                        total_ord += int(m.get("ordered_units", 0) or 0)
                        total_cnc += int(m.get("cancellations", 0) or 0)
                        total_ret += int(m.get("returns", 0) or 0)
                return float(max(0, total_ord - total_cnc - total_ret))

            skus = _watch_skus()
            if not skus:
                try:
                    pairs = SR.list_skus() or []
                    skus = [int(s) for s, _ in pairs]
                except Exception:
                    skus = []
            out = {}
            for s in skus:
                try:
                    u30 = _sum_units_30(int(s))
                    if u30 > 0:
                        out[int(s)] = float(u30)
                except Exception:
                    continue
            base = out
        except Exception:
            base = {}

    # ── Перекрытие целями (если есть)
    if goals30:
        merged: Dict[int, float] = dict(base or {})
        for s, v in goals30.items():
            try:
                merged[int(s)] = float(v or 0.0)
            except Exception:
                continue
        return merged

    return base or {}


# ─────────────────────────────────────────────────────────────────────────────
# Утилиты для шапки и закрытых складов
# ─────────────────────────────────────────────────────────────────────────────


def get_wh_method_title(method_code: str) -> str:
    code = (method_code or "").strip().lower()
    return {
        "average": "Среднесуточный спрос",
        "dynamics": "Динамика заказов",
        "hybrid": "Адаптивный гибрид",
        "plan_distribution": "Распределение плана",
    }.get(code, code or "-")


def load_closed_warehouses() -> List[int]:
    """
    Возвращает список закрытых складов.
    Основной путь: modules_shipments/data/cache/common/closed_warehouses.json
    Фолбэк:       ../data/cache/common/closed_warehouses.json
    Формат:
      {"closed":[<wid>, ...], "updated_at":"YYYY-MM-DD HH:MM"}
    """
    try:
        mod_dir = os.path.dirname(__file__)
        path_main = os.path.join(mod_dir, "data", "cache", "common", "closed_warehouses.json")
        path_fallback = os.path.join(
            os.path.abspath(os.path.join(mod_dir, "..")),
            "data",
            "cache",
            "common",
            "closed_warehouses.json",
        )

        path = path_main if os.path.exists(path_main) else path_fallback
        if not os.path.exists(path):
            return []
        with open(path, "r", encoding="utf-8") as f:
            js = json.load(f) or {}
        closed = js.get("closed") or []
        out: List[int] = []
        for x in closed:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out
    except Exception:
        return []
