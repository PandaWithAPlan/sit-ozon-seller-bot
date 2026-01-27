# modules_shipments/shipments_leadtime_stats.py
from __future__ import annotations

"""
Фасад для раздела «Статистика сроков доставки» (фаза B).

Задачи файла:
1) Экспортировать ПУБЛИЧНЫЕ функции статистики/настроек для пакета modules_shipments.
2) Не дублировать бизнес-логику: вся математика/ингест/кэш — в shipments_leadtime_stats_data.py.
3) В случае проблем с основным модулем — отдавать безопасные заглушки (нули/пусто).
"""

from typing import Dict, List, Tuple, Any
import datetime as dt

# ─────────────────────────────────────────────────────────────────────────────
# Основной путь — тонкий прокси в data_stats
# ─────────────────────────────────────────────────────────────────────────────
try:
    # ✅ фикс опечатки: корректный модуль — shipments_leadtime_stats_data
    from .shipments_leadtime_stats_data import (
        # prefs
        get_stat_period as _get_stat_period_impl,
        save_stat_period as _save_stat_period_impl,
        set_lead_allocation_flag as _set_alloc_impl,
        # maintenance
        rebuild_events_from_states as _rebuild_impl,
        invalidate_stats_cache as _invalidate_stats_impl,
        # public stats
        get_lead_stats_summary as _summary_impl,
        get_lead_stats_by_warehouse as _by_wh_impl,
        get_lead_stats_by_cluster as _by_cluster_impl,
        get_lead_stats_by_sku as _by_sku_impl,
        # drill-down
        get_lead_stats_sku_for_warehouse as _sku_for_wh_impl,
        get_lead_stats_sku_for_cluster as _sku_for_cluster_impl,
        # prefs (read)
        get_lead_allocation_flag as _alloc_flag_impl,
    )

    _HAS_IMPL = True
except Exception:
    _HAS_IMPL = False

# ─────────────────────────────────────────────────────────────────────────────
# Лёгкие заглушки (если основной модуль не загрузился)
# ─────────────────────────────────────────────────────────────────────────────
if not _HAS_IMPL:

    def _get_stat_period_impl() -> int:
        return 180

    def _save_stat_period_impl(_period: int) -> None:
        return

    def _set_alloc_impl(_flag: bool) -> None:
        return

    def _rebuild_impl() -> int:
        return 0

    def _invalidate_stats_impl() -> None:
        return

    def _summary_impl(_period_days: int | None = None) -> Dict[str, float]:
        return {"avg": 0.0, "p50": 0.0, "p90": 0.0, "n": 0.0}

    def _by_wh_impl(_period_days: int | None = None):
        return []

    def _by_cluster_impl(_period_days: int | None = None):
        return []

    def _by_sku_impl(_period_days: int | None = None):
        return []

    def _sku_for_wh_impl(_warehouse_id: int, _period_days: int | None = None):
        return []

    def _sku_for_cluster_impl(_cluster_id: int, _period_days: int | None = None):
        return []

    def _alloc_flag_impl() -> bool:
        return True


# ─────────────────────────────────────────────────────────────────────────────
# Публичные прокси (единый API пакета)
# ─────────────────────────────────────────────────────────────────────────────


def get_stat_period() -> int:
    return _get_stat_period_impl()


def save_stat_period(period: int) -> None:
    _save_stat_period_impl(period)


def set_lead_allocation_flag(flag: bool) -> None:
    _set_alloc_impl(flag)


def rebuild_events_from_states() -> int:
    """Полная регенерация событий из states с учётом текущих настроек."""
    return _rebuild_impl()


def invalidate_stats_cache() -> None:
    """Сбросить кэш статистики (пересчитается при следующем чтении)."""
    _invalidate_stats_impl()


def get_lead_stats_summary(period_days: int | None = None) -> Dict[str, float]:
    return _summary_impl(period_days)


def get_lead_stats_by_warehouse(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    return _by_wh_impl(period_days)


def get_lead_stats_by_cluster(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    return _by_cluster_impl(period_days)


def get_lead_stats_by_sku(
    period_days: int | None = None,
) -> List[Tuple[int, str, Dict[str, float]]]:
    return _by_sku_impl(period_days)


def get_lead_stats_sku_for_warehouse(
    warehouse_id: int, period_days: int | None = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    """Дрилл-даун: агрегаты по SKU внутри конкретного склада."""
    return _sku_for_wh_impl(warehouse_id, period_days)


def get_lead_stats_sku_for_cluster(
    cluster_id: int, period_days: int | None = None
) -> List[Tuple[int, str, Dict[str, float]]]:
    """Дрилл-даун: агрегаты по SKU внутри конкретного кластера."""
    return _sku_for_cluster_impl(cluster_id, period_days)


# ─────────────────────────────────────────────────────────────────────────────
# Текстовый фасад для уведомлений (новая верстка по SKU)
# ─────────────────────────────────────────────────────────────────────────────


def _alloc_phrase() -> str:
    try:
        return "учитывать вес партии" if bool(_alloc_flag_impl()) else "не учитывать вес партии"
    except Exception:
        return "учитывать вес партии"


def _now_str() -> str:
    # фикс неправильного форматирования: используем латинскую 'M' в %M
    return dt.datetime.now().strftime("%d.%m.%Y %H:%M")


def _fmt_days(x: float) -> str:
    try:
        return f"{float(x):.2f}"
    except Exception:
        return "0.00"


def _header(period: int) -> List[str]:
    return [
        "📄 Показатели сроков доставки — Σ∅/SKU",
        f"⏱ Обновлено: {_now_str()}",
        "",
        f"⚙️ Период: {int(period)} дн. • Распределение по SKU: {_alloc_phrase()}",
    ]


# Фолбэк‑имя склада (для человекочитаемого вывода), если из stocks пришёл id/wh:<id>
try:
    from .shipments_leadtime_data import get_warehouse_title as _wh_title  # type: ignore
except Exception:

    def _wh_title(wid: int) -> str:
        return f"wh:{wid}"


def _weighted_total(rows: List[Tuple[int, str, Dict[str, float]]]) -> float:
    """Взвешенное среднее по avg с весом n — только по тем SKU, что в отчёте."""
    total_n = 0.0
    total_sum = 0.0
    for _sku, _alias, m in rows or []:
        n = float((m or {}).get("n", 0) or 0.0)
        avg = float((m or {}).get("avg", 0.0) or 0.0)
        total_n += n
        total_sum += avg * n
    return (total_sum / total_n) if total_n > 0 else 0.0


def _format_sku_rows(rows: List[Tuple[int, str, Dict[str, float]]]) -> List[str]:
    out: List[str] = ["📦 По SKU:"]
    if not rows:
        out.append("ℹ️ Нет событий/поставок по SKU за выбранный период.")
        return out
    for sku, alias, m in rows:
        avg = _fmt_days((m or {}).get("avg", 0.0))
        name = (alias or str(sku)).strip()
        n = int((m or {}).get("n", 0) or 0)
        out.append(f"🔹 {name}: {avg} дн" + (f" (n={n})" if n else ""))
    return out


def _footer(total_avg: float, sku_count: int) -> List[str]:
    return ["", f"📊 ИТОГО — ∅={_fmt_days(total_avg)} дн • SKU: {int(sku_count)}"]


def leadtime_stats_text(**kwargs) -> str:
    """
    Универсальный рендер «Сроки доставки» для уведомлений.
    По умолчанию — агрегирование по SKU.
    """
    # выбор группировки
    group = "sku"
    for k in ("view", "group_by", "by", "scope"):
        v = str(kwargs.get(k) or "").strip().lower()
        if v in ("sku", "warehouse", "cluster"):
            group = v
            break
        if v in ("склад", "склады", "ware", "wh"):
            group = "warehouse"
            break
        if v in ("кластер", "кластеры"):
            group = "cluster"
            break

    # период
    period = None
    for k in ("days", "period_days", "lookback_days"):
        if k in kwargs and kwargs[k] is not None:
            try:
                v = int(kwargs[k])
                if v > 0:
                    period = v
                    break
            except Exception:
                pass
    period = period or get_stat_period()

    parts: List[str] = _header(period)

    if group == "sku":
        rows = get_lead_stats_by_sku(period) or []
        body = _format_sku_rows(rows)
        total_avg = _weighted_total(rows)
        tail = _footer(total_avg, len(rows))
        return "\n".join(parts + body + tail)

    if group == "warehouse":
        rows = get_lead_stats_by_warehouse(period) or []
        out: List[str] = ["🏭 По складам:"]
        if not rows:
            out.append("ℹ️ Нет событий/поставок по складам за выбранный период.")
        else:
            for wid, wname, m in rows:
                # человеко‑читаемое имя склада (фолбэк на локальный кэш базы сроков)
                show_name = _wh_title(int(wid)) or (wname or f"wh:{wid}")
                out.append(f"🔹 {show_name}: {_fmt_days((m or {}).get('avg', 0.0))} дн")
        summary = get_lead_stats_summary(period) or {}
        total_avg = float(summary.get("avg", 0.0) or 0.0)
        tail = _footer(total_avg, len(rows))
        return "\n".join(parts + out + tail)

    if group == "cluster":
        rows = get_lead_stats_by_cluster(period) or []
        out: List[str] = ["🏢 По кластерам:"]
        if not rows:
            out.append("ℹ️ Нет событий/поставок по кластерам за выбранный период.")
        else:
            for _cid, cname, m in rows:
                out.append(f"🔹 {cname}: {_fmt_days((m or {}).get('avg', 0.0))} дн")
        summary = get_lead_stats_summary(period) or {}
        total_avg = float(summary.get("avg", 0.0) or 0.0)
        tail = _footer(total_avg, len(rows))
        return "\n".join(parts + out + tail)

    # запасной вариант
    rows = get_lead_stats_by_sku(period) or []
    body = _format_sku_rows(rows)
    total_avg = _weighted_total(rows)
    tail = _footer(total_avg, len(rows))
    return "\n".join(parts + body + tail)


# Алиасы на тот же рендер — на случай разных интеграций


def delivery_stats_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


def lead_stats_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


def stats_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


def report_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


def leadtime_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


def leadtime_report_text(**kwargs) -> str:
    return leadtime_stats_text(**kwargs)


__all__ = [
    "get_stat_period",
    "save_stat_period",
    "set_lead_allocation_flag",
    "rebuild_events_from_states",
    "invalidate_stats_cache",
    "get_lead_stats_summary",
    "get_lead_stats_by_warehouse",
    "get_lead_stats_by_cluster",
    "get_lead_stats_by_sku",
    "get_lead_stats_sku_for_warehouse",
    "get_lead_stats_sku_for_cluster",
    # текстовые фасады
    "leadtime_stats_text",
    "delivery_stats_text",
    "lead_stats_text",
    "stats_text",
    "report_text",
    "leadtime_text",
    "leadtime_report_text",
]
