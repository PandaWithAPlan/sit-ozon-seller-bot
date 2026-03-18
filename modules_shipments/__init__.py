# modules_shipments/__init__.py
"""
Инициализация пакета modules_shipments
Обеспечивает единый API для всех разделов блока 📦 Отгрузки.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Optional

log = logging.getLogger(__name__)


def _try_import(submod: str) -> Optional[Any]:
    base_pkg = "modules_shipments"
    full_name = f"{base_pkg}.{submod}"
    try:
        return importlib.import_module(full_name)
    except ModuleNotFoundError:
        log.info("modules_shipments: optional submodule '%s' is not found (ok)", full_name)
        return None
    except Exception:
        log.exception("modules_shipments: failed to import optional submodule '%s'", full_name)
        return None


# 🚚 Статус отгрузок — ленивое разрешение провайдера
_STATUS_MOD: Optional[Any] = None


def _resolve_status_module(refresh: bool = False) -> Optional[Any]:
    global _STATUS_MOD
    if not refresh and _STATUS_MOD and hasattr(_STATUS_MOD, "shipments_status_text"):
        return _STATUS_MOD
    for sub in (
        "shipments_report",
        "shipments_report_data",
        "status_report",
        "shipments_status",
        "shipments_status_report",
    ):
        mod = _try_import(sub)
        if mod and hasattr(mod, "shipments_status_text"):
            _STATUS_MOD = mod
            return mod
    return None


def shipments_status_text(*args, **kwargs) -> str:
    mod = _resolve_status_module()
    if mod and hasattr(mod, "shipments_status_text"):
        try:
            return mod.shipments_status_text(*args, **kwargs)  # type: ignore[attr-defined]
        except Exception:
            log.exception(
                "shipments_status_text: provider crashed, falling back to shipments_report_data"
            )
    try:
        from .shipments_report_data import shipments_status_text as _fallback  # type: ignore

        return _fallback(*args, **kwargs)
    except Exception:
        return (
            "⚠️ Раздел «Статус отгрузок» временно недоступен: отсутствует модуль отчёта.\n"
            "Добавьте файл <code>modules_shipments/shipments_report.py</code> (или эквивалент) и повторите запрос."
        )


def shipments_status_text_group(*args, **kwargs) -> str:
    mod = _resolve_status_module()
    if mod and hasattr(mod, "shipments_status_text_group"):
        try:
            return mod.shipments_status_text_group(*args, **kwargs)  # type: ignore[attr-defined]
        except Exception:
            log.exception(
                "shipments_status_text_group: provider crashed, falling back to shipments_report_data"
            )
    try:
        from .shipments_report_data import shipments_status_text_group as _fallback  # type: ignore

        return _fallback(*args, **kwargs)
    except Exception:
        return "⚠️ Групповой отчёт по статусу отгрузок недоступен — отсутствует модуль отчёта."


# 📊 Необходимо отгрузить
try:
    from .shipments_need import (  # type: ignore
        compute_need,
        format_need_text,
        export_need_excel,
    )

    def compute_need_summary() -> dict:
        try:
            payload = compute_need("sku")
            groups = payload.get("groups") or {}
            return {
                "total_sku": len(payload.get("lines") or []),
                "deficit": len(groups.get("DEFICIT", [])),
                "ok": len(groups.get("ENOUGH", [])),
                "surplus": len(groups.get("SURPLUS", [])),
                "updated_at": payload.get("updated_at"),
            }
        except Exception:
            return {"total_sku": 0, "deficit": 0, "ok": 0, "surplus": 0, "updated_at": None}

except Exception:

    def compute_need(*_a, **_k) -> dict:  # type: ignore
        return {
            "updated_at": None,
            "scope": "sku",
            "lines": [],
            "groups": {"DEFICIT": [], "ENOUGH": [], "SURPLUS": []},
        }

    def format_need_text(_payload: dict) -> str:  # type: ignore
        return "📊 Необходимо отгрузить\nДанные временно недоступны."

    def export_need_excel(*_a, **_k) -> str:  # type: ignore
        return ""

    def compute_need_summary() -> dict:  # type: ignore
        return {"total_sku": 0, "deficit": 0, "ok": 0, "surplus": 0, "updated_at": None}


# 📁 Экспорт Excel «Отгрузки»
try:
    from .shipments_export import export_shipments_report  # type: ignore
except Exception:

    def export_shipments_report(*_a, **_k) -> str:  # type: ignore
        return ""


# ⏰ Lead Days (ручной ввод сроков доставки)
try:
    from .shipments_leadtime import (  # type: ignore
        LEAD_EDIT_PAGE_SIZE,
        get_progress,
        list_warehouses_page,
        get_warehouse_title,
        save_lead_days,
        reset_lead_days,
        delete_lead_record,
    )

    def derive_cluster_lead_map(*_a, **_k):  # type: ignore
        return {}
except Exception:
    LEAD_EDIT_PAGE_SIZE = 20  # type: ignore

    def get_progress(*_a, **_k):  # type: ignore
        return {"done": 0, "total": 0}

    def list_warehouses_page(*_a, **_k):  # type: ignore
        return []

    def get_warehouse_title(*_a, **_k):  # type: ignore
        return "Склад"

    def save_lead_days(*_a, **_k):  # type: ignore
        return None

    def reset_lead_days(*_a, **_k):  # type: ignore
        return None

    def delete_lead_record(*_a, **_k):  # type: ignore
        return None

    def derive_cluster_lead_map(*_a, **_k):  # type: ignore
        return {}


# ⏱ Lead-time статистика (агрегаты) — с фолбэками
try:
    from .shipments_leadtime_stats import (  # type: ignore
        get_lead_stats_by_sku,
        get_lead_stats_by_cluster,
        get_lead_stats_by_warehouse,
        get_lead_stats_summary,
        set_lead_allocation_flag,
        get_lead_allocation_flag,
    )
except Exception:

    def get_lead_stats_by_sku(*args, **kwargs):
        return []

    def get_lead_stats_by_cluster(*args, **kwargs):
        return []

    def get_lead_stats_by_warehouse(*args, **kwargs):
        return []

    def get_lead_stats_summary(*args, **kwargs):
        return {"avg": 0.0, "p50": 0.0, "p90": 0.0, "n": 0.0}

    def set_lead_allocation_flag(*args, **kwargs):
        return None

    def get_lead_allocation_flag(*args, **kwargs):
        return True


# 🧮 Потребность / спрос — ленивые прокси


def _lazy_demand_modules():
    dd = _try_import("shipments_demand_data")
    dm = _try_import("shipments_demand")
    return dd, dm


def fetch_sales_view(*args, **kwargs):
    dd, _ = _lazy_demand_modules()
    if dd and hasattr(dd, "fetch_sales_view"):
        return dd.fetch_sales_view(*args, **kwargs)  # type: ignore[attr-defined]
    return []


def _dm_call(name: str, *args, **kwargs):
    _, dm = _lazy_demand_modules()
    if dm and hasattr(dm, name):
        return getattr(dm, name)(*args, **kwargs)  # type: ignore[misc]
    if name.startswith("compute_"):
        return {}
    if name.startswith("rows_by_"):
        return []
    if name == "aggregate_to_cluster" or name == "aggregate_to_sku":
        return []
    if name == "export_excel":
        return ""
    return None


def compute_D_average(*args, **kwargs):
    return _dm_call("compute_D_average", *args, **kwargs)


def compute_D_dynamics(*args, **kwargs):
    return _dm_call("compute_D_dynamics", *args, **kwargs)


def compute_D_plan_distribution(*args, **kwargs):
    return _dm_call("compute_D_plan_distribution", *args, **kwargs)


def compute_D_hybrid(*args, **kwargs):
    return _dm_call("compute_D_hybrid", *args, **kwargs)


def aggregate_to_cluster(*args, **kwargs):
    return _dm_call("aggregate_to_cluster", *args, **kwargs)


def aggregate_to_sku(*args, **kwargs):
    return _dm_call("aggregate_to_sku", *args, **kwargs)


def rows_by_warehouse(*args, **kwargs):
    return _dm_call("rows_by_warehouse", *args, **kwargs)


def rows_by_cluster(*args, **kwargs):
    return _dm_call("rows_by_cluster", *args, **kwargs)


def rows_by_sku(*args, **kwargs):
    return _dm_call("rows_by_sku", *args, **kwargs)


def export_excel(*args, **kwargs):
    return _dm_call("export_excel", *args, **kwargs)


def leadtime_load_xlsx(_filepath: str):
    raise NotImplementedError("Lead Days: импорт из Excel больше не поддерживается.")


def leadtime_dump_xlsx(_filepath: str):
    raise NotImplementedError("Lead Days: экспорт в Excel больше не поддерживается.")


def leadtime_required_clusters():
    try:
        from .shipments_leadtime import get_warehouse_cluster_map, get_all_leads  # type: ignore

        wid_to_cluster = get_warehouse_cluster_map()
        leads = get_all_leads()
        clusters: dict[int, list[int]] = {}
        for wid, cid in wid_to_cluster.items():
            clusters.setdefault(cid, []).append(wid)
        missing = []
        for cid, wids in clusters.items():
            if not any(wid in leads for wid in wids):
                missing.append(cid)
        try:
            missing.sort()
        except Exception:
            pass
        return missing
    except Exception:
        return []


def leadtime_is_complete() -> bool:
    try:
        return len(leadtime_required_clusters()) == 0
    except Exception:
        return True


__all__ = [
    "shipments_status_text",
    "shipments_status_text_group",
    "compute_need",
    "format_need_text",
    "export_need_excel",
    "compute_need_summary",
    "export_shipments_report",
    "LEAD_EDIT_PAGE_SIZE",
    "get_progress",
    "list_warehouses_page",
    "get_warehouse_title",
    "save_lead_days",
    "reset_lead_days",
    "delete_lead_record",
    "derive_cluster_lead_map",
    "get_lead_stats_by_sku",
    "get_lead_stats_by_cluster",
    "get_lead_stats_by_warehouse",
    "get_lead_stats_summary",
    "set_lead_allocation_flag",
    "get_lead_allocation_flag",
    "fetch_sales_view",
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
    "leadtime_load_xlsx",
    "leadtime_dump_xlsx",
    "leadtime_required_clusters",
    "leadtime_is_complete",
]
