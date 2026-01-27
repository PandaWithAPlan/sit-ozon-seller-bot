# modules_purchases/__init__.py
"""
Инициализация пакета modules_purchases.

Важно:
- Не тянуть «тяжёлые» зависимости при импорте пакета.
- Экспортируемые сущности получаем лениво (прокси‑функции и __getattr__).
"""

from __future__ import annotations
from typing import Any, Callable

__all__ = [
    # Тексты раздела «Выкупы»
    "need_to_purchase_text",  # прокси-обёртка (ленивый импорт)
    "purchases_status_text",  # из .purchases_report (fallback на inprogress_text)
    "vrc_calc_start_text",  # из .purchases_calc
    "vrc_calc_distribution_text",  # из .purchases_calc
    # Данные/метаданные
    "fetch_seller_rows",
    "fetch_ozon_platform_totals",
    "fetch_plan_units",
    "get_forecast_method_title",
    # Константы/настройки
    "BUY_COEF",
    # Шаблон Excel
    "ensure_purchases_template",
    "get_purchases_template_path",
]

# ─────────────────────────────────────────────────────────────────────────────
# Прокси‑функции (легковесные, без «тяжёлого» импорта на этапе init)
# ─────────────────────────────────────────────────────────────────────────────


def need_to_purchase_text(*args, **kwargs):
    """
    Ленивый импорт реальной реализации (в .purchases_need) при первом вызове.
    В случае ошибки возвращаем безопасный текст.
    """
    try:
        from .purchases_need import need_to_purchase_text as _impl

        return _impl(*args, **kwargs)
    except Exception as e:
        import traceback

        print("[modules_purchases] failed to import purchases_need.need_to_purchase_text:", e)
        traceback.print_exc()
        return (
            "📦 ВЫКУПЫ — РЕКОМЕНДАЦИИ\n"
            "⚠️ Не удалось загрузить расчётный модуль «Необходимо закупить». "
            "Проверьте логи приложения.\n"
        )


def ensure_purchases_template(*args, **kwargs) -> str:
    """Ленивый прокси генератора Excel‑шаблона (создаёт/пересобирает файл на диске)."""
    from .purchases_report_data import ensure_purchases_template as _impl

    return _impl(*args, **kwargs)


def get_purchases_template_path() -> str:
    """Возвращает путь к текущему шаблону (без генерации)."""
    from .purchases_report_data import TEMPLATE_PATH as _path

    return _path


# ─────────────────────────────────────────────────────────────────────────────
# Остальные атрибуты — через __getattr__ (PEP 562)
# ─────────────────────────────────────────────────────────────────────────────


def __getattr__(name: str) -> Any:
    # Константа из purchases_need_data — импорт по требованию
    if name == "BUY_COEF":
        from .purchases_need_data import BUY_COEF as _coef

        return _coef

    # Тексты других подпунктов/репортов
    if name == "purchases_status_text":
        # Сначала современное имя, затем — обратная совместимость со старым inprogress_text
        try:
            from .purchases_report import purchases_status_text as _impl  # type: ignore

            return _impl
        except Exception:
            try:
                from .purchases_report import inprogress_text as _impl  # type: ignore

                return _impl
            except Exception:
                # Безопасный плейсхолдер, чтобы бот не падал
                def _fallback(*_a, **_kw) -> str:
                    return (
                        "🏷️ Статус выкупов — недоступно.\n"
                        "Модуль отчёта не найден (ни purchases_status_text, ни inprogress_text).\n"
                    )

                return _fallback

    if name == "vrc_calc_start_text":
        from .purchases_calc import calc_start_text as _impl  # type: ignore

        return _impl

    if name == "vrc_calc_distribution_text":
        from .purchases_calc import calc_distribution_text as _impl  # type: ignore

        return _impl

    # Данные/метаданные для «Выкупов»
    if name in (
        "fetch_seller_rows",
        "fetch_ozon_platform_totals",
        "fetch_plan_units",
        "get_forecast_method_title",
    ):
        from .purchases_need_data import (  # type: ignore
            fetch_seller_rows as _fs,
            fetch_ozon_platform_totals as _fo,
            fetch_plan_units as _fp,
            get_forecast_method_title as _fmt,
        )

        mapping: dict[str, Callable[..., Any]] = {
            "fetch_seller_rows": _fs,
            "fetch_ozon_platform_totals": _fo,
            "fetch_plan_units": _fp,
            "get_forecast_method_title": _fmt,
        }
        return mapping[name]

    raise AttributeError(name)
