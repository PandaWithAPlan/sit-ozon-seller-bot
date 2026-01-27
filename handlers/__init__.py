# handlers/__init__.py
from __future__ import annotations

import os
from time import monotonic
from typing import Any, Dict, Tuple

from aiogram import Router, F
from aiogram.types import CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# Подмодули с разделами (Продажи / Выкупы)
from .handlers_sales import router as sales_router
from .handlers_purchases import router as purchases_router

# Отгрузки: подключаем под-роутеры напрямую
from .handlers_shipments_status import router as shipments_status_router  # 📍 Статус отгрузок

# 📊 Необходимо отгрузить + 📑 Отчёт
from .handlers_shipments_need import router as shipments_need_router

# ⏰ Сроки доставки (ручной ввод)
from .handlers_shipments_leadtime import router as shipments_leadtime_router

# 📄 Статистика сроков доставки
from .handlers_shipments_leadtime_stats import router as shipments_leadtime_stats_router
from .handlers_shipments_demand import (
    router as shipments_demand_router,
)  # 🧮 Потребность по складам

# Меню для возврата
from menu import main_menu, sales_menu, buyouts_menu, shipments_menu

# ─────────────────────────────────────────────────────────────────────────────
# anti-bounce: быстрые повторы (дедупликатор кликов)
# ─────────────────────────────────────────────────────────────────────────────
_DEDUP_WINDOW_SEC: float = max(
    0.0, float(os.getenv("CB_DEDUP_WINDOW_MS", "800").strip() or "800") / 1000.0
)

_last_click: Dict[Tuple[int, str], float] = {}


def _is_rapid_duplicate(cb: CallbackQuery) -> bool:
    """
    Возвращает True, если пользователь нажал ТО ЖЕ САМОЕ (cb.data) слишком быстро подряд.
    Это снижает вероятность гонок/подвисаний и ошибок 'message is not modified'.
    """
    try:
        uid = int(cb.from_user.id) if cb.from_user else 0
        data = str(cb.data or "")
    except Exception:
        return False

    now = monotonic()
    key = (uid, data)
    last = _last_click.get(key)
    _last_click[key] = now
    if last is None:
        return False
    return (now - last) < _DEDUP_WINDOW_SEC


# ─────────────────────────────────────────────────────────────────────────────
# utils: safe ack + safe edit
# ─────────────────────────────────────────────────────────────────────────────


async def _ack(cb: CallbackQuery) -> None:
    """Подтверждаем callback, игнорируя устаревшие/дублирующие запросы."""
    try:
        await cb.answer()
    except TelegramBadRequest:
        pass
    except Exception:
        pass


async def _safe_edit(cb: CallbackQuery, text: str, **kwargs: Any) -> None:
    """
    Безопасно меняем текст сообщения (игнорируем 'message is not modified').
    """
    try:
        await cb.message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Корневой агрегирующий роутер
# ─────────────────────────────────────────────────────────────────────────────
router = Router(name="handlers_root")
router.include_router(sales_router)
router.include_router(purchases_router)

# Блок «Отгрузки» — порядок важен!
# Ставим "Статус" ПЕРЕД "Необходимо отгрузить", чтобы коллбеки
# shipments:view:* обрабатывались корректно.
router.include_router(shipments_status_router)  # ← приоритет для shipments:view:*
router.include_router(shipments_need_router)
router.include_router(shipments_leadtime_router)  # ручной ввод lead-time
router.include_router(shipments_leadtime_stats_router)  # статистика lead-time (отдельно)
router.include_router(shipments_demand_router)

# ─────────────────────────────────────────────────────────────────────────────
# Навигация
# ─────────────────────────────────────────────────────────────────────────────


@router.callback_query(F.data == "nav:home")
async def on_nav_home(cb: CallbackQuery, state: FSMContext) -> None:
    """Всегда возвращает в главное меню и очищает локальное состояние."""
    await _ack(cb)
    if _is_rapid_duplicate(cb):
        return
    try:
        await state.clear()
    except Exception:
        pass
    await state.update_data(step="home")
    await _safe_edit(cb, "🏠 Главное меню — выберите раздел:", reply_markup=main_menu())


# ✅ Корневой обработчик раздела «Отгрузки»
@router.callback_query(F.data == "shipments")
async def on_shipments_root(cb: CallbackQuery, state: FSMContext) -> None:
    """
    Показывает главное меню раздела «Отгрузки» и помечает step='ship_root',
    чтобы обработчик «Назад» корректно работал.
    Также сбрасываем вид для «Статуса отгрузок» на 'sku' — дефолт.
    """
    await _ack(cb)
    if _is_rapid_duplicate(cb):
        return
    await state.update_data(step="ship_root", ship_view="sku")
    await _safe_edit(cb, "🚚 Отгрузки — выберите действие:", reply_markup=shipments_menu())


@router.callback_query(F.data == "nav:back")
async def on_nav_back(cb: CallbackQuery, state: FSMContext) -> None:
    """
    ЕДИНЫЙ обработчик «Назад».
    Опираться на state["step"] (если есть) и на признаки активного раздела.
    """
    await _ack(cb)
    if _is_rapid_duplicate(cb):
        return

    data: dict[str, Any] = await state.get_data()
    step: str = str(data.get("step") or "")

    # Продажи
    if (
        step.startswith("facts")
        or step.startswith("plan")
        or step.startswith("sales_report")
        or step == "sales_root"
    ):
        await state.update_data(step="sales_root")
        await _safe_edit(cb, "📈 Продажи — выберите раздел:", reply_markup=sales_menu())
        return

    # Выкупы
    if step.startswith("buyouts") or step == "buyouts_root":
        await state.update_data(step="buyouts_root")
        await _safe_edit(cb, "🏷️ Выкупы — выберите действие:", reply_markup=buyouts_menu())
        return

    # Отгрузки
    if step.startswith("ship_") or step == "ship_root":
        await state.update_data(step="ship_root")
        await _safe_edit(cb, "🚚 Отгрузки — выберите действие:", reply_markup=shipments_menu())
        return

    # Потребность по складам — если открывали
    if ("demand_method" in data) or ("demand_period" in data) or ("demand_view" in data):
        await state.update_data(step="ship_root")
        await _safe_edit(cb, "🚚 Отгрузки — выберите действие:", reply_markup=shipments_menu())
        return

    # Фолбэк — главное меню
    await state.update_data(step="home")
    await _safe_edit(cb, "🏠 Главное меню — выберите раздел:", reply_markup=main_menu())


__all__ = ["router"]
