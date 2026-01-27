"""
Роутер для команды /warehouse и управления настройками складов.
"""

from modules_common.ui import build_warehouse_kb, WH_METHOD_TITLES, WH_PERIODS, get_wh_prefs
from modules_common.cache_manager import WarehouseCache
from scheduler import register_notice_chat

import logging
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest

# Логирование
log = logging.getLogger("seller-bot.warehouse_router")

# Константы для настроек
WH_METHODS = tuple(WH_METHOD_TITLES.keys())

# Создаем роутер
warehouse_router = Router(name="warehouse")

def _save_wh_global(method: str, period: int) -> None:
    """Сохранение настроек склада."""
    payload = {"method": method, "period": int(period)}
    WarehouseCache.get_prefs_manager().set_data(payload)
    log.info(f"Warehouse preferences saved: {payload}")


# ==================== /warehouse ====================
@warehouse_router.message(Command("warehouse"))
async def on_warehouse(message: Message, state: FSMContext):
    """
    Обработчик команды /warehouse.
    Показывает меню управления настройками потребности складов.
    """
    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /warehouse")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    method, period = get_wh_prefs()
    await state.update_data(wh_method=method, wh_period=period)

    txt = (
        "🏬 <b>Методика расчёта потребности по складам/кластерам</b>\n"
        f"Текущие глобальные настройки:\n"
        f"• Метод: <b>{WH_METHOD_TITLES.get(method, method)}</b>\n"
        f"• Период: <b>{period} дн.</b>\n\n"
        "Измените метод и/или период — изменения сохраняются сразу для всех пользователей."
    )

    keyboard = build_warehouse_kb(method, period)
    await message.answer(txt, reply_markup=keyboard)


# ==================== Callback: выбор метода ====================
@warehouse_router.callback_query(F.data.startswith("wh:method:set:"))
async def wh_set_method(cb: CallbackQuery, state: FSMContext):
    """Обработчик выбора метода потребности."""
    try:
        await cb.answer()
    except Exception:
        pass

    method = cb.data.split(":")[-1]
    if method not in WH_METHODS:
        method = "average"

    data = await state.get_data()
    period = int(data.get("wh_period", 90))
    _save_wh_global(method, period)
    await state.update_data(wh_method=method)

    try:
        await cb.message.edit_reply_markup(reply_markup=build_warehouse_kb(method, period))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            log.warning(f"Failed to update warehouse method: {e}")
    except Exception as e:
        log.error(f"Unexpected error updating warehouse method: {e}", exc_info=True)


# ==================== Callback: выбор периода ====================
@warehouse_router.callback_query(F.data.startswith("wh:period:set:"))
async def wh_set_period(cb: CallbackQuery, state: FSMContext):
    """Обработчик выбора периода потребности."""
    try:
        await cb.answer()
    except Exception:
        pass

    try:
        period = int(cb.data.split(":")[-1])
    except (ValueError, IndexError):
        period = 90

    if period not in WH_PERIODS:
        period = 90

    data = await state.get_data()
    method = data.get("wh_method", "average")
    _save_wh_global(method, period)
    await state.update_data(wh_period=period)

    try:
        await cb.message.edit_reply_markup(reply_markup=build_warehouse_kb(method, period))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            log.warning(f"Failed to update warehouse period: {e}")
    except Exception as e:
        log.error(f"Unexpected error updating warehouse period: {e}", exc_info=True)
