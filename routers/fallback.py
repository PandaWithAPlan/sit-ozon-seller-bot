"""
Роутер для обработки неизвестных сообщений и команды /data.
"""

from config_package import validate_sku, filter_valid_skus
import logging

from aiogram import Router, F, StateFilter
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.fsm.state import StatesGroup, State

# Логирование
log = logging.getLogger("seller-bot.fallback_router")

# Импорты из config_package

# FSM состояния из handlers_purchases
try:
    from handlers.handlers_purchases import BuyoutsUpload
except ImportError:
    # Если модуль не доступен, создаем локально
    class BuyoutsUpload(StatesGroup):
        waiting_file = State()


# Создаем роутер
fallback_router = Router(name="fallback")


# ==================== /data ====================
@fallback_router.message(Command("data"))
async def on_data(message: Message, state):
    """
    Обработчик команды /data.
    Показывает инструкцию по загрузке файла.
    """
    from bot import register_notice_chat, BACK_HOME_MENU

    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on /data")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    await state.set_state(BuyoutsUpload.waiting_file)

    hint = (
        "🗂 <b>Загрузка файла «Товары.xlsx»</b>\n\n"
        "Пришлите файл Excel <i>как документ</i> (формат .xlsx).\n"
        "Будет сохранён как <code>Товары.xlsx</code> в папку <code>data/</code>.\n\n"
        "Поддерживаемые столбцы: SKU/Артикул, Статус, Кол-во. Город (Москва/Хабаровск) можно не указывать — "
        "если указан, значения суммируются по SKU."
    )

    # Создаем клавиатуру с кнопками
    from menu import back_home_menu

    keyboard = back_home_menu()

    await message.answer(hint, reply_markup=keyboard)


# ==================== Fallback для всех остальных сообщений ====================
@fallback_router.message(StateFilter(None), ~F.text.regexp(r"^/"))
async def on_any_message(message: Message, state):
    """
    Обработчик всех остальных сообщений.
    Сбрасывает состояние и показывает главное меню.
    """
    from bot import register_notice_chat, _welcome_text, _build_main_menu_kb

    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on fallback")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    try:
        await state.clear()
    except Exception as e:
        log.warning(f"Failed to clear state: {e}")

    await message.answer(_welcome_text(), reply_markup=_build_main_menu_kb())
