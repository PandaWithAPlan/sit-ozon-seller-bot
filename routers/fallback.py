"""
Роутер для обработки неизвестных сообщений и команды /data.
"""

import logging

from aiogram import Router, F
from aiogram.filters import StateFilter
from aiogram.types import Message

# Логирование
log = logging.getLogger("seller-bot.fallback_router")

# Создаем роутер
fallback_router = Router(name="fallback")

# ==================== Fallback для всех остальных сообщений ====================
@fallback_router.message(StateFilter(None), ~F.text.regexp(r"^/"))
async def on_any_message(message: Message, state):
    """
    Обработчик всех остальных сообщений.
    Сбрасывает состояние и показывает главное меню.
    """
    from modules_common.ui import welcome_text, build_main_menu_kb
    from scheduler import register_notice_chat

    try:
        register_notice_chat(message.chat.id)
        log.debug(f"Registered chat {message.chat.id} for notices on fallback")
    except Exception as e:
        log.error(f"Failed to register chat {message.chat.id}: {e}", exc_info=True)

    try:
        await state.clear()
    except Exception as e:
        log.warning(f"Failed to clear state: {e}")

    await message.answer(welcome_text(), reply_markup=build_main_menu_kb())
