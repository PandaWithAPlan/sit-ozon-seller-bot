"""
Глобальный обработчик ошибок.
"""
import logging
import traceback
from aiogram import Router
from aiogram.types import ErrorEvent, Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

error_router = Router(name="errors")
log = logging.getLogger("seller-bot.errors")

@error_router.errors()
async def global_error_handler(event: ErrorEvent):
    """
    Перехватывает все необработанные исключения в хендлерах.
    """
    exception = event.exception
    update = event.update

    # Игнорируем старые апдейты или известные безопасные ошибки
    if isinstance(exception, TelegramBadRequest):
        if "message is not modified" in str(exception):
            return
        if "query is too old" in str(exception):
            return

    # Логируем полный трейсбек
    log.error(f"Global error handler caught: {exception}", exc_info=exception)

    # Пытаемся уведомить пользователя, если это возможно
    try:
        if update.message:
            await update.message.answer("⚠️ Произошла внутренняя ошибка. Мы уже работаем над исправлением.")
        elif update.callback_query:
            await update.callback_query.message.answer("⚠️ Произошла ошибка при обработке кнопки.")
            # Пытаемся закрыть часики, если они крутятся
            try:
                await update.callback_query.answer()
            except Exception:
                pass
    except Exception as e:
        log.error(f"Failed to send error notification: {e}")
